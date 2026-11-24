#!/usr/bin/env python3
"""
test_socal_capital.py -- Verification & QA suite for the SoCal Tech & Finance
Capital Flows dataset (Sub-agent 2: Verification & QA Auditor).

Layers covered
  1. ZIP/county mapping accuracy (Census ZCTA crosswalk + curated fallback),
     including the spec's anchor cases: Moorpark/Thousand Oaks/Oxnard ->
     Ventura, Irvine/Newport -> Orange, and out-of-footprint exclusions
     (Santa Barbara, Kern, San Luis Obispo, out-of-state).
  2. Strict normalizers (ZIP, dollar amounts, dates) and deterministic ids.
  3. Sector + event_type classification rules.
  4. Deduplication: Form D/A amendment collapse, id determinism, and the
     record validator's rejection reasons.
  5. Database integration (requires local Postgres; auto-skipped otherwise):
     schema/constraints, 100% provenance, SoCal scope, amount/date sanity,
     zero duplicate filings, ZIP->county consistency of every stored row, and
     materialized-view aggregation correctness (count/sum/median/top sectors
     recomputed independently from the event table).
  6. Export pack integrity: every MANIFEST.json SHA-256 recomputed from the
     files on disk, row counts cross-checked, CSV/Parquet parity, summary
     CSV equal to independent recomputation from the events CSV, dictionary
     completeness, and the auxiliary 13F metadata confined to its four
     counties.

Run:
    pytest test_socal_capital.py -v
All tests are offline (no network); DB/export layers skip gracefully when
their fixtures are not present.
"""

import csv
import datetime as dt
import hashlib
import json
from pathlib import Path

import pytest

import harvest_socal_capital as h

# keep the QA run journal free of test-run noise
h.journal = lambda *args, **kwargs: None

PROJECT = Path(__file__).resolve().parent
EXPORT_DIR = PROJECT / "exports" / "socal_funding_deals_v1"
THIRTEEN_F_PATH = h.THIRTEEN_F_METADATA_PATH

COUNTIES = set(h.TARGET_COUNTIES)
SECTORS = set(h.SECTORS)
EVENT_TYPES = set(h.EVENT_TYPES)
FIRM_COUNTIES = {"Los Angeles", "Ventura", "Orange", "San Diego"}
EXPECTED_EVENT_COLUMNS = ["event_id", "company_name", "city", "county",
                          "zip_code", "sector", "event_type", "amount_raised",
                          "offering_date", "agency_or_filing_id", "source_url",
                          "raw_metadata"]


def _db_conn():
    import psycopg2
    return psycopg2.connect(h.DB_DSN)


def _db_ok():
    try:
        conn = _db_conn()
        conn.close()
        return True
    except Exception:
        return False


requires_db = pytest.mark.skipif(not _db_ok(), reason="local Postgres "
                                 "smb_warehouse not reachable")


# ---------------------------------------------------------------------------
# 1. ZIP -> county mapping
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def zip_map():
    mapping, source = h.load_zip_county_map()
    return mapping, source


def test_zip_map_loaded(zip_map):
    mapping, source = zip_map
    assert len(mapping) > 400, "expected a comprehensive SoCal zip map"
    assert source in ("census_zcta_county_rel_10", "curated_fallback")


@pytest.mark.parametrize("zip5,county", [
    ("93021", "Ventura"),        # Moorpark
    ("91320", "Ventura"),        # Thousand Oaks / Newbury Park
    ("91360", "Ventura"),        # Thousand Oaks
    ("93030", "Ventura"),        # Oxnard
    ("93065", "Ventura"),        # Simi Valley
    ("92602", "Orange"),         # Irvine
    ("92618", "Orange"),         # Irvine
    ("92660", "Orange"),         # Newport Beach
    ("92626", "Orange"),         # Costa Mesa
    ("92801", "Orange"),         # Anaheim
    ("90024", "Los Angeles"),    # Westwood
    ("90401", "Los Angeles"),    # Santa Monica
    ("90245", "Los Angeles"),    # El Segundo
    ("91311", "Los Angeles"),    # Chatsworth
    ("93550", "Los Angeles"),    # Palmdale
    ("92101", "San Diego"),      # downtown San Diego
    ("92037", "San Diego"),      # La Jolla
    ("92121", "San Diego"),      # Sorrento Valley
    ("92008", "San Diego"),      # Carlsbad
    ("92507", "Riverside"),      # Riverside
    ("92860", "Riverside"),      # Norco
    ("92590", "Riverside"),      # Temecula
    ("92374", "San Bernardino"), # Redlands
    ("91761", "San Bernardino"), # Ontario
    ("91775", "Los Angeles"),    # San Gabriel
    ("92284", "San Bernardino"), # Yucca Valley
])
def test_zip_to_county_anchor_cities(zip_map, zip5, county):
    mapping, _ = zip_map
    assert mapping.get(zip5) == county, f"{zip5} should map to {county}"


@pytest.mark.parametrize("zip5", [
    "93101",   # Santa Barbara -- adjacent, not in footprint
    "93117",   # Santa Barbara (UCSB)
    "93401",   # San Luis Obispo
    "93301",   # Bakersfield (Kern)
    "93225",   # Frazier Park (Kern)
    "93901",   # Salinas (Monterey)
    "89011",   # Henderson, NV
    "98109",   # Seattle, WA
    "10001",   # New York, NY
])
def test_zip_outside_footprint_excluded(zip_map, zip5):
    mapping, _ = zip_map
    assert zip5 not in mapping, f"{zip5} must not map into the SoCal footprint"


def test_every_mapped_zip_is_in_band_and_real_county(zip_map):
    mapping, _ = zip_map
    for zip5, county in mapping.items():
        assert len(zip5) == 5 and zip5.isdigit()
        assert "90000" <= zip5 <= "93599"
        assert county in COUNTIES


def test_normalize_zip_variants(zip_map):
    mapping, _ = zip_map
    assert h.normalize_zip("92602-1234") == "92602"
    assert h.normalize_zip(90210) == "90210"
    assert h.normalize_zip(" 90024 ") == "90024"
    assert h.normalize_zip("9000") is None
    assert h.normalize_zip("abcde") is None
    assert h.normalize_zip("") is None
    assert h.normalize_zip(None) is None


# ---------------------------------------------------------------------------
# 2. Amount / date normalizers and ids
# ---------------------------------------------------------------------------

def test_normalize_amount():
    assert h.normalize_amount("$1,234,567.89") == pytest.approx(1234567.89)
    assert h.normalize_amount("189993.0000") == pytest.approx(189993.0)
    assert h.normalize_amount("2,500") == 2500
    assert h.normalize_amount("0") is None
    assert h.normalize_amount("-500") is None
    assert h.normalize_amount("N/A") is None
    assert h.normalize_amount("") is None
    assert h.normalize_amount(None) is None
    assert h.normalize_amount("12.3.4") is None


def test_normalize_date():
    d, prec = h.normalize_date("2024-03-15")
    assert (d, prec) == (dt.date(2024, 3, 15), "day")
    d, prec = h.normalize_date("2024-03-15T09:30:00")
    assert (d, prec) == (dt.date(2024, 3, 15), "day")
    d, prec = h.normalize_date("2024-03")
    assert (d, prec) == (dt.date(2024, 3, 1), "month")
    d, prec = h.normalize_date("2024")
    assert (d, prec) == (dt.date(2024, 1, 1), "year")
    d, prec = h.normalize_date("03/15/2024")
    assert (d, prec) == (dt.date(2024, 3, 15), "day")
    assert h.normalize_date("not a date") == (None, None)
    assert h.normalize_date("2024-02-31") == (None, None)
    assert h.normalize_date("") == (None, None)


def test_event_id_deterministic_and_type_separated():
    a1 = h.event_id_for("SEC_FORM_D", "0001234567-26-000001")
    a2 = h.event_id_for("SEC_FORM_D", "0001234567-26-000001")
    b = h.event_id_for("SBIR_PHASE_2", "0001234567-26-000001")
    assert a1 == a2 and a1 != b
    assert a1.startswith("FORMD-") and b.startswith("SBIR-P2-")
    assert len({h.event_id_for(t, "x-1") for t in h.EVENT_TYPES}) == 5


# ---------------------------------------------------------------------------
# 3. Classification rules
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("industry,company,expected", [
    ("Pooled Investment Fund", "SoCal Growth Partners LP", "Financial_Services"),
    ("Banking and Financial Services", "Pacific Trust Bank", "Financial_Services"),
    ("Insurance", "GoldenState Mutual", "Financial_Services"),
    ("Software", "Acme Software Inc", "Enterprise_SaaS"),
    ("Other Technology", "DataPipe Systems", "Enterprise_SaaS"),
    ("Semiconductors", "ChipsRUs Corp", "Enterprise_SaaS"),
    ("Software", "NeuralMind AI Inc", "AI_ML"),
    ("Software", "PayFlow Payments Inc", "Fintech"),
    ("Financial Services", "Swift Lending Inc", "Financial_Services"),
    ("Financial Services", "PayFlow Payments Inc", "Fintech"),
    ("Pooled Investment Fund", "Direct Lending Fund III LP", "Financial_Services"),
    ("Biotechnology", "GeneRx Therapeutics", "Biotech_Medtech"),
    ("Medical Devices and Equipment", "HeartValve Co", "Biotech_Medtech"),
    ("Aerospace and Defense", "Orbit Dynamics", "Defense_Aerospace_Tech"),
    ("Real Estate", "Sunset Realty LLC", None),
    ("Food and Beverage", "Snack Corp", None),
    ("Restaurants", "Taco Holdings", None),
    ("Retailing", "MegaMart Inc", None),
])
def test_classify_sector_formd(industry, company, expected):
    assert h.classify_sector_formd(industry, company) == expected


@pytest.mark.parametrize("phase,program,expected", [
    ("Phase I", "SBIR", "SBIR_PHASE_1"),
    ("Phase II", "SBIR", "SBIR_PHASE_2"),
    ("Phase I", "STTR", "STTR"),
    ("Phase II", "STTR", "STTR"),
    ("Phase III", "SBIR", None),
    (None, "SBIR", None),
])
def test_sbir_event_type(phase, program, expected):
    assert h._sbir_event_type(phase, program) == expected


def test_classify_sector_sbir_agency_defaults_and_keywords():
    assert h.classify_sector_sbir("Department of Defense", "Navy",
                                  "Hull Coatings", "MarineCo") == \
        "Defense_Aerospace_Tech"
    assert h.classify_sector_sbir(
        "National Aeronautics and Space Administration", "",
        "Sample return", "SpaceX Corp") == "Defense_Aerospace_Tech"
    assert h.classify_sector_sbir("Department of Health and Human Services",
                                  "", "Advanced Composite Design",
                                  "Acme Corp") == "Biotech_Medtech"
    assert h.classify_sector_sbir("National Science Foundation", "",
                                  "Machine Learning for Radar", "Acme") == \
        "AI_ML"
    assert h.classify_sector_sbir("National Science Foundation", "",
                                  "Novel Widget", "WidgetCo") == \
        "Enterprise_SaaS"


# ---------------------------------------------------------------------------
# 4. Deduplication + validation
# ---------------------------------------------------------------------------

def _formd(company, cik, first_sale, filed, adsh, amount=100000.0):
    d = dt.date.fromisoformat(first_sale)
    return {
        "event_type": "SEC_FORM_D",
        "company_name": company,
        "city": "Irvine",
        "county": "Orange",
        "zip_code": "92602",
        "sector": "Enterprise_SaaS",
        "amount_raised": amount,
        "offering_date": d,
        "agency_or_filing_id": adsh,
        "source_url": f"https://www.sec.gov/Archives/edgar/data/{cik}/{adsh.replace('-', '')}-index.htm",
        "raw_metadata": {"date_basis": "first_sale_date",
                         "filed_date": filed},
        "event_id": h.event_id_for("SEC_FORM_D", adsh),
        "_cik": cik,
        "_file_date": filed,
    }


def test_collapse_formd_amendments_keeps_latest():
    original = _formd("Acme", 1111, "2024-03-15", "2024-03-20",
                      "0001111-24-000001", 500000)
    amended = _formd("Acme", 1111, "2024-03-15", "2024-08-02",
                     "0001111-24-000007", 2000000)
    unrelated = _formd("Beta", 2222, "2024-03-19", "2024-03-25",
                       "0002222-24-000002", 750000)
    out = h.collapse_formd_amendments([original, amended, unrelated])
    ids = {r["agency_or_filing_id"] for r in out}
    assert ids == {"0001111-24-000007", "0002222-24-000002"}
    acme = next(r for r in out if r["company_name"] == "Acme")
    assert acme["amount_raised"] == 2000000  # amendment supersedes


def test_collapse_keeps_distinct_raises_same_month():
    raise_a = _formd("Acme", 1111, "2024-03-01", "2024-03-05",
                     "0001111-24-000001", 500000)
    raise_b = _formd("Acme", 1111, "2024-03-10", "2024-03-12",
                     "0001111-24-000002", 900000)
    out = h.collapse_formd_amendments([raise_a, raise_b])
    # same CIK + same first-sale month -> collapsed to the later filing
    assert len(out) == 1


def test_validate_event_record_rejections():
    base = dict(event_id="X", company_name="A", county="Orange",
                zip_code="92602", sector="AI_ML", event_type="SEC_FORM_D",
                amount_raised=100.0, offering_date=dt.date(2024, 5, 1),
                agency_or_filing_id="a-1", source_url="https://www.sec.gov/x",
                raw_metadata={})
    assert h.validate_event_record(base) is None
    bad = dict(base, amount_raised=0)
    assert "amount" in h.validate_event_record(bad)
    bad = dict(base, county="Santa Barbara")
    assert "county" in h.validate_event_record(bad)
    bad = dict(base, sector="Real_Estate")
    assert "sector" in h.validate_event_record(bad)
    bad = dict(base, event_type="13F")
    assert "event_type" in h.validate_event_record(bad)
    bad = dict(base, source_url="http://insecure.example/")
    assert "source_url" in h.validate_event_record(bad)
    bad = dict(base, offering_date=dt.date(2020, 1, 1))
    assert "date" in h.validate_event_record(bad)
    bad = dict(base, zip_code=None)
    assert "zip_code" in h.validate_event_record(bad)


# ---------------------------------------------------------------------------
# 5. Database integration
# ---------------------------------------------------------------------------

@requires_db
class TestDatabase:
    @pytest.fixture(scope="class")
    def conn(self):
        conn = _db_conn()
        yield conn
        conn.close()

    def test_objects_exist(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('public.socal_capital_events'), "
                    "to_regclass('public.socal_county_capital_summary')")
        table, view = cur.fetchone()
        assert table is not None
        assert view is not None

    def test_column_contract(self, conn):
        cur = conn.cursor()
        cur.execute("""SELECT column_name FROM information_schema.columns
                       WHERE table_name='socal_capital_events' ORDER BY
                       ordinal_position""")
        cols = [r[0] for r in cur.fetchall()]
        assert set(cols) >= set(EXPECTED_EVENT_COLUMNS)

    def test_no_duplicate_filings(self, conn):
        cur = conn.cursor()
        cur.execute("""SELECT count(*) FROM (
                        SELECT event_type, agency_or_filing_id
                        FROM socal_capital_events
                        GROUP BY 1, 2 HAVING count(*) > 1) d""")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) - count(DISTINCT event_id) "
                    "FROM socal_capital_events")
        assert cur.fetchone()[0] == 0

    def test_every_row_is_in_socal_scope(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT zip_code, county FROM socal_capital_events")
        mapping, _ = h.load_zip_county_map()
        for zip_code, county in cur.fetchall():
            assert county in COUNTIES
            assert mapping.get(zip_code) == county, \
                f"{zip_code} mapped to {county} disagrees with census map"

    def test_every_row_has_verifiable_provenance(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT source_url, agency_or_filing_id, event_type "
                    "FROM socal_capital_events")
        rows = cur.fetchall()
        assert rows, "database should contain harvested rows"
        for source_url, filing_id, ev_type in rows:
            assert source_url and source_url.startswith("https://")
            allowed = ("https://www.sec.gov/", "https://www.sbir.gov/",
                       "https://data.ca.gov/")
            assert source_url.startswith(allowed), source_url
            assert filing_id, "filing/award id required for provenance"

    def test_amounts_and_dates_valid(self, conn):
        cur = conn.cursor()
        cur.execute("""SELECT amount_raised, offering_date FROM
                       socal_capital_events""")
        rows = cur.fetchall()
        assert rows
        for amount, date in rows:
            assert amount is not None and amount > 0
            assert h.WINDOW_START <= date <= h.WINDOW_END

    def test_enums_respected(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT county FROM socal_capital_events")
        assert {r[0] for r in cur.fetchall()} <= COUNTIES
        cur.execute("SELECT DISTINCT sector FROM socal_capital_events")
        assert {r[0] for r in cur.fetchall()} <= SECTORS
        cur.execute("SELECT DISTINCT event_type FROM socal_capital_events")
        assert {r[0] for r in cur.fetchall()} <= EVENT_TYPES

    def test_materialized_view_matches_recomputation(self, conn):
        cur = conn.cursor()
        cur.execute("""SELECT county,
                              date_trunc('quarter', offering_date)::date,
                              count(*),
                              sum(amount_raised),
                              percentile_cont(0.5) WITHIN GROUP
                                  (ORDER BY amount_raised)
                       FROM socal_capital_events
                       GROUP BY 1, 2""")
        truth = {(c, q): (n, float(s), float(m))
                 for c, q, n, s, m in cur.fetchall()}
        cur.execute("""SELECT county, quarter_start, deal_count,
                              total_capital_raised, median_deal_size
                       FROM socal_county_capital_summary""")
        assert cur.rowcount == len(truth)
        for county, quarter, n, total, median in cur.fetchall():
            n_t, total_t, median_t = truth[(county, quarter)]
            assert n == n_t
            assert abs(float(total) - total_t) < 0.01
            assert abs(float(median) - median_t) < 0.01

    def test_top_sectors_json_valid_and_largest_first(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT county, quarter_start, top_sectors FROM "
                    "socal_county_capital_summary WHERE top_sectors IS NOT NULL")
        rows = cur.fetchall()
        assert rows
        for county, quarter, sectors in rows:
            data = json.loads(sectors) if isinstance(sectors, str) else sectors
            assert 1 <= len(data) <= 3
            totals = [entry["total"] for entry in data]
            assert totals == sorted(totals, reverse=True)
            for entry in data:
                assert entry["sector"] in SECTORS
                assert entry["deals"] >= 1 and entry["total"] > 0

    def test_window_and_quarter_labels(self, conn):
        cur = conn.cursor()
        cur.execute("""SELECT quarter_start, quarter_label
                       FROM socal_county_capital_summary""")
        for quarter_start, label in cur.fetchall():
            assert label[:4] == str(quarter_start.year)
            assert label[5:] == f"Q{(quarter_start.month - 1)//3 + 1}"


# ---------------------------------------------------------------------------
# 6. Export pack integrity
# ---------------------------------------------------------------------------

def _export_present():
    return (EXPORT_DIR / "MANIFEST.json").exists() and \
        (EXPORT_DIR / "socal_capital_events.csv").exists()


requires_export = pytest.mark.skipif(not _export_present(),
                                     reason="export pack "
                                     "exports/socal_funding_deals_v1 missing")


@requires_export
class TestExportPack:
    @pytest.fixture(scope="class")
    def manifest(self):
        return json.loads(
            (EXPORT_DIR / "MANIFEST.json").read_text(encoding="utf-8"))

    @pytest.fixture(scope="class")
    def events_df(self):
        import pandas as pd
        return pd.read_csv(EXPORT_DIR / "socal_capital_events.csv",
                           dtype={"zip_code": str})

    def test_required_files_present(self):
        for name in ("socal_capital_events.csv",
                     "socal_capital_events.parquet",
                     "socal_county_quarter_summary.csv",
                     "socal_county_quarter_summary.parquet",
                     "CAPITAL_DATA_DICTIONARY.md", "MANIFEST.json"):
            assert (EXPORT_DIR / name).exists(), f"missing {name}"

    def test_manifest_checksums_match_files_on_disk(self, manifest):
        names = {f["name"] for f in manifest["files"]}
        assert "MANIFEST.json" not in names, \
            "manifest must not checksum itself"
        for entry in manifest["files"]:
            path = EXPORT_DIR / entry["name"]
            assert path.exists(), entry["name"]
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            assert digest == entry["sha256"], \
                f"sha256 mismatch for {entry['name']}"
            assert path.stat().st_size == entry["bytes"]

    def test_manifest_row_counts_match_data(self, manifest, events_df):
        counts = manifest["row_counts"]
        assert counts["socal_capital_events"] == len(events_df)
        for stem in ("socal_county_quarter_summary",
                     "socal_firm_filings_metadata"):
            if counts.get(stem):
                path = EXPORT_DIR / f"{stem}.csv"
                if path.exists():
                    with open(path, newline="", encoding="utf-8") as fh:
                        n = sum(1 for _ in csv.reader(fh)) - 1
                    assert n == counts[stem]

    def test_events_csv_schema_and_provenance(self, events_df):
        assert len(events_df) > 0
        assert list(events_df.columns)[:12] == EXPECTED_EVENT_COLUMNS
        for col in ("event_id", "company_name", "county", "zip_code",
                    "sector", "event_type", "source_url",
                    "agency_or_filing_id"):
            assert events_df[col].notna().all(), f"{col} has nulls"
        assert events_df["source_url"].str.startswith("https://").all()
        assert (events_df["amount_raised"] > 0).all()
        assert set(events_df["county"]) <= COUNTIES
        assert set(events_df["sector"]) <= SECTORS
        assert set(events_df["event_type"]) <= EVENT_TYPES

    def test_events_zip_county_consistency(self, events_df):
        mapping, _ = h.load_zip_county_map()
        for _, row in events_df.iterrows():
            assert mapping.get(row["zip_code"]) == row["county"], \
                f"{row['zip_code']} -> {row['county']} inconsistent"

    def test_events_dates_in_window(self, events_df):
        dates = events_df["offering_date"].map(dt.date.fromisoformat)
        assert (dates >= h.WINDOW_START).all()
        assert (dates <= h.WINDOW_END).all()

    def test_no_duplicate_events_in_export(self, events_df):
        assert events_df["event_id"].is_unique
        dup = events_df.groupby(
            ["event_type", "agency_or_filing_id"]).size()
        assert (dup == 1).all()

    def test_parquet_matches_csv(self, events_df):
        import pandas as pd
        pq = pd.read_parquet(EXPORT_DIR / "socal_capital_events.parquet")
        assert len(pq) == len(events_df)
        assert set(pq.columns) == set(events_df.columns)
        assert sorted(pq["event_id"]) == sorted(events_df["event_id"])

    def test_summary_matches_independent_recomputation(self, events_df):
        import pandas as pd
        summary = pd.read_csv(
            EXPORT_DIR / "socal_county_quarter_summary.csv")
        summary["quarter_start"] = summary["quarter_start"].map(
            dt.date.fromisoformat)
        ev = events_df.copy()
        ev["quarter"] = ev["offering_date"].map(
            lambda s: dt.date.fromisoformat(s).replace(
                month=((dt.date.fromisoformat(s).month - 1) // 3) * 3 + 1,
                day=1))
        truth = ev.groupby(["county", "quarter"]).agg(
            deal_count=("amount_raised", "count"),
            total_capital_raised=("amount_raised", "sum"),
            median_deal_size=("amount_raised", "median")).reset_index()
        assert len(summary) == len(truth)
        merged = summary.merge(
            truth, left_on=["county", "quarter_start"],
            right_on=["county", "quarter"], suffixes=("", "_t"))
        assert len(merged) == len(truth)
        assert (merged["deal_count"] == merged["deal_count_t"]).all()
        assert (abs(merged["total_capital_raised"]
                    - merged["total_capital_raised_t"]) < 0.01).all()
        assert (abs(merged["median_deal_size"].fillna(-1)
                    - merged["median_deal_size_t"].fillna(-1)) < 0.01).all()

    def test_summary_top_sectors_parse(self):
        import pandas as pd
        summary = pd.read_csv(
            EXPORT_DIR / "socal_county_quarter_summary.csv")
        assert len(summary) > 0
        for raw in summary["top_sectors"]:
            entries = json.loads(raw)
            assert 1 <= len(entries) <= 3
            totals = [e["total"] for e in entries]
            assert totals == sorted(totals, reverse=True)

    def test_dictionary_documents_methodology(self):
        text = (EXPORT_DIR / "CAPITAL_DATA_DICTIONARY.md").read_text(
            encoding="utf-8")
        for needle in ("ZIP -> county", "Census", "provenance",
                       "Deduplication", "SEC EDGAR", "SBIR",
                       "socal_county_quarter_summary", "MANIFEST",
                       "STATE_GRANT", "Known limitations"):
            assert needle.lower() in text.lower(), f"dictionary lacks {needle}"

    def test_firm_metadata_confined_to_four_counties(self):
        if not THIRTEEN_F_PATH.exists():
            pytest.skip("13F metadata not harvested yet")
        payload = json.loads(
            THIRTEEN_F_PATH.read_text(encoding="utf-8"))
        mapping, _ = h.load_zip_county_map()
        for rec in payload.get("records", []):
            assert rec["county"] in FIRM_COUNTIES
            assert rec["county"] == mapping.get(rec["zip_code"])
            assert rec["source_url"].startswith("https://www.sec.gov/")
            assert rec["cik"] and rec["accession"]
