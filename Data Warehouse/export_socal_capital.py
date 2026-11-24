#!/usr/bin/env python3
"""
export_socal_capital.py -- export the verified SoCal Tech & Finance Capital
Flows dataset into exports/socal_funding_deals_v1/ (Sub-agent 1 module).

Outputs
  socal_capital_events.csv / .parquet      one row per verified capital event
                                           (SEC Form D raises, SBIR Phase I/II
                                           and STTR awards) with 100%
                                           source_url provenance
  socal_county_quarter_summary.csv/.parquet  county x quarter aggregation from
                                           the socal_county_capital_summary
                                           materialized view
  socal_firm_filings_metadata.csv          auxiliary SEC 13F-HR firm metadata
                                           for LA/Ventura/Orange/San Diego
                                           investment managers (holdings
                                           disclosures, not raises)
  CAPITAL_DATA_DICTIONARY.md               schema + methodology + provenance
  MANIFEST.json                            SHA-256 checksums + row counts for
                                           every file above

The materialized view is refreshed (non-concurrently -- local single-writer
assumption inside this module's own MV) before export so the summary always
matches the event table.

Run:
    python3 export_socal_capital.py
    python3 export_socal_capital.py --out-dir exports/socal_funding_deals_v1
"""

import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2

DB_DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/smb_warehouse")

PROJECT_DIR = Path(__file__).resolve().parent
THIRTEEN_F_METADATA_PATH = (PROJECT_DIR / "data_cache" / "capital"
                            / "edgar_13f_metadata.json")
SBIR_BULK_PATH = (PROJECT_DIR / "data_cache" / "capital" / "sbir_bulk"
                  / "award_data_no_abstract.csv")
SBIR_BULK_URL = ("https://data.www.sbir.gov/mod_awarddatapublic_no_abstract/"
                 "award_data_no_abstract.csv")
CENSUS_REL_URL = ("https://www2.census.gov/geo/docs/maps-data/data/rel/"
                  "zcta_county_rel_10.txt")
SBIR_PORTAL_URL = "https://www.sbir.gov/awards"
DEFAULT_OUT = PROJECT_DIR / "exports" / "socal_funding_deals_v1"

EVENT_COLUMNS = ["event_id", "company_name", "city", "county", "zip_code",
                 "sector", "event_type", "amount_raised", "offering_date",
                 "agency_or_filing_id", "source_url", "raw_metadata"]
SUMMARY_COLUMNS = ["county", "quarter_start", "quarter_label", "deal_count",
                   "total_capital_raised", "median_deal_size", "top_sectors"]
FIRM_COLUMNS = ["filer_name", "cik", "accession", "form", "filed_date",
                "period_of_report", "city", "state", "zip_code", "county",
                "total_holdings_value_usd", "source_url"]


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch_rows(cur, sql):
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)


def export_frame(df, stem, out_dir, manifest_files, description):
    csv_path = out_dir / f"{stem}.csv"
    parquet_path = out_dir / f"{stem}.parquet"
    df.to_csv(csv_path, index=False, quoting=csv.QUOTE_MINIMAL,
              encoding="utf-8")
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    for path in (csv_path, parquet_path):
        manifest_files.append({
            "name": path.name,
            "sha256": _sha256(path),
            "bytes": path.stat().st_size,
            "rows": len(df),
            "description": description,
        })
    return len(df)


def build_dictionary(df_events, df_summary, df_firms, manifest_files,
                     db_breakdowns, window, zip_map_source, bulk_sha):
    gen_ts = f"{dt.datetime.now():%Y-%m-%d %H:%M:%S %Z}".strip()
    w_start, w_end = window
    total = len(df_events)
    total_dollars = float(df_events["amount_raised"].sum()) if total else 0.0

    county_lines = []
    for county, grp in sorted(df_events.groupby("county")):
        n = len(grp)
        dol = grp["amount_raised"].sum()
        by_type = grp["event_type"].value_counts().to_dict()
        type_str = ", ".join(f"{k}: {v}" for k, v in sorted(by_type.items()))
        county_lines.append(f"| {county} | {n} | ${dol:,.0f} | {type_str} |")

    type_lines = []
    for ev_type, grp in sorted(df_events.groupby("event_type")):
        type_lines.append(
            f"| {ev_type} | {len(grp)} | ${grp['amount_raised'].sum():,.0f} "
            f"| ${grp['amount_raised'].median():,.0f} |")

    sector_lines = []
    for sector, grp in sorted(df_events.groupby("sector")):
        sector_lines.append(
            f"| {sector} | {len(grp)} | ${grp['amount_raised'].sum():,.0f} |")

    files_rows = "\n".join(
        f"| `{f['name']}` | {f['rows']:,} | {f['bytes']:,} | "
        f"`{f['sha256'][:16]}…` |" for f in manifest_files)

    dictionary = f"""# CAPITAL_DATA_DICTIONARY
## Southern California Tech & Finance Capital Flows Dataset (`socal_funding_deals_v1`)

Generated: {gen_ts}
Coverage window: **{w_start} through {w_end}** (rolling three years at harvest
time; the window is enforced when records are loaded).
Footprint: Los Angeles, Ventura, Orange, San Diego, Riverside and
San Bernardino counties, California (ZIP Code Tabulation Areas 90000-93599
that map to those six counties).

Every record is a REAL public record. Nothing in this dataset is modeled,
synthesized or interpolated; rows that could not be verified against a public
filing/award were dropped, and {total:,} rows survived the full verification
pipeline (${total_dollars:,.0f} in aggregate capital events).

---

## 1. Files in this pack

| File | Rows | Bytes | SHA-256 (first 16) |
|---|---|---|---|
{files_rows}

`MANIFEST.json` carries the full SHA-256 of every file (it excludes itself),
plus row counts and the harvest window. Verify with, e.g.:
`shasum -a 256 socal_capital_events.csv` and compare against the manifest.

## 2. `socal_capital_events` (CSV + Parquet)

| Column | Type | Description |
|---|---|---|
| `event_id` | string | Stable primary key: a type prefix (FORMD-,
  SBIR-P1-, SBIR-P2-, STTR- or GRANT-) plus 12 hex chars of SHA-1 over
  `event_type|agency_or_filing_id` -- the same filing always maps to the
  same id, so re-harvests are naturally idempotent. |
| `company_name` | string | Issuer / awardee company name, verbatim from the
  source record (whitespace-normalized). |
| `city` | string | Company mailing city from the source record. |
| `county` | enum | One of {', '.join('`'+c+'`' for c in ['Los Angeles','Orange','Ventura','San Diego','Riverside','San Bernardino'])}. Derived from ZIP (see §4). |
| `zip_code` | string | 5-digit ZIP of the company's filing/award address. |
| `sector` | enum | {', '.join('`'+s+'`' for s in ['Fintech','Enterprise_SaaS','AI_ML','Defense_Aerospace_Tech','Biotech_Medtech','Financial_Services'])} -- see §5. |
| `event_type` | enum | `SEC_FORM_D` (SEC EDGAR private placement),
  `SBIR_PHASE_1`, `SBIR_PHASE_2` (federal SBIR award), `STTR` (federal
  STTR award), `STATE_GRANT` (reserved; see §7). |
| `amount_raised` | number | USD. Form D: "Total Amount Sold" when > 0,
  otherwise the planned "Total Offering Amount" (`amount_basis` in
  `raw_metadata` says which). SBIR/STTR: federal award amount. |
| `offering_date` | date | Form D: date of first sale when known (partial
  first-sale dates are normalized to month start; `date_basis` /
  `date_precision` in `raw_metadata` record the basis), else the filing
  date. SBIR/STTR: "Proposal Award Date". |
| `agency_or_filing_id` | string | SEC accession number (e.g.
  `0001234567-26-000123`) or SBIR agency tracking id (e.g. `NSF-2527821`). |
| `source_url` | string | URL where the row can be verified (see §3). |
| `raw_metadata` | JSON string | Full provenance: amounts reported, dates,
  industry group / award title, agency, phase, bulk-file checksums, EDGAR
  filing links, dedup flags. |

Deduplication guarantees (enforced in the DB by `PRIMARY KEY (event_id)` and
`UNIQUE (event_type, agency_or_filing_id)`):
* one row per SEC accession -- the same Form D filing can never appear twice;
* Form D/A amendments are collapsed to the **most recently filed** accession
  per (issuer CIK, first-sale month), so an offering amended by a D/A is
  counted once at its updated amount (`raw_metadata.amendment_collapsed`);
* one row per agency tracking number for SBIR/STTR awards.

## 3. Provenance -- how to verify every row

| Source | Rows | Verification path |
|---|---|---|
| SEC EDGAR Form D | {{SEC_FORM_D_ROWS}} | `source_url` points at the EDGAR
  filing index (`https://www.sec.gov/Archives/edgar/data/<cik>/<accession>-
  index.htm`); open it, open `formd.xml`/`primary_doc.xml`, and the issuer
  address, ZIP, amounts and dates in `raw_metadata` are all visible in the
  XML. |
| SBIR/STTR awards | {{SBIR_ROWS}} | `source_url` is the SBIR.gov awards
  database (`https://www.sbir.gov/awards`) where each award is searchable by
  company or tracking number; `raw_metadata.bulk_file_url` +
  `raw_metadata.bulk_file_sha256` pin the exact official bulk CSV row the
  record was normalized from (sha256 `{bulk_sha[:16]}…`). |
| SEC EDGAR 13F-HR (auxiliary) | {{FIRM_ROWS}} | Same EDGAR filing-index
  pattern in `source_url`. |

SEC EDGAR Form ADV filings are registered with the IAPD system, not EDGAR,
and IAPD has no public bulk API; ADV metadata is therefore out of scope for
this harvest and recorded here as a documented gap.

## 4. ZIP -> county methodology

ZIP attribution uses the **US Census 2010 ZCTA-to-county relationship file**
(`{CENSUS_REL_URL}`, cached at
`data_cache/capital/census/zcta_county_rel_10.txt` at harvest time): each
ZIP is assigned to the county holding the **largest share of the ZCTA's
population** (`ZPOPPCT`), the standard rule for split ZCTAs (e.g. 91361
Westlake Village is majority Los Angeles County). Spot checks:
Moorpark 93021 / Thousand Oaks 91320 / Oxnard 930xx -> Ventura;
Irvine 926xx / Newport Beach 9266x -> Orange; Santa Monica 904xx ->
Los Angeles; La Jolla 92037 -> San Diego; Riverside 9250x -> Riverside;
Redlands 92373 -> San Bernardino. ZCTAs outside the six counties
(e.g. Santa Barbara 931xx, Kern 932xx/933xx, San Luis Obispo 934xx) are
excluded even though they fall inside the 90000-93599 numeric band.
`raw_metadata.county_source` names the mapping edition used per record.
(zip_map_source at harvest time: `{zip_map_source}`)

## 5. Sector classification

| Dataset sector | Mapping rule |
|---|---|
| `Financial_Services` | Form D industry groups: Pooled Investment Fund,
  Banking and Financial Services, Financial Services, Investment Banking,
  Other Finance, Insurance. |
| `Fintech` | Finance/tech filings whose issuer name shows fintech-software
  signals (payments, lending, crypto, blockchain, insurtech, wallets). |
| `Enterprise_SaaS` | Form D Software / Internet / IT Services / Computers /
  Electronics / Semiconductors / Telecom / Other Technology / Business
  Services, and zero-signal SBIR awards from NSF/DOE/DOC etc. |
| `AI_ML` | Tech/defense filings or awards whose name/title/abstract shows
  AI keywords (artificial intelligence, machine learning, deep learning,
  neural networks, computer vision, LLM...). |
| `Defense_Aerospace_Tech` | Form D Aerospace and Defense; SBIR awards from
  DoD/NASA or with defense keywords. |
| `Biotech_Medtech` | Form D Biotechnology / Medical Devices / Healthcare;
  HHS/NIH awards or biomedical keywords. |

Form D filings in industries outside the six Tech & Finance sectors (real
estate, food, retail, energy, media...) are **excluded** rather than
force-classified -- this keeps the dataset scoped to Tech & Finance capital
flows.

## 6. Dataset totals

Total events: {total:,} -- ${total_dollars:,.0f}

By county:

| County | Events | Capital | Breakdown |
|---|---|---|---|
{chr(10).join(county_lines) or '| (empty) | | | |'}

By event type:

| Event type | Events | Total | Median deal |
|---|---|---|---|
{chr(10).join(type_lines) or '| (empty) | | | |'}

By sector:

| Sector | Events | Total |
|---|---|---|
{chr(10).join(sector_lines) or '| (empty) | | | |'}

## 7. `socal_county_quarter_summary` (CSV + Parquet)

Materialized-view export from `socal_county_capital_summary` (definition in
`funding_schema.sql`), one row per county x calendar quarter:

| Column | Description |
|---|---|
| `county` | SoCal county. |
| `quarter_start` | First day of the quarter (date_trunc on `offering_date`). |
| `quarter_label` | e.g. `2024Q3`. |
| `deal_count` | Events in the county-quarter. |
| `total_capital_raised` | Sum of `amount_raised`. |
| `median_deal_size` | Median `amount_raised` (percentile_cont 0.5). |
| `top_sectors` | JSON array of the 3 largest sectors by dollars:
  `[{{"sector": "...", "deals": n, "total": usd}}, ...]`. |

## 8. `socal_firm_filings_metadata.csv` (auxiliary)

SEC 13F-HR filings by investment managers whose filer address is in Los
Angeles, Ventura, Orange or San Diego county. 13F is a quarterly *holdings*
disclosure -- not a raise -- so these rows are provided as firm metadata
(filer, CIK, accession, filing/period dates, location, total reported
holdings value) rather than as capital events. Columns: {', '.join(FIRM_COLUMNS)}.

## 9. Known limitations (documented, not hidden)

* SBIR bulk data has no per-award abstract in the no-abstract edition; sector
  classification uses award title + agency/branch + company name, with agency
  defaults for zero-signal awards.
* EDGAR Form D coverage is driven by a curated list of ~190 SoCal mailing
  cities used as full-text search probes; issuers whose filing lists a city
  outside that list could be missed. Every retained row is re-verified from
  the filing XML (state=CA + ZIP->county), so inclusion is strict even where
  coverage is approximate.
* Form D "date of first sale" is sometimes filed at month precision; such
  rows are bucketed at month start and flagged in `raw_metadata`.
* 13F values are reported in full dollars for periods after 2022 (SEC rule
  change effective January 2023), matching this dataset's window.
* `STATE_GRANT` is reserved: the California Grants Portal (data.ca.gov)
  publishes no recipient ZIP and is dominated by non-business service
  grants, so no rows were loaded rather than fabricate locations/sectors.
* Public sources occasionally restate/correct records; re-running
  `harvest_socal_capital.py` refreshes the warehouse from the live sources.

## 10. Reproducibility

```
psql "$DATABASE_URL" -f funding_schema.sql
python3 harvest_socal_capital.py            # --refresh to bypass the cache
python3 export_socal_capital.py
```
Raw source responses are cached under `data_cache/capital/` and every
(request, status) is journaled in `data_cache/capital/run_log.jsonl`.
"""
    sec_d = int((df_events["event_type"] == "SEC_FORM_D").sum()) if total else 0
    sbir_n = int(df_events["event_type"].isin(
        ["SBIR_PHASE_1", "SBIR_PHASE_2", "STTR"]).sum()) if total else 0
    dictionary = dictionary.replace("{SEC_FORM_D_ROWS}", f"{sec_d:,}")
    dictionary = dictionary.replace("{SBIR_ROWS}", f"{sbir_n:,}")
    dictionary = dictionary.replace("{FIRM_ROWS}", f"{len(df_firms):,}")
    return dictionary


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Export the SoCal capital flows dataset pack")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT))
    parser.add_argument("--no-mv-refresh", action="store_true",
                        help="skip refreshing the materialized view")
    args = parser.parse_args(argv)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    dsn = sys.modules[__name__].DB_DSN

    conn = psycopg2.connect(dsn)
    try:
        cur = conn.cursor()
        if not args.no_mv_refresh:
            cur.execute("REFRESH MATERIALIZED VIEW "
                        "socal_county_capital_summary")
            conn.commit()
        df_events = fetch_rows(cur, f"""
            SELECT {', '.join(EVENT_COLUMNS)}
            FROM socal_capital_events
            ORDER BY county, offering_date, event_id""")
        df_summary = fetch_rows(cur, f"""
            SELECT {', '.join(SUMMARY_COLUMNS)}
            FROM socal_county_capital_summary
            ORDER BY county, quarter_start""")
        cur.execute("SELECT min(offering_date), max(offering_date) "
                    "FROM socal_capital_events")
        row = cur.fetchone()
    finally:
        conn.close()

    # normalize types
    df_events["amount_raised"] = df_events["amount_raised"].astype("float64")
    df_events["offering_date"] = df_events["offering_date"].astype(str)
    for col in ("quarter_start",):
        df_summary[col] = df_summary[col].astype(str)
    df_summary["total_capital_raised"] = \
        df_summary["total_capital_raised"].astype("float64")
    df_summary["median_deal_size"] = \
        df_summary["median_deal_size"].astype("float64")
    df_summary["top_sectors"] = df_summary["top_sectors"].apply(
        lambda v: json.dumps(v, ensure_ascii=False) if v is not None else None)
    df_events["raw_metadata"] = df_events["raw_metadata"].apply(
        lambda v: json.dumps(v, ensure_ascii=False) if v is not None else "{}")
    df_events["agency_or_filing_id"] = df_events["agency_or_filing_id"] \
        .astype(str)
    df_events["zip_code"] = df_events["zip_code"].astype(str)

    # auxiliary 13F firm metadata
    firm_records = []
    if THIRTEEN_F_METADATA_PATH.exists():
        payload = json.loads(
            THIRTEEN_F_METADATA_PATH.read_text(encoding="utf-8"))
        firm_records = payload.get("records", [])
    df_firms = pd.DataFrame(firm_records, columns=FIRM_COLUMNS)
    if len(df_firms):
        df_firms = df_firms.sort_values(
            ["county", "filer_name", "accession"]).reset_index(drop=True)
        df_firms["total_holdings_value_usd"] = \
            pd.to_numeric(df_firms["total_holdings_value_usd"])

    manifest_files = []
    n_events = export_frame(df_events, "socal_capital_events", out_dir,
                            manifest_files,
                            "Verified SoCal capital events (SEC Form D "
                            "raises, SBIR Phase I/II, STTR awards)")
    n_summary = export_frame(df_summary, "socal_county_quarter_summary",
                             out_dir, manifest_files,
                             "County x quarter aggregation from "
                             "socal_county_capital_summary")
    if len(df_firms):
        n_firms = export_frame(df_firms, "socal_firm_filings_metadata",
                               out_dir, manifest_files,
                               "Auxiliary SEC 13F-HR firm metadata "
                               "(holdings disclosures, not raises)")
    else:
        n_firms = 0

    bulk_sha = _sha256(SBIR_BULK_PATH) if SBIR_BULK_PATH.exists() else None
    zip_map_source = "census_zcta_county_rel_10"
    county_breakdown = df_events.groupby("county")["amount_raised"].agg(
        ["count", "sum"]).to_dict("index") if len(df_events) else {}

    dictionary = build_dictionary(
        df_events, df_summary, df_firms, manifest_files,
        {"county": county_breakdown},
        window=(row[0], row[1]) if row and row[0] else
        (dt.date.today() - dt.timedelta(days=3 * 365), dt.date.today()),
        zip_map_source=zip_map_source, bulk_sha=bulk_sha or "")
    dict_path = out_dir / "CAPITAL_DATA_DICTIONARY.md"
    dict_path.write_text(dictionary, encoding="utf-8")
    manifest_files.append({
        "name": dict_path.name,
        "sha256": _sha256(dict_path),
        "bytes": dict_path.stat().st_size,
        "rows": None,
        "description": "Data dictionary: schema, methodology, provenance",
    })

    manifest = {
        "dataset": "socal_funding_deals_v1",
        "description": "Southern California Tech & Finance Capital Flows "
                       "Dataset -- real private placements (SEC Form D), "
                       "federal SBIR/STTR R&D awards and (auxiliary) 13F-HR "
                       "firm filings for the six SoCal counties",
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "coverage_window": {
            "start": str(row[0]) if row and row[0] else None,
            "end": str(row[1]) if row and row[1] else None,
        },
        "checksum_algorithm": "sha256",
        "row_counts": {
            "socal_capital_events": n_events,
            "socal_county_quarter_summary": n_summary,
            "socal_firm_filings_metadata": n_firms,
        },
        "provenance": {
            "sources": [
                "SEC EDGAR full-text search + Form D primary documents "
                "(efts.sec.gov, sec.gov/Archives)",
                "SBIR.gov official bulk award data (no-abstract CSV) -- "
                "api.www.sbir.gov/public/api/awards is under maintenance",
                "SEC EDGAR 13F-HR primary documents (auxiliary metadata)",
            ],
            "zip_map_source": zip_map_source,
            "zip_map_url": CENSUS_REL_URL,
            "sbir_bulk_file": SBIR_BULK_URL,
            "sbir_bulk_sha256": bulk_sha,
            "event_types": ["SEC_FORM_D", "SBIR_PHASE_1", "SBIR_PHASE_2",
                            "STTR", "STATE_GRANT"],
            "state_grant_note": "California Grants Portal publishes no "
                                "recipient ZIP; 0 rows loaded (see data "
                                "dictionary §9)",
        },
        "files": manifest_files,
    }
    manifest_path = out_dir / "MANIFEST.json"
    manifest_path.write_text(json.dumps(manifest, indent=1) + "\n",
                             encoding="utf-8")

    print(f"exported to {out_dir}:")
    print(f"  socal_capital_events          {n_events:,} rows (csv+parquet)")
    print(f"  socal_county_quarter_summary  {n_summary:,} rows (csv+parquet)")
    print(f"  socal_firm_filings_metadata   {n_firms:,} rows (csv)")
    print(f"  CAPITAL_DATA_DICTIONARY.md    {len(dictionary):,} bytes")
    print(f"  MANIFEST.json                 {len(manifest_files)} checksummed files")
    return 0


if __name__ == "__main__":
    sys.exit(main())
