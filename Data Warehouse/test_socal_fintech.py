"""
QA suite for the SoCal Finance/Tech Intelligence Pack (schema.sql sections
2.11-2.16 + mv_socal_rate_guidance + harvest_socal_fintech.py +
export_dataset_pack.py --pack socal_v2).

SELF-CONTAINED, following the house throwaway-DB conventions of
test_schema.py: the whole suite runs against a THROWAWAY database it creates
and drops itself (socal_fintech_qa_test on the same PostgreSQL server), so it
is safe to run at any time and NEVER touches smb_warehouse -- or any other
existing database -- regardless of what data they contain. Run it with:

    cd "/Users/Owner/Desktop/GLM-5.3/Data Warehouse"
    python3 -m pytest test_socal_fintech.py -v

Lifecycle (module-scoped `conn` fixture; everything lives inside try/finally
so the teardown runs even if setup or a test fails):
  1. connect to the parent database (DATABASE_URL below; only the SERVER part
     matters; default: the live warehouse URL) and
     DROP DATABASE IF EXISTS socal_fintech_qa_test WITH (FORCE),
  2. CREATE DATABASE socal_fintech_qa_test,
  3. apply schema.sql, then analytics_views.sql, to the scratch database via
     subprocess psql (-v ON_ERROR_STOP=1; a non-zero exit fails the session
     with psql's captured stdout/stderr),
  4. seed SMALL synthetic SoCal fixtures (hand-picked plausible values, NOT
     the live harvest) covering all six new tables, refresh the views,
  5. run all tests against the scratch connection,
  6. teardown: close the test connection and
     DROP DATABASE IF EXISTS socal_fintech_qa_test WITH (FORCE).

Fixture vocabulary (all synthetic, prefixed 'QAtest' where names are free
text; deterministic so every expected value below is hand-computable):

  * socal_wage_benchmarks: 48 rows = 6 regions x 8 roles, data_year 2024,
    annual = hourly x 2080 exactly for every percentile. Region medians are
    hand-picked; oxnard_ventura_msa / 11-3021 (IT Manager) is seeded FULLY
    SUPPRESSED (all wage cells NULL, employment NULL) to mirror the live
    source suppression and prove the mv degrades to a NULL-rate row.
  * regional_industry_profiles: 24 rows = 6 verticals x 4 regions
    (california_state, la_msa, inland_empire_msa, oxnard_ventura_msa).
    Margins vary BY REGION on purpose (statewide = base, la = +3, inland = -2,
    oxnard = +1) so the market-comparison math is non-trivial. inland_empire's
    locality_burden is the required NEGATIVE case (-2.0); california_state is
    the ZERO-burden baseline; b2b_saas carries the required 65% utilization.
  * socal_macro_indicators: 6 monthly rows incl. the documented CPIAUCSL
    proxy pattern (same series_id under california_state AND
    oxnard_ventura_msa -- the reason the dedup key includes region_code).
  * socal_business_patterns: 3 county rows (incl. a 5112 -> 5132 NAICS-mirror
    pair and a noise flag) + 2 zip rows (establishments-only).
  * socal_loan_activity: 8 rows = 2 programs x 2 fiscal years x (county +
    state).
  * socal_harvest_log: 3 rows (ok / cache_hit / failed).

Coverage (see the section banners below for the per-area detail):

  * schema + constraints: the 6 tables exist with PK + expected UNIQUE keys
    (catalog), every documented CHECK constraint exists by name (catalog) AND
    is enforced behaviorally (savepoint-rolled-back inserts must raise
    CheckViolation / UniqueViolation),
  * regional wage lookups: ON CONFLICT upsert idempotency, exact 6-region
    set, 8 role_labels, suppressed row stored as NULLs (never sentinels),
  * mv_socal_rate_guidance: all three rate stages + both delta columns
    recomputed to the penny with Decimal against hand-picked fixture inputs
    for the 3 fully-seeded regions x 6 verticals; explicit zero-burden,
    negative-burden and 65%-utilization cases; NULL-rate row for the
    suppressed wage join; refresh_all_analytics() succeeds,
  * six-vertical margin simulations: pricing_simulator.simulate_job prefilled
    mv-style (base_rate = hourly_cost, price = suggested_billable_rate x
    hours, material_ratio solved so the documented definition holds) returns
    margin_pct == the profile's gross margin and margin_vs_market_pct == the
    region-vs-statewide margin difference, for every (vertical, region) mv
    row with a rate; OT-clamp reporting and price/hours ValueError,
  * harvest_socal_fintech pure functions, OFFLINE (no network, no DB):
    _naics2022_mirror crosswalk cases, num_or_none/_int_or_none suppression
    markers, _bp_row AP x 1000 + payroll-per-employee rule, wage-row assembly
    with '*' suppression -> NULLs and the 2080-consistency checker flagging a
    synthetic bad row, to_monthly sub-monthly averaging with '.' skipping,
  * v2 export pack: export_dataset_pack.main() run against the THROWAWAY DB
    with the output monkeypatched to a tmp dir (the real exports/ tree is
    hash-snapshotted and proven untouched); all 7 CSVs + 3 parquet twins +
    dictionary + manifest exist; MANIFEST sha256/bytes/rows match the files;
    dictionary row-count lines match; parquet twins equal their CSVs; JSONB
    notes serialize as valid JSON; no empty required fields,
  * anomaly/detector non-regression: detect_anomalies() still runs with the
    SoCal tables present; engineered 50% and 100% labor overruns alert
    (medium/high), a break-even job does not; a second run adds nothing
    (idempotent),
  * LIVE warehouse (read-only, skippable): a handful of read-only SELECT
    audits of the real smb_warehouse -- skipped unless the live DB is
    reachable AND DATABASE_URL (if set) names smb_warehouse itself.

Connection: DATABASE_URL names the PARENT database from which the throwaway
socal_fintech_qa_test database is derived (the database name in the URL is
replaced by the scratch name). The suite itself always runs against
socal_fintech_qa_test; the live-warehouse tests open their own READ-ONLY
connection and skip when the live DB is not the configured target.
"""

import csv
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import urllib.parse
from datetime import date
from decimal import Decimal, ROUND_HALF_UP, getcontext

import psycopg2
import psycopg2.errors
import pytest

# The PARENT database (server + maintenance DB) the scratch database is
# derived from; the database component of this URL is replaced by
# SCRATCH_DB_NAME. The suite never opens the parent's data.
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/smb_warehouse",
)

SCRATCH_DB_NAME = "socal_fintech_qa_test"

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if _BASE_DIR not in sys.path:
    sys.path.insert(0, _BASE_DIR)  # project-local imports (see below)

SCHEMA_SQL_PATH = os.path.join(_BASE_DIR, "schema.sql")
ANALYTICS_VIEWS_SQL_PATH = os.path.join(_BASE_DIR, "analytics_views.sql")

PSQL_BIN = shutil.which("psql") or os.path.expanduser("~/bin/psql")

# Live warehouse (read-only audits): only when the configured DB IS the live
# warehouse, so pointing DATABASE_URL at some other DB skips these.
LIVE_DB_NAME = "smb_warehouse"

# Project modules under test (import only; neither touches the network or a
# database at import time).
import harvest_socal_fintech as harvest          # noqa: E402
import export_dataset_pack as exporter           # noqa: E402
import pricing_simulator                          # noqa: E402

# High-precision decimal context: the mv recomputations must round to the
# penny exactly like PostgreSQL's numeric ROUND (half away from zero).
getcontext().prec = 50


# ---------------------------------------------------------------------------
# Fixed vocabulary (must match the CHECK constraints; asserted against the
# live schema in tests, and used to build the fixtures)
# ---------------------------------------------------------------------------

REGION_CODES = ("la_msa", "oxnard_ventura_msa", "orange_county",
                "san_diego_msa", "inland_empire_msa", "california_state")
VERTICAL_KEYS = ("wealth_advisory", "b2b_saas", "custom_software_ai",
                 "fractional_cfo", "fintech_payments", "managed_it_cyber")

EXPECTED_SOCAL_TABLES = (
    "regional_industry_profiles",
    "socal_wage_benchmarks",
    "socal_macro_indicators",
    "socal_business_patterns",
    "socal_loan_activity",
    "socal_harvest_log",
)

EXPECTED_UNIQUE_KEYS = {
    "regional_industry_profiles": ("uq_regional_profiles_vertical_region",
                                   ("vertical_key", "region_code")),
    "socal_wage_benchmarks": ("uq_socal_wage_region_occ_year",
                              ("region_code", "occupation_code", "data_year")),
    "socal_macro_indicators": ("uq_socal_macro_region_series_period",
                               ("region_code", "series_id", "period")),
    "socal_business_patterns": ("uq_socal_bp_geo_naics_year",
                                ("geo_level", "geo_fips", "naics_code",
                                 "data_year")),
    "socal_loan_activity": ("uq_socal_loan_program_year_geo",
                            ("program", "fiscal_year", "geo_level",
                             "geo_fips")),
}

EXPECTED_CHECKS = {
    "regional_industry_profiles": (
        "chk_regional_profiles_vertical_key",
        "chk_regional_profiles_region_code",
        "chk_regional_profiles_payroll_ratio_range",
        "chk_regional_profiles_cac_nonnegative",
        "chk_regional_profiles_utilization_range",
        "chk_regional_profiles_locality_burden_range",
    ),
    "socal_wage_benchmarks": (
        "chk_socal_wage_region_code",
        "chk_socal_wage_hourly_ordered",
        "chk_socal_wage_hourly_positive",
        "chk_socal_wage_annual_ordered",
        "chk_socal_wage_annual_positive",
        "chk_socal_wage_employment_nonnegative",
    ),
    "socal_macro_indicators": (
        "chk_socal_macro_region_code",
        "chk_socal_macro_category",
        "chk_socal_macro_period_is_month_start",
        "chk_socal_macro_value_positive",
    ),
    "socal_business_patterns": (
        "chk_socal_bp_geo_level",
        "chk_socal_bp_region_code",
        "chk_socal_bp_establishments_nonnegative",
        "chk_socal_bp_employment_nonnegative",
    ),
    "socal_loan_activity": (
        "chk_socal_loan_program",
        "chk_socal_loan_geo_level",
        "chk_socal_loan_region_code",
        "chk_socal_loan_loans_nonnegative",
        "chk_socal_loan_gross_nonnegative",
    ),
    "socal_harvest_log": ("chk_socal_harvest_log_status",),
}


# ---------------------------------------------------------------------------
# Fixture data (SMALL, synthetic, hand-picked -- NOT the live harvest)
# ---------------------------------------------------------------------------

WAGE_YEAR = 2024

# region -> occupation_code -> hourly median ($/hr). None = fully suppressed
# at source (mirrors the live oxnard IT Manager row). The other three
# percentiles derive from the median below.
WAGE_HOURLY_MEDIANS = {
    "california_state": {"15-1252": 60.00, "15-2051": 48.00, "13-2051": 45.00,
                         "13-2011": 40.00, "11-3021": 70.00, "43-3031": 25.00,
                         "13-1041": 38.00, "41-9031": 50.00},
    "la_msa":           {"15-1252": 65.00, "15-2051": 52.00, "13-2051": 47.50,
                         "13-2011": 42.00, "11-3021": 75.00, "43-3031": 27.00,
                         "13-1041": 40.00, "41-9031": 54.00},
    "inland_empire_msa": {"15-1252": 55.00, "15-2051": 44.00, "13-2051": 40.00,
                          "13-2011": 36.00, "11-3021": 62.50, "43-3031": 23.00,
                          "13-1041": 35.00, "41-9031": 46.00},
    # IT Manager suppressed in Ventura (documented source suppression)
    "oxnard_ventura_msa": {"15-1252": 58.00, "15-2051": 46.00, "13-2051": 43.00,
                           "13-2011": 39.00, "11-3021": None, "43-3031": 24.00,
                           "13-1041": 37.00, "41-9031": 48.00},
    "orange_county":    {"15-1252": 62.00, "15-2051": 50.00, "13-2051": 46.00,
                         "13-2011": 41.00, "11-3021": 72.00, "43-3031": 26.00,
                         "13-1041": 39.00, "41-9031": 52.00},
    "san_diego_msa":    {"15-1252": 61.00, "15-2051": 49.00, "13-2051": 45.00,
                         "13-2011": 40.00, "11-3021": 71.00, "43-3031": 25.50,
                         "13-1041": 38.50, "41-9031": 51.00},
}

ROLE_TITLES = {
    "15-1252": "Software Developers",
    "15-2051": "Data Scientists",
    "13-2051": "Financial and Investment Analysts",
    "13-2011": "Accountants and Auditors",
    "11-3021": "Computer and Information Systems Managers",
    "43-3031": "Bookkeeping, Accounting, and Auditing Clerks",
    "13-1041": "Compliance Officers",
    "41-9031": "Sales Engineers",
}
ROLE_LABELS = {
    "15-1252": "Software Engineer", "15-2051": "Data Analyst",
    "13-2051": "Financial Analyst", "13-2011": "Accountant/CPA",
    "11-3021": "IT Manager", "43-3031": "Bookkeeper",
    "13-1041": "Compliance Officer", "41-9031": "Sales Engineer",
}


def hourly_percentiles(median):
    """(p25, median, p75, p90) hourly, symmetric band, 2dp exact."""
    m = Decimal(str(median))
    return tuple(float((m * f).quantize(Decimal("0.01")))
                 for f in (Decimal("0.75"), Decimal("1"),
                           Decimal("1.25"), Decimal("1.5")))


def annual_from_hourly(h):
    """OEWS convention for the fixture: annual = hourly x 2080 exactly."""
    return float((Decimal(str(h)) * 2080).quantize(Decimal("0.01")))


# Profiles: 6 verticals x 4 regions. Margins DELIBERATELY VARY BY REGION so
# the market-comparison math (region vs california_state) is non-trivial.
PROFILE_REGIONS = ("california_state", "la_msa", "inland_empire_msa",
                   "oxnard_ventura_msa")
PROFILE_MARGIN_BASE = {"wealth_advisory": 40, "b2b_saas": 75,
                       "custom_software_ai": 50, "fractional_cfo": 60,
                       "fintech_payments": 62, "managed_it_cyber": 45}
PROFILE_REGION_MARGIN_DELTA = {"california_state": 0, "la_msa": 3,
                               "inland_empire_msa": -2, "oxnard_ventura_msa": 1}
PROFILE_UTIL = {"wealth_advisory": 75, "b2b_saas": 65, "custom_software_ai": 80,
                "fractional_cfo": 75, "fintech_payments": 70,
                "managed_it_cyber": 78}
PROFILE_PAYROLL = {"wealth_advisory": 0.35, "b2b_saas": 0.55,
                   "custom_software_ai": 0.60, "fractional_cfo": 0.45,
                   "fintech_payments": 0.50, "managed_it_cyber": 0.58}
PROFILE_CAC = {"wealth_advisory": 3500, "b2b_saas": 12000,
               "custom_software_ai": 8000, "fractional_cfo": 2500,
               "fintech_payments": 15000, "managed_it_cyber": 1800}
PROFILE_ANCHOR = {"wealth_advisory": "13-2051", "b2b_saas": "15-1252",
                  "custom_software_ai": "15-1252", "fractional_cfo": "13-2011",
                  "fintech_payments": "15-1252", "managed_it_cyber": "11-3021"}
PROFILE_VERTICAL_NAME = {
    "wealth_advisory": "QAtest Wealth Advisory",
    "b2b_saas": "QAtest B2B SaaS",
    "custom_software_ai": "QAtest Custom Software/AI",
    "fractional_cfo": "QAtest Fractional CFO",
    "fintech_payments": "QAtest Fintech Payments",
    "managed_it_cyber": "QAtest Managed IT/Cyber",
}
PROFILE_BURDEN = {"california_state": 0.0, "la_msa": 6.0,
                  "inland_empire_msa": -2.0, "oxnard_ventura_msa": 2.0}


def profile_margin(vertical, region):
    return PROFILE_MARGIN_BASE[vertical] + PROFILE_REGION_MARGIN_DELTA[region]


def profile_salary(vertical, region):
    """Fixture median_salary_usd: the anchor's annual median in the region;
    the suppressed Ventura IT Manager falls back to the statewide value (the
    harvester's documented region -> CA -> LA chain, collapsed for the
    fixture)."""
    occ = PROFILE_ANCHOR[vertical]
    med = WAGE_HOURLY_MEDIANS[region][occ]
    if med is None:
        med = WAGE_HOURLY_MEDIANS["california_state"][occ]
        assert med is not None, "fallback must exist in the fixture"
    return annual_from_hourly(med)


MACRO_ROWS = [
    # (region, series_id, series_name, category, period, value, unit)
    ("california_state", "CPIAUCSL",
     "QAtest US CPI-U, All Items (US) -- CA proxy note", "cpi",
     "2026-01-01", "310.5000", "Index 1982-84=100, US CPI-U as CA proxy"),
    ("california_state", "CPIAUCSL",
     "QAtest US CPI-U, All Items (US) -- CA proxy note", "cpi",
     "2026-02-01", "311.2000", "Index 1982-84=100, US CPI-U as CA proxy"),
    ("oxnard_ventura_msa", "CPIAUCSL",
     "QAtest US CPI-U, All Items (US) -- CA proxy note", "cpi",
     "2026-01-01", "310.5000", "Index 1982-84=100, US CPI-U as CA proxy"),
    ("california_state", "FEDFUNDS",
     "QAtest Effective Federal Funds Rate (US)", "interest_rate",
     "2026-01-01", "4.3300", "percent"),
    ("la_msa", "SMU06310805500000001",
     "QAtest Financial activities employment, Los Angeles MSA", "employment",
     "2026-02-01", "671.2000", "thousands of employees"),
    ("san_diego_msa", "CUURS49ESA0",
     "QAtest CPI-U All Items, San Diego (NSA, bimonthly)", "cpi",
     "2026-01-01", "318.9000", "Index 1982-84=100"),
]

BP_ROWS = [
    # (geo_level, geo_fips, geo_name, region_code, naics_code, naics_label,
    #  establishments, employment, employment_flag, annual_payroll_usd,
    #  payroll_per_employee)
    ("county", "06037", "QAtest Los Angeles County", "la_msa", "51",
     "Information", 1200, 2400, None, 3200000.00, 1333.33),
    ("county", "06059", "QAtest Orange County", "orange_county", "5112",
     "Software Publishers (NAICS 2017 as published)", 210, 420, "H",
     950000.00, 2261.90),
    # the documented 2017->2022 publishing-family mirror of the row above
    ("county", "06059", "QAtest Orange County", "orange_county", "5132",
     "Software Publishers", 210, 420, "H", 950000.00, 2261.90),
    ("county", "06073", "QAtest San Diego County", "san_diego_msa", "54",
     "Professional, Scientific, and Technical Services", 5000, None, "J",
     7800000.00, None),
    ("zip", "90001", "QAtest City, CA (QAtest Los Angeles County)", "la_msa",
     "5415", None, 12, None, None, None, None),
    ("zip", "92801", "QAtest City, CA (QAtest Orange County)",
     "orange_county", "5132", "Software Publishers", 8, None, None, None,
     None),
]

LOAN_ROWS = [
    # (program, fiscal_year, geo_level, geo_fips, geo_name, region_code,
    #  loans_count, gross_approval_usd, avg_loan_usd, top_lender,
    #  top_lender_loans)
    ("7a", 2025, "county", "06037", "QAtest Los Angeles County", "la_msa",
     120, 48000000.00, 400000.00, "QAtest Bank", 35),
    ("7a", 2026, "county", "06037", "QAtest Los Angeles County", "la_msa",
     132, 52800000.00, 400000.00, "QAtest Bank", 41),
    ("504", 2025, "county", "06037", "QAtest Los Angeles County", "la_msa",
     24, 18000000.00, 750000.00, "QAtest CDC", 9),
    ("504", 2026, "county", "06037", "QAtest Los Angeles County", "la_msa",
     30, 22500000.00, 750000.00, "QAtest CDC", 11),
    ("7a", 2025, "state", "06", "QAtest California", "california_state",
     3000, 1200000000.00, 400000.00, "QAtest Bank", 500),
    ("7a", 2026, "state", "06", "QAtest California", "california_state",
     3300, 1320000000.00, 400000.00, "QAtest Bank", 540),
    ("504", 2025, "state", "06", "QAtest California", "california_state",
     600, 450000000.00, 750000.00, "QAtest CDC", 150),
    ("504", 2026, "state", "06", "QAtest California", "california_state",
     660, 495000000.00, 750000.00, "QAtest CDC", 160),
]

LOG_ROWS = [
    # (run_id, source_name, url, status, http_status, rows_loaded, message)
    ("qatest-run-1", "oews",
     "https://example.test/QAtest/oesm24ma.zip", "ok", 200, 48,
     "QAtest synthetic harvest log row"),
    ("qatest-run-1", "fred",
     "https://example.test/QAtest/fredgraph.csv?id=CPIAUCSL", "cache_hit",
     None, 48, "QAtest synthetic cache hit"),
    ("qatest-run-1", "sba",
     "https://example.test/QAtest/FOIA_QAtest.csv", "failed", None, 0,
     "QAtest synthetic failure (documented degradation)"),
]

HARVEST_SOURCE_NOTE = ("QAtest synthetic fixture row, retrieved 2026-01-01 "
                       "from https://example.test/QAtest/source.zip")


# ---------------------------------------------------------------------------
# Scratch database lifecycle (self-contained suite; house conventions)
# ---------------------------------------------------------------------------

def scratch_database_url():
    """DATABASE_URL with the database component replaced by SCRATCH_DB_NAME."""
    parts = urllib.parse.urlsplit(DATABASE_URL)
    return urllib.parse.urlunsplit(
        (parts.scheme, parts.netloc, "/" + SCRATCH_DB_NAME, parts.query,
         parts.fragment)
    )


def _parent_connect():
    """Autocommit connection to the parent database (CREATE/DROP DATABASE
    cannot run inside a transaction block)."""
    conn_ = psycopg2.connect(DATABASE_URL)
    conn_.autocommit = True
    return conn_


def drop_scratch_database():
    conn_ = _parent_connect()
    try:
        with conn_.cursor() as cur:
            cur.execute(
                f"DROP DATABASE IF EXISTS {SCRATCH_DB_NAME} WITH (FORCE)"
            )
    finally:
        conn_.close()


def create_scratch_database():
    conn_ = _parent_connect()
    try:
        with conn_.cursor() as cur:
            cur.execute(f"CREATE DATABASE {SCRATCH_DB_NAME}")
    finally:
        conn_.close()


def apply_sql_file(scratch_url, path):
    """Apply one .sql file via subprocess psql; non-zero exit fails the
    session with psql's captured output."""
    if not os.path.isfile(path):
        pytest.fail(f"SQL file not found: {path}", pytrace=False)
    proc = subprocess.run(
        [PSQL_BIN, scratch_url, "-X", "-v", "ON_ERROR_STOP=1", "-f", path],
        capture_output=True, text=True, timeout=180,
    )
    if proc.returncode != 0:
        pytest.fail(
            f"psql failed applying {os.path.basename(path)} to"
            f" {SCRATCH_DB_NAME} (exit code {proc.returncode}):\n"
            f"--- psql stdout ---\n{proc.stdout}\n"
            f"--- psql stderr ---\n{proc.stderr}",
            pytrace=False,
        )


@pytest.fixture(scope="module")
def conn():
    """Module-scoped connection to the THROWAWAY socal_fintech_qa_test
    database (schema.sql + analytics_views.sql applied). Teardown drops the
    database even on failure."""
    scratch_url = scratch_database_url()
    _conn = None
    try:
        drop_scratch_database()
        create_scratch_database()
        apply_sql_file(scratch_url, SCHEMA_SQL_PATH)
        apply_sql_file(scratch_url, ANALYTICS_VIEWS_SQL_PATH)
        _conn = psycopg2.connect(scratch_url)
        yield _conn
    finally:
        if _conn is not None:
            try:
                _conn.close()
            except Exception:  # pragma: no cover - connection already broken
                pass
        try:
            drop_scratch_database()
        except Exception as exc:  # pragma: no cover - backstop only
            print(f"WARNING: could not drop scratch database"
                  f" {SCRATCH_DB_NAME}: {exc!r}")


def _seed_wages(cur):
    for region, meds in WAGE_HOURLY_MEDIANS.items():
        for occ, med in meds.items():
            if med is None:
                hp = ha = (None,) * 4
                emp = rse = None
            else:
                hp = hourly_percentiles(med)
                ha = tuple(annual_from_hourly(h) for h in hp)
                emp, rse = 1200 + int(occ.replace("-", "")) % 900, 3.1
            cur.execute(
                """INSERT INTO socal_wage_benchmarks
                     (region_code, occupation_code, occupation_title,
                      role_label, data_year,
                      hourly_p25, hourly_median, hourly_p75, hourly_p90,
                      annual_p25, annual_median, annual_p75, annual_p90,
                      employment, employment_rse, source)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (region, occ, ROLE_TITLES[occ], ROLE_LABELS[occ], WAGE_YEAR,
                 *hp, *ha, emp, rse, HARVEST_SOURCE_NOTE),
            )


def _seed_profiles(cur):
    for vert in VERTICAL_KEYS:
        for region in PROFILE_REGIONS:
            margin = profile_margin(vert, region)
            notes = {
                "modeling_basis":
                    "QAtest synthetic modeled constants -- NOT scraped data",
                "gross_margin_pct": {"value": margin, "p25": margin - 6,
                                     "p75": margin + 6,
                                     "basis": "QAtest synthetic band"},
                "median_salary_usd": {
                    "value": profile_salary(vert, region),
                    "source": "QAtest synthetic wage fixture"
                              + (" -- regional fallback"
                                 if WAGE_HOURLY_MEDIANS[region][
                                     PROFILE_ANCHOR[vert]] is None else "")},
                "rate_formula": "QAtest: mv_socal_rate_guidance formula",
            }
            cur.execute(
                """INSERT INTO regional_industry_profiles
                     (vertical_key, vertical_name, region_code,
                      anchor_occupation_code, gross_margin_pct,
                      gross_margin_p25_pct, gross_margin_p75_pct,
                      payroll_to_revenue_ratio, cac_usd,
                      utilization_target_pct, locality_burden_pct,
                      median_salary_usd, notes, source)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (vert, PROFILE_VERTICAL_NAME[vert], region,
                 PROFILE_ANCHOR[vert], margin, margin - 6, margin + 6,
                 PROFILE_PAYROLL[vert], PROFILE_CAC[vert],
                 PROFILE_UTIL[vert], PROFILE_BURDEN[region],
                 profile_salary(vert, region), json.dumps(notes),
                 "QAtest synthetic modeled profile, retrieved 2026-01-01 "
                 "from https://example.test/QAtest/model.json"),
            )


def _seed_misc(cur):
    for row in MACRO_ROWS:
        region, sid, name, cat, period, value, unit = row
        cur.execute(
            """INSERT INTO socal_macro_indicators
                 (region_code, series_id, series_name, category, period,
                  value, unit, source)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (region, sid, name, cat, period, value, unit,
             HARVEST_SOURCE_NOTE),
        )
    for row in BP_ROWS:
        (geo_level, geo_fips, geo_name, region_code, naics, label, est, emp,
         flag, payroll, ppe) = row
        cur.execute(
            """INSERT INTO socal_business_patterns
                 (geo_level, geo_fips, geo_name, region_code, naics_code,
                  naics_label, data_year, establishments, employment,
                  employment_flag, annual_payroll_usd,
                  payroll_per_employee, source)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (geo_level, geo_fips, geo_name, region_code, naics, label, 2023,
             est, emp, flag, payroll, ppe, HARVEST_SOURCE_NOTE),
        )
    for row in LOAN_ROWS:
        (program, fy, geo_level, geo_fips, geo_name, region_code, loans,
         gross, avg, lender, lender_loans) = row
        cur.execute(
            """INSERT INTO socal_loan_activity
                 (program, fiscal_year, geo_level, geo_fips, geo_name,
                  region_code, loans_count, gross_approval_usd, avg_loan_usd,
                  top_lender, top_lender_loans, source)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (program, fy, geo_level, geo_fips, geo_name, region_code, loans,
             gross, avg, lender, lender_loans, HARVEST_SOURCE_NOTE),
        )
    for row in LOG_ROWS:
        cur.execute(
            """INSERT INTO socal_harvest_log
                 (run_id, source_name, url, status, http_status, rows_loaded,
                  message)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""", row)


@pytest.fixture(scope="module", autouse=True)
def seeded(conn):
    """Seed the small synthetic SoCal fixtures once, then materialize the
    analytics views over them (autouse: every test sees the same state;
    mutations inside tests are savepoint-rolled-back or QAtest-tagged)."""
    with conn.cursor() as cur:
        _seed_wages(cur)
        _seed_profiles(cur)
        _seed_misc(cur)
        cur.execute("REFRESH MATERIALIZED VIEW mv_socal_rate_guidance")
    conn.commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch_all(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def fetch_one(conn, sql, params=None):
    return fetch_all(conn, sql, params)[0]


def expect_rejected(conn, statements, expected_exc):
    """Run `statements` inside a savepoint. Every statement except the LAST
    must succeed; the last must raise `expected_exc`. Rolled back afterwards
    so neither a rejected insert nor a preceding control insert persists."""
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT qa_reject_sp")
        try:
            for stmt in statements[:-1]:
                cur.execute(stmt)
            with pytest.raises(expected_exc):
                cur.execute(statements[-1])
        finally:
            cur.execute("ROLLBACK TO SAVEPOINT qa_reject_sp")
            cur.execute("RELEASE SAVEPOINT qa_reject_sp")


def expect_accepted(conn, statements):
    """Positive control: run `statements` inside a savepoint; all must
    succeed. Rolled back afterwards so nothing persists."""
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT qa_accept_sp")
        try:
            for stmt in statements:
                cur.execute(stmt)
        finally:
            cur.execute("ROLLBACK TO SAVEPOINT qa_accept_sp")
            cur.execute("RELEASE SAVEPOINT qa_accept_sp")


def r2(x):
    """Postgres numeric ROUND(x, 2): nearest cent, ties away from zero."""
    return Decimal(x).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def expected_rate_stages(salary, burden_pct, utilization_pct, margin_pct):
    """The documented mv formula, computed independently with Decimal from
    UNROUNDED upstream values (only the output columns are rounded, exactly
    as the view definition does):
        hourly_cost             = salary/2080 * (1 + burden/100)
        loaded_cost             = hourly_raw / (utilization/100)
        suggested_billable_rate = hourly_raw / (utilization/100)
                                  / (1 - margin/100)
    """
    h_raw = (Decimal(str(salary)) / Decimal(2080)
             * (1 + Decimal(str(burden_pct)) / 100))
    loaded_raw = h_raw / (Decimal(str(utilization_pct)) / 100)
    suggested_raw = loaded_raw / (1 - Decimal(str(margin_pct)) / 100)
    return r2(h_raw), r2(loaded_raw), r2(suggested_raw)


def expected_delta(rate, reference_rate):
    """(rate / reference - 1) * 100, rounded to 2dp; None when the reference
    rate is NULL/0 (mirrors the view's NULLIF)."""
    if reference_rate is None or Decimal(reference_rate) == 0:
        return None
    return r2((Decimal(rate) / Decimal(reference_rate) - 1) * 100)


def sim_material_ratio(vertical, region):
    """Materials fraction of the quote that makes simulate_job return exactly
    the vertical's REGIONAL target margin at the mv's economics: with
    ot_pct=0 and material_change=0, margin_frac = 1 - labor/price - mr -
    other_ratio, and labor/price = hourly/suggested = (util/100)
    (1 - margin/100) by the rate formula. Solving: mr = 1 - other -
    labor_frac - margin_frac (the REGIONAL margin, not the statewide one)."""
    m = Decimal(profile_margin(vertical, region)) / 100
    util = Decimal(PROFILE_UTIL[vertical]) / 100
    other = Decimal("0.05")                       # simulate_job default
    return float(1 - other - util * (1 - m) - m)


def dict_rows(conn, sql, params=None):
    """fetch_all with column names attached (list of dicts)."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# 1) Schema + constraints
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table", EXPECTED_SOCAL_TABLES)
def test_socal_tables_exist_with_primary_key(conn, table):
    (n_tables,) = fetch_one(
        conn,
        """SELECT count(*) FROM information_schema.tables
            WHERE table_schema='public' AND table_type='BASE TABLE'
              AND table_name=%s""", (table,))
    assert n_tables == 1, f"{table}: table missing"
    (pk,) = fetch_one(
        conn,
        """SELECT count(*) FROM pg_constraint con
             JOIN pg_class c ON c.oid = con.conrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname='public' AND c.relname=%s AND con.contype='p'""",
        (table,))
    assert pk == 1, f"{table}: expected exactly 1 PK (identity id)"


def test_socal_unique_keys_in_catalog(conn):
    for table, (name, cols) in EXPECTED_UNIQUE_KEYS.items():
        row = fetch_one(
            conn,
            """SELECT con.conname, pg_get_constraintdef(con.oid)
                 FROM pg_constraint con
                 JOIN pg_class c ON c.oid = con.conrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname='public' AND c.relname=%s
                  AND con.contype='u' AND con.conname=%s""",
            (table, name),
        )
        assert row, f"{table}: UNIQUE constraint {name} missing"
        for col in cols:
            assert col in row[1], \
                f"{table}.{name}: column {col} not in {row[1]}"


@pytest.mark.parametrize("table", sorted(EXPECTED_CHECKS))
def test_socal_check_constraints_in_catalog(conn, table):
    rows = fetch_all(
        conn,
        """SELECT con.conname FROM pg_constraint con
             JOIN pg_class c ON c.oid = con.conrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname='public' AND c.relname=%s AND con.contype='c'
              AND con.conname LIKE 'chk%%'""",
        (table,),
    )
    present = {r[0] for r in rows}
    missing = set(EXPECTED_CHECKS[table]) - present
    assert not missing, f"{table}: missing CHECK constraints: {sorted(missing)}"


REGION_CHECK_TABLES = {
    "regional_industry_profiles": "region_code",
    "socal_wage_benchmarks": "region_code",
    "socal_macro_indicators": "region_code",
}


@pytest.mark.parametrize("table,col", sorted(REGION_CHECK_TABLES.items()))
def test_region_code_check_rejects_unknown_value(conn, table, col):
    base = {
        "regional_industry_profiles":
            "INSERT INTO regional_industry_profiles (vertical_key,"
            " vertical_name, region_code, anchor_occupation_code,"
            " gross_margin_pct, payroll_to_revenue_ratio, cac_usd,"
            " utilization_target_pct, locality_burden_pct, source)"
            " VALUES ('wealth_advisory','QAtest bad','mars_msa','13-2051',"
            " 40, 0.35, 100, 75, 0, 'QAtest')",
        "socal_wage_benchmarks":
            "INSERT INTO socal_wage_benchmarks (region_code,"
            " occupation_code, role_label, data_year, source)"
            " VALUES ('mars_msa','15-1252','QAtest',2024,'QAtest')",
        "socal_macro_indicators":
            "INSERT INTO socal_macro_indicators (region_code, series_id,"
            " series_name, category, period, value, unit, source)"
            " VALUES ('mars_msa','QAtestS1','QAtest','cpi',"
            " DATE '2026-01-01', 100, 'percent','QAtest')",
    }[table]
    expect_rejected(conn, [base], psycopg2.errors.CheckViolation)


def test_profile_vertical_key_and_ranges_rejected(conn):
    expect_rejected(conn, [
        # unknown vertical_key
        "INSERT INTO regional_industry_profiles (vertical_key, vertical_name,"
        " region_code, anchor_occupation_code, gross_margin_pct,"
        " payroll_to_revenue_ratio, cac_usd, utilization_target_pct,"
        " locality_burden_pct, source)"
        " VALUES ('dropshipping','QAtest bad','la_msa','13-2051',40,0.35,100,"
        " 75, 0, 'QAtest')",
    ], psycopg2.errors.CheckViolation)
    for bad_sql in (
        # payroll ratio must be strictly inside (0, 1)
        "UPDATE regional_industry_profiles SET payroll_to_revenue_ratio = 0"
        " WHERE vertical_key='wealth_advisory' AND region_code='california_state'",
        "UPDATE regional_industry_profiles SET payroll_to_revenue_ratio = 1"
        " WHERE vertical_key='wealth_advisory' AND region_code='california_state'",
        # CAC non-negative
        "UPDATE regional_industry_profiles SET cac_usd = -1"
        " WHERE vertical_key='wealth_advisory' AND region_code='california_state'",
        # utilization inside [1, 100]
        "UPDATE regional_industry_profiles SET utilization_target_pct = 0"
        " WHERE vertical_key='wealth_advisory' AND region_code='california_state'",
        "UPDATE regional_industry_profiles SET utilization_target_pct = 100.5"
        " WHERE vertical_key='wealth_advisory' AND region_code='california_state'",
        # locality burden inside [-20, 40]
        "UPDATE regional_industry_profiles SET locality_burden_pct = -20.01"
        " WHERE vertical_key='wealth_advisory' AND region_code='california_state'",
        "UPDATE regional_industry_profiles SET locality_burden_pct = 40.01"
        " WHERE vertical_key='wealth_advisory' AND region_code='california_state'",
    ):
        expect_rejected(conn, [bad_sql], psycopg2.errors.CheckViolation)


def test_wage_percentile_ordering_and_positivity_rejected(conn):
    key = "region_code='california_state' AND occupation_code='15-1252' AND data_year=2024"
    for bad_sql in (
        # p25 above the median (hourly + annual variants)
        f"UPDATE socal_wage_benchmarks SET hourly_p25 = hourly_median + 1 WHERE {key}",
        f"UPDATE socal_wage_benchmarks SET annual_p25 = annual_median + 100 WHERE {key}",
        # median above p75 / p75 above p90
        f"UPDATE socal_wage_benchmarks SET hourly_p75 = hourly_median - 1 WHERE {key}",
        f"UPDATE socal_wage_benchmarks SET annual_p90 = annual_p75 - 100 WHERE {key}",
        # non-positive wage
        f"UPDATE socal_wage_benchmarks SET hourly_median = 0 WHERE {key}",
        f"UPDATE socal_wage_benchmarks SET annual_median = -500 WHERE {key}",
        # negative employment
        f"UPDATE socal_wage_benchmarks SET employment = -1 WHERE {key}",
    ):
        expect_rejected(conn, [bad_sql], psycopg2.errors.CheckViolation)


def test_macro_category_and_value_checks_rejected(conn):
    key = "region_code='california_state' AND series_id='FEDFUNDS' AND period=DATE '2026-01-01'"
    expect_rejected(
        conn,
        [f"UPDATE socal_macro_indicators SET category='weather' WHERE {key}"],
        psycopg2.errors.CheckViolation)
    expect_rejected(
        conn,
        [f"UPDATE socal_macro_indicators SET value=0 WHERE {key}"],
        psycopg2.errors.CheckViolation)


def test_macro_period_day1_check_enforced(conn):
    expect_rejected(conn, [
        "INSERT INTO socal_macro_indicators (region_code, series_id,"
        " series_name, category, period, value, unit, source)"
        " VALUES ('california_state','QAtestMID','QAtest mid-month','cpi',"
        " DATE '2026-01-15', 100, 'percent','QAtest')",
    ], psycopg2.errors.CheckViolation)
    # positive control: a day-1 period is accepted (savepoint-rolled-back)
    expect_accepted(conn, [
        "INSERT INTO socal_macro_indicators (region_code, series_id,"
        " series_name, category, period, value, unit, source)"
        " VALUES ('california_state','QAtestOK','QAtest month start','cpi',"
        " DATE '2026-03-01', 100, 'percent','QAtest')",
    ])


def test_macro_unique_key_blocks_duplicate(conn):
    expect_rejected(conn, [
        # (region, series, period) already seeded -> duplicate insert rejected
        "INSERT INTO socal_macro_indicators (region_code, series_id,"
        " series_name, category, period, value, unit, source)"
        " VALUES ('california_state','FEDFUNDS','QAtest dup','interest_rate',"
        " DATE '2026-01-01', 4.5, 'percent','QAtest')",
    ], psycopg2.errors.UniqueViolation)
    # positive control: the SAME series under ANOTHER region is allowed (the
    # documented proxy pattern is why the key includes region_code)
    expect_accepted(conn, [
        "INSERT INTO socal_macro_indicators (region_code, series_id,"
        " series_name, category, period, value, unit, source)"
        " VALUES ('orange_county','FEDFUNDS','QAtest proxy ok','interest_rate',"
        " DATE '2026-01-01', 4.33, 'percent','QAtest')",
    ])


def test_business_patterns_unique_and_checks(conn):
    expect_rejected(conn, [
        # duplicate (geo_level, geo_fips, naics_code, data_year)
        "INSERT INTO socal_business_patterns (geo_level, geo_fips, geo_name,"
        " region_code, naics_code, naics_label, data_year, establishments,"
        " source)"
        " VALUES ('county','06059','QAtest dup','orange_county','5132',NULL,"
        " 2023, 1, 'QAtest')",
    ], psycopg2.errors.UniqueViolation)
    expect_rejected(conn, [
        "INSERT INTO socal_business_patterns (geo_level, geo_fips, geo_name,"
        " region_code, naics_code, naics_label, data_year, establishments,"
        " source)"
        " VALUES ('state','06','QAtest bad level','orange_county','55',"
        " NULL, 2023, 1, 'QAtest')",
    ], psycopg2.errors.CheckViolation)
    expect_rejected(conn, [
        "UPDATE socal_business_patterns SET establishments = -1"
        " WHERE geo_level='county' AND geo_fips='06037' AND naics_code='51'",
    ], psycopg2.errors.CheckViolation)
    expect_rejected(conn, [
        "UPDATE socal_business_patterns SET employment = -5"
        " WHERE geo_level='county' AND geo_fips='06037' AND naics_code='51'",
    ], psycopg2.errors.CheckViolation)
    # NULL region_code IS allowed (documented unmapped-zip degradation) but
    # 'mars_msa' is not
    expect_rejected(conn, [
        "INSERT INTO socal_business_patterns (geo_level, geo_fips, geo_name,"
        " region_code, naics_code, naics_label, data_year, establishments,"
        " source)"
        " VALUES ('zip','11111','QAtest unmapped','mars_msa','51',NULL,2023,"
        " 1, 'QAtest')",
    ], psycopg2.errors.CheckViolation)


def test_loan_program_geo_and_unique_checks(conn):
    expect_rejected(conn, [
        "INSERT INTO socal_loan_activity (program, fiscal_year, geo_level,"
        " geo_fips, loans_count, gross_approval_usd, source)"
        " VALUES ('paycheck', 2026, 'county', '06037', 1, 1000, 'QAtest')",
    ], psycopg2.errors.CheckViolation)
    expect_rejected(conn, [
        "INSERT INTO socal_loan_activity (program, fiscal_year, geo_level,"
        " geo_fips, loans_count, gross_approval_usd, source)"
        " VALUES ('7a', 2026, 'zip', '90001', 1, 1000, 'QAtest')",
    ], psycopg2.errors.CheckViolation)
    expect_rejected(conn, [
        # duplicate (program, fiscal_year, geo_level, geo_fips)
        "INSERT INTO socal_loan_activity (program, fiscal_year, geo_level,"
        " geo_fips, loans_count, gross_approval_usd, source)"
        " VALUES ('7a', 2025, 'county', '06037', 1, 1000, 'QAtest dup')",
    ], psycopg2.errors.UniqueViolation)
    expect_rejected(conn, [
        "UPDATE socal_loan_activity SET loans_count = -1"
        " WHERE program='7a' AND fiscal_year=2025 AND geo_level='county'",
    ], psycopg2.errors.CheckViolation)


def test_harvest_log_status_check(conn):
    expect_rejected(conn, [
        "INSERT INTO socal_harvest_log (run_id, source_name, url, status,"
        " rows_loaded) VALUES ('qatest-run-2','oews','https://example.test/x',"
        " 'meh', 0)",
    ], psycopg2.errors.CheckViolation)


def test_profile_notes_jsonb_roundtrips(conn):
    (notes,) = fetch_one(
        conn,
        """SELECT notes FROM regional_industry_profiles
            WHERE vertical_key='b2b_saas' AND region_code='california_state'""")
    assert isinstance(notes, dict), "JSONB must parse back as a dict"
    assert notes["gross_margin_pct"]["value"] == 75
    assert "QAtest" in notes["modeling_basis"]


# ---------------------------------------------------------------------------
# 2) Regional wage lookups
# ---------------------------------------------------------------------------

def test_wage_upsert_idempotency_on_region_occ_year(conn):
    """Re-running the exact same batch through the harvester's ON CONFLICT
    upsert never duplicates (the harvester's idempotency mechanism)."""
    batch = [
        ("california_state", f"99-{i:04d}", "QAtest Synthetic Role",
         f"QAtest Role {i}", 2024, 10.0 + i, 20.0 + i, 30.0 + i, 40.0 + i,
         (10.0 + i) * 2080, (20.0 + i) * 2080, (30.0 + i) * 2080,
         (40.0 + i) * 2080, 100 + i, 2.5,
         "QAtest synthetic batch, retrieved 2026-01-01 from "
         "https://example.test/QAtest.zip")
        for i in range(8)
    ]
    upsert = """
        INSERT INTO socal_wage_benchmarks (region_code, occupation_code,
            occupation_title, role_label, data_year,
            hourly_p25, hourly_median, hourly_p75, hourly_p90,
            annual_p25, annual_median, annual_p75, annual_p90,
            employment, employment_rse, source)
        VALUES %s
        ON CONFLICT (region_code, occupation_code, data_year) DO UPDATE SET
            hourly_median = EXCLUDED.hourly_median,
            annual_median = EXCLUDED.annual_median,
            source = EXCLUDED.source
    """
    from psycopg2.extras import execute_values
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT qa_upsert_sp")
        try:
            for _ in range(3):                      # run the SAME batch 3x
                execute_values(cur, upsert, batch,
                               template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                                        "%s,%s,%s,%s,%s)")
            cur.execute(
                "SELECT count(*) FROM socal_wage_benchmarks"
                " WHERE occupation_code LIKE '99-%'")
            assert cur.fetchone()[0] == 8, "upsert must never duplicate"
            cur.execute(
                "SELECT annual_median FROM socal_wage_benchmarks"
                " WHERE occupation_code='99-0000'")
            assert cur.fetchone()[0] == Decimal(str(20.0 * 2080))
        finally:
            cur.execute("ROLLBACK TO SAVEPOINT qa_upsert_sp")
            cur.execute("RELEASE SAVEPOINT qa_upsert_sp")


def test_wage_region_filter_returns_exactly_six_regions(conn):
    rows = fetch_all(
        conn, "SELECT DISTINCT region_code FROM socal_wage_benchmarks")
    assert sorted(r[0] for r in rows) == sorted(REGION_CODES)
    # per-region row counts: all regions have the full 8-role set
    rows = fetch_all(
        conn,
        """SELECT region_code, count(*), count(DISTINCT occupation_code)
             FROM socal_wage_benchmarks GROUP BY region_code ORDER BY 1""")
    for region, n, n_occ in rows:
        assert n == 8 and n_occ == 8, \
            f"{region}: expected 8 rows / 8 occupations, got {n}/{n_occ}"


def test_wage_role_label_coverage_is_eight(conn):
    labels = {r[0] for r in fetch_all(
        conn, "SELECT DISTINCT role_label FROM socal_wage_benchmarks")}
    assert len(labels) == 8
    assert labels == set(ROLE_LABELS.values())


def test_wage_suppressed_row_stored_as_nulls_not_sentinels(conn):
    row = fetch_one(
        conn,
        """SELECT hourly_p25, hourly_median, hourly_p75, hourly_p90,
                  annual_p25, annual_median, annual_p75, annual_p90,
                  employment, employment_rse
             FROM socal_wage_benchmarks
            WHERE region_code='oxnard_ventura_msa'
              AND occupation_code='11-3021'""")
    assert all(v is None for v in row), \
        "suppressed source row must load as NULLs, never sentinels"
    # ...while a normal row keeps all percentiles positive and ordered
    row = fetch_one(
        conn,
        """SELECT hourly_p25, hourly_median, hourly_p75, hourly_p90
             FROM socal_wage_benchmarks
            WHERE region_code='california_state'
              AND occupation_code='15-1252'""")
    assert row[0] < row[1] < row[2] < row[3] and all(v > 0 for v in row)


# ---------------------------------------------------------------------------
# 3) mv_socal_rate_guidance math
# ---------------------------------------------------------------------------

MV_RATE_ROWS = [(region, vert)
                for region in ("california_state", "la_msa",
                               "inland_empire_msa")
                for vert in VERTICAL_KEYS]


@pytest.mark.parametrize("region,vertical", MV_RATE_ROWS,
                         ids=[f"{r}-{v}" for r, v in MV_RATE_ROWS])
def test_mv_rate_formula_recomputed_to_the_penny(conn, region, vertical):
    """hourly_cost / loaded_cost / suggested_billable_rate recomputed with
    Decimal from the hand-picked fixture inputs (an INDEPENDENT recompute:
    the expected values never read the view)."""
    salary = annual_from_hourly(
        WAGE_HOURLY_MEDIANS[region][PROFILE_ANCHOR[vertical]])
    burden = PROFILE_BURDEN[region]
    util = PROFILE_UTIL[vertical]
    margin = profile_margin(vertical, region)
    exp_hourly, exp_loaded, exp_suggested = expected_rate_stages(
        salary, burden, util, margin)

    row = fetch_one(
        conn,
        """SELECT hourly_cost, loaded_cost, suggested_billable_rate,
                  annual_median_salary, data_year
             FROM mv_socal_rate_guidance
            WHERE region_code=%s AND vertical_key=%s""",
        (region, vertical))
    assert row is not None, "mv row missing"
    assert Decimal(row[0]) == exp_hourly, \
        f"hourly_cost {row[0]} != {exp_hourly}"
    assert Decimal(row[1]) == exp_loaded, \
        f"loaded_cost {row[1]} != {exp_loaded}"
    assert Decimal(row[2]) == exp_suggested, \
        f"suggested_billable_rate {row[2]} != {exp_suggested}"
    assert Decimal(row[3]) == Decimal(str(salary))
    assert row[4] == WAGE_YEAR


def test_mv_zero_burden_statewide_row_hand_computed(conn):
    """california_state has locality_burden 0.0: the formula collapses to
    salary/2080 -> /0.65 -> /0.25 for b2b_saas (salary 124800 = 60 x 2080)."""
    row = fetch_one(
        conn,
        """SELECT hourly_cost, loaded_cost, suggested_billable_rate,
                  rate_vs_statewide_pct
             FROM mv_socal_rate_guidance
            WHERE region_code='california_state' AND vertical_key='b2b_saas'""")
    assert Decimal(row[0]) == Decimal("60.00")       # 124800/2080
    assert Decimal(row[1]) == Decimal("92.31")       # 60/0.65 = 92.307... -> 92.31
    assert Decimal(row[2]) == Decimal("369.23")      # 60/0.65/0.25 -> 369.23
    assert Decimal(row[3]) == Decimal("0.00")        # statewide vs itself


def test_mv_negative_burden_inland_row_hand_computed(conn):
    """inland_empire_msa is the required NEGATIVE burden case (-2.0):
    managed_it_cyber (salary 130000 = 62.50 x 2080, util 78, margin 43)."""
    row = fetch_one(
        conn,
        """SELECT hourly_cost, loaded_cost, suggested_billable_rate
             FROM mv_socal_rate_guidance
            WHERE region_code='inland_empire_msa'
              AND vertical_key='managed_it_cyber'""")
    assert Decimal(row[0]) == Decimal("61.25")       # 62.50 x 0.98
    assert Decimal(row[1]) == Decimal("78.53")       # 61.25/0.78 -> 78.53
    assert Decimal(row[2]) == Decimal("137.76")      # 61.25/0.78/0.57 -> 137.76


def test_mv_utilization_65_pct_case(conn):
    """b2b_saas is the required 65%-utilization case: statewide loaded_cost
    = 60/0.65 and LA loaded_cost = 68.90/0.65 = 106.00 exactly."""
    rows = dict_rows(
        conn,
        """SELECT region_code, hourly_cost, loaded_cost
             FROM mv_socal_rate_guidance
            WHERE vertical_key='b2b_saas'
              AND region_code IN ('california_state','la_msa')""")
    by_region = {r["region_code"]: r for r in rows}
    assert Decimal(by_region["california_state"]["loaded_cost"]) == \
        Decimal("92.31")
    assert Decimal(by_region["la_msa"]["hourly_cost"]) == Decimal("68.90")
    assert Decimal(by_region["la_msa"]["loaded_cost"]) == Decimal("106.00")


@pytest.mark.parametrize("region,vertical", MV_RATE_ROWS,
                         ids=[f"{r}-{v}" for r, v in MV_RATE_ROWS])
def test_mv_delta_columns_recomputed(conn, region, vertical):
    """rate_vs_statewide_pct / rate_vs_la_pct = (rate/ref - 1) x 100 off the
    ROUNDED suggested rates, recomputed independently."""
    rates = dict_rows(
        conn,
        """SELECT region_code, suggested_billable_rate,
                  rate_vs_statewide_pct, rate_vs_la_pct
             FROM mv_socal_rate_guidance
            WHERE vertical_key=%s""", (vertical,))
    by_region = {r["region_code"]: r for r in rates}
    mine = by_region[region]
    st = by_region["california_state"]["suggested_billable_rate"]
    la = by_region["la_msa"]["suggested_billable_rate"]
    assert Decimal(mine["rate_vs_statewide_pct"]) == \
        expected_delta(mine["suggested_billable_rate"], st)
    assert Decimal(mine["rate_vs_la_pct"]) == \
        expected_delta(mine["suggested_billable_rate"], la)


def test_mv_null_rate_row_for_suppressed_wage_join(conn):
    """The suppressed Ventura IT Manager wage means oxnard_ventura_msa /
    managed_it_cyber still APPEARS (row survives the join miss) but its rate
    columns are NULL -- documented degradation, never a zero or sentinel."""
    row = fetch_one(
        conn,
        """SELECT hourly_cost, loaded_cost, suggested_billable_rate,
                  rate_vs_statewide_pct, rate_vs_la_pct, data_year
             FROM mv_socal_rate_guidance
            WHERE region_code='oxnard_ventura_msa'
              AND vertical_key='managed_it_cyber'""")
    assert row is not None, "mv row must exist even when the wage join misses"
    # the wage ROW exists (year 2024) but its values are suppressed, so only
    # the rate columns are NULL -- data_year still resolves
    assert all(v is None for v in row[:5])
    assert row[5] == WAGE_YEAR


def test_mv_row_count_and_unique_index(conn):
    (n,) = fetch_one(
        conn, "SELECT count(*) FROM mv_socal_rate_guidance")
    assert n == len(PROFILE_REGIONS) * len(VERTICAL_KEYS) == 24
    (idx,) = fetch_one(
        conn,
        """SELECT count(*) FROM pg_indexes
            WHERE schemaname='public'
              AND indexname='uq_mv_socal_rate_guidance'""")
    assert idx == 1


def test_refresh_all_analytics_includes_socal_view(conn):
    """refresh_all_analytics() must refresh the SoCal view too (its body
    includes mv_socal_rate_guidance and the call must succeed)."""
    (has_view,) = fetch_one(
        conn,
        """SELECT position('mv_socal_rate_guidance' in pg_get_functiondef(
                 'refresh_all_analytics()'::regprocedure)) > 0""")
    assert has_view
    with conn.cursor() as cur:
        cur.execute("REFRESH MATERIALIZED VIEW mv_socal_rate_guidance")
        cur.execute("SELECT count(*) FROM mv_socal_rate_guidance")
        assert cur.fetchone()[0] == 24
        cur.execute("SELECT refresh_all_analytics()")
        cur.execute("SELECT count(*) FROM mv_socal_rate_guidance")
        assert cur.fetchone()[0] == 24
    conn.commit()


# ---------------------------------------------------------------------------
# 4) Six-vertical margin simulations (pricing_simulator.simulate_job)
# ---------------------------------------------------------------------------

SIM_REGIONS = PROFILE_REGIONS                # includes the statewide baseline


def _mv_row(conn, region, vertical):
    row = fetch_one(
        conn,
        """SELECT hourly_cost, suggested_billable_rate, gross_margin_pct,
                  utilization_target_pct
             FROM mv_socal_rate_guidance
            WHERE region_code=%s AND vertical_key=%s""",
        (region, vertical))
    return row


@pytest.mark.parametrize("region,vertical", MV_RATE_ROWS,
                         ids=[f"{r}-{v}" for r, v in MV_RATE_ROWS])
def test_margin_simulation_hits_profile_margin(conn, region, vertical):
    """Prefill simulate_job mv-style: base_rate = hourly_cost, price =
    suggested_billable_rate x hours, target_margin = profile gross_margin,
    market_margin = the california_state profile margin, material_ratio
    solved from the documented definition. The returned margin_pct must
    equal the profile's gross_margin (that IS the definition of the
    suggested rate) and margin_vs_market_pct must equal the region-vs-
    statewide margin difference."""
    hourly, suggested, mv_margin, util = _mv_row(conn, region, vertical)
    target = Decimal(str(profile_margin(vertical, region)))
    market = Decimal(str(profile_margin(vertical, "california_state")))

    hours = 100.0
    result = pricing_simulator.simulate_job(
        hours=hours,
        base_rate=float(hourly),
        ot_pct=0.0,
        material_ratio=sim_material_ratio(vertical, region),
        material_change_pct=0.0,
        price=float(suggested) * hours,
        other_cost_ratio=0.05,
        target_margin=float(target) / 100,
        market_margin=float(market) / 100,
    )
    # the definition of the suggested rate: the sim lands on the target margin
    assert abs(result["margin_pct"] - float(target)) <= 0.1, \
        f"{region}/{vertical}: margin_pct {result['margin_pct']} != {target}"
    # margin_vs_market_pct = margin_pct - market_margin*100 (pp, exact)
    assert result["margin_vs_market_pct"] == pytest.approx(
        result["margin_pct"] - float(market), abs=1e-9)
    # and that difference IS the region-vs-statewide margin difference
    assert abs(result["margin_vs_market_pct"]
               - float(target - market)) <= 0.1
    # the sim's market margin snapshot round-trips
    assert result["market_margin_pct"] == pytest.approx(float(market),
                                                        abs=1e-9)


def test_margin_simulation_statewide_vs_market_is_zero(conn):
    """For california_state itself the market IS the region: the delta is 0."""
    hourly, suggested, mv_margin, util = _mv_row(conn, "california_state",
                                                 "fintech_payments")
    result = pricing_simulator.simulate_job(
        hours=50, base_rate=float(hourly), ot_pct=0.0,
        material_ratio=sim_material_ratio("fintech_payments",
                                          "california_state"),
        material_change_pct=0.0,
        price=float(suggested) * 50, market_margin=0.62)
    assert abs(result["margin_pct"] - 62.0) <= 0.1
    assert abs(result["margin_vs_market_pct"]) <= 0.1


@pytest.mark.parametrize("vertical", VERTICAL_KEYS)
def test_margin_simulation_every_vertical_ot_clamp(conn, vertical):
    """OT beyond the 50% ceiling clamps (and reports), which raises labor
    cost and lowers the realized margin below the target."""
    hourly, suggested, mv_margin, util = _mv_row(conn, "la_msa", vertical)
    mr = sim_material_ratio(vertical, "la_msa")
    price = float(suggested) * 40
    base = pricing_simulator.simulate_job(
        hours=40, base_rate=float(hourly), ot_pct=0.0, material_ratio=mr,
        material_change_pct=0.0, price=price)
    clamped = pricing_simulator.simulate_job(
        hours=40, base_rate=float(hourly), ot_pct=0.80, material_ratio=mr,
        material_change_pct=0.0, price=price)
    assert clamped["clamps_applied"], "ot_pct 0.80 must be reported clamped"
    assert clamped["ot_pct"] == 0.5
    assert clamped["labor_cost"] == pytest.approx(base["labor_cost"] * 1.25)
    assert clamped["margin_pct"] < base["margin_pct"]
    # the unclamped baseline still lands on the profile margin
    assert abs(base["margin_pct"]
               - float(profile_margin(vertical, "la_msa"))) <= 0.1


@pytest.mark.parametrize("vertical", ["b2b_saas"])
def test_margin_simulation_value_error_on_bad_inputs(conn, vertical):
    hourly, suggested, mv_margin, util = _mv_row(conn, "california_state",
                                                 vertical)
    with pytest.raises(ValueError):
        pricing_simulator.simulate_job(
            hours=10, base_rate=float(hourly), ot_pct=0.0,
            material_ratio=sim_material_ratio(vertical, "california_state"),
            material_change_pct=0.0, price=0)
    with pytest.raises(ValueError):
        pricing_simulator.simulate_job(
            hours=10, base_rate=float(hourly), ot_pct=0.0,
            material_ratio=sim_material_ratio(vertical, "california_state"),
            material_change_pct=0.0, price=-100)
    with pytest.raises(ValueError):
        pricing_simulator.simulate_job(
            hours=0, base_rate=float(hourly), ot_pct=0.0,
            material_ratio=sim_material_ratio(vertical, "california_state"),
            material_change_pct=0.0, price=1000)


# ---------------------------------------------------------------------------
# 5) Harvester pure functions (OFFLINE -- no network, no DB)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("code,expected", [
    ("5112", "5132"),
    ("511210", "513210"),
    ("52", None),
    ("5415", None),
    ("511", "513"),          # the whole publishing family mirrors 511->513
    ("5132", None),          # already NAICS 2022: no second mirror
])
def test_naics2022_mirror_cases(code, expected):
    assert harvest._naics2022_mirror(code) == expected


@pytest.mark.parametrize("text,expected", [
    ("$1,234.50", 1234.50),
    (" 2,000 ", 2000.0),
    ("*", None), ("**", None), ("#", None), ("NA", None), ("N", None),
    ("S", None), (".", None), ("(S)", None), ("", None), (None, None),
    ("abc", None),
])
def test_num_or_none_suppression_markers(text, expected):
    assert harvest.num_or_none(text) == expected


@pytest.mark.parametrize("text,expected", [
    ("12", 12), ("-1", -1),        # negatives pass through; callers NULL them
    ("abc", None), ("", None), (None, None),
])
def test_int_or_none_never_sentinels(text, expected):
    """_int_or_none parses or NULLs; the harvest loops turn negatives into
    None themselves (emp < 0 -> None), so sentinels never reach the DB."""
    assert harvest._int_or_none(text) == expected


def test_bp_row_ap_thousands_conversion_and_ppe_rule():
    # AP in $1,000 -> stored x 1000; payroll/employee only when emp > 0
    row = harvest._bp_row("county", "06059", "Orange County", "orange_county",
                          "5112", 210, 420, "H", 950.0, "QAtest desc")
    assert row[10] == 950000.0                 # annual_payroll_usd
    assert row[11] == 2261.90                  # 950000/420
    assert row[5] == harvest.NAICS_LABELS.get("5112")
    assert "retrieved" in row[12] and "zbp23detail" not in row[12] \
        and "cbp23co" in row[12]
    # zero/None employment -> payroll_per_employee NULL even with payroll
    row = harvest._bp_row("county", "06073", "San Diego County",
                          "san_diego_msa", "54", 10, None, "J", 7800.0, "d")
    assert row[11] is None
    row = harvest._bp_row("county", "06073", "San Diego County",
                          "san_diego_msa", "54", 10, 0, None, 7800.0, "d")
    assert row[11] is None
    # no payroll -> both payroll columns NULL
    row = harvest._bp_row("zip", "90001", "LA", "la_msa", "5415", 12,
                          None, None, None, "d")
    assert row[10] is None and row[11] is None


def test_wage_row_assembly_suppressed_cells_become_none():
    frame_row = {
        "OCC_CODE": "11-3021", "OCC_TITLE": "Computer and Information "
        "Systems Managers",
        "TOT_EMP": "*", "EMP_PRSE": "*",
        "H_PCT25": "*", "H_MEDIAN": "*", "H_PCT75": "*", "H_PCT90": "*",
        "A_PCT25": "*", "A_MEDIAN": "*", "A_PCT75": "*", "A_PCT90": "*",
    }
    warnings = []
    row = harvest._wage_row_from(frame_row, "oxnard_ventura_msa",
                                 "QAtest source", warnings)
    assert row[0] == "oxnard_ventura_msa"
    assert row[1] == "11-3021"
    assert row[3] == "IT Manager"                       # role_label map
    assert row[4] == harvest.OEWS_DATA_YEAR
    assert all(v is None for v in row[5:13])            # all wage cells NULL
    assert row[13] is None and row[14] is None          # employment/rse NULL
    assert "retrieved" in row[15] and "http" in row[15]
    assert warnings == []                               # nothing to check


def test_wage_row_assembly_2080_checker_flags_drift():
    # annual median $500,000 vs hourly median $100 x 2080 = $208,000:
    # drift $292,000 >> $120 -> warning collected, values kept AS PUBLISHED
    frame_row = {
        "OCC_CODE": "15-1252", "OCC_TITLE": "Software Developers",
        "TOT_EMP": "100", "EMP_PRSE": "2.1",
        "H_PCT25": "80", "H_MEDIAN": "100", "H_PCT75": "120", "H_PCT90": "150",
        "A_PCT25": "166400", "A_MEDIAN": "500000", "A_PCT75": "249600",
        "A_PCT90": "312000",
    }
    warnings = []
    row = harvest._wage_row_from(frame_row, "la_msa", "QAtest source",
                                 warnings)
    assert len(warnings) == 1
    assert "15-1252" in warnings[0] and "drift" in warnings[0]
    assert row[10] == 500000.0 and row[6] == 100.0      # as published


def test_wage_row_assembly_2080_consistent_row_is_quiet():
    frame_row = {
        "OCC_CODE": "15-1252", "OCC_TITLE": "Software Developers",
        "TOT_EMP": "100", "EMP_PRSE": "2.1",
        "H_PCT25": "45", "H_MEDIAN": "60", "H_PCT75": "75", "H_PCT90": "90",
        "A_PCT25": "93600", "A_MEDIAN": "124800", "A_PCT75": "156000",
        "A_PCT90": "187200",
    }
    warnings = []
    row = harvest._wage_row_from(frame_row, "california_state",
                                 "QAtest source", warnings)
    assert warnings == []
    assert row[10] == 124800.0
    # california_state rows cite the STATE file URL
    assert "oesm24st" in row[15]


def test_to_monthly_averages_submonthly_and_skips_gaps():
    observations = [
        (date(2026, 1, 5), 10.0),      # two January observations -> mean 15
        (date(2026, 1, 20), 20.0),
        (date(2026, 1, 22), None),     # caller pre-filters; see parse test
        (date(2026, 2, 1), 30.0),      # February has one -> 30
    ]
    monthly = harvest.to_monthly([(d, v) for d, v in observations
                                  if v is not None])
    assert set(monthly) == {date(2026, 1, 1), date(2026, 2, 1)}
    assert monthly[date(2026, 1, 1)] == 15.0
    assert monthly[date(2026, 2, 1)] == 30.0
    # every key is a first-of-month date (the schema CHECK's precondition)
    assert all(k.day == 1 for k in monthly)


def test_fred_parse_series_skips_dot_gaps():
    payload = (
        b"observation_date,PRIME\n"
        b"2026-01-01,7.50\n"
        b"2026-01-02,.\n"          # FRED missing-data marker
        b"2026-01-03,7.50\n"
        b"not-a-date,9.9\n"        # junk row skipped
        b"2026-02-01,7.25\n"
    )
    out = harvest._fred_parse_series(payload)
    assert (date(2026, 1, 1), 7.50) in out
    assert (date(2026, 1, 3), 7.50) in out
    assert (date(2026, 2, 1), 7.25) in out
    assert len(out) == 3            # '.' row and junk row dropped


def test_harvester_vocabulary_matches_schema():
    """The harvester's fixed vocabulary must match the schema CHECK lists and
    the 8-role/6-region/6-vertical contract."""
    assert sorted(harvest.REGION_CODES) == sorted(REGION_CODES)
    assert sorted(v["key"] for v in harvest.VERTICALS) == sorted(VERTICAL_KEYS)
    assert len(harvest.ROLES) == 8
    assert set(harvest.ROLE_LABEL_BY_CODE) == set(ROLE_LABELS)
    assert sorted(harvest.COUNTIES) == \
        ["06037", "06059", "06065", "06071", "06073", "06111"]
    assert harvest.COUNTIES["06037"][0] == "la_msa"
    assert harvest.COUNTIES["06059"][0] == "orange_county"
    assert harvest.COUNTIES["06111"][0] == "oxnard_ventura_msa"
    assert harvest.COUNTIES["06073"][0] == "san_diego_msa"
    assert harvest.COUNTIES["06065"][0] == "inland_empire_msa"
    assert harvest.COUNTIES["06071"][0] == "inland_empire_msa"
    assert harvest.REGION_LOCALITY_BURDEN["inland_empire_msa"] == -2.0
    assert harvest.REGION_LOCALITY_BURDEN["california_state"] == 0.0


def test_harvest_src_provenance_format():
    s = harvest.src("QAtest description", "https://example.test/QAtest.zip")
    assert re.fullmatch(
        r"QAtest description, retrieved \d{4}-\d{2}-\d{2} from "
        r"https://example\.test/QAtest\.zip", s)


# ---------------------------------------------------------------------------
# 6) v2 export pack verification (run against the THROWAWAY DB, tmp output)
# ---------------------------------------------------------------------------

EXPECTED_SOCAL_FILES = {
    "socal_wage_benchmarks": True,        # parquet twin?
    "socal_macro_indicators": False,
    "socal_business_patterns": True,
    "socal_loan_activity": True,
    "regional_industry_profiles": False,
    "socal_harvest_log": False,
    "mv_socal_rate_guidance": False,
}

REQUIRED_NON_EMPTY = {
    "socal_wage_benchmarks": ("region_code", "occupation_code", "role_label",
                              "data_year", "source"),
    "socal_macro_indicators": ("region_code", "series_id", "series_name",
                               "category", "period", "value", "unit",
                               "source"),
    "socal_business_patterns": ("geo_level", "geo_fips", "naics_code",
                                "data_year", "establishments", "source"),
    "socal_loan_activity": ("program", "fiscal_year", "geo_level", "geo_fips",
                            "loans_count", "gross_approval_usd", "source"),
    "regional_industry_profiles": ("vertical_key", "vertical_name",
                                   "region_code", "anchor_occupation_code",
                                   "gross_margin_pct",
                                   "payroll_to_revenue_ratio", "cac_usd",
                                   "utilization_target_pct",
                                   "locality_burden_pct", "source"),
    "socal_harvest_log": ("run_id", "source_name", "url", "status",
                          "rows_loaded"),
    "mv_socal_rate_guidance": ("region_code", "vertical_key", "vertical_name"),
}


def _hash_tree(root):
    out = {}
    if os.path.isdir(root):
        for dirpath, _dirs, files in os.walk(root):
            for name in files:
                p = os.path.join(dirpath, name)
                with open(p, "rb") as fh:
                    out[p] = hashlib.sha256(fh.read()).hexdigest()
    return out


@pytest.fixture(scope="module")
def socal_pack(conn, seeded, tmp_path_factory):
    """Run export_dataset_pack.py --pack socal_v2 against the THROWAWAY DB
    with both pack dirs monkeypatched to a tmp dir. Snapshots the REAL
    exports/ tree before and after so a test can prove the live packs were
    untouched. Patching is manual (try/finally) because a module-scoped
    fixture cannot depend on the function-scoped monkeypatch fixture."""
    import export_dataset_pack as edp
    out_root = tmp_path_factory.mktemp("socal_pack_qa")
    pack_dir = out_root / "socal_fintech_benchmark_pack_v2"
    real_exports = os.path.join(_BASE_DIR, "exports")

    before = _hash_tree(real_exports)
    saved = (edp.DB, edp.SOCAL_PACK_DIR, edp.PACK_DIR, sys.argv)
    try:
        edp.DB = scratch_database_url()
        edp.SOCAL_PACK_DIR = pack_dir
        edp.PACK_DIR = out_root / "v1_not_written"
        sys.argv = ["export_dataset_pack.py", "--pack", "socal_v2"]
        edp.main()                       # v1 branch must NOT run
    finally:
        edp.DB, edp.SOCAL_PACK_DIR, edp.PACK_DIR, sys.argv = saved
    after = _hash_tree(real_exports)
    return pack_dir, before, after


def test_v1_and_v2_live_packs_unaffected_by_socal_v2_run(socal_pack):
    """Running the exporter with --pack socal_v2 (and the QA DB) must not
    rewrite a byte of the real exports/ tree -- v1 stays untouched."""
    _pack_dir, before, after = socal_pack
    assert before, "exports/ snapshot missing -- run in the project dir"
    assert before == after


def test_pack_artifact_set_complete(socal_pack):
    pack_dir, _b, _a = socal_pack
    expected = {"DATA_DICTIONARY.md", "MANIFEST.json"}
    for stem, parquet in EXPECTED_SOCAL_FILES.items():
        expected.add(f"{stem}.csv")
        if parquet:
            expected.add(f"{stem}.parquet")
    on_disk = {p.name for p in pack_dir.iterdir() if p.is_file()}
    assert on_disk == expected
    assert len(on_disk) == 12            # 7 CSVs + 3 parquet + dict + manifest


def test_manifest_sha256_bytes_and_rows_match_disk(socal_pack):
    pack_dir, _b, _a = socal_pack
    manifest = json.loads((pack_dir / "MANIFEST.json").read_text())
    assert manifest["pack"] == "socal_fintech_benchmark_pack_v2"
    listed = {f["file"] for f in manifest["files"]}
    on_disk = {p.name for p in pack_dir.iterdir()
               if p.is_file() and p.name != "MANIFEST.json"}
    assert listed == on_disk
    csv_rows = {}
    for f in manifest["files"]:
        p = pack_dir / f["file"]
        raw = p.read_bytes()
        assert hashlib.sha256(raw).hexdigest() == f["sha256"], \
            f"{f['file']}: manifest sha256 stale"
        assert p.stat().st_size == f["bytes"]
        if f["file"].endswith(".csv"):
            with open(p, newline="", encoding="utf-8") as fh:
                csv_rows[f["file"]] = sum(1 for _ in csv.reader(fh)) - 1
            assert csv_rows[f["file"]] == f["rows"], \
                f"{f['file']}: manifest rows != CSV data rows"
    # parquet twins carry the same row count as their CSV
    for stem, parquet in EXPECTED_SOCAL_FILES.items():
        if parquet:
            pq_entry = next(f for f in manifest["files"]
                            if f["file"] == f"{stem}.parquet")
            assert pq_entry["rows"] == csv_rows[f"{stem}.csv"]


def test_manifest_rows_match_live_query_counts(conn, socal_pack):
    """Manifest row counts must equal the scratch warehouse's own counts (the
    exporter double-checks this itself; we re-verify independently)."""
    pack_dir, _b, _a = socal_pack
    manifest = json.loads((pack_dir / "MANIFEST.json").read_text())
    for f in manifest["files"]:
        stem = f["file"].rsplit(".", 1)[0]
        if stem in EXPECTED_SOCAL_FILES or stem == "mv_socal_rate_guidance":
            (wh_count,) = fetch_one(conn, f"SELECT count(*) FROM {stem}")
            assert wh_count == f["rows"], \
                f"{stem}: manifest rows {f['rows']} != warehouse {wh_count}"


def test_dictionary_row_counts_match(socal_pack):
    pack_dir, _b, _a = socal_pack
    manifest = json.loads((pack_dir / "MANIFEST.json").read_text())
    dict_text = (pack_dir / "DATA_DICTIONARY.md").read_text()
    for f in manifest["files"]:
        if f["rows"] is None:
            continue
        stem = f["file"].rsplit(".", 1)[0]
        m = re.search(
            r"## `%s\.csv`.*?\*\*Rows:\*\* (\d+)" % re.escape(stem),
            dict_text, re.S)
        assert m, f"{stem}: DATA_DICTIONARY section missing"
        assert int(m.group(1)) == f["rows"], \
            f"{stem}: dictionary rows {m.group(1)} != manifest {f['rows']}"


def test_parquet_twins_equal_csvs(socal_pack):
    import pyarrow.parquet as pq
    pack_dir, _b, _a = socal_pack
    for stem, _parquet in EXPECTED_SOCAL_FILES.items():
        if not (pack_dir / f"{stem}.parquet").exists():
            continue
        tbl = pq.read_table(pack_dir / f"{stem}.parquet")
        with open(pack_dir / f"{stem}.csv", newline="",
                  encoding="utf-8") as fh:
            rows = list(csv.reader(fh))
        header, data = rows[0], rows[1:]
        assert list(tbl.column_names) == header, f"{stem}: column drift"
        assert tbl.num_rows == len(data), f"{stem}: row drift"
        # spot columns: compare the first three columns cell-by-cell
        for i, col in enumerate(header[:3]):
            pq_vals = ["" if v is None else str(v)
                       for v in tbl.column(col).to_pylist()]
            assert pq_vals == [r[i] for r in data], \
                f"{stem}.{col}: parquet/CSV values diverge"


def test_pack_jsonb_notes_and_required_fields(socal_pack):
    pack_dir, _b, _a = socal_pack
    with open(pack_dir / "regional_industry_profiles.csv", newline="",
              encoding="utf-8") as fh:
        profile_rows = list(csv.DictReader(fh))
    assert len(profile_rows) == len(PROFILE_REGIONS) * len(VERTICAL_KEYS)
    for row in profile_rows:
        notes = json.loads(row["notes"])            # valid JSON string
        assert isinstance(notes, dict)
        assert "QAtest" in notes["modeling_basis"]

    # no empty required fields anywhere
    for stem, cols in REQUIRED_NON_EMPTY.items():
        with open(pack_dir / f"{stem}.csv", newline="",
                  encoding="utf-8") as fh:
            for line_no, row in enumerate(csv.DictReader(fh), start=2):
                for col in cols:
                    assert (row.get(col) or "").strip() != "", \
                        f"{stem}.csv line {line_no}: empty required {col}"


# ---------------------------------------------------------------------------
# 7) Anomaly detector non-regression (SoCal tables present)
# ---------------------------------------------------------------------------

def _seed_detector_graph(conn):
    """QAtest-tagged customer/job_type/employee + 3 completed jobs:
    50% overrun (medium), 100% overrun (high), break-even control."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO customers (name, email, city) VALUES (%s, %s, %s)"
            " RETURNING id",
            ("QAtest socal customer", "QAtest.socal@example.test", "QA City"))
        cust = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO job_types (name, default_hourly_rate,"
            " typical_duration_hours) VALUES (%s, 100.00, 8.00) RETURNING id",
            ("QAtest socal type",))
        jt = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO employees (first_name, last_name, email, hire_date,"
            " hourly_rate) VALUES (%s,%s,%s,%s,100.00) RETURNING id",
            ("QA", "Socal_Tester", "QAtest.socal.emp@example.test",
             date(2020, 1, 1)))
        emp = cur.fetchone()[0]
        job_ids = {}
        for tag, estimate in (("overrun50", 10000.00),
                              ("overrun100", 5000.00),
                              ("control", 8000.00)):
            cur.execute(
                "INSERT INTO jobs (job_number, customer_id, job_type_id,"
                " job_date, quoted_price, status, estimated_labor_cost)"
                " VALUES (%s,%s,%s,%s,20000.00,'completed',%s) RETURNING id",
                (f"QAtest-socal-{tag}", cust, jt, date(2026, 3, 1), estimate))
            job_ids[tag] = cur.fetchone()[0]
        # actual labor spread over 10 workdays (per-day hours <= 24):
        # 150h total (50% over), 100h (100% over), 80h (break-even)
        for tag, daily in (("overrun50", 15), ("overrun100", 10),
                           ("control", 8)):
            for day in range(10):
                cur.execute(
                    "INSERT INTO labor_logs (job_id, employee_id, work_date,"
                    " hours, hourly_rate, description)"
                    " VALUES (%s,%s,%s,%s,100.00,'QAtest work')",
                    (job_ids[tag], emp, date(2026, 3, 2 + day), daily))
        # a quiet flat expense history (no spike possible: constant, and
        # fewer than 3 prior months anywhere)
        cur.execute(
            "INSERT INTO financial_ledger (entry_type, category, entry_date,"
            " amount, description)"
            " VALUES ('expense','QAtest utilities',%s,100.00,'QAtest')"
            ", ('expense','QAtest utilities',%s,100.00,'QAtest')"
            ", ('expense','QAtest utilities',%s,100.00,'QAtest')",
            (date(2026, 1, 10), date(2026, 2, 10), date(2026, 3, 10)))
    conn.commit()
    return job_ids


def test_detect_anomalies_still_runs_and_alerts(conn):
    """The schema growth must not break detect_anomalies(): engineered 50%
    and 100% labor overruns alert (medium / high), the break-even job does
    not, and SoCal rows coexist harmlessly."""
    job_ids = _seed_detector_graph(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM detect_anomalies()")
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.commit()
    overruns = [r for r in rows if r["out_anomaly_type"] == "labor_overrun"]
    # map alerts back via details.job_number for clarity
    by_number = {r["out_details"]["job_number"]: r for r in overruns}
    assert by_number["QAtest-socal-overrun50"]["out_severity"] == "medium"
    assert by_number["QAtest-socal-overrun100"]["out_severity"] == "high"
    assert Decimal(by_number["QAtest-socal-overrun50"]
                   ["out_deviation_pct"]) == Decimal("50.00")
    assert "QAtest-socal-control" not in by_number
    # the two alerts are persisted, exactly once per job
    (n_open,) = fetch_one(
        conn,
        """SELECT count(*) FROM anomaly_alerts
            WHERE anomaly_type='labor_overrun' AND is_resolved = FALSE
              AND details->>'job_number' IN
                  ('QAtest-socal-overrun50','QAtest-socal-overrun100')""")
    assert n_open == 2


def test_detect_anomalies_idempotent_on_rerun(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM detect_anomalies()")
        first = cur.fetchall()
    conn.commit()
    assert first == [], "no NEW alerts on the second run (idempotent)"
    (n,) = fetch_one(
        conn,
        """SELECT count(*) FROM anomaly_alerts
            WHERE details->>'job_number' LIKE 'QAtest-socal-%'""")
    assert n == 2


# ---------------------------------------------------------------------------
# 8) LIVE warehouse audits (READ-ONLY, skippable)
# ---------------------------------------------------------------------------

def live_database_url():
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    return "postgresql://postgres:postgres@localhost:5432/smb_warehouse"


@pytest.fixture(scope="module")
def live():
    """READ-ONLY connection to the live warehouse; skipped when DATABASE_URL
    points at another database or the live DB is unreachable."""
    url = live_database_url()
    dbname = urllib.parse.urlsplit(url).path.lstrip("/")
    if dbname != LIVE_DB_NAME:
        pytest.skip(f"DATABASE_URL points at {dbname!r}, not the live "
                    f"{LIVE_DB_NAME!r}")
    try:
        conn_ = psycopg2.connect(url)
        conn_.set_session(readonly=True, autocommit=True)
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"live warehouse unreachable: {exc}")
    yield conn_
    conn_.close()


def test_live_wage_2080_consistency_zero_violations(live):
    (n,) = fetch_one(
        live,
        """SELECT count(*) FROM socal_wage_benchmarks
            WHERE (hourly_p25 IS NOT NULL AND annual_p25 IS NOT NULL
                   AND ABS(annual_p25 - hourly_p25*2080) > 120)
               OR (hourly_median IS NOT NULL AND annual_median IS NOT NULL
                   AND ABS(annual_median - hourly_median*2080) > 120)
               OR (hourly_p75 IS NOT NULL AND annual_p75 IS NOT NULL
                   AND ABS(annual_p75 - hourly_p75*2080) > 120)
               OR (hourly_p90 IS NOT NULL AND annual_p90 IS NOT NULL
                   AND ABS(annual_p90 - hourly_p90*2080) > 120)""")
    assert n == 0, f"{n} live wage rows violate the 2080-hour convention"


def test_live_no_duplicate_keys(live):
    dup_checks = [
        ("regional_industry_profiles", "vertical_key, region_code"),
        ("socal_wage_benchmarks", "region_code, occupation_code, data_year"),
        ("socal_macro_indicators", "region_code, series_id, period"),
        ("socal_business_patterns", "geo_level, geo_fips, naics_code,"
         " data_year"),
        ("socal_loan_activity", "program, fiscal_year, geo_level, geo_fips"),
    ]
    for table, key in dup_checks:
        (n,) = fetch_one(
            live,
            f"""SELECT COALESCE(SUM(n-1),0) FROM
                  (SELECT count(*) n FROM {table}
                    GROUP BY {key} HAVING count(*)>1) d""")
        assert n == 0, f"{table}: {n} duplicate rows on ({key})"


def test_live_source_citations_have_date_and_url(live):
    for table in EXPECTED_SOCAL_TABLES:
        if table == "socal_harvest_log":
            # the log's URL column carries no date by design: the retrieval
            # timestamp lives in fetched_at (append-only audit trail)
            (n_bad,) = fetch_one(
                live,
                """SELECT count(*) FROM socal_harvest_log
                    WHERE url IS NULL OR btrim(url) = ''
                       OR url NOT LIKE '%http%'
                       OR fetched_at IS NULL""")
            assert n_bad == 0, \
                f"socal_harvest_log: {n_bad} rows lack a URL/fetch timestamp"
            continue
        (n_bad,) = fetch_one(
            live,
            f"""SELECT count(*) FROM {table}
                 WHERE source IS NULL OR btrim(source) = ''
                    OR source !~ '\\d{{4}}-\\d{{2}}-\\d{{2}}'
                    OR source NOT LIKE '%http%'""")
        assert n_bad == 0, f"{table}: {n_bad} rows lack a dated source URL"
    # every modeled profile discloses MODELED (or "modeled") in source/notes
    (n_undisclosed,) = fetch_one(
        live,
        """SELECT count(*) FROM regional_industry_profiles
            WHERE source NOT ILIKE '%modeled%'
              AND COALESCE(notes::text,'') NOT ILIKE '%modeled%'
              AND COALESCE(notes::text,'') NOT ILIKE '%model%'""")
    assert n_undisclosed == 0


def test_live_mv_formula_matches_recompute(live):
    rows = fetch_all(
        live,
        """WITH latest_wage AS (
               SELECT region_code, occupation_code, MAX(data_year) dy
                 FROM socal_wage_benchmarks
                GROUP BY region_code, occupation_code)
           SELECT mv.region_code, mv.vertical_key, mv.hourly_cost,
                  mv.loaded_cost, mv.suggested_billable_rate,
                  ROUND(w.annual_median/2080.0
                        * (1 + p.locality_burden_pct/100.0), 2),
                  ROUND(w.annual_median/2080.0
                        * (1 + p.locality_burden_pct/100.0)
                        / (p.utilization_target_pct/100.0), 2),
                  ROUND(w.annual_median/2080.0
                        * (1 + p.locality_burden_pct/100.0)
                        / (p.utilization_target_pct/100.0)
                        / (1 - p.gross_margin_pct/100.0), 2)
             FROM mv_socal_rate_guidance mv
             JOIN regional_industry_profiles p USING (region_code,
                                                      vertical_key)
             LEFT JOIN latest_wage lw
                    ON lw.region_code = p.region_code
                   AND lw.occupation_code = p.anchor_occupation_code
             LEFT JOIN socal_wage_benchmarks w
                    ON w.region_code = lw.region_code
                   AND w.occupation_code = lw.occupation_code
                   AND w.data_year = lw.dy""")
    assert rows, "live mv must have rows"
    for (region, vert, h, l, s, exp_h, exp_l, exp_s) in rows:
        if s is None:
            assert (h, l, exp_h, exp_l) == (None, None, None, None), \
                f"{region}/{vert}: partial NULL rate columns"
            continue
        assert Decimal(h) == Decimal(exp_h), f"{region}/{vert}: hourly_cost"
        assert Decimal(l) == Decimal(exp_l), f"{region}/{vert}: loaded_cost"
        assert Decimal(s) == Decimal(exp_s), f"{region}/{vert}: rate"


def test_live_counts_in_expected_ranges(live):
    counts = {}
    for name, sql in (
        ("wages", "SELECT count(*) FROM socal_wage_benchmarks"),
        ("profiles", "SELECT count(*) FROM regional_industry_profiles"),
        ("mv", "SELECT count(*) FROM mv_socal_rate_guidance"),
        ("macro", "SELECT count(*) FROM socal_macro_indicators"),
        ("bp", "SELECT count(*) FROM socal_business_patterns"),
        ("loans", "SELECT count(*) FROM socal_loan_activity"),
    ):
        (counts[name],) = fetch_one(live, sql)
    assert counts["wages"] == 48, f"expected 48 wage rows, got {counts}"
    assert counts["profiles"] == 36, f"expected 36 profiles, got {counts}"
    assert counts["mv"] == 36, f"expected 36 mv rows, got {counts}"
    assert counts["macro"] == 1238, f"expected 1238 macro rows, got {counts}"
    assert counts["bp"] == 36219, f"expected 36219 bp rows, got {counts}"
    assert counts["loans"] == 84, f"expected 84 loan rows, got {counts}"
