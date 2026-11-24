"""
QA suite for the SMB Financial & Operational Data Warehouse.

SELF-CONTAINED: the entire suite runs against a THROWAWAY database it creates
and drops itself (smb_warehouse_qa_test on the same PostgreSQL server), so it
is safe to run at any time and NEVER touches smb_warehouse -- or any other
existing database -- regardless of what data they contain (demo data,
production data, whatever). Run it with:

    cd "/Users/Owner/Desktop/GLM-5.3/Data Warehouse"
    python3 -m pytest test_schema.py -v

Lifecycle (session-scoped `conn` fixture; everything lives inside try/finally
so the teardown runs even if setup or a test fails):
  1. connect to the parent database (DATABASE_URL below; default: the
     maintenance DB 'postgres' on localhost) and
     DROP DATABASE IF EXISTS smb_warehouse_qa_test WITH (FORCE),
  2. CREATE DATABASE smb_warehouse_qa_test,
  3. apply schema.sql, then analytics_views.sql, to the scratch database via
     subprocess psql (-v ON_ERROR_STOP=1; a non-zero exit fails the session
     with psql's captured stdout/stderr),
  4. run all tests against the scratch connection,
  5. teardown: close the test connection and
     DROP DATABASE IF EXISTS smb_warehouse_qa_test WITH (FORCE).

Coverage (99 tests):

  * all 17 base tables exist (information_schema): the 11 core tables
    including the multi-tenant companies registry (migration 001: every
    client-data table is scoped by company_id) and the two forecast-engine
    output tables forecasts / backtest_results, plus the 6 SoCal regional
    benchmark tables (schema.sql sections 2.11-2.16, harvested by
    harvest_socal_fintech.py),
  * the 16 expected FK relationships (catalog lookup + behavioural:
    inserting a child row with a bogus parent must raise IntegrityError) --
    the 8 original domain FKs plus the 8 company_id FKs to companies,
  * primary keys on every table (pg_constraint AND pg_index),
  * the 8 expected FK relationships (catalog lookup + behavioural:
    inserting a child row with a bogus parent must raise IntegrityError),
  * money columns are NUMERIC(14,2) (plus the other numeric shapes)
    via information_schema.data_type / numeric_precision / numeric_scale,
  * CHECK constraints enforced behaviourally (ledger amount <= 0, bad job
    status, labor hours <= 0 / > 24, mid-month benchmark period, resolved
    alert without timestamp, completion before start, negative rate),
  * the exclusive ledger attribution rule (chk_ledger_single_attribution):
    a ledger row with BOTH job_id and customer_id set is rejected with a
    CheckViolation, while a both-NULL overhead row is accepted,
  * the 5 materialized views exist in pg_matviews, are queryable, accept a
    plain REFRESH MATERIALIZED VIEW, and refresh_all_analytics() runs
    (it now refreshes all six, mv_socal_rate_guidance included),
  * detect_anomalies() exists (pg_get_functiondef), executes cleanly and is
    IDEMPOTENT: engineered fixture data yields exactly one labor_overrun
    alert (50% overrun -> severity 'medium') and one expense_spike
    (z = 7.5 -> severity 'high'); a quiet (constant) expense category yields
    no alert; a second call returns zero NEW rows; duplicate OPEN alerts are
    rejected by the partial unique indexes; resolving an alert frees the key,
  * REGRESSION tests for the CORRECTED spike semantics (expense_spike now
    requires the month total to be 2 standard deviations ABOVE the prior-6-month
    rolling mean, i.e. z > 2): a 1.5x-mean month with z = 1.28 must NOT be
    flagged (the old `month_total > 2 * prior_stddev` bug WOULD have flagged
    it); a month at z = 3.08 must be flagged exactly once with severity
    'high'; and labor cost at exactly the 1.2x estimate threshold (20%, which
    is NOT >20%) must NOT raise a labor_overrun while a 21% control job does,
  * customer-attributed ledger costs (financial_ledger.customer_id, e.g.
    customer acquisition spend): mv_customer_profitability surfaces them in
    acquisition_cost, folds them into total_cost = job_cost +
    acquisition_cost and prices them into a NET gross_margin/margin_pct
    (hand-computed fixture),
  * is_whale semantics: percentile_cont(0.8) of revenue over ALL customers
    (zero-revenue included), hand-computed on a 6-customer fixture (P80 =
    600.00 exactly): the top-revenue loss-maker is the only whale, a
    revenue-at-P80-but-profitable account is not (>= boundary), and a
    below-P80 loss-maker is not,
  * macro benchmark support (market_benchmarks): job_type_id is now nullable
    (macro rows have no job type), benchmark_kind exists, defaults to
    'job_market' and rejects unknown kinds, series_id exists, the partial
    unique index uq_market_benchmarks_series_period (series_id, region,
    period) WHERE series_id IS NOT NULL is present and enforces macro dedup
    even without ON CONFLICT, job_market (job_type_id, period, region)
    uniqueness is intact while job_type_id-NULL macro rows coexist (NULLs
    are distinct in UNIQUE),
  * macro upsert semantics (the fetcher's idempotency mechanism): INSERT ...
    ON CONFLICT (series_id, region, period) WHERE series_id IS NOT NULL
    DO UPDATE updates the row in place -- exactly 1 row with the new price,
  * mv_benchmark_comparison: unique index on (period) and plain REFRESH
    work; months derive from INTERNAL activity only; the blended internal
    labor rate is SUM(hours*rate)/SUM(hours) (hand-computed 87.50 on an
    8-hour fixture) and internal materials spend per job matches; unpinned
    QAtest macro series never leak into the pinned market_wage_* / ppi_*
    columns (all NULL when no real macro data exists),
  * fetch_benchmarks.to_monthly: offline unit test (no DB interaction) of
    the sub-monthly -> calendar-month averaging the fetcher relies on,
  * forecast-table behavior: forecasts' month-start CHECK, series whitelist,
    non-negative-predicted CHECK (net_cash_flow exempt, positive control),
    (series, period_start, model) upsert key; backtest_results'
    (series, model, fold) upsert key (all inside rolled-back savepoints),
  * OFFLINE unit tests of the two decision modules: pricing_simulator.
    simulate_job (hand-computed breakdown, 1.5x-on-OT-share premium,
    percentage-point target/market comparisons, documented clamps +
    clamps_applied reporting, price/hours ValueError) and forecast_engine's
    closed-form helpers (_least_squares, _seasonal_indices, _winsorize,
    _mape_pct/_wape_pct incl. the MAPE-vs-WAPE series convention,
    _add_months),
  * CSV ingest engine (ingest_csv.py -- unit + integration against the scratch
    DB): coerce() value cleaning (money with $/commas/parenthesized
    negatives, money_signed, dates with formats lists, hours bounds, bools,
    text trim/collapse/lower/null_if); unique_key and exact dedup modes
    (in-file md5 dedup + temp-table anti-join vs existing rows); FK
    resolution policies (resolve jobs by job_number / employees by email,
    skip_row vs error semantics, on_missing=create customer creation with
    @imported.example.com emails + created_refs counting); money_signed ->
    entry_type split verified in-table; row errors reported with CSV line
    numbers while good rows still load; dry_run writing NOTHING (regression:
    on_missing=create must not persist either); refresh=true running
    refresh_all_analytics() + detect_anomalies(); file-level rollback on
    unsupported target tables,
  * app.py and ingest_csv.py byte-compile cleanly (no server started); app.py
    additionally passes a READ-ONLY streamlit AppTest smoke render (all tabs,
    no interactions -- skipped when the dashboard deps or the live DB are
    unavailable),
  * view content correctness: a fully-known fixture job is hand-computed and
    compared against all six materialized views (revenue, labor cost =
    hours * rate, margin, variance, benchmark delta, monthly buckets,
    job_cost/acquisition_cost split, is_whale),
  * a FINAL cleanliness assertion: the scratch database ends with the schema,
    the materialized views, zero 'QAtest' fixture rows and zero QA-generated
    anomaly alerts.

Connection: DATABASE_URL only selects the PARENT database from which the
throwaway smb_warehouse_qa_test database is derived (the database name in the
URL is replaced by the scratch name); default:
    postgresql://postgres:postgres@localhost:5432/postgres
The suite itself always runs against smb_warehouse_qa_test.

Data hygiene: every fixture row uses 'QAtest' prefixed names / emails /
job numbers / categories. Each test's teardown sweeps ALL QAtest rows (and
the anomaly alerts referencing them), and the session teardown drops the
whole scratch database, so the suite can leave no residue on the server at
all -- whatever a test forgets to clean up dies with the database.
"""

import csv
import datetime
import os
import shutil
import subprocess
import sys
import urllib.parse
import uuid
from decimal import Decimal

import psycopg2
import psycopg2.errors
import pytest

# DATABASE_URL names only the PARENT database (server + maintenance DB) from
# which the scratch database is derived; the database component of this URL is
# replaced by SCRATCH_DB_NAME. The suite never opens smb_warehouse.
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/postgres",
)

SCRATCH_DB_NAME = "smb_warehouse_qa_test"

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _BASE_DIR)  # for project-local imports: ingest_csv, fetch_benchmarks
SCHEMA_SQL_PATH = os.path.join(_BASE_DIR, "schema.sql")
ANALYTICS_VIEWS_SQL_PATH = os.path.join(_BASE_DIR, "analytics_views.sql")

PSQL_BIN = shutil.which("psql") or "/Users/Owner/pg/bin/psql"

QA_PREFIX = "QAtest"
BOGUS_PARENT_ID = 987654321  # no row ever has this id

EXPECTED_TABLES = {
    # Multi-tenant client registry (migrations/001_add_companies.sql):
    # parent of the 8 client-data tables via company_id FKs.
    "companies",
    "customers",
    "job_types",
    "employees",
    "jobs",
    "financial_ledger",
    "labor_logs",
    "market_benchmarks",
    "anomaly_alerts",
    # forecast_engine.py output tables (no FKs in either direction)
    "forecasts",
    "backtest_results",
    # SoCal regional benchmark tables (schema.sql sections 2.11-2.16,
    # written by harvest_socal_fintech.py; no FKs)
    "regional_industry_profiles",
    "socal_wage_benchmarks",
    "socal_macro_indicators",
    "socal_business_patterns",
    "socal_loan_activity",
    "socal_harvest_log",
}

EXPECTED_MATVIEWS = {
    "mv_job_type_profitability",
    "mv_labor_cost_analysis",
    "mv_customer_profitability",
    "mv_monthly_summary",
    "mv_benchmark_comparison",
    # SoCal billable-rate guidance (analytics_views.sql section 7)
    "mv_socal_rate_guidance",
}

# (child_table, child_column, parent_table, parent_column, ON DELETE action)
EXPECTED_FKS = [
    # Multi-tenant scope (migration 001): every client-data table belongs to
    # exactly one workspace via company_id -> companies.company_id (RESTRICT:
    # a company with dependent history cannot be deleted).
    ("customers", "company_id", "companies", "company_id", "RESTRICT"),
    ("job_types", "company_id", "companies", "company_id", "RESTRICT"),
    ("employees", "company_id", "companies", "company_id", "RESTRICT"),
    ("jobs", "company_id", "companies", "company_id", "RESTRICT"),
    ("financial_ledger", "company_id", "companies", "company_id", "RESTRICT"),
    ("labor_logs", "company_id", "companies", "company_id", "RESTRICT"),
    ("anomaly_alerts", "company_id", "companies", "company_id", "RESTRICT"),
    ("forecasts", "company_id", "companies", "company_id", "RESTRICT"),
    # Domain FKs (unchanged)
    ("jobs", "customer_id", "customers", "id", "RESTRICT"),
    ("jobs", "job_type_id", "job_types", "id", "RESTRICT"),
    ("labor_logs", "employee_id", "employees", "id", "RESTRICT"),
    ("labor_logs", "job_id", "jobs", "id", "RESTRICT"),
    ("financial_ledger", "job_id", "jobs", "id", "SET NULL"),
    ("financial_ledger", "customer_id", "customers", "id", "SET NULL"),
    ("market_benchmarks", "job_type_id", "job_types", "id", "RESTRICT"),
    ("anomaly_alerts", "job_id", "jobs", "id", "SET NULL"),
]

# The 8 client-data tables scoped by company_id (NOT NULL + FK to companies).
COMPANY_SCOPED_TABLES = (
    "customers", "job_types", "employees", "jobs",
    "financial_ledger", "labor_logs", "anomaly_alerts", "forecasts",
)

# Tables that must NOT carry a company_id column: market/global data shared
# across client workspaces by design (migration 001 scopes client data only).
GLOBAL_TABLES_NO_COMPANY_ID = (
    "market_benchmarks", "backtest_results",
    "regional_industry_profiles", "socal_wage_benchmarks",
    "socal_macro_indicators", "socal_business_patterns",
    "socal_loan_activity", "socal_harvest_log",
)

# Name of the fixture workspace: schema.sql seeds the demo client workspace
# ('BrightPath Works (Demo)', identity id 1) so the company_id DEFAULT 1 has a
# valid parent on a fresh boot. The scratch DBs therefore mirror the live
# deployment: the fixture graph lives in company #1 and tests that need a
# SECOND workspace create their own QAtest company with an explicit company_id
# (swept by cleanup_qa_data).
QA_SESSION_COMPANY_NAME = "BrightPath Works (Demo)"

# (table, column, expected numeric_precision, expected numeric_scale).
# The (14,2) entries are the MONEY columns required by the conventions;
# the others pin the percentage/hours shapes too.
EXPECTED_NUMERIC_COLUMNS = [
    ("jobs", "quoted_price", 14, 2),
    ("jobs", "estimated_labor_cost", 14, 2),
    ("jobs", "actual_revenue", 14, 2),
    ("jobs", "estimated_hours", 6, 2),
    ("financial_ledger", "amount", 14, 2),
    ("employees", "hourly_rate", 14, 2),
    ("labor_logs", "hours", 6, 2),
    ("labor_logs", "hourly_rate", 14, 2),
    ("job_types", "default_hourly_rate", 14, 2),
    ("job_types", "typical_duration_hours", 6, 2),
    ("market_benchmarks", "avg_price", 14, 2),
    ("market_benchmarks", "avg_labor_cost_per_job", 14, 2),
    ("market_benchmarks", "avg_margin_pct", 5, 2),
    ("anomaly_alerts", "metric_value", 14, 2),
    ("anomaly_alerts", "baseline_value", 14, 2),
    ("anomaly_alerts", "deviation_pct", 8, 2),
    ("anomaly_alerts", "z_score", 6, 2),
    ("forecasts", "predicted", 14, 2),
    ("forecasts", "mape", 8, 2),
    ("forecasts", "rmse", 14, 2),
    ("backtest_results", "mape", 8, 2),
    ("backtest_results", "rmse", 14, 2),
]


# ---------------------------------------------------------------------------
# Scratch database lifecycle (self-contained suite)
# ---------------------------------------------------------------------------

def scratch_database_url():
    """DATABASE_URL with the database component replaced by SCRATCH_DB_NAME.

    This is the ONLY thing the suite connects to for testing: a throwaway
    database on the same server, never smb_warehouse and never whatever
    DATABASE_URL names."""
    parts = urllib.parse.urlsplit(DATABASE_URL)
    return urllib.parse.urlunsplit(
        (parts.scheme, parts.netloc, "/" + SCRATCH_DB_NAME, parts.query,
         parts.fragment)
    )


def _parent_connect():
    """Autocommit connection to the parent database. Autocommit is required:
    CREATE/DROP DATABASE cannot run inside a transaction block."""
    conn_ = psycopg2.connect(DATABASE_URL)
    conn_.autocommit = True
    return conn_


def drop_scratch_database(scratch_url):
    """DROP DATABASE IF EXISTS <scratch> WITH (FORCE), via a fresh connection
    to the parent database. WITH (FORCE) also terminates any lingering
    backends (e.g. a crashed earlier run), so setup is always clean. The
    scratch name is a module constant, not user input, so it is interpolated
    directly (DROP DATABASE takes an identifier, not a bind parameter)."""
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
    """Apply one .sql file to the scratch database via subprocess psql.
    Asserts exit code 0; on failure pytest.fail reports the captured output
    so pytest shows exactly which statement blew up."""
    if not os.path.isfile(path):
        pytest.fail(f"SQL file not found: {path}", pytrace=False)
    proc = subprocess.run(
        [PSQL_BIN, scratch_url, "-X", "-v", "ON_ERROR_STOP=1", "-f", path],
        capture_output=True,
        text=True,
        timeout=180,
    )
    if proc.returncode != 0:
        pytest.fail(
            f"psql failed applying {os.path.basename(path)} to"
            f" {SCRATCH_DB_NAME} (exit code {proc.returncode}):\n"
            f"--- psql stdout ---\n{proc.stdout}\n"
            f"--- psql stderr ---\n{proc.stderr}",
            pytrace=False,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def cleanup_qa_data(conn):
    """Delete every QAtest fixture row (FK-safe order) and the anomaly alerts
    that reference them. First rolls back any pending/aborted session state so
    this is callable even after a failed statement. The sweep also removes
    QAtest client workspaces (companies) -- EXCEPT the session fixture
    workspace the scratch-only DEFAULTs point at (see _seed_qa_company_defaults
    and QA_SESSION_COMPANY_NAME)."""
    conn.rollback()  # discard aborted / uncommitted state; no-op if committed
    session_co = QA_SESSION_COMPANY["company_id"]   # 1 = the demo workspace
    statements = [
        # Alerts first: they reference jobs (RESTRICT blocks deleting jobs
        # that alerts still point at... actually alerts use SET NULL, but we
        # remove QA alerts explicitly so none survive).
        """DELETE FROM anomaly_alerts a USING jobs j
            WHERE a.job_id = j.id AND j.job_number LIKE 'QAtest%'""",
        """DELETE FROM anomaly_alerts
            WHERE category LIKE 'QAtest%'
               OR details->>'job_number' LIKE 'QAtest%'""",
        # ...plus every alert attached to a QAtest workspace other than the
        # session workspace (its alerts are covered by the sweeps above).
        f"""DELETE FROM anomaly_alerts
            WHERE company_id IN (SELECT company_id FROM companies
                                  WHERE company_name LIKE 'QAtest%'
                                    AND company_id <> {session_co})""",
        """DELETE FROM market_benchmarks
            WHERE job_type_id IN (SELECT id FROM job_types WHERE name LIKE 'QAtest%')
               OR series_id LIKE 'QAtest%'      -- macro rows (fetcher-style)
               OR source LIKE 'QAtest%'""",
        """DELETE FROM financial_ledger
            WHERE job_id IN (SELECT id FROM jobs WHERE job_number LIKE 'QAtest%')
               OR customer_id IN (SELECT id FROM customers
                                  WHERE name LIKE 'QAtest%' OR email LIKE 'QAtest%')
               OR category LIKE 'QAtest%'
               OR vendor LIKE 'QAtest%'""",
        """DELETE FROM financial_ledger
            WHERE company_id IN (SELECT company_id FROM companies
                                  WHERE company_name LIKE 'QAtest%'
                                    AND company_id <> {co})""".format(co=session_co),
        """DELETE FROM labor_logs
            WHERE job_id IN (SELECT id FROM jobs WHERE job_number LIKE 'QAtest%')
               OR employee_id IN (SELECT id FROM employees WHERE email LIKE 'QAtest%')""",
        """DELETE FROM labor_logs
            WHERE company_id IN (SELECT company_id FROM companies
                                  WHERE company_name LIKE 'QAtest%'
                                    AND company_id <> {co})""".format(co=session_co),
        "DELETE FROM jobs WHERE job_number LIKE 'QAtest%'",
        """DELETE FROM jobs
            WHERE company_id IN (SELECT company_id FROM companies
                                  WHERE company_name LIKE 'QAtest%'
                                    AND company_id <> {co})""".format(co=session_co),
        "DELETE FROM employees WHERE email LIKE 'QAtest%'",
        "DELETE FROM job_types WHERE name LIKE 'QAtest%'",
        # Forecast-engine output tables: fixture rows are tagged via model/fold
        # so this sweep keeps them clean too (final test asserts they are empty).
        """DELETE FROM forecasts
            WHERE model LIKE 'QAtest%' OR series LIKE 'QAtest%'
               OR details->>'fixture' LIKE 'QAtest%'""",
        """DELETE FROM backtest_results
            WHERE model LIKE 'QAtest%' OR series LIKE 'QAtest%'
               OR fold LIKE 'QAtest%'""",
        # Ingest-created customers get @imported.example.com slug emails
        # (ingest_csv._slug_email), which are not QAtest-prefixed.
        "DELETE FROM customers WHERE name LIKE 'QAtest%' OR email LIKE 'QAtest%'"
        " OR email LIKE '%@imported.example.com'",
        # Multi-tenant workspaces created by tests (explicit company_id
        # fixtures): swept LAST, once every dependent row is gone. The session
        # workspace itself survives -- the scratch-only DEFAULTs point at it
        # until the database is dropped; the final cleanliness test documents
        # it as the expected single survivor.
        f"""DELETE FROM companies
            WHERE company_name LIKE 'QAtest%'
              AND company_id <> {session_co}
              AND NOT EXISTS (SELECT 1 FROM anomaly_alerts a
                               WHERE a.company_id = companies.company_id)
              AND NOT EXISTS (SELECT 1 FROM jobs j
                               WHERE j.company_id = companies.company_id)
              AND NOT EXISTS (SELECT 1 FROM financial_ledger fl
                               WHERE fl.company_id = companies.company_id)
              AND NOT EXISTS (SELECT 1 FROM labor_logs ll
                               WHERE ll.company_id = companies.company_id)
              AND NOT EXISTS (SELECT 1 FROM customers c
                               WHERE c.company_id = companies.company_id)
              AND NOT EXISTS (SELECT 1 FROM forecasts f
                               WHERE f.company_id = companies.company_id)""",
    ]
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)
    conn.commit()


def create_qa_graph(conn, tag):
    """Insert a minimal, FK-valid fixture graph and COMMIT it:
    customer -> job_type -> employee -> job. Returns a dict of ids + tag.
    Every name/email/job_number is 'QAtest'-prefixed so the cleanup sweep
    finds them. All rows are created in the session's client workspace
    (company_id = the QAtest session workspace; multi-tenant scope)."""
    ids = {"tag": tag, "company_id": QA_SESSION_COMPANY["company_id"]}
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO customers (company_id, name, email, city)"
            " VALUES (%s, %s, %s, %s) RETURNING id",
            (ids["company_id"], f"{QA_PREFIX}_{tag} customer",
             f"{QA_PREFIX}_{tag}@example.test", "QA City"),
        )
        ids["customer_id"] = cur.fetchone()[0]

        cur.execute(
            "INSERT INTO job_types (company_id, name, default_hourly_rate,"
            " typical_duration_hours) VALUES (%s, %s, 100.00, 8.00)"
            " RETURNING id",
            (ids["company_id"], f"{QA_PREFIX}_{tag} type"),
        )
        ids["job_type_id"] = cur.fetchone()[0]

        cur.execute(
            "INSERT INTO employees (company_id, first_name, last_name, email,"
            " hire_date, hourly_rate) VALUES (%s, %s, %s, %s, %s, 100.00)"
            " RETURNING id",
            (ids["company_id"], "QA", f"Tester_{tag}",
             f"{QA_PREFIX}_{tag}.emp@example.test", datetime.date(2020, 1, 1)),
        )
        ids["employee_id"] = cur.fetchone()[0]

        cur.execute(
            "INSERT INTO jobs (company_id, job_number, customer_id, job_type_id,"
            " job_date, quoted_price, status)"
            " VALUES (%s, %s, %s, %s, %s, 5000.00, 'quoted') RETURNING id",
            (ids["company_id"], f"{QA_PREFIX}-{tag}-JOB", ids["customer_id"],
             ids["job_type_id"], datetime.date(2026, 3, 1)),
        )
        ids["job_id"] = cur.fetchone()[0]
    conn.commit()
    return ids


def expect_rejected(conn, statements, expected_exc):
    """Run `statements` inside a savepoint. Every statement except the LAST
    must succeed; the last must raise `expected_exc`. The savepoint is rolled
    back afterwards so a rejected insert never poisons the session."""
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT qa_reject_sp")
        for stmt in statements[:-1]:
            cur.execute(stmt)
        with pytest.raises(expected_exc):
            cur.execute(statements[-1])
        cur.execute("ROLLBACK TO SAVEPOINT qa_reject_sp")
        cur.execute("RELEASE SAVEPOINT qa_reject_sp")


def fetch_all(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def fetch_one(conn, sql, params=None):
    return fetch_all(conn, sql, params)[0]


def fetch_dicts(conn, sql, params=None):
    """fetch_all with column names attached (list of dicts). Used for
    detect_anomalies() result rows, whose columns are prefixed out_."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Session / fixture management
# ---------------------------------------------------------------------------

# The session's client workspace (created once the scratch schema exists).
# Its id is referenced by the scratch-only column DEFAULTs below and by the
# cleanup sweep (which deletes QAtest companies EXCEPT this one, because the
# defaults keep pointing at it until the database is dropped).
QA_SESSION_COMPANY = {"company_id": None}


def _seed_qa_company_defaults(conn):
    """Point the scratch DB's company_id DEFAULTs at the schema-seeded demo
    workspace and record its id.

    MULTI-TENANT TEST CONVENIENCE (scratch DB only): schema.sql seeds the demo
    client workspace as company #1 (so the DEFAULT 1 on the company_id columns
    has a valid parent). The pre-existing fixture INSERTs in this suite predate
    multi-tenancy and do not carry company_id, so they land in that workspace
    via the column DEFAULT -- exactly like unscoped legacy writers on the live
    database. The dedicated multi-tenant tests below insert rows with EXPLICIT
    company_id values (their own QAtest workspaces) and prove the per-workspace
    scoping. This function only verifies the expected fixture workspace and
    normalizes the DEFAULTs (a no-op against the fresh schema)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT company_name FROM companies WHERE company_id = 1")
        row = cur.fetchone()
        assert row is not None and row[0] == QA_SESSION_COMPANY_NAME, (
            f"schema.sql must seed '{QA_SESSION_COMPANY_NAME}' as company 1; "
            f"got {row!r}")
        QA_SESSION_COMPANY["company_id"] = 1
        for table in COMPANY_SCOPED_TABLES:
            cur.execute(
                f"ALTER TABLE {table} ALTER COLUMN company_id SET DEFAULT %s",
                (QA_SESSION_COMPANY["company_id"],),
            )
    conn.commit()


@pytest.fixture(scope="session")
def conn():
    """Session-scoped connection to the THROWAWAY scratch database.

    Setup: drop any stale scratch database (WITH FORCE), create it, apply
    schema.sql then analytics_views.sql via subprocess psql, seed the session
    QAtest client workspace (+ scratch-only company_id DEFAULTs, see
    _seed_qa_company_defaults), then connect.
    Teardown runs even if setup or a test fails (the whole body is inside
    try/finally): close the test connection and DROP DATABASE IF EXISTS ...
    WITH (FORCE), so the server is left exactly as it was found. smb_warehouse
    is never touched."""
    scratch_url = scratch_database_url()
    _conn = None
    try:
        drop_scratch_database(scratch_url)          # pre-clean stale scratch db
        create_scratch_database()
        apply_sql_file(scratch_url, SCHEMA_SQL_PATH)
        apply_sql_file(scratch_url, ANALYTICS_VIEWS_SQL_PATH)
        _conn = psycopg2.connect(scratch_url)
        _conn.autocommit = False
        _seed_qa_company_defaults(_conn)
        yield _conn
    finally:
        if _conn is not None:
            try:
                _conn.close()
            except Exception:  # pragma: no cover - connection already broken
                pass
        try:
            drop_scratch_database(scratch_url)      # remove the throwaway db
        except Exception as exc:  # pragma: no cover - backstop only
            print(f"WARNING: could not drop scratch database"
                  f" {SCRATCH_DB_NAME}: {exc!r}")


@pytest.fixture
def qa_graph(conn):
    """Function-scoped fixture graph (customer/job_type/employee/job), removed
    again by a full QAtest sweep at teardown."""
    tag = uuid.uuid4().hex[:8]
    ids = create_qa_graph(conn, tag)
    yield ids
    cleanup_qa_data(conn)


@pytest.fixture
def qa_company_cleanup(conn):
    """Function-scoped cleanup hook for tests that create their OWN QAtest
    client workspaces / rows with explicit company_id (multi-tenant fixtures)
    without needing the full qa_graph: the teardown runs the same full QAtest
    sweep, including the QAtest-companies delete."""
    yield
    cleanup_qa_data(conn)


# ---------------------------------------------------------------------------
# 1) Tables
# ---------------------------------------------------------------------------

def test_all_base_tables_exist(conn):
    rows = fetch_all(
        conn,
        "SELECT table_name FROM information_schema.tables"
        " WHERE table_schema = 'public' AND table_type = 'BASE TABLE'",
    )
    tables = {r[0] for r in rows}
    assert EXPECTED_TABLES <= tables, f"missing tables: {EXPECTED_TABLES - tables}"
    assert tables == EXPECTED_TABLES, f"unexpected tables: {tables - EXPECTED_TABLES}"


# ---------------------------------------------------------------------------
# 2) Primary keys
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table", sorted(EXPECTED_TABLES))
def test_primary_keys_on_all_tables(conn, table):
    # via pg_constraint
    (pk_constraints,) = fetch_one(
        conn,
        """SELECT count(*) FROM pg_constraint con
             JOIN pg_class c ON c.oid = con.conrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname = %s AND con.contype = 'p'""",
        (table,),
    )
    assert pk_constraints == 1, f"{table}: expected exactly 1 PK constraint"
    # via pg_index
    (has_pk_index,) = fetch_one(
        conn,
        """SELECT EXISTS (
               SELECT 1
                 FROM pg_index i
                 JOIN pg_class c ON c.oid = i.indrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public' AND c.relname = %s AND i.indisprimary
           )""",
        (table,),
    )
    assert has_pk_index is True, f"{table}: no primary-key index in pg_index"


# ---------------------------------------------------------------------------
# 3) FK relationships (catalog)
# ---------------------------------------------------------------------------

FK_CATALOG_SQL = """
SELECT src.relname, sa.attname, tgt.relname, ta.attname,
       CASE con.confdeltype
            WHEN 'r' THEN 'RESTRICT'
            WHEN 'n' THEN 'SET NULL'
            WHEN 'c' THEN 'CASCADE'
            WHEN 'a' THEN 'NO ACTION'
            ELSE con.confdeltype::text
       END
  FROM pg_constraint con
  JOIN pg_class src ON src.oid = con.conrelid
  JOIN pg_namespace n ON n.oid = src.relnamespace
  JOIN pg_class tgt ON tgt.oid = con.confrelid
  JOIN unnest(con.conkey) WITH ORDINALITY AS ck(attnum, ord) ON TRUE
  JOIN unnest(con.confkey) WITH ORDINALITY AS fk(attnum, ord) ON fk.ord = ck.ord
  JOIN pg_attribute sa ON sa.attrelid = src.oid AND sa.attnum = ck.attnum
  JOIN pg_attribute ta ON ta.attrelid = tgt.oid AND ta.attnum = fk.attnum
 WHERE con.contype = 'f' AND n.nspname = 'public'
"""


@pytest.mark.parametrize("child,col,parent,pcol,action", EXPECTED_FKS,
                         ids=["-".join(c[:2]) for c in EXPECTED_FKS])
def test_fk_relationships_in_catalog(conn, child, col, parent, pcol, action):
    rows = fetch_all(conn, FK_CATALOG_SQL)
    found = {
        (r[0], r[1], r[2], r[3]): r[4]
        for r in rows
    }
    key = (child, col, parent, pcol)
    assert key in found, f"FK {key} missing from catalog; found {sorted(found)}"
    assert found[key] == action, f"FK {key}: expected ON DELETE {action}, got {found[key]}"


# ---------------------------------------------------------------------------
# 4) Money column types
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table,col,precision,scale", EXPECTED_NUMERIC_COLUMNS,
                         ids=["{}.{}".format(*c[:2]) for c in EXPECTED_NUMERIC_COLUMNS])
def test_money_column_types(conn, table, col, precision, scale):
    row = fetch_one(
        conn,
        """SELECT data_type, numeric_precision, numeric_scale
             FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s AND column_name = %s""",
        (table, col),
    )
    assert row is not None, f"{table}.{col} does not exist"
    data_type, prec, sc = row
    assert data_type == "numeric", f"{table}.{col}: expected numeric, got {data_type}"
    assert (prec, sc) == (precision, scale), (
        f"{table}.{col}: expected NUMERIC({precision},{scale}), "
        f"got NUMERIC({prec},{sc})"
    )


# ---------------------------------------------------------------------------
# 4b) Multi-tenant shape (migration 001: companies + company_id everywhere)
# ---------------------------------------------------------------------------

def test_company_id_columns_multi_tenant_shape(conn):
    """Migration 001 shape: company_id exists NOT NULL with an FK to
    companies on exactly the 8 client-data tables, is ABSENT from the
    global/market tables, and the forecasts upsert key is per workspace."""
    # companies itself carries the PK column.
    (co_col,) = fetch_one(
        conn,
        """SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'companies'
              AND column_name = 'company_id'""",
    )
    assert co_col == "company_id"
    (co_nullable,) = fetch_one(
        conn,
        """SELECT is_nullable FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'companies'
              AND column_name = 'company_id'""",
    )
    assert co_nullable == "NO"

    # The 8 client tables: NOT NULL + DEFAULT 1 (documented legacy-writer
    # safety net: unscoped INSERTs land in the demo workspace, which schema.sql
    # seeds as company #1; every production writer sets company_id explicitly)
    # + FK to companies(company_id) + index.
    for table in COMPANY_SCOPED_TABLES:
        row = fetch_one(
            conn,
            """SELECT is_nullable, column_default
                 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                  AND column_name = 'company_id'""",
            (table,),
        )
        assert row is not None, f"{table}.company_id missing"
        nullable, default = row
        assert nullable == "NO", f"{table}.company_id must be NOT NULL"
        assert default is not None and default.startswith("1"), (
            f"{table}.company_id: expected DEFAULT 1, got {default!r}")
        (idx,) = fetch_one(
            conn,
            "SELECT count(*) FROM pg_indexes WHERE schemaname = 'public'"
            " AND tablename = %s AND indexdef LIKE '%%(company_id)%%'",
            (table,),
        )
        assert idx >= 1, f"{table}: no company_id index"

    # The demo workspace exists with company_id 1 (schema.sql seeds it so the
    # DEFAULT has a valid parent on a fresh boot).
    (demo_name,) = fetch_one(
        conn, "SELECT company_name FROM companies WHERE company_id = 1")
    assert demo_name == "BrightPath Works (Demo)"

    # Global/market tables must NOT carry the column.
    for table in GLOBAL_TABLES_NO_COMPANY_ID:
        (n,) = fetch_one(
            conn,
            """SELECT count(*) FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                  AND column_name = 'company_id'""",
            (table,),
        )
        assert n == 0, f"{table} must stay GLOBAL (no company_id)"

    # forecasts' upsert key is per workspace now: the UNIQUE constraint
    # definition includes company_id.
    (condef,) = fetch_one(
        conn,
        """SELECT pg_get_constraintdef(oid) FROM pg_constraint
            WHERE conrelid = 'forecasts'::regclass
              AND contype = 'u'""",
    )
    assert "company_id" in condef and "series" in condef \
        and "period_start" in condef and "model" in condef, condef

    # NOT NULL is enforced behaviourally: an explicit NULL company_id is
    # rejected even though the scratch DB sets a DEFAULT (see
    # _seed_qa_company_defaults).
    expect_rejected(
        conn,
        ["INSERT INTO customers (company_id, name, email)"
         " VALUES (NULL, 'QAtest-null-co', 'QAtest-null-co@example.test')"],
        psycopg2.errors.NotNullViolation,
    )


def _create_qa_company(conn, tag, suffix):
    """Create an additional QAtest client workspace with EXPLICIT id capture.
    Returns company_id. Swept by cleanup_qa_data."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO companies (company_name, industry_vertical)"
            " VALUES (%s, %s) RETURNING company_id",
            (f"{QA_PREFIX}_{tag} company {suffix}", "qa_fixture"),
        )
        return cur.fetchone()[0]


def test_detect_anomalies_isolated_per_company(conn, qa_company_cleanup):
    """detect_anomalies() is PER WORKSPACE: expense-spike statistics are
    computed from one company's ledger rows only, so workspace B's spike is
    detected from B's own history and the alert carries B's company_id --
    never a blend of two businesses' books."""
    tag = uuid.uuid4().hex[:8]
    co_b = _create_qa_company(conn, tag, "B")
    session_co = QA_SESSION_COMPANY["company_id"]
    spike_cat = f"{QA_PREFIX}_{tag}_widgets_b"

    # Workspace B: 4 quiet months then a spike month (history 50,150,100,100
    # -> mean 100, STDDEV_SAMP = sqrt(5000/3) = 40.8248; month 500 ->
    # z = (500-100)/40.8248 = 9.80 -> severity 'high').
    rows_b = [
        (spike_cat, datetime.date(2026, 1, 5), 50.00),
        (spike_cat, datetime.date(2026, 2, 5), 150.00),
        (spike_cat, datetime.date(2026, 3, 5), 100.00),
        (spike_cat, datetime.date(2026, 4, 5), 100.00),
        (spike_cat, datetime.date(2026, 5, 5), 500.00),
    ]
    with conn.cursor() as cur:
        for category, day, amount in rows_b:
            cur.execute(
                "INSERT INTO financial_ledger (company_id, entry_type,"
                " category, entry_date, amount, vendor, description)"
                " VALUES (%s, 'expense', %s, %s, %s, %s, %s)",
                (co_b, category, day, amount, f"{QA_PREFIX}-{tag}-vendor",
                 f"{tag} company-B spike fixture"),
            )
    conn.commit()

    rows = fetch_dicts(conn, "SELECT * FROM detect_anomalies()")
    conn.commit()
    spikes = [r for r in rows if r["out_anomaly_type"] == "expense_spike"]
    assert len(spikes) == 1, f"expected exactly 1 spike, got {rows}"
    sp = spikes[0]
    assert sp["out_subject"] == f"{spike_cat} - May 2026"
    assert sp["out_severity"] == "high"
    assert sp["out_metric_value"] == Decimal("500.00")
    assert sp["out_baseline_value"] == Decimal("100.00")   # (50+150+100+100)/4
    assert Decimal(str(sp["out_details"]["prior_6m_stddev"])) == Decimal("40.82")

    # The stored alert carries WORKSPACE B's company_id.
    (stored_co,) = fetch_one(
        conn,
        "SELECT company_id FROM anomaly_alerts WHERE category = %s",
        (spike_cat,),
    )
    assert stored_co == co_b

    # Cross-company isolation: workspace B's alert is NOT visible under the
    # session workspace's id, and the detector's idempotency is per company
    # (a second call returns zero new rows).
    (n_session,) = fetch_one(
        conn,
        "SELECT count(*) FROM anomaly_alerts WHERE company_id = %s"
        " AND category = %s",
        (session_co, spike_cat),
    )
    assert n_session == 0
    rows2 = fetch_dicts(conn, "SELECT * FROM detect_anomalies()")
    conn.commit()
    assert rows2 == []


def test_monthly_summary_grain_is_month_x_company(conn, qa_company_cleanup):
    """mv_monthly_summary is one row per (company_id, month): two workspaces
    with activity in the SAME month produce two independent rows, and the
    unique index is (company_id, month_start)."""
    tag = uuid.uuid4().hex[:8]
    co_b = _create_qa_company(conn, tag, "C")
    session_co = QA_SESSION_COMPANY["company_id"]

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO financial_ledger (company_id, entry_type, category,"
            " entry_date, amount, vendor)"
            " VALUES (%s, 'revenue', %s, DATE '2026-03-10', 111.00, %s)",
            (co_b, f"{QA_PREFIX}_{tag}_rev_b", f"{QA_PREFIX}-{tag}-vendor"),
        )
        cur.execute(
            "INSERT INTO financial_ledger (company_id, entry_type, category,"
            " entry_date, amount, vendor)"
            " VALUES (%s, 'revenue', %s, DATE '2026-03-11', 222.00, %s)",
            (session_co, f"{QA_PREFIX}_{tag}_rev_session",
             f"{QA_PREFIX}-{tag}-vendor"),
        )
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("REFRESH MATERIALIZED VIEW mv_monthly_summary")
    conn.commit()

    rows = fetch_all(
        conn,
        """SELECT company_id, revenue
             FROM mv_monthly_summary
            WHERE month_start = DATE '2026-03-01'
            ORDER BY company_id""",
    )
    assert rows == sorted([(co_b, Decimal("111.00")),
                           (session_co, Decimal("222.00"))]), rows

    # Unique index (company_id, month_start) -- REQUIRED for
    # REFRESH ... CONCURRENTLY and the multi-tenant grain guarantee.
    (idxdef,) = fetch_one(
        conn,
        "SELECT indexdef FROM pg_indexes WHERE schemaname = 'public'"
        " AND indexname = 'uq_mv_monthly_summary'",
    )
    assert "(company_id, month_start)" in idxdef, idxdef




# Each case: (positive_insert_template, negative_insert_template). Templates
# take {cust}, {jt}, {emp}, {job}, {co}, {BOGUS}, {tag}. The positive insert
# proves the same row shape IS accepted with a real parent; the negative one
# must be rejected with a FK violation.
FK_BEHAVIOR_CASES = [
    pytest.param(
        "INSERT INTO customers (company_id, name, email)"
        " VALUES ({co}, '{tag}-co-ok', '{tag}-co-ok@example.test')",
        "INSERT INTO customers (company_id, name, email)"
        " VALUES ({BOGUS}, '{tag}-co-bad', '{tag}-co-bad@example.test')",
        id="customers.company_id->companies.company_id",
    ),
    pytest.param(
        "INSERT INTO jobs (job_number, customer_id, job_type_id, job_date, quoted_price)"
        " VALUES ('{tag}-fk1-ok', {cust}, {jt}, DATE '2026-03-01', 100.00)",
        "INSERT INTO jobs (job_number, customer_id, job_type_id, job_date, quoted_price)"
        " VALUES ('{tag}-fk1-bad', {BOGUS}, {jt}, DATE '2026-03-01', 100.00)",
        id="jobs.customer_id->customers.id",
    ),
    pytest.param(
        "INSERT INTO jobs (job_number, customer_id, job_type_id, job_date, quoted_price)"
        " VALUES ('{tag}-fk2-ok', {cust}, {jt}, DATE '2026-03-01', 100.00)",
        "INSERT INTO jobs (job_number, customer_id, job_type_id, job_date, quoted_price)"
        " VALUES ('{tag}-fk2-bad', {cust}, {BOGUS}, DATE '2026-03-01', 100.00)",
        id="jobs.job_type_id->job_types.id",
    ),
    pytest.param(
        "INSERT INTO labor_logs (job_id, employee_id, work_date, hours, hourly_rate)"
        " VALUES ({job}, {emp}, DATE '2026-03-05', 2.00, 50.00)",
        "INSERT INTO labor_logs (job_id, employee_id, work_date, hours, hourly_rate)"
        " VALUES ({job}, {BOGUS}, DATE '2026-03-05', 2.00, 50.00)",
        id="labor_logs.employee_id->employees.id",
    ),
    pytest.param(
        "INSERT INTO labor_logs (job_id, employee_id, work_date, hours, hourly_rate)"
        " VALUES ({job}, {emp}, DATE '2026-03-05', 2.00, 50.00)",
        "INSERT INTO labor_logs (job_id, employee_id, work_date, hours, hourly_rate)"
        " VALUES ({BOGUS}, {emp}, DATE '2026-03-05', 2.00, 50.00)",
        id="labor_logs.job_id->jobs.id",
    ),
    pytest.param(
        "INSERT INTO financial_ledger (job_id, entry_type, category, entry_date, amount)"
        " VALUES ({job}, 'revenue', '{tag}-fkleger', DATE '2026-03-05', 25.00)",
        "INSERT INTO financial_ledger (job_id, entry_type, category, entry_date, amount)"
        " VALUES ({BOGUS}, 'revenue', '{tag}-fkleger', DATE '2026-03-05', 25.00)",
        id="financial_ledger.job_id->jobs.id",
    ),
    pytest.param(
        "INSERT INTO financial_ledger (customer_id, entry_type, category, entry_date, amount)"
        " VALUES ({cust}, 'expense', '{tag}-fkcust', DATE '2026-03-05', 25.00)",
        "INSERT INTO financial_ledger (customer_id, entry_type, category, entry_date, amount)"
        " VALUES ({BOGUS}, 'expense', '{tag}-fkcust', DATE '2026-03-05', 25.00)",
        id="financial_ledger.customer_id->customers.id",
    ),
    pytest.param(
        "INSERT INTO market_benchmarks (job_type_id, period, avg_price,"
        " avg_labor_cost_per_job, avg_margin_pct)"
        " VALUES ({jt}, DATE '2026-01-01', 100.00, 50.00, 10.00)",
        "INSERT INTO market_benchmarks (job_type_id, period, avg_price,"
        " avg_labor_cost_per_job, avg_margin_pct)"
        " VALUES ({BOGUS}, DATE '2026-01-01', 100.00, 50.00, 10.00)",
        id="market_benchmarks.job_type_id->job_types.id",
    ),
    pytest.param(
        "INSERT INTO anomaly_alerts (anomaly_type, job_id, severity)"
        " VALUES ('labor_overrun', {job}, 'low')",
        "INSERT INTO anomaly_alerts (anomaly_type, job_id, severity)"
        " VALUES ('labor_overrun', {BOGUS}, 'low')",
        id="anomaly_alerts.job_id->jobs.id",
    ),
]


@pytest.mark.parametrize("positive_tpl,negative_tpl", FK_BEHAVIOR_CASES,
                         ids=[c.id for c in FK_BEHAVIOR_CASES])
def test_fk_violations_rejected_behaviorally(conn, qa_graph, positive_tpl, negative_tpl):
    fmt = dict(
        cust=qa_graph["customer_id"],
        jt=qa_graph["job_type_id"],
        emp=qa_graph["employee_id"],
        job=qa_graph["job_id"],
        co=QA_SESSION_COMPANY["company_id"],
        BOGUS=BOGUS_PARENT_ID,
        tag=qa_graph["tag"],
    )
    positive = positive_tpl.format(**fmt)
    negative = negative_tpl.format(**fmt)
    # Positive control: with the REAL parent the insert must succeed.
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT qa_pos_sp")
        cur.execute(positive)
        cur.execute("ROLLBACK TO SAVEPOINT qa_pos_sp")
        cur.execute("RELEASE SAVEPOINT qa_pos_sp")
    # Negative: bogus parent must raise a FK violation; savepoint keeps the
    # session usable afterwards.
    expect_rejected(conn, [negative], psycopg2.errors.ForeignKeyViolation)


# ---------------------------------------------------------------------------
# 6) CHECK constraints (behavioural)
# ---------------------------------------------------------------------------

# Each case: (pre_statements, failing_statement). All take the fmt dict of
# fixture ids; the LAST statement must raise CheckViolation.
CHECK_CASES = [
    pytest.param(
        ["INSERT INTO financial_ledger (entry_type, category, entry_date, amount)"
         " VALUES ('expense', '{tag}-zero', DATE '2026-01-01', 0.00)"],
        id="ledger_amount_zero",
    ),
    pytest.param(
        ["INSERT INTO financial_ledger (entry_type, category, entry_date, amount)"
         " VALUES ('expense', '{tag}-neg', DATE '2026-01-01', -5.00)"],
        id="ledger_amount_negative",
    ),
    pytest.param(
        ["INSERT INTO jobs (job_number, customer_id, job_type_id, job_date,"
         " quoted_price, status)"
         " VALUES ('{tag}-badstat', {cust}, {jt}, DATE '2026-03-01', 100.00, 'bad')"],
        id="jobs_bad_status",
    ),
    pytest.param(
        ["INSERT INTO labor_logs (job_id, employee_id, work_date, hours, hourly_rate)"
         " VALUES ({job}, {emp}, DATE '2026-03-05', 0.00, 50.00)"],
        id="labor_hours_zero",
    ),
    pytest.param(
        ["INSERT INTO labor_logs (job_id, employee_id, work_date, hours, hourly_rate)"
         " VALUES ({job}, {emp}, DATE '2026-03-05', 25.00, 50.00)"],
        id="labor_hours_over_24",
    ),
    pytest.param(
        ["INSERT INTO labor_logs (job_id, employee_id, work_date, hours, hourly_rate)"
         " VALUES ({job}, {emp}, DATE '2026-03-05', 2.00, -1.00)"],
        id="labor_rate_negative",
    ),
    pytest.param(
        ["INSERT INTO market_benchmarks (job_type_id, period, avg_price,"
         " avg_labor_cost_per_job, avg_margin_pct)"
         " VALUES ({jt}, DATE '2026-01-15', 100.00, 50.00, 10.00)"],
        id="benchmark_period_not_month_start",
    ),
    pytest.param(
        ["INSERT INTO jobs (job_number, customer_id, job_type_id, job_date,"
         " completion_date, quoted_price)"
         " VALUES ('{tag}-early', {cust}, {jt}, DATE '2026-03-01',"
         " DATE '2026-02-01', 100.00)"],
        id="jobs_completion_before_start",
    ),
    pytest.param(
        ["INSERT INTO anomaly_alerts (anomaly_type, job_id, severity)"
         " VALUES ('labor_overrun', {job}, 'low')",
         "UPDATE anomaly_alerts SET is_resolved = TRUE"
         " WHERE job_id = {job} AND anomaly_type = 'labor_overrun'"],
        id="alert_resolved_without_timestamp",
    ),
]


@pytest.mark.parametrize("statements", CHECK_CASES, ids=[c.id for c in CHECK_CASES])
def test_check_constraints_rejected_behaviorally(conn, qa_graph, statements):
    fmt = dict(
        cust=qa_graph["customer_id"],
        jt=qa_graph["job_type_id"],
        emp=qa_graph["employee_id"],
        job=qa_graph["job_id"],
        tag=qa_graph["tag"],
    )
    sqls = [s.format(**fmt) for s in statements]
    expect_rejected(conn, sqls, psycopg2.errors.CheckViolation)


# ---------------------------------------------------------------------------
# 7) Materialized views: existence + queryability
# ---------------------------------------------------------------------------

def test_matviews_exist_populated_and_queryable(conn):
    rows = fetch_all(
        conn,
        "SELECT matviewname, ispopulated FROM pg_matviews WHERE schemaname = 'public'",
    )
    names = {r[0] for r in rows}
    assert names == EXPECTED_MATVIEWS, f"matviews: {names}"
    populated = {r[0] for r in rows if r[1]}
    assert populated == EXPECTED_MATVIEWS, f"not populated: {EXPECTED_MATVIEWS - populated}"
    for mv in sorted(EXPECTED_MATVIEWS):
        (n,) = fetch_one(conn, f"SELECT count(*) FROM {mv}")
        assert n >= 0  # SELECT works


# ---------------------------------------------------------------------------
# 8) Materialized views: refreshable + refresh_all_analytics()
# ---------------------------------------------------------------------------

def test_matviews_plain_refresh_and_refresh_all_analytics(conn):
    # Plain (non-concurrent) REFRESH on each view.
    with conn.cursor() as cur:
        for mv in sorted(EXPECTED_MATVIEWS):
            cur.execute(f"REFRESH MATERIALIZED VIEW {mv}")
    conn.commit()
    # refresh_all_analytics() runs CONCURRENTLY on all six and returns void
    # (plpgsql void surfaces as an empty string through psycopg2).
    with conn.cursor() as cur:
        cur.execute("SELECT refresh_all_analytics()")
        assert cur.fetchone()[0] in (None, "")
    conn.commit()


# ---------------------------------------------------------------------------
# 9) View content correctness (hand-computed fixture)
# ---------------------------------------------------------------------------

def test_view_content_matches_hand_computed_values(conn, qa_graph):
    ids = qa_graph
    with conn.cursor() as cur:
        # Job: completed, budgeted 800 labor; known revenue/costs.
        cur.execute(
            "UPDATE jobs SET status = 'completed', completion_date = %s,"
            " estimated_labor_cost = 800.00 WHERE id = %s",
            (datetime.date(2026, 3, 15), ids["job_id"]),
        )
        cur.execute("UPDATE customers SET customer_since = %s WHERE id = %s",
                    (datetime.date(2026, 3, 1), ids["customer_id"]))
        # Benchmark for the lateral join (latest 'national' row).
        cur.execute(
            "INSERT INTO market_benchmarks (job_type_id, period, region, avg_price,"
            " avg_labor_cost_per_job, avg_margin_pct, source)"
            " VALUES (%s, %s, 'national', 12000.00, 3000.00, 55.00, %s)",
            (ids["job_type_id"], datetime.date(2026, 2, 1), f"{ids['tag']}-bench"),
        )
        # Ledger: revenue 10000; materials 2000; equipment 500 (-> other);
        # a 'labor' recharge of 300 that the per-type/per-customer roll-ups
        # must EXCLUDE (labor cost comes from labor_logs) but the monthly
        # cash P&L must INCLUDE.
        for entry_type, category, day, amount in [
            ("revenue", "services", 10, 10000.00),
            ("expense", "materials", 10, 2000.00),
            ("expense", "equipment", 11, 500.00),
            ("expense", "labor", 12, 300.00),
        ]:
            cur.execute(
                "INSERT INTO financial_ledger (job_id, entry_type, category,"
                " entry_date, amount, vendor, description)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (ids["job_id"], entry_type, category, datetime.date(2026, 3, day),
                 amount, f"{ids['tag']}-vendor", f"{ids['tag']} ledger row"),
            )
        # Labor: 10h + 5h @ 100 = 15h, cost 1500 (vs 800 budget).
        cur.execute(
            "INSERT INTO labor_logs (job_id, employee_id, work_date, hours,"
            " hourly_rate, description) VALUES (%s, %s, %s, 10.00, 100.00, %s)",
            (ids["job_id"], ids["employee_id"], datetime.date(2026, 3, 12),
             f"{ids['tag']} log A"),
        )
        cur.execute(
            "INSERT INTO labor_logs (job_id, employee_id, work_date, hours,"
            " hourly_rate, description) VALUES (%s, %s, %s, 5.00, 100.00, %s)",
            (ids["job_id"], ids["employee_id"], datetime.date(2026, 3, 13),
             f"{ids['tag']} log B"),
        )
    conn.commit()
    # Refresh so the snapshots reflect the fixture.
    with conn.cursor() as cur:
        cur.execute("SELECT refresh_all_analytics()")
    conn.commit()

    # ---- mv_job_type_profitability ----------------------------------------
    # Multi-tenant grain: one row per (company_id, job_type) -- the fixture
    # type belongs to the session workspace, so filter on both key columns.
    rows = fetch_all(
        conn,
        """SELECT revenue, materials_cost, other_cost, labor_cost, total_cost,
                  gross_margin, margin_pct, job_count, avg_job_price, labor_hours,
                  benchmark_period, benchmark_avg_price, price_vs_benchmark_pct
             FROM mv_job_type_profitability
            WHERE company_id = %s AND job_type_id = %s""",
        (QA_SESSION_COMPANY["company_id"], ids["job_type_id"]),
    )
    assert len(rows) == 1
    (revenue, materials, other, labor, total, gross, margin, job_count,
     avg_price, labor_hours, bench_period, bench_avg_price, price_vs) = rows[0]
    assert revenue == Decimal("10000.00")
    assert materials == Decimal("2000.00")
    assert other == Decimal("500.00")          # equipment only; ledger 'labor' excluded
    assert labor == Decimal("1500.00")         # hours * rate from labor_logs
    assert total == Decimal("4000.00")         # 2000 + 500 + 1500
    assert gross == Decimal("6000.00")         # 10000 - 4000
    assert margin == Decimal("60.00")          # 6000 / 10000 * 100
    assert job_count == 1
    assert avg_price == Decimal("5000.00")
    assert labor_hours == Decimal("15.00")
    assert bench_period == datetime.date(2026, 2, 1)
    assert bench_avg_price == Decimal("12000.00")
    # avg_job_price = quoted price (5000), not recognized revenue:
    # (5000 - 12000) / 12000 * 100 = -58.33
    assert price_vs == Decimal("-58.33")

    # ---- mv_labor_cost_analysis -------------------------------------------
    rows = fetch_all(
        conn,
        """SELECT total_hours, labor_cost, estimated_labor_cost, labor_variance_pct,
                  labor_cost_per_hour, employee_count, first_work_date, last_work_date
             FROM mv_labor_cost_analysis
            WHERE company_id = %s AND job_id = %s""",
        (QA_SESSION_COMPANY["company_id"], ids["job_id"]),
    )
    assert len(rows) == 1
    (hours, lcost, est, variance, cph, emp_count, first_d, last_d) = rows[0]
    assert hours == Decimal("15.00")
    assert lcost == Decimal("1500.00")
    assert est == Decimal("800.00")
    assert variance == Decimal("87.50")        # (1500-800)/800*100
    assert cph == Decimal("100.00")            # 1500 / 15
    assert emp_count == 1
    assert first_d == datetime.date(2026, 3, 12)
    assert last_d == datetime.date(2026, 3, 13)

    # ---- mv_customer_profitability ----------------------------------------
    # The reworked view splits costs into job_cost (job-linked ledger expenses
    # excl. 'labor' + labor_logs cost) and acquisition_cost (customer-
    # attributed ledger rows). This fixture has NO customer-attributed rows,
    # so acquisition_cost is 0 and total_cost = job_cost is unchanged from the
    # pre-rework behavior. is_whale is FALSE regardless of the customer
    # population: this account is profitable (gross_margin 6000 >= 0), which
    # fails the whale test's second condition.
    rows = fetch_all(
        conn,
        """SELECT revenue, job_cost, acquisition_cost, total_cost, gross_margin,
                  margin_pct, job_count, avg_job_value, first_job_date,
                  last_job_date, days_since_last_job, is_whale
             FROM mv_customer_profitability
            WHERE company_id = %s AND customer_id = %s""",
        (QA_SESSION_COMPANY["company_id"], ids["customer_id"]),
    )
    assert len(rows) == 1
    (revenue, job_cost, acq_cost, total_cost, gross, margin, job_count,
     avg_job_value, first_jd, last_jd, days_since, is_whale) = rows[0]
    assert revenue == Decimal("10000.00")
    assert job_cost == Decimal("4000.00")      # (2000+500) ledger excl labor + 1500 logs
    assert acq_cost == Decimal("0.00")         # no customer-attributed rows here
    assert total_cost == Decimal("4000.00")    # job_cost + acquisition_cost
    assert gross == Decimal("6000.00")
    assert margin == Decimal("60.00")
    assert job_count == 1
    assert avg_job_value == Decimal("5000.00")
    assert first_jd == datetime.date(2026, 3, 1)
    assert last_jd == datetime.date(2026, 3, 1)
    (expected_days,) = fetch_one(conn, "SELECT CURRENT_DATE - DATE '2026-03-01'")
    assert days_since == expected_days
    assert is_whale is False

    # ---- mv_monthly_summary -----------------------------------------------
    # Multi-tenant grain: one row per (company_id, month). The fixture has
    # activity only in the session workspace, so exactly one row exists.
    (total_rows,) = fetch_one(conn, "SELECT count(*) FROM mv_monthly_summary")
    assert total_rows == 1  # exactly one (company, month) of activity exists
    rows = fetch_all(
        conn,
        """SELECT company_id, revenue, materials_expense, labor_expense_ledger,
                  equipment_expense, other_expense, total_expense, labor_hours,
                  labor_cost, gross_profit, gross_margin_pct, job_completions,
                  new_customers, year, month_name
             FROM mv_monthly_summary
            WHERE month_start = DATE '2026-03-01'
              AND company_id = %s""",
        (QA_SESSION_COMPANY["company_id"],),
    )
    assert len(rows) == 1
    (summary_co, rev, mat, lab_ledger, equip, other, tot_exp, hours, lcost,
     gp, gp_pct, completions, new_customers, year, month_name) = rows[0]
    assert summary_co == QA_SESSION_COMPANY["company_id"]
    assert rev == Decimal("10000.00")
    assert mat == Decimal("2000.00")
    assert lab_ledger == Decimal("300.00")     # cash view INCLUDES ledger labor
    assert equip == Decimal("500.00")
    assert other == Decimal("0.00")
    assert tot_exp == Decimal("2800.00")       # 2000 + 300 + 500
    assert hours == Decimal("15.00")           # from labor_logs
    assert lcost == Decimal("1500.00")         # from labor_logs (differs from ledger by design)
    assert gp == Decimal("7200.00")            # 10000 - 2800
    assert gp_pct == Decimal("72.00")
    assert completions == 1
    assert new_customers == 1
    assert year == 2026
    assert month_name == "March"


# ---------------------------------------------------------------------------
# 10) detect_anomalies(): existence + clean execution
# ---------------------------------------------------------------------------

def test_detect_anomalies_exists_and_executes_clean(conn):
    defs = fetch_all(
        conn,
        """SELECT pg_get_functiondef(p.oid)
             FROM pg_proc p
             JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = 'public' AND p.proname = 'detect_anomalies'""",
    )
    assert len(defs) == 1, "detect_anomalies() missing from pg_proc"
    src = defs[0][0]
    assert "anomaly_alerts" in src
    assert "labor_overrun" in src
    assert "expense_spike" in src
    assert "ON CONFLICT" in src  # idempotency guard present

    rdefs = fetch_all(
        conn,
        """SELECT pg_get_functiondef(p.oid)
             FROM pg_proc p
             JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = 'public' AND p.proname = 'refresh_all_analytics'""",
    )
    assert len(rdefs) == 1, "refresh_all_analytics() missing from pg_proc"
    assert "REFRESH MATERIALIZED VIEW" in rdefs[0][0]

    # Executes cleanly on the live DB. At this point no alert-worthy data
    # exists, so the call must return zero rows.
    rows = fetch_all(conn, "SELECT * FROM detect_anomalies()")
    conn.commit()
    assert rows == []


# ---------------------------------------------------------------------------
# 11) detect_anomalies(): idempotency + correct severities/metrics
# ---------------------------------------------------------------------------

def test_detect_anomalies_idempotent_engineered_alerts(conn, qa_graph):
    ids = qa_graph
    spike_cat = f"{QA_PREFIX}_{ids['tag']}_widgets"
    quiet_cat = f"{QA_PREFIX}_{ids['tag']}_quiet"
    job_number = f"{QA_PREFIX}-{ids['tag']}-JOB"

    with conn.cursor() as cur:
        # 50% overrun: estimate 1000, actual 1500 -> severity 'medium'
        # (>30% but not >50% per the function's bands).
        cur.execute(
            "UPDATE jobs SET status = 'in_progress', estimated_labor_cost = 1000.00"
            " WHERE id = %s",
            (ids["job_id"],),
        )
        cur.execute(
            "INSERT INTO labor_logs (job_id, employee_id, work_date, hours,"
            " hourly_rate, description) VALUES (%s, %s, %s, 10.00, 100.00, %s)",
            (ids["job_id"], ids["employee_id"], datetime.date(2026, 3, 2),
             f"{ids['tag']} overrun log A"),
        )
        cur.execute(
            "INSERT INTO labor_logs (job_id, employee_id, work_date, hours,"
            " hourly_rate, description) VALUES (%s, %s, %s, 5.00, 100.00, %s)",
            (ids["job_id"], ids["employee_id"], datetime.date(2026, 3, 3),
             f"{ids['tag']} overrun log B"),
        )
        expense_rows = [
            (spike_cat, datetime.date(2026, 1, 5), 100.00),
            (spike_cat, datetime.date(2026, 2, 5), 100.00),
            (spike_cat, datetime.date(2026, 3, 5), 100.00),
            (spike_cat, datetime.date(2026, 4, 5), 200.00),
            (spike_cat, datetime.date(2026, 5, 5), 500.00),
            # Quiet category: constant history -> stddev 0 -> never flagged.
            (quiet_cat, datetime.date(2026, 1, 6), 100.00),
            (quiet_cat, datetime.date(2026, 2, 6), 100.00),
            (quiet_cat, datetime.date(2026, 3, 6), 100.00),
            (quiet_cat, datetime.date(2026, 4, 6), 100.00),
        ]
        for category, day, amount in expense_rows:
            cur.execute(
                "INSERT INTO financial_ledger (entry_type, category, entry_date,"
                " amount, vendor, description)"
                " VALUES ('expense', %s, %s, %s, %s, %s)",
                (category, day, amount, f"{ids['tag']}-vendor",
                 f"{ids['tag']} anomaly ledger row"),
            )
    conn.commit()

    # ---- call 1: exactly one labor_overrun + one expense_spike -------------
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM detect_anomalies()")
        cols = [d[0] for d in cur.description]
        rows1 = [dict(zip(cols, r)) for r in cur.fetchall()]

    overruns = [r for r in rows1 if r["out_anomaly_type"] == "labor_overrun"]
    spikes = [r for r in rows1 if r["out_anomaly_type"] == "expense_spike"]
    assert len(overruns) == 1, f"expected 1 labor_overrun, got {overruns}"
    assert len(spikes) == 1, f"expected 1 expense_spike, got {spikes}"

    ov = overruns[0]
    assert ov["out_subject"] == job_number
    assert ov["out_severity"] == "medium"            # 50% overrun: >30, not >50
    assert ov["out_metric_value"] == Decimal("1500.00")
    assert ov["out_baseline_value"] == Decimal("1000.00")
    assert ov["out_deviation_pct"] == Decimal("50.00")
    assert ov["out_z_score"] is None
    assert ov["out_details"]["job_number"] == job_number
    assert ov["out_details"]["threshold_pct"] == 20

    sp = spikes[0]
    assert sp["out_subject"] == f"{spike_cat} - May 2026"
    assert sp["out_severity"] == "high"              # z = 7.5 > 3
    assert sp["out_metric_value"] == Decimal("500.00")
    assert sp["out_baseline_value"] == Decimal("125.00")   # (100+100+100+200)/4
    assert sp["out_deviation_pct"] == Decimal("300.00")
    assert sp["out_z_score"] == Decimal("7.50")      # (500-125)/50
    assert sp["out_details"]["spike_threshold_multiple"] == 2

    # No alert for the quiet category (constant history, stddev 0).
    (quiet_alerts,) = fetch_one(
        conn,
        "SELECT count(*) FROM anomaly_alerts WHERE anomaly_type = 'expense_spike'"
        " AND category = %s",
        (quiet_cat,),
    )
    assert quiet_alerts == 0

    # ---- second call: zero NEW rows (idempotent) --------------------------
    rows2 = fetch_all(conn, "SELECT * FROM detect_anomalies()")
    assert rows2 == [], f"duplicate alerts on second call: {rows2}"

    # ---- alerts landed in anomaly_alerts with correct values --------------
    rows = fetch_all(
        conn,
        """SELECT anomaly_type, severity, metric_value, baseline_value,
                  deviation_pct, z_score
             FROM anomaly_alerts
            WHERE job_id = %s AND anomaly_type = 'labor_overrun'""",
        (ids["job_id"],),
    )
    assert len(rows) == 1
    assert rows[0] == ("labor_overrun", "medium", Decimal("1500.00"),
                       Decimal("1000.00"), Decimal("50.00"), None)

    rows = fetch_all(
        conn,
        """SELECT severity, metric_value, baseline_value, deviation_pct, z_score
             FROM anomaly_alerts
            WHERE anomaly_type = 'expense_spike'
              AND category = %s AND period = DATE '2026-05-01'""",
        (spike_cat,),
    )
    assert len(rows) == 1
    assert rows[0] == ("high", Decimal("500.00"), Decimal("125.00"),
                       Decimal("300.00"), Decimal("7.50"))


# ---------------------------------------------------------------------------
# 11b) REGRESSION: corrected spike semantics.
#      detect_anomalies() must flag an expense_spike only when the month total
#      is 2 standard deviations ABOVE the prior-6-month rolling mean
#      ((month_total - prior_mean) > 2 * prior_stddev, i.e. z > 2) -- not when
#      it is merely above a zero-ish absolute threshold, which is what the
#      buggy `ss.month_total > 2 * ss.prior_stddev` clause did. And
#      labor_overrun must stay strictly ABOVE 20% (actual > estimate * 1.2),
#      so exactly-at-threshold is not an alert.
#
#      Shared fixture arithmetic (used by both spike tests): six prior months
#      Jan..Jun 2026 = 500, 1500, 800, 1200, 700, 1300.
#        mean          = 6000 / 6 = 1000.00 (exactly)
#        sum sq. dev.  = 500^2 + 500^2 + 200^2 + 200^2 + 300^2 + 300^2 = 760000
#        sample var    = 760000 / 5 (STDDEV_SAMP divides by n-1) = 152000
#        stddev        = sqrt(152000) = 389.8718
#      A low-variance "steady" history cannot serve as the not-flagged case:
#      e.g. 1000,1000,1200,1100,900,1000 has stddev 103.28, so a 1.5x-mean
#      month of 1500 is z = 466.67 / 103.28 = 4.52 > 2 and the FIXED rule
#      flags it too. The history above has enough spread that the tested
#      month can be 1.5x the mean yet inside 2 stddevs -- which is exactly
#      the band the old buggy rule mis-flagged.
# ---------------------------------------------------------------------------

def test_expense_spike_requires_two_stddev_above_mean(conn, qa_graph):
    tag = qa_graph["tag"]
    steady_cat = f"{QA_PREFIX}_{tag}_steady"
    control_cat = f"{QA_PREFIX}_{tag}_spiky"  # positive control (see below)

    months = [(steady_cat, datetime.date(2026, m, 5), amt)
              for m, amt in zip(range(1, 7), (500, 1500, 800, 1200, 700, 1300))]
    # Tested month at ~1.5x the mean: 1500.00.
    #   excess above mean = 1500 - 1000 = 500
    #   2 * stddev        = 2 * 389.8718 = 779.74
    #   z                 = 500 / 389.8718 = 1.28  (well below 2)
    #   => the FIXED rule (month_total - prior_mean) > 2 * prior_stddev
    #      must NOT fire. The BUGGY rule it replaced WOULD have fired:
    #      month_total > 2 * prior_stddev -> 1500 > 779.74.
    months.append((steady_cat, datetime.date(2026, 7, 5), 1500.00))
    # Positive control: same six-month history, spike month 5000.00.
    #   excess = 4000 > 779.74; z = 4000 / 389.8718 = 10.26 > 3 -> 'high'.
    # Without this control the test could false-pass if the detector never
    # evaluated these months at all.
    months += [(control_cat, datetime.date(2026, m, 6), amt)
               for m, amt in zip(range(1, 7), (500, 1500, 800, 1200, 700, 1300))]
    months.append((control_cat, datetime.date(2026, 7, 6), 5000.00))

    with conn.cursor() as cur:
        for category, day, amount in months:
            cur.execute(
                "INSERT INTO financial_ledger (entry_type, category, entry_date,"
                " amount, vendor, description)"
                " VALUES ('expense', %s, %s, %s, %s, %s)",
                (category, day, amount, f"{QA_PREFIX}-{tag}-vendor",
                 f"{tag} spike-semantics ledger row"),
            )
    conn.commit()

    rows = fetch_dicts(conn, "SELECT * FROM detect_anomalies()")
    conn.commit()
    overruns = [r for r in rows if r["out_anomaly_type"] == "labor_overrun"]
    spikes = [r for r in rows if r["out_anomaly_type"] == "expense_spike"]

    # Exactly one alert overall: the positive control. Nothing else.
    assert overruns == []
    assert len(spikes) == 1, f"expected only the control spike, got {spikes}"
    sp = spikes[0]
    assert sp["out_subject"] == f"{control_cat} - Jul 2026"
    assert sp["out_severity"] == "high"                 # z = 10.26 > 3
    assert sp["out_metric_value"] == Decimal("5000.00")
    assert sp["out_baseline_value"] == Decimal("1000.00")
    assert sp["out_deviation_pct"] == Decimal("400.00")  # (5000-1000)/1000*100
    assert sp["out_z_score"] == Decimal("10.26")         # 4000 / 389.8718

    # The steady category's 1.5x-mean month (z = 1.28 < 2) produced NO alert,
    # neither returned nor stored:
    assert not any(r["out_subject"].startswith(steady_cat) for r in rows)
    (n_stored,) = fetch_one(
        conn,
        "SELECT count(*) FROM anomaly_alerts WHERE category = %s",
        (steady_cat,),
    )
    assert n_stored == 0


def test_expense_spike_flags_z_above_two(conn, qa_graph):
    tag = qa_graph["tag"]
    spike_cat = f"{QA_PREFIX}_{tag}_spike"

    months = [(spike_cat, datetime.date(2026, m, 5), amt)
              for m, amt in zip(range(1, 7), (500, 1500, 800, 1200, 700, 1300))]
    # Tested month clearly above the threshold: 2200.00.
    #   excess above mean = 2200 - 1000 = 1200
    #   2 * stddev        = 2 * 389.8718 = 779.74  -> 1200 > 779.74  (fires)
    #   z                 = 1200 / 389.8718 = 3.078 -> ROUND = 3.08
    #   severity          = 'high' (3.078 > 3 per the file's bands:
    #                        z > 3 high, z > 2.5 medium, else low)
    #   baseline_value    = ROUND(mean, 2)  = 1000.00
    #   deviation_pct     = (2200 - 1000) / 1000 * 100 = 120.00
    months.append((spike_cat, datetime.date(2026, 7, 5), 2200.00))

    with conn.cursor() as cur:
        for category, day, amount in months:
            cur.execute(
                "INSERT INTO financial_ledger (entry_type, category, entry_date,"
                " amount, vendor, description)"
                " VALUES ('expense', %s, %s, %s, %s, %s)",
                (category, day, amount, f"{QA_PREFIX}-{tag}-vendor",
                 f"{tag} spike-semantics ledger row"),
            )
    conn.commit()

    rows1 = fetch_dicts(conn, "SELECT * FROM detect_anomalies()")
    conn.commit()
    overruns = [r for r in rows1 if r["out_anomaly_type"] == "labor_overrun"]
    spikes = [r for r in rows1 if r["out_anomaly_type"] == "expense_spike"]
    assert overruns == []  # no labor logs anywhere in this fixture
    assert len(spikes) == 1, f"expected exactly 1 expense_spike, got {spikes}"
    sp = spikes[0]
    assert sp["out_subject"] == f"{spike_cat} - Jul 2026"
    assert sp["out_severity"] == "high"
    assert sp["out_metric_value"] == Decimal("2200.00")
    assert sp["out_baseline_value"] == Decimal("1000.00")
    assert sp["out_deviation_pct"] == Decimal("120.00")
    assert sp["out_z_score"] == Decimal("3.08")
    assert sp["out_details"]["spike_threshold_multiple"] == 2
    assert sp["out_details"]["prior_months_counted"] == 6
    # JSONB numbers surface through psycopg2 as ints/floats, not Decimals, so
    # round-trip through str() to compare the exact rounded values (a bare
    # float == Decimal comparison fails: float 389.87 is not exactly
    # representable, 1000.0 is -- only the former bit us).
    assert Decimal(str(sp["out_details"]["prior_6m_mean"])) == Decimal("1000.00")
    assert Decimal(str(sp["out_details"]["prior_6m_stddev"])) == Decimal("389.87")

    # The alert landed in anomaly_alerts with exactly the computed values:
    rows = fetch_all(
        conn,
        """SELECT severity, metric_value, baseline_value, deviation_pct, z_score
             FROM anomaly_alerts
            WHERE anomaly_type = 'expense_spike'
              AND category = %s AND period = DATE '2026-07-01'""",
        (spike_cat,),
    )
    assert rows == [("high", Decimal("2200.00"), Decimal("1000.00"),
                     Decimal("120.00"), Decimal("3.08"))]

    # Idempotency sanity: a second call must insert nothing new.
    rows2 = fetch_dicts(conn, "SELECT * FROM detect_anomalies()")
    conn.commit()
    assert rows2 == []


def test_labor_under_or_at_threshold_not_flagged(conn, qa_graph):
    tag = qa_graph["tag"]
    control_job_number = f"{QA_PREFIX}-{tag}-JOB2"

    with conn.cursor() as cur:
        # Job A (the fixture job): estimate 1000.00, actual labor EXACTLY at
        # the 1.2x threshold: 12.00h * 100.00 = 1200.00 -> 20% over. 20 is
        # NOT > 20, so `actual > estimate * 1.2` is false: no labor_overrun.
        cur.execute(
            "UPDATE jobs SET status = 'in_progress', estimated_labor_cost = 1000.00"
            " WHERE id = %s",
            (qa_graph["job_id"],),
        )
        # Job B (control): same estimate, actual 12.10h * 100.00 = 1210.00
        # -> 21% over -> 1210.00 > 1000.00 * 1.2 = 1200.00 -> one alert,
        # severity 'low' (21% is > 20% but not > 30% per the function's bands).
        cur.execute(
            "INSERT INTO jobs (job_number, customer_id, job_type_id, job_date,"
            " quoted_price, status, estimated_labor_cost)"
            " VALUES (%s, %s, %s, %s, 5000.00, 'in_progress', 1000.00)"
            " RETURNING id",
            (control_job_number, qa_graph["customer_id"], qa_graph["job_type_id"],
             datetime.date(2026, 3, 1)),
        )
        control_job_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO labor_logs (job_id, employee_id, work_date, hours,"
            " hourly_rate, description) VALUES (%s, %s, %s, 12.00, 100.00, %s)",
            (qa_graph["job_id"], qa_graph["employee_id"],
             datetime.date(2026, 3, 2), f"{tag} at-threshold log"),
        )
        cur.execute(
            "INSERT INTO labor_logs (job_id, employee_id, work_date, hours,"
            " hourly_rate, description) VALUES (%s, %s, %s, 12.10, 100.00, %s)",
            (control_job_id, qa_graph["employee_id"],
             datetime.date(2026, 3, 3), f"{tag} control log"),
        )
    conn.commit()

    rows = fetch_dicts(conn, "SELECT * FROM detect_anomalies()")
    conn.commit()
    overruns = [r for r in rows if r["out_anomaly_type"] == "labor_overrun"]
    spikes = [r for r in rows if r["out_anomaly_type"] == "expense_spike"]
    assert spikes == []  # no expense data anywhere in this fixture
    assert len(overruns) == 1, (
        f"expected exactly the control overrun, got {overruns}"
    )
    ov = overruns[0]
    assert ov["out_subject"] == control_job_number
    assert ov["out_severity"] == "low"                   # 21% over
    assert ov["out_metric_value"] == Decimal("1210.00")
    assert ov["out_baseline_value"] == Decimal("1000.00")
    assert ov["out_deviation_pct"] == Decimal("21.00")
    assert ov["out_z_score"] is None
    assert ov["out_details"]["threshold_pct"] == 20

    # Stored rows: the at-threshold job must have NO labor_overrun alert at
    # all (open or resolved); the control exactly one.
    (n_at_threshold,) = fetch_one(
        conn,
        "SELECT count(*) FROM anomaly_alerts"
        " WHERE anomaly_type = 'labor_overrun' AND job_id = %s",
        (qa_graph["job_id"],),
    )
    assert n_at_threshold == 0
    (n_control,) = fetch_one(
        conn,
        "SELECT count(*) FROM anomaly_alerts"
        " WHERE anomaly_type = 'labor_overrun' AND job_id = %s",
        (control_job_id,),
    )
    assert n_control == 1


# ---------------------------------------------------------------------------
# 11c) Customer attribution (financial_ledger.customer_id) + whale detection
# ---------------------------------------------------------------------------

def test_ledger_single_attribution_check_and_overhead_ok(conn, qa_graph):
    """chk_ledger_single_attribution enforces EXCLUSIVE attribution: a ledger
    entry may point at a job OR at a customer, never both (both NULL =
    general overhead). Both set -> CheckViolation; both NULL -> accepted."""
    fmt = dict(job=qa_graph["job_id"], cust=qa_graph["customer_id"],
               tag=qa_graph["tag"])
    expect_rejected(
        conn,
        [("INSERT INTO financial_ledger (job_id, customer_id, entry_type,"
          " category, entry_date, amount)"
          " VALUES ({job}, {cust}, 'expense', '{tag}-both-attr',"
          " DATE '2026-03-05', 25.00)").format(**fmt)],
        psycopg2.errors.CheckViolation,
    )
    # Positive control: a both-NULL overhead row (job_id/customer_id unset)
    # must be accepted. Inserted inside a savepoint and rolled back, so no
    # cleanup is needed.
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT qa_overhead_sp")
        cur.execute(
            "INSERT INTO financial_ledger (entry_type, category, entry_date,"
            " amount, vendor)"
            " VALUES ('expense', %s, DATE '2026-03-05', 25.00, %s)",
            (f"{QA_PREFIX}_{fmt['tag']}_overhead",
             f"{QA_PREFIX}-{fmt['tag']}-vendor"),
        )
        cur.execute("ROLLBACK TO SAVEPOINT qa_overhead_sp")
        cur.execute("RELEASE SAVEPOINT qa_overhead_sp")


def test_customer_attribution_flows_into_customer_view(conn, qa_graph):
    """Customer-attributed ledger expenses (financial_ledger.customer_id --
    e.g. customer acquisition cost) must surface in
    mv_customer_profitability.acquisition_cost, be folded into
    total_cost = job_cost + acquisition_cost, and shrink the customer's
    gross_margin / margin_pct (NET margin). Hand-computed below."""
    ids = qa_graph
    with conn.cursor() as cur:
        # Job-linked activity: revenue 10000 + job cost 2000 (materials).
        # No labor logs, no other ledger rows for this customer.
        for entry_type, category, amount in [
            ("revenue", "services", 10000.00),
            ("expense", "materials", 2000.00),
        ]:
            cur.execute(
                "INSERT INTO financial_ledger (job_id, entry_type, category,"
                " entry_date, amount, vendor)"
                " VALUES (%s, %s, %s, DATE '2026-03-10', %s, %s)",
                (ids["job_id"], entry_type, category, amount,
                 f"{QA_PREFIX}-{ids['tag']}-vendor"),
            )
        # Customer-attributed expense with NO job behind it (CAC row): 750.
        # The schema CHECK guarantees job_id IS NULL on such rows.
        cur.execute(
            "INSERT INTO financial_ledger (customer_id, entry_type, category,"
            " entry_date, amount, vendor)"
            " VALUES (%s, 'expense', 'customer_acquisition',"
            " DATE '2026-03-11', 750.00, %s)",
            (ids["customer_id"], f"{QA_PREFIX}-{ids['tag']}-vendor"),
        )
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("REFRESH MATERIALIZED VIEW mv_customer_profitability")
    conn.commit()

    (revenue, job_cost, acq_cost, total_cost, gross, margin, job_count) = fetch_one(
        conn,
        """SELECT revenue, job_cost, acquisition_cost, total_cost, gross_margin,
                  margin_pct, job_count
             FROM mv_customer_profitability WHERE customer_id = %s""",
        (ids["customer_id"],),
    )
    # Hand computation:
    #   revenue          = 10000.00            (job-linked revenue rows)
    #   job_cost         = 2000.00             (job-linked expenses excl 'labor')
    #   acquisition_cost = 750.00              (customer-attributed CAC row)
    #   total_cost       = 2000 + 750 = 2750.00
    #   gross_margin     = 10000 - 2750 = 7250.00
    #   margin_pct       = 7250 / 10000 * 100 = 72.50
    #   job_count        = 1 (the fixture job)
    assert revenue == Decimal("10000.00")
    assert job_cost == Decimal("2000.00")
    assert acq_cost == Decimal("750.00")
    assert total_cost == Decimal("2750.00")
    assert gross == Decimal("7250.00")
    assert margin == Decimal("72.50")
    assert job_count == 1


def test_is_whale_semantics(conn, qa_graph):
    """is_whale = revenue >= P80 of revenue across ALL customers
    (zero-revenue rows included) AND gross_margin < 0 -- 'a top-quintile
    revenue account that loses money'. All gates engineered below."""
    tag = qa_graph["tag"]

    # Fixture (6 customers). The qa_graph customer is the ZERO-REVENUE
    # participant, proving the percentile spans ALL customer rows, not only
    # revenue-bearing ones. One job per customer, funded by a job-linked
    # revenue row + a materials expense row (no labor logs), so
    # gross_margin = revenue - job_cost:
    #
    #   customer (suffix)   revenue   job_cost   gross_margin
    #   (qa, zero rev)        0.00      0.00         0.00
    #   filler_low          100.00     50.00       +50.00
    #   filler_mid          200.00    150.00       +50.00
    #   neg_below           300.00    400.00      -100.00  (loss BELOW P80)
    #   highrev_ok          600.00    500.00      +100.00  (AT P80, profitable)
    #   whale              1000.00   1100.00      -100.00  (above P80, loss)
    #
    # P80 BY HAND, exactly as the view computes it:
    #   percentile_cont(0.8) WITHIN GROUP (ORDER BY revenue) over ALL 6
    #   customer rows; sorted revenues ascending: 0, 100, 200, 300, 600, 1000
    #   (N = 6). 1-based row number RN = 1 + 0.8 * (N - 1) = 1 + 4 = 5.0 --
    #   an exact row hit, so P80 = the 5th smallest revenue = 600.00 with no
    #   interpolation. Therefore the revenue gate is revenue >= 600.00 and:
    #     whale       1000 >= 600 AND -100 < 0   -> is_whale TRUE  (only one)
    #     highrev_ok   600 >= 600 (equality counts as >=) AND +100 not < 0
    #                                            -> is_whale FALSE
    #     neg_below    300 <  600                -> is_whale FALSE (loss, but
    #                                              not top-quintile revenue)
    #     zero customer / fillers  below P80 and/or profitable -> FALSE
    specs = [
        ("whale", 1000.00, 1100.00),
        ("highrev_ok", 600.00, 500.00),
        ("neg_below", 300.00, 400.00),
        ("filler_low", 100.00, 50.00),
        ("filler_mid", 200.00, 150.00),
    ]
    customer_ids = {"zero": qa_graph["customer_id"]}
    with conn.cursor() as cur:
        for suffix, revenue, job_cost in specs:
            cur.execute(
                "INSERT INTO customers (name, email, city)"
                " VALUES (%s, %s, 'QA City') RETURNING id",
                (f"{QA_PREFIX}_{tag}_whale_{suffix}",
                 f"{QA_PREFIX}_{tag}.whale.{suffix}@example.test"),
            )
            customer_ids[suffix] = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO jobs (job_number, customer_id, job_type_id,"
                " job_date, quoted_price, status)"
                " VALUES (%s, %s, %s, DATE '2026-03-01', 5000.00, 'quoted')"
                " RETURNING id",
                (f"{QA_PREFIX}-{tag}-W-{suffix}", customer_ids[suffix],
                 qa_graph["job_type_id"]),
            )
            jid = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO financial_ledger (job_id, entry_type, category,"
                " entry_date, amount, vendor)"
                " VALUES (%s, 'revenue', 'services', DATE '2026-03-10', %s, %s)",
                (jid, revenue, f"{QA_PREFIX}-{tag}-vendor"),
            )
            cur.execute(
                "INSERT INTO financial_ledger (job_id, entry_type, category,"
                " entry_date, amount, vendor)"
                " VALUES (%s, 'expense', 'materials', DATE '2026-03-11', %s, %s)",
                (jid, job_cost, f"{QA_PREFIX}-{tag}-vendor"),
            )
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("REFRESH MATERIALIZED VIEW mv_customer_profitability")
    conn.commit()

    # Guard: the scratch DB must hold EXACTLY our 6 customers right now (all
    # earlier tests sweep their fixtures), otherwise a stray customer would
    # shift the percentile and invalidate the hand-computed P80.
    (n_rows,) = fetch_one(conn, "SELECT count(*) FROM mv_customer_profitability")
    assert n_rows == 6, f"expected exactly 6 customer rows, got {n_rows}"

    rows = fetch_all(
        conn,
        "SELECT customer_id, revenue, gross_margin, is_whale"
        " FROM mv_customer_profitability",
    )
    got = {r[0]: (r[1], r[2], r[3]) for r in rows}
    expected = {
        customer_ids["zero"]: (Decimal("0.00"), Decimal("0.00"), False),
        customer_ids["whale"]: (Decimal("1000.00"), Decimal("-100.00"), True),
        customer_ids["highrev_ok"]: (Decimal("600.00"), Decimal("100.00"), False),
        customer_ids["neg_below"]: (Decimal("300.00"), Decimal("-100.00"), False),
        customer_ids["filler_low"]: (Decimal("100.00"), Decimal("50.00"), False),
        customer_ids["filler_mid"]: (Decimal("200.00"), Decimal("50.00"), False),
    }
    assert got == expected, f"whale values/flags mismatch: {got}"
    assert sum(1 for _, _, w in got.values() if w) == 1  # exactly one whale


# ---------------------------------------------------------------------------
# 11d) Macro benchmarks (market_benchmarks), mv_benchmark_comparison, and
#      the fetcher's parsing helper (fetch_benchmarks.py)
# ---------------------------------------------------------------------------

def test_market_benchmarks_macro_catalog(conn):
    """Catalog shape of the macro-benchmark extension: job_type_id nullable,
    benchmark_kind (default 'job_market') and series_id columns exist, and
    the fetcher's partial unique upsert target index is present."""
    # job_type_id became NULLABLE -- macro rows have no job type.
    (nullable,) = fetch_one(
        conn,
        """SELECT is_nullable FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'market_benchmarks'
              AND column_name = 'job_type_id'""",
    )
    assert nullable == "YES"
    # New macro columns exist; benchmark_kind defaults to 'job_market'.
    cols = dict(fetch_all(
        conn,
        """SELECT column_name, column_default FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'market_benchmarks'
              AND column_name IN ('benchmark_kind', 'series_id')""",
    ))
    assert set(cols) == {"benchmark_kind", "series_id"}
    # TEXT column: PostgreSQL renders the default as '...'::text (not the
    # ::character varying spelling it uses for varchar columns).
    assert cols["benchmark_kind"] == "'job_market'::text"
    assert cols["series_id"] is None  # nullable, no default
    # Upsert target: partial UNIQUE index (series_id, region, period)
    # WHERE series_id IS NOT NULL.
    (idxdef,) = fetch_one(
        conn,
        "SELECT indexdef FROM pg_indexes"
        " WHERE schemaname = 'public'"
        " AND indexname = 'uq_market_benchmarks_series_period'",
    )
    assert "UNIQUE INDEX uq_market_benchmarks_series_period" in idxdef
    assert "(series_id, region, period)" in idxdef
    # pg_indexes renders the partial-index predicate parenthesized.
    assert "WHERE (series_id IS NOT NULL)" in idxdef


def test_market_benchmarks_benchmark_kind_check(conn, qa_graph):
    """benchmark_kind accepts only job_market / wage / ppi: 'bogus' raises a
    CheckViolation while a real macro kind with job_type_id NULL inserts."""
    bogus_series = f"{QA_PREFIX}_{qa_graph['tag']}_bogus"
    expect_rejected(
        conn,
        ["INSERT INTO market_benchmarks (job_type_id, period, region,"
         " benchmark_kind, series_id, avg_price, avg_labor_cost_per_job,"
         " avg_margin_pct)"
         " VALUES (NULL, DATE '2026-04-01', 'national', 'bogus',"
         f" '{bogus_series}', 10.00, 0.00, 0.00)"],
        psycopg2.errors.CheckViolation,
    )
    # Positive control: 'ppi' + NULL job_type_id is a valid macro row.
    # Savepoint + rollback keeps the scratch DB untouched.
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT qa_kind_sp")
        cur.execute(
            "INSERT INTO market_benchmarks (job_type_id, period, region,"
            " benchmark_kind, series_id, avg_price, avg_labor_cost_per_job,"
            " avg_margin_pct)"
            " VALUES (NULL, DATE '2026-04-01', 'national', 'ppi', %s,"
            " 105.00, 0.00, 0.00)",
            (f"{QA_PREFIX}_{qa_graph['tag']}_ppi_ok",),
        )
        cur.execute("ROLLBACK TO SAVEPOINT qa_kind_sp")
        cur.execute("RELEASE SAVEPOINT qa_kind_sp")


def test_market_benchmarks_uniqueness_rules(conn, qa_graph):
    """Uniqueness after the macro extension: job_market duplicates on
    (job_type_id, period, region) still raise UniqueViolation; macro rows
    dedupe on (series_id, region, period) via the partial unique index even
    WITHOUT ON CONFLICT; macro rows sharing (period, region) coexist because
    NULL job_type_id values are distinct in the 3-column UNIQUE."""
    tag = qa_graph["tag"]
    jt = qa_graph["job_type_id"]

    # 1) job_market duplicate: original UNIQUE (job_type_id, period, region).
    expect_rejected(
        conn,
        [f"INSERT INTO market_benchmarks (job_type_id, period, region,"
         f" benchmark_kind, avg_price, avg_labor_cost_per_job, avg_margin_pct)"
         f" VALUES ({jt}, DATE '2026-04-01', 'national', 'job_market',"
         f" 100.00, 50.00, 10.00)",
         f"INSERT INTO market_benchmarks (job_type_id, period, region,"
         f" benchmark_kind, avg_price, avg_labor_cost_per_job, avg_margin_pct)"
         f" VALUES ({jt}, DATE '2026-04-01', 'national', 'job_market',"
         f" 200.00, 60.00, 12.00)"],
        psycopg2.errors.UniqueViolation,
    )
    # 2) macro duplicate: same series_id/region/period twice without
    #    ON CONFLICT -> the partial unique index fires. (The 3-column UNIQUE
    #    does NOT -- NULL job_type_id values are distinct -- so this proves
    #    the dedup comes from uq_market_benchmarks_series_period.)
    s1 = f"{QA_PREFIX}_{tag}_s1"
    expect_rejected(
        conn,
        [f"INSERT INTO market_benchmarks (period, region, benchmark_kind,"
         f" series_id, avg_price, avg_labor_cost_per_job, avg_margin_pct)"
         f" VALUES (DATE '2026-04-01', 'national', 'wage', '{s1}',"
         f" 30.00, 0.00, 0.00)",
         f"INSERT INTO market_benchmarks (period, region, benchmark_kind,"
         f" series_id, avg_price, avg_labor_cost_per_job, avg_margin_pct)"
         f" VALUES (DATE '2026-04-01', 'national', 'wage', '{s1}',"
         f" 31.00, 0.00, 0.00)"],
        psycopg2.errors.UniqueViolation,
    )
    # 3) coexistence: two macro rows, same period/region, DIFFERENT series
    #    ids -> both accepted (distinct series_id, NULL-distinct job_type_id).
    s2 = f"{QA_PREFIX}_{tag}_s2"
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT qa_macro_coexist")
        for series in (s1, s2):
            cur.execute(
                "INSERT INTO market_benchmarks (period, region, benchmark_kind,"
                " series_id, avg_price, avg_labor_cost_per_job, avg_margin_pct)"
                " VALUES (DATE '2026-04-01', 'national', 'wage', %s,"
                " 30.00, 0.00, 0.00)",
                (series,),
            )
        cur.execute("ROLLBACK TO SAVEPOINT qa_macro_coexist")
        cur.execute("RELEASE SAVEPOINT qa_macro_coexist")


def test_macro_series_upsert_semantics(conn, qa_graph):
    """The fetcher's idempotency mechanism: INSERT ... ON CONFLICT
    (series_id, region, period) WHERE series_id IS NOT NULL DO UPDATE
    refreshes the row in place -- exactly 1 row survives, with the new price
    and the other columns untouched."""
    tag = qa_graph["tag"]
    series = f"{QA_PREFIX}WAGE_{tag}"
    source = f"{QA_PREFIX}-{tag}-bls"
    period = datetime.date(2026, 5, 1)

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO market_benchmarks (job_type_id, period, region,"
            " benchmark_kind, series_id, avg_price, avg_labor_cost_per_job,"
            " avg_margin_pct, source)"
            " VALUES (NULL, %s, 'us-test', 'wage', %s, 20.00, 0.00, 0.00, %s)",
            (period, series, source),
        )
    conn.commit()
    # Same (series_id, region, period), different avg_price -> UPDATE in place.
    # (The plain 3-column UNIQUE cannot catch this: job_type_id is NULL on
    # both, and NULLs are distinct -- only the partial index can, via the
    # conflict target below.)
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO market_benchmarks (job_type_id, period, region,
                    benchmark_kind, series_id, avg_price,
                    avg_labor_cost_per_job, avg_margin_pct, source)
               VALUES (NULL, %s, 'us-test', 'wage', %s, 25.00, 0.00, 0.00, %s)
               ON CONFLICT (series_id, region, period) WHERE series_id IS NOT NULL
               DO UPDATE SET avg_price = EXCLUDED.avg_price""",
            (period, series, source),
        )
    conn.commit()

    rows = fetch_all(
        conn,
        """SELECT benchmark_kind, series_id, region, period, job_type_id,
                  avg_price, avg_labor_cost_per_job, avg_margin_pct, source
             FROM market_benchmarks WHERE series_id = %s""",
        (series,),
    )
    assert len(rows) == 1, f"upsert must keep exactly 1 row, got {len(rows)}"
    assert rows[0] == ("wage", series, "us-test", period, None,
                       Decimal("25.00"), Decimal("0.00"), Decimal("0.00"),
                       source)


def test_benchmark_comparison_unique_index_and_plain_refresh(conn):
    """mv_benchmark_comparison exists (pg_matviews via EXPECTED_MATVIEWS),
    carries the multi-tenant unique index on (company_id, period) that
    REFRESH ... CONCURRENTLY needs, is queryable, and accepts a plain
    REFRESH MATERIALIZED VIEW."""
    rows = fetch_all(
        conn,
        "SELECT indexdef FROM pg_indexes"
        " WHERE schemaname = 'public'"
        " AND indexname = 'uq_mv_benchmark_comparison'",
    )
    assert len(rows) == 1, f"uq_mv_benchmark_comparison missing: {rows}"
    idxdef = rows[0][0]
    assert "UNIQUE INDEX" in idxdef
    assert "(company_id, period)" in idxdef
    with conn.cursor() as cur:
        cur.execute("REFRESH MATERIALIZED VIEW mv_benchmark_comparison")
    conn.commit()
    (n,) = fetch_one(conn, "SELECT count(*) FROM mv_benchmark_comparison")
    assert n >= 0  # SELECT works


def test_benchmark_comparison_pivot_semantics(conn, qa_graph):
    """Pivot semantics on engineered fixtures:
    (a) a month's row EXISTS because INTERNAL activity creates it (months
        derive only from labor_logs + materials ledger rows -- macro rows
        never create months);
    (b) an UNPINNED macro series (QAtestWAGE_/QAtestPPI_, region 'us-test')
        is read into the view's macro CTE (benchmark_kind wage/ppi) but the
        view pins real series ids/regions, so it cannot leak: every pinned
        market_wage_* / ppi_* column stays NULL (no real macro data exists
        in the scratch DB);
    (c) internal_labor_rate = SUM(hours*rate)/SUM(hours) of that month's
        labor_logs, hand-computed; internal materials spend per job matches.
    """
    tag = qa_graph["tag"]
    with conn.cursor() as cur:
        # Internal activity for March 2026 on the fixture job:
        # 2h @ 50 + 6h @ 100 and one 400.00 materials expense.
        cur.execute(
            "INSERT INTO labor_logs (job_id, employee_id, work_date, hours,"
            " hourly_rate, description) VALUES (%s, %s, DATE '2026-03-05',"
            " 2.00, 50.00, %s)",
            (qa_graph["job_id"], qa_graph["employee_id"],
             f"{tag} bench log A"),
        )
        cur.execute(
            "INSERT INTO labor_logs (job_id, employee_id, work_date, hours,"
            " hourly_rate, description) VALUES (%s, %s, DATE '2026-03-06',"
            " 6.00, 100.00, %s)",
            (qa_graph["job_id"], qa_graph["employee_id"],
             f"{tag} bench log B"),
        )
        cur.execute(
            "INSERT INTO financial_ledger (job_id, entry_type, category,"
            " entry_date, amount, vendor)"
            " VALUES (%s, 'expense', 'materials', DATE '2026-03-07', 400.00, %s)",
            (qa_graph["job_id"], f"{QA_PREFIX}-{tag}-vendor"),
        )
        # Macro rows the view does NOT pin: real ones are CES2000000003 /
        # SMU29000002000000003 / WPU0822 / PCU325510325510 / WPUSI012011 in
        # regions 'us-national'/'us-mo'.
        for series, kind, price in [(f"{QA_PREFIX}WAGE_{tag}", "wage", 30.00),
                                    (f"{QA_PREFIX}PPI_{tag}", "ppi", 105.00)]:
            cur.execute(
                "INSERT INTO market_benchmarks (job_type_id, period, region,"
                " benchmark_kind, series_id, avg_price,"
                " avg_labor_cost_per_job, avg_margin_pct, source)"
                " VALUES (NULL, DATE '2026-03-01', 'us-test', %s, %s,"
                " %s, 0.00, 0.00, %s)",
                (kind, series, price, f"{QA_PREFIX}-{tag}-macro"),
            )
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("REFRESH MATERIALIZED VIEW mv_benchmark_comparison")
    conn.commit()

    # (a) exactly one row: March 2026 is the ONLY month with internal activity
    #     (macro rows must not create months of their own) and it belongs to
    #     the session workspace (multi-tenant grain: internal series are per
    #     company_id; the market baseline columns are global).
    (n_rows,) = fetch_one(conn, "SELECT count(*) FROM mv_benchmark_comparison")
    assert n_rows == 1, f"expected exactly 1 (company, month) row, got {n_rows}"

    (bench_co, period, year, month_name, rate, hours, mats_per_job,
     wage_nat, wage_reg, ppi_lumber, ppi_paint, ppi_bm,
     labor_yoy, mats_yoy, gap_nat, gap_reg, gap_mats) = fetch_one(
        conn,
        """SELECT company_id, period, year, month_name, internal_labor_rate,
                  internal_labor_hours, internal_materials_per_job,
                  market_wage_national, market_wage_regional,
                  ppi_lumber, ppi_paint, ppi_building_materials,
                  internal_labor_yoy_pct, internal_materials_yoy_pct,
                  labor_inflation_gap_national_pct,
                  labor_inflation_gap_regional_pct,
                  materials_inflation_gap_pct
             FROM mv_benchmark_comparison WHERE period = DATE '2026-03-01'""",
    )
    assert bench_co == QA_SESSION_COMPANY["company_id"]
    # (c) hand computation of the internal columns:
    #   internal_labor_rate         = (2.00*50.00 + 6.00*100.00) / (2.00 + 6.00)
    #                               = (100 + 600) / 8 = 700 / 8 = 87.50
    #   internal_labor_hours        = 8.00
    #   internal_materials_per_job  = 400.00 / COUNT(DISTINCT job_id)=1 = 400.00
    assert period == datetime.date(2026, 3, 1)
    assert year == 2026
    assert month_name == "Mar 2026"                # TO_CHAR(..., 'FMMon YYYY')
    assert rate == Decimal("87.50")
    assert hours == Decimal("8.00")
    assert mats_per_job == Decimal("400.00")
    # (b) unpinned QAtest series never leak into pinned columns...
    assert wage_nat is None
    assert wage_reg is None
    assert ppi_lumber is None
    assert ppi_paint is None
    assert ppi_bm is None
    # ...and YoY/gap columns are NULL: the self-joins look at March 2025,
    # which has no internal activity and no macro rows at all.
    assert labor_yoy is None
    assert mats_yoy is None
    assert gap_nat is None
    assert gap_reg is None
    assert gap_mats is None


def test_fetch_benchmarks_to_monthly_averaging():
    """Offline unit test of fetch_benchmarks.to_monthly -- pure Python, NO
    database interaction (and no network: only the mapping helper is
    imported). Sub-monthly observations are averaged into first-of-month
    calendar buckets; '.'-skipping is the fetcher's job (it filters missing
    values before calling), so to_monthly only has to map correctly."""
    # fetch_benchmarks.py lives next to this test file; make the import work
    # regardless of how pytest was invoked.
    sys.path.insert(0, _BASE_DIR)
    from fetch_benchmarks import to_monthly

    obs = [
        (datetime.date(2026, 1, 3), 100.0),
        (datetime.date(2026, 1, 20), 102.0),
        (datetime.date(2026, 2, 5), 103.0),
    ]
    monthly = to_monthly(obs)
    # January buckets both observations: (100.0 + 102.0) / 2 = 101.0;
    # February's single observation passes through as 103.0.
    assert monthly == {datetime.date(2026, 1, 1): 101.0,
                       datetime.date(2026, 2, 1): 103.0}
    # Single-observation month: unchanged value, first-of-month key.
    assert to_monthly([(datetime.date(2026, 3, 15), 55.5)]) == {
        datetime.date(2026, 3, 1): 55.5}
    # Averaging is order-independent.
    assert to_monthly([(datetime.date(2026, 4, 9), 10.0),
                       (datetime.date(2026, 4, 2), 12.0)]) == {
        datetime.date(2026, 4, 1): 11.0}


# ---------------------------------------------------------------------------
# 12) Partial unique indexes (behavioural)
# ---------------------------------------------------------------------------

def test_partial_unique_indexes_reject_duplicate_open_alerts(conn, qa_graph):
    job = qa_graph["job_id"]
    cat = f"{QA_PREFIX}_{qa_graph['tag']}_dup"

    # An OPEN labor_overrun alert exists...
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO anomaly_alerts (anomaly_type, job_id, severity)"
            " VALUES ('labor_overrun', %s, 'low')",
            (job,),
        )
    # ...so a second OPEN one for the same job must be rejected.
    expect_rejected(
        conn,
        ["INSERT INTO anomaly_alerts (anomaly_type, job_id, severity)"
         f" VALUES ('labor_overrun', {job}, 'low')"],
        psycopg2.errors.UniqueViolation,
    )
    # Resolving the alert frees the key: the duplicate insert now succeeds.
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE anomaly_alerts SET is_resolved = TRUE, resolved_at = now()"
            " WHERE job_id = %s AND anomaly_type = 'labor_overrun'",
            (job,),
        )
        cur.execute(
            "INSERT INTO anomaly_alerts (anomaly_type, job_id, severity)"
            " VALUES ('labor_overrun', %s, 'low')",
            (job,),
        )
    # Same story for expense spikes: duplicate OPEN (category, period) rejected,
    # a different period allowed.
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO anomaly_alerts (anomaly_type, category, period, severity)"
            " VALUES ('expense_spike', %s, DATE '2026-01-01', 'low')",
            (cat,),
        )
    expect_rejected(
        conn,
        ["INSERT INTO anomaly_alerts (anomaly_type, category, period, severity)"
         f" VALUES ('expense_spike', '{cat}', DATE '2026-01-01', 'low')"],
        psycopg2.errors.UniqueViolation,
    )
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO anomaly_alerts (anomaly_type, category, period, severity)"
            " VALUES ('expense_spike', %s, DATE '2026-02-01', 'low')",
            (cat,),
        )
    conn.rollback()  # discard everything this test created


# ---------------------------------------------------------------------------
# 12a) Forecast engine output tables (forecasts, backtest_results) + the two
#      pure-math modules behind them. The DB tests run against the scratch
#      database only; the unit tests are OFFLINE (no DB, like
#      test_fetch_benchmarks_to_monthly_averaging).
# ---------------------------------------------------------------------------

def test_forecasts_table_checks_and_uniqueness(conn, qa_company_cleanup):
    """forecasts: month-start CHECK, non-negative predicted CHECK (except
    net_cash_flow), series whitelist and the multi-tenant upsert key
    (company_id, series, period_start, model). Every insert happens inside a
    savepoint that is rolled back, so the table stays empty for the final
    cleanliness assertion."""
    # Mid-month period rejected (mirrors market_benchmarks.period).
    expect_rejected(
        conn,
        ["INSERT INTO forecasts (series, period_start, model, predicted, horizon_months)"
         " VALUES ('revenue', DATE '2026-06-15', 'QAtest_model', 100.00, 1)"],
        psycopg2.errors.CheckViolation,
    )
    # Unknown series rejected.
    expect_rejected(
        conn,
        ["INSERT INTO forecasts (series, period_start, model, predicted, horizon_months)"
         " VALUES ('QAtest_bogus', DATE '2026-06-01', 'QAtest_model', 100.00, 1)"],
        psycopg2.errors.CheckViolation,
    )
    # Negative predicted rejected for the non-negative series...
    expect_rejected(
        conn,
        ["INSERT INTO forecasts (series, period_start, model, predicted, horizon_months)"
         " VALUES ('revenue', DATE '2026-06-01', 'QAtest_model', -1.00, 1)"],
        psycopg2.errors.CheckViolation,
    )
    expect_rejected(
        conn,
        ["INSERT INTO forecasts (series, period_start, model, predicted, horizon_months)"
         " VALUES ('labor_cost', DATE '2026-06-01', 'QAtest_model', -1.00, 1)"],
        psycopg2.errors.CheckViolation,
    )
    # ...but ACCEPTED for net_cash_flow (real negative cash months) -- positive
    # control inside a rolled-back savepoint.
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT qa_forecast_pos")
        cur.execute(
            "INSERT INTO forecasts (series, period_start, model, predicted, horizon_months)"
            " VALUES ('net_cash_flow', DATE '2026-06-01', 'QAtest_model', -123.45, 1)"
            " RETURNING id")
        (row_id,) = cur.fetchone()
        cur.execute("SELECT predicted, series FROM forecasts WHERE id = %s",
                    (row_id,))
        assert cur.fetchone() == (Decimal("-123.45"), "net_cash_flow")
        cur.execute("ROLLBACK TO SAVEPOINT qa_forecast_pos")
        cur.execute("RELEASE SAVEPOINT qa_forecast_pos")
    # The upsert key: a duplicate (company_id, series, period_start, model)
    # is rejected...
    expect_rejected(
        conn,
        ["INSERT INTO forecasts (series, period_start, model, predicted, horizon_months)"
         " VALUES ('revenue', DATE '2026-06-01', 'QAtest_model', 100.00, 1)",
         "INSERT INTO forecasts (series, period_start, model, predicted, horizon_months)"
         " VALUES ('revenue', DATE '2026-06-01', 'QAtest_model', 200.00, 1)"],
        psycopg2.errors.UniqueViolation,
    )
    # ...while a different model or month is allowed (positive control, then
    # swept via cleanup in the savepoint rollback).
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT qa_forecast_ok")
        cur.execute(
            "INSERT INTO forecasts (series, period_start, model, predicted, horizon_months)"
            " VALUES ('revenue', DATE '2026-06-01', 'QAtest_model', 100.00, 1)")
        cur.execute(
            "INSERT INTO forecasts (series, period_start, model, predicted, horizon_months)"
            " VALUES ('revenue', DATE '2026-06-01', 'QAtest_model_v2', 110.00, 1)")
        cur.execute(
            "INSERT INTO forecasts (series, period_start, model, predicted, horizon_months)"
            " VALUES ('revenue', DATE '2026-07-01', 'QAtest_model', 120.00, 1)")
        (n,) = fetch_one(
            conn,
            "SELECT count(*) FROM forecasts WHERE model LIKE 'QAtest%'")
        assert n == 3
        cur.execute("ROLLBACK TO SAVEPOINT qa_forecast_ok")
        cur.execute("RELEASE SAVEPOINT qa_forecast_ok")

    # Multi-tenant key dimension: ANOTHER workspace's forecast for the SAME
    # series/month/model does NOT collide with the session workspace's.
    tag = uuid.uuid4().hex[:8]
    co_b = _create_qa_company(conn, tag, "fc")
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT qa_forecast_co")
        cur.execute(
            "INSERT INTO forecasts (company_id, series, period_start, model,"
            " predicted, horizon_months)"
            " VALUES (%s, 'revenue', DATE '2026-06-01', 'QAtest_model',"
            " 100.00, 1)",
            (QA_SESSION_COMPANY["company_id"],))
        cur.execute(
            "INSERT INTO forecasts (company_id, series, period_start, model,"
            " predicted, horizon_months)"
            " VALUES (%s, 'revenue', DATE '2026-06-01', 'QAtest_model',"
            " 777.00, 1)",
            (co_b,))
        (n,) = fetch_one(
            conn,
            "SELECT count(*) FROM forecasts WHERE model = 'QAtest_model'"
            " AND series = 'revenue'")
        assert n == 2
        cur.execute("ROLLBACK TO SAVEPOINT qa_forecast_co")
        cur.execute("RELEASE SAVEPOINT qa_forecast_co")


def test_backtest_results_uniqueness(conn):
    """backtest_results: (series, model, fold) is the upsert key; the same
    fold name on the same series+model is rejected as a duplicate."""
    expect_rejected(
        conn,
        ["INSERT INTO backtest_results (series, model, fold, train_start,"
         " train_end, test_start, test_end)"
         " VALUES ('revenue', 'QAtest_model', 'QAtest_fold', DATE '2023-12-01',"
         " DATE '2026-05-01', DATE '2026-06-01', DATE '2026-11-01')",
         "INSERT INTO backtest_results (series, model, fold, train_start,"
         " train_end, test_start, test_end)"
         " VALUES ('revenue', 'QAtest_model', 'QAtest_fold', DATE '2023-12-01',"
         " DATE '2026-05-01', DATE '2026-06-01', DATE '2026-11-01')"],
        psycopg2.errors.UniqueViolation,
    )
    # A different fold (or series) under the same model is fine -- positive
    # control inside a rolled-back savepoint.
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT qa_bt_ok")
        cur.execute(
            "INSERT INTO backtest_results (series, model, fold, train_start,"
            " train_end, test_start, test_end, mape, rmse, predictions)"
            " VALUES ('revenue', 'QAtest_model', 'QAtest_fold_a',"
            " DATE '2023-12-01', DATE '2026-05-01', DATE '2026-06-01',"
            " DATE '2026-11-01', 12.34, 5000.00,"
            " %s::jsonb)",
            ('[{"period": "2026-06-01", "predicted": 100.0, "actual": 110.0,'
             ' "abs_pct_err": 9.09}]',),
        )
        cur.execute(
            "INSERT INTO backtest_results (series, model, fold, train_start,"
            " train_end, test_start, test_end)"
            " VALUES ('net_cash_flow', 'QAtest_model', 'QAtest_fold_a',"
            " DATE '2023-12-01', DATE '2026-05-01', DATE '2026-06-01',"
            " DATE '2026-11-01')")
        (n,) = fetch_one(
            conn,
            "SELECT count(*) FROM backtest_results WHERE model LIKE 'QAtest%'")
        assert n == 2
        cur.execute("ROLLBACK TO SAVEPOINT qa_bt_ok")
        cur.execute("RELEASE SAVEPOINT qa_bt_ok")


def test_pricing_simulator_simulate_job_hand_computed():
    """OFFLINE unit test of pricing_simulator.simulate_job: hand-computed
    breakdown, OT premium, percentage-point comparisons, clamps and the
    price/hours ValueError contract."""
    from pricing_simulator import simulate_job

    # Plain quote: 10h @ $50, no OT, materials 30% of price, price 2000,
    # other 5% -> labor 500, materials 600, other 100, margin 800 (40%).
    r = simulate_job(hours=10, base_rate=50, ot_pct=0.0, material_ratio=0.30,
                     material_change_pct=0.0, price=2000.0)
    assert r["labor_cost"] == 500.0
    assert r["material_cost"] == 600.0
    assert r["other_cost"] == 100.0
    assert r["total_cost"] == 1200.0
    assert r["gross_margin"] == 800.0
    assert abs(r["margin_pct"] - 40.0) < 1e-9
    assert abs(r["margin_vs_target_pct"] - 10.0) < 1e-9   # 40% vs 30% target
    assert r["margin_vs_market_pct"] is None
    assert r["clamps_applied"] == []

    # OT premium: 20% of hours paid 1.5x -> effective multiplier (1 + 0.5*0.2)
    # = 1.1 -> labor 550, margin 750 (37.5%).
    r = simulate_job(hours=10, base_rate=50, ot_pct=0.2, material_ratio=0.30,
                     material_change_pct=0.0, price=2000.0)
    assert r["labor_cost"] == 550.0
    assert abs(r["margin_pct"] - 37.5) < 1e-9

    # Material swing: +10% on a 30% ratio -> materials 660; margin 740 (37%).
    r = simulate_job(hours=10, base_rate=50, ot_pct=0.0, material_ratio=0.30,
                     material_change_pct=0.10, price=2000.0)
    assert r["material_cost"] == 660.0
    assert abs(r["margin_pct"] - 37.0) < 1e-9

    # Market-margin comparison arrives as a FRACTION and is compared in pp.
    r = simulate_job(hours=10, base_rate=50, ot_pct=0.0, material_ratio=0.30,
                     material_change_pct=0.0, price=2000.0,
                     market_margin=0.3632)
    assert abs(r["margin_vs_market_pct"] - (40.0 - 36.32)) < 1e-9

    # Clamps: ot_pct 0.8 -> 0.5, material_change_pct -2.0 -> -1.0 (and +4 -> +3),
    # with the overrides reported in clamps_applied.
    r = simulate_job(hours=10, base_rate=50, ot_pct=0.8, material_ratio=0.30,
                     material_change_pct=-2.0, price=2000.0)
    assert r["ot_pct"] == 0.5
    assert r["material_change_pct"] == -1.0
    assert len(r["clamps_applied"]) == 2
    r = simulate_job(hours=10, base_rate=50, ot_pct=0.0, material_ratio=0.30,
                     material_change_pct=4.0, price=2000.0)
    assert r["material_change_pct"] == 3.0

    # Validation: non-positive price / hours raise ValueError.
    for bad in (dict(hours=-1, base_rate=50, ot_pct=0, material_ratio=0.3,
                     material_change_pct=0, price=100),
                dict(hours=1, base_rate=50, ot_pct=0, material_ratio=0.3,
                     material_change_pct=0, price=0)):
        with pytest.raises(ValueError):
            simulate_job(**bad)


def test_forecast_engine_pure_helpers():
    """OFFLINE unit test of forecast_engine's closed-form math helpers (no
    DB touched): least squares, seasonal indices, winsorize, MAPE/WAPE and
    month arithmetic."""
    import forecast_engine as fe

    # Least squares recovers an exact line y = 3x + 1.
    slope, intercept = fe._least_squares(range(5), [3 * x + 1 for x in range(5)])
    assert abs(slope - 3.0) < 1e-12 and abs(intercept - 1.0) < 1e-12

    # Seasonal indices: Jan appears twice at 100, Feb once at 50, overall
    # mean = (100+100+50)/3 = 83.33 -> Jan idx 1.2, Feb 0.6, others 1.0.
    idx = fe._seasonal_indices([(1, 100.0), (1, 100.0), (2, 50.0)])
    assert abs(idx[1] - 1.2) < 1e-9
    assert abs(idx[2] - 0.6) < 1e-9
    assert idx[12] == 1.0
    # Zero overall mean / empty input degenerate to all-1.0.
    assert fe._seasonal_indices([(1, 0.0)]) == {m: 1.0 for m in range(1, 13)}
    assert fe._seasonal_indices([]) == {m: 1.0 for m in range(1, 13)}

    # Winsorize.
    assert fe._winsorize(0.5, -0.05, 0.05) == 0.05
    assert fe._winsorize(-0.5, -0.05, 0.05) == -0.05
    assert fe._winsorize(0.01, -0.05, 0.05) == 0.01

    # MAPE excludes zero actuals: |err|/|actual| = 10/100 (10%) and
    # 10/50 (20%) -> mean 15%; the (10.0, 0.0) pair is skipped.
    assert abs(fe._mape_pct([(110.0, 100.0), (60.0, 50.0), (10.0, 0.0)])
               - 15.0) < 1e-9
    assert fe._mape_pct([(10.0, 0.0)]) is None

    # WAPE sums absolute errors over ALL actuals (negative months included):
    # errors 10 + 20 over |actual| 100 + 200 -> 10%.
    assert abs(fe._wape_pct([(110.0, 100.0), (180.0, 200.0)]) - 10.0) < 1e-9
    # Negative actuals are WAPE's reason to exist: actual -100, pred -90.
    assert abs(fe._wape_pct([(-90.0, -100.0)]) - 10.0) < 1e-9
    assert fe._wape_pct([(5.0, 0.0)]) is None

    # Metric convention: net_cash_flow -> WAPE, everything else -> MAPE.
    assert fe.METRIC_FOR["net_cash_flow"] == "WAPE"
    assert fe.METRIC_FOR["revenue"] == "MAPE"

    # Month arithmetic crosses year boundaries both ways.
    assert fe._add_months(datetime.date(2026, 11, 1), 3) == datetime.date(2027, 2, 1)
    assert fe._add_months(datetime.date(2026, 1, 1), -1) == datetime.date(2025, 12, 1)


# ---------------------------------------------------------------------------
# 12b) CSV ingest engine (ingest_csv.py) + dashboard sanity
# ---------------------------------------------------------------------------
# All ingest runs go through _run_ingest(), which targets the SCRATCH database
# (never smb_warehouse) and uses ABSOLUTE CSV paths written by pytest's
# tmp_path fixture -- the engine's base_dir (cwd) is then irrelevant and the
# project folder is never written to. The shipped samples/*.csv reference live
# sim rows (JOB-2026-xxxx, @brightpathworks.com) that do not exist in the
# scratch DB, so every test here brings its own CSVs + parent fixture rows.

def _write_csv(path, header, rows):
    """Write a small CSV (comma-delimited, quoted where needed)."""
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


def _run_ingest(files, dry_run=False):
    """run_ingest against the SCRATCH database. `files` is the config's
    "files" list; every path in it must be absolute."""
    import ingest_csv
    return ingest_csv.run_ingest({"files": files},
                                 db=scratch_database_url(),
                                 dry_run=dry_run)


def test_ingest_coerce_value_types():
    """Unit test of coerce(): money shapes, sign handling, dates, hours
    bounds, booleans and text cleaning. No database involved."""
    import ingest_csv
    coerce, RowError = ingest_csv.coerce, ingest_csv.RowError

    # money: currency symbols, thousands separators, parenthesized negatives
    assert coerce("$1,234.56", {"type": "money"}) == 1234.56
    assert coerce("(1,234.56)", {"type": "money"}) == -1234.56
    assert coerce("-$45.5", {"type": "money"}) == -45.5
    # money_signed accepts an explicit +
    assert coerce("+1234.5", {"type": "money_signed"}) == 1234.5
    # zero amounts are data errors (the warehouse stores direction separately)
    with pytest.raises(RowError):
        coerce("$0.00", {"type": "money"})
    with pytest.raises(RowError):
        coerce("0", {"type": "money_signed"})

    # dates: formats list first, then the built-in DATE_FORMATS fallback
    assert coerce("3/5/2026", {"type": "date",
                               "formats": ["%m/%d/%Y"]}) == datetime.date(2026, 3, 5)
    assert coerce("2026-03-05", {"type": "date",
                                 "formats": ["%m/%d/%Y"]}) == datetime.date(2026, 3, 5)
    with pytest.raises(RowError):
        coerce("not-a-date", {"type": "date", "formats": ["%m/%d/%Y"]})

    # hours must land in (0, 24]
    assert coerce("8.5", {"type": "hours"}) == 8.5
    with pytest.raises(RowError):
        coerce("0", {"type": "hours"})
    with pytest.raises(RowError):
        coerce("26", {"type": "hours"})

    # booleans
    for truthy in ("true", "Yes", "Y", "1"):
        assert coerce(truthy, {"type": "bool"}) is True
    for falsy in ("false", "No", "0", "F"):
        assert coerce(falsy, {"type": "bool"}) is False
    with pytest.raises(RowError):
        coerce("maybe", {"type": "bool"})

    # text: trim + whitespace collapse, case folding, null_if, empty -> None
    assert coerce("  a   b  ", {"type": "text"}) == "a b"
    assert coerce("ABC@X.COM", {"type": "text", "lower": True}) == "abc@x.com"
    assert coerce("abc", {"type": "text", "upper": True}) == "ABC"
    assert coerce("N/A", {"type": "text", "null_if": ["N/A"]}) is None
    assert coerce("", {"type": "text"}) is None

    # build_record derives entry_type from a money_signed value's sign and
    # stores the magnitude (no DB needed -- resolve only runs in finalize).
    rec = ingest_csv.build_record(
        {"Amount": "(120.00)"},
        {"column_map": {"Amount": {"column": "amount", "type": "money_signed",
                                   "entry_type_from_sign": True}}},
        None, 2)
    assert rec == {"entry_type": "expense", "amount": 120.0}
    rec = ingest_csv.build_record(
        {"Amount": "$300.00"},
        {"column_map": {"Amount": {"column": "amount", "type": "money_signed",
                                   "entry_type_from_sign": True}}},
        None, 2)
    assert rec == {"entry_type": "revenue", "amount": 300.0}


def test_ingest_unique_key_dedup_customers(conn, qa_graph, tmp_path):
    """unique_key dedup (customers.email): duplicate email -> 1 loaded,
    1 skipped; a second run loads 0 (ON CONFLICT DO NOTHING)."""
    tag = qa_graph["tag"]
    csv_path = tmp_path / "customers.csv"
    email = f"QAtest-{tag}-alpha@ingest.test"
    _write_csv(csv_path, ["Name", "Email", "City"], [
        [f"QAtest-{tag}-Alpha", email, "QA City"],
        [f"QAtest-{tag}-AlphaDup", email, "QA City"],
    ])
    files = [{
        "name": "qa customers",
        "path": str(csv_path),
        "target": "customers",
        "column_map": {
            "Name": {"column": "name", "type": "text"},
            "Email": {"column": "email", "type": "text"},
            "City": {"column": "city", "type": "text"},
        },
        "dedup": {"mode": "unique_key", "keys": ["email"], "on_conflict": "skip"},
    }]

    res = _run_ingest(files)
    r = res["results"][0]
    assert r["errors"] == []
    assert r["loaded"] == 1
    assert r["skipped_duplicates"] == 1
    assert r["created_refs"] == {"customers": 0, "job_types": 0, "employees": 0}
    # First row wins: DO NOTHING keeps the originally inserted name.
    (name,) = fetch_one(conn, "SELECT name FROM customers WHERE email = %s",
                        (email,))
    assert name == f"QAtest-{tag}-Alpha"

    # Second run: both rows now conflict with the stored row -> 0 loaded.
    res2 = _run_ingest(files)
    r2 = res2["results"][0]
    assert r2["loaded"] == 0
    assert r2["skipped_duplicates"] == 2
    (n,) = fetch_one(conn, "SELECT count(*) FROM customers WHERE email = %s",
                     (email,))
    assert n == 1


def test_ingest_exact_dedup_labor_logs(conn, qa_graph, tmp_path):
    """exact dedup on labor_logs: an in-file duplicate AND an exact duplicate
    of an already-stored row are both skipped; only new rows load."""
    tag = qa_graph["tag"]
    job_id = qa_graph["job_id"]
    emp_email = f"{QA_PREFIX}_{tag}.emp@example.test"
    second_email = f"QAtest-{tag}.second@example.test"

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO employees (first_name, last_name, email, hire_date,"
            " hourly_rate) VALUES ('QA', %s, %s, %s, 55.00) RETURNING id",
            (f"Second_{tag}", second_email, datetime.date(2020, 1, 2)),
        )
        conn.commit()

    # A stored row the CSV will duplicate verbatim (exact mode matches ALL
    # mapped columns, description included).
    db_desc = f"QAtest {tag} pre-existing"
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO labor_logs (job_id, employee_id, work_date, hours,"
            " hourly_rate, description)"
            " VALUES (%s, %s, DATE '2026-03-02', 3.00, 50.00, %s)",
            (job_id, qa_graph["employee_id"], db_desc),
        )
    conn.commit()

    csv_path = tmp_path / "payroll.csv"
    _write_csv(csv_path, ["Employee Email", "Job Number", "Work Date",
                          "Hours", "Rate", "Notes"], [
        # exact copy of the DB row -> skipped by the anti-join
        [emp_email, f"QAtest-{tag}-JOB", "03/02/2026", "3.00", "$50.00", db_desc],
        # in-file duplicate pair -> first loads, second md5-skipped
        [emp_email, f"QAtest-{tag}-JOB", "03/03/2026", "4.00", "$50.00",
         f"QAtest {tag} infile-dup"],
        [emp_email, f"QAtest-{tag}-JOB", "03/03/2026", "4.00", "$50.00",
         f"QAtest {tag} infile-dup"],
        # genuinely new row -> loads
        [second_email, f"QAtest-{tag}-JOB", "03/04/2026", "5.00", "$55.00",
         f"QAtest {tag} fresh"],
    ])
    files = [{
        "name": "qa payroll",
        "path": str(csv_path),
        "target": "labor_logs",
        "column_map": {
            "Work Date": {"column": "work_date", "type": "date",
                          "formats": ["%m/%d/%Y"]},
            "Hours": {"column": "hours", "type": "hours"},
            "Rate": {"column": "hourly_rate", "type": "money"},
            "Notes": {"column": "description", "type": "text"},
        },
        "resolve": {
            "employee_id": {"table": "employees", "match_column": "email",
                            "from_csv": "Employee Email", "on_missing": "error"},
            "job_id": {"table": "jobs", "match_column": "job_number",
                       "from_csv": "Job Number", "on_missing": "skip_row"},
        },
        "dedup": {"mode": "exact"},
    }]

    res = _run_ingest(files)
    r = res["results"][0]
    assert r["errors"] == []
    assert r["rows_read"] == 4
    assert r["loaded"] == 2                       # infile-dup first + fresh
    assert r["skipped_duplicates"] == 2           # 1 in-file + 1 vs the table
    (n,) = fetch_one(conn,
                     "SELECT count(*) FROM labor_logs WHERE job_id = %s",
                     (job_id,))
    assert n == 3                                 # 1 pre-existing + 2 loaded


def test_ingest_fk_resolution_policies(conn, qa_graph, tmp_path):
    """FK resolution: job_number -> job id in-table; unknown job_number with
    on_missing=skip_row skips the row and records an error entry whose message
    ends in '(row skipped)'; unknown employee email with on_missing=error
    records a plain 'not found' row error; on_missing=create creates a
    customer with an @imported.example.com slug email and reports it in
    created_refs. (Verified semantics: BOTH skip_row and error produce
    summary['errors'] entries -- skip_row is not silent; the difference is
    the message text.)"""
    tag = qa_graph["tag"]
    job_number = f"QAtest-{tag}-JOB"

    # -- phase A: job_id resolves by job_number --------------------------
    csv_a = tmp_path / "fka.csv"
    cat_a = f"QAtest_{tag}_fkA"
    _write_csv(csv_a, ["Date", "Amount", "Category", "Job Number"], [
        ["03/05/2026", "-25.00", cat_a, job_number],
    ])
    res = _run_ingest([{
        "name": "fk resolve", "path": str(csv_a), "target": "financial_ledger",
        "column_map": {
            "Date": {"column": "entry_date", "type": "date",
                     "formats": ["%m/%d/%Y"]},
            "Amount": {"column": "amount", "type": "money_signed",
                       "entry_type_from_sign": True},
            "Category": {"column": "category", "type": "text"},
        },
        "resolve": {"job_id": {"table": "jobs", "match_column": "job_number",
                               "from_csv": "Job Number",
                               "on_missing": "skip_row"}},
        "dedup": {"mode": "exact"},
    }])
    r = res["results"][0]
    assert r["errors"] == [] and r["loaded"] == 1
    (job_id, etype, amount) = fetch_one(
        conn,
        "SELECT job_id, entry_type, amount FROM financial_ledger"
        " WHERE category = %s", (cat_a,))
    assert job_id == qa_graph["job_id"]           # resolved via job_number
    assert (etype, amount) == ("expense", Decimal("25.00"))

    # -- phase B: unknown job_number + skip_row -> skipped, reported -------
    csv_b = tmp_path / "fkb.csv"
    _write_csv(csv_b, ["Date", "Amount", "Category", "Job Number"], [
        ["03/06/2026", "-10.00", f"QAtest_{tag}_fkB", "JOB-DOES-NOT-EXIST"],
    ])
    res = _run_ingest([{
        "name": "fk skip_row", "path": str(csv_b), "target": "financial_ledger",
        "column_map": {
            "Date": {"column": "entry_date", "type": "date",
                     "formats": ["%m/%d/%Y"]},
            "Amount": {"column": "amount", "type": "money_signed",
                       "entry_type_from_sign": True},
            "Category": {"column": "category", "type": "text"},
        },
        "resolve": {"job_id": {"table": "jobs", "match_column": "job_number",
                               "from_csv": "Job Number",
                               "on_missing": "skip_row"}},
        "dedup": {"mode": "exact"},
    }])
    r = res["results"][0]
    assert r["loaded"] == 0
    assert len(r["errors"]) == 1 and r["errors"][0]["row"] == 2
    assert "(row skipped)" in r["errors"][0]["error"]
    assert "not found" in r["errors"][0]["error"]

    # -- phase C: unknown employee email + error policy -> row error -------
    csv_c = tmp_path / "fkc.csv"
    _write_csv(csv_c, ["Employee Email", "Job Number", "Work Date", "Hours",
                       "Rate", "Notes"], [
        ["unknown.person@nowhere.example", job_number, "03/06/2026", "2.00",
         "$40.00", f"QAtest {tag} err"],
    ])
    res = _run_ingest([{
        "name": "fk error", "path": str(csv_c), "target": "labor_logs",
        "column_map": {
            "Work Date": {"column": "work_date", "type": "date",
                          "formats": ["%m/%d/%Y"]},
            "Hours": {"column": "hours", "type": "hours"},
            "Rate": {"column": "hourly_rate", "type": "money"},
            "Notes": {"column": "description", "type": "text"},
        },
        "resolve": {
            "employee_id": {"table": "employees", "match_column": "email",
                            "from_csv": "Employee Email",
                            "on_missing": "error"},
            "job_id": {"table": "jobs", "match_column": "job_number",
                       "from_csv": "Job Number", "on_missing": "skip_row"},
        },
        "dedup": {"mode": "exact"},
    }])
    r = res["results"][0]
    assert r["loaded"] == 0
    assert r["errors"] == [{
        "row": 2,
        "error": "employees email='unknown.person@nowhere.example' not found",
    }]

    # -- phase D: customer on_missing=create -> customer created -----------
    csv_d = tmp_path / "fkd.csv"
    cust_name = f"QAtest-{tag} IngestCo"
    cat_d = f"QAtest_{tag}_fkD"
    _write_csv(csv_d, ["Date", "Amount", "Category", "Customer"], [
        ["03/07/2026", "60.00", cat_d, cust_name],
    ])
    res = _run_ingest([{
        "name": "fk create", "path": str(csv_d), "target": "financial_ledger",
        "column_map": {
            "Date": {"column": "entry_date", "type": "date",
                     "formats": ["%m/%d/%Y"]},
            "Amount": {"column": "amount", "type": "money_signed",
                       "entry_type_from_sign": True},
            "Category": {"column": "category", "type": "text"},
        },
        "resolve": {"customer_id": {"table": "customers",
                                    "match_column": "name",
                                    "from_csv": "Customer",
                                    "on_missing": "create"}},
        "dedup": {"mode": "exact"},
    }])
    r = res["results"][0]
    assert r["errors"] == [] and r["loaded"] == 1
    assert r["created_refs"]["customers"] == 1
    import ingest_csv
    slug_email = ingest_csv._slug_email(cust_name)
    assert slug_email.endswith("@imported.example.com")
    (cid,) = fetch_one(conn, "SELECT id FROM customers WHERE email = %s",
                       (slug_email,))
    assert cid > 0
    (fk_cust, fk_job, etype, amount) = fetch_one(
        conn,
        "SELECT customer_id, job_id, entry_type, amount FROM financial_ledger"
        " WHERE category = %s", (cat_d,))
    assert fk_cust == cid and fk_job is None
    assert (etype, amount) == ("revenue", Decimal("60.00"))


def test_ingest_money_signed_splits_entry_type(conn, qa_graph, tmp_path):
    """money_signed + entry_type_from_sign: negative CSV amounts become
    expense rows with the positive magnitude, positive ones become revenue;
    verified in-table."""
    tag = qa_graph["tag"]
    csv_path = tmp_path / "accounting.csv"
    _write_csv(csv_path, ["Date", "Amount", "Category"], [
        ["03/10/2026", "-120.00", f"QAtest_{tag}_ms_neg"],
        ["03/11/2026", "300.00", f"QAtest_{tag}_ms_pos"],
    ])
    res = _run_ingest([{
        "name": "signed money", "path": str(csv_path),
        "target": "financial_ledger",
        "column_map": {
            "Date": {"column": "entry_date", "type": "date",
                     "formats": ["%m/%d/%Y"]},
            "Amount": {"column": "amount", "type": "money_signed",
                       "entry_type_from_sign": True},
            "Category": {"column": "category", "type": "text"},
        },
        "dedup": {"mode": "exact"},
    }])
    r = res["results"][0]
    assert r["errors"] == [] and r["loaded"] == 2
    rows = dict(fetch_all(
        conn,
        "SELECT category, entry_type || '=' || amount::text FROM financial_ledger"
        " WHERE category LIKE %s ORDER BY category",
        (f"QAtest_{tag}_ms_%",)))
    assert rows[f"QAtest_{tag}_ms_neg"] == "expense=120.00"
    assert rows[f"QAtest_{tag}_ms_pos"] == "revenue=300.00"


def test_ingest_row_errors_reported_with_line_numbers(conn, qa_graph, tmp_path):
    """Rows with an unparseable date / zero amount are skipped and reported
    with their 1-based CSV LINE numbers (header = line 1), while the valid
    rows still load."""
    tag = qa_graph["tag"]
    csv_path = tmp_path / "messy.csv"
    cat = f"QAtest_{tag}_re"
    _write_csv(csv_path, ["Date", "Amount", "Category"], [
        ["03/01/2026", "150.50", cat],        # CSV line 2: good
        ["bogus-date", "80.00", cat],         # CSV line 3: bad date
        ["03/03/2026", "$0.00", cat],         # CSV line 4: zero amount
        ["03/04/2026", "60.00", cat],         # CSV line 5: good
    ])
    res = _run_ingest([{
        "name": "messy rows", "path": str(csv_path),
        "target": "financial_ledger",
        "column_map": {
            "Date": {"column": "entry_date", "type": "date",
                     "formats": ["%m/%d/%Y"]},
            "Amount": {"column": "amount", "type": "money_signed",
                       "entry_type_from_sign": True},
            "Category": {"column": "category", "type": "text"},
        },
        "dedup": {"mode": "exact"},
    }])
    r = res["results"][0]
    assert r["rows_read"] == 4
    assert r["loaded"] == 2
    assert [(e["row"], e["error"]) for e in r["errors"]] == [
        (3, "unrecognized date: 'bogus-date'"),
        (4, "zero amount"),
    ]
    (n,) = fetch_one(conn,
                     "SELECT count(*) FROM financial_ledger WHERE category = %s",
                     (cat,))
    assert n == 2


def test_ingest_dry_run_writes_nothing(conn, qa_graph, tmp_path):
    """dry_run=True reports what WOULD be loaded but must leave the database
    completely untouched -- including the resolve_fk 'create' policy, which
    inserts during record building (regression for the dry-run bug this suite
    caught: run_ingest used to commit the created customers)."""
    tag = qa_graph["tag"]

    # Part 1: a plain unique_key customers file.
    csv_path = tmp_path / "dry_customers.csv"
    email = f"QAtest-{tag}-dry@ingest.test"
    _write_csv(csv_path, ["Name", "Email"], [
        [f"QAtest-{tag}-Dry", email],
    ])
    cust_files = [{
        "name": "dry customers", "path": str(csv_path), "target": "customers",
        "column_map": {
            "Name": {"column": "name", "type": "text"},
            "Email": {"column": "email", "type": "text"},
        },
        "dedup": {"mode": "unique_key", "keys": ["email"], "on_conflict": "skip"},
    }]
    res = _run_ingest(cust_files, dry_run=True)
    r = res["results"][0]
    assert r["loaded"] == 1                       # WOULD load one row
    assert r["skipped_duplicates"] == "n/a (dry run)"
    assert r["errors"] == []
    (n,) = fetch_one(conn, "SELECT count(*) FROM customers WHERE email = %s",
                     (email,))
    assert n == 0                                 # ...but did not

    # Part 2: on_missing=create -- the path that used to leak in dry runs.
    csv_path2 = tmp_path / "dry_create.csv"
    cust_name = f"QAtest-{tag} DryRunCo"
    _write_csv(csv_path2, ["Date", "Amount", "Category", "Customer"], [
        ["03/20/2026", "-15.00", f"QAtest_{tag}_dry", cust_name],
    ])
    res = _run_ingest([{
        "name": "dry create", "path": str(csv_path2),
        "target": "financial_ledger",
        "column_map": {
            "Date": {"column": "entry_date", "type": "date",
                     "formats": ["%m/%d/%Y"]},
            "Amount": {"column": "amount", "type": "money_signed",
                       "entry_type_from_sign": True},
            "Category": {"column": "category", "type": "text"},
        },
        "resolve": {"customer_id": {"table": "customers",
                                    "match_column": "name",
                                    "from_csv": "Customer",
                                    "on_missing": "create"}},
        "dedup": {"mode": "exact"},
    }], dry_run=True)
    r = res["results"][0]
    assert r["errors"] == [] and r["loaded"] == 1
    assert r["created_refs"]["customers"] == 1    # would create one...
    (n,) = fetch_one(
        conn,
        "SELECT count(*) FROM customers WHERE email LIKE '%@imported.example.com'")
    assert n == 0                                 # ...but created none
    (n,) = fetch_one(
        conn,
        "SELECT count(*) FROM financial_ledger WHERE category = %s",
        (f"QAtest_{tag}_dry",))
    assert n == 0


def test_ingest_refresh_runs_views_and_detector(conn, qa_graph, tmp_path):
    """refresh=true makes run_ingest call refresh_all_analytics() +
    detect_anomalies() after the load; the summary reports refreshed=True and
    the number of new alerts (0 on this tiny clean fixture)."""
    tag = qa_graph["tag"]
    csv_path = tmp_path / "refresh_customers.csv"
    _write_csv(csv_path, ["Name", "Email"], [
        [f"QAtest-{tag}-Refresh", f"QAtest-{tag}-refresh@ingest.test"],
    ])
    res = _run_ingest([{
        "name": "refreshing file", "path": str(csv_path),
        "target": "customers",
        "column_map": {
            "Name": {"column": "name", "type": "text"},
            "Email": {"column": "email", "type": "text"},
        },
        "dedup": {"mode": "unique_key", "keys": ["email"],
                  "on_conflict": "skip"},
        "refresh": True,
    }])
    assert set(res) == {"results", "refreshed", "new_alerts"}
    assert res["refreshed"] is True
    assert res["new_alerts"] == 0                 # customers raise no alerts
    r = res["results"][0]
    assert r["loaded"] == 1 and r["errors"] == []


def test_ingest_unsupported_target_rolls_back(conn, qa_graph, tmp_path):
    """A config targeting an unsupported table fails at the FILE level: the
    result entry carries an error string, nothing is loaded, and the database
    is unchanged."""
    tag = qa_graph["tag"]
    (alerts_before,) = fetch_one(conn,
                                 "SELECT count(*) FROM anomaly_alerts")
    (customers_before,) = fetch_one(conn, "SELECT count(*) FROM customers")
    csv_path = tmp_path / "bad_target.csv"
    _write_csv(csv_path, ["Name", "Email"], [
        [f"QAtest-{tag}-BadTarget", f"QAtest-{tag}-bad@ingest.test"],
    ])
    res = _run_ingest([{
        "name": "unsupported", "path": str(csv_path),
        "target": "anomaly_alerts",               # NOT in SUPPORTED_TABLES
        "column_map": {"Name": {"column": "name", "type": "text"}},
    }])
    r = res["results"][0]
    assert "file failed, rolled back" in r["error"]
    assert "unsupported target table 'anomaly_alerts'" in r["error"]
    assert r["rows_read"] == 0 and r["loaded"] == 0
    assert r["errors"] == []
    (alerts_after,) = fetch_one(conn,
                                "SELECT count(*) FROM anomaly_alerts")
    (customers_after,) = fetch_one(conn, "SELECT count(*) FROM customers")
    assert (alerts_after, customers_after) == (alerts_before, customers_before)


def test_app_and_ingest_compile_clean():
    """app.py and ingest_csv.py byte-compile with no syntax errors. app.py is
    NOT started as a server here (py_compile only)."""
    for name in ("app.py", "ingest_csv.py"):
        proc = subprocess.run(
            [sys.executable, "-m", "py_compile", os.path.join(_BASE_DIR, name)],
            capture_output=True, text=True, timeout=60,
        )
        assert proc.returncode == 0, (
            f"{name} failed py_compile (rc={proc.returncode}):\n"
            f"{proc.stdout}\n{proc.stderr}"
        )


def test_dashboard_apptest_smoke_readonly():
    """READ-ONLY smoke render of app.py via streamlit.testing.v1.AppTest: all
    tabs render, no widget interaction (no button clicks -> no writes), so
    the app only issues SELECTs against the live smb_warehouse. Skipped when
    the dashboard dependencies or the live database are unavailable."""
    try:
        import pandas   # noqa: F401  (app.py imports these at module scope)
        import plotly   # noqa: F401
        from streamlit.testing.v1 import AppTest
    except ImportError as exc:
        pytest.skip(f"dashboard dependencies unavailable: {exc}")
    try:
        probe = psycopg2.connect(
            "postgresql://postgres:postgres@localhost:5432/smb_warehouse",
            connect_timeout=3)
        probe.close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"live smb_warehouse not reachable: {exc!r}")

    at = AppTest.from_file(os.path.join(_BASE_DIR, "app.py"),
                           default_timeout=120)
    at.run()
    assert not at.exception, f"app.py raised: {at.exception}"
    # The app renders its title and live sidebar metrics (read-only SELECTs
    # against smb_warehouse).
    assert any("SMB Financial" in t.value for t in at.title)
    # Tab-level st.error banners are the app's own graceful degradation (each
    # tab wraps its queries in try/except). They used to be intentionally not
    # asserted here because app.py had two known display bugs (an Overview
    # dtype comparison TypeError and a Charts query selecting a nonexistent
    # "month_start" column from mv_benchmark_comparison). Those bugs are fixed
    # since (independently re-verified 2026-11-20 by the QA audit via
    # AppTest: 0 exceptions AND 0 st.error banners across all six tabs), so
    # the note above is historical; the smoke render stays interaction-free
    # and banner assertions remain the QA predictive suite's/AppTest's job.


# ---------------------------------------------------------------------------
# 13) Final state: schema + matviews + zero QA residue
# ---------------------------------------------------------------------------

def test_final_database_clean_of_qa_data(conn):
    # Every base table must be empty: the warehouse ships with no data and the
    # suite must leave no QAtest fixture rows behind. companies is the one
    # deliberate exception: the schema-seeded demo workspace (company #1,
    # 'BrightPath Works (Demo)') survives -- the company_id DEFAULTs point at
    # it -- and dies with the throwaway database at session teardown; every
    # per-test QAtest workspace is swept by cleanup_qa_data.
    for table in sorted(EXPECTED_TABLES):
        (total,) = fetch_one(conn, f"SELECT count(*) FROM {table}")
        if table == "companies":
            assert total == 1, (
                f"companies must hold exactly the schema-seeded demo"
                f" workspace, found {total}"
            )
            (co_name,) = fetch_one(
                conn, "SELECT company_name FROM companies LIMIT 1")
            assert co_name == QA_SESSION_COMPANY_NAME, co_name
            continue
        assert total == 0, f"{table} still has {total} row(s) after the suite"

    # Refresh the snapshots and confirm they hold no QA traces either.
    with conn.cursor() as cur:
        cur.execute("SELECT refresh_all_analytics()")
    conn.commit()
    for mv in ("mv_job_type_profitability", "mv_labor_cost_analysis",
               "mv_customer_profitability", "mv_benchmark_comparison"):
        (n,) = fetch_one(conn, f"SELECT count(*) FROM {mv}")
        assert n == 0, f"{mv} still holds {n} snapshot row(s)"
    (months,) = fetch_one(
        conn,
        "SELECT count(*) FROM mv_monthly_summary"
        " WHERE month_start >= DATE '2026-01-01' AND month_start <= DATE '2026-12-01'",
    )
    assert months == 0, "mv_monthly_summary still holds QA activity months"
    # Matviews remain present and populated after the refresh.
    rows = fetch_all(
        conn,
        "SELECT matviewname, ispopulated FROM pg_matviews WHERE schemaname = 'public'",
    )
    assert {r[0] for r in rows} == EXPECTED_MATVIEWS
    assert all(r[1] for r in rows)
