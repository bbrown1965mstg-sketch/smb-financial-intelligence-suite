"""
QA suite for nl_to_sql.py -- the "Ask Your Data" engine (LLM -> ONE strictly
read-only SQL query) -- and the dashboard's Ask-Your-Data tab (app.py).

HOW TO RUN
----------
    cd "/Users/Owner/Desktop/GLM-5.3/Data Warehouse"
    python3 -m pytest test_nl_to_sql.py -v

or together with the other suites (full regression):

    python3 -m pytest test_schema.py test_predictive.py test_nl_to_sql.py -v

SAFETY MODEL (deliberately different from test_schema.py's scratch-DB suite)
----------------------------------------------------------------------------
This suite needs NO scratch database, because NOTHING in it is allowed to
modify the warehouse:

  * Part A (guardrail battery) and Part C (mock-LLM pipeline) and Part D
    (config) are fully OFFLINE -- no DB, no network. LLM calls are mocked by
    monkeypatching nl_to_sql.chat; no real GLM/OpenAI request is ever made.
  * Part B, Part R and the dashboard tests connect to the LIVE smb_warehouse
    READ-ONLY. run_readonly_query() itself now connects as the RESTRICTED
    nl2sql_reader role (migrations/002_add_rls.sql: NOSUPERUSER, NOBYPASSRLS,
    SELECT grants only, row-level-security scoped by the
    app.current_company_id GUC) -- the role cannot write even in principle.
    The only statements that touch a write path are the SANCTIONED must-fail
    attempts (UPDATE / DELETE / INSERT / REFRESH through
    run_readonly_query), each of which must be blocked by the read-only
    transaction (which fires before even the role's missing grants are
    consulted) with ZERO rows changed, proven by COUNT(*) before/after on
    the suite's own (postgres, read-only) connection.
  * A session-scoped fixture opens its connection with
    set_session(readonly=True, autocommit=True) as defense in depth, so even
    a test bug could not write through it.
  * A final integrity test pins the live row counts (jobs 2502,
    anomaly_alerts 71, 5 whales, market_benchmarks 864, financial_ledger
    6373, labor_logs 18504, forecasts 12, backtest_results 4) to prove the
    suite left the warehouse untouched. (If the demo data is ever re-seeded
    with different volumes, update the constants here.)

Coverage
--------
  A. validate_sql() guardrail battery (offline, parametrized):
     injection classics / multi-statements, write + admin statements, CTE
     write tricks, SELECT ... INTO, comments (including inside literals --
     a pinned fail-closed deviation), non-allowlist relations (pg_catalog,
     information_schema, other schemas, comma mixes, JOINs, subqueries,
     functions, LATERAL, dblink, the RAW mv_* matviews), FALSE-POSITIVE
     cases that MUST pass (literal masking, '' escapes, public.-prefix,
     JOIN aliases, LIMIT handling, trailing ';', auto LIMIT 200 exactly
     once), read-only CTEs (WITH ... AS) now allowed, and the double-quote /
     dollar-quote contract (pinned as implemented, deviations called out).
  B. Live read-only execution through the restricted role: the scoped_*
     views return data consistent with a plain psycopg2 connection; the
     DB-level backstop blocks UPDATE/DELETE/INSERT/REFRESH with zero
     changes; statement_timeout kills pg_sleep within ~2 s wall clock; the
     500-row client cap; clean failure for SQL that passes the parser but
     not the database; ask() never executes parser-rejected SQL (tripwire).
  R. Row-level security enforcement (live): the migration's role attributes,
     the 9 policies, the grant matrix INCLUDING the negative checks (the 5
     raw mv_* matviews denied), the two fail-closed pins (no company_id ->
     zero client rows; non-int company_id -> ValueError), and role-session
     proofs on the single-workspace live warehouse (GUC=1 sees 2502 jobs;
     WHERE company_id=2 returns 0 because RLS ANDs with the WHERE clause;
     the companies registry shows only the workspace's own row). R12-R14
     (RLS-round audit additions): the LIVE scoped-view catalog audit with
     drift detection against both SQL definition files; the residual-
     visibility honesty table (what the role sees WITHOUT a GUC: global
     data yes, client data zero, raw matviews denied, information_schema
     metadata-only); and the AI-vector bypass battery through the real
     pipeline (nothing parser-rejected ever executes) plus the executor's
     own tenant-GUC backstop (multi-statement + set_config rejected before
     any connection opens -- both were proven leak vectors pre-fix). The
     TWO-workspace proof lives in test_multitenant_pdf.py (its scratch DB
     seeds a second company and applies the migrations).
  C. Mock-LLM pipeline: fence stripping, the one repair retry with the
     guardrail verdict fed back into the messages, double failure, and
     clean RuntimeError propagation from chat().
  D. Config: defaults with no .env (incl. the db_role/db_password keys),
     env-var fallback, .env precedence, invalid/floored timeouts, save/load
     round-trip on tmp_path (the real project .env is NEVER touched --
     ENV_PATH is monkeypatched everywhere), PRESETS content, hand-rolled
     .env parsing.
  E. Dashboard: AppTest smoke render (10 tabs, no exceptions, no error
     banners, "Ask Your Data" present, starter buttons), the LLM-less
     "Run query" flow (valid SQL renders results; DROP TABLE is rejected by
     the guardrails before execution), and the app._nl_auto_chart selection
     logic (pinned incl. its datetime64-column deviation).
  F. One LIVE GLM end-to-end ask(), skipped unless an API key is configured
     (it stays skipped here -- no key is added).
  G. Final live-integrity assertion (see above).
"""

import datetime as _dt
import importlib
import os
import re
import sys
import time
import urllib.parse

import pandas as pd
import psycopg2
import pytest

import plotly.graph_objects as go

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _BASE_DIR)

import nl_to_sql  # noqa: E402  (project-local module under test)

LIVE_URL = nl_to_sql.DB_URL

# Documented live-warehouse baseline (see module docstring). Read-only proof
# that the QA run left the warehouse untouched.
INTEGRITY_COUNTS = {
    "jobs": 2502,
    "anomaly_alerts": 71,
    "market_benchmarks": 864,
    "financial_ledger": 6373,
    "labor_logs": 18504,
    "forecasts": 12,
    "backtest_results": 4,
}
WHALE_COUNT = 5  # mv_customer_profitability rows with is_whale

VALID_SQL = ("SELECT job_type, margin_pct FROM scoped_job_type_profitability "
             "ORDER BY margin_pct DESC LIMIT 1")


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def live_conn():
    """Session-scoped READ-ONLY connection to the LIVE smb_warehouse.

    set_session(readonly=True, autocommit=True) is defense in depth: the
    suite never intends to write, and even a buggy test could not. Skips
    (rather than fails) every DB-dependent test when the warehouse is down.
    """
    try:
        conn = psycopg2.connect(LIVE_URL, connect_timeout=5)
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("SELECT 1")            # connectivity probe
    except Exception as exc:                    # noqa: BLE001
        pytest.skip(f"live smb_warehouse not reachable: {exc!r}")
    yield conn
    try:
        conn.close()
    except Exception:                           # noqa: BLE001
        pass


def plain_count(conn, table, where="TRUE"):
    """Plain-connection SELECT COUNT(*) -- the independent oracle the
    run_readonly_query results are checked against."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}")
        return cur.fetchone()[0]


NL_ENV_KEYS = ["NL2SQL_BASE_URL", "NL2SQL_API_KEY", "NL2SQL_MODEL",
               "NL2SQL_LLM_TIMEOUT", "NL2SQL_QUERY_TIMEOUT"]


@pytest.fixture
def clean_env(tmp_path, monkeypatch):
    """Isolate nl_to_sql config from the machine: remove every NL2SQL_*
    process-environment variable and point ENV_PATH at a nonexistent file in
    tmp_path, so the REAL project .env is never read or written."""
    for key in NL_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    missing = tmp_path / "absent.env"
    monkeypatch.setattr(nl_to_sql, "ENV_PATH", missing)
    return missing


def _mock_cfg():
    """Config dict for the pipeline tests. chat() is monkeypatched in those
    tests, so no HTTP request can ever leave the machine."""
    return {"base_url": "https://mock.invalid/v1", "api_key": "test-key",
            "model": "test-model", "llm_timeout": 10, "query_timeout": 5}


class _ScriptedChat:
    """chat() stand-in: returns scripted responses in order (repeating the
    last one) and records the messages of every call.

    Mimics the real chat() contract faithfully -- chat() strips code fences
    before returning -- so scripted responses may be fenced exactly like a
    real LLM reply."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, messages, cfg):
        self.calls.append([dict(m) for m in messages])
        idx = min(len(self.calls) - 1, len(self.responses) - 1)
        return nl_to_sql._strip_code_fences(self.responses[idx])


@pytest.fixture(scope="module")
def app_module():
    """The imported app.py module (for _nl_auto_chart / STARTER_QUESTIONS).

    Importing app.py executes the Streamlit script in bare mode: every widget
    call no-ops and every DB access is one of app.py's read-only SELECTs.
    Skips when the dashboard dependencies or the live DB are unavailable."""
    for mod in ("pandas", "plotly", "streamlit"):
        try:
            __import__(mod)
        except ImportError as exc:
            pytest.skip(f"dashboard dependencies unavailable: {exc}")
    try:
        probe = psycopg2.connect(LIVE_URL, connect_timeout=3)
        probe.close()
    except Exception as exc:                    # noqa: BLE001
        pytest.skip(f"live smb_warehouse not reachable: {exc!r}")
    return importlib.import_module("app")


def _make_apptest():
    from streamlit.testing.v1 import AppTest
    return AppTest.from_file(os.path.join(_BASE_DIR, "app.py"),
                             default_timeout=90)


@pytest.fixture(scope="module")
def apptest_smoke(app_module):
    """One full read-only render of app.py, reused by the smoke assertions."""
    at = _make_apptest()
    at.run()
    return at


# ===========================================================================
# A) validate_sql guardrail battery (offline)
# ===========================================================================

# --- A1: injection classics / multi-statement ------------------------------
INJECTION_CASES = [
    ("classic stack", "SELECT * FROM jobs WHERE name = 'x'; DROP TABLE jobs; --",
     "multi-statement"),
    ("quote-prefixed stack", "'; DROP TABLE jobs; --", "multi-statement"),
    ("stacked UPDATE", "SELECT 1; UPDATE anomaly_alerts SET is_resolved = TRUE;",
     "multi-statement"),
    ("stacked DELETE", "SELECT 1; DELETE FROM jobs", "multi-statement"),
    ("stacked DROP SCHEMA", "SELECT 1; DROP SCHEMA public CASCADE",
     "multi-statement"),
    ("stacked after CTE", "WITH t AS (SELECT 1) SELECT * FROM t; DROP TABLE jobs",
     "multi-statement"),
    ("stacked newline+tabs", "select 1 ;\n drop table jobs", "multi-statement"),
    ("stacked COMMIT", "\tSELECT 1; COMMIT;", "multi-statement"),
    ("tautology stack", "x' OR '1'='1'; UPDATE jobs SET status='hacked'",
     "multi-statement"),
    ("tautology with comment tail",
     "SELECT * FROM jobs WHERE name = 'x' OR '1'='1' --",
     "comments are not allowed"),
]


@pytest.mark.parametrize("label, sql, reason", INJECTION_CASES,
                         ids=[c[0] for c in INJECTION_CASES])
def test_injection_classics_rejected(label, sql, reason):
    ok, verdict = nl_to_sql.validate_sql(sql)
    assert not ok, f"{label}: expected rejection, got ok with {verdict!r}"
    assert verdict == reason, f"{label}: {verdict!r}"


def test_single_statement_tautology_is_read_only_and_allowed():
    """' OR '1'='1 variants that stay ONE SELECT over an allowlisted relation
    cannot write anything or read anything outside the allowlist, so the
    guardrails accept them (the injection variants that stack a second
    command are rejected above)."""
    ok, verdict = nl_to_sql.validate_sql(
        "SELECT * FROM jobs WHERE name = 'x' OR '1'='1'")
    assert ok
    assert verdict.endswith("LIMIT 200")


# --- A2: write / admin statements rejected at parser level ------------------
WRITE_CASES = [
    ("drop table", "DROP TABLE jobs", "drop"),
    ("drop lower", "drop table jobs", "drop"),
    ("drop matview", "DROP MATERIALIZED VIEW mv_monthly_summary", "drop"),
    ("alter", "ALTER TABLE jobs ADD COLUMN hack TEXT", "alter"),
    ("delete", "DELETE FROM jobs", "delete"),
    ("insert", "INSERT INTO jobs (job_number) VALUES ('QAtest')", "insert"),
    ("truncate table", "TRUNCATE TABLE jobs", "truncate"),
    ("truncate", "TRUNCATE jobs", "truncate"),
    ("update", "UPDATE jobs SET status = 'done'", "update"),
    ("update alerts", "UPDATE anomaly_alerts SET is_resolved = TRUE", "update"),
    ("for update", "SELECT * FROM jobs FOR UPDATE", "update"),
    ("create table", "CREATE TABLE evil (id INT)", "create"),
    ("create index", "CREATE INDEX evil_i ON jobs (job_number)", "create"),
    ("grant", "GRANT SELECT ON jobs TO PUBLIC", "grant"),
    ("revoke", "REVOKE ALL ON jobs FROM PUBLIC", "revoke"),
    ("copy to", "COPY jobs TO '/tmp/evil.csv'", "copy"),
    ("copy from", "COPY jobs FROM '/tmp/evil.csv'", "copy"),
    ("vacuum", "VACUUM jobs", "vacuum"),
    ("vacuum full", "VACUUM FULL jobs", "vacuum"),
    ("reindex", "REINDEX TABLE jobs", "reindex"),
    ("comment on", "COMMENT ON TABLE jobs IS 'x'", "comment"),
    ("lock", "LOCK TABLE jobs", "lock"),
    ("merge", "MERGE INTO jobs USING jobs s ON TRUE WHEN MATCHED DO NOTHING",
     "merge"),
    ("do block", "DO $$ BEGIN END $$", "do"),
    ("call", "CALL some_proc()", "call"),
    ("set", "SET statement_timeout = 0", "set"),
    ("reset", "RESET ALL", "reset"),
    ("prepare", "PREPARE p AS SELECT 1", "prepare"),
    ("execute", "EXECUTE p", "execute"),
    ("notify", "NOTIFY chan", "notify"),
    ("listen", "LISTEN chan", "listen"),
    # PostgreSQL EXECUTES these two inside read-only transactions (ANALYZE
    # rewrites pg_statistic, CHECKPOINT forces a WAL flush): found by the
    # adversarial pass and fixed by adding them to FORBIDDEN_KEYWORDS.
    ("analyze", "ANALYZE jobs", "analyze"),
    ("checkpoint", "CHECKPOINT", "checkpoint"),
    # remaining non-SELECT verbs the read-only transaction would not stop or
    # would run as no-ops (same fix).
    ("load", "LOAD '/tmp/evil.so'", "load"),
    ("close", "CLOSE ALL", "close"),
    ("deallocate", "DEALLOCATE ALL", "deallocate"),
    ("discard", "DISCARD ALL", "discard"),
    # superuser file / large-object helpers callable from a SELECT list
    ("pg_read_file", "SELECT pg_read_file('/etc/passwd')", "pg_read_file"),
    ("pg_read_binary_file", "SELECT pg_read_binary_file('/etc/passwd')",
     "pg_read_binary_file"),
    ("pg_ls_dir", "SELECT pg_ls_dir('.')", "pg_ls_dir"),
    ("lo_import", "SELECT lo_import('/etc/passwd')", "lo_import"),
    ("lo_export", "SELECT lo_export(1, '/tmp/x')", "lo_export"),
    # write hidden in a subquery in the select list
    ("subquery delete", "SELECT * FROM (DELETE FROM jobs RETURNING *) d",
     "delete"),
]


@pytest.mark.parametrize("label, sql, keyword", WRITE_CASES,
                         ids=[c[0] for c in WRITE_CASES])
def test_write_and_admin_statements_rejected(label, sql, keyword):
    ok, verdict = nl_to_sql.validate_sql(sql)
    assert not ok, f"{label}: expected rejection, got ok with {verdict!r}"
    assert verdict == f"forbidden keyword: {keyword}", f"{label}: {verdict!r}"


def test_forbidden_keywords_pin_core_entries():
    for kw in ("insert", "update", "delete", "drop", "alter", "truncate",
               "create", "grant", "copy", "vacuum", "analyze", "checkpoint",
               "into"):
        assert kw in nl_to_sql.FORBIDDEN_KEYWORDS


# --- A3: CTE write tricks rejected; CTE bodies still scanned ----------------
CTE_WRITE_CASES = [
    ("cte insert", "WITH t AS (INSERT INTO jobs SELECT * FROM jobs) "
                   "SELECT * FROM t;", "forbidden keyword: insert"),
    ("cte delete returning", "WITH t AS (DELETE FROM labor_logs RETURNING *) "
                             "SELECT * FROM t", "forbidden keyword: delete"),
    ("cte update returning", "WITH t AS (UPDATE anomaly_alerts SET "
                             "severity='high' RETURNING *) SELECT * FROM t",
     "forbidden keyword: update"),
    ("cte drop", "WITH t AS (DROP TABLE jobs) SELECT 1",
     "forbidden keyword: drop"),
    ("multi-cte write", "WITH t AS (SELECT 1), u AS (DELETE FROM jobs) "
                        "SELECT * FROM t", "forbidden keyword: delete"),
    ("write as outer statement", "WITH t AS (SELECT 1) DELETE FROM jobs",
     "forbidden keyword: delete"),
    ("cte body bad relation", "WITH t AS (SELECT * FROM pg_catalog.pg_tables) "
                              "SELECT * FROM t",
     "relation not allowed: pg_catalog.pg_tables"),
]


@pytest.mark.parametrize("label, sql, reason", CTE_WRITE_CASES,
                         ids=[c[0] for c in CTE_WRITE_CASES])
def test_cte_write_tricks_rejected(label, sql, reason):
    ok, verdict = nl_to_sql.validate_sql(sql)
    assert not ok, f"{label}: expected rejection, got ok with {verdict!r}"
    assert verdict == reason, f"{label}: {verdict!r}"


# --- A4: SELECT ... INTO rejected -------------------------------------------
@pytest.mark.parametrize("sql", [
    "SELECT * INTO evil FROM jobs",
    "SELECT * INTO TEMP TABLE evil FROM jobs",
    "WITH t AS (SELECT 1) SELECT * INTO evil FROM t",
    "SELECT job_number INTO evil FROM jobs WHERE id = 1",
], ids=["plain", "temp", "after-cte", "with-where"])
def test_select_into_rejected(sql):
    ok, verdict = nl_to_sql.validate_sql(sql)
    assert not ok
    assert verdict == "forbidden keyword: into"


# --- A5: comments rejected (including the pinned fail-closed deviation) -----
@pytest.mark.parametrize("sql", [
    "SELECT 1 -- comment",
    "/* comment */ SELECT 1",
    "SELECT 1 /* mid */ FROM jobs",
    "SELECT * FROM jobs -- trailing comment",
    "SELECT * FROM jobs\n-- newline comment",
], ids=["dash-suffix", "block-prefix", "block-mid", "dash-trailing",
        "dash-newline"])
def test_comments_rejected(sql):
    ok, verdict = nl_to_sql.validate_sql(sql)
    assert not ok
    assert verdict == "comments are not allowed"


def test_comment_marker_inside_string_literal_rejected():
    """Pinned deviation (fail-closed by design): '--' is checked BEFORE the
    literal masking, so even a comment marker inside a string literal is
    rejected. Over-rejection here is safe; under-rejection would not be."""
    ok, verdict = nl_to_sql.validate_sql("SELECT 'a--b' AS note FROM jobs")
    assert not ok
    assert verdict == "comments are not allowed"


def test_semicolon_inside_string_literal_rejected():
    """Pinned deviation (fail-closed by design): ';' is checked BEFORE the
    literal masking, so a semicolon inside a string literal is rejected as a
    multi-statement."""
    ok, verdict = nl_to_sql.validate_sql("SELECT 'a;b' AS note FROM jobs")
    assert not ok
    assert verdict == "multi-statement"


# --- A6: non-allowlist relations rejected -----------------------------------
RELATION_CASES = [
    ("pg_catalog", "SELECT * FROM pg_catalog.pg_tables",
     "relation not allowed: pg_catalog.pg_tables"),
    ("pg_catalog upper", "SELECT * FROM PG_CATALOG.PG_TABLES",
     "relation not allowed: PG_CATALOG.PG_TABLES"),
    ("information_schema", "SELECT * FROM information_schema.tables",
     "relation not allowed: information_schema.tables"),
    ("information_schema cols", "SELECT * FROM information_schema.columns",
     "relation not allowed: information_schema.columns"),
    ("other schema", "SELECT * FROM other_schema.jobs",
     "relation not allowed: other_schema.jobs"),
    ("public-qualified pg", "SELECT * FROM public.pg_tables",
     "relation not allowed: public.pg_tables"),
    ("secret table", "SELECT * FROM secret_table",
     "relation not allowed: secret_table"),
    ("comma mix trailing", "SELECT * FROM jobs, secret_table",
     "relation not allowed: secret_table"),
    ("comma mix leading", "SELECT * FROM secret_table, jobs",
     "relation not allowed: secret_table"),
    ("join pg_proc", "SELECT * FROM jobs j JOIN pg_proc ON TRUE",
     "relation not allowed: pg_proc"),
    ("union mix", "SELECT * FROM jobs UNION SELECT * FROM secret_table",
     "relation not allowed: secret_table"),
    ("subquery from", "SELECT * FROM (SELECT * FROM secret_table) x",
     "relation not allowed: secret_table"),
    ("function from", "SELECT * FROM generate_series(1, 10)",
     "relation not allowed: generate_series"),
    ("lateral", "SELECT * FROM jobs, LATERAL (SELECT 1) x",
     "relation not allowed: LATERAL"),
    ("dblink", "SELECT * FROM dblink('dbname=x', 'SELECT 1') d",
     "relation not allowed: dblink"),
    ("oracle dual", "SELECT 1 FROM dual", "relation not allowed: dual"),
]


@pytest.mark.parametrize("label, sql, reason", RELATION_CASES,
                         ids=[c[0] for c in RELATION_CASES])
def test_non_allowlist_relations_rejected(label, sql, reason):
    ok, verdict = nl_to_sql.validate_sql(sql)
    assert not ok, f"{label}: expected rejection, got ok with {verdict!r}"
    assert verdict == reason, f"{label}: {verdict!r}"


def test_allowed_relations_pin():
    """The allowlist is exactly the 11 base tables (incl. the multi-tenant
    companies client registry) + 6 SoCal benchmark tables + 2 SoCal
    capital-flows relations (funding_schema.sql) + 2 SEC peer-ratios
    relations (peer_ratios_schema.sql) + 6 matviews -- where the 5 raw SMB
    mv_* names are REPLACED by the 5 company-scoped scoped_* wrapper views
    (the AI-SQL role has no grant on the raw matviews; it reads them only
    through the scoped_* views, which carry the RLS GUC predicate)."""
    assert nl_to_sql.ALLOWED_RELATIONS == {
        # companies = the multi-tenant client-workspace registry
        # (migrations/001_add_companies.sql)
        "companies",
        "customers", "job_types", "employees", "jobs", "financial_ledger",
        "labor_logs", "market_benchmarks", "anomaly_alerts", "forecasts",
        "backtest_results",
        # SoCal regional benchmark tables (schema.sql sections 2.11-2.16)
        "regional_industry_profiles", "socal_wage_benchmarks",
        "socal_macro_indicators", "socal_business_patterns",
        "socal_loan_activity", "socal_harvest_log",
        # SoCal capital flows (funding_schema.sql, harvest_socal_capital.py)
        "socal_capital_events", "socal_county_capital_summary",
        # SEC peer ratios (peer_ratios_schema.sql, harvest_sec_peer_ratios.py)
        "public_peer_financials", "mv_vertical_financial_quartiles",
        # company-scoped wrapper views over the raw SMB matviews
        # (analytics_views.sql section 9 + migrations/002_add_rls.sql) -- the
        # raw mv_monthly_summary / mv_customer_profitability /
        # mv_job_type_profitability / mv_labor_cost_analysis /
        # mv_benchmark_comparison are NOT allowlisted
        "scoped_monthly_summary", "scoped_customer_profitability",
        "scoped_job_type_profitability", "scoped_labor_cost_analysis",
        "scoped_benchmark_comparison", "mv_socal_rate_guidance",
    }
    assert len(nl_to_sql.ALLOWED_RELATIONS) == 27
    # the 5 RAW SMB matviews must be rejected at the parser level now
    for raw in ("mv_monthly_summary", "mv_customer_profitability",
                "mv_job_type_profitability", "mv_labor_cost_analysis",
                "mv_benchmark_comparison"):
        assert raw not in nl_to_sql.ALLOWED_RELATIONS
        ok, verdict = nl_to_sql.validate_sql(f"SELECT * FROM {raw}")
        assert not ok and verdict == f"relation not allowed: {raw}"
    # ... and the system prompt advertises exactly the same surface.
    for rel in nl_to_sql.ALLOWED_RELATIONS:
        assert rel in nl_to_sql.SYSTEM_PROMPT


# --- A7: false positives MUST pass (literal masking + LIMIT contract) -------
FP_CASES = [
    ("like update literal",
     "SELECT * FROM scoped_customer_profitability WHERE customer_name "
     "LIKE '%update%'",
     "SELECT * FROM scoped_customer_profitability WHERE customer_name "
     "LIKE '%update%' LIMIT 200"),
    ("drop in string literal",
     "SELECT 'DROP TABLE jobs' AS note FROM jobs",
     "SELECT 'DROP TABLE jobs' AS note FROM jobs LIMIT 200"),
    ("write verbs in string literal",
     "SELECT 'insert or delete or update' AS note FROM jobs",
     "SELECT 'insert or delete or update' AS note FROM jobs LIMIT 200"),
    ("doubled-quote escape",
     "SELECT 'it''s fine' AS note FROM jobs",
     "SELECT 'it''s fine' AS note FROM jobs LIMIT 200"),
    ("public-prefixed table",
     "SELECT * FROM public.jobs",
     "SELECT * FROM public.jobs LIMIT 200"),
    ("public-prefixed scoped view",
     "SELECT * FROM public.scoped_monthly_summary",
     "SELECT * FROM public.scoped_monthly_summary LIMIT 200"),
    ("join with aliases",
     "SELECT j.job_number, c.name FROM jobs j "
     "JOIN customers c ON c.id = j.customer_id",
     "SELECT j.job_number, c.name FROM jobs j "
     "JOIN customers c ON c.id = j.customer_id LIMIT 200"),
    ("explicit limit preserved",
     "SELECT * FROM jobs LIMIT 10",
     "SELECT * FROM jobs LIMIT 10"),
    ("explicit lowercase limit preserved",
     "select * from jobs limit 3",
     "select * from jobs limit 3"),
    ("whitespace stripped",
     "  SELECT 1  ",
     "SELECT 1 LIMIT 200"),
    ("trailing semicolon tolerated",
     "SELECT job_type FROM scoped_job_type_profitability;",
     "SELECT job_type FROM scoped_job_type_profitability LIMIT 200"),
    ("comma-from all allowlisted",
     "SELECT a.revenue, b.name FROM scoped_monthly_summary a, customers b",
     "SELECT a.revenue, b.name FROM scoped_monthly_summary a, customers b "
     "LIMIT 200"),
    ("like drop literal",
     "SELECT * FROM scoped_labor_cost_analysis WHERE job_number LIKE '%drop%' "
     "AND status = 'quoted'",
     "SELECT * FROM scoped_labor_cost_analysis WHERE job_number LIKE '%drop%' "
     "AND status = 'quoted' LIMIT 200"),
    ("single-read cte",
     "WITH m AS (SELECT * FROM scoped_monthly_summary) SELECT * FROM m",
     "WITH m AS (SELECT * FROM scoped_monthly_summary) SELECT * FROM m "
     "LIMIT 200"),
    ("multi-read cte",
     "WITH a AS (SELECT * FROM jobs), b AS (SELECT * FROM customers) "
     "SELECT * FROM a JOIN b ON TRUE",
     "WITH a AS (SELECT * FROM jobs), b AS (SELECT * FROM customers) "
     "SELECT * FROM a JOIN b ON TRUE LIMIT 200"),
    ("recursive read cte",
     "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r "
     "WHERE n < 5) SELECT * FROM r",
     "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r "
     "WHERE n < 5) SELECT * FROM r LIMIT 200"),
]


@pytest.mark.parametrize("label, sql, expected", FP_CASES,
                         ids=[c[0] for c in FP_CASES])
def test_false_positives_pass_with_exact_output(label, sql, expected):
    ok, verdict = nl_to_sql.validate_sql(sql)
    assert ok, f"{label}: false positive rejected with {verdict!r}"
    assert verdict == expected, f"{label}: returned {verdict!r}"


def test_auto_limit_appended_exactly_once():
    ok, verdict = nl_to_sql.validate_sql("SELECT * FROM jobs")
    assert ok
    assert verdict == f"SELECT * FROM jobs LIMIT {nl_to_sql.DEFAULT_LIMIT}"
    assert verdict.endswith(f" LIMIT {nl_to_sql.DEFAULT_LIMIT}")
    assert verdict.upper().count("LIMIT") == 1   # never double-appended


def test_limit_constants_pinned():
    assert nl_to_sql.MAX_ROWS == 500
    assert nl_to_sql.DEFAULT_LIMIT == 200


# --- A9: quoting contract (pinned as implemented) ----------------------------
@pytest.mark.parametrize("sql", [
    'SELECT "name" FROM jobs',
    'SELECT * FROM "pg_catalog"."pg_tables"',
    'SELECT job_number AS "Job #" FROM jobs',
    'SELECT * FROM jobs WHERE name = "x"',
], ids=["benign-column", "sneaky-pg_catalog", "quoted-alias", "quoted-value"])
def test_double_quoted_identifiers_rejected(sql):
    """Pinned: ANY double quote is rejected (validate_sql docstring step 2b) --
    a defense against FROM "pg_catalog"."pg_tables" hiding from the plain
    token scans. Even benign double-quoted aliases are refused."""
    ok, verdict = nl_to_sql.validate_sql(sql)
    assert not ok
    assert verdict == "double-quoted identifiers are not allowed"


@pytest.mark.parametrize("sql", [
    "SELECT $$ DROP TABLE jobs $$ AS note FROM jobs",
    "SELECT $tag$ delete me $tag$ AS note FROM jobs",
    "SELECT $$ update me $$ AS note FROM jobs",
], ids=["dollar-dollar", "tagged-dollar", "dollar-update"])
def test_dollar_quoted_bodies_masked_and_accepted(sql):
    """Pinned: closed dollar-quoted bodies are blanked by _mask_literals, so
    keywords inside them cannot trip the scans (the SQL stays a harmless
    string constant in the result set)."""
    ok, verdict = nl_to_sql.validate_sql(sql)
    assert ok, f"expected pass, got {verdict!r}"
    assert verdict.endswith(f" LIMIT {nl_to_sql.DEFAULT_LIMIT}")


def test_dollar_quote_with_semicolons_rejected():
    """Pinned deviation (fail-closed): the semicolon check runs BEFORE literal
    masking, so even dollar-quoted semicolons are rejected as a
    multi-statement."""
    ok, verdict = nl_to_sql.validate_sql(
        "SELECT $$; DROP TABLE jobs;$$ AS note FROM jobs")
    assert not ok
    assert verdict == "multi-statement"


def test_unclosed_dollar_quote_passes_parser_but_fails_db():
    """Pinned deviation: an unterminated dollar quote is invisible to the
    masking regex, so the parser accepts it -- the database then rejects it
    with a clean syntax error (proven live in Part B, see
    test_malformed_sql_that_passes_parser_fails_cleanly)."""
    ok, verdict = nl_to_sql.validate_sql("SELECT $tag$ open FROM jobs")
    assert ok
    assert verdict.endswith("LIMIT 200")


# --- A10: empty statements ----------------------------------------------------
@pytest.mark.parametrize("sql", [None, "", "   ", ";", "  ;  "],
                         ids=["none", "empty", "spaces", "bare-semicolon",
                              "spaces-semicolon"])
def test_empty_statements_rejected(sql):
    """Bare ';' / whitespace-only used to slip through as ' LIMIT 200'
    (found by the adversarial pass; fixed by re-checking emptiness after the
    trailing-';' strip)."""
    ok, verdict = nl_to_sql.validate_sql(sql)
    assert not ok
    assert verdict == "empty statement"


def test_double_semicolon_is_multi_statement():
    ok, verdict = nl_to_sql.validate_sql(";;")
    assert not ok
    assert verdict == "multi-statement"


# ===========================================================================
# B) read-only execution against the LIVE warehouse
# ===========================================================================

MV_EXPECTED_COLUMNS = {
    "scoped_job_type_profitability":
        {"job_type", "revenue", "materials_cost", "labor_cost",
         "total_cost", "gross_margin", "margin_pct", "job_count"},
    "scoped_monthly_summary":
        {"month_start", "year", "month_name", "revenue", "total_expense",
         "gross_profit", "gross_margin_pct", "job_completions"},
    "scoped_customer_profitability":
        {"customer_name", "revenue", "job_cost", "acquisition_cost",
         "total_cost", "gross_margin", "margin_pct", "job_count",
         "is_whale"},
}


@pytest.mark.parametrize("mv", sorted(MV_EXPECTED_COLUMNS))
def test_matview_queryable_readonly_and_consistent(live_conn, mv):
    """B1: SELECT * through run_readonly_query (as the restricted role, with
    the live demo workspace's company_id) returns exactly the row count a
    plain psycopg2 connection sees through the RAW matview, with the
    documented columns. (The role reads the scoped_* wrapper view; the
    live warehouse holds only company 1, so scoped == raw there.)"""
    res = nl_to_sql.run_readonly_query(f"SELECT * FROM {mv}",
                                       query_timeout=10, company_id=1)
    assert "error" not in res, res.get("error")
    raw_matview = mv.replace("scoped_", "mv_", 1)
    expected_rows = plain_count(live_conn, raw_matview)
    assert res["rowcount"] == expected_rows > 0
    assert len(res["rows"]) == res["rowcount"]
    assert set(MV_EXPECTED_COLUMNS[mv]) <= set(res["columns"])
    assert isinstance(res["elapsed_ms"], int)


@pytest.mark.parametrize("table", ["jobs", "labor_logs", "financial_ledger",
                                   "market_benchmarks", "anomaly_alerts"])
def test_run_readonly_count_matches_plain_connection(live_conn, table):
    """B1: COUNT(*) through the read-only executor (restricted role, active
    workspace company_id 1) == COUNT(*) through a plain psycopg2 connection."""
    res = nl_to_sql.run_readonly_query(f"SELECT COUNT(*) AS n FROM {table}",
                                       company_id=1)
    assert "error" not in res, res.get("error")
    assert res["rowcount"] == 1
    assert res["rows"][0][0] == plain_count(live_conn, table)


# --- B2: DB-level backstop: writes must fail with ZERO rows changed --------
MUST_FAIL_WRITES = [
    ("update",
     "UPDATE anomaly_alerts SET is_resolved = TRUE"),
    ("delete",
     "DELETE FROM anomaly_alerts"),
    ("insert",
     "INSERT INTO anomaly_alerts (anomaly_type, severity) "
     "VALUES ('expense_spike', 'low')"),
]


@pytest.mark.parametrize("label, sql", MUST_FAIL_WRITES, ids=[c[0] for c in
                                                              MUST_FAIL_WRITES])
def test_db_backstop_blocks_writes(live_conn, label, sql):
    """B2 (the SANCTIONED must-fail writes, now via the restricted role): the
    read-only transaction must reject each write before it touches anything
    (the read-only check even fires BEFORE the role's missing-write-grant
    check), and the table must hold exactly the same rows (and the same
    number of OPEN alerts) afterwards."""
    before = plain_count(live_conn, "anomaly_alerts")
    open_before = plain_count(live_conn, "anomaly_alerts", "NOT is_resolved")

    res = nl_to_sql.run_readonly_query(sql, query_timeout=5)

    assert res.get("error"), f"{label}: expected a read-only error"
    assert "read-only" in res["error"], res["error"]
    assert res["rowcount"] == 0 and res["rows"] == []

    after = plain_count(live_conn, "anomaly_alerts")
    open_after = plain_count(live_conn, "anomaly_alerts", "NOT is_resolved")
    assert after == before, f"{label}: anomaly_alerts changed {before} -> {after}"
    assert open_after == open_before


def test_db_backstop_blocks_parser_gap_refresh(live_conn):
    """Layer 3 behind layer 1: REFRESH MATERIALIZED VIEW carries no forbidden
    keyword, so the parser lets it through -- and the read-only transaction
    still blocks it (data write), with zero effect on the matview. (Via the
    restricted role this is doubly blocked: read-only txn first, and the raw
    matview is not even granted to nl2sql_reader.)"""
    ok, verdict = nl_to_sql.validate_sql(
        "REFRESH MATERIALIZED VIEW mv_monthly_summary")
    assert ok  # documented parser gap; the DB is the backstop

    before = plain_count(live_conn, "mv_monthly_summary")
    res = nl_to_sql.run_readonly_query(
        "REFRESH MATERIALIZED VIEW mv_monthly_summary", query_timeout=5)
    assert res.get("error") and "read-only" in res["error"], res.get("error")
    assert plain_count(live_conn, "mv_monthly_summary") == before


# --- B3: statement_timeout ---------------------------------------------------
def test_statement_timeout_kills_long_query(live_conn):
    """B3: pg_sleep(10) under a 2 s statement_timeout dies with the timeout
    error well before the sleep would finish."""
    started = time.perf_counter()
    res = nl_to_sql.run_readonly_query("SELECT pg_sleep(10)", query_timeout=2,
                                       company_id=1)
    wall = time.perf_counter() - started
    assert res.get("error"), "expected the timeout error"
    assert "timeout" in res["error"], res["error"]
    assert "2-second" in res["error"], res["error"]      # docstring wording
    assert res["rowcount"] == 0 and res["rows"] == []
    assert wall < 7.0, f"timeout took {wall:.1f}s wall clock"


def test_client_side_row_cap():
    """MAX_ROWS=500 caps what comes back even for a raw 700-row result
    (the cap is a run_readonly_query contract, independent of validate_sql)."""
    res = nl_to_sql.run_readonly_query("SELECT generate_series(1, 700)")
    assert "error" not in res, res.get("error")
    assert res["rowcount"] == nl_to_sql.MAX_ROWS == 500
    assert res.get("truncated") is True
    assert len(res["rows"]) == 500
    assert res["rows"][0][0] == 1 and res["rows"][-1][0] == 500


def test_malformed_sql_that_passes_parser_fails_cleanly():
    """Unclosed dollar quote passes the parser (pinned deviation in Part A)
    but PostgreSQL rejects it -- run_readonly_query must return a clean error
    dict, never raise."""
    res = nl_to_sql.run_readonly_query("SELECT $tag$ open FROM jobs")
    assert res.get("error")
    assert "unterminated dollar-quoted string" in res["error"]
    assert res["rowcount"] == 0 and res["columns"] == []


# --- B4: guardrails run BEFORE execution in the ask() pipeline ---------------
def test_ask_never_executes_parser_rejected_sql(live_conn, monkeypatch):
    """B4: a hostile model that answers 'DROP TABLE jobs' twice must yield
    sql=None with a clean error, and run_readonly_query must not even be
    called (tripwire)."""
    scripted = _ScriptedChat(["DROP TABLE jobs"])

    def tripwire(sql, query_timeout=5, company_id=None, db=None):
        raise AssertionError("run_readonly_query was called for SQL that "
                             "validate_sql rejected -- guardrail bypass!")

    monkeypatch.setattr(nl_to_sql, "chat", scripted)
    monkeypatch.setattr(nl_to_sql, "run_readonly_query", tripwire)
    before = plain_count(live_conn, "anomaly_alerts")

    res = nl_to_sql.ask("drop everything", _mock_cfg(), company_id=1)

    assert res["sql"] is None
    assert res["attempts"] == 2
    assert "Could not produce a valid read-only query" in res["error"]
    assert "forbidden keyword: drop" in res["error"]
    assert res["rowcount"] == 0 and res["rows"] == []
    assert plain_count(live_conn, "anomaly_alerts") == before


def test_ask_full_pipeline_with_mocked_llm_executes_live(live_conn,
                                                         monkeypatch):
    """Happy path: mocked LLM emits one clean SELECT; ask() validates,
    auto-Limits and runs it read-only as the restricted role for the active
    workspace (company_id=1); the count matches a plain connection."""
    monkeypatch.setattr(nl_to_sql, "chat",
                        _ScriptedChat(["SELECT COUNT(*) AS n FROM jobs"]))
    res = nl_to_sql.ask("how many jobs do I have?", _mock_cfg(), company_id=1)
    assert res["sql"] == f"SELECT COUNT(*) AS n FROM jobs LIMIT 200"
    assert res["attempts"] == 1
    assert "error" not in res or res["error"] is None
    assert res["rowcount"] == 1
    assert res["rows"][0][0] == plain_count(live_conn, "jobs")


def test_ask_surfaces_db_errors_cleanly(live_conn, monkeypatch):
    """Accuracy boundary: validate_sql checks RELATIONS, not COLUMNS, so a
    hallucinated column passes the parser and the DB error must surface as a
    clean dict (no exception, no partial results)."""
    monkeypatch.setattr(nl_to_sql, "chat", _ScriptedChat(
        ["SELECT * FROM jobs WHERE no_such_column = 1"]))
    res = nl_to_sql.ask("bogus column question", _mock_cfg(), company_id=1)
    assert res["sql"] is not None                     # parser accepted it
    assert res.get("error")
    assert "no_such_column" in res["error"]
    assert res["rowcount"] == 0 and res["rows"] == []


# ===========================================================================
# R) row-level security enforcement against the LIVE warehouse
#    (migrations/002_add_rls.sql: nl2sql_reader role + 9 policies + scoped_*
#    views). The TWO-workspace proof lives in test_multitenant_pdf.py -- the
#    live warehouse holds a single workspace, so these pins use company_id 1.
# ===========================================================================

ROLE_NAME = "nl2sql_reader"
CLIENT_TABLES_RLS = ("customers", "job_types", "employees", "jobs",
                     "financial_ledger", "labor_logs", "anomaly_alerts",
                     "forecasts")
RAW_MATVIEWS = ("mv_monthly_summary", "mv_customer_profitability",
                "mv_job_type_profitability", "mv_labor_cost_analysis",
                "mv_benchmark_comparison")
GRANTED_GLOBAL = ("market_benchmarks", "backtest_results",
                  "mv_socal_rate_guidance", "mv_vertical_financial_quartiles",
                  "regional_industry_profiles", "socal_wage_benchmarks",
                  "socal_macro_indicators", "socal_business_patterns",
                  "socal_loan_activity", "socal_harvest_log",
                  "socal_capital_events", "socal_county_capital_summary",
                  "public_peer_financials")
SCOPED_VIEWS = ("scoped_monthly_summary", "scoped_customer_profitability",
                "scoped_job_type_profitability", "scoped_labor_cost_analysis",
                "scoped_benchmark_comparison")


@pytest.fixture(scope="module")
def role_conn():
    """Module-scoped connection AS THE RESTRICTED ROLE (nl2sql_reader), built
    by nl_to_sql's own production DSN builder. This is the exact connection
    path the AI-SQL executor uses (minus the app-level GUC/timeout setup,
    which the proofs below issue explicitly). Skips when the role is absent
    or unreachable."""
    dsn = nl_to_sql._reader_dsn(LIVE_URL)
    try:
        conn = psycopg2.connect(dsn, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute("SELECT current_user")
            assert cur.fetchone()[0] == ROLE_NAME
    except Exception as exc:                    # noqa: BLE001
        pytest.skip(f"nl2sql_reader role not reachable: {exc!r}")
    yield conn
    try:
        conn.close()
    except Exception:                           # noqa: BLE001
        pass


def test_role_exists_with_expected_attributes(live_conn):
    """R1: nl2sql_reader is LOGIN, NOSUPERUSER, NOCREATEDB, NOCREATEROLE,
    NOINHERIT and -- the point of the whole design -- NOBYPASSRLS."""
    with live_conn.cursor() as cur:
        cur.execute("SELECT rolsuper, rolcanlogin, rolcreatedb, "
                    "rolcreaterole, rolinherit, rolbypassrls "
                    "FROM pg_roles WHERE rolname = %s", (ROLE_NAME,))
        row = cur.fetchone()
    assert row is not None, f"{ROLE_NAME} role is missing (apply migration 002)"
    rolsuper, canlogin, createdb, createrole, inherit, bypassrls = row
    assert canlogin is True
    assert rolsuper is False and createdb is False and createrole is False
    assert bypassrls is False and inherit is False


def test_rls_policies_present(live_conn):
    """R2: all 9 tenant_isolation_* policies exist (8 client tables +
    companies) and every one of those tables has RLS ENABLED."""
    expected = {f"tenant_isolation_{t}" for t in CLIENT_TABLES_RLS}
    expected.add("tenant_isolation_companies")
    with live_conn.cursor() as cur:
        cur.execute("SELECT polname FROM pg_policy")
        names = {r[0] for r in cur.fetchall()}
        cur.execute("SELECT relname FROM pg_class WHERE relrowsecurity "
                    "AND relnamespace = 'public'::regnamespace")
        rls_tables = {r[0] for r in cur.fetchall()}
    assert expected <= names, names
    assert set(CLIENT_TABLES_RLS) | {"companies"} <= rls_tables


def test_policy_expression_fail_closed(live_conn):
    """R2b: every policy's USING and WITH CHECK expressions are exactly the
    fail-closed GUC comparison (unset/empty app.current_company_id -> NULL ->
    zero rows), rendered back to SQL text with pg_get_expr."""
    with live_conn.cursor() as cur:
        cur.execute(
            "SELECT polname, pg_get_expr(polqual, polrelid), "
            "       pg_get_expr(polwithcheck, polrelid) FROM pg_policy")
        rows = cur.fetchall()
    assert len(rows) == 9
    for polname, using, check in rows:
        for expr in (using, check):
            assert "current_setting('app.current_company_id'::text, true)" \
                in expr, f"{polname}: {expr!r}"
            assert expr.startswith("(company_id = (NULLIF("), \
                f"{polname}: unexpected expression {expr!r}"


def test_grant_matrix_including_negatives(live_conn):
    """R3: the role has SELECT on the client tables + companies + ALL the
    global/market relations + the 5 scoped views -- and on NOTHING else that
    matters: the 5 RAW SMB matviews are explicitly DENIED (has_table_privilege
    negative checks; the whole reason the scoped_* views exist)."""
    with live_conn.cursor() as cur:
        for rel in CLIENT_TABLES_RLS + ("companies",) + GRANTED_GLOBAL \
                + SCOPED_VIEWS:
            cur.execute("SELECT has_table_privilege(%s, %s, 'SELECT')",
                        (ROLE_NAME, rel))
            assert cur.fetchone()[0] is True, f"{rel}: expected SELECT grant"
        for rel in RAW_MATVIEWS:
            cur.execute("SELECT has_table_privilege(%s, %s, 'SELECT')",
                        (ROLE_NAME, rel))
            assert cur.fetchone()[0] is False, \
                f"{rel}: the raw matview MUST NOT be granted to {ROLE_NAME}"
        for rel in ("mv_monthly_summary", "jobs"):
            for priv in ("INSERT", "UPDATE", "DELETE"):
                cur.execute("SELECT has_table_privilege(%s, %s, %s)",
                            (ROLE_NAME, rel, priv))
                assert cur.fetchone()[0] is False, \
                    f"{rel}: {priv} must be denied for {ROLE_NAME}"


def test_role_session_scopes_counts(role_conn, live_conn):
    """R4 (the live two-sided count proof): with the GUC set to the active
    workspace, the role sees exactly the same counts a plain superuser
    connection sees; with NO GUC the role sees ZERO client rows
    (fail-closed); and a WHERE company_id=<other> returns 0 because RLS
    ANDs with the caller's predicate."""
    with live_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM jobs")
        (pg_jobs,) = cur.fetchone()
        cur.execute("SELECT count(*) FROM jobs WHERE company_id = 1")
        (pg_jobs_c1,) = cur.fetchone()
    assert pg_jobs == pg_jobs_c1 > 0     # live = single-workspace warehouse

    with role_conn.cursor() as cur:
        cur.execute("SET app.current_company_id = '1'")
        cur.execute("SELECT count(*) FROM jobs")
        (scoped_jobs,) = cur.fetchone()
        cur.execute("SELECT count(*) FROM jobs WHERE company_id = 2")
        (cross_where,) = cur.fetchone()
        cur.execute("SELECT count(*) FROM customers")
        (scoped_customers,) = cur.fetchone()
    role_conn.rollback()                 # ends the txn; the SET is undone
    assert scoped_jobs == pg_jobs        # GUC=1 -> exactly company 1's rows
    assert cross_where == 0              # RLS ANDs with WHERE company_id = 2
    with role_conn.cursor() as cur:      # no GUC on the fresh transaction
        cur.execute("SELECT count(*) FROM jobs")
        (unscoped_jobs,) = cur.fetchone()
        cur.execute("SELECT count(*) FROM companies")
        (unscoped_companies,) = cur.fetchone()
    assert unscoped_jobs == 0            # no GUC -> fail-closed zero rows
    assert unscoped_companies == 0       # same for the companies registry


def test_role_session_scoped_views_match_raw(role_conn, live_conn):
    """R5: through the scoped_* wrapper views the role (GUC=1) sees exactly
    the rows the superuser sees through the RAW matviews (single-workspace
    warehouse)."""
    for scoped in SCOPED_VIEWS:
        raw = scoped.replace("scoped_", "mv_", 1)
        with role_conn.cursor() as cur:
            cur.execute("SET LOCAL app.current_company_id = '1'")
            cur.execute(f"SELECT count(*) FROM {scoped}")  # code constant
            (n_scoped,) = cur.fetchone()
        role_conn.rollback()             # SET LOCAL + any error contained
        with live_conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {raw}")     # code constant
            (n_raw,) = cur.fetchone()
        assert n_scoped == n_raw > 0, \
            f"{scoped}={n_scoped} vs raw {raw}={n_raw}"


def test_role_session_raw_matview_permission_denied(role_conn):
    """R5b: a DIRECT SELECT on a raw SMB matview is denied for the role --
    the scoped_* views are the only read surface (clean psycopg2 exception,
    mapped to a clean message by run_readonly_query)."""
    for raw in RAW_MATVIEWS:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            with role_conn.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM {raw}")  # code constant
                cur.fetchall()
        role_conn.rollback()             # recover from the expected denial


def test_role_session_registry_row_only(role_conn):
    """R6: with the GUC set, the role's view of the companies registry is
    exactly ONE row -- its own workspace (it cannot enumerate the client
    list)."""
    with role_conn.cursor() as cur:
        cur.execute("SET app.current_company_id = '1'")
        cur.execute("SELECT company_id, company_name FROM companies")
        rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 1
    assert "BrightPath" in rows[0][1]


def test_role_session_writes_blocked_outside_readonly_txn(role_conn,
                                                          live_conn):
    """R7 (role-level, no read-only transaction): even OUTSIDE the AI
    executor's read-only transaction, the role cannot write -- missing
    grants (and, for rows it could touch, the WITH CHECK policy) block it,
    and the live tables hold exactly the same rows afterwards. psycopg2
    opens a read-write transaction by default, so this proves the block is
    the ROLE's, not the executor's read-only setting."""
    before = plain_count(live_conn, "anomaly_alerts")
    with role_conn.cursor() as cur:
        cur.execute("SET app.current_company_id = '1'")
        with pytest.raises(psycopg2.Error):
            cur.execute("UPDATE anomaly_alerts SET is_resolved = TRUE")
            cur.fetchall()
    role_conn.rollback()                 # recovers + clears the session GUC
    assert plain_count(live_conn, "anomaly_alerts") == before


def test_run_readonly_query_fail_closed_without_company_id(live_conn):
    """R8 (pinned): run_readonly_query with company_id=None sets NO GUC, so
    client-data tables return ZERO rows through the executor -- and the
    scoped views are empty too. Market data is global and still visible."""
    for sql in ("SELECT COUNT(*) AS n FROM jobs",
                "SELECT COUNT(*) AS n FROM customers",
                "SELECT COUNT(*) AS n FROM scoped_monthly_summary",
                "SELECT COUNT(*) AS n FROM scoped_customer_profitability"):
        res = nl_to_sql.run_readonly_query(sql, query_timeout=5,
                                           company_id=None)
        assert "error" not in res, res.get("error")
        assert res["rows"][0][0] == 0, f"{sql}: expected fail-closed 0 rows"
    res = nl_to_sql.run_readonly_query(
        "SELECT COUNT(*) AS n FROM market_benchmarks", company_id=None)
    assert "error" not in res and res["rows"][0][0] > 0   # global data


def test_run_readonly_query_rejects_non_integer_company_id():
    """R9 (pinned): company_id must be int-coercible -- a bad value raises
    ValueError BEFORE any connection is opened (fail-closed, no DB round
    trip). The coerced int is the only thing string-interpolated into the
    SET LOCAL."""
    for bad in ("abc", "1; DROP TABLE jobs", "", "one"):
        with pytest.raises(ValueError):
            nl_to_sql.run_readonly_query("SELECT 1", company_id=bad)
    # ...and the coercion accepts int-like values (int('1'))
    res = nl_to_sql.run_readonly_query("SELECT COUNT(*) AS n FROM jobs",
                                       company_id="1")
    assert "error" not in res and res["rows"][0][0] == 2502


def test_ask_without_company_id_returns_zero_client_rows(live_conn,
                                                         monkeypatch):
    """R10 (pinned): ask() without a company_id fail-closes -- the pipeline
    runs fine but the restricted role sees zero client rows. The app ALWAYS
    passes the active workspace id (pinned in the AppTest flow tests)."""
    monkeypatch.setattr(nl_to_sql, "chat",
                        _ScriptedChat(["SELECT COUNT(*) AS n FROM jobs"]))
    res = nl_to_sql.ask("how many jobs do I have?", _mock_cfg())
    assert res["sql"] == f"SELECT COUNT(*) AS n FROM jobs LIMIT 200"
    assert res.get("error") is None
    assert res["rows"][0][0] == 0


def test_reader_dsn_builds_role_url():
    """R11: _reader_dsn swaps ONLY the user/password into the DATABASE_URL
    (host, port, db name and query params preserved), sourced from the
    config defaults (the documented migration role/password)."""
    dsn = nl_to_sql._reader_dsn("postgresql://postgres:pgpass@dbhost:5433/"
                                "smb_warehouse?connect_timeout=3")
    parsed = urllib.parse.urlsplit(dsn)
    assert parsed.hostname == "dbhost" and parsed.port == 5433
    assert parsed.path == "/smb_warehouse"
    assert parsed.query == "connect_timeout=3"
    assert urllib.parse.unquote(parsed.username or "") == ROLE_NAME
    assert urllib.parse.unquote(parsed.password or "") == \
        nl_to_sql.DEFAULT_DB_PASSWORD


# --- R12-R14: RLS-round isolation & QA audit additions (live) ---------------

RLS_GLOBAL_VISIBLE_LIVE = GRANTED_GLOBAL   # the 13 global/market relations


def test_r12_live_scoped_view_catalog_and_file_drift(live_conn):
    """R12 (live catalog audit): every scoped_* view carries
    security_barrier=true, its pg_get_viewdef embeds the exact fail-closed
    GUC predicate and reads FROM the right raw matview, the definitions in
    migrations/002_add_rls.sql and analytics_views.sql are semantically
    identical to each other AND to the live DB (no drift), and the raw
    matviews themselves are NOT granted to the role."""
    base_dir = os.path.dirname(os.path.abspath(__file__))

    def file_viewdefs(path):
        text = open(path, encoding="utf-8").read()
        out = {}
        for m in re.finditer(
                r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)\s+"
                r"WITH\s*\(security_barrier\)\s+AS\s+(.*?);",
                text, re.S | re.I):
            out[m.group(1).lower()] = " ".join(m.group(2).split()).lower()
        return out

    mig = file_viewdefs(os.path.join(base_dir, "migrations",
                                     "002_add_rls.sql"))
    ana = file_viewdefs(os.path.join(base_dir, "analytics_views.sql"))
    with live_conn.cursor() as cur:
        for scoped in SCOPED_VIEWS:
            raw = "mv_" + scoped[len("scoped_"):]
            cur.execute("SELECT reloptions FROM pg_class"
                        " WHERE oid = %s::regclass", (scoped,))
            (opts,) = cur.fetchone()
            assert opts is not None and "security_barrier=true" in opts, \
                (scoped, opts)
            cur.execute("SELECT pg_get_viewdef(%s::regclass, true)",
                        (scoped,))
            (viewdef,) = cur.fetchone()
            low = " ".join(viewdef.lower().split()).replace("::text", "")
            assert f"from {raw}" in low, viewdef
            assert ("nullif(current_setting('app.current_company_id', true),"
                    " '')::bigint") in low, viewdef
            # drift: both files define the view, identically, with the
            # exact fail-closed idiom
            assert scoped in mig and scoped in ana, scoped
            assert mig[scoped] == ana[scoped], (
                f"DRIFT between migrations/002 and analytics_views.sql for "
                f"{scoped}:\n  002: {mig[scoped]}\n  ana: {ana[scoped]}")
            for fdef in (mig[scoped], ana[scoped]):
                assert (f" from {raw} where company_id = nullif(" in fdef), \
                    fdef
                assert ("current_setting('app.current_company_id', true), "
                        "'')::bigint" in fdef), fdef


def test_r13_live_residual_visibility_no_guc(role_conn, live_conn):
    """R13 (the documented threat-model boundary, pinned on the LIVE
    warehouse): a restricted session WITHOUT any GUC sees
      * ZERO rows in the 8 client tables, companies registry and 5 scoped
        views (fail-closed),
      * PERMISSION DENIED on the 5 raw mv_* matviews,
      * ALL the global/market data -- exactly the same counts the plain
        connection sees (no tenant data lives there),
      * information_schema relation NAMES only (metadata every PostgreSQL
        role gets by default; carries no tenant data; the parser layer
        rejects SELECTs against it at the pipeline entry point)."""
    with live_conn.cursor() as cur:
        for rel in RLS_GLOBAL_VISIBLE_LIVE:
            cur.execute("SELECT to_regclass('public.' || %s) IS NOT NULL",
                        (rel,))
            if cur.fetchone()[0] is not True:
                continue
            cur.execute(f"SELECT COUNT(*) FROM {rel}")   # code constant
            (expected,) = cur.fetchone()
            got = plain_count(role_conn, rel)
            assert got == expected, f"{rel}: role saw {got}, plain {expected}"
    for rel in CLIENT_TABLES_RLS + ("companies",) + SCOPED_VIEWS:
        n = plain_count(role_conn, rel)
        assert n == 0, f"{rel}: no-GUC role session saw {n} rows (must be 0)"
    for raw in RAW_MATVIEWS:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            with role_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {raw}")  # code constant
                cur.fetchall()
        role_conn.rollback()
    # metadata-only residual: information_schema names are visible...
    got = plain_count(role_conn, "information_schema.tables")
    assert got > 0
    # ...but joining them to a client table yields ZERO tenant rows
    n = plain_count(role_conn,
                    "jobs j JOIN information_schema.tables t"
                    " ON t.table_name = 'jobs'", where="j.id > 0")
    assert n == 0, "information_schema join leaked tenant rows"


AI_VECTOR_ATTEMPTS_LIVE = (
    ("SET app.current_company_id = '1'", "forbidden keyword: set"),
    ("WITH sneak AS (INSERT INTO jobs (company_id, job_number)"
     " VALUES (1, 'HACK') RETURNING 1) SELECT * FROM sneak",
     "forbidden keyword: insert"),
    ("SELECT * FROM mv_monthly_summary",
     "relation not allowed: mv_monthly_summary"),
    ("COPY (SELECT company_id FROM jobs) TO PROGRAM 'curl evil.example'",
     "forbidden keyword: copy"),
    ("SELECT table_name FROM information_schema.tables",
     "relation not allowed: information_schema.tables"),
    ("SELECT 1; SET app.current_company_id = '1'", "multi-statement"),
    ("WITH flipped AS (SELECT set_config('app.current_company_id', '1',"
     " false)) SELECT j.id FROM jobs j, flipped f",
     "forbidden keyword: set_config"),
)


@pytest.mark.parametrize("sql, expect", AI_VECTOR_ATTEMPTS_LIVE,
                         ids=[a[0][:42] for a in AI_VECTOR_ATTEMPTS_LIVE])
def test_r14_ai_vector_bypass_never_executes(live_conn, monkeypatch, sql,
                                             expect):
    """R14a: through the REAL pipeline (ask -> generate_sql ->
    validate_sql -> run_readonly_query) every AI-vector bypass attempt is
    rejected at the parser BEFORE any statement reaches the live
    warehouse: sql is None, the note carries the exact guardrail verdict,
    zero rows, and the scripted LLM's hostile SQL never executed (the
    live job count proves the warehouse is untouched)."""
    scripted = _ScriptedChat([sql, sql])   # hostile SQL for both attempts
    monkeypatch.setattr(nl_to_sql, "chat", scripted)
    res = nl_to_sql.ask("show me everything", _mock_cfg(), company_id=1)
    assert res["sql"] is None, res
    assert res["rows"] == [] and res["rowcount"] == 0
    assert expect in res["note"], res["note"]
    assert len(scripted.calls) == 2        # the one documented repair retry
    # the live warehouse is untouched
    assert plain_count(live_conn, "jobs") == INTEGRITY_COUNTS["jobs"]


@pytest.mark.parametrize("sql, expect_error, expect_rows", [
    ("SET app.current_company_id = '1'", None, 0),          # harmless no-op
    ("SET app.current_company_id = '1'; SELECT company_id FROM jobs",
     "multi-statement", 0),
    ("SELECT 1; SET app.current_company_id = '1'", "multi-statement", 0),
    ("WITH flipped AS (SELECT set_config('app.current_company_id', '1',"
     " false)) SELECT j.id, j.company_id FROM jobs j, flipped f",
     "forbidden keyword: set_config", 0),
    ("SELECT COUNT(*) FROM mv_monthly_summary",
     "permission denied: the AI", 0),
])
def test_r14b_executor_guc_backstop_live(sql, expect_error, expect_rows):
    """R14b (live executor backstop): run_readonly_query enforces its own
    ONE-SELECT contract -- the two tenant-GUC override vectors
    (multi-statement smuggle; CTE-body set_config flip, both PROVEN leaks
    pre-fix) are rejected BEFORE any connection is opened, and a lone SET
    is a harmless no-op that the rollback discards (0 rows)."""
    res = nl_to_sql.run_readonly_query(sql, query_timeout=5, company_id=1)
    if expect_error is None:
        assert res.get("error") is None, res.get("error")
    else:
        got = res.get("error", "")
        assert got == expect_error or got.startswith(expect_error), got
    assert res["rowcount"] == expect_rows
    assert res["rows"] == []


@pytest.mark.parametrize("raw, label", [
    ("```sql\n" + VALID_SQL + "\n```", "sql-fence"),
    ("```\n" + VALID_SQL + "\n```", "bare-fence"),
    (VALID_SQL, "plain"),
    ("  " + VALID_SQL + "  ", "padded"),
], ids=["sql-fence", "bare-fence", "plain", "padded"])
def test_generate_sql_accepts_clean_sql(monkeypatch, raw, label):
    monkeypatch.setattr(nl_to_sql, "chat", _ScriptedChat([raw]))
    sql, attempts, note = nl_to_sql.generate_sql("best margins?", _mock_cfg())
    assert sql == VALID_SQL, f"{label}: {sql!r}"
    assert attempts == 1
    assert note == "Accepted after 1 attempt."


def test_generate_sql_repair_retry_feeds_reason_back(monkeypatch):
    """First attempt invalid -> the SECOND call's messages must contain the
    model's bad candidate (as an assistant turn) and the guardrail verdict
    ('forbidden keyword: drop') in the repair instruction; the retry then
    returns valid SQL with attempts == 2."""
    scripted = _ScriptedChat(["DROP TABLE jobs", VALID_SQL])
    monkeypatch.setattr(nl_to_sql, "chat", scripted)

    sql, attempts, note = nl_to_sql.generate_sql(
        "drop the jobs table please", _mock_cfg())

    assert sql == VALID_SQL
    assert attempts == 2
    assert note == "Accepted after 1 repair retry (2 attempts)."

    assert len(scripted.calls) == 2
    first, second = scripted.calls
    # first call: system prompt + the user question only
    assert [m["role"] for m in first] == ["system", "user"]
    assert first[0]["content"] == nl_to_sql.SYSTEM_PROMPT
    assert "drop the jobs table please" in first[1]["content"]
    # second call: original pair + assistant's bad SQL + repair instruction
    assert [m["role"] for m in second] == ["system", "user", "assistant",
                                           "user"]
    assert second[2]["content"] == "DROP TABLE jobs"
    repair = second[3]["content"]
    assert "forbidden keyword: drop" in repair
    assert "Re-issue" in repair


def test_generate_sql_strips_fences_before_validation(monkeypatch):
    """A fenced DROP must be stripped and validated, so the repair reason is
    the guardrail verdict, not a fence artifact."""
    scripted = _ScriptedChat(["```sql\nDROP TABLE jobs\n```", VALID_SQL])
    monkeypatch.setattr(nl_to_sql, "chat", scripted)
    sql, attempts, note = nl_to_sql.generate_sql("q", _mock_cfg())
    assert sql == VALID_SQL and attempts == 2
    assert "forbidden keyword: drop" in scripted.calls[1][3]["content"]


def test_generate_sql_both_attempts_invalid(monkeypatch):
    scripted = _ScriptedChat(["DROP TABLE jobs", "DELETE FROM jobs"])
    monkeypatch.setattr(nl_to_sql, "chat", scripted)
    sql, attempts, note = nl_to_sql.generate_sql("q", _mock_cfg())
    assert sql is None
    assert attempts == 2
    assert "Could not produce a valid read-only query" in note
    assert "forbidden keyword: delete" in note      # the LAST verdict


def test_generate_sql_runtimeerror_propagates_cleanly(monkeypatch):
    def boom(messages, cfg):
        raise RuntimeError("LLM request failed (mock): connection refused")

    monkeypatch.setattr(nl_to_sql, "chat", boom)
    with pytest.raises(RuntimeError, match="connection refused"):
        nl_to_sql.generate_sql("q", _mock_cfg())
    # ask() propagates the transport error too (documented contract) ...
    with pytest.raises(RuntimeError, match="connection refused"):
        nl_to_sql.ask("q", _mock_cfg())


@pytest.mark.parametrize("cfg, msg", [
    ({}, "No API base URL configured"),
    ({"base_url": "https://x.test/v1"}, "No model configured"),
    ({"base_url": "https://x.test/v1", "model": "m"}, "No API key configured"),
], ids=["no-base", "no-model", "no-key"])
def test_chat_missing_config_raises_runtimeerror(cfg, msg):
    """chat()'s contract: a clear RuntimeError BEFORE any network call (the
    request is only attempted once base/model/key are all present)."""
    with pytest.raises(RuntimeError, match=msg):
        nl_to_sql.chat([{"role": "user", "content": "hi"}], dict(cfg))


class _FakeResponse:
    """requests.Response stand-in for offline chat() tests (no network)."""

    def __init__(self, payload, status_code=200):
        import json as _json
        self._payload = payload
        self.status_code = status_code
        self.text = _json.dumps(payload)

    def json(self):
        return self._payload


def test_chat_strips_fences_from_a_real_shaped_response(monkeypatch):
    """End-to-end fence contract of the real chat() (requests.post mocked):
    an OpenAI-shaped body whose content is a ```sql fence comes back as the
    bare SELECT."""
    payload = {"choices": [{"message": {"content": "```sql\nSELECT 1\n```"}}]}
    monkeypatch.setattr(nl_to_sql.requests, "post",
                        lambda *a, **k: _FakeResponse(payload))
    out = nl_to_sql.chat([{"role": "user", "content": "q"}], _mock_cfg())
    assert out == "SELECT 1"


def test_chat_sends_auth_header_and_model(monkeypatch):
    seen = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        seen["url"] = url
        seen["headers"] = headers
        seen["json"] = json
        payload = {"choices": [{"message": {"content": "SELECT 1"}}]}
        return _FakeResponse(payload)

    monkeypatch.setattr(nl_to_sql.requests, "post", fake_post)
    out = nl_to_sql.chat([{"role": "user", "content": "q"}], _mock_cfg())
    assert out == "SELECT 1"
    assert seen["url"] == "https://mock.invalid/v1/chat/completions"
    assert seen["headers"]["Authorization"] == "Bearer test-key"
    assert seen["json"]["model"] == "test-model"
    assert seen["json"]["temperature"] == 0


def test_chat_http_error_raises_runtimeerror_with_status(monkeypatch):
    monkeypatch.setattr(
        nl_to_sql.requests, "post",
        lambda *a, **k: _FakeResponse({"error": "bad key"}, status_code=401))
    with pytest.raises(RuntimeError, match="HTTP 401"):
        nl_to_sql.chat([{"role": "user", "content": "q"}], _mock_cfg())


def test_chat_timeout_raises_runtimeerror(monkeypatch):
    import requests as _requests

    def fake_post(*a, **k):
        raise _requests.exceptions.Timeout("simulated timeout")

    monkeypatch.setattr(nl_to_sql.requests, "post", fake_post)
    with pytest.raises(RuntimeError, match="timed out"):
        nl_to_sql.chat([{"role": "user", "content": "q"}], _mock_cfg())


def test_chat_malformed_payload_raises_runtimeerror(monkeypatch):
    monkeypatch.setattr(nl_to_sql.requests, "post",
                        lambda *a, **k: _FakeResponse({"no": "choices"}))
    with pytest.raises(RuntimeError, match="Unexpected LLM response shape"):
        nl_to_sql.chat([{"role": "user", "content": "q"}], _mock_cfg())


def test_strip_code_fences_edges():
    f = nl_to_sql._strip_code_fences
    assert f("```sql\nSELECT 1\n```") == "SELECT 1"
    assert f("```\nSELECT 1\n```") == "SELECT 1"
    assert f("```sql\nSELECT 1") == "SELECT 1"        # unterminated
    assert f("SELECT 1") == "SELECT 1"                # no fence
    assert f("  ```sql\r\nSELECT 1\r\n```  ") == "SELECT 1"
    assert f("") == ""


# ===========================================================================
# D) config (offline; the REAL project .env is never read or written)
# ===========================================================================

def test_load_config_defaults_without_env_file(clean_env):
    cfg = nl_to_sql.load_config()
    assert cfg["base_url"] == "https://open.bigmodel.cn/api/paas/v4"
    assert cfg["model"] == "glm-4-flash"
    assert cfg["api_key"] == ""
    assert cfg["llm_timeout"] == 60
    assert cfg["query_timeout"] == 5
    # the restricted AI-SQL role credentials default to the documented
    # migration values (migrations/002_add_rls.sql)
    assert cfg["db_role"] == "nl2sql_reader"
    assert cfg["db_password"] == "nl2sql_readonly_local"
    assert set(cfg) == {"base_url", "api_key", "model", "llm_timeout",
                        "query_timeout", "db_role", "db_password"}


def test_db_role_password_env_overrides(clean_env, monkeypatch, tmp_path):
    """NL2SQL_DB_ROLE / NL2SQL_DB_PASSWORD follow the same precedence rules
    (.env file > process env > built-in default) as the other keys."""
    monkeypatch.setenv("NL2SQL_DB_ROLE", "custom_reader")
    cfg = nl_to_sql.load_config()
    assert cfg["db_role"] == "custom_reader"
    assert cfg["db_password"] == "nl2sql_readonly_local"   # default
    env_file = tmp_path / "settings.env"
    env_file.write_text("NL2SQL_DB_ROLE=file_reader\n"
                        "NL2SQL_DB_PASSWORD=file_pw\n", encoding="utf-8")
    cfg = nl_to_sql.load_config(env_path=env_file)
    assert cfg["db_role"] == "file_reader"                 # file beats env
    assert cfg["db_password"] == "file_pw"
    dsn = nl_to_sql._reader_dsn(
        "postgresql://postgres:postgres@localhost:5432/smb_warehouse",
        env_path=env_file)
    parsed = urllib.parse.urlsplit(dsn)
    assert urllib.parse.unquote(parsed.username or "") == "file_reader"
    assert urllib.parse.unquote(parsed.password or "") == "file_pw"


def test_environment_variables_used_when_no_file(clean_env, monkeypatch):
    monkeypatch.setenv("NL2SQL_MODEL", "env-model")
    monkeypatch.setenv("NL2SQL_QUERY_TIMEOUT", "9")
    cfg = nl_to_sql.load_config()
    assert cfg["model"] == "env-model"
    assert cfg["query_timeout"] == 9.0
    assert cfg["base_url"] == "https://open.bigmodel.cn/api/paas/v4"  # default


def test_env_file_takes_precedence_over_environment(clean_env, monkeypatch,
                                                    tmp_path):
    env_file = tmp_path / "settings.env"
    env_file.write_text("NL2SQL_MODEL=file-model\n"
                        "NL2SQL_BASE_URL=https://file.test/v1\n",
                        encoding="utf-8")
    monkeypatch.setenv("NL2SQL_MODEL", "env-model")
    cfg = nl_to_sql.load_config(env_path=env_file)
    assert cfg["model"] == "file-model"          # file beats the environment
    assert cfg["base_url"] == "https://file.test/v1"


def test_invalid_timeout_falls_back_and_floor_clamped(clean_env, tmp_path):
    env_file = tmp_path / "settings.env"
    env_file.write_text("NL2SQL_LLM_TIMEOUT=banana\n"
                        "NL2SQL_QUERY_TIMEOUT=0\n", encoding="utf-8")
    cfg = nl_to_sql.load_config(env_path=env_file)
    assert cfg["llm_timeout"] == 60              # unparsable -> default
    assert cfg["query_timeout"] == 0.5           # clamped to the 0.5 s floor


def test_save_load_roundtrip_all_five_keys(tmp_path):
    env_file = tmp_path / "settings.env"
    cfg = {"base_url": "https://roundtrip.test/v1", "api_key": "sk-QAtest-123",
           "model": "test-model-x", "llm_timeout": 33, "query_timeout": 7}
    written = nl_to_sql.save_config(cfg, env_path=env_file)
    assert written == env_file and env_file.exists()
    back = nl_to_sql.load_config(env_path=env_file)
    assert back["base_url"] == "https://roundtrip.test/v1"
    assert back["api_key"] == "sk-QAtest-123"
    assert back["model"] == "test-model-x"
    assert back["llm_timeout"] == 33.0
    assert back["query_timeout"] == 7.0


def test_save_load_via_patched_default_env_path(clean_env, tmp_path,
                                                monkeypatch):
    """The dashboard calls save_config()/load_config() WITHOUT an env_path
    (i.e. via ENV_PATH): with ENV_PATH patched to tmp_path, the round-trip
    works and the real project .env is never touched."""
    target = tmp_path / "settings.env"
    monkeypatch.setattr(nl_to_sql, "ENV_PATH", target)
    nl_to_sql.save_config({"base_url": "https://d.test/v1",
                           "api_key": "k2", "model": "m2",
                           "llm_timeout": 11, "query_timeout": 4})
    cfg = nl_to_sql.load_config()
    assert target.exists()
    assert cfg["api_key"] == "k2" and cfg["model"] == "m2"
    assert cfg["llm_timeout"] == 11.0 and cfg["query_timeout"] == 4.0


def test_save_config_leaves_no_tmp_file(tmp_path):
    env_file = tmp_path / "settings.env"
    nl_to_sql.save_config({"base_url": "", "api_key": "", "model": "",
                           "llm_timeout": 60, "query_timeout": 5},
                          env_path=env_file)
    assert not (tmp_path / "settings.env.tmp").exists()
    assert "NL2SQL_API_KEY=" in env_file.read_text(encoding="utf-8")


def test_load_config_survives_a_directory_as_env_path(clean_env, tmp_path):
    """Robustness contract: 'Never raises for a missing/broken .env'. A
    directory where the file should be raises OSError on read -- which
    load_config must swallow into defaults."""
    cfg = nl_to_sql.load_config(env_path=tmp_path)   # tmp_path is a directory
    assert cfg["model"] == "glm-4-flash"


def test_presets_content():
    p = nl_to_sql.PRESETS
    assert p["GLM (Zhipu)"] == {"base_url": "https://open.bigmodel.cn/api/paas/v4",
                                "model": "glm-4-flash"}
    assert p["OpenAI"]["base_url"] == "https://api.openai.com/v1"
    assert p["OpenAI"]["model"] == "gpt-4o-mini"
    assert p["Ollama (local)"]["base_url"] == "http://localhost:11434/v1"
    assert p["Ollama (local)"]["model"] == "llama3.1"
    assert p["Custom"]["base_url"] == "" and p["Custom"]["model"] == ""


def test_parse_env_text_basics():
    parse = nl_to_sql._parse_env_text
    assert parse("export FOO=bar\nBAZ=qux") == {"FOO": "bar", "BAZ": "qux"}
    assert parse('A="v v"\nB=\'w\'') == {"A": "v v", "B": "w"}
    assert parse("# comment\n\nC=1\nno equals sign here") == {"C": "1"}
    assert parse("") == {}
    assert parse(None) == {}


# ===========================================================================
# E) dashboard: AppTest (read-only, no API key) + _nl_auto_chart
# ===========================================================================

EXPECTED_TAB_LABELS = ["Overview", "Charts", "Anomaly alerts", "Ingest CSVs",
                       "Pricing simulator", "SoCal Benchmarks",
                       "Forecast & cash runway", "SoCal Capital & Deals",
                       "Public Peer Financials", "Ask Your Data"]


def test_dashboard_renders_ten_tabs_clean(apptest_smoke):
    at = apptest_smoke
    assert not at.exception, f"app.py raised: {at.exception}"
    assert len(at.error) == 0, [e.value for e in at.error]  # no error banners
    assert len(at.tabs) == 10
    labels = [t.label for t in at.tabs]
    assert labels == EXPECTED_TAB_LABELS
    assert "Ask Your Data" in labels
    assert "SoCal Benchmarks" in labels
    assert "SoCal Capital & Deals" in labels
    assert "Public Peer Financials" in labels


def test_ask_tab_widgets_present(apptest_smoke, app_module):
    at = apptest_smoke
    # the six starter buttons nl_starter_0..5, labelled with the questions
    starter = {b.key: b.label for b in at.button
               if (b.key or "").startswith("nl_starter_")}
    assert set(starter) == {f"nl_starter_{i}" for i in range(6)}
    assert set(starter.values()) == set(app_module.STARTER_QUESTIONS)
    assert len(app_module.STARTER_QUESTIONS) == 6
    # the question + SQL-preview text areas and the two action buttons
    assert {t.key for t in at.text_area} == {"nl_question", "nl_sql"}
    button_labels = {b.label for b in at.button}
    assert {"Generate SQL", "Run query", "Save settings to .env"} \
        <= button_labels


def test_ask_tab_valid_query_flow(app_module):
    """LLM-less flow: type a valid query into the SQL preview and click
    'Run query' -> read-only results render (new dataframe + the read-only
    caption). No API key involved."""
    at = _make_apptest()
    at.run()
    assert not at.exception
    dfs_before = len(at.dataframe)

    at.text_area(key="nl_sql").set_value(
        "SELECT month_start, revenue, gross_profit FROM "
        "scoped_monthly_summary ORDER BY month_start LIMIT 36")
    run_btn = [b for b in at.button if b.label == "Run query"][0]
    run_btn.click()
    at.run()

    assert not at.exception, f"app.py raised: {at.exception}"
    assert not any("Guardrails rejected" in e.value for e in at.error)
    captions = [c.value for c in at.caption]
    assert any("read-only transaction" in c for c in captions)
    assert len(at.dataframe) == dfs_before + 1   # one new result table


def test_ask_tab_drop_query_rejected_before_execution(app_module):
    """'DROP TABLE jobs' in the SQL preview -> guardrail st.error, no result
    table, no read-only caption: nothing is executed."""
    at = _make_apptest()
    at.run()
    dfs_before = len(at.dataframe)

    at.text_area(key="nl_sql").set_value("DROP TABLE jobs")
    run_btn = [b for b in at.button if b.label == "Run query"][0]
    run_btn.click()
    at.run()

    assert not at.exception
    errors = [e.value for e in at.error]
    assert any("Guardrails rejected this SQL" in e for e in errors)
    assert any("forbidden keyword: drop" in e for e in errors)
    assert not any("read-only transaction" in c.value for c in at.caption)
    assert len(at.dataframe) == dfs_before       # no result table added


# --- app._nl_auto_chart selection logic (pure function) ----------------------
def _df_dates_objects():
    # object-dtype dates: what psycopg2 DATE columns arrive as in
    # pd.DataFrame(res["rows"], columns=res["columns"])
    return pd.DataFrame({
        "month": [_dt.date(2026, 1, 1) + _dt.timedelta(days=30 * i)
                  for i in range(6)],
        "revenue": [10.0, 20.0, 15.0, 30.0, 25.0, 40.0],
    })


def _df_dates_strings():
    return pd.DataFrame({
        "month": ["2026-01-01", "2026-02-01", "2026-03-01"],
        "revenue": [10.0, 20.0, 30.0],
    })


def _df_cat_one_num():
    return pd.DataFrame({"job_type": ["a", "b", "c"],
                         "revenue": [1.0, 2.0, 3.0]})


def _df_cat_two_nums():
    return pd.DataFrame({"job_type": ["a", "b", "c"],
                         "revenue": [1.0, 2.0, 3.0],
                         "cost": [4.0, 5.0, 6.0]})


def _df_cat_three_nums():
    return pd.DataFrame({"job_type": ["a", "b", "c"],
                         "n1": [1.0, 2.0, 3.0], "n2": [4.0, 5.0, 6.0],
                         "n3": [7.0, 8.0, 9.0]})


def _df_single_row():
    return pd.DataFrame({"job_type": ["a"], "revenue": [1.0]})


def _df_empty():
    return pd.DataFrame()


def _df_201_rows():
    return pd.DataFrame({
        "month": [_dt.date(2026, 1, 1) + _dt.timedelta(days=i)
                  for i in range(201)],
        "v": list(range(201)),
    })


def _df_200_rows():
    return pd.DataFrame({
        "month": [_dt.date(2026, 1, 1) + _dt.timedelta(days=i)
                  for i in range(200)],
        "v": list(range(200)),
    })


def _df_datetime64():
    # pandas-native datetime64 column: pd.to_numeric turns it into epoch
    # ints, so the classifier sees two NUMERIC columns and no date column.
    return pd.DataFrame({
        "month": pd.to_datetime(["2026-01-01", "2026-02-01", "2026-03-01"]),
        "revenue": [10.0, 20.0, 30.0],
    })


def _df_text_only():
    return pd.DataFrame({"job_type": ["a", "b", "c"],
                         "status": ["quoted", "done", "done"]})


CHART_CASES = [
    ("date-objects+numeric -> line", _df_dates_objects, ("figure", "scatter", 1)),
    ("date-strings+numeric -> line", _df_dates_strings, ("figure", "scatter", 1)),
    ("category+1numeric -> bar", _df_cat_one_num, ("figure", "bar", 1)),
    ("category+2numerics -> 2 bars", _df_cat_two_nums, ("figure", "bar", 2)),
    ("single row -> None", _df_single_row, None),
    ("empty -> None", _df_empty, None),
    ("201 rows -> None", _df_201_rows, None),
    ("200 rows -> line", _df_200_rows, ("figure", "scatter", 1)),
    ("category+3numerics -> None", _df_cat_three_nums, None),
    ("datetime64 column -> None (pinned deviation)", _df_datetime64, None),
    ("text only -> None", _df_text_only, None),
]


@pytest.mark.parametrize("label, builder, expected", CHART_CASES,
                         ids=[c[0] for c in CHART_CASES])
def test_nl_auto_chart_selection(app_module, label, builder, expected):
    fig = app_module._nl_auto_chart(builder())
    if expected is None:
        assert fig is None, f"{label}: expected None, got a figure"
        return
    kind, trace_type, n_traces = expected
    assert isinstance(fig, go.Figure), f"{label}: expected a Figure"
    assert len(fig.data) == n_traces, f"{label}: {len(fig.data)} trace(s)"
    assert all(t.type == trace_type for t in fig.data), \
        f"{label}: trace types {[t.type for t in fig.data]}"


def test_nl_auto_chart_docstring_rows_contract(app_module):
    """The docstring promises charts only for 2..200 rows -- both boundaries
    behave as documented (200 -> chart, 201 -> None, 1 -> None)."""
    assert app_module._nl_auto_chart(_df_200_rows()) is not None
    assert app_module._nl_auto_chart(_df_201_rows()) is None
    assert app_module._nl_auto_chart(_df_single_row()) is None


# ===========================================================================
# F) LIVE GLM end-to-end (skipped without an API key -- by design)
# ===========================================================================

# Read once for the skip decision only; the key's VALUE is never printed or
# asserted anywhere in this suite.
_LIVE_CFG = nl_to_sql.load_config()
_HAS_LLM_KEY = bool(str(_LIVE_CFG.get("api_key") or "").strip())


@pytest.mark.skipif(not _HAS_LLM_KEY,
                    reason="no NL2SQL API key configured (.env / "
                           "NL2SQL_API_KEY) -- live GLM E2E stays skipped")
def test_live_glm_end_to_end_best_margin(live_conn):
    """One real ask() against the configured LLM: the SQL must be a single
    SELECT that passes the guardrails and executes read-only with rows."""
    res = nl_to_sql.ask("Which service line has the best gross margin?",
                        nl_to_sql.load_config())
    sql = (res.get("sql") or "").strip()
    assert sql.upper().startswith("SELECT")
    assert "\n" not in sql or sql.count(";") == 0
    assert res.get("error") in (None, "") or res["rowcount"] > 0
    assert res["rowcount"] > 0
    assert res["columns"] and res["rows"]


# ===========================================================================
# G) final integrity: the live warehouse is untouched after ALL tests above
# ===========================================================================

def test_final_live_integrity_untouched(live_conn):
    """Read-only proof that this suite (including the sanctioned must-fail
    writes of Part B) left the warehouse exactly as it found it. Constants
    per the documented demo baseline; update them only if the demo data is
    deliberately re-seeded with different volumes."""
    for table, expected in INTEGRITY_COUNTS.items():
        assert plain_count(live_conn, table) == expected, \
            f"{table}: expected {expected} rows"
    assert plain_count(live_conn, "mv_customer_profitability",
                       "is_whale") == WHALE_COUNT
