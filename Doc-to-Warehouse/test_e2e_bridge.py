"""End-to-end QA for the Doc-to-Warehouse -> Data Warehouse bridge.

Exercises the REAL pipeline against the LIVE ``smb_warehouse`` PostgreSQL
database (localhost:5432):

    6 sample PDFs -> pdf_parser.extract_transactions
                  -> categorizer.categorize_all  (flags the deliberate
                     invoice re-send as a duplicate)
                  -> warehouse_bridge.push_transactions(QA company)
                  -> Data Warehouse/ingest_csv.run_ingest  (company_id
                     backfill + exact-mode anti-join dedup)
                  -> refresh_all_analytics() -> detect_anomalies()

and proves, end to end:
  * every ground-truth transaction is extracted and the re-send invoice is
    duplicate-flagged by the categorizer;
  * the first push inserts one ledger row per non-duplicate transaction and
    one labor_logs row per payroll line_item (4 employees x 4 work dates);
  * rows land in the TEST company only -- company_id 1 (the demo workspace)
    is byte-for-byte untouched by row count, and every client table grows by
    exactly the pushed amounts;
  * ZERO RLS leakage for the restricted ``nl2sql_reader`` role: per-GUC
    scoping, fail-closed when the GUC is unset/RESET, no cross-tenant
    visibility, scoped_* wrapper views agree with the superuser matviews,
    and the raw matviews stay denied to the reader role;
  * the materialized views reflect the pushed amounts to the penny
    (Decimal comparisons) after refresh_all_analytics();
  * the anomaly cadence ran and created no alerts for other workspaces;
  * a second identical push inserts 0 rows (idempotent exact-dedup) and
    creates no new reference rows;
  * include_duplicates=True inserts exactly the not-byte-identical re-send
    invoice (its raw vendor text differs from the original, so the exact
    anti-join must let exactly that one row through);
  * input validation: ValueError on empty txns / unknown company_id.

Database hygiene: the module creates ONE company named
``QA_BRIDGE_E2E_<uuid8>`` and only ever INSERTs / DELETEs rows carrying its
own company_id. company_id 1 (demo) is never written. Cleanup is FK-ordered
and runs in try/finally even when assertions fail; QA companies orphaned by
crashed earlier runs are removed at setup (by exact company_id). The module
never DROPs / ALTERs any database object.

Ownership: this test file only. warehouse_bridge.py, app.py,
export_warehouse_csv.py, conftest.py, test_extractor.py and everything
under "Data Warehouse/" are read-only for these tests.
"""

from __future__ import annotations

import os
import sys
import uuid
from decimal import Decimal

import pytest

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

import psycopg2  # noqa: E402  (suite requirement: psycopg2 2.9.x)
from psycopg2.extensions import parse_dsn  # noqa: E402

# ---------------------------------------------------------------------------
# Module-level guards -- skip (never hard-fail) when the live warehouse
# cannot be exercised, so the suite stays usable offline.
# ---------------------------------------------------------------------------
try:
    import warehouse_bridge as bridge  # noqa: E402
    _BRIDGE_IMPORT_ERROR = None
except Exception as _exc:  # pragma: no cover - suite layout is fixed
    bridge = None
    _BRIDGE_IMPORT_ERROR = _exc

try:
    import export_warehouse_csv as exporter  # noqa: E402
    _EXPORTER_IMPORT_ERROR = None
except Exception as _exc:  # pragma: no cover - suite layout is fixed
    exporter = None
    _EXPORTER_IMPORT_ERROR = _exc

_DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/smb_warehouse"


def _postgres_reachable(timeout_seconds: int = 3):
    """Quick superuser connect probe. Returns (ok, failure_reason)."""
    dsn = bridge.DB_DSN if bridge is not None else _DEFAULT_DSN
    try:
        conn = psycopg2.connect(dsn, connect_timeout=timeout_seconds)
    except Exception as exc:  # noqa: BLE001 - any failure means unreachable
        return False, str(exc).strip() or exc.__class__.__name__
    conn.close()
    return True, None


_SKIP_REASONS = []
_probe_ok, _probe_why = _postgres_reachable()
if not _probe_ok:
    _SKIP_REASONS.append("Postgres not running on localhost:5432"
                         + (f" ({_probe_why})" if _probe_why else ""))
if bridge is None:
    _SKIP_REASONS.append(f"warehouse_bridge import failed: "
                         f"{_BRIDGE_IMPORT_ERROR}")
if exporter is None:
    _SKIP_REASONS.append(f"export_warehouse_csv import failed: "
                         f"{_EXPORTER_IMPORT_ERROR}")

pytestmark = pytest.mark.skipif(
    bool(_SKIP_REASONS), reason="; ".join(_SKIP_REASONS))

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
#: The 8 client-data tables (all carry company_id; Data Warehouse/schema.sql).
CLIENT_TABLES = (
    "financial_ledger", "labor_logs", "anomaly_alerts", "forecasts",
    "jobs", "employees", "job_types", "customers",
)

#: FK-safe DELETE order: children first (labor_logs -> jobs/employees;
#: financial_ledger / anomaly_alerts -> jobs SET NULL, but emptied first
#: anyway), then jobs (RESTRICTed by customers/job_types children? no --
#: jobs REFERENCES customers/job_types, so jobs goes BEFORE them), then
#: employees / job_types / customers, then the parent companies row.
DELETE_ORDER = CLIENT_TABLES

#: Unique prefix for the module's own test workspace.
QA_PREFIX = "QA_BRIDGE_E2E_"

READER_ROLE = "nl2sql_reader"
READER_PASSWORD = "nl2sql_readonly_local"  # documented in 002_add_rls.sql

#: Raw SMB matviews (Data Warehouse/analytics_views.sql) and their scoped_*
#: reader wrapper views (migrations/002_add_rls.sql).
RAW_MATVIEWS = ("mv_monthly_summary", "mv_job_type_profitability",
                "mv_customer_profitability", "mv_labor_cost_analysis",
                "mv_benchmark_comparison")
SCOPED_VIEWS = ("scoped_monthly_summary", "scoped_job_type_profitability",
                "scoped_customer_profitability", "scoped_labor_cost_analysis",
                "scoped_benchmark_comparison")


# ---------------------------------------------------------------------------
# small DB helpers
# ---------------------------------------------------------------------------
def _connect_superuser():
    """Trusted-path superuser connection (same DSN the bridge itself uses)."""
    return psycopg2.connect(bridge.DB_DSN, connect_timeout=10)


def _connect_reader():
    """Restricted nl2sql_reader connection to the same database."""
    base = parse_dsn(bridge.DB_DSN)
    return psycopg2.connect(
        host=base.get("host") or "localhost",
        port=base.get("port") or "5432",
        dbname=base.get("dbname") or "smb_warehouse",
        user=READER_ROLE,
        password=READER_PASSWORD,
        connect_timeout=10,
    )


def _count(cur, table, where="TRUE", args=()):
    cur.execute("SELECT count(*) FROM {} WHERE {}".format(table, where), args)
    return int(cur.fetchone()[0])


def _table_exists(cur, qualified_name):
    cur.execute("SELECT to_regclass(%s)", (qualified_name,))
    return cur.fetchone()[0] is not None


def _snapshot(conn):
    """Row-count snapshot used to prove zero leakage / exact growth."""
    totals, company1 = {}, {}
    with conn.cursor() as cur:
        for table in CLIENT_TABLES:
            totals[table] = _count(cur, table)
            company1[table] = _count(cur, table, "company_id = 1")
        cur.execute("SELECT company_id, count(*) FROM anomaly_alerts "
                    "GROUP BY company_id")
        anomaly_by_company = {int(r[0]): int(r[1]) for r in cur.fetchall()}
    return {"totals": totals, "company1": company1,
            "anomaly_by_company": anomaly_by_company}


def _delete_company_rows(conn, company_id):
    """DELETE every row belonging to ``company_id`` in FK-safe order."""
    with conn.cursor() as cur:
        for table in DELETE_ORDER:
            cur.execute("DELETE FROM {} WHERE company_id = %s".format(table),
                        (company_id,))
        cur.execute("DELETE FROM companies WHERE company_id = %s",
                    (company_id,))


def _orphan_qa_company_ids(conn):
    """company_ids of QA_BRIDGE_E2E_* companies (crashed earlier runs)."""
    with conn.cursor() as cur:
        cur.execute("SELECT company_id FROM companies "
                    "WHERE company_name LIKE %s", (QA_PREFIX + "%",))
        return [int(r[0]) for r in cur.fetchall()]


def _delete_orphan_qa_companies(conn):
    """Clean leftovers of crashed earlier runs (only exact QA company_ids)."""
    removed = []
    for orphan_id in _orphan_qa_company_ids(conn):
        _delete_company_rows(conn, orphan_id)
        removed.append(orphan_id)
    if removed:
        conn.commit()
    return removed


# ---------------------------------------------------------------------------
# derived expectations (same code path as the bridge -- never hardcoded)
# ---------------------------------------------------------------------------
def _mapped_ledger_tuple(txn):
    """The exact financial_ledger tuple the bridge builds for one txn.

    Mirrors warehouse_bridge._build_ledger_rows: entry_type/category from
    exporter._map_category, amount from exporter._fmt_amount (positive 2dp),
    raw description/vendor. This is the tuple the ingest engine's exact-mode
    anti-join compares against the table."""
    entry_type, wh_category = exporter._map_category(
        txn.get("category"), txn.get("overhead_subtype"))
    return (entry_type, wh_category, str(txn.get("date") or ""),
            Decimal(exporter._fmt_amount(txn.get("amount"))),
            str(txn.get("description") or ""),
            str(txn.get("vendor") or ""))


def _ledger_tuples_in_db(cur, company_id):
    cur.execute(
        "SELECT entry_type, category, entry_date::text, amount, "
        "COALESCE(description, ''), COALESCE(vendor, '') "
        "FROM financial_ledger WHERE company_id = %s", (company_id,))
    return {(r[0], r[1], r[2], Decimal(r[3]), r[4], r[5])
            for r in cur.fetchall()}


def _labor_line_rows(pushable):
    """Payroll line_items of the pushable set, mirroring the bridge's
    warehouse CHECK filters (0 < hours <= 24, rate >= 0)."""
    rows = []
    for txn in pushable:
        if str(txn.get("doc_type") or "") != "payroll":
            continue
        for item in txn.get("line_items") or []:
            try:
                hours = Decimal(str(item.get("hours")))
                rate = Decimal(str(item.get("rate")))
            except Exception:  # noqa: BLE001 - malformed lines are skip-able
                continue
            if not (Decimal("0") < hours <= Decimal("24")) or rate < 0:
                continue
            rows.append((hours, rate, item))
    return rows


def _derive_expectations(pushable, company_id):
    """Everything later assertions compare against, derived from the
    categorized transactions + the shared exporter mapping."""
    mapped = [_mapped_ledger_tuple(t) for t in pushable]

    months = {}
    for t, tup in zip(pushable, mapped):
        month = tup[2][:7]
        if not month:
            continue
        bucket = months.setdefault(
            month, {"revenue": Decimal("0"), "expense": Decimal("0")})
        bucket["revenue" if tup[0] == "revenue" else "expense"] += tup[3]

    labor_rows = _labor_line_rows(pushable)
    hours = sum((r[0] for r in labor_rows), Decimal("0"))
    cost = sum((r[0] * r[1] for r in labor_rows), Decimal("0"))
    employees = {str(r[2].get("employee") or "") for r in labor_rows}
    work_dates = {str(r[2].get("work_date") or "") for r in labor_rows}
    payroll_files = sorted({str(t.get("source_file") or "") for t in pushable
                            if str(t.get("doc_type") or "") == "payroll"})
    job_numbers = [bridge.job_number_for(f, company_id)
                   for f in payroll_files]
    employee_emails = {bridge.employee_email_for(name, company_id)
                       for name in employees}

    return {
        "mapped": mapped,
        "ledger_rows": len(set(mapped)),
        "ledger_rows_unfiltered": len(mapped),
        "labor_rows": len(labor_rows),
        "labor_hours": hours,
        "labor_cost": cost,
        "employees": employees,
        "employee_emails": employee_emails,
        "work_dates": work_dates,
        "job_numbers": job_numbers,
        "months": months,
    }


# ---------------------------------------------------------------------------
# module fixture: one QA company, snapshots, FK-ordered cleanup in finally
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def e2e_env():
    state = {"company_id": None, "company_name": None, "pre": None,
             "push1": None, "push2": None, "push3": None}
    conn = _connect_superuser()
    try:
        # crashed earlier runs must never poison this run
        removed = _delete_orphan_qa_companies(conn)
        state["orphans_removed_at_setup"] = removed

        company_name = QA_PREFIX + uuid.uuid4().hex[:8]
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO companies (company_name, industry_vertical) "
                "VALUES (%s, 'qa_e2e') RETURNING company_id",
                (company_name,))
            state["company_id"] = int(cur.fetchone()[0])
        conn.commit()
        state["company_name"] = company_name

        # pre-push snapshot: company 1 per-table counts + total counts, so
        # leakage / unexpected growth can be proven later.
        state["pre"] = _snapshot(conn)

        yield state
    finally:
        try:
            _run_teardown(conn, state)
        finally:
            conn.close()


def _run_teardown(conn, state):
    """FK-ordered cleanup + verification. The DELETEs always run; the
    assertions afterwards prove zero leftovers and an untouched demo
    workspace. Runs even when a test assertion failed."""
    company_id = state.get("company_id")
    pre = state.get("pre") or {}
    try:
        if company_id is not None:
            _delete_company_rows(conn, company_id)
        _delete_orphan_qa_companies(conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    # best-effort analytics refresh so the matviews also drop the QA
    # workspace (pure REFRESH, mutates no base-table data).
    refresh_ok = False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT refresh_all_analytics()")
        conn.commit()
        refresh_ok = True
    except Exception:
        conn.rollback()

    if company_id is not None:
        with conn.cursor() as cur:
            for table in CLIENT_TABLES:
                assert _count(cur, table, "company_id = %s",
                              (company_id,)) == 0, (
                    "leftover rows in {} for QA company {}".format(
                        table, company_id))
            if refresh_ok:
                for matview in RAW_MATVIEWS:
                    if _table_exists(cur, "public." + matview):
                        assert _count(cur, matview, "company_id = %s",
                                      (company_id,)) == 0, (
                            "leftover {} rows for QA company {}".format(
                                matview, company_id))
        assert _orphan_qa_company_ids(conn) == [], (
            "leftover {} companies after cleanup".format(QA_PREFIX))
        with conn.cursor() as cur:
            for table in CLIENT_TABLES:
                now = _count(cur, table, "company_id = 1")
                assert now == pre.get("company1", {}).get(table), (
                    "company_id 1 {} count changed by cleanup: snapshot={}, "
                    "now={}".format(table, pre.get("company1", {}).get(table),
                                    now))


# ---------------------------------------------------------------------------
# 1) the real pipeline: extract -> categorize (duplicate flagged)
# ---------------------------------------------------------------------------
def test_pipeline_extracts_ground_truth_and_flags_resend(
        categorized_transactions, ground_truth):
    cat = categorized_transactions
    gt_docs = ground_truth["documents"]

    gt_total = sum(len(d.get("transactions", [])) for d in gt_docs)
    assert gt_total > 0, "ground truth has no transactions"
    assert len(cat) == gt_total, (
        "pipeline extracted {} transactions, ground truth has {}".format(
            len(cat), gt_total))

    per_file = {}
    for txn in cat:
        per_file[txn.get("source_file")] = \
            per_file.get(txn.get("source_file"), 0) + 1
    for doc in gt_docs:
        assert per_file.get(doc["file"], 0) == len(doc.get("transactions", [])), (
            "document {}: extracted {}, ground truth {}".format(
                doc["file"], per_file.get(doc["file"], 0),
                len(doc.get("transactions", []))))

    # every extracted txn is pushable-shaped: dated, positive amount
    for txn in cat:
        assert txn.get("date"), "txn {}: empty date".format(txn.get("txn_id"))
        amount = txn.get("amount")
        assert isinstance(amount, (int, float)) and amount > 0, (
            "txn {}: non-positive amount {!r}".format(
                txn.get("txn_id"), amount))

    # the deliberate re-send must be the one duplicate flagged
    dupes = [t for t in cat if t.get("is_duplicate")]
    expected_dups = ground_truth.get("expected_duplicates", [])
    assert len(dupes) == len(expected_dups), (
        "categorizer flagged {} duplicates, ground truth expects {}".format(
            len(dupes), len(expected_dups)))
    if dupes and expected_dups:
        resend = dupes[0]
        assert str(resend.get("source_file", "")).endswith(
            "invoice_abc_lumber_resend.pdf"), (
            "flagged duplicate is not the re-send document: {!r}".format(
                resend.get("source_file")))
        assert str(resend.get("duplicate_of") or "").startswith(
            "invoice_abc_lumber.pdf"), (
            "duplicate_of does not point at the original ABC Lumber txn: "
            "{!r}".format(resend.get("duplicate_of")))
        assert resend.get("invoice_number") == \
            expected_dups[0]["invoice_number"], (
            "re-send invoice_number {!r} != expected {!r}".format(
                resend.get("invoice_number"),
                expected_dups[0]["invoice_number"]))


# ---------------------------------------------------------------------------
# 2) the first push: counts, cadence, created references
# ---------------------------------------------------------------------------
def test_first_push_row_counts_and_cadence(e2e_env,
                                           categorized_transactions):
    state = e2e_env
    company_id = state["company_id"]
    cat = categorized_transactions

    pushable = [t for t in cat if not t.get("is_duplicate")]
    flagged = len(cat) - len(pushable)
    expect = _derive_expectations(pushable, company_id)
    # ground truth sanity: distinct mapped ledger rows == pushable txns (no
    # in-file exact duplicates), and labor = 4 employees x 4 work dates.
    assert expect["ledger_rows"] == expect["ledger_rows_unfiltered"] == \
        len(pushable), (
        "mapped ledger rows are not pairwise distinct ({} distinct vs {} "
        "pushable)".format(expect["ledger_rows"], len(pushable)))
    assert expect["labor_rows"] == \
        len(expect["employees"]) * len(expect["work_dates"]) == 16, (
        "payroll line items {} != 4 employees x 4 work dates".format(
            expect["labor_rows"]))

    result = bridge.push_transactions(cat, company_id)
    state["push1"] = result
    state["expect"] = expect

    assert result["company_id"] == company_id
    assert result["company_name"] == state["company_name"]
    assert result["pushed_txns"] == len(pushable), (
        "pushed_txns {} != non-duplicate txns {}".format(
            result["pushed_txns"], len(pushable)))
    assert result["skipped_flagged_duplicates"] == flagged
    assert result["refreshed"] is True, "post-load cadence did not run"
    assert result["errors"] == [], "push reported row errors: {!r}".format(
        result["errors"])
    assert result["ledger_rows"] == expect["ledger_rows"], (
        "ledger_rows {} != derived {} (one ledger row per non-duplicate "
        "txn)".format(result["ledger_rows"], expect["ledger_rows"]))
    assert result["labor_rows"] == expect["labor_rows"], (
        "labor_rows {} != payroll line_items {}".format(
            result["labor_rows"], expect["labor_rows"]))
    created = result["created_refs"]
    assert created["employees"] == len(expect["employees"]), (
        "created employees {} != ground truth employees {}".format(
            created["employees"], len(expect["employees"])))
    assert created["jobs"] >= 1, (
        "created jobs {} < 1 (one warehouse job per payroll document)".format(
            created["jobs"]))


# ---------------------------------------------------------------------------
# 3) correct company placement (superuser view) + exact total growth
# ---------------------------------------------------------------------------
def test_push_places_rows_in_test_company_only(e2e_env):
    state = e2e_env
    result = state.get("push1")
    assert result is not None, "first push did not complete (earlier failure)"
    company_id = state["company_id"]
    pre = state["pre"]
    expect = state["expect"]

    conn = _connect_superuser()
    try:
        with conn.cursor() as cur:
            # every ledger/labor row for the test company is attributed to it
            assert _count(cur, "financial_ledger",
                          "company_id = %s", (company_id,)) == \
                result["ledger_rows"]
            assert _count(cur, "labor_logs",
                          "company_id = %s", (company_id,)) == \
                result["labor_rows"]
            cur.execute("SELECT count(*) FROM financial_ledger "
                        "WHERE company_id = %s AND company_id <> %s",
                        (company_id, company_id))
            assert cur.fetchone()[0] == 0
            # bridge rows carry no job/customer attribution (mirrors the CSV
            # export path: attribution stays NULL = general/overhead)
            assert _count(cur, "financial_ledger",
                          "company_id = %s AND (job_id IS NOT NULL OR "
                          "customer_id IS NOT NULL)",
                          (company_id,)) == 0

            # deterministic, company-scoped references
            cur.execute("SELECT email FROM employees WHERE company_id = %s",
                        (company_id,))
            emails = {r[0] for r in cur.fetchall()}
            assert emails == expect["employee_emails"], (
                "employee emails {} != deterministic bridge emails {}".format(
                    sorted(emails), sorted(expect["employee_emails"])))
            cur.execute("SELECT job_number FROM jobs WHERE company_id = %s",
                        (company_id,))
            job_numbers = {r[0] for r in cur.fetchall()}
            assert set(expect["job_numbers"]) <= job_numbers, (
                "deterministic job numbers missing: {} vs {}".format(
                    sorted(expect["job_numbers"]), sorted(job_numbers)))
            assert all(j.startswith("DOC-C{}-".format(company_id))
                       for j in job_numbers), (
                "job numbers are not company-scoped: {}".format(
                    sorted(job_numbers)))

            # company_id 1 (demo) untouched vs the pre-push snapshot
            for table in CLIENT_TABLES:
                now = _count(cur, table, "company_id = 1")
                assert now == pre["company1"][table], (
                    "demo workspace {}: {} rows now, {} before the "
                    "push".format(table, now, pre["company1"][table]))

            # every client table grew by exactly the pushed amount
            expected_delta = {
                "financial_ledger": result["ledger_rows"],
                "labor_logs": result["labor_rows"],
                "jobs": result["created_refs"]["jobs"],
                "employees": result["created_refs"]["employees"],
                "job_types": result["created_refs"]["job_types"],
                "customers": result["created_refs"]["customers"],
                "anomaly_alerts": result["anomalies_new"],
                "forecasts": 0,
            }
            for table in CLIENT_TABLES:
                delta = _count(cur, table) - pre["totals"][table]
                assert delta == expected_delta[table], (
                    "{} grew by {} rows, expected {} (push result + "
                    "created_refs)".format(table, delta,
                                           expected_delta[table]))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 4) ZERO RLS leakage for the restricted reader role
# ---------------------------------------------------------------------------
def _reader_set_guc(cur, value):
    cur.execute("SET app.current_company_id = '{}'".format(value))


def _reader_reset_guc(cur):
    cur.execute("RESET app.current_company_id")


def test_rls_reader_zero_leakage(e2e_env):
    state = e2e_env
    result = state.get("push1")
    assert result is not None, "first push did not complete (earlier failure)"
    company_id = state["company_id"]
    company_name = state["company_name"]
    pre = state["pre"]

    # superuser reference counts for the scoped-view consistency check
    super_conn = _connect_superuser()
    try:
        with super_conn.cursor() as cur:
            mv_counts = {}
            for matview in RAW_MATVIEWS:
                if _table_exists(cur, "public." + matview):
                    mv_counts[matview] = _count(cur, matview,
                                                "company_id = %s",
                                                (company_id,))
    finally:
        super_conn.close()

    conn = _connect_reader()
    try:
        with conn.cursor() as cur:
            # (a) test GUC -> exactly the pushed rows, all test company
            _reader_set_guc(cur, company_id)
            for table, expected in (("financial_ledger",
                                     result["ledger_rows"]),
                                    ("labor_logs", result["labor_rows"])):
                cur.execute("SELECT count(*) FROM {}".format(table))
                assert int(cur.fetchone()[0]) == expected, (
                    "reader (GUC={}) sees wrong {} count".format(
                        company_id, table))
                cur.execute("SELECT count(*) FROM {} WHERE company_id <> %s"
                            .format(table), (company_id,))
                assert int(cur.fetchone()[0]) == 0, (
                    "reader (GUC={}) sees rows of OTHER companies in {}".format(
                        company_id, table))
            # reference rows scoped the same way
            assert _count(cur, "employees") == result["created_refs"][
                "employees"]
            assert _count(cur, "jobs") == result["created_refs"]["jobs"]

            # (b) demo GUC -> zero test-company rows visible
            _reader_set_guc(cur, "1")
            for table in ("financial_ledger", "labor_logs"):
                assert _count(cur, table, "company_id = %s",
                              (company_id,)) == 0, (
                    "LEAK: reader scoped to company 1 sees test company "
                    "rows in {}".format(table))
            assert _count(cur, "financial_ledger") == \
                pre["company1"]["financial_ledger"], (
                "reader (GUC=1) should see exactly the demo workspace's "
                "ledger rows")

            # (c) RESET (unset GUC) -> fail-closed: zero rows at all
            _reader_reset_guc(cur)
            for table in ("financial_ledger", "labor_logs"):
                assert _count(cur, table) == 0, (
                    "FAIL-CLOSED BROKEN: reader with unset GUC sees rows "
                    "in {}".format(table))

            # (d) the companies registry obeys the same scoping
            _reader_set_guc(cur, company_id)
            cur.execute("SELECT company_id, company_name FROM companies")
            rows = cur.fetchall()
            assert len(rows) == 1 and int(rows[0][0]) == company_id \
                and rows[0][1] == company_name, (
                "reader (GUC={}) must see exactly the test company, got "
                "{!r}".format(company_id, rows))
            _reader_set_guc(cur, "1")
            cur.execute("SELECT company_id, company_name FROM companies")
            rows = cur.fetchall()
            assert len(rows) == 1 and int(rows[0][0]) == 1 \
                and rows[0][1] != company_name, (
                "reader (GUC=1) must see exactly the demo company, got "
                "{!r}".format(rows))
            _reader_reset_guc(cur)
            cur.execute("SELECT count(*) FROM companies")
            assert int(cur.fetchone()[0]) == 0, (
                "FAIL-CLOSED BROKEN: reader with unset GUC sees companies")

            # scoped_* wrapper views: consistent with the superuser matview,
            # every visible row test-company, fail-closed when unset
            for view in SCOPED_VIEWS:
                if not _table_exists(cur, "public." + view):
                    continue
                matview = view.replace("scoped_", "mv_", 1)
                _reader_set_guc(cur, company_id)
                assert _count(cur, view) == mv_counts[matview], (
                    "{} (GUC={}) count {} != superuser {} count for the "
                    "test company {}".format(view, company_id,
                                             _count(cur, view), matview,
                                             mv_counts[matview]))
                cur.execute("SELECT count(*) FROM {} WHERE company_id <> %s"
                            .format(view), (company_id,))
                assert int(cur.fetchone()[0]) == 0, (
                    "LEAK: {} shows non-test companies under the test "
                    "GUC".format(view))
                _reader_reset_guc(cur)
                assert _count(cur, view) == 0, (
                    "FAIL-CLOSED BROKEN: {} with unset GUC".format(view))

            # the raw matviews stay denied to the reader role (defensive
            # REVOKE invariant of migrations/002_add_rls.sql)
            for matview in RAW_MATVIEWS:
                if _table_exists(cur, "public." + matview):
                    cur.execute("SELECT has_table_privilege(%s, %s, 'SELECT')",
                                (READER_ROLE, matview))
                    assert cur.fetchone()[0] is False, (
                        "reader role must have NO privilege on raw "
                        "matview {}".format(matview))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 5) materialized views refreshed: to-the-penny Decimal totals
# ---------------------------------------------------------------------------
def test_materialized_views_reflect_pushed_amounts(e2e_env):
    state = e2e_env
    result = state.get("push1")
    assert result is not None, "first push did not complete (earlier failure)"
    company_id = state["company_id"]
    expect = state["expect"]

    conn = _connect_superuser()
    try:
        with conn.cursor() as cur:
            # explicit refresh (the push already refreshed; this one proves
            # refresh_all_analytics() itself picks the test workspace up)
            cur.execute("SELECT refresh_all_analytics()")

            cur.execute("SELECT month_start, revenue, total_expense, "
                        "labor_hours, labor_cost, gross_profit "
                        "FROM mv_monthly_summary WHERE company_id = %s "
                        "ORDER BY month_start", (company_id,))
            rows = cur.fetchall()
            assert len(rows) == len(expect["months"]) >= 1, (
                "mv_monthly_summary has {} row(s) for the test company, "
                "expected {} month(s)".format(len(rows),
                                              len(expect["months"])))
            for month_start, revenue, total_expense, labor_hours, \
                    labor_cost, gross_profit in rows:
                month_key = str(month_start)[:7]
                expected = expect["months"].get(month_key)
                assert expected is not None, (
                    "unexpected month {} for the test company".format(
                        month_key))
                assert Decimal(revenue) == expected["revenue"], (
                    "month {} revenue {} != pushed {}".format(
                        month_key, revenue, expected["revenue"]))
                assert Decimal(total_expense) == expected["expense"], (
                    "month {} total_expense {} != pushed {}".format(
                        month_key, total_expense, expected["expense"]))
                assert Decimal(gross_profit) == \
                    expected["revenue"] - expected["expense"], (
                    "month {} gross_profit {} != revenue - expense".format(
                        month_key, gross_profit))
                assert Decimal(labor_hours) == expect["labor_hours"], (
                    "month {} labor_hours {} != pushed payroll hours "
                    "{}".format(month_key, labor_hours, expect["labor_hours"]))
                assert Decimal(labor_cost) == expect["labor_cost"], (
                    "month {} labor_cost {} != pushed payroll cost {}".format(
                        month_key, labor_cost, expect["labor_cost"]))

            # mv_labor_cost_analysis is scoped per (company_id, job): the
            # bridge created exactly one job (the payroll document).
            cur.execute("SELECT job_number, total_hours, labor_cost, "
                        "employee_count FROM mv_labor_cost_analysis "
                        "WHERE company_id = %s", (company_id,))
            labor_rows = cur.fetchall()
            assert len(labor_rows) == len(expect["job_numbers"]), (
                "mv_labor_cost_analysis has {} row(s) for the test company, "
                "expected {}".format(len(labor_rows),
                                     len(expect["job_numbers"])))
            for job_number, total_hours, labor_cost, employee_count \
                    in labor_rows:
                assert job_number in expect["job_numbers"], (
                    "unexpected job_number {!r} in "
                    "mv_labor_cost_analysis".format(job_number))
                assert Decimal(total_hours) == expect["labor_hours"], (
                    "job {}: total_hours {} != pushed {}".format(
                        job_number, total_hours, expect["labor_hours"]))
                assert Decimal(labor_cost) == expect["labor_cost"], (
                    "job {}: labor_cost {} != pushed {}".format(
                        job_number, labor_cost, expect["labor_cost"]))
                assert int(employee_count) == len(expect["employees"]), (
                    "job {}: employee_count {} != {}".format(
                        job_number, employee_count, len(expect["employees"])))

            # mv_customer_profitability keeps zero-revenue customers too
            # (LEFT JOIN): the bridge's import customer must be there.
            cur.execute("SELECT company_id FROM mv_customer_profitability "
                        "WHERE company_id = %s", (company_id,))
            cust_rows = cur.fetchall()
            assert len(cust_rows) >= 1, (
                "mv_customer_profitability has no row for the test company")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 6) anomaly cadence: ran, reported, and never touched other workspaces
# ---------------------------------------------------------------------------
def test_anomaly_cadence_and_no_cross_company_alerts(e2e_env):
    state = e2e_env
    result = state.get("push1")
    assert result is not None, "first push did not complete (earlier failure)"
    company_id = state["company_id"]
    pre = state["pre"]

    assert result["refreshed"] is True
    assert isinstance(result["anomalies_new"], int), (
        "anomalies_new is not an int: {!r}".format(result["anomalies_new"]))
    assert result["anomalies_new"] >= 0

    conn = _connect_superuser()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT company_id, count(*) FROM anomaly_alerts "
                        "GROUP BY company_id")
            post = {int(r[0]): int(r[1]) for r in cur.fetchall()}
    finally:
        conn.close()

    for other_id, other_count in post.items():
        if other_id == company_id:
            continue
        assert other_count == pre["anomaly_by_company"].get(other_id, 0), (
            "anomaly_alerts grew for company {} ({} -> {}): the cadence "
            "touched a foreign workspace".format(
                other_id, pre["anomaly_by_company"].get(other_id, 0),
                other_count))
    assert post.get(company_id, 0) - \
        pre["anomaly_by_company"].get(company_id, 0) == \
        result["anomalies_new"], (
        "test company's anomaly_alerts growth {} != reported "
        "anomalies_new {}".format(
            post.get(company_id, 0)
            - pre["anomaly_by_company"].get(company_id, 0),
            result["anomalies_new"]))


# ---------------------------------------------------------------------------
# 7) idempotency: identical re-push inserts 0 rows, creates 0 references
# ---------------------------------------------------------------------------
def test_idempotent_repush_inserts_nothing(e2e_env,
                                           categorized_transactions):
    state = e2e_env
    first = state.get("push1")
    assert first is not None, "first push did not complete (earlier failure)"
    company_id = state["company_id"]

    conn = _connect_superuser()
    try:
        with conn.cursor() as cur:
            before = {table: _count(cur, table) for table in CLIENT_TABLES}
    finally:
        conn.close()

    second = bridge.push_transactions(categorized_transactions, company_id)
    state["push2"] = second

    assert second["pushed_txns"] == first["pushed_txns"]
    assert second["skipped_flagged_duplicates"] == \
        first["skipped_flagged_duplicates"], (
        "the same duplicate re-send must be flagged on every push")
    assert second["ledger_rows"] == 0, (
        "idempotent re-push inserted {} ledger rows, expected 0 (exact "
        "anti-join dedup)".format(second["ledger_rows"]))
    assert second["labor_rows"] == 0, (
        "idempotent re-push inserted {} labor rows, expected 0".format(
            second["labor_rows"]))
    assert second["already_in_warehouse"] == \
        first["ledger_rows"] + first["labor_rows"], (
        "already_in_warehouse {} != first push inserted count {}".format(
            second["already_in_warehouse"],
            first["ledger_rows"] + first["labor_rows"]))
    assert second["errors"] == []
    assert second["refreshed"] is True
    created = second["created_refs"]
    assert created["employees"] == 0 and created["jobs"] == 0 and \
        created["job_types"] == 0 and created["customers"] == 0, (
        "idempotent re-push created reference rows: {!r}".format(created))

    conn = _connect_superuser()
    try:
        with conn.cursor() as cur:
            for table in CLIENT_TABLES:
                assert _count(cur, table) == before[table], (
                    "{} row count changed on the idempotent re-push "
                    "({} -> {})".format(table, before[table],
                                        _count(cur, table)))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 8) include_duplicates=True: exactly the not-byte-identical re-send row
# ---------------------------------------------------------------------------
def test_include_duplicates_pushes_resend_exactly_once(e2e_env,
                                                       categorized_transactions):
    state = e2e_env
    company_id = state["company_id"]
    cat = categorized_transactions
    dup_txns = [t for t in cat if t.get("is_duplicate")]
    assert dup_txns, "no duplicate txns in the categorized set"

    conn = _connect_superuser()
    try:
        with conn.cursor() as cur:
            before = _count(cur, "financial_ledger")
            existing = _ledger_tuples_in_db(cur, company_id)
    finally:
        conn.close()

    third = bridge.push_transactions(cat, company_id, include_duplicates=True)
    state["push3"] = third

    assert third["pushed_txns"] == len(cat), (
        "include_duplicates push must push all {} txns, got {}".format(
            len(cat), third["pushed_txns"]))
    assert third["skipped_flagged_duplicates"] == 0, (
        "include_duplicates=True must not skip flagged duplicates")

    # Derive the expectation from the exact anti-join semantics: a flagged
    # duplicate is inserted only when its mapped ledger tuple is NOT already
    # in the test company's book. The re-send's raw vendor text differs from
    # the original invoice's, so it is not byte-identical -> exactly 1 row.
    expected_new = [t for t in dup_txns
                    if _mapped_ledger_tuple(t) not in existing]
    assert third["ledger_rows"] == len(expected_new), (
        "include_duplicates push inserted {} ledger rows, anti-join "
        "semantics expect {}".format(third["ledger_rows"],
                                     len(expected_new)))
    assert len(expected_new) == 1, (
        "expected exactly 1 insertable re-send row (raw vendor text differs "
        "from the original), derived {}".format(len(expected_new)))
    assert third["labor_rows"] == 0, (
        "re-pushing the same payroll must insert 0 labor rows, got {}"
        .format(third["labor_rows"]))
    assert third["errors"] == []

    conn = _connect_superuser()
    try:
        with conn.cursor() as cur:
            after = _count(cur, "financial_ledger")
    finally:
        conn.close()
    assert after - before == third["ledger_rows"] == 1, (
        "financial_ledger grew by {}, expected exactly {} (the re-send "
        "invoice row)".format(after - before, third["ledger_rows"]))


# ---------------------------------------------------------------------------
# 9) input validation: ValueError, no side effects
# ---------------------------------------------------------------------------
def test_input_validation(e2e_env, categorized_transactions):
    state = e2e_env
    company_id = state["company_id"]

    conn = _connect_superuser()
    try:
        with conn.cursor() as cur:
            before = {table: _count(cur, table) for table in CLIENT_TABLES}
            before_companies = _count(cur, "companies")
    finally:
        conn.close()

    with pytest.raises(ValueError):
        bridge.push_transactions([], company_id)
    with pytest.raises(ValueError):
        bridge.push_transactions(categorized_transactions, 999999)

    conn = _connect_superuser()
    try:
        with conn.cursor() as cur:
            for table in CLIENT_TABLES:
                assert _count(cur, table) == before[table], (
                    "invalid push mutated {} ({} -> {})".format(
                        table, before[table], _count(cur, table)))
            assert _count(cur, "companies") == before_companies, (
                "invalid push mutated the companies registry")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# bonus: the prior smoke company (ZZZ_BRIDGE_SMOKE_20261122, id 8) is gone
# ---------------------------------------------------------------------------
def test_no_zzz_smoke_leftovers(e2e_env):
    """company_id 8 belonged to the earlier ZZZ_BRIDGE_SMOKE_20261122 smoke
    run which reported complete cleanup. Assert-only; if rows DO exist,
    delete only company_id=8 rows (FK order) and fail with the evidence."""
    removed = False
    conn = _connect_superuser()
    try:
        with conn.cursor() as cur:
            leftovers = {table: _count(cur, table, "company_id = 8")
                         for table in CLIENT_TABLES}
            leftovers["companies"] = _count(cur, "companies",
                                            "company_id = 8")
        if any(leftovers.values()):
            # only company_id=8 rows, FK-ordered, then re-verify
            _delete_company_rows(conn, 8)
            conn.commit()
            removed = True
            with conn.cursor() as cur:
                leftovers = {table: _count(cur, table, "company_id = 8")
                             for table in CLIENT_TABLES}
                leftovers["companies"] = _count(cur, "companies",
                                                "company_id = 8")
    finally:
        conn.close()
    assert not any(leftovers.values()), (
        "company_id 8 (ZZZ smoke) leftovers{}: {!r}".format(
            " (deleted by this test)" if removed else "", leftovers))
