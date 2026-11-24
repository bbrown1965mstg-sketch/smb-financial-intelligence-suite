#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for SMB-Watchdog (watchdog.py).

Test strategy
-------------
* Pure-logic tests (exit codes, table truncation, renderers) need no DB.
* Offline DB-path tests run ``watchdog.main`` against a ``FakeConn`` -- a
  programmable stand-in connection whose cursor answers each query by
  matching a distinctive SQL fragment. ``fetch_all`` and every check run
  for real against this fake, so dict-building and control flow are
  exercised without touching PostgreSQL.
* Live integration tests (read-only enforcement, real scan exit code 2)
  run against the demo warehouse and ``pytest.skip`` gracefully when the
  warehouse is unreachable, so the suite passes offline.

SAFETY: the suite is strictly read-only. The only write-shaped statement
it ever sends is an UPDATE that matches zero rows (``company_id = -999``)
through a connection already verified read-only -- and it asserts that the
server rejects it. No INSERT/UPDATE/DELETE/DDL is ever issued against the
warehouse, and report files are written only to pytest ``tmp_path`` dirs.
"""

import functools
import json
import os
import sys
from datetime import date, datetime

import psycopg2
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import watchdog  # noqa: E402  (local module under test)

# ---------------------------------------------------------------------------
# Constants / helpers
# ---------------------------------------------------------------------------

LIVE_DSN = watchdog.DEFAULT_DSN  # postgresql://postgres:postgres@localhost:5432/smb_warehouse

COMPANY_ROW = {"company_id": 7, "company_name": "MockCo Industries",
               "industry_vertical": "Consulting"}


@functools.lru_cache(maxsize=None)
def live_db_reachable() -> bool:
    """Cheap probe with a short connect timeout; cached for the whole run."""
    try:
        conn = psycopg2.connect(LIVE_DSN + ("&" if "?" in LIVE_DSN else "?") + "connect_timeout=2")
        conn.close()
        return True
    except Exception:  # noqa: BLE001 -- unreachable is an expected outcome
        return False


def require_live_db():
    if not live_db_reachable():
        pytest.skip("live warehouse unreachable at %s" % watchdog.describe_dsn(LIVE_DSN))


# ---------------------------------------------------------------------------
# Fake connection: cursor() context manager + fragment-routed query answers
# ---------------------------------------------------------------------------


class FakeCursor:
    """Mimics the slice of psycopg2's cursor API that watchdog.py uses."""

    def __init__(self, conn):
        self._conn = conn
        self.description = None
        self._rows = []

    def execute(self, sql, params=None):
        self._conn.executed.append(sql)
        rows = self._conn.responder(sql, params)
        self._rows = list(rows)
        # fetch_all reads column names from description[0]; FakeConn rows are
        # dicts whose keys are the column names in SELECT order.
        self.description = [(k,) for k in self._rows[0].keys()] if self._rows else []

    def fetchone(self):
        return tuple(self._rows[0].values()) if self._rows else None

    def fetchall(self):
        return [tuple(r.values()) for r in self._rows]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    """Connection stand-in; answers queries by SQL-fragment routing.

    ``routes`` is a list of ``(fragment, rows)`` pairs (fragment lowercase;
    rows may be a list of dicts or a zero-arg callable returning one).
    ``default`` is returned for unmatched SQL; the sentinel STRICT raises
    instead so missing routes fail loudly.
    """

    STRICT = object()

    def __init__(self, routes, default=STRICT):
        self._routes = [(frag.lower(), rows) for frag, rows in routes]
        self._default = default
        self.executed = []
        self.autocommit = False
        self.closed = False

    def responder(self, sql, params=None):
        low = " ".join(str(sql).lower().split())
        for frag, rows in self._routes:
            if frag in low:
                return rows() if callable(rows) else rows
        if self._default is FakeConn.STRICT:
            raise AssertionError("FakeConn has no route for SQL:\n%s" % sql)
        return self._default

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


def alert_row(rid, severity="high", anomaly_type="labor_overrun", job_number="J-1001",
              category=None, period=None, deviation=42.5,
              details='{"actual_labor_cost": 1234.0, "estimated_labor_cost": 800.0}'):
    return {"id": rid, "anomaly_type": anomaly_type, "severity": severity,
            "category": category, "period": period,
            "job_id": 1 if anomaly_type == "labor_overrun" else None,
            "deviation_pct": deviation, "details": details, "job_number": job_number}


def populated_routes(alert_rows=None):
    """One company, one high alert, flagged margin, overtime + unprofitable whale."""
    if alert_rows is None:
        alert_rows = [alert_row(101),
                      alert_row(102, anomaly_type="expense_spike", job_number=None,
                                category="Software", period=date(2026, 10, 1),
                                deviation=310.0,
                                details=json.dumps({"category": "Software",
                                                    "month_total": 9000.0,
                                                    "prior_6m_mean": 2000.0}))]
    return [
        ("from companies", [COMPANY_ROW]),
        ("group by severity, anomaly_type",
         [{"severity": "high", "anomaly_type": "labor_overrun", "n": len(alert_rows)},
          {"severity": "medium", "anomaly_type": "expense_spike", "n": 1}]),
        ("order by case a.severity", alert_rows),
        ("from job_types jt", [
            {"job_type_id": 1, "job_type": "Consulting",
             "revenue": 100000.0, "expense": 95000.0},   # margin 5% vs target 40 -> FLAG
            {"job_type_id": 2, "job_type": "Managed Services",
             "revenue": 50000.0, "expense": 20000.0},    # margin 60% vs target 35 -> OK
        ]),
        ("distinct on (job_type_id)", [
            {"job_type_id": 1, "period": date(2026, 11, 1), "target_pct": 40.0},
            {"job_type_id": 2, "period": date(2026, 11, 1), "target_pct": 35.0},
        ]),
        ("from labor_logs l", [
            {"employee_id": 1, "employee": "Dana Reed", "role": "Consultant",
             "hours": 200.0, "days": 18},                # > 184 h -> OVERTIME
            {"employee_id": 2, "employee": "Lee Park", "role": "Analyst",
             "hours": 120.0, "days": 19},
        ]),
        ("is_whale = true", [
            {"customer_id": 11, "customer_name": "Acme Corp", "revenue": 50000.0,
             "total_cost": 60000.0, "gross_margin": -10000.0, "margin_pct": -20.0,
             "job_count": 3, "days_since_last_job": 5},  # margin < 0 -> UNPROFITABLE
        ]),
        ("order by margin_pct asc", []),                 # fallback unused when whales exist
        ("sum(amount) filter",
         [{"total_revenue": 320000.0, "total_expense": 290000.0}]),  # cash proxy 30000
        ("from mv_monthly_summary", [
            {"month_start": date(2026, 10, 1), "revenue": 30000.0, "total_expense": 33000.0},
            {"month_start": date(2026, 9, 1), "revenue": 30000.0, "total_expense": 31000.0},
            {"month_start": date(2026, 8, 1), "revenue": 30000.0, "total_expense": 31000.0},
        ]),
        ("from forecasts", [
            {"period_start": date(2026, 12, 1), "model": "arima", "predicted": 1500.0,
             "ci_low": -500.0, "ci_high": 3500.0, "horizon_months": 3,
             "run_date": datetime(2026, 11, 20, 6, 0)},
        ]),
    ]


EMPTY_ROUTES = [("from companies", [COMPANY_ROW])]


def run_main_mocked(monkeypatch, tmp_path, routes, extra_args=(), default=FakeConn.STRICT):
    """Run watchdog.main() with connect_ro swapped for a FakeConn."""
    fake = FakeConn(routes, default=default)
    monkeypatch.setattr(watchdog, "connect_ro", lambda dsn: fake)
    rc = watchdog.main(["--quiet", "--output-dir", str(tmp_path)] + list(extra_args))
    return rc, fake


def make_report(alerts_status, alerts_metrics, extra_checks=()):
    alerts = watchdog.CheckResult(key="alerts", title="Open Critical / Warning Anomaly Alerts",
                                  status=alerts_status, summary="synthetic",
                                  metrics=alerts_metrics)
    return watchdog.CompanyReport(company_id=7, name="MockCo Industries",
                                  industry="Consulting",
                                  checks=[alerts] + list(extra_checks))


def sample_scan(exit_code=2):
    chk = watchdog.CheckResult(
        key="alerts", title="Open Critical / Warning Anomaly Alerts", status="CRITICAL",
        summary="2 open critical/warning alert(s): 2 high labor_overrun.",
        tables=[watchdog.Table(title="Open critical/warning anomaly alerts",
                               headers=["SEV", "TYPE"],
                               rows=[["HIGH", "labor_overrun"]])],
        notes=["severity mapping note"], metrics={"high": 2, "medium": 0, "low": 0})
    company = watchdog.CompanyReport(company_id=7, name="MockCo Industries",
                                     industry="Consulting", checks=[chk])
    overall = watchdog.OverallResult(status="CRITICAL",
                                     summary="2 open critical alert(s) across 1 company(ies).")
    return watchdog.ScanResult(generated_at="2026-11-22 06:00:00",
                               warehouse="localhost:5432/smb_warehouse",
                               company_count=1, companies=[company],
                               overall=overall, exit_code=exit_code)


# ---------------------------------------------------------------------------
# 1. Read-only connection verification
# ---------------------------------------------------------------------------


def test_connect_ro_live_session_is_read_only():
    require_live_db()
    conn = watchdog.connect_ro(LIVE_DSN)
    try:
        assert conn.autocommit is True
        with conn.cursor() as cur:
            cur.execute("SELECT current_setting('default_transaction_read_only')")
            assert cur.fetchone()[0] == "on"
    finally:
        conn.close()


def test_connect_ro_live_rejects_writes():
    require_live_db()
    conn = watchdog.connect_ro(LIVE_DSN)
    try:
        # WHERE matches zero rows; the read-only session must still reject
        # the statement server-side (SQLSTATE 25006) before touching anything.
        with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction):
            with conn.cursor() as cur:
                cur.execute("UPDATE companies SET company_name = company_name "
                            "WHERE company_id = -999")
    finally:
        conn.close()


def test_connect_ro_issues_set_and_verifies(monkeypatch):
    fake = FakeConn([("select current_setting", [{"current_setting": "on"}])], default=[])
    captured = {}

    def fake_connect(dsn):
        captured["dsn"] = dsn
        return fake

    monkeypatch.setattr(watchdog.psycopg2, "connect", fake_connect)
    conn = watchdog.connect_ro("postgresql://user:pw@host.example:5432/db")
    assert conn is fake
    assert "connect_timeout" in captured["dsn"]           # timeout auto-appended
    assert conn.autocommit is True                        # enforced before any query
    joined = "\n".join(conn.executed).lower()
    assert "set default_transaction_read_only = on" in joined
    assert "select current_setting('default_transaction_read_only')" in joined
    assert conn.closed is False


def test_connect_ro_raises_when_verification_fails(monkeypatch):
    fake = FakeConn([("select current_setting", [{"current_setting": "off"}])], default=[])

    def fake_connect(dsn):
        return fake

    monkeypatch.setattr(watchdog.psycopg2, "connect", fake_connect)
    with pytest.raises(RuntimeError, match="read-only enforcement failed"):
        watchdog.connect_ro("postgresql://user:pw@host.example:5432/db")
    assert fake.closed is True  # connection is torn down, never returned


# ---------------------------------------------------------------------------
# 2. Exit-code behavior
# ---------------------------------------------------------------------------


def test_compute_exit_code_zero_alerts_and_empty_reports():
    healthy = make_report("HEALTHY", {"high": 0, "medium": 0, "low": 0})
    assert watchdog.compute_exit_code([healthy]) == watchdog.EXIT_OK
    assert watchdog.compute_exit_code([]) == watchdog.EXIT_OK  # nothing scanned


def test_compute_exit_code_warnings_allowed():
    warnings_only = make_report("WARNING", {"high": 0, "medium": 3, "low": 1})
    assert watchdog.compute_exit_code([warnings_only]) == watchdog.EXIT_OK


def test_compute_exit_code_high_alert_is_critical():
    critical = make_report("CRITICAL", {"high": 1, "medium": 2, "low": 0})
    assert watchdog.compute_exit_code([critical]) == watchdog.EXIT_CRITICAL


def test_compute_exit_code_ignores_non_alert_critical_status():
    # Per spec: exit 2 is keyed to open severity='high' anomaly ALERTS only;
    # a CRITICAL runway status must NOT change the exit code.
    runway = watchdog.CheckResult(key="runway", title="90-Day Cash Runway",
                                  status="CRITICAL", summary="runway 15 days",
                                  metrics={"runway_days": 15.0})
    report = make_report("HEALTHY", {"high": 0, "medium": 0, "low": 0},
                         extra_checks=[runway])
    assert watchdog.compute_exit_code([report]) == watchdog.EXIT_OK


def test_compute_exit_code_alert_check_error_is_query_catastrophe():
    errored = make_report("ERROR", {})  # alerts check failed -> no exit signal
    assert watchdog.compute_exit_code([errored]) == watchdog.EXIT_ERROR
    # Mixed: one ERROR + one healthy report -> healthy side still decides.
    healthy = make_report("HEALTHY", {"high": 0, "medium": 0, "low": 0})
    assert watchdog.compute_exit_code([errored, healthy]) == watchdog.EXIT_OK
    critical = make_report("CRITICAL", {"high": 2, "medium": 0, "low": 0})
    assert watchdog.compute_exit_code([errored, critical]) == watchdog.EXIT_CRITICAL


def test_main_returns_3_when_db_unreachable(monkeypatch, tmp_path):
    def unreachable(dsn):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(watchdog, "connect_ro", unreachable)
    assert watchdog.main(["--quiet", "--output-dir", str(tmp_path)]) == watchdog.EXIT_ERROR


def test_main_returns_3_when_companies_registry_unreadable(monkeypatch, tmp_path):
    def broken_registry():
        raise ValueError("companies table unreadable")

    fake = FakeConn([("from companies", broken_registry)])
    monkeypatch.setattr(watchdog, "connect_ro", lambda dsn: fake)
    assert watchdog.main(["--quiet", "--output-dir", str(tmp_path)]) == watchdog.EXIT_ERROR


def test_main_returns_3_when_report_write_fails(monkeypatch, tmp_path):
    fake = FakeConn(populated_routes())
    monkeypatch.setattr(watchdog, "connect_ro", lambda dsn: fake)

    def failing_write(scan, args):
        raise OSError("disk full")

    monkeypatch.setattr(watchdog, "write_reports", failing_write)
    assert watchdog.main(["--quiet", "--output-dir", str(tmp_path)]) == watchdog.EXIT_ERROR


def test_main_live_exit_code_2_and_report_markers(tmp_path, monkeypatch):
    require_live_db()
    monkeypatch.delenv(watchdog.DSN_ENV_VAR, raising=False)  # force documented DSN
    rc = watchdog.main(["--quiet", "--output-dir", str(tmp_path)])
    assert rc == watchdog.EXIT_CRITICAL  # demo warehouse has open high-severity alerts
    md = (tmp_path / "digest_report.md").read_text(encoding="utf-8")
    html = (tmp_path / "digest_report.html").read_text(encoding="utf-8")
    assert "BrightPath Works (Demo)" in md
    assert "BrightPath Works (Demo)" in html


# ---------------------------------------------------------------------------
# 3. Markdown / HTML report generation (mocked pipeline)
# ---------------------------------------------------------------------------


def test_main_mocked_writes_reports_with_markers(monkeypatch, tmp_path):
    rc, _ = run_main_mocked(monkeypatch, tmp_path, populated_routes())
    assert rc == watchdog.EXIT_CRITICAL

    assert sorted(os.listdir(tmp_path)) == ["digest_report.html", "digest_report.md"]
    md = (tmp_path / "digest_report.md").read_text(encoding="utf-8")
    html = (tmp_path / "digest_report.html").read_text(encoding="utf-8")
    assert md and html

    # Markdown: overall status, company name, all four check section headers.
    assert "# SMB-Watchdog -- Executive Morning Digest" in md
    assert "**Overall status: CRITICAL**" in md
    assert "2 open critical (severity='high') alert(s) across 1 company(ies)" in md
    assert "MockCo Industries" in md
    assert "### (a) Open Critical / Warning Anomaly Alerts -- **CRITICAL**" in md
    assert "### (b) Trailing 30-Day Gross Margin vs Target (by Job Type) -- **WARNING**" in md
    assert "### (c) Overtime-Heavy Employees & Whale Clients -- **WARNING**" in md
    assert "### (d) 90-Day Cash Runway -- **HEALTHY**" in md
    assert "## Interpretations & Method Notes" in md
    assert "**Exit code: 2**" in md

    # Markdown must never carry terminal ANSI escapes or hidden check errors.
    assert "\x1b" not in md
    assert "Query failed" not in md

    # HTML: same content, self-contained (inline CSS only, no external assets).
    assert "MockCo Industries" in html
    assert 'class="badge badge-CRITICAL"' in html
    assert '<span class="tok-critical">UNPROFITABLE</span>' in html
    assert "<style>" in html and "</html>" in html
    assert "<script" not in html.lower()
    assert "<link" not in html.lower()
    assert "\x1b" not in html


def test_main_mocked_empty_workspace_is_healthy(monkeypatch, tmp_path):
    # A company row with no alerts/ledger/labor/benchmarks: every check must
    # render gracefully ('no data' style), no exception, no ZeroDivisionError.
    rc, _ = run_main_mocked(monkeypatch, tmp_path, EMPTY_ROUTES, default=[])
    assert rc == watchdog.EXIT_OK

    md = (tmp_path / "digest_report.md").read_text(encoding="utf-8")
    assert "**Overall status: HEALTHY**" in md
    assert "**Exit code: 0**" in md
    assert "### (a) Open Critical / Warning Anomaly Alerts -- **HEALTHY**" in md
    assert "### (b) Trailing 30-Day Gross Margin vs Target (by Job Type) -- **HEALTHY**" in md
    assert "### (c) Overtime-Heavy Employees & Whale Clients -- **HEALTHY**" in md
    assert "### (d) 90-Day Cash Runway -- **INFO**" in md
    # Graceful 'no data' summaries from each check.
    assert "0 open critical/warning alert(s): none." in md
    assert "0 job type(s) with revenue in the trailing 30 days" in md
    assert "No whale (is_whale) rows; showing 0 lowest-margin customer(s)" in md
    assert "burn cannot be computed" in md
    # Empty tables still render their header row.
    assert "| SEV | TYPE | JOB / CATEGORY | PERIOD | DEV | CONTEXT |" in md
    assert "| METRIC | VALUE |" in md
    assert "Query failed" not in md
    assert "ZeroDivisionError" not in md

    html = (tmp_path / "digest_report.html").read_text(encoding="utf-8")
    assert 'class="badge badge-HEALTHY"' in html
    assert 'class="badge badge-INFO"' in html


# ---------------------------------------------------------------------------
# 4. Check-level behavior against FakeConn
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("counts,expected_status,exp_high,exp_medium,exp_low", [
    ([{"severity": "high", "anomaly_type": "labor_overrun", "n": 2}],
     "CRITICAL", 2, 0, 0),
    ([{"severity": "medium", "anomaly_type": "expense_spike", "n": 1}],
     "WARNING", 0, 1, 0),
    ([{"severity": "low", "anomaly_type": "expense_spike", "n": 4}],
     "HEALTHY", 0, 0, 4),
    ([], "HEALTHY", 0, 0, 0),
])
def test_check_alerts_status_mapping(counts, expected_status, exp_high, exp_medium, exp_low):
    conn = FakeConn([("group by severity, anomaly_type", counts),
                     ("order by case a.severity", [])])
    res = watchdog.check_alerts(conn, 7, 10)
    assert res.key == "alerts"
    assert res.status == expected_status
    assert res.metrics["high"] == exp_high
    assert res.metrics["medium"] == exp_medium
    assert res.metrics["low"] == exp_low


def test_check_alerts_rows_and_context_rendering():
    rows = [alert_row(101),
            alert_row(102, severity="medium", anomaly_type="expense_spike", job_number=None,
                      category="Software", period=date(2026, 10, 1), deviation=310.0,
                      details=json.dumps({"category": "Software", "month_total": 9000.0,
                                          "prior_6m_mean": 2000.0}))]
    conn = FakeConn([("group by severity, anomaly_type",
                      [{"severity": "high", "anomaly_type": "labor_overrun", "n": 1},
                       {"severity": "medium", "anomaly_type": "expense_spike", "n": 1}]),
                     ("order by case a.severity", rows)])
    res = watchdog.check_alerts(conn, 7, 10)
    assert res.status == "CRITICAL"
    table = res.tables[0]
    assert [r[0] for r in table.rows] == ["HIGH", "MEDIUM"]
    assert table.rows[0][1] == "labor_overrun"
    assert table.rows[0][2] == "J-1001"
    assert "actual $1,234.00 vs est $800.00" in table.rows[0][5]  # details JSONB parsed
    assert table.rows[1][2] == "Software"                          # category subject
    assert table.rows[1][3] == "2026-10"                           # fmt_month(period)


def test_check_alerts_truncates_to_max_rows():
    rows = [alert_row(200 + i) for i in range(5)]
    conn = FakeConn([("group by severity, anomaly_type",
                      [{"severity": "high", "anomaly_type": "labor_overrun", "n": 5}]),
                     ("order by case a.severity", rows)])
    res = watchdog.check_alerts(conn, 7, max_rows=2)
    assert len(res.tables[0].rows) == 2
    assert res.tables[0].note == "showing 2 of 5 rows"


def test_build_table_truncation_and_note():
    rows = [["r%d" % i] for i in range(6)]
    t = watchdog.build_table("T", ["H"], rows, 4, note="source: x")
    assert len(t.rows) == 4
    assert t.note == "source: x; showing 4 of 6 rows"
    t2 = watchdog.build_table("T", ["H"], rows[:2], 10)
    assert t2.note is None
    assert len(t2.rows) == 2


def test_check_margin_flag_no_benchmark_and_status():
    rows = [
        {"job_type_id": 1, "job_type": "Consulting",
         "revenue": 100000.0, "expense": 95000.0},      # 5% vs 40 -> FLAG
        {"job_type_id": 2, "job_type": "Managed Services",
         "revenue": 50000.0, "expense": 20000.0},       # 60% vs 35 -> OK
        {"job_type_id": 3, "job_type": "Training",
         "revenue": 10000.0, "expense": 1000.0},        # no benchmark row
    ]
    targets = [{"job_type_id": 1, "period": date(2026, 11, 1), "target_pct": 40.0},
               {"job_type_id": 2, "period": date(2026, 11, 1), "target_pct": 35.0}]
    conn = FakeConn([("from job_types jt", rows), ("distinct on (job_type_id)", targets)])
    res = watchdog.check_margin(conn, 7, 10)
    assert res.status == "WARNING"
    assert [r[6] for r in res.tables[0].rows] == ["FLAG", "OK", "NO BENCHMARK"]
    assert res.metrics["flagged"] == 1
    assert res.metrics["types_evaluated"] == 3
    assert "1 type(s) had no national job_market benchmark" in res.summary


def test_check_margin_healthy_when_all_within_target():
    rows = [{"job_type_id": 1, "job_type": "Consulting",
             "revenue": 100000.0, "expense": 50000.0}]   # 50% vs 40 -> OK
    targets = [{"job_type_id": 1, "period": date(2026, 11, 1), "target_pct": 40.0}]
    conn = FakeConn([("from job_types jt", rows), ("distinct on (job_type_id)", targets)])
    res = watchdog.check_margin(conn, 7, 10)
    assert res.status == "HEALTHY"
    assert res.metrics["flagged"] == 0


def test_check_labor_whales_overtime_and_unprofitable_whale():
    labor = [{"employee_id": 1, "employee": "Dana Reed", "role": "Consultant",
              "hours": 200.0, "days": 18},
             {"employee_id": 2, "employee": "Lee Park", "role": "Analyst",
              "hours": 120.0, "days": 19}]
    whales = [{"customer_id": 11, "customer_name": "Acme Corp", "revenue": 50000.0,
               "total_cost": 60000.0, "gross_margin": -10000.0, "margin_pct": -20.0,
               "job_count": 3, "days_since_last_job": 5}]
    conn = FakeConn([("from labor_logs l", labor), ("is_whale = true", whales)])
    res = watchdog.check_labor_whales(conn, 7, 10)
    assert res.status == "WARNING"
    assert res.tables[0].rows[0][4] == "OVERTIME"     # 200 h > 184 h threshold
    assert res.tables[0].rows[1][4] == "OK"
    assert res.tables[1].rows[0][7] == "UNPROFITABLE"  # gross_margin < 0
    assert res.metrics == {"overtime_flags": 1, "unprofitable_whales": 1}


def test_check_labor_whales_fallback_when_no_whales():
    fallback = [{"customer_id": 21, "customer_name": "Beta LLC", "revenue": 8000.0,
                 "total_cost": 9000.0, "gross_margin": -1000.0, "margin_pct": -12.5},
                {"customer_id": 22, "customer_name": "Gamma Co", "revenue": 12000.0,
                 "total_cost": 11000.0, "gross_margin": 1000.0, "margin_pct": 8.3}]
    conn = FakeConn([("from labor_logs l", []), ("is_whale = true", []),
                     ("order by margin_pct asc", fallback)])
    res = watchdog.check_labor_whales(conn, 7, 10)
    assert res.status == "HEALTHY"  # informational only
    assert "No whale (is_whale) rows; showing 2 lowest-margin customer(s)" in res.summary
    assert res.tables[1].rows[0][5] == "LOW MARGIN"


def _month_rows(nets):
    labels = [date(2026, 10, 1), date(2026, 9, 1), date(2026, 8, 1), date(2026, 7, 1)]
    return [{"month_start": labels[i], "revenue": r, "total_expense": e}
            for i, (r, e) in enumerate(nets)]


def _runway_conn(total_revenue, total_expense, month_rows):
    return FakeConn([
        ("sum(amount) filter", [{"total_revenue": total_revenue,
                                 "total_expense": total_expense}]),
        ("from mv_monthly_summary", month_rows),
        ("from forecasts", []),
    ])


@pytest.mark.parametrize("rev,exp,nets,expected_status,cell_substr,expected_days", [
    # cash 500, burn 1000/mo -> 15 days -> CRITICAL
    (1000.0, 500.0, [(1000.0, 2000.0)] * 3, "CRITICAL", "15 days", 15.0),
    # cash 2000, burn 1000/mo -> 60 days -> WARNING
    (5000.0, 3000.0, [(1000.0, 2000.0)] * 3, "WARNING", "60 days", 60.0),
    # positive trailing average -> no burn computed
    (5000.0, 3000.0, [(1500.0, 1000.0)] * 3, "HEALTHY", "n/a (positive cash flow)", None),
    # no monthly history -> INFO, runway n/a
    (5000.0, 3000.0, [], "INFO", "n/a", None),
    # cash proxy <= 0 with positive burn -> runway forced to 0 days
    (100.0, 500.0, [(1000.0, 2000.0)] * 3, "CRITICAL", "0 days", 0.0),
])
def test_check_runway_bands(rev, exp, nets, expected_status, cell_substr, expected_days):
    conn = _runway_conn(rev, exp, _month_rows(nets))
    res = watchdog.check_runway(conn, 7, 10)
    assert res.status == expected_status
    runway_row = next(r for r in res.tables[0].rows if r[0] == "Runway")
    assert cell_substr in runway_row[1]
    if expected_days is None:
        assert res.metrics["runway_days"] is None
    else:
        assert res.metrics["runway_days"] == pytest.approx(expected_days, abs=0.5)
    if rev - exp <= 0 and expected_days == 0.0:
        assert any("cash proxy is <= 0" in n for n in res.notes)


def test_check_runway_forecast_context():
    forecasts = [{"period_start": date(2026, 12, 1), "model": "arima", "predicted": 1500.0,
                  "ci_low": -500.0, "ci_high": 3500.0, "horizon_months": 3,
                  "run_date": datetime(2026, 11, 20, 6, 0)},
                 {"period_start": date(2027, 1, 1), "model": "arima", "predicted": -250.0,
                  "ci_low": -2000.0, "ci_high": 2000.0, "horizon_months": 3,
                  "run_date": datetime(2026, 11, 20, 6, 0)}]
    conn = _runway_conn(5000.0, 3000.0, _month_rows([(1500.0, 1000.0)] * 3))
    conn._routes = [(frag, forecasts if frag == "from forecasts" else rows)
                    for frag, rows in conn._routes]
    res = watchdog.check_runway(conn, 7, 10)
    assert res.metrics["forecast_90d"] == pytest.approx(1250.0)
    forecast_row = next(r for r in res.tables[0].rows if r[0].startswith("Projected 90-day"))
    assert "+$1,250.00 (Dec 2026-Jan 2027, 2 row(s))" == forecast_row[1]


# ---------------------------------------------------------------------------
# 5. CLI flags, stable filenames, renderer purity
# ---------------------------------------------------------------------------


def test_main_mocked_no_html_writes_md_only(monkeypatch, tmp_path):
    rc, _ = run_main_mocked(monkeypatch, tmp_path, populated_routes(),
                            extra_args=["--no-html"])
    assert rc == watchdog.EXIT_CRITICAL
    assert (tmp_path / "digest_report.md").exists()
    assert not (tmp_path / "digest_report.html").exists()


def test_main_mocked_no_md_writes_html_only(monkeypatch, tmp_path):
    rc, _ = run_main_mocked(monkeypatch, tmp_path, populated_routes(),
                            extra_args=["--no-md"])
    assert rc == watchdog.EXIT_CRITICAL
    assert (tmp_path / "digest_report.html").exists()
    assert not (tmp_path / "digest_report.md").exists()


def test_main_mocked_max_alerts_per_table_truncates(monkeypatch, tmp_path):
    routes = populated_routes(alert_rows=[alert_row(300 + i) for i in range(5)])
    rc, _ = run_main_mocked(monkeypatch, tmp_path, routes,
                            extra_args=["--max-alerts-per-table", "2"])
    assert rc == watchdog.EXIT_CRITICAL
    md = (tmp_path / "digest_report.md").read_text(encoding="utf-8")
    assert len([ln for ln in md.splitlines() if ln.startswith("| HIGH |")]) == 2
    assert "*showing 2 of 5 rows*" in md


def test_main_mocked_reports_overwritten_at_stable_paths(monkeypatch, tmp_path):
    # Run 1: critical data -> digest files at stable paths with exit-code 2 text.
    with monkeypatch.context() as m:
        rc1, _ = run_main_mocked(m, tmp_path, populated_routes())
        assert rc1 == watchdog.EXIT_CRITICAL
    md1 = (tmp_path / "digest_report.md").read_text(encoding="utf-8")
    assert "**Exit code: 2**" in md1

    # Run 2: same paths, empty workspace -> files overwritten in place.
    with monkeypatch.context() as m:
        rc2, _ = run_main_mocked(m, tmp_path, EMPTY_ROUTES, default=[])
        assert rc2 == watchdog.EXIT_OK
    md2 = (tmp_path / "digest_report.md").read_text(encoding="utf-8")
    assert "**Exit code: 0**" in md2
    assert "**Exit code: 2**" not in md2
    assert sorted(os.listdir(tmp_path)) == ["digest_report.html", "digest_report.md"]


def test_render_markdown_written_to_disk_has_no_ansi(tmp_path):
    scan = sample_scan()
    text = watchdog.render_markdown(scan)
    path = tmp_path / "digest_report.md"
    path.write_text(text, encoding="utf-8")
    back = path.read_text(encoding="utf-8")
    assert "\x1b" not in back
    assert "## MockCo Industries (company_id=7 -- Consulting)" in back
    assert "### (a) Open Critical / Warning Anomaly Alerts -- **CRITICAL**" in back


def test_render_terminal_palette_on_and_off():
    scan = sample_scan()
    plain = watchdog.render_terminal(scan, watchdog.Palette(enabled=False))
    assert "\x1b" not in plain
    assert "COMPANY: MockCo Industries (id=7, Consulting)" in plain
    assert "[CRITICAL]" in plain
    assert "EXIT CODE 2" in plain

    fancy = watchdog.render_terminal(scan, watchdog.Palette(enabled=True))
    assert "\x1b[" in fancy                       # ANSI present when enabled
    assert "\x1b[0m" in fancy                     # and reset sequences
    assert "[CRITICAL]" in fancy
    assert "EXIT CODE 2" in fancy


def test_render_html_self_contained():
    out = watchdog.render_html(sample_scan())
    assert "<script" not in out.lower()
    assert "<link" not in out.lower()
    assert "<style>" in out


def test_parse_args_version_flag_and_row_cap_validation(capsys):
    with pytest.raises(SystemExit) as ei:
        watchdog.parse_args(["--version"])
    assert ei.value.code == 0
    assert watchdog.__version__ in capsys.readouterr().out
    with pytest.raises(SystemExit):
        watchdog.parse_args(["--max-alerts-per-table", "0"])  # must be >= 1
    args = watchdog.parse_args(["--quiet", "--max-alerts-per-table", "3"])
    assert args.quiet is True and args.max_alerts_per_table == 3


def test_aggregate_overall_empty_and_warning_summary():
    overall = watchdog.aggregate_overall([])
    assert overall.status == "INFO"
    assert "No companies found" in overall.summary
    warn_report = make_report("WARNING", {"high": 0, "medium": 1, "low": 0})
    overall2 = watchdog.aggregate_overall([warn_report])
    assert overall2.status == "WARNING"
    assert "No critical alerts; 1 check(s) at warning level across 1 company(ies)" in overall2.summary
