#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SMB-Watchdog -- headless executive watchdog & morning digest CLI.

Scans an existing PostgreSQL SMB data warehouse READ-ONLY, evaluates four
executive risk checks per company, prints a color-coded terminal summary and
writes stable-filename digest reports (``digest_report.md`` and
``digest_report.html``) that cron consumers can pick up after every run.

READ-ONLY POSTURE
    This tool is strictly a reader. Every connection created by
    :func:`connect_ro` runs ``SET default_transaction_read_only = on``
    immediately after connect and the setting is re-verified via
    ``current_setting('default_transaction_read_only')`` before any scan
    query runs. The tool issues SELECT statements only -- it never INSERTs,
    UPDATEs, DELETEs, DDLs, VACUUMs, or calls functions that write to the
    database. A compromised or mistaken code path therefore cannot mutate
    the warehouse.

CHECKS (run per company, looping over ALL rows of ``companies``)
    (a) Open CRITICAL/WARNING anomaly alerts
        anomaly_alerts WHERE is_resolved = false AND severity IN
        ('high','medium'); severity 'high' maps to CRITICAL, 'medium' to
        WARNING. Labor overruns join to jobs (job_number); expense spikes
        use category/period; details JSONB is parsed for human context.
    (b) Trailing 30-day gross margin vs target, by job type
        financial_ledger attributed through jobs -> job_types over the last
        30 days (overhead entries with no job attribution are excluded from
        the per-type view); target = latest national ``job_market`` row in
        market_benchmarks per job type. FLAG when actual margin is more
        than 5.0 PERCENTAGE POINTS below target.
    (c) Overtime-heavy employees & whale clients
        Top 3 employees by trailing-30-day hours (WARNING when hours
        exceed 184 h = 1.15 x 160 h). Whale clients from
        mv_customer_profitability (is_whale = true); WARNING when
        gross_margin < 0. If no whale rows exist, the 3 lowest-margin
        customers with revenue > 0 are listed as informational context.
    (d) 90-day cash runway
        Cash-on-hand proxy = lifetime SUM(revenue) - SUM(expense) from
        financial_ledger (the ledger is the only cash source in the
        warehouse). Burn basis = average monthly net cash flow over the 3
        most recent months in mv_monthly_summary. Runway bands:
        < 30 days CRITICAL, 30-90 days WARNING, > 90 days HEALTHY; a
        non-negative trailing average is reported as "positive cash flow --
        no burn". Forecasts (series='net_cash_flow', period_start in
        (current month, today + 90 days]) are shown as context.

EXIT CODES
    0  healthy -- no critical alerts across scanned companies (warnings
       allowed).
    2  at least one open critical (severity='high') anomaly alert exists
       across scanned companies. Per spec, exit code 2 is keyed to open
       severity='high' anomaly alerts; other CRITICAL check statuses (e.g.
       runway < 30 days) are displayed prominently but do not change the
       exit code.
    3  operational error -- warehouse unreachable, companies registry not
       readable, report write failure, or every alert check failing
       (query catastrophe).

USAGE
    python3 watchdog.py                     # scan + terminal summary + reports
    python3 watchdog.py --quiet             # reports only (cron friendly)
    SMB_WATCHDOG_DSN=postgresql://... python3 watchdog.py --quiet
    python3 watchdog.py --dsn postgresql://user:pw@host:5432/db --no-html

Requires Python 3.9+ and psycopg2-binary. No other third-party dependencies.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import sys
import textwrap
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

import psycopg2

__version__ = "1.0.0"

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/smb_warehouse"
DSN_ENV_VAR = "SMB_WATCHDOG_DSN"

TRAILING_DAYS = 30               # rolling window for checks (b) and (c)
TRAILING_MONTHS = 3              # burn-basis window for check (d)
MARGIN_FLAG_PP = 5.0             # "dropped >5% below target" == 5 percentage points
FULL_TIME_HOURS = 160.0          # standard monthly full-time hours
OVERTIME_FACTOR = 1.15
OVERTIME_THRESHOLD_HOURS = FULL_TIME_HOURS * OVERTIME_FACTOR   # 184.0 h
RUNWAY_CRITICAL_DAYS = 30.0      # runway < 30d -> CRITICAL
RUNWAY_WARNING_DAYS = 90.0       # 30..90d -> WARNING, > 90d -> HEALTHY

EXIT_OK = 0
EXIT_CRITICAL = 2
EXIT_ERROR = 3

EXIT_MEANINGS: Dict[int, str] = {
    EXIT_OK: "healthy -- no critical alerts (warnings allowed)",
    EXIT_CRITICAL: "at least one open critical (severity='high') anomaly alert",
    EXIT_ERROR: "operational error (warehouse unreachable / query catastrophe)",
}

STATUS_CRITICAL = "CRITICAL"
STATUS_WARNING = "WARNING"
STATUS_HEALTHY = "HEALTHY"
STATUS_INFO = "INFO"
STATUS_ERROR = "ERROR"

STATUS_SEVERITY_RANK: Dict[str, int] = {
    STATUS_CRITICAL: 3,
    STATUS_WARNING: 2,
    STATUS_HEALTHY: 1,
    STATUS_INFO: 0,
    STATUS_ERROR: -1,
}

CHECK_LETTERS = {"alerts": "(a)", "margin": "(b)", "labor_whales": "(c)", "runway": "(d)"}

TERMINAL_TITLES = {
    "alerts": "OPEN CRITICAL/WARNING ALERTS",
    "margin": "TRAILING 30-DAY MARGIN VS TARGET",
    "labor_whales": "LABOR & WHALE CLIENTS",
    "runway": "90-DAY CASH RUNWAY",
}

# Tokens (exact cell text) colorized in terminal tables and HTML status cells.
STATUS_TOKENS = {
    "CRITICAL": "critical", "ERROR": "critical", "UNPROFITABLE": "critical",
    "WARNING": "warning", "FLAG": "warning", "OVERTIME": "warning",
    "LOW MARGIN": "warning",
    "HEALTHY": "healthy", "OK": "healthy",
    "INFO": "info", "NO BENCHMARK": "info",
}

INTERPRETATIONS: List[str] = [
    "Severity mapping: anomaly_alerts.severity 'high' is reported as CRITICAL, "
    "'medium' as WARNING; 'low' alerts are counted as context but not listed.",
    "Margin shortfall flag (check b): a job type is flagged when its trailing-30-day "
    "gross margin is more than 5.0 PERCENTAGE POINTS below its benchmark target "
    "(\"dropped >5% below target\" is interpreted as 5 percentage points, not a 5% "
    "relative decline).",
    "Margin window (check b): ledger entries with entry_date >= CURRENT_DATE - 30, "
    "attributed via financial_ledger.job_id -> jobs.job_type_id. Overhead entries with "
    "no job attribution are excluded from the per-type view. Types with zero revenue in "
    "the window are not evaluated.",
    "Margin target (check b): the latest national 'job_market' row per job type from "
    "market_benchmarks (region='national', ORDER BY period DESC LIMIT 1).",
    "Overtime heuristic (check c): an employee is flagged when trailing-30-day logged "
    "hours exceed 184 h (1.15 x 160 h full-time month). The top 3 employees by hours "
    "are listed regardless of flag status.",
    "Whale clients (check c): rows with is_whale = true in mv_customer_profitability "
    "(a LIFETIME aggregate, not a trailing-30-day window); unprofitable means "
    "gross_margin < 0. If no whale rows exist, the 3 lowest-margin customers with "
    "revenue > 0 are listed as informational context instead.",
    "Cash-on-hand proxy (check d): lifetime SUM(revenue) - SUM(expense) from "
    "financial_ledger. The warehouse holds no bank/balance table, so accumulated "
    "ledger flow is the only available cash source; treat the runway figure as a "
    "proxy, not a bank reconciliation.",
    "Burn basis (check d): average monthly net cash flow (revenue - total_expense) "
    "over the 3 most recent months present in mv_monthly_summary. If that average is "
    ">= 0 the company is reported as \"positive cash flow -- no burn\" and no runway "
    "figure is computed. Bands: runway < 30 days CRITICAL, 30-90 days WARNING, "
    "> 90 days HEALTHY.",
    "Forecasts (check d): series='net_cash_flow' rows with period_start in (current "
    "month, today + 90 days]; when several runs exist per period the latest run_date "
    "wins. Forecast totals are context only and do not drive the runway status.",
    "Exit codes: 0 healthy (warnings allowed); 2 at least one open critical "
    "(severity='high') anomaly alert across scanned companies; 3 operational error "
    "(DB unreachable / query catastrophe). Exit code 2 is keyed to open severity='high' "
    "anomaly alerts per spec; other CRITICAL check statuses (e.g. runway < 30 days) are "
    "displayed but do not change the exit code.",
    "Read-only posture: every connection runs SET default_transaction_read_only = on "
    "and the tool issues SELECT statements only.",
]

# ---------------------------------------------------------------------------
# ANSI color helpers (enabled only when stdout is a TTY)
# ---------------------------------------------------------------------------

ANSI_RESET = "\033[0m"
ANSI_BOLD = "\033[1m"
ANSI_RED = "\033[31m"
ANSI_GREEN = "\033[32m"
ANSI_YELLOW = "\033[33m"
ANSI_MAGENTA = "\033[35m"
ANSI_CYAN = "\033[36m"


class Palette:
    """Tiny ANSI wrapper; paints nothing when disabled (non-TTY / piped output)."""

    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled

    def _c(self, code: str, text: str) -> str:
        if not self.enabled:
            return text
        return code + text + ANSI_RESET

    def bold(self, text: str) -> str:
        return self._c(ANSI_BOLD, text)

    def red(self, text: str) -> str:
        return self._c(ANSI_RED, text)

    def green(self, text: str) -> str:
        return self._c(ANSI_GREEN, text)

    def yellow(self, text: str) -> str:
        return self._c(ANSI_YELLOW, text)

    def magenta(self, text: str) -> str:
        return self._c(ANSI_MAGENTA, text)

    def cyan(self, text: str) -> str:
        return self._c(ANSI_CYAN, text)

    def status(self, status: str) -> str:
        return self._c(STATUS_ANSI.get(status, ""), status)

    def badge(self, status: str) -> str:
        """Paint a [STATUS] badge with the color of the bare status name."""
        return self._c(STATUS_ANSI.get(status, ""), "[" + status + "]")

    def token(self, text: str) -> str:
        kind = STATUS_TOKENS.get(text)
        if kind == "critical":
            return self.red(text)
        if kind == "warning":
            return self.yellow(text)
        if kind == "healthy":
            return self.green(text)
        if kind == "info":
            return self.cyan(text)
        return text


STATUS_ANSI = {
    STATUS_CRITICAL: ANSI_RED,
    STATUS_WARNING: ANSI_YELLOW,
    STATUS_HEALTHY: ANSI_GREEN,
    STATUS_INFO: ANSI_CYAN,
    STATUS_ERROR: ANSI_MAGENTA,
}

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class Table:
    """A pre-formatted result table (cells are display-ready strings)."""
    title: str
    headers: List[str]
    rows: List[List[str]] = field(default_factory=list)
    note: Optional[str] = None


@dataclass
class CheckResult:
    """Outcome of one check for one company."""
    key: str
    title: str
    status: str
    summary: str
    tables: List[Table] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CompanyReport:
    """All check outcomes for one company."""
    company_id: int
    name: str
    industry: Optional[str]
    checks: List[CheckResult] = field(default_factory=list)

    @property
    def overall_status(self) -> str:
        """Worst business status across this company's checks (ERROR ignored)."""
        statuses = [c.status for c in self.checks if c.status != STATUS_ERROR]
        if not statuses:
            return STATUS_INFO
        return max(statuses, key=lambda s: STATUS_SEVERITY_RANK[s])


@dataclass
class OverallResult:
    status: str
    summary: str


@dataclass
class ScanResult:
    """Everything the renderers need; MD and HTML render the same object."""
    generated_at: str
    warehouse: str
    company_count: int
    companies: List[CompanyReport]
    overall: OverallResult
    exit_code: int


# ---------------------------------------------------------------------------
# Database access (READ-ONLY)
# ---------------------------------------------------------------------------


def connect_ro(dsn: str) -> "psycopg2.extensions.connection":
    """Open a READ-ONLY connection.

    ``SET default_transaction_read_only = on`` runs immediately after connect;
    the setting is then verified via current_setting(). Autocommit is enabled
    so no transaction is held open between checks.
    """
    eff_dsn = dsn
    if "connect_timeout" not in eff_dsn:
        eff_dsn += ("&" if "?" in eff_dsn else "?") + "connect_timeout=10"
    conn = psycopg2.connect(eff_dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SET default_transaction_read_only = on")
        cur.execute("SELECT current_setting('default_transaction_read_only')")
        value = cur.fetchone()[0]
    if value != "on":
        conn.close()
        raise RuntimeError("read-only enforcement failed to apply on connection")
    return conn


def fetch_all(conn: Any, sql: str, params: Optional[Tuple[Any, ...]] = None) -> List[Dict[str, Any]]:
    """Run one SELECT and return rows as dicts keyed by column name."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def describe_dsn(dsn: str) -> str:
    """Human-safe warehouse description (host:port/db, no credentials)."""
    try:
        parts = urlsplit(dsn)
        host = parts.hostname or "?"
        port = parts.port or 5432
        db = (parts.path or "/").lstrip("/") or "?"
        return "%s:%s/%s" % (host, port, db)
    except ValueError:
        return "<dsn>"


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def fmt_money(value: Any, signed: bool = False) -> str:
    if value is None:
        return "-"
    f = float(value)
    sign = "-" if f < 0 else ("+" if signed and f > 0 else "")
    return "%s$%s" % (sign, format(abs(f), ",.2f"))


def fmt_pct(value: Any, decimals: int = 1) -> str:
    if value is None:
        return "-"
    return "%.*f%%" % (decimals, float(value))


def fmt_pp(value: Any) -> str:
    """Signed percentage-point gap."""
    if value is None:
        return "-"
    return "%+.1f" % float(value)


def fmt_dev(value: Any) -> str:
    if value is None:
        return "-"
    return "%+.1f%%" % float(value)


def fmt_num(value: Any, decimals: int = 1) -> str:
    if value is None:
        return "-"
    return format(float(value), ",.%df" % decimals)


def fmt_month(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m")
    return str(value)[:7]


def fmt_month_label(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, (datetime, date)):
        return value.strftime("%b %Y")
    return str(value)[:7]


def describe_details(anomaly_type: str, details: Any) -> str:
    """Render the alert details JSONB as a short human string."""
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except (ValueError, TypeError):
            return details[:120]
    if not isinstance(details, dict):
        return ""
    try:
        if anomaly_type == "labor_overrun":
            parts = []
            if "actual_labor_cost" in details or "estimated_labor_cost" in details:
                parts.append("actual %s vs est %s" % (
                    fmt_money(details.get("actual_labor_cost")),
                    fmt_money(details.get("estimated_labor_cost")),
                ))
            if details.get("threshold_pct") is not None:
                parts.append("threshold %s" % fmt_pct(details.get("threshold_pct"), 0))
            return "; ".join(parts)
        if anomaly_type == "expense_spike":
            parts = []
            if details.get("month_total") is not None:
                parts.append("%s month total %s vs prior mean %s" % (
                    details.get("category", "?"),
                    fmt_money(details.get("month_total")),
                    fmt_money(details.get("prior_6m_mean")),
                ))
            if details.get("prior_6m_stddev") is not None:
                parts.append("sd %s" % fmt_money(details.get("prior_6m_stddev")))
            return "; ".join(parts)
    except Exception:  # noqa: BLE001 -- context rendering must never fail a check
        pass
    try:
        return "; ".join("%s=%s" % (k, details[k]) for k in sorted(details))[:160]
    except Exception:  # noqa: BLE001
        return ""


# ---------------------------------------------------------------------------
# Check (a): open critical/warning anomaly alerts
# ---------------------------------------------------------------------------


def check_alerts(conn: Any, company_id: int, max_rows: int) -> CheckResult:
    counts = fetch_all(conn, """
        SELECT severity, anomaly_type, count(*) AS n
        FROM anomaly_alerts
        WHERE company_id = %s AND is_resolved = false
        GROUP BY severity, anomaly_type
    """, (company_id,))

    by_severity: Dict[str, int] = {}
    detail_bits: List[str] = []
    for r in counts:
        by_severity[r["severity"]] = by_severity.get(r["severity"], 0) + int(r["n"])
        if r["severity"] in ("high", "medium"):
            detail_bits.append("%d %s %s" % (int(r["n"]), r["severity"], r["anomaly_type"]))
    detail_bits.sort()

    high = by_severity.get("high", 0)
    medium = by_severity.get("medium", 0)
    low = by_severity.get("low", 0)

    rows = fetch_all(conn, """
        SELECT a.id, a.anomaly_type, a.severity, a.category, a.period, a.job_id,
               a.deviation_pct::float8 AS deviation_pct, a.details, j.job_number
        FROM anomaly_alerts a
        LEFT JOIN jobs j ON j.company_id = a.company_id AND j.id = a.job_id
        WHERE a.company_id = %s
          AND a.is_resolved = false
          AND a.severity IN ('high', 'medium')
        ORDER BY CASE a.severity WHEN 'high' THEN 0 ELSE 1 END,
                 a.id DESC
    """, (company_id,))

    table_rows = []
    for r in rows:
        if r["anomaly_type"] == "labor_overrun":
            subject = r["job_number"] or ("job_id %s" % r["job_id"] if r["job_id"] else "-")
            period = "-"
        else:
            subject = r["category"] or "-"
            period = fmt_month(r["period"])
        table_rows.append([
            r["severity"].upper(),
            r["anomaly_type"],
            subject,
            period,
            fmt_dev(r["deviation_pct"]),
            describe_details(r["anomaly_type"], r["details"]),
        ])

    if high:
        status = STATUS_CRITICAL
    elif medium:
        status = STATUS_WARNING
    else:
        status = STATUS_HEALTHY

    summary = "%d open critical/warning alert(s): %s." % (high + medium, ", ".join(detail_bits) or "none")
    if low:
        summary += " Plus %d low/info alert(s) open (counted, not listed)." % low

    notes = [
        "severity mapping: high -> CRITICAL, medium -> WARNING; low/info counted only.",
    ]
    table = build_table(
        "Open critical/warning anomaly alerts",
        ["SEV", "TYPE", "JOB / CATEGORY", "PERIOD", "DEV", "CONTEXT"],
        table_rows, max_rows)

    return CheckResult(
        key="alerts", title="Open Critical / Warning Anomaly Alerts", status=status,
        summary=summary, tables=[table], notes=notes,
        metrics={"high": high, "medium": medium, "low": low},
    )


# ---------------------------------------------------------------------------
# Check (b): trailing 30-day gross margin vs target by job type
# ---------------------------------------------------------------------------


def check_margin(conn: Any, company_id: int, max_rows: int) -> CheckResult:
    rows = fetch_all(conn, """
        SELECT jt.id AS job_type_id,
               jt.name AS job_type,
               COALESCE(SUM(l.amount) FILTER (WHERE l.entry_type = 'revenue'), 0)::float8 AS revenue,
               COALESCE(SUM(l.amount) FILTER (WHERE l.entry_type = 'expense'), 0)::float8 AS expense
        FROM job_types jt
        LEFT JOIN jobs j
               ON j.company_id = jt.company_id
              AND j.job_type_id = jt.id
        LEFT JOIN financial_ledger l
               ON l.company_id = j.company_id
              AND l.job_id = j.id
              AND l.entry_date >= (CURRENT_DATE - INTERVAL '30 days')::date
        WHERE jt.company_id = %s
        GROUP BY jt.id, jt.name
        HAVING COALESCE(SUM(l.amount) FILTER (WHERE l.entry_type = 'revenue'), 0) > 0
        ORDER BY revenue DESC
    """, (company_id,))

    targets: Dict[int, Optional[float]] = {}
    target_period: Optional[str] = None
    type_ids = [r["job_type_id"] for r in rows]
    if type_ids:
        trows = fetch_all(conn, """
            SELECT DISTINCT ON (job_type_id)
                   job_type_id, period, avg_margin_pct::float8 AS target_pct
            FROM market_benchmarks
            WHERE region = 'national'
              AND benchmark_kind = 'job_market'
              AND job_type_id = ANY(%s)
            ORDER BY job_type_id, period DESC
        """, (type_ids,))
        targets = {t["job_type_id"]: t["target_pct"] for t in trows}
        if trows:
            target_period = fmt_month(max(t["period"] for t in trows))

    table_rows = []
    flagged = 0
    missing_target = 0
    for r in rows:
        revenue = float(r["revenue"] or 0.0)
        expense = float(r["expense"] or 0.0)
        margin = (revenue - expense) / revenue * 100.0 if revenue else 0.0
        target = targets.get(r["job_type_id"])
        if target is None:
            status_cell = "NO BENCHMARK"
            gap = None
            missing_target += 1
        else:
            gap = margin - float(target)
            if margin < float(target) - MARGIN_FLAG_PP:
                status_cell = "FLAG"
                flagged += 1
            else:
                status_cell = "OK"
        table_rows.append([
            r["job_type"], fmt_money(revenue), fmt_money(expense), fmt_pct(margin),
            fmt_pct(target), fmt_pp(gap), status_cell,
        ])

    status = STATUS_WARNING if flagged else STATUS_HEALTHY
    summary = "%d job type(s) with revenue in the trailing 30 days; %d flagged more than %.0f pp below target." % (
        len(rows), flagged, MARGIN_FLAG_PP)
    if missing_target:
        summary += " %d type(s) had no national job_market benchmark." % missing_target

    notes = [
        "target = latest national job_market benchmark%s." % (
            " (period %s)" % target_period if target_period else ""),
        "flag rule: actual margin < target - %.1f percentage points." % MARGIN_FLAG_PP,
        "overhead ledger entries without job attribution are excluded from per-type margins.",
    ]
    table = build_table(
        "Trailing 30-day gross margin vs target by job type",
        ["JOB TYPE", "REV (30D)", "EXP (30D)", "MARGIN", "TARGET", "GAP PP", "STATUS"],
        table_rows, max_rows)

    return CheckResult(
        key="margin", title="Trailing 30-Day Gross Margin vs Target (by Job Type)",
        status=status, summary=summary, tables=[table], notes=notes,
        metrics={"flagged": flagged, "types_evaluated": len(rows)},
    )


# ---------------------------------------------------------------------------
# Check (c): overtime-heavy employees & whale clients
# ---------------------------------------------------------------------------


def check_labor_whales(conn: Any, company_id: int, max_rows: int) -> CheckResult:
    labor = fetch_all(conn, """
        SELECT e.id AS employee_id,
               (e.first_name || ' ' || e.last_name) AS employee,
               e.role,
               SUM(l.hours)::float8 AS hours,
               COUNT(DISTINCT l.work_date) AS days
        FROM labor_logs l
        JOIN employees e ON e.company_id = l.company_id AND e.id = l.employee_id
        WHERE l.company_id = %s
          AND l.work_date >= (CURRENT_DATE - INTERVAL '30 days')::date
        GROUP BY e.id, e.first_name, e.last_name, e.role
        ORDER BY hours DESC
        LIMIT 3
    """, (company_id,))

    labor_rows = []
    overtime_flags = 0
    for r in labor:
        hours = float(r["hours"] or 0.0)
        is_ot = hours > OVERTIME_THRESHOLD_HOURS
        if is_ot:
            overtime_flags += 1
        labor_rows.append([
            r["employee"], r["role"] or "-", fmt_num(hours),
            str(r["days"]) if r["days"] is not None else "-",
            "OVERTIME" if is_ot else "OK",
        ])

    whales = fetch_all(conn, """
        SELECT customer_id, customer_name, revenue::float8 AS revenue,
               total_cost::float8 AS total_cost, gross_margin::float8 AS gross_margin,
               margin_pct::float8 AS margin_pct, job_count, days_since_last_job
        FROM mv_customer_profitability
        WHERE company_id = %s AND is_whale = true
        ORDER BY gross_margin ASC
    """, (company_id,))

    whale_tables: List[Table] = []
    unprofitable = 0
    whale_summary = ""
    extra_notes: List[str] = []

    if whales:
        whale_rows = []
        for r in whales:
            bad = (r["gross_margin"] or 0.0) < 0
            if bad:
                unprofitable += 1
            whale_rows.append([
                r["customer_name"], fmt_money(r["revenue"]), fmt_money(r["total_cost"]),
                fmt_money(r["gross_margin"]), fmt_pct(r["margin_pct"]),
                str(r["job_count"]) if r["job_count"] is not None else "-",
                str(r["days_since_last_job"]) if r["days_since_last_job"] is not None else "-",
                "UNPROFITABLE" if bad else "OK",
            ])
        whale_summary = "%d whale client(s), %d unprofitable (gross_margin < 0)." % (
            len(whales), unprofitable)
        whale_tables.append(build_table(
            "Whale clients (mv_customer_profitability, lifetime aggregates)",
            ["CUSTOMER", "REVENUE", "TOTAL COST", "GROSS MARGIN", "MARGIN", "JOBS", "DAYS IDLE", "STATUS"],
            whale_rows, max_rows))
    else:
        fallback = fetch_all(conn, """
            SELECT customer_id, customer_name, revenue::float8 AS revenue,
                   total_cost::float8 AS total_cost, gross_margin::float8 AS gross_margin,
                   margin_pct::float8 AS margin_pct
            FROM mv_customer_profitability
            WHERE company_id = %s AND revenue > 0
            ORDER BY margin_pct ASC
            LIMIT 3
        """, (company_id,))
        fallback_rows = []
        for r in fallback:
            fallback_rows.append([
                r["customer_name"], fmt_money(r["revenue"]), fmt_money(r["total_cost"]),
                fmt_money(r["gross_margin"]), fmt_pct(r["margin_pct"]),
                "LOW MARGIN",
            ])
        whale_summary = "No whale (is_whale) rows; showing %d lowest-margin customer(s) with revenue > 0 (informational)." % len(fallback_rows)
        extra_notes.append(
            "mv_customer_profitability returned no is_whale rows for this company; "
            "the 3 lowest-margin customers with revenue > 0 are informational context.")
        whale_tables.append(build_table(
            "Lowest-margin customers (informational, no whale rows)",
            ["CUSTOMER", "REVENUE", "TOTAL COST", "GROSS MARGIN", "MARGIN", "STATUS"],
            fallback_rows, max_rows))

    status = STATUS_WARNING if (overtime_flags or unprofitable) else STATUS_HEALTHY
    summary = "Top-3 trailing-30d hours: %d above %.0f h threshold. %s" % (
        overtime_flags, OVERTIME_THRESHOLD_HOURS, whale_summary)

    labor_table = build_table(
        "Top 3 employees by trailing 30-day hours",
        ["EMPLOYEE", "ROLE", "HOURS (30D)", "DAYS", "STATUS"],
        labor_rows, max_rows)
    notes = [
        "overtime threshold: > %.0f h in trailing 30 days (= %.2f x %d h)." % (
            OVERTIME_THRESHOLD_HOURS, OVERTIME_FACTOR, int(FULL_TIME_HOURS)),
        "whale/unprofitable definitions are lifetime aggregates from the matview.",
    ] + extra_notes

    return CheckResult(
        key="labor_whales", title="Overtime-Heavy Employees & Whale Clients",
        status=status, summary=summary, tables=[labor_table] + whale_tables,
        notes=notes,
        metrics={"overtime_flags": overtime_flags, "unprofitable_whales": unprofitable},
    )


# ---------------------------------------------------------------------------
# Check (d): 90-day cash runway
# ---------------------------------------------------------------------------


def check_runway(conn: Any, company_id: int, max_rows: int) -> CheckResult:
    ledger = fetch_all(conn, """
        SELECT COALESCE(SUM(amount) FILTER (WHERE entry_type = 'revenue'), 0)::float8 AS total_revenue,
               COALESCE(SUM(amount) FILTER (WHERE entry_type = 'expense'), 0)::float8 AS total_expense
        FROM financial_ledger
        WHERE company_id = %s
    """, (company_id,))
    revenue = float(ledger[0]["total_revenue"] or 0.0) if ledger else 0.0
    expense = float(ledger[0]["total_expense"] or 0.0) if ledger else 0.0
    cash_proxy = revenue - expense

    months = fetch_all(conn, """
        SELECT month_start, revenue::float8 AS revenue, total_expense::float8 AS total_expense
        FROM mv_monthly_summary
        WHERE company_id = %s
        ORDER BY month_start DESC
        LIMIT %s
    """, (company_id, TRAILING_MONTHS))
    nets = [(m["month_start"], float(m["revenue"] or 0.0) - float(m["total_expense"] or 0.0))
            for m in months]
    avg_net = sum(n for _, n in nets) / len(nets) if nets else None

    forecasts = fetch_all(conn, """
        SELECT DISTINCT ON (period_start)
               period_start, model, predicted::float8 AS predicted,
               ci_low::float8 AS ci_low, ci_high::float8 AS ci_high,
               horizon_months, run_date
        FROM forecasts
        WHERE company_id = %s
          AND series = 'net_cash_flow'
          AND period_start > date_trunc('month', CURRENT_DATE)::date
          AND period_start <= (CURRENT_DATE + INTERVAL '90 days')::date
        ORDER BY period_start, run_date DESC
    """, (company_id,))
    forecast_sum = sum(float(f["predicted"] or 0.0) for f in forecasts)
    forecast_label = ""
    if forecasts:
        forecast_label = "%s-%s, %d row(s)" % (
            fmt_month_label(forecasts[0]["period_start"]),
            fmt_month_label(forecasts[-1]["period_start"]),
            len(forecasts))

    burn: Optional[float] = None
    runway_days: Optional[float] = None
    if avg_net is None:
        status = STATUS_INFO
        runway_cell = "n/a"
        burn_cell = "n/a"
    elif avg_net >= 0:
        status = STATUS_HEALTHY
        runway_cell = "n/a (positive cash flow)"
        burn_cell = "$0.00 (net positive)"
    else:
        burn = -avg_net
        runway_days = (cash_proxy / (burn / 30.0)) if cash_proxy > 0 else 0.0
        if runway_days < RUNWAY_CRITICAL_DAYS:
            status = STATUS_CRITICAL
        elif runway_days <= RUNWAY_WARNING_DAYS:
            status = STATUS_WARNING
        else:
            status = STATUS_HEALTHY
        runway_cell = "%.0f days" % runway_days
        burn_cell = fmt_money(burn)

    rows = [
        ["Cash on hand (ledger proxy)", fmt_money(cash_proxy)],
        ["Trailing %d-month net cash (avg/mo)" % TRAILING_MONTHS,
         fmt_money(avg_net, signed=True) if avg_net is not None else "n/a"],
        ["Implied monthly burn", burn_cell],
        ["Runway", runway_cell],
        ["Projected 90-day net cash (forecast)",
         ("%s (%s)" % (fmt_money(forecast_sum, signed=True), forecast_label)) if forecasts
         else "no forecast rows in window"],
    ]
    table = build_table(
        "90-day cash runway", ["METRIC", "VALUE"], rows, max_rows)

    if avg_net is None:
        summary = "Cash proxy %s; no monthly history in mv_monthly_summary -- burn cannot be computed." % fmt_money(cash_proxy)
    elif avg_net >= 0:
        summary = ("Cash proxy %s; trailing %d-month net %s/mo -- positive cash flow, no burn. "
                   "Projected 90-day net cash %s." % (
                       fmt_money(cash_proxy), TRAILING_MONTHS,
                       fmt_money(avg_net, signed=True), fmt_money(forecast_sum, signed=True)))
    else:
        summary = ("Cash proxy %s; trailing %d-month net %s/mo (burn %s/mo); runway %.0f days [%s]. "
                   "Projected 90-day net cash %s." % (
                       fmt_money(cash_proxy), TRAILING_MONTHS, fmt_money(avg_net, signed=True),
                       fmt_money(burn), runway_days, status, fmt_money(forecast_sum, signed=True)))

    basis_label = ", ".join(fmt_month_label(m) for m, _ in reversed(nets)) or "n/a"
    notes = [
        "cash-on-hand proxy: lifetime revenue - expense from financial_ledger (no bank table in warehouse).",
        "burn basis: %d most recent months in mv_monthly_summary (%s)." % (TRAILING_MONTHS, basis_label),
        "bands: < %d days CRITICAL, %d-%d days WARNING, > %d days HEALTHY." % (
            int(RUNWAY_CRITICAL_DAYS), int(RUNWAY_CRITICAL_DAYS),
            int(RUNWAY_WARNING_DAYS), int(RUNWAY_WARNING_DAYS)),
    ]
    if cash_proxy <= 0 and burn:
        notes.append("WARNING: cash proxy is <= 0 while burn is positive -- runway reported as 0 days.")

    return CheckResult(
        key="runway", title="90-Day Cash Runway", status=status, summary=summary,
        tables=[table], notes=notes,
        metrics={
            "cash_proxy": cash_proxy,
            "avg_monthly_net": avg_net,
            "runway_days": runway_days,
            "forecast_90d": forecast_sum,
        },
    )


# ---------------------------------------------------------------------------
# Scan orchestration
# ---------------------------------------------------------------------------

CHECK_BUILDERS: List[Tuple[str, str, Any]] = [
    ("alerts", "Open Critical / Warning Anomaly Alerts", check_alerts),
    ("margin", "Trailing 30-Day Gross Margin vs Target (by Job Type)", check_margin),
    ("labor_whales", "Overtime-Heavy Employees & Whale Clients", check_labor_whales),
    ("runway", "90-Day Cash Runway", check_runway),
]


def scan_company(conn: Any, company: Dict[str, Any], max_rows: int) -> CompanyReport:
    """Run all four checks for one company; a failing check becomes an ERROR row."""
    company_id = int(company["company_id"])
    checks: List[CheckResult] = []
    for key, title, fn in CHECK_BUILDERS:
        try:
            result = fn(conn, company_id, max_rows)
        except Exception as exc:  # noqa: BLE001 -- one failed check must not abort the run
            result = CheckResult(
                key=key, title=title, status=STATUS_ERROR,
                summary="Query failed: %s" % exc, tables=[], notes=[], metrics={})
        checks.append(result)
    return CompanyReport(
        company_id=company_id,
        name=str(company.get("company_name") or ("company %d" % company_id)),
        industry=company.get("industry_vertical"),
        checks=checks,
    )


def aggregate_overall(reports: List[CompanyReport]) -> OverallResult:
    """Combine per-company statuses into the digest-level status + summary line."""
    if not reports:
        return OverallResult(STATUS_INFO, "No companies found in the registry -- nothing to scan.")

    worst = max((c.overall_status for c in reports),
                key=lambda s: STATUS_SEVERITY_RANK[s])
    total_high = 0
    warn_checks = 0
    error_checks = 0
    for c in reports:
        for chk in c.checks:
            if chk.status == STATUS_WARNING:
                warn_checks += 1
            elif chk.status == STATUS_ERROR:
                error_checks += 1
        alerts = next((k for k in c.checks if k.key == "alerts"), None)
        if alerts is not None and alerts.status != STATUS_ERROR:
            total_high += int(alerts.metrics.get("high", 0))

    critical_companies = sum(1 for c in reports if c.overall_status == STATUS_CRITICAL)
    if worst == STATUS_CRITICAL:
        summary = "%d open critical (severity='high') alert(s) across %d company(ies); %d check(s) at warning level." % (
            total_high, critical_companies, warn_checks)
    elif worst == STATUS_WARNING:
        summary = "No critical alerts; %d check(s) at warning level across %d company(ies)." % (
            warn_checks, len(reports))
    else:
        summary = "All checks healthy across %d company(ies)." % len(reports)
    if error_checks:
        summary += " %d check(s) returned ERROR (incomplete scan)." % error_checks
    return OverallResult(worst, summary)


def compute_exit_code(reports: List[CompanyReport]) -> int:
    """Exit code 2 iff open severity='high' anomaly alerts exist (per spec)."""
    alert_checks = [next((k for k in c.checks if k.key == "alerts"), None) for c in reports]
    alert_checks = [a for a in alert_checks if a is not None]
    if alert_checks and all(a.status == STATUS_ERROR for a in alert_checks):
        return EXIT_ERROR  # primary exit signal unavailable -> query catastrophe
    total_high = sum(int(a.metrics.get("high", 0)) for a in alert_checks
                     if a.status != STATUS_ERROR)
    return EXIT_CRITICAL if total_high > 0 else EXIT_OK


# ---------------------------------------------------------------------------
# Table truncation + ASCII rendering (terminal)
# ---------------------------------------------------------------------------


def build_table(title: str, headers: List[str], rows: List[List[str]],
                max_rows: int, note: Optional[str] = None) -> Table:
    """Truncate rows to max_rows (across all tables) and attach a note."""
    shown = list(rows)
    merged_note = note
    if max_rows and len(rows) > max_rows:
        shown = rows[:max_rows]
        extra = "showing %d of %d rows" % (len(shown), len(rows))
        merged_note = ("%s; %s" % (note, extra)) if note else extra
    return Table(title=title, headers=list(headers), rows=shown, note=merged_note)


def _compute_widths(headers: List[str], rows: List[List[str]], max_total: int) -> List[int]:
    ncols = len(headers)
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    # Each column never shrinks below its header length (floored at 8).
    floors = [max(8, len(h)) for h in headers]
    overhead = ncols * 3 + 1
    budget = max(max_total - overhead, sum(floors))
    while sum(widths) > budget:
        widest = max(widths)
        idx = widths.index(widest)
        if widest <= floors[idx]:
            candidates = [i for i, w in enumerate(widths) if w > floors[i]]
            if not candidates:
                break
            idx = max(candidates, key=lambda i: widths[i])
        widths[idx] -= 1
    return widths


def ascii_table(headers: List[str], rows: List[List[str]], palette: Palette,
                max_total: int = 110) -> str:
    """Render a fixed-width +-style ASCII table (status tokens colorized)."""
    widths = _compute_widths(headers, rows, max_total)

    def fit(cell: str, width: int) -> str:
        if len(cell) <= width:
            return cell.ljust(width)
        if width >= 4:
            return cell[: width - 3] + "..."
        return cell[:width]

    def cell_out(cell: str, width: int) -> str:
        # Exact status tokens are colorized as-is with padding kept outside
        # the ANSI span, so column alignment stays intact.
        if cell in STATUS_TOKENS and len(cell) <= width:
            return palette.token(cell) + " " * (width - len(cell))
        return fit(cell, width)

    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"

    def line(cells: List[str]) -> str:
        body = "|".join(" " + cell_out(c, widths[i]) + " "
                        for i, c in enumerate(cells))
        return "|" + body + "|"

    out = [sep, line(headers), sep]
    for row in rows:
        out.append(line(row))
    out.append(sep)
    return "\n".join(out)


def render_terminal(scan: ScanResult, palette: Palette) -> str:
    """Color-coded terminal summary. Clean ASCII (no box-drawing unicode)."""
    W = 78
    out: List[str] = []
    out.append(palette.bold((" SMB-WATCHDOG v%s - EXECUTIVE MORNING DIGEST " % __version__).center(W, "=")))
    out.append(" Generated : %s" % scan.generated_at)
    out.append(" Warehouse : %s  [READ-ONLY]" % scan.warehouse)
    out.append(" Companies : %d scanned" % scan.company_count)
    out.append("-" * W)
    out.append(" OVERALL STATUS: %s %s" % (palette.badge(scan.overall.status), scan.overall.summary))

    for company in scan.companies:
        out.append("")
        label = " COMPANY: %s (id=%d%s)" % (
            company.name, company.company_id,
            (", " + company.industry) if company.industry else "")
        out.append(palette.bold(label))
        out.append("-" * W)
        for chk in company.checks:
            left = " %s %s" % (CHECK_LETTERS.get(chk.key, "-"), TERMINAL_TITLES.get(chk.key, chk.title.upper()))
            dots = "." * max(1, W - len(left) - len(chk.status) - 4)
            out.append("%s %s %s" % (left, dots, palette.badge(chk.status)))
            if chk.summary:
                out.append(textwrap.fill(chk.summary, width=W - 4,
                                         initial_indent="    ", subsequent_indent="    "))
            for tbl in chk.tables:
                for line in ascii_table(tbl.headers, tbl.rows, palette).splitlines():
                    out.append("  " + line)
                if tbl.note:
                    out.append("    (%s)" % tbl.note)
            for note in chk.notes:
                out.append(textwrap.fill(note, width=W - 6,
                                         initial_indent="    - ", subsequent_indent="      "))

    out.append("=" * W)
    exit_text = " EXIT CODE %d - %s" % (scan.exit_code, EXIT_MEANINGS.get(scan.exit_code, ""))
    if scan.exit_code == EXIT_OK:
        out.append(palette.green(exit_text))
    elif scan.exit_code == EXIT_CRITICAL:
        out.append(palette.red(palette.bold(exit_text)))
    else:
        out.append(palette.magenta(exit_text))
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------


def _md_cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def render_markdown(scan: ScanResult) -> str:
    md: List[str] = []
    md.append("# SMB-Watchdog -- Executive Morning Digest")
    md.append("")
    md.append("- **Generated:** %s" % scan.generated_at)
    md.append("- **Warehouse:** %s (read-only connection)" % scan.warehouse)
    md.append("- **Companies scanned:** %d" % scan.company_count)
    md.append("- **Tool:** SMB-Watchdog v%s" % __version__)
    md.append("")
    md.append("**Overall status: %s** -- %s" % (scan.overall.status, scan.overall.summary))
    md.append("")

    for company in scan.companies:
        header = "## %s (company_id=%d" % (company.name, company.company_id)
        if company.industry:
            header += " -- %s" % company.industry
        header += ")"
        md.append(header)
        md.append("")
        for chk in company.checks:
            md.append("### %s %s -- **%s**" % (
                CHECK_LETTERS.get(chk.key, ""), chk.title, chk.status))
            md.append("")
            if chk.summary:
                md.append(chk.summary)
                md.append("")
            for tbl in chk.tables:
                md.append("| " + " | ".join(_md_cell(h) for h in tbl.headers) + " |")
                md.append("|" + "|".join([" --- "] * len(tbl.headers)) + "|")
                for row in tbl.rows:
                    md.append("| " + " | ".join(_md_cell(c) for c in row) + " |")
                md.append("")
                if tbl.note:
                    md.append("*%s*" % tbl.note)
                    md.append("")
            for note in chk.notes:
                md.append("- %s" % note)
            if chk.notes:
                md.append("")

    md.append("## Interpretations & Method Notes")
    md.append("")
    for item in INTERPRETATIONS:
        md.append("- %s" % item)
    md.append("")
    md.append("**Exit code: %d** -- %s." % (scan.exit_code, EXIT_MEANINGS.get(scan.exit_code, "")))
    md.append("")
    return "\n".join(md)


# ---------------------------------------------------------------------------
# HTML rendering (fully self-contained: inline CSS, no JS, no external assets)
# ---------------------------------------------------------------------------

_HTML_CSS = """
:root { color-scheme: light; }
* { box-sizing: border-box; }
body { font-family: -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif;
       max-width: 1080px; margin: 2rem auto; padding: 0 1.5rem;
       color: #1c2333; background: #f6f7fa; line-height: 1.5; }
h1 { font-size: 1.55rem; margin: 0 0 .35rem; }
h2 { font-size: 1.2rem; margin: 2rem 0 .5rem; border-bottom: 2px solid #d8dce6; padding-bottom: .25rem; }
h3 { font-size: 1rem; margin: 1.4rem 0 .4rem; }
.meta { color: #5a6272; font-size: .9rem; margin-bottom: .8rem; }
.overall { background: #fff; border: 1px solid #d8dce6; border-left: 6px solid #8a94a6;
           border-radius: 6px; padding: .7rem 1rem; }
.badge { display: inline-block; padding: 1px 10px; border-radius: 11px;
         font-weight: 700; font-size: .78rem; letter-spacing: .03em; }
.badge-CRITICAL { background: #c0392b; color: #fff; }
.badge-WARNING  { background: #d99a06; color: #fff; }
.badge-HEALTHY  { background: #2e8b57; color: #fff; }
.badge-INFO     { background: #607d8b; color: #fff; }
.badge-ERROR    { background: #6c3483; color: #fff; }
table { border-collapse: collapse; width: 100%; font-size: .85rem;
        background: #fff; margin: .4rem 0 .2rem; }
th, td { border: 1px solid #d8dce6; padding: 5px 9px; text-align: left; }
th { background: #eef1f6; font-weight: 600; }
tr:nth-child(even) td { background: #fafbfc; }
.note { color: #5a6272; font-size: .82rem; margin: .15rem 0 .8rem; }
ul.notes { color: #5a6272; font-size: .84rem; margin: .2rem 0 .8rem 1.1rem; padding: 0; }
.footnote li { margin-bottom: .35rem; }
.tok-critical { color: #c0392b; font-weight: 700; }
.tok-warning  { color: #b07d00; font-weight: 700; }
.tok-healthy  { color: #24774a; font-weight: 700; }
.tok-info     { color: #46627a; font-weight: 600; }
code { background: #eceff4; padding: 0 4px; border-radius: 3px; }
"""


def _html_cell(text: str) -> str:
    kind = STATUS_TOKENS.get(text)
    esc = html.escape(text)
    if kind:
        return '<span class="tok-%s">%s</span>' % (kind, esc)
    return esc


def render_html(scan: ScanResult) -> str:
    out: List[str] = []
    out.append("<!DOCTYPE html>")
    out.append('<html lang="en">')
    out.append("<head>")
    out.append('<meta charset="utf-8">')
    out.append('<meta name="viewport" content="width=device-width, initial-scale=1">')
    out.append("<title>SMB-Watchdog -- Executive Morning Digest</title>")
    out.append("<style>%s</style>" % _HTML_CSS)
    out.append("</head>")
    out.append("<body>")
    out.append("<h1>SMB-Watchdog &mdash; Executive Morning Digest</h1>")
    out.append('<div class="meta">Generated %s &middot; Warehouse %s (read-only connection) '
               '&middot; Companies scanned: %d &middot; SMB-Watchdog v%s</div>' % (
                   html.escape(scan.generated_at), html.escape(scan.warehouse),
                   scan.company_count, __version__))
    out.append('<div class="overall"><span class="badge badge-%s">%s</span> %s</div>' % (
        scan.overall.status, scan.overall.status, html.escape(scan.overall.summary)))

    for company in scan.companies:
        title = "%s <small>(company_id=%d%s)</small>" % (
            html.escape(company.name), company.company_id,
            (" &mdash; " + html.escape(company.industry)) if company.industry else "")
        out.append("<h2>%s</h2>" % title)
        for chk in company.checks:
            out.append("<h3>%s %s <span class=\"badge badge-%s\">%s</span></h3>" % (
                CHECK_LETTERS.get(chk.key, ""), html.escape(chk.title), chk.status, chk.status))
            if chk.summary:
                out.append("<p>%s</p>" % html.escape(chk.summary))
            for tbl in chk.tables:
                out.append("<table>")
                out.append("<thead><tr>" + "".join(
                    "<th>%s</th>" % html.escape(h) for h in tbl.headers) + "</tr></thead>")
                out.append("<tbody>")
                for row in tbl.rows:
                    out.append("<tr>" + "".join(
                        "<td>%s</td>" % _html_cell(c) for c in row) + "</tr>")
                out.append("</tbody></table>")
                if tbl.note:
                    out.append('<p class="note">%s</p>' % html.escape(tbl.note))
            if chk.notes:
                out.append('<ul class="notes">')
                for note in chk.notes:
                    out.append("<li>%s</li>" % html.escape(note))
                out.append("</ul>")

    out.append("<h2>Interpretations &amp; Method Notes</h2>")
    out.append('<ul class="notes footnote">')
    for item in INTERPRETATIONS:
        out.append("<li>%s</li>" % html.escape(item))
    out.append("</ul>")
    out.append("<p><strong>Exit code: %d</strong> &mdash; %s.</p>" % (
        scan.exit_code, html.escape(EXIT_MEANINGS.get(scan.exit_code, ""))))
    out.append("</body>")
    out.append("</html>")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def default_output_dir() -> str:
    """Reports default to <script dir>/reports so cron finds stable paths."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="watchdog.py",
        description="SMB-Watchdog: headless executive watchdog & morning digest CLI "
                    "(read-only PostgreSQL warehouse scanner).",
        epilog="Exit codes: 0 = healthy (warnings allowed); "
               "2 = at least one open critical (severity='high') anomaly alert; "
               "3 = operational error (DB unreachable / query catastrophe).",
    )
    parser.add_argument("--dsn", default=os.environ.get(DSN_ENV_VAR, DEFAULT_DSN),
                        help="PostgreSQL DSN to scan (default: built-in DSN; "
                             "env %s overrides it)." % DSN_ENV_VAR)
    parser.add_argument("--output-dir", default=None,
                        help="Directory for digest_report.md / digest_report.html "
                             "(default: <script dir>/reports).")
    parser.add_argument("--max-alerts-per-table", type=int, default=10,
                        help="Row cap per digest table; extra rows are summarized "
                             "in a note (default: 10).")
    parser.add_argument("--no-html", action="store_true", help="Skip the HTML report.")
    parser.add_argument("--no-md", action="store_true", help="Skip the Markdown report.")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress terminal output (reports still written; cron friendly).")
    parser.add_argument("--version", action="version", version="%%(prog)s %s" % __version__)
    args = parser.parse_args(argv)
    if args.max_alerts_per_table < 1:
        parser.error("--max-alerts-per-table must be >= 1")
    return args


def write_reports(scan: ScanResult, args: argparse.Namespace) -> List[str]:
    """Write digest reports with stable filenames; returns paths written."""
    written: List[str] = []
    out_dir = args.output_dir or default_output_dir()
    if not args.no_md:
        path = os.path.join(out_dir, "digest_report.md")
        os.makedirs(out_dir, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(render_markdown(scan))
        written.append(path)
    if not args.no_html:
        path = os.path.join(out_dir, "digest_report.html")
        os.makedirs(out_dir, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(render_html(scan))
        written.append(path)
    return written


def eprint(message: str) -> None:
    print(message, file=sys.stderr)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    palette = Palette(enabled=(not args.quiet) and sys.stdout.isatty())

    try:
        conn = connect_ro(args.dsn)
    except Exception as exc:  # noqa: BLE001 -- any connect failure is operational
        eprint("FATAL: cannot connect to warehouse (%s): %s" % (describe_dsn(args.dsn), exc))
        return EXIT_ERROR

    try:
        companies = fetch_all(conn, """
            SELECT company_id, company_name, industry_vertical
            FROM companies
            ORDER BY company_id
        """)
    except Exception as exc:  # noqa: BLE001
        eprint("FATAL: cannot read companies registry: %s" % exc)
        conn.close()
        return EXIT_ERROR

    try:
        reports = [scan_company(conn, c, args.max_alerts_per_table) for c in companies]
    except Exception as exc:  # noqa: BLE001
        eprint("FATAL: scan failed: %s" % exc)
        conn.close()
        return EXIT_ERROR
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass

    overall = aggregate_overall(reports)
    exit_code = compute_exit_code(reports)
    scan = ScanResult(
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        warehouse=describe_dsn(args.dsn),
        company_count=len(reports),
        companies=reports,
        overall=overall,
        exit_code=exit_code,
    )

    try:
        written = write_reports(scan, args)
    except Exception as exc:  # noqa: BLE001 -- report delivery failure is operational
        eprint("FATAL: writing digest reports failed: %s" % exc)
        return EXIT_ERROR

    if not args.quiet:
        print(render_terminal(scan, palette))
        if written:
            print("")
            print(" Reports written: %s" % ", ".join(written))
        else:
            print("")
            print(" Report files disabled (--no-md/--no-html).")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
