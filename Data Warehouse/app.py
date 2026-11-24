#!/usr/bin/env python3
"""
app.py -- local web dashboard for the SMB Financial & Operational Data
Warehouse (multi-client Fractional CFO edition).

Streamlit app (run:  streamlit run app.py --server.port 8602 --server.address localhost)
with ten tabs. A sidebar "Client workspace" selector (top, above "Warehouse
actions") picks the ACTIVE client workspace; every company-scoped tab
(Overview, Charts, Anomaly alerts, Ingest CSVs, Pricing simulator's local
mode, Forecast & cash runway, Ask Your Data) filters its queries by that
workspace's company_id and the forecast/ingest engines run FOR it. The
Overview tab additionally offers a board-ready "Download Executive PDF
Brief" (generate_pdf_brief.py).

  * Overview        KPIs, monthly P&L table, Executive PDF Brief download
                    (all scoped to the active workspace)
  * Charts          interactive Plotly charts (P&L, service margins, labor
                    variances, customer margins, benchmark comparison --
                    client data scoped; market baselines global)
  * Anomaly alerts  open alerts, Resolve with note, Refresh & Detect button
                    (alerts of the active workspace)
  * Ingest CSVs     upload messy accounting/payroll CSV exports, pick a
                    mapping preset or custom JSON config, load into the
                    ACTIVE workspace (delegates to ingest_csv.run_ingest
                    with its company_id)
  * Pricing simulator   what-if gross-margin math for one job quote
                    (prefilled from the ACTIVE workspace's service-line
                    data OR from the harvested SoCal Finance/Tech rate
                    guidance; delegates to pricing_simulator.simulate_job)
  * SoCal Benchmarks    regional Finance/Tech benchmark data for the six
                    SoCal metros loaded by harvest_socal_fintech.py: OEWS
                    wages, FRED macro series, Census CBP/ZBP business
                    patterns, SBA lending, and the modeled vertical
                    economics behind mv_socal_rate_guidance
                    (GLOBAL market data -- shared across workspaces)
  * Forecast & cash runway   seasonal_trend_v1 monthly forecasts with 90% CI
                    bands, cumulative cash runway, backtest accuracy table
                    (engine runs FOR the active workspace; backtest_results
                    stays a global model audit trail)
  * SoCal Capital & Deals   REAL harvested capital events for the six SoCal
                    counties loaded by harvest_socal_capital.py into
                    socal_capital_events: SEC Form D private placements +
                    SBA SBIR/STTR awards, filterable by county / sector /
                    funding source (GLOBAL market data)
  * Public Peer Financials  REAL SEC EDGAR XBRL financials for 50 public
                    peers (incl. 20 SoCal companies) loaded by
                    harvest_sec_peer_ratios.py into public_peer_financials,
                    benchmarked against mv_vertical_financial_quartiles
                    (GLOBAL market data)
  * Ask Your Data   plain-English business questions answered by an LLM that
                    emits ONE strictly read-only SQL query -- generate, then
                    inspect/edit the SQL, then run under three guardrail
                    layers (delegates to nl_to_sql.generate_sql /
                    validate_sql / run_readonly_query; never touches qdf).
                    The question is suffixed with a workspace-context line
                    so client-data answers filter by the active company_id.

All writes go through the same functions the CLI uses (ingest_csv.run_ingest,
detect_anomalies(), refresh_all_analytics(), forecast_engine.run_forecasts /
run_backtest), so the dashboard and the command line stay consistent.
"""

import datetime as dt
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import psycopg2.extras
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

import forecast_engine  # noqa: E402  (project-local module)
import generate_pdf_brief  # noqa: E402  (project-local module)
import ingest_csv  # noqa: E402  (project-local module)
import nl_to_sql  # noqa: E402  (project-local module)
import pricing_simulator  # noqa: E402  (project-local module)

# Honors DATABASE_URL (used by the Docker stack: postgresql://postgres:postgres@db:5432/...)
DB = os.environ.get("DATABASE_URL",
                    "postgresql://postgres:postgres@localhost:5432/smb_warehouse")
PRESET_CONFIG = BASE_DIR / "ingest_config.example.json"
UPLOADS_DIR = BASE_DIR / "uploads"

st.set_page_config(page_title="SMB Warehouse", page_icon="warehouse",
                   layout="wide")


# ---------------------------------------------------------------------------
# data access
# ---------------------------------------------------------------------------
@st.cache_data(ttl=120)
def qdf(sql: str) -> pd.DataFrame:
    conn = psycopg2.connect(DB)
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


@st.cache_data(ttl=120)
def qdf_params(sql: str, params: tuple = ()) -> pd.DataFrame:
    """qdf() with bound parameters: the SQL keeps psycopg2 %s placeholders
    and the values travel as query parameters (never string-formatted into
    the SQL text). Used by the data-module tabs whose filters come from
    selectboxes; the tab code still only feeds values that originate from
    the data itself."""
    conn = psycopg2.connect(DB)
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()


def exec_sql(sql: str, params=None) -> None:
    conn = psycopg2.connect(DB)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def db_alive() -> bool:
    try:
        exec_sql("SELECT 1")
        return True
    except Exception:                               # noqa: BLE001
        return False


# ---------------------------------------------------------------------------
# client workspaces (multi-tenant: the active workspace scopes every
# company-data query below; market/SoCal/peer data stays global)
# ---------------------------------------------------------------------------
def set_active_company(cid: int) -> None:
    """Switch the active client workspace and drop every cached query so the
    next render re-reads the freshly scoped data."""
    st.session_state["active_company_id"] = int(cid)
    st.cache_data.clear()


def list_companies() -> pd.DataFrame:
    """The client-workspace registry (companies table)."""
    return qdf("""
        SELECT company_id, company_name, industry_vertical, socal_region,
               created_at
          FROM companies
         ORDER BY company_id
    """)


def create_workspace(company_name: str, industry_vertical: str,
                     socal_region: str) -> int:
    """Insert a new client workspace; returns its company_id. The caller
    validates the name (required + unique) before calling; the UNIQUE
    constraint is the final guard."""
    conn = psycopg2.connect(DB)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO companies (company_name, industry_vertical,
                                       socal_region)
                VALUES (%s, %s, %s)
                RETURNING company_id
            """, (company_name.strip(),
                  (industry_vertical or "").strip() or None,
                  (socal_region or "").strip() or None))
            new_id = cur.fetchone()[0]
        conn.commit()
        return new_id
    finally:
        conn.close()


def company_slug(name: str, cid: int) -> str:
    """Filesystem-safe slug for the Executive PDF Brief filename."""
    slug = re.sub(r"[^a-z0-9]+", "_", (name or "").lower()).strip("_")
    return slug or f"company_{cid}"


# ---------------------------------------------------------------------------
# mutation helpers
# ---------------------------------------------------------------------------
def refresh_and_detect() -> int:
    conn = psycopg2.connect(DB)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT refresh_all_analytics();")
            cur.execute("SELECT * FROM detect_anomalies();")
            rows = cur.fetchall()
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def resolve_alerts(ids: list, note: str) -> int:
    if not ids:
        return 0
    conn = psycopg2.connect(DB)
    try:
        with conn.cursor() as cur:
            note_json = json.dumps({"resolution_note": note}) if note else None
            if note_json:
                cur.execute("""
                    UPDATE anomaly_alerts
                    SET is_resolved = TRUE, resolved_at = now(),
                        details = COALESCE(details, '{}'::jsonb) || %s::jsonb
                    WHERE id = ANY(%s) AND NOT is_resolved
                """, (note_json, ids))
            else:
                cur.execute("""
                    UPDATE anomaly_alerts
                    SET is_resolved = TRUE, resolved_at = now()
                    WHERE id = ANY(%s) AND NOT is_resolved
                """, (ids,))
            n = cur.rowcount
        conn.commit()
        return n
    finally:
        conn.close()


def run_ingest_from_uploads(uploads: list, mapping_bytes, company_id: int) -> dict:
    """Build an ingest config from uploaded files and run it via ingest_csv
    into the ACTIVE client workspace (company_id backfills every loaded
    record and scopes the FK lookups)."""
    UPLOADS_DIR.mkdir(exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    saved = []
    for up in uploads:
        dest = UPLOADS_DIR / f"{stamp}_{up.name.replace(' ', '_')}"
        dest.write_bytes(up.getvalue())
        saved.append(dest)

    if mapping_bytes is not None:
        config = json.loads(mapping_bytes.getvalue().decode("utf-8"))
        for spec, dest in zip(config.get("files", []), saved):
            spec["path"] = str(dest)
        return ingest_csv.run_ingest(config, db=DB, company_id=company_id)
    return ingest_csv.run_ingest({"files": []}, db=DB, company_id=company_id)


# ---------------------------------------------------------------------------
# page
# ---------------------------------------------------------------------------
st.title("SMB Financial & Operational Data Warehouse")
st.caption("PostgreSQL · smb_warehouse · data loaded via seed_demo.py / "
           "fetch_benchmarks.py / ingest_csv.py")

if not db_alive():
    st.error("Cannot reach the database. Start it with `./start_db.sh`, and "
             "apply `schema.sql` + `analytics_views.sql` if the schema is "
             "missing.")
    st.stop()

tab_overview, tab_charts, tab_alerts, tab_ingest, tab_pricing, tab_socal, \
    tab_forecast, tab_capital, tab_peers, tab_ask = st.tabs(
    ["Overview", "Charts", "Anomaly alerts", "Ingest CSVs",
     "Pricing simulator", "SoCal Benchmarks", "Forecast & cash runway",
     "SoCal Capital & Deals", "Public Peer Financials", "Ask Your Data"])

# ---------------------------------------------------------------------------
# SoCal regional benchmark vocabulary (FIXED strings -- shared by the SoCal
# Benchmarks tab and the vertical-mode pricing simulator; matches the CHECK
# constraints in schema.sql and the harvester's constants)
# ---------------------------------------------------------------------------
SOCAL_REGION_LABELS = {
    "la_msa":             "Los Angeles-Long Beach-Anaheim (LA MSA)",
    "oxnard_ventura_msa": "Oxnard-Thousand Oaks-Ventura (Ventura County MSA)",
    "orange_county":      "Orange County (Santa Ana-Anaheim-Irvine MD)",
    "san_diego_msa":      "San Diego-Chula Vista-Carlsbad (SD MSA)",
    "inland_empire_msa":  "Riverside-San Bernardino-Ontario (Inland Empire MSA)",
    "california_state":   "California statewide",
}
SOCAL_VERTICAL_LABELS = {
    "wealth_advisory":    "Boutique Wealth/Advisory Firm",
    "b2b_saas":           "B2B SaaS Startup",
    "custom_software_ai": "Custom Software/AI Agency",
    "fractional_cfo":     "Fractional CFO/Accounting Practice",
    "fintech_payments":   "Fintech/Payments Company",
    "managed_it_cyber":   "Managed IT/Cybersecurity Provider",
}
SOCAL_HARVEST_HINT = ("No SoCal benchmark data yet — run "
                      "`python3 harvest_socal_fintech.py` to load it "
                      "(REAL OEWS/FRED/CBP/ZBP/SBA harvests + modeled "
                      "vertical profiles).")

# -- data-module vocabularies (fixed strings matching funding_schema.sql /
#    peer_ratios_schema.sql CHECK constraints; the display labels are ours) --
CAPITAL_EVENT_TYPE_LABELS = {
    "SEC_FORM_D":     "SEC Form D",
    "SBIR_PHASE_1":   "SBIR Phase I",
    "SBIR_PHASE_2":   "SBIR Phase II",
    "STTR":           "STTR",
    "STATE_GRANT":    "State grant",
}
CAPITAL_SOURCE_LABELS = {
    "All sources": None,
    "Form D":      ("SEC_FORM_D",),
    "SBIR-STTR":   ("SBIR_PHASE_1", "SBIR_PHASE_2", "STTR"),
}
PEER_METRIC_LABELS = {
    "gross_margin_pct":     "Gross margin %",
    "operating_margin_pct": "Operating margin %",
    "rd_pct_of_rev":        "R&D as % of revenue",
    "sga_pct_of_rev":       "SG&A as % of revenue",
}
# mv_vertical_financial_quartiles names its percentile columns with these
# prefixes (note: the two *_margin_pct metrics DROP the _pct suffix in the
# matview: gross_margin_p25 / operating_margin_p75, etc.).
PEER_METRIC_MV_PREFIX = {
    "gross_margin_pct":     "gross_margin",
    "operating_margin_pct": "operating_margin",
    "rd_pct_of_rev":        "rd_pct_of_rev",
    "sga_pct_of_rev":       "sga_pct_of_rev",
}
SOCAL_CAPITAL_HINT = ("No SoCal capital events yet — run "
                      "`python3 harvest_socal_capital.py` to load REAL SEC "
                      "Form D filings + SBA SBIR/STTR awards for the six "
                      "SoCal counties (funding_schema.sql).")
PEER_RATIOS_HINT = ("No public peer financials yet — run "
                    "`python3 harvest_sec_peer_ratios.py` to harvest REAL "
                    "SEC EDGAR XBRL data for the 50-company peer set "
                    "(peer_ratios_schema.sql).")

@st.cache_data(ttl=300, show_spinner=False)
def _executive_brief_bytes(company_id: int) -> bytes:
    """Cached board-ready Executive Financial Brief for the active workspace
    (generate_pdf_brief.generate_pdf_brief opens its own read-only
    connection; the cache keeps reruns cheap)."""
    return generate_pdf_brief.generate_pdf_brief(company_id)


# -- sidebar ------------------------------------------------------------------
with st.sidebar:
    # ------------------------------ client workspace selector (multi-tenant)
    st.subheader("Client workspace")
    try:
        companies_df = list_companies()
    except Exception as exc:                        # noqa: BLE001
        companies_df = pd.DataFrame()
        st.error(f"Cannot load client workspaces: {exc}")

    active_company_id = None
    active_company_name = None
    if companies_df.empty:
        st.warning("No client workspaces exist yet — create one below "
                   "(or run migrations/001_add_companies.sql).")
    else:
        name_by_id = dict(zip(companies_df.company_id.astype(int),
                              companies_df.company_name))
        ids = [int(i) for i in companies_df.company_id]
        prev = st.session_state.get("active_company_id")
        default_id = prev if prev in ids else ids[0]
        sel = st.selectbox(
            "Client workspace", ids,
            index=ids.index(default_id),
            format_func=lambda cid: name_by_id.get(cid, str(cid)),
            help="Every company-scoped tab (Overview, Charts, Alerts, "
                 "Ingest, Pricing's local mode, Forecast, Ask Your Data) "
                 "reads and writes THIS client's data. Market/SoCal/peer "
                 "tabs stay global by design.")
        if sel != prev:
            set_active_company(sel)
        active_company_id = int(sel)
        active_company_name = name_by_id.get(active_company_id,
                                             str(active_company_id))
        st.caption(f"Active: **{active_company_name}**")

    if st.button("➕ New client workspace", width='stretch',
                 help="Register another client (a companies row). New "
                      "workspaces start empty — load their data via the "
                      "Ingest CSVs tab."):
        st.session_state["show_new_client_form"] = True
    if st.session_state.get("show_new_client_form"):
        with st.form("new_client_form", border=True):
            new_name = st.text_input("Company name (required)")
            new_industry = st.text_input("Industry vertical (optional)",
                                         placeholder="e.g. home_services_remodeling")
            new_region = st.text_input("SoCal region (optional)",
                                       placeholder="e.g. la_msa")
            form_ok = st.form_submit_button("Create workspace", type="primary")
        if form_ok:
            try:
                if not (new_name or "").strip():
                    st.error("Company name is required.")
                else:
                    existing = qdf_params(
                        "SELECT company_id FROM companies "
                        "WHERE lower(company_name) = lower(%s)",
                        (new_name.strip(),))
                    if not existing.empty:
                        st.error(f"A workspace named '{new_name.strip()}' "
                                 f"already exists.")
                    else:
                        new_id = create_workspace(new_name, new_industry,
                                                  new_region)
                        set_active_company(new_id)
                        st.session_state["show_new_client_form"] = False
                        st.success(f"Workspace '{new_name.strip()}' created "
                                   f"and selected.")
                        st.rerun()
            except Exception as exc:                # noqa: BLE001
                st.error(f"Could not create the workspace: {exc}")

    st.header("Warehouse actions")
    if st.button("Refresh & Detect Anomalies",
                 help="REFRESH all materialized views, then run "
                      "detect_anomalies() (per client workspace)",
                 type="primary", width='stretch'):
        try:
            n = refresh_and_detect()
            st.cache_data.clear()
            if n:
                st.success(f"{n} new alert(s) raised — see the Alerts tab.")
            else:
                st.info("Views refreshed. No new anomalies detected.")
        except Exception as exc:                    # noqa: BLE001
            st.error(f"Failed: {exc}")

    with st.expander("Maintenance"):
        st.caption("Reload the deterministic 36-month demo simulation as "
                   "client workspace #1. **Truncates all warehouse data** "
                   "(then re-fetches real wage/PPI baselines).")
        sure = st.checkbox("I understand this wipes current data")
        if st.button("Reload demo data", disabled=not sure,
                     width='stretch'):
            with st.spinner("Running seed_demo.py + fetch_benchmarks.py ..."):
                r1 = subprocess.run([sys.executable, str(BASE_DIR / "seed_demo.py")],
                                    capture_output=True, text=True)
                r2 = subprocess.run([sys.executable, str(BASE_DIR / "fetch_benchmarks.py")],
                                    capture_output=True, text=True)
            if r1.returncode == 0 and r2.returncode == 0:
                st.cache_data.clear()
                st.success("Demo data + benchmarks reloaded.")
            else:
                st.error(f"seed: {r1.stderr[-400:] or 'ok'} | "
                         f"fetch: {r2.stderr[-400:] or 'ok'}")

    try:
        if active_company_id is None:
            st.warning("Select or create a workspace to see its stats.")
        else:
            counts = qdf_params("""
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
            """, (active_company_id,) * 8).iloc[0]
            st.header("At a glance")
            st.caption(f"for **{active_company_name}**")
            st.metric("Jobs", int(counts.jobs))
            st.metric("Ledger rows", int(counts.ledger_rows))
            st.metric("Open alerts", int(counts.open_alerts))
            st.metric("Whale clients", int(counts.whales))
    except Exception as exc:                        # noqa: BLE001
        st.warning(f"Stats unavailable: {exc}")

# -- Overview tab --------------------------------------------------------------
# Multi-tenant: every query below is scoped to the ACTIVE client workspace
# (company_id), and the Executive PDF Brief is generated for it.
with tab_overview:
    st.caption(f"Client workspace: **{active_company_name}** "
               f"(company_id {active_company_id}) — all figures below are "
               f"this client's own books.")
    try:
        monthly = qdf_params("""
            SELECT month_start, year, month_name, revenue, total_expense,
                   gross_profit, gross_margin_pct, job_completions, new_customers
            FROM mv_monthly_summary
            WHERE company_id = %s
            ORDER BY month_start
        """, (active_company_id,))
        monthly["month_start"] = pd.to_datetime(monthly["month_start"])
        c1, c2, c3, c4 = st.columns(4)
        last_full = monthly[monthly.month_start
                            < pd.Timestamp.today().to_period("M").start_time.tz_localize(None)]
        if not last_full.empty:
            row = last_full.iloc[-1]
            c1.metric("Revenue (last full month)", f"${row.revenue:,.0f}")
            c2.metric("Gross profit", f"${row.gross_profit:,.0f}",
                      f"{row.gross_margin_pct:.1f}% margin")
            c3.metric("Completions", int(row.job_completions))
        open_n = int(counts.open_alerts)
        c4.metric("Open anomaly alerts", open_n,
                  delta=None if open_n == 0 else "needs triage",
                  delta_color="inverse")

        st.subheader("Monthly summary")
        st.dataframe(
            monthly.assign(month_start=lambda d: d.month_start.dt.date),
            width='stretch', hide_index=True)

        # ---- board-ready Executive Financial Brief (PDF) -------------------
        st.subheader("Executive PDF Brief")
        st.caption("A 2–3 page board-ready brief for this workspace: "
                   "executive KPIs, monthly P&L, service-line profitability, "
                   "top open anomalies and the 3-month forecast outlook "
                   "(generate_pdf_brief.py).")
        try:
            pdf_bytes = _executive_brief_bytes(active_company_id)
            st.download_button(
                "Download Executive PDF Brief",
                data=pdf_bytes,
                file_name=f"{company_slug(active_company_name, active_company_id)}"
                          f"_executive_brief_{dt.date.today():%Y-%m}.pdf",
                mime="application/pdf",
                help="Generates the brief for the ACTIVE client workspace "
                     "and downloads it as a PDF.")
        except Exception as exc:                    # noqa: BLE001
            st.error(f"Could not generate the Executive PDF Brief: {exc}")
    except Exception as exc:                        # noqa: BLE001
        st.error(f"Overview unavailable: {exc}")

# -- Charts tab ----------------------------------------------------------------
# Multi-tenant: the client-data charts read the ACTIVE workspace's rows
# (company_id); the market-baseline lines in the inflation chart are global.
with tab_charts:
    st.caption(f"Client workspace: **{active_company_name}** — client data "
               f"is this workspace's own; market baselines are shared.")
    try:
        st.subheader("Monthly cash P&L")
        if monthly.empty:
            st.info(f"No monthly activity yet for {active_company_name} — "
                    f"load ledger or labor data first (Ingest CSVs tab).")
        else:
            fig = go.Figure()
            fig.add_bar(x=monthly.month_name + " " + monthly.year.astype(str),
                        y=monthly.revenue, name="Revenue", marker_color="#2E7D32")
            fig.add_bar(x=monthly.month_name + " " + monthly.year.astype(str),
                        y=monthly.total_expense, name="Expenses", marker_color="#C62828")
            fig.add_scatter(x=monthly.month_name + " " + monthly.year.astype(str),
                            y=monthly.gross_profit, name="Gross profit",
                            mode="lines+markers", line={"width": 3})
            fig.update_layout(barmode="group", height=420, legend_orientation="h")
            st.plotly_chart(fig, width='stretch')

        left, right = st.columns(2)

        with left:
            st.subheader("Gross margin by service line")
            jt = qdf_params("""
                SELECT job_type, ROUND(margin_pct::numeric, 1) AS margin_pct,
                       revenue, job_count
                FROM mv_job_type_profitability
                WHERE company_id = %s
                ORDER BY margin_pct DESC
            """, (active_company_id,))
            if jt.empty:
                st.info("No service lines yet for this workspace.")
            else:
                fig = go.Figure(go.Bar(
                    x=jt.margin_pct, y=jt.job_type, orientation="h",
                    text=jt.margin_pct.map("{:.1f}%".format), textposition="outside",
                    marker_color="#1565C0"))
                fig.update_layout(height=340, xaxis_title="Gross margin %",
                                  yaxis={"autorange": "reversed"})
                st.plotly_chart(fig, width='stretch')

        with right:
            st.subheader("Worst labor overruns (top 15)")
            lab = qdf_params("""
                SELECT job_number, customer_name, labor_cost, estimated_labor_cost,
                       labor_variance_pct
                FROM mv_labor_cost_analysis
                WHERE company_id = %s
                  AND labor_variance_pct IS NOT NULL
                ORDER BY labor_variance_pct DESC LIMIT 15
            """, (active_company_id,))
            if lab.empty:
                st.info("No labor variances yet for this workspace (jobs "
                        "with logged hours create them).")
            else:
                fig = go.Figure(go.Bar(
                    x=lab.labor_variance_pct, y=lab.job_number, orientation="h",
                    text=lab.labor_variance_pct.map("{:+.0f}%".format),
                    textposition="outside",
                    marker_color=lab.labor_variance_pct.map(
                        lambda v: "#C62828" if v > 50 else "#EF6C00"),
                    customdata=lab.customer_name,
                    hovertemplate="%{y} (%{customdata})<br>%{x:+.1f}%<extra></extra>"))
                fig.update_layout(height=340, xaxis_title="Labor cost vs estimate %",
                                  yaxis={"autorange": "reversed"})
                st.plotly_chart(fig, width='stretch')

        st.subheader("Customer economics (whales highlighted)")
        cust = qdf_params("""
            SELECT customer_name, revenue, gross_margin, margin_pct, job_count,
                   is_whale, acquisition_cost
            FROM mv_customer_profitability
            WHERE company_id = %s AND job_count > 0
        """, (active_company_id,))
        if cust.empty:
            st.info("No customer economics yet for this workspace.")
        else:
            cust["size"] = cust.job_count.clip(lower=2)
            fig = go.Figure(go.Scatter(
                x=cust.revenue, y=cust.gross_margin, mode="markers",
                marker={"size": cust["size"] * 3,
                        "color": ["#C62828" if w else "#1565C0" for w in cust.is_whale]},
                customdata=cust[["customer_name", "margin_pct", "acquisition_cost"]],
                hovertemplate="%{customdata[0]}<br>Revenue $%{x:,.0f} · "
                              "Margin $%{y:,.0f} (%{customdata[1]}%)<br>"
                              "CAC $%{customdata[2]:,.0f}<extra></extra>"))
            fig.add_hline(y=0, line_dash="dot", line_color="#9E9E9E")
            fig.update_layout(height=380, xaxis_title="Revenue ($)",
                              yaxis_title="Gross margin ($)")
            st.plotly_chart(fig, width='stretch')
            st.caption("Red = whale (top-quintile revenue, negative net "
                       "margin). Bubble size = job count.")

        st.subheader("Our inflation vs the real market (YoY %)")
        bench = qdf_params("""
            SELECT period AS month_start,
                   internal_labor_yoy_pct, market_wage_national_yoy_pct,
                   internal_materials_yoy_pct, ppi_building_materials_yoy_pct
            FROM mv_benchmark_comparison
            WHERE company_id = %s
              AND (internal_labor_yoy_pct IS NOT NULL
                   OR internal_materials_yoy_pct IS NOT NULL)
            ORDER BY period
        """, (active_company_id,))
        if bench.empty:
            st.info("No internal inflation history yet for this workspace "
                    "(labor logs and materials expenses create it); the "
                    "market baseline columns are global.")
        else:
            b1, b2 = st.columns(2)
            with b1:
                fig = go.Figure()
                fig.add_scatter(x=bench.month_start, y=bench.internal_labor_yoy_pct,
                                name="Our labor", mode="lines", line={"width": 2})
                fig.add_scatter(x=bench.month_start,
                                y=bench.market_wage_national_yoy_pct,
                                name="BLS wage", mode="lines", line={"width": 2})
                fig.add_hline(y=0, line_dash="dot", line_color="#9E9E9E")
                fig.update_layout(height=320, yaxis_title="YoY %",
                                  title="Labor cost inflation")
                st.plotly_chart(fig, width='stretch')
            with b2:
                fig = go.Figure()
                fig.add_scatter(x=bench.month_start,
                                y=bench.internal_materials_yoy_pct,
                                name="Our materials", mode="lines", line={"width": 2})
                fig.add_scatter(x=bench.month_start,
                                y=bench.ppi_building_materials_yoy_pct,
                                name="PPI construction materials", mode="lines",
                                line={"width": 2})
                fig.add_hline(y=0, line_dash="dot", line_color="#9E9E9E")
                fig.update_layout(height=320, yaxis_title="YoY %",
                                  title="Materials cost inflation")
                st.plotly_chart(fig, width='stretch')
    except Exception as exc:                        # noqa: BLE001
        st.error(f"Charts unavailable: {exc}")

# -- Alerts tab ----------------------------------------------------------------
# Multi-tenant: only the ACTIVE workspace's alerts are triaged here.
with tab_alerts:
    st.subheader("Anomaly triage")
    st.caption(f"Showing open alerts for **{active_company_name}** "
               f"(company_id {active_company_id}) — each client workspace's "
               f"alerts are detected and triaged separately.")
    try:
        open_df = qdf_params("""
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
        """, (active_company_id,))
        if open_df.empty:
            st.success("No open alerts. Run **Refresh & Detect Anomalies** "
                       "after loading new data.")
        else:
            edit_df = open_df.copy()
            edit_df.insert(0, "resolve", False)
            edit_df["metric"] = edit_df.metric.map("{:,.2f}".format)
            edit_df["baseline"] = edit_df.baseline.map("{:,.2f}".format)
            edit_df["dev_pct"] = edit_df.dev_pct.map("{:+,.1f}%".format)
            edited = st.data_editor(
                edit_df, hide_index=True, width='stretch',
                column_config={
                    "resolve": st.column_config.CheckboxColumn("Resolve?"),
                    "severity": st.column_config.TextColumn("Severity"),
                },
                disabled=["id", "type", "severity", "subject", "metric",
                          "baseline", "dev_pct", "z", "detected"])
            note = st.text_input("Resolution note (optional, stored in the "
                                 "alert's JSONB details)",
                                 placeholder="e.g. lumber supplier credit received")
            if st.button("Resolve selected alerts",
                         disabled=not edited["resolve"].any()):
                ids = edited.loc[edited["resolve"], "id"].astype(int).tolist()
                n = resolve_alerts(ids, note)
                st.cache_data.clear()
                st.success(f"Resolved {n} alert(s). The detector may re-flag "
                           f"the same subject later if it recurs.")
    except Exception as exc:                        # noqa: BLE001
        st.error(f"Alerts unavailable: {exc}")

# -- Ingest tab ----------------------------------------------------------------
# Multi-tenant: uploads load into the ACTIVE client workspace —
# ingest_csv.run_ingest(company_id=...) backfills company_id on every loaded
# row and scopes FK resolution, so rows can never land in another client's
# books.
with tab_ingest:
    st.subheader("Load CSV exports")
    st.caption(f"Loading into client workspace **{active_company_name}** "
               f"(company_id {active_company_id}) — every loaded row is "
               f"tagged with this workspace. Upload messy accounting/payroll "
               f"CSVs and map them into the warehouse with a JSON "
               f"column-mapping config (see `ingest_config.example.json`). "
               f"Cleaning, FK resolution and deduplication happen "
               f"automatically; bad rows are skipped and reported, never "
               f"fatal.")
    uploads = st.file_uploader("CSV files", type=["csv", "txt"],
                               accept_multiple_files=True)
    mapping = st.file_uploader("Custom mapping config (optional JSON)",
                               type=["json"])
    preset_name = None
    if mapping is None and uploads:
        preset_name = st.select_slider(
            "Mapping preset",
            options=["QuickBooks-style accounting export",
                     "Payroll timesheet export"])
        st.caption(f"Using the built-in preset for '{preset_name}' — CSV "
                   f"columns must match the preset's headers (Date, "
                   f"Description, Amount, Category, Job — or Employee Email, "
                   f"Job Number, Work Date, Hours, Rate, Notes).")

    if st.button("Ingest uploads", type="primary",
                 disabled=not uploads) and uploads:
        try:
            with st.spinner("Ingesting..."):
                if mapping is not None:
                    summary = run_ingest_from_uploads(uploads, mapping,
                                                      active_company_id)
                else:
                    preset_cfg = json.loads(PRESET_CONFIG.read_text())
                    specs = preset_cfg["files"]
                    pick = 0 if (preset_name or "").startswith("QuickBooks") else 1
                    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
                    UPLOADS_DIR.mkdir(exist_ok=True)
                    saved = []
                    for up in uploads:
                        dest = UPLOADS_DIR / f"{stamp}_{up.name.replace(' ', '_')}"
                        dest.write_bytes(up.getvalue())
                        saved.append(dest)
                    config = {"files": [dict(specs[pick], path=str(p))
                                        for p in saved]}
                    summary = ingest_csv.run_ingest(config, db=DB,
                                                    company_id=active_company_id)
            st.session_state["last_ingest"] = summary
            st.cache_data.clear()
        except Exception as exc:                    # noqa: BLE001
            st.error(f"Ingest failed: {exc}")

    summary = st.session_state.get("last_ingest")
    if summary:
        st.caption(f"Rows were loaded into client workspace "
                   f"**{active_company_name}** (the workspace selected at "
                   f"ingest time).")
        for res in summary["results"]:
            with st.container(border=True):
                if res.get("error"):
                    st.error(f"{res.get('file')}: {res['error']}")
                    continue
                m1, m2, m3 = st.columns(3)
                m1.metric("Rows read", res["rows_read"])
                m2.metric("Loaded", res["loaded"])
                m3.metric("Duplicates skipped", res["skipped_duplicates"])
                if res.get("created_refs") and any(res["created_refs"].values()):
                    st.caption(f"Auto-created references: {res['created_refs']}")
                if res.get("errors"):
                    st.markdown("**Skipped rows**")
                    st.dataframe(pd.DataFrame(res["errors"]),
                                 width='stretch', hide_index=True)
        if summary.get("refreshed"):
            st.info(f"Views refreshed — detect_anomalies() raised "
                    f"{summary['new_alerts']} new alert(s).")

    with st.expander("Preset mapping config (ingest_config.example.json)"):
        st.json(json.loads(PRESET_CONFIG.read_text()))
    with st.expander("Recent uploads on disk"):
        if UPLOADS_DIR.exists():
            st.write([p.name for p in sorted(UPLOADS_DIR.iterdir())[-10:]])
        else:
            st.write("No uploads yet.")

# -- Pricing simulator tab ------------------------------------------------------
# What-if gross-margin math for ONE job quote, prefilled from the live
# service-line data. All math lives in pricing_simulator.simulate_job (pure,
# unit-tested in test_schema.py); this tab only pre-fills its arguments and
# renders the result. Prefill per service line:
#   hours/job        = labor_hours / job_count        (mv_job_type_profitability)
#   blended base rate = labor_cost / labor_hours      (hours-weighted)
#   materials ratio  = materials_cost / revenue       (both from the ledger)
#   last quoted avg  = avg quoted_price over the trailing 12 months of the
#                      type's most recent job date (falls back to the all-time
#                      avg_job_price, then to the national benchmark price)
#   market margin    = latest 'national' benchmark avg_margin_pct for the type
with tab_pricing:
    st.subheader("Pricing simulator")
    scope = st.radio(
        "Prefill from:",
        ["Local service line", "SoCal Finance/Tech vertical"],
        horizontal=True,
        help="Local service line: existing behaviour, prefilled from the "
             "warehouse's own service-line data. SoCal Finance/Tech vertical: "
             "prefilled from the harvested regional benchmarks "
             "(harvest_socal_fintech.py -> mv_socal_rate_guidance).")

    if scope == "SoCal Finance/Tech vertical":
        # -- vertical mode: prefilled from the harvested SoCal benchmarks ----
        # hourly_cost / loaded_cost / suggested_billable_rate come straight
        # from mv_socal_rate_guidance (the documented formula: hourly_cost =
        # annual OEWS median / 2080 * (1 + locality_burden); loaded_cost =
        # hourly_cost / utilization_target; suggested rate = loaded_cost /
        # (1 - gross_margin)). Pure labor math: material_ratio and the other-
        # cost ratio are 0 here because the rate guidance already covers the
        # full labor economics of a billable hour. The California statewide
        # profile's margin is passed as the market margin so the delta shows
        # the regional premium/discount; the view's rate_vs_statewide_pct /
        # rate_vs_la_pct quantify the same premium on the RATE itself.
        st.caption("Rate-guidance mode — pure labor math (no materials/other "
                   "costs), prefilled from mv_socal_rate_guidance.")
        try:
            sel_v_region = st.selectbox(
                "SoCal Metro Region", list(SOCAL_REGION_LABELS),
                format_func=SOCAL_REGION_LABELS.get, key="sim_socal_region")
            sel_v_vertical = st.selectbox(
                "Finance/Tech Vertical", list(SOCAL_VERTICAL_LABELS),
                format_func=SOCAL_VERTICAL_LABELS.get, key="sim_socal_vertical")
            guide = qdf(f"""
                SELECT region_code, vertical_key, vertical_name, data_year,
                       role_label, annual_median_salary, locality_burden_pct,
                       utilization_target_pct, gross_margin_pct, hourly_cost,
                       loaded_cost, suggested_billable_rate, cac_usd,
                       statewide_billable_rate, rate_vs_statewide_pct,
                       la_billable_rate, rate_vs_la_pct
                  FROM mv_socal_rate_guidance
                 WHERE region_code = '{sel_v_region}'
                   AND vertical_key = '{sel_v_vertical}'
            """)
            if guide.empty:
                st.info("No rate guidance for this region/vertical yet — run "
                        "`python3 harvest_socal_fintech.py` (sources oews + "
                        "profiles) to load it.")
            else:
                row = guide.iloc[0]
                hourly_cost = float(row.hourly_cost)
                loaded_cost = float(row.loaded_cost)
                suggested = float(row.suggested_billable_rate)
                util = float(row.utilization_target_pct)
                target_margin = int(round(float(row.gross_margin_pct)))

                st.markdown(
                    f"**Prefill from rate guidance** — {row.vertical_name} · "
                    f"{SOCAL_REGION_LABELS[sel_v_region]} · OEWS "
                    f"{row.data_year} {row.role_label} median "
                    f"${float(row.annual_median_salary):,.0f} · locality "
                    f"burden {float(row.locality_burden_pct):+.1f}% · "
                    f"utilization {util:.0f}% · hourly cost "
                    f"${hourly_cost:,.2f} → loaded ${loaded_cost:,.2f} → "
                    f"suggested **${suggested:,.2f}/hr**")
                c1, c2 = st.columns(2)
                with c1:
                    sim_hours = st.slider("Billable hours per month", 40, 400,
                                          160, step=4, width='stretch')
                    sim_ot = st.slider("Overtime (% of hours paid 1.5x)", 0, 50,
                                       0, format="%d%%", width='stretch')
                with c2:
                    sim_padj = st.slider("Price adjustment vs suggested rate",
                                         -25, 50, 0, format="%+d%%",
                                         width='stretch')
                    sim_target = st.slider("Target gross margin", 10, 90,
                                           min(max(target_margin, 10), 90),
                                           format="%d%%", width='stretch')

                sim_price = suggested * sim_hours * (1 + sim_padj / 100.0)
                ca_margin = qdf(f"""
                    SELECT gross_margin_pct FROM regional_industry_profiles
                     WHERE vertical_key = '{sel_v_vertical}'
                       AND region_code = 'california_state'
                """)
                market_margin = (float(ca_margin.iloc[0, 0]) / 100.0
                                 if not ca_margin.empty else None)
                sim = pricing_simulator.simulate_job(
                    hours=sim_hours, base_rate=hourly_cost,
                    ot_pct=sim_ot / 100.0, material_ratio=0.0,
                    material_change_pct=0.0, price=sim_price,
                    other_cost_ratio=0.0, target_margin=sim_target / 100.0,
                    market_margin=market_margin)

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Monthly price", f"${sim['price']:,.0f}",
                          f"{sim_hours} billable h × ${sim_price / sim_hours:,.2f}")
                m2.metric("Margin %", f"{sim['margin_pct']:.1f}%",
                          f"{sim['margin_vs_target_pct']:+.1f} pp vs target")
                m3.metric("Rate vs statewide",
                          (f"{float(row.rate_vs_statewide_pct):+.1f}%"
                           if row.rate_vs_statewide_pct is not None else "n/a"),
                          (f"statewide ${float(row.statewide_billable_rate):,.2f}/hr"
                           if row.statewide_billable_rate is not None else None))
                m4.metric("Rate vs LA",
                          (f"{float(row.rate_vs_la_pct):+.1f}%"
                           if row.rate_vs_la_pct is not None else "n/a"),
                          (f"LA ${float(row.la_billable_rate):,.2f}/hr"
                           if row.la_billable_rate is not None else None))
                st.caption(
                    f"Suggested rate ${suggested:,.2f}/hr = loaded cost "
                    f"${loaded_cost:,.2f} (hourly ${hourly_cost:,.2f} ÷ "
                    f"utilization {util:.0f}%) ÷ (1 − target margin "
                    f"{float(row.gross_margin_pct):.0f}%). CAC for this "
                    f"vertical: ${float(row.cac_usd):,.0f}. Regional "
                    f"premium/discount lives in the RATE (locality burden "
                    f"{float(row.locality_burden_pct):+.1f}%), not in the "
                    f"margin target.")
        except Exception as exc:                    # noqa: BLE001
            st.error(f"SoCal vertical simulator unavailable: {exc}")

    if scope == "Local service line":
        st.caption(f"Prefilled from **{active_company_name}**'s own "
                   f"service-line data (company_id {active_company_id}). "
                   f"Adjust one job quote's crew hours, overtime, material "
                   f"costs and price to see the gross margin move; overtime "
                   f"premium = 1.5x pay on the OT share of hours, other "
                   f"costs default to 5% of price (equipment, permits, "
                   f"disposal).")
        try:
            lines = qdf_params("""
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
            """, (active_company_id,))
            if lines.empty:
                st.info(f"No service lines yet for {active_company_name} — "
                        f"load data first (Ingest CSVs tab).")
            else:
                sel_type = st.selectbox("Service line", lines.job_type.tolist())
                row = lines[lines.job_type == sel_type].iloc[0]

                job_count = max(int(row.job_count or 0), 1)
                base_hours = float(row.labor_hours or 0.0) / job_count
                if base_hours <= 0:
                    base_hours = (float(row.typical_duration_hours)
                                  if row.typical_duration_hours else 8.0)
                base_hours = max(base_hours, 0.5)
                blended_rate = (float(row.labor_cost) / float(row.labor_hours)
                                if row.labor_hours else float(row.default_hourly_rate))
                material_ratio = (float(row.materials_cost) / float(row.revenue)
                                  if row.revenue else 0.0)
                last_price = (float(row.recent_avg_price)
                              if row.recent_avg_price is not None
                              else (float(row.avg_job_price) if row.avg_job_price else 0.0))
                if last_price <= 0 and row.benchmark_avg_price:
                    last_price = float(row.benchmark_avg_price)
                market_margin = (float(row.benchmark_avg_margin_pct) / 100.0
                                 if row.benchmark_avg_margin_pct is not None else None)

                st.markdown(f"**Prefill from live data** — avg hours/job "
                            f"{base_hours:.2f} · blended rate ${blended_rate:,.2f}/hr · "
                            f"materials {material_ratio * 100:.1f}% of price · "
                            f"recent avg quote ${last_price:,.0f}")
                c1, c2 = st.columns(2)
                with c1:
                    h_min, h_max = round(0.25 * base_hours, 2), round(3.0 * base_hours, 2)
                    if h_max <= h_min:
                        h_max = h_min + 1.0
                    sim_hours = st.slider("Crew hours per job", h_min, h_max,
                                          min(max(base_hours, h_min), h_max),
                                          step=0.25, format="%.2f", width='stretch')
                    sim_ot = st.slider("Overtime (% of crew hours paid 1.5x)", 0, 50, 0,
                                       format="%d%%", width='stretch')
                    sim_mat = st.slider("Material cost change", -30, 100, 0,
                                        format="%+d%%", width='stretch')
                with c2:
                    sim_padj = st.slider("Price adjustment vs recent avg quote",
                                         -25, 50, 0, format="%+d%%", width='stretch')
                    sim_target = st.slider("Target gross margin", 15, 60, 30,
                                           format="%d%%", width='stretch')

                sim_price = last_price * (1 + sim_padj / 100.0)
                sim = pricing_simulator.simulate_job(
                    hours=sim_hours, base_rate=blended_rate, ot_pct=sim_ot / 100.0,
                    material_ratio=material_ratio, material_change_pct=sim_mat / 100.0,
                    price=sim_price, target_margin=sim_target / 100.0,
                    market_margin=market_margin)

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Projected margin", f"${sim['gross_margin']:,.0f}",
                          f"{sim['margin_vs_target_pct']:+.1f} pp vs target",
                          delta_color="normal")
                m2.metric("Margin %", f"{sim['margin_pct']:.1f}%",
                          (f"{sim['margin_vs_market_pct']:+.1f} pp vs market"
                           if sim["margin_vs_market_pct"] is not None else None))
                m3.metric("Total cost", f"${sim['total_cost']:,.0f}",
                          f"price ${sim['price']:,.0f}")
                m4.metric("Labor / materials",
                          f"${sim['labor_cost']:,.0f} / ${sim['material_cost']:,.0f}",
                          (f"OT {sim['ot_pct'] * 100:.0f}% · mat "
                           f"{sim['material_change_pct'] * 100:+.0f}%"))

                brk = go.Figure(go.Bar(
                    x=["Labor", "Materials", "Other", "Gross margin"],
                    y=[sim["labor_cost"], sim["material_cost"], sim["other_cost"],
                       sim["gross_margin"]],
                    marker_color=["#C62828", "#EF6C00", "#9E9E9E", "#2E7D32"],
                    text=[f"${v:,.0f}" for v in (sim["labor_cost"], sim["material_cost"],
                                                 sim["other_cost"], sim["gross_margin"])],
                    textposition="outside"))
                brk.update_layout(height=320, yaxis_title="$ per job",
                                  title=f"Quote breakdown — {sel_type}")
                st.plotly_chart(brk, width='stretch')
                if sim["clamps_applied"]:
                    st.caption("Clamped inputs: " + "; ".join(sim["clamps_applied"]))

                st.markdown("**Current actual average for this service line**")
                k1, k2, k3, k4 = st.columns(4)
                k1.metric("Actual margin %", (f"{float(row.margin_pct):.1f}%"
                                              if row.margin_pct is not None else "n/a"),
                          (f"market {float(row.benchmark_avg_margin_pct):.1f}%"
                           if row.benchmark_avg_margin_pct is not None else None))
                k2.metric("Actual avg quote",
                          f"${float(row.avg_job_price or 0):,.0f}",
                          (f"benchmark ${float(row.benchmark_avg_price):,.0f}"
                           if row.benchmark_avg_price is not None else None))
                k3.metric("Actual labor $/job",
                          f"${float(row.labor_cost or 0) / job_count:,.0f}")
                k4.metric("Actual materials $/job",
                          f"${float(row.materials_cost or 0) / job_count:,.0f}")
                st.caption(f"Comparison basis: latest 'national' market_benchmarks row "
                           f"for {sel_type} (see mv_job_type_profitability).")
        except Exception as exc:                        # noqa: BLE001
            st.error(f"Pricing simulator unavailable: {exc}")

# -- SoCal Benchmarks tab -------------------------------------------------------
# Regional Finance/Tech benchmark data for the six SoCal metros, loaded by
# harvest_socal_fintech.py: REAL BLS OEWS wages (May 2024), REAL FRED/BLS
# macro series, REAL Census CBP/ZBP 2023 business patterns, REAL SBA 7(a)/504
# lending, plus the MODELED vertical economics distilled into
# mv_socal_rate_guidance (hourly cost -> loaded cost -> suggested billable
# rate; the documented formula lives in analytics_views.sql section 7).
# Every section degrades to an st.info hint (never a crash) when the harvest
# has not run yet.
with tab_socal:
    st.subheader("SoCal Benchmarks")
    st.caption("Market data — shared across client workspaces. "
               "Finance/Tech benchmarks for the six SoCal metros — REAL "
               "harvested data (BLS OEWS wages, FRED/BLS macro series, Census "
               "CBP/ZBP business patterns, SBA lending) + MODELED vertical "
               "economics (harvest_socal_fintech.py). Rate guidance: "
               "hourly cost = OEWS median / 2080 × (1 + locality burden) → "
               "loaded cost = ÷ utilization target → suggested billable rate "
               "= ÷ (1 − gross margin target).")
    try:
        sel_region = st.selectbox("SoCal Metro Region", list(SOCAL_REGION_LABELS),
                                  format_func=SOCAL_REGION_LABELS.get,
                                  key="socal_region")
        sel_vertical = st.selectbox("Finance/Tech Vertical", list(SOCAL_VERTICAL_LABELS),
                                    format_func=SOCAL_VERTICAL_LABELS.get,
                                    key="socal_vertical")

        # (a) regional wage table (REAL OEWS, as published) -----------------
        st.markdown("**Regional wages — BLS OEWS (as published)**")
        wages = qdf(f"""
            SELECT occupation_code, role_label, occupation_title, data_year,
                   hourly_p25, hourly_median, hourly_p75, hourly_p90,
                   annual_median, employment, source
              FROM socal_wage_benchmarks
             WHERE region_code = '{sel_region}'
             ORDER BY data_year DESC, occupation_code
        """)
        if wages.empty:
            st.info(SOCAL_HARVEST_HINT)
        else:
            wage_view = wages.rename(columns={
                "occupation_code": "OEWS code", "role_label": "Role",
                "occupation_title": "OEWS title", "data_year": "Year",
                "hourly_p25": "$/hr p25", "hourly_median": "$/hr median",
                "hourly_p75": "$/hr p75", "hourly_p90": "$/hr p90",
                "annual_median": "annual median", "employment": "employment"})
            st.dataframe(wage_view, width='stretch', hide_index=True)
            st.caption("Source vintage per row in `source` (metro/division "
                       "file oesm24ma.zip + state file oesm24st.zip, May "
                       "2024). Orange County rows use the California "
                       "statewide estimate as a documented proxy: BLS no "
                       "longer publishes the Santa Ana-Anaheim-Irvine metro "
                       "division separately.")

        # (b) rate-guidance table (modeled economics x real wages) ----------
        st.markdown("**Billable-rate guidance** — this region vs statewide / LA")
        guidance = qdf(f"""
            SELECT vertical_key, vertical_name, data_year, role_label,
                   annual_median_salary, hourly_cost, loaded_cost,
                   suggested_billable_rate, statewide_billable_rate,
                   rate_vs_statewide_pct, la_billable_rate, rate_vs_la_pct
              FROM mv_socal_rate_guidance
             WHERE region_code = '{sel_region}'
             ORDER BY vertical_key
        """)
        if guidance.empty:
            st.info(SOCAL_HARVEST_HINT)
        else:
            guide_view = guidance.rename(columns={
                "vertical_key": "vertical", "vertical_name": "Vertical",
                "data_year": "OEWS year", "role_label": "Anchor role",
                "annual_median_salary": "median salary",
                "hourly_cost": "hourly cost", "loaded_cost": "loaded cost",
                "suggested_billable_rate": "suggested rate",
                "statewide_billable_rate": "statewide rate",
                "rate_vs_statewide_pct": "vs statewide %",
                "la_billable_rate": "LA rate", "rate_vs_la_pct": "vs LA %"})
            st.dataframe(guide_view, width='stretch', hide_index=True)
            st.caption("Rate deltas are percent: positive = this region's "
                       "guidance is pricier than the reference region "
                       "(locality burden + salary mix).")

        # (c) vertical economics card (modeled constants across ALL regions)
        st.markdown("**Vertical economics — "
                    f"{SOCAL_VERTICAL_LABELS[sel_vertical]} across all regions**")
        profile = qdf(f"""
            SELECT region_code, gross_margin_pct, gross_margin_p25_pct,
                   gross_margin_p75_pct, payroll_to_revenue_ratio, cac_usd,
                   utilization_target_pct, locality_burden_pct,
                   median_salary_usd
              FROM regional_industry_profiles
             WHERE vertical_key = '{sel_vertical}'
             ORDER BY region_code
        """)
        if profile.empty:
            st.info(SOCAL_HARVEST_HINT)
        else:
            prof_view = profile.rename(columns={
                "region_code": "region", "gross_margin_pct": "margin %",
                "gross_margin_p25_pct": "p25", "gross_margin_p75_pct": "p75",
                "payroll_to_revenue_ratio": "payroll/revenue",
                "cac_usd": "CAC", "utilization_target_pct": "utilization %",
                "locality_burden_pct": "locality burden %",
                "median_salary_usd": "anchor median salary"})
            st.dataframe(prof_view, width='stretch', hide_index=True)
            st.caption("Margin band / payroll ratio / CAC / utilization are "
                       "MODELED industry-norm constants (industry norms "
                       "2025-2026 — NOT scraped data, documented in the "
                       "notes JSONB). Locality burden is the modeled "
                       "regional payroll-burden delta vs the California "
                       "baseline. Median salary is harvested OEWS data.")

        # (d) macro charts: CPI + office-lease PPI, employment trend -------
        ch1, ch2 = st.columns(2)
        with ch1:
            st.markdown("**CPI & office-lease cost index**")
            macro = qdf(f"""
                SELECT period, category, series_name, value
                  FROM socal_macro_indicators
                 WHERE region_code = '{sel_region}'
                   AND category IN ('cpi', 'real_estate')
                 ORDER BY period
            """)
            if macro.empty:
                st.info(SOCAL_HARVEST_HINT)
            else:
                fig = go.Figure()
                colors = {"cpi": "#1565C0", "real_estate": "#EF6C00"}
                for cat, label in (("cpi", "CPI"), ("real_estate",
                                                   "PPI lessors of nonres. buildings")):
                    part = macro[macro.category == cat]
                    if part.empty:
                        continue
                    # one line per series (a region may hold several CPI series)
                    for name, seg in part.groupby("series_name"):
                        fig.add_trace(go.Scatter(
                            x=pd.to_datetime(seg.period), y=seg.value,
                            mode="lines", name=label,
                            hovertext=name, line=dict(color=colors[cat])))
                fig.update_layout(height=340, margin=dict(l=10, r=10, t=24, b=10),
                                  yaxis_title="index level")
                st.plotly_chart(fig, width='stretch')
                st.caption("CPI = this region's CPI-U (US CPI-U proxy where "
                           "no metro series exists); PPI = BLS PPI lessors of "
                           "nonresidential buildings (national office-landlord "
                           "price index — the closest official office-lease-"
                           "cost proxy).")
        with ch2:
            st.markdown("**Finance/Tech employment**")
            emp = qdf(f"""
                SELECT period, series_name, value
                  FROM socal_macro_indicators
                 WHERE region_code = '{sel_region}'
                   AND category = 'employment'
                 ORDER BY period
            """)
            if emp.empty:
                st.info(SOCAL_HARVEST_HINT)
            else:
                fig = go.Figure()
                for name, seg in emp.groupby("series_name"):
                    fig.add_trace(go.Scatter(x=pd.to_datetime(seg.period),
                                             y=seg.value, mode="lines",
                                             name=name.split(" employment")[0]))
                fig.update_layout(height=340, margin=dict(l=10, r=10, t=24, b=10),
                                  yaxis_title="thousands of employees")
                st.plotly_chart(fig, width='stretch')
                st.caption("CES employment, thousands (FRED). LA also has "
                           "metro-level financial activities / information / "
                           "professional-services lines; other regions show "
                           "the California / US baselines stored under "
                           "california_state.")

        # (e) SBA lending table (REAL FOIA aggregates) -----------------------
        st.markdown("**SBA lending — 7(a) & 504 approvals**")
        loans = qdf(f"""
            SELECT program, fiscal_year, geo_level, geo_name, loans_count,
                   gross_approval_usd, avg_loan_usd, top_lender,
                   top_lender_loans, source
              FROM socal_loan_activity
             WHERE region_code = '{sel_region}'
             ORDER BY program, fiscal_year DESC, geo_level
        """)
        if loans.empty:
            st.info("No SBA loan rows for this region (the harvest degrades "
                    "gracefully when data.sba.gov is unreachable — see "
                    "socal_harvest_log). " + SOCAL_HARVEST_HINT)
        else:
            loan_view = loans.drop(columns=["source"]).rename(columns={
                "program": "program", "fiscal_year": "FY",
                "geo_level": "geo", "geo_name": "geography",
                "loans_count": "loans", "gross_approval_usd": "gross approvals",
                "avg_loan_usd": "avg loan", "top_lender": "top lender",
                "top_lender_loans": "top lender loans"})
            st.dataframe(loan_view, width='stretch', hide_index=True)
            st.caption("Aggregated from the loan-level SBA FOIA CSVs "
                       "(vintage per row in `source`).")
    except Exception as exc:                        # noqa: BLE001
        st.error(f"SoCal Benchmarks unavailable: {exc}")

# -- Forecast & cash runway tab -------------------------------------------------
# seasonal_trend_v1 output (method documented in forecast_engine.py's docstring
# and README section 11). The buttons call the SAME engine functions the CLI
# uses; forecasts/backtest_results are upserted (never duplicated) and the data
# cache is cleared so every tab immediately reflects the new rows.
FORECAST_LABELS = {
    "revenue": "Revenue",
    "labor_cost": "Labor cost",
    "material_expense": "Materials expense",
    "net_cash_flow": "Net cash flow",
}
# forecasts.series -> mv_monthly_summary column of the same monthly history
FORECAST_HIST_COL = {
    "revenue": "revenue",
    "labor_cost": "labor_cost",
    "material_expense": "materials_expense",
    "net_cash_flow": "net_cash_flow",       # aliased from gross_profit below
}
FORECAST_COLORS = {
    "revenue": "#2E7D32", "labor_cost": "#1565C0",
    "material_expense": "#EF6C00", "net_cash_flow": "#6A1B9A",
}

with tab_forecast:
    st.subheader("Forecast & cash runway")
    st.caption(f"Forecasts for client workspace **{active_company_name}** "
               f"(company_id {active_company_id}). Model seasonal_trend_v1: "
               "multiplicative seasonal indices + least-squares trend, "
               "drift = 0.5 * model trend (capped at +/-5%/mo) + 0.5 * "
               "market inflation (BLS wage / PPI YoY), 90% CI = pred +/- "
               "1.645 * sigma * sqrt(h). Accuracy: MAPE for the positive "
               "series, WAPE for net_cash_flow (negative months make MAPE "
               "undefined). Details: forecast_engine.py docstring.")

    f1, f2 = st.columns(2)
    if f1.button("Generate forecasts", type="primary", width='stretch',
                 help=f"Run seasonal_trend_v1 for all 4 series of "
                      f"{active_company_name}, 3 months ahead (same as "
                      f"`python3 forecast_engine.py forecast --company-id "
                      f"{active_company_id}`)"):
        try:
            with st.spinner("Fitting seasonal_trend_v1 ..."):
                res = forecast_engine.run_forecasts(
                    months_ahead=3, company_id=active_company_id)
            st.cache_data.clear()
            n_series = sum(1 for s in forecast_engine.SERIES if res.get(s))
            n_rows = sum(len(res.get(s) or []) for s in forecast_engine.SERIES)
            st.success(f"Wrote/updated {n_rows} forecast rows across "
                       f"{n_series} series for {active_company_name}.")
        except Exception as exc:                    # noqa: BLE001
            st.error(f"Forecast failed: {exc}")
    if f2.button("Run backtest", width='stretch',
                 help=f"Walk-forward validation of {active_company_name}'s "
                      f"series, train 30 months / test 6 (same as "
                      f"`python3 forecast_engine.py backtest --company-id "
                      f"{active_company_id}`; folds land in the GLOBAL "
                      f"model audit trail)"):
        try:
            with st.spinner("Running walk-forward backtest ..."):
                bt = forecast_engine.run_backtest(company_id=active_company_id)
            st.cache_data.clear()
            done = [s for s, v in bt.items() if "error" not in v]
            st.success(f"Backtest complete: {len(done)} series validated "
                       f"({', '.join(done) or 'none'}).")
        except Exception as exc:                    # noqa: BLE001
            st.error(f"Backtest failed: {exc}")

    try:
        hist = qdf_params("""
            SELECT month_start, revenue, labor_cost, materials_expense,
                   gross_profit AS net_cash_flow
              FROM mv_monthly_summary
             WHERE company_id = %s
             ORDER BY month_start
        """, (active_company_id,))
        hist["month_start"] = pd.to_datetime(hist["month_start"])

        fc = qdf_params("""
            SELECT series, period_start, predicted, ci_low, ci_high,
                   horizon_months, mape, rmse
              FROM forecasts
             WHERE company_id = %s
             ORDER BY series, horizon_months
        """, (active_company_id,))
        if fc.empty:
            st.info("No forecasts yet — click **Generate forecasts** above "
                    "(or run `python3 forecast_engine.py forecast`).")
        else:
            fc["period_start"] = pd.to_datetime(fc["period_start"])
            present = [s for s in forecast_engine.SERIES
                       if s in set(fc.series)]
            chosen = st.selectbox("Series", present,
                                  format_func=lambda s: FORECAST_LABELS[s])
            frow = fc[fc.series == chosen].sort_values("horizon_months")
            hcol = FORECAST_HIST_COL[chosen]
            actual = hist[["month_start", hcol]].dropna().tail(12)

            fig = go.Figure()
            if not actual.empty:
                fig.add_scatter(x=actual.month_start, y=actual[hcol],
                                name="Actual (last 12 mo)", mode="lines+markers",
                                line={"width": 3, "color": FORECAST_COLORS[chosen]})
                # Connect the forecast line to the last actual point, then the
                # predicted path, then the 90% CI band as a filled self-closed
                # ribbon (upper bound out, lower bound back).
                connect_x = [actual.month_start.iloc[-1]] + frow.period_start.tolist()
                connect_y = [actual[hcol].iloc[-1]] + frow.predicted.tolist()
            else:
                connect_x = frow.period_start.tolist()
                connect_y = frow.predicted.tolist()
            fig.add_scatter(x=connect_x, y=connect_y,
                            name="Forecast (seasonal_trend_v1)",
                            mode="lines+markers",
                            line={"width": 3, "color": FORECAST_COLORS[chosen],
                                  "dash": "dash"},
                            marker={"symbol": "diamond"})
            band_x = frow.period_start.tolist() + frow.period_start.tolist()[::-1]
            band_y = frow.ci_high.tolist() + frow.ci_low.tolist()[::-1]
            fig.add_scatter(x=band_x, y=band_y, name="90% CI",
                            fill="toself", mode="lines", line={"width": 0},
                            fillcolor="rgba(150,150,150,0.25)",
                            hoverinfo="skip")
            fig.update_layout(height=380,
                              yaxis_title="$ / month",
                              title=f"{FORECAST_LABELS[chosen]} — actuals + "
                                    f"{len(frow)}-month forecast",
                              legend_orientation="h")
            st.plotly_chart(fig, width='stretch')

            acc_col = ("WAPE" if chosen == "net_cash_flow" else "MAPE")
            acc_val = frow.mape.iloc[0] if frow.mape.notna().any() else None
            st.caption(f"Backtest accuracy for this series: "
                       f"{acc_col} {acc_val:.1f}% · RMSE "
                       f"${float(frow.rmse.iloc[0]):,.0f} (also the CI sigma)"
                       if acc_val is not None else
                       "No backtest yet — accuracy columns are NULL until "
                       "**Run backtest** has been executed.")

        # ---- cash runway: cumulative projected cash position ----------------
        # Cash proxy = cumulative net cash flow over ALL ledger history (the
        # warehouse holds no opening bank balance). The projection adds the
        # forecast net_cash_flow months (CI band cumulated alongside).
        cash_hist = hist[["month_start", "net_cash_flow"]].dropna()
        cash_fc = fc[fc.series == "net_cash_flow"].sort_values("horizon_months") \
            if not fc.empty else pd.DataFrame()
        if not cash_hist.empty and not cash_fc.empty:
            cash0 = float(cash_hist.net_cash_flow.cumsum().iloc[-1])
            run_x = cash_hist.month_start.tolist() + cash_fc.period_start.tolist()
            run_y = cash_hist.net_cash_flow.cumsum().tolist()
            proj = [cash0]
            for pred in cash_fc.predicted:
                proj.append(proj[-1] + float(pred))
            low = [cash0]
            high = [cash0]
            for lo, hi in zip(cash_fc.ci_low, cash_fc.ci_high):
                low.append(low[-1] + float(lo))
                high.append(high[-1] + float(hi))
            run_y = run_y + proj[1:]

            rfig = go.Figure()
            rfig.add_scatter(x=run_x, y=run_y, name="Cumulative net cash",
                             mode="lines", line={"width": 3, "color": "#6A1B9A"})
            # CI ribbon: out along the upper cumulative bound, back along the
            # lower one (self-closed polygon).
            ribbon_x = ([cash_hist.month_start.iloc[-1]]
                        + cash_fc.period_start.tolist())
            rfig.add_scatter(
                x=ribbon_x + ribbon_x[::-1],
                y=([cash0] + high[1:]) + ([cash0] + low[1:])[::-1],
                name="Projected 90% CI", fill="toself",
                mode="lines", line={"width": 0},
                fillcolor="rgba(150,150,150,0.25)",
                hoverinfo="skip")
            rfig.add_hline(y=0, line_dash="dot", line_color="#9E9E9E")
            rfig.update_layout(height=340, yaxis_title="$ (cumulative)",
                               title="Cash runway — cumulative net cash flow "
                                     "(cash proxy: no opening balance in the "
                                     "warehouse)",
                               legend_orientation="h")
            st.plotly_chart(rfig, width='stretch')
            end_lo, end_hi = low[-1], high[-1]
            st.caption(f"Cash position today (proxy): ${cash0:,.0f}. In 3 "
                       f"months: ${proj[-1]:,.0f} projected "
                       f"(90% CI ${end_lo:,.0f} .. ${end_hi:,.0f}).")

        # ---- backtest accuracy table (GLOBAL model audit trail) ------------
        bt_df = qdf("""
            SELECT series, model, fold, train_start::date AS train_start,
                   test_start::date AS test_start, test_end::date AS test_end,
                   mape, rmse
              FROM backtest_results ORDER BY series, fold
        """)
        st.subheader("Backtest accuracy (walk-forward)")
        st.caption("Market/model data — shared across client workspaces: "
                   "backtest_results is the GLOBAL model audit trail (no "
                   "company_id by design); each workspace's forecasts carry "
                   "their own pooled accuracy columns.")
        if bt_df.empty:
            st.info("No backtest runs stored yet.")
        else:
            bt_df["metric"] = bt_df.series.map(
                lambda s: "WAPE" if s == "net_cash_flow" else "MAPE")
            st.dataframe(bt_df, width='stretch', hide_index=True)
            st.caption("MAPE = mean |error|/|actual| over non-zero-actual test "
                       "months (positive series). net_cash_flow uses WAPE = "
                       "SUM|error| / SUM|actual| because cash months can be "
                       "negative, which makes MAPE undefined. One fold = train "
                       "window -> held-out test window (no look-ahead; macro "
                       "drift data is read as of the fold's train end).")
    except Exception as exc:                        # noqa: BLE001
        st.error(f"Forecast tab unavailable: {exc}")


# -- SoCal Capital & Deals tab ---------------------------------------------------
# REAL harvested capital events for the six SoCal counties (funding_schema.sql,
# loaded by harvest_socal_capital.py): SEC Form D private placements + SBA
# SBIR/STTR R&D awards. One row per verified event, each with its EDGAR/SBIR
# source URL in raw_metadata/source_url. Filter values only ever come from the
# data itself (selectboxes built with DISTINCT queries) and travel as bound
# %s parameters via qdf_params -- nothing is string-formatted into SQL.
with tab_capital:
    st.subheader("SoCal Capital & Deals")
    st.caption("Market data — shared across client workspaces. REAL capital "
               "events for the six SoCal counties — SEC Form D "
               "private placements + SBA SBIR/STTR R&D awards "
               "(harvest_socal_capital.py → funding_schema.sql). One row per "
               "verified event; provenance per row via the filing id and "
               "source URL.")
    try:
        county_opts = ["All counties"] + qdf(
            "SELECT DISTINCT county FROM socal_capital_events "
            "ORDER BY county")["county"].tolist()
        sector_opts = ["All sectors"] + qdf(
            "SELECT DISTINCT sector FROM socal_capital_events "
            "ORDER BY sector")["sector"].tolist()

        f_county, f_sector, f_source = st.columns(3)
        sel_county = f_county.selectbox("County", county_opts,
                                        key="cap_county")
        sel_sector = f_sector.selectbox("Sector", sector_opts,
                                        key="cap_sector")
        sel_source = f_source.radio(
            "Funding source", list(CAPITAL_SOURCE_LABELS), horizontal=True,
            key="cap_source",
            help="Form D = SEC private-placement raise; SBIR-STTR = federal "
                 "R&D awards (Phase I / Phase II / STTR).")

        where = ["TRUE"]
        params = []
        if sel_county != "All counties":
            where.append("county = %s")
            params.append(sel_county)
        if sel_sector != "All sectors":
            where.append("sector = %s")
            params.append(sel_sector)
        if CAPITAL_SOURCE_LABELS[sel_source]:
            where.append("event_type = ANY(%s)")
            params.append(list(CAPITAL_SOURCE_LABELS[sel_source]))
        events = qdf_params(f"""
            SELECT offering_date, company_name, city, county, sector,
                   event_type, amount_raised, agency_or_filing_id, source_url
              FROM socal_capital_events
             WHERE {' AND '.join(where)}
             ORDER BY offering_date DESC
        """, tuple(params))
        events["amount_raised"] = pd.to_numeric(events["amount_raised"])
        events["offering_date"] = pd.to_datetime(events["offering_date"])

        n_total = int(qdf("SELECT COUNT(*) AS n FROM socal_capital_events")
                      .iloc[0].n)
        if n_total == 0:
            st.info(SOCAL_CAPITAL_HINT)
        elif events.empty:
            st.info("No events match the current filters.")
        else:
            total_cap = float(events.amount_raised.sum())
            med_deal = float(events.amount_raised.median())
            k1, k2, k3 = st.columns(3)
            k1.metric("Total capital raised (filtered)",
                      f"${total_cap:,.0f}")
            k2.metric("Events", f"{len(events):,}")
            k3.metric("Latest event date",
                      events.offering_date.max().date().isoformat())
            st.caption(f"Median deal size in the filtered set: "
                       f"${med_deal:,.0f}. A handful of mega-fund Form D "
                       f"filings can dominate the total — use the median "
                       f"for a feel of the typical deal.")

            cc1, cc2 = st.columns(2)
            with cc1:
                st.markdown("**Capital raised by county**")
                by_county = (events.groupby("county", as_index=False)
                                   .amount_raised.sum()
                                   .sort_values("amount_raised"))
                fig = go.Figure(go.Bar(
                    x=by_county.amount_raised, y=by_county.county,
                    orientation="h", marker_color="#1565C0",
                    text=by_county.amount_raised.map("${:,.0f}".format),
                    textposition="outside"))
                fig.update_layout(height=320, xaxis_title="$ raised",
                                  yaxis={"autorange": "reversed"},
                                  margin=dict(l=10, r=10, t=24, b=10))
                st.plotly_chart(fig, width='stretch')
            with cc2:
                st.markdown("**Capital raised by sector**")
                by_sector = (events.groupby("sector", as_index=False)
                                    .amount_raised.sum()
                                    .sort_values("amount_raised",
                                                 ascending=False))
                fig = go.Figure(go.Bar(
                    x=by_sector.sector, y=by_sector.amount_raised,
                    marker_color="#2E7D32",
                    text=by_sector.amount_raised.map("${:,.0f}".format),
                    textposition="outside",
                    customdata=by_sector.sector.map(
                        lambda s: f"{int((events.sector == s).sum()):,} events"),
                    hovertemplate="%{x}<br>%{y:$,.0f} · %{customdata}"
                                  "<extra></extra>"))
                fig.update_layout(height=320, yaxis_title="$ raised",
                                  margin=dict(l=10, r=10, t=24, b=10))
                st.plotly_chart(fig, width='stretch')

            st.markdown("**Monthly capital raised**")
            monthly = (events.assign(
                            month=events.offering_date.dt.to_period("M")
                                          .dt.to_timestamp())
                            .groupby("month", as_index=False)
                            .amount_raised.sum()
                            .sort_values("month"))
            fig = go.Figure(go.Scatter(
                x=monthly.month, y=monthly.amount_raised,
                mode="lines+markers", fill="tozeroy",
                line={"width": 3, "color": "#6A1B9A"},
                fillcolor="rgba(106,27,154,0.15)"))
            fig.update_layout(height=340, yaxis_title="$ raised / month",
                              margin=dict(l=10, r=10, t=24, b=10))
            st.plotly_chart(fig, width='stretch')

            st.markdown("**Events (most recent first)**")
            show = events.head(500).copy()
            show["event_type"] = show.event_type.map(
                lambda t: CAPITAL_EVENT_TYPE_LABELS.get(t, t))
            show["amount_raised"] = show.amount_raised.map("${:,.0f}".format)
            show["offering_date"] = show.offering_date.dt.date
            st.dataframe(
                show.rename(columns={
                    "offering_date": "date", "company_name": "company",
                    "city": "city", "county": "county", "sector": "sector",
                    "event_type": "source / type",
                    "amount_raised": "amount raised",
                    "agency_or_filing_id": "filing / award id",
                    "source_url": "source"}),
                width='stretch', hide_index=True)
            if len(events) > 500:
                st.caption(f"Showing the 500 most recent of "
                           f"{len(events):,} filtered event(s).")
    except Exception as exc:                        # noqa: BLE001
        st.error(f"SoCal Capital tab unavailable: {exc}")


# -- Public Peer Financials tab ---------------------------------------------------
# REAL SEC EDGAR XBRL financials for 50 public peers, FY2021-FY2025, including
# 20 Southern California public companies (peer_ratios_schema.sql, loaded by
# harvest_sec_peer_ratios.py). public_peer_financials holds the company-period
# facts; mv_vertical_financial_quartiles holds the SoCal-vs-National p25 /
# median / p75 benchmarks per (vertical, fiscal_year). Metric names in the
# selectbox come from PEER_METRIC_LABELS (a fixed, whitelisted vocabulary that
# matches the matview columns); vertical/year values come from the data.
with tab_peers:
    st.subheader("Public Peer Financials")
    st.caption("Market data — shared across client workspaces. "
               "REAL SEC EDGAR XBRL financials for 50 public peers "
               "(FY2021-FY2025, annual + quarterly periods), including 20 "
               "Southern California public companies "
               "(harvest_sec_peer_ratios.py → peer_ratios_schema.sql). "
               "Compare a vertical's SoCal cohort against the national peer "
               "set, then drill into the company-level table.")
    try:
        verticals = qdf("SELECT DISTINCT vertical FROM public_peer_financials "
                        "ORDER BY vertical")["vertical"].tolist()
        if not verticals:
            st.info(PEER_RATIOS_HINT)
        else:
            p_vert, p_metric, p_year = st.columns(3)
            sel_vertical = p_vert.selectbox("Peer vertical", verticals,
                                            key="peer_vertical")
            sel_metric = p_metric.selectbox(
                "Ratio metric", list(PEER_METRIC_LABELS),
                format_func=PEER_METRIC_LABELS.get, key="peer_metric")
            years = qdf_params(
                "SELECT DISTINCT fiscal_year FROM "
                "mv_vertical_financial_quartiles WHERE vertical = %s "
                "ORDER BY fiscal_year DESC",
                (sel_vertical,))["fiscal_year"].tolist()
            if not years:
                # MV not refreshed / empty: fall back to the fact table years
                years = qdf_params(
                    "SELECT DISTINCT fiscal_year FROM public_peer_financials "
                    "WHERE vertical = %s ORDER BY fiscal_year DESC",
                    (sel_vertical,))["fiscal_year"].tolist()
            sel_year = (p_year.selectbox("Fiscal year", years,
                                         key="peer_year")
                        if years else None)

            st.markdown(
                f"**SoCal vs National quartiles — "
                f"{PEER_METRIC_LABELS[sel_metric]}, {sel_vertical}**")
            if sel_year is None:
                st.info(PEER_RATIOS_HINT)
            else:
                mv_prefix = PEER_METRIC_MV_PREFIX[sel_metric]
                quart = qdf_params(f"""
                    SELECT peer_group, n_records,
                           {mv_prefix}_p25, {mv_prefix}_median,
                           {mv_prefix}_p75
                      FROM mv_vertical_financial_quartiles
                     WHERE vertical = %s AND fiscal_year = %s
                """, (sel_vertical, int(sel_year)))
                if quart.empty:
                    st.info(f"No quartile benchmark rows for {sel_vertical} "
                            f"in FY{sel_year} (refresh the matview after "
                            "loading data).")
                else:
                    band_x = ["p25", "median", "p75"]
                    fig = go.Figure()
                    ns = {}
                    for group, color in (("SoCal", "#C62828"),
                                         ("National", "#1565C0")):
                        row = quart[quart.peer_group == group]
                        if row.empty:
                            continue
                        r = row.iloc[0]
                        ns[group] = int(r.n_records)
                        vals = pd.to_numeric(pd.Series(
                            [r[f"{mv_prefix}_p25"],
                             r[f"{mv_prefix}_median"],
                             r[f"{mv_prefix}_p75"]]))
                        fig.add_bar(
                            name=group, x=band_x, y=vals,
                            marker_color=color,
                            text=vals.map(
                                lambda v: "" if pd.isna(v)
                                else f"{v:.1f}"),
                            textposition="outside",
                            hovertemplate=f"{group} · %{{x}}: "
                                          "%{y:.2f}%<extra></extra>")
                    fig.update_layout(
                        barmode="group", height=380,
                        yaxis_title=PEER_METRIC_LABELS[sel_metric],
                        xaxis_title="quartile band",
                        legend_orientation="h",
                        margin=dict(l=10, r=10, t=24, b=10))
                    if not fig.data:
                        st.info("This vertical has no SoCal/National "
                                "quartile rows yet.")
                    else:
                        st.plotly_chart(fig, width='stretch')
                        st.caption(
                            f"FY{sel_year} · vertical '{sel_vertical}' · "
                            + " · ".join(f"{g}: n={n}" for g, n in ns.items())
                            + " — percentiles of "
                            f"{PEER_METRIC_LABELS[sel_metric].lower()} over "
                            "company-periods (NULL-free metrics only).")

            st.markdown("**Company-level financials — "
                        f"{sel_vertical}**")
            peers = qdf_params("""
                SELECT ticker, company_name, hq_state, is_socal, fiscal_year,
                       fiscal_period, revenue, gross_profit, rd_expense,
                       sga_expense, operating_income, net_income,
                       gross_margin_pct, operating_margin_pct,
                       rd_pct_of_rev, sga_pct_of_rev, source_form, source_url
                  FROM public_peer_financials
                 WHERE vertical = %s
                 ORDER BY fiscal_year DESC, ticker, fiscal_period
            """, (sel_vertical,))
            if peers.empty:
                st.info(PEER_RATIOS_HINT)
            else:
                for col in ("revenue", "gross_profit", "rd_expense",
                            "sga_expense", "operating_income", "net_income"):
                    peers[col] = pd.to_numeric(peers[col],
                                               errors="coerce").round(0)
                for col in ("gross_margin_pct", "operating_margin_pct",
                            "rd_pct_of_rev", "sga_pct_of_rev"):
                    peers[col] = pd.to_numeric(peers[col],
                                               errors="coerce").round(2)
                n_socal_cos = int(qdf_params(
                    "SELECT COUNT(DISTINCT ticker) AS n FROM "
                    "public_peer_financials WHERE vertical = %s AND is_socal",
                    (sel_vertical,)).iloc[0].n)
                n_all_cos = int(qdf_params(
                    "SELECT COUNT(DISTINCT ticker) AS n FROM "
                    "public_peer_financials WHERE vertical = %s",
                    (sel_vertical,)).iloc[0].n)
                st.caption(f"{n_all_cos} public peers in this vertical "
                           f"({n_socal_cos} SoCal-headquartered) · "
                           f"{len(peers):,} company-period rows · "
                           "FY = annual 10-K period, Q1-Q4 = fiscal "
                           "quarters. Click a column header to sort.")
                st.dataframe(
                    peers.rename(columns={
                        "ticker": "ticker", "company_name": "company",
                        "hq_state": "HQ state",
                        "is_socal": "SoCal", "fiscal_year": "FY",
                        "fiscal_period": "period", "revenue": "revenue",
                        "gross_profit": "gross profit",
                        "rd_expense": "R&D", "sga_expense": "SG&A",
                        "operating_income": "operating income",
                        "net_income": "net income",
                        "gross_margin_pct": "gross margin %",
                        "operating_margin_pct": "operating margin %",
                        "rd_pct_of_rev": "R&D % rev",
                        "sga_pct_of_rev": "SG&A % rev",
                        "source_form": "form", "source_url": "source"}),
                    width='stretch', hide_index=True)

                # Scatter: revenue vs the selected ratio, SoCal highlighted
                # (public_peer_financials.is_socal flags SoCal-headquartered
                # filers). Log revenue axis — peers span six orders of
                # magnitude.
                st.markdown(
                    f"**Revenue vs {PEER_METRIC_LABELS[sel_metric].lower()} "
                    "(SoCal highlighted)**")
                scat = peers[(peers.revenue > 0)
                             & peers[sel_metric].notna()].copy()
                if scat.empty:
                    st.info("No plotable rows for this metric (all NULL or "
                            "zero-revenue periods).")
                else:
                    fig = go.Figure()
                    for flag, color, name in ((True, "#C62828",
                                               "SoCal public cos"),
                                              (False, "#1565C0",
                                               "Other US / international")):
                        seg = scat[scat.is_socal == flag]
                        if seg.empty:
                            continue
                        fig.add_scatter(
                            x=seg.revenue, y=seg[sel_metric], mode="markers",
                            name=name,
                            marker={"size": 7, "color": color,
                                    "opacity": 0.75},
                            customdata=seg[["ticker", "company_name",
                                            "fiscal_year",
                                            "fiscal_period"]].values,
                            hovertemplate="%{customdata[0]} "
                                          "(%{customdata[1]})<br>"
                                          "FY%{customdata[2]} "
                                          "%{customdata[3]}<br>"
                                          "Revenue $%{x:,.0f} · "
                                          f"{PEER_METRIC_LABELS[sel_metric]} "
                                          "%{y:.1f}%<extra></extra>")
                    fig.update_layout(
                        height=400, xaxis_type="log",
                        xaxis_title="Revenue (USD, log scale)",
                        yaxis_title=PEER_METRIC_LABELS[sel_metric],
                        legend_orientation="h",
                        margin=dict(l=10, r=10, t=24, b=10))
                    st.plotly_chart(fig, width='stretch')
                    st.caption("Each point is one company-period. "
                               "Red = EDGAR business address in a SoCal "
                               "county (is_socal). Log revenue axis because "
                               "the peer set spans six orders of magnitude.")
    except Exception as exc:                        # noqa: BLE001
        st.error(f"Public Peer Financials tab unavailable: {exc}")


def _nl_auto_chart(df: pd.DataFrame):
    """Pick a simple Plotly chart for an arbitrary query result, or None.

    Line chart when there is >= 1 date-like column and >= 1 numeric column
    (x = the first date-like column, one trace per numeric column); else a
    bar chart when there is >= 1 text/category column and 1-2 numeric
    columns. Skipped for <= 1 or > 200 rows. Column types are detected with
    pandas on the returned rows (numeric tried before date-like).
    """
    if df.empty or len(df) < 2 or len(df) > 200:
        return None
    date_cols, num_cols, text_cols = [], [], []
    for col in df.columns:
        if df[col].dropna().empty:
            continue
        if pd.to_numeric(df[col], errors="coerce").notna().mean() >= 0.9:
            num_cols.append(col)
        elif pd.to_datetime(df[col], errors="coerce").notna().mean() >= 0.9:
            date_cols.append(col)
        else:
            text_cols.append(col)
    fig = go.Figure()
    if date_cols and num_cols:
        x = pd.to_datetime(df[date_cols[0]], errors="coerce")
        for c in num_cols:
            fig.add_scatter(x=x, y=pd.to_numeric(df[c], errors="coerce"),
                            name=str(c), mode="lines+markers")
        fig.update_layout(height=380, legend_orientation="h",
                          xaxis_title=str(date_cols[0]))
        return fig
    if text_cols and 1 <= len(num_cols) <= 2:
        x = df[text_cols[0]].astype(str)
        for c in num_cols[:2]:
            fig.add_bar(x=x, y=pd.to_numeric(df[c], errors="coerce"),
                        name=str(c))
        fig.update_layout(height=380, legend_orientation="h",
                          xaxis_title=str(text_cols[0]))
        return fig
    return None


# -- Ask Your Data tab ----------------------------------------------------------
# Plain-English questions translated into STRICTLY READ-ONLY SQL by an LLM
# (nl_to_sql.py). Four guardrail layers protect the warehouse:
#   1. parser-level validate_sql(): single statement, no comments, forbidden
#      keyword scan, FROM/JOIN allowlist, auto LIMIT;
#   2. a read-only transaction (the connection is opened READ ONLY);
#   3. a statement_timeout (default 5 s) on the DB side;
#   4. DATABASE-ENFORCED TENANCY: nl_to_sql connects as the restricted
#      nl2sql_reader role (migrations/002_add_rls.sql: NOSUPERUSER,
#      NOBYPASSRLS, SELECT grants only) and sets
#      app.current_company_id = <the ACTIVE workspace> for the session, so
#      row-level security filters every client-data row to THIS workspace --
#      even when the generated SQL has no WHERE company_id clause. The role
#      reads the SMB matviews only through the company-scoped scoped_* views
#      (the raw mv_* views are permission-denied for it); market/SoCal/peer
#      data is global.
# Nothing runs unless validate_sql() passed, and this tab never touches
# qdf()/exec_sql(): nl_to_sql opens its own read-only connection, so no
# dashboard code path can write through here.
STARTER_QUESTIONS = [
    "Which service line has the best gross margin?",
    "Which months lost money?",
    "Who are my whale clients?",
    "How did our material costs trend vs the construction-materials PPI?",
    "Top 10 customers by revenue",
    "Show all open anomaly alerts with severity",
]

with tab_ask:
    st.subheader("Ask Your Data")
    st.caption(f"Active workspace: **{active_company_name}** (company_id "
               f"{active_company_id}). Ask a business question in plain "
               "English; the AI turns it into one read-only SQL query. The "
               "question is suffixed with a workspace-context line so "
               "client-data answers filter by this company_id (market/"
               "benchmark questions may ignore it) — and the query itself "
               "runs under row-level security for this workspace, so rows "
               "from other clients are invisible at the database level "
               "even without a WHERE clause. Inspect or edit the SQL "
               "before running it — the guardrails still apply to edited "
               "SQL.")
    try:
        cfg = nl_to_sql.load_config()

        with st.expander("AI provider settings"):
            preset = st.selectbox(
                "Provider preset", list(nl_to_sql.PRESETS), key="nl_preset",
                help="Choosing a preset fills the base URL and model fields "
                     "(Custom leaves them to you).")
            # Sync the base URL / model fields when the preset changes, and
            # prefill them from the saved .env on first load.
            if "nl_preset_prev" not in st.session_state:
                st.session_state["nl_preset_prev"] = preset
                st.session_state["nl_base_url"] = (
                    cfg["base_url"] or nl_to_sql.PRESETS[preset]["base_url"])
                st.session_state["nl_model"] = (
                    cfg["model"] or nl_to_sql.PRESETS[preset]["model"])
            elif st.session_state["nl_preset_prev"] != preset:
                st.session_state["nl_preset_prev"] = preset
                if preset != "Custom":
                    st.session_state["nl_base_url"] = \
                        nl_to_sql.PRESETS[preset]["base_url"]
                    st.session_state["nl_model"] = \
                        nl_to_sql.PRESETS[preset]["model"]
            base_url = st.text_input("API base URL", key="nl_base_url")
            model = st.text_input("Model", key="nl_model")
            api_key = st.text_input("API key", value=cfg["api_key"],
                                    type="password",
                                    help="Stored only in the local .env file "
                                         "in this project folder.")
            if st.button("Save settings to .env"):
                saved_path = nl_to_sql.save_config(
                    {"base_url": base_url, "model": model,
                     "api_key": api_key, "llm_timeout": cfg["llm_timeout"],
                     "query_timeout": cfg["query_timeout"]})
                st.success(f"Settings saved to {saved_path.name} in the "
                           f"project folder.")
            st.caption("The API key stays in the local .env file; requests go "
                       "only to the endpoint chosen above.")

        live_cfg = {"base_url": base_url, "model": model, "api_key": api_key,
                    "llm_timeout": cfg["llm_timeout"],
                    "query_timeout": cfg["query_timeout"]}
        if not api_key.strip():
            st.info("No API key yet — open **AI provider settings** above, "
                    "paste your key (e.g. a Zhipu/GLM key from the console) "
                    "and click **Save settings to .env**.")

        st.markdown("**Starter questions**")
        srow1, srow2 = st.columns(3), st.columns(3)
        cells = [srow1[0], srow1[1], srow1[2], srow2[0], srow2[1], srow2[2]]
        for i, starter in enumerate(STARTER_QUESTIONS):
            with cells[i]:
                if st.button(starter, key=f"nl_starter_{i}", width='stretch'):
                    st.session_state["nl_question"] = starter

        question = st.text_area(
            "Your question", height=80, key="nl_question",
            placeholder="e.g. Which service line has the best gross margin?")

        if st.button("Generate SQL", type="primary", width='stretch',
                     disabled=not api_key.strip()
                     or not (question or "").strip()):
            # Multi-tenant context: the workspace line is APPENDED to the
            # user message (the system prompt already documents the
            # companies/company_id model), so client-data questions come
            # back scoped to the active workspace.
            question_with_context = (
                f"{question.strip()}\n\n"
                f"[Workspace context: the active client workspace is "
                f"'{active_company_name}' (company_id = {active_company_id}). "
                f"All client data questions should filter company_id = "
                f"{active_company_id} unless the question is about market "
                f"data.]")
            try:
                with st.spinner("Asking the model ..."):
                    sql, attempts, note = nl_to_sql.generate_sql(
                        question_with_context, live_cfg)
                st.session_state["nl_sql"] = sql or ""
                st.session_state["nl_attempts"] = attempts
                st.session_state["nl_note"] = note
                st.session_state["nl_note_error"] = sql is None
            except Exception as exc:                # noqa: BLE001
                st.session_state["nl_sql"] = ""
                st.session_state["nl_note"] = f"LLM error: {exc}"
                st.session_state["nl_note_error"] = True

        gen_note = st.session_state.get("nl_note")
        if gen_note:
            if st.session_state.get("nl_note_error"):
                st.error(gen_note)
            else:
                st.caption(gen_note)

        with st.expander("SQL preview — inspect or edit before running"):
            st.text_area("SQL", height=150, key="nl_sql")

        sql_to_run = (st.session_state.get("nl_sql") or "").strip()
        if st.button("Run query", width='stretch', disabled=not sql_to_run):
            ok, verdict = nl_to_sql.validate_sql(sql_to_run)
            if not ok:
                st.error(f"Guardrails rejected this SQL — not running it. "
                         f"{verdict}")
            else:
                with st.spinner("Running the read-only query ..."):
                    res = nl_to_sql.run_readonly_query(
                        verdict, query_timeout=cfg["query_timeout"],
                        company_id=active_company_id)
                if res.get("error"):
                    st.error(res["error"])
                    st.caption("The query ran (or was stopped) inside a "
                               "read-only transaction; nothing in the "
                               "warehouse changed.")
                else:
                    st.caption(f"Results for client workspace "
                               f"**{active_company_name}** — results are "
                               f"database-enforced to the active workspace "
                               f"via row-level security (the AI query runs "
                               f"as the restricted nl2sql_reader role with "
                               f"company_id = {active_company_id} set at the "
                               f"session level, so rows from other "
                               f"workspaces are invisible even if the SQL "
                               f"above omits the company_id filter).")
                    df = pd.DataFrame(res["rows"], columns=res["columns"])
                    cap = (f"{res['rowcount']} row(s) · {res['elapsed_ms']} ms "
                           f"· read-only transaction · row-level security "
                           f"(active workspace)")
                    if res.get("truncated"):
                        cap += " · capped at the first 500 rows"
                    st.caption(cap)
                    st.dataframe(df, width='stretch', hide_index=True)
                    fig = _nl_auto_chart(df)
                    if fig is not None:
                        st.plotly_chart(fig, width='stretch')
    except Exception as exc:                        # noqa: BLE001
        st.error(f"Ask Your Data unavailable: {exc}")
