#!/usr/bin/env python3
"""
generate_pdf_brief.py -- board-ready Executive Financial Brief (PDF) for ONE
client workspace of the SMB Financial & Operational Data Warehouse.

Pure-Python module built on reportlab platypus (SimpleDocTemplate, Table,
Paragraph) -- no external chart images, no non-stdlib dependencies beyond
reportlab itself. Reads ONLY warehouse aggregates (the multi-tenant
materialized views + anomaly_alerts + forecasts, all scoped by company_id);
it never writes to the database.

PDF shape (2-3 pages, A4):

  Page 1
    * header block   -- company name, "Executive Financial Brief",
                        reporting period (last N months), generated date
    * executive summary KPIs -- trailing-12-month revenue, gross profit +
                        margin, open anomaly count, whale-client count,
                        next-3-month revenue forecast (sum of the
                        seasonal_trend_v1 point forecasts in scope)
    * monthly P&L table (last N months from mv_monthly_summary for the
      company: month, revenue, total_expense, gross_profit, margin_pct,
      completions)

  Page 2
    * service-line profitability (mv_job_type_profitability for the company:
      type, revenue, margin_pct, jobs, vs-benchmark)
    * top open anomalies (severity, subject, metric vs baseline -- up to 8)
    * forecast outlook (the next 3 monthly revenue forecasts with their 90%
      confidence intervals)

  Footer on every page: generated-by line + timestamp.

A company with NO data never crashes the generator: every section degrades
to an informative "no data yet" placeholder row/paragraph.

API:
    generate_pdf_brief(company_id, conn=None, months=12) -> bytes
        Pass your own psycopg2 connection (it is used read-only and NOT
        closed), or None to let the module open (and close) its own.

CLI:
    python3 generate_pdf_brief.py --company-id 1 --out brief.pdf
    python3 generate_pdf_brief.py --company-id 1 --months 6 --out brief.pdf
"""

import argparse
import datetime as dt
import io
import os

import psycopg2
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase.pdfmetrics import registerFontFamily
from reportlab.platypus import (Paragraph, SimpleDocTemplate, Spacer, Table,
                                TableStyle)

# Honors DATABASE_URL (used by the Docker stack: postgresql://postgres:postgres@db:5432/...)
DB = os.environ.get("DATABASE_URL",
                    "postgresql://postgres:postgres@localhost:5432/smb_warehouse")

GENERATED_BY = ("SMB Financial & Operational Data Warehouse -- "
                "generate_pdf_brief.py (multi-client Fractional CFO edition)")

PAGE_SIZE = A4
MARGIN = 18 * mm

# reportlab table palette (kept deliberately muted / board-ready)
HEADER_BG = colors.HexColor("#1F3B57")
ROW_ALT_BG = colors.HexColor("#F2F5F8")
GRID = colors.HexColor("#C9D2DA")
ACCENT = colors.HexColor("#B3401E")


def _q(cur, sql, params=None):
    cur.execute(sql, params or ())
    cols = [d[0] for d in cur.description] if cur.description else []
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _money(v):
    """Warehouse NUMERIC -> '$1,234' (None/zero-safe)."""
    try:
        return f"${float(v):,.0f}"
    except (TypeError, ValueError):
        return "n/a"


def _pct(v):
    try:
        return f"{float(v):,.1f}%"
    except (TypeError, ValueError):
        return "n/a"


# ---------------------------------------------------------------------------
# data assembly (all queries are company-scoped)
# ---------------------------------------------------------------------------
def _company_row(conn, company_id):
    with conn.cursor() as cur:
        rows = _q(cur, """
            SELECT company_id, company_name, industry_vertical, socal_region,
                   created_at
              FROM companies
             WHERE company_id = %s
        """, (company_id,))
    return rows[0] if rows else None


def _monthly_pnl(conn, company_id, months):
    """Last `months` rows of the workspace's cash P&L (mv_monthly_summary)."""
    with conn.cursor() as cur:
        return _q(cur, """
            SELECT month_start, revenue, total_expense, gross_profit,
                   gross_margin_pct, job_completions
              FROM mv_monthly_summary
             WHERE company_id = %s
             ORDER BY month_start DESC
             LIMIT %s
        """, (company_id, months))


def _exec_kpis(conn, company_id):
    """TTM revenue / gross profit, open anomaly count, whale count and the
    next-3-month revenue forecast total -- the executive summary numbers."""
    with conn.cursor() as cur:
        ttm = _q(cur, """
            SELECT COALESCE(SUM(revenue), 0)    AS revenue,
                   COALESCE(SUM(gross_profit), 0) AS gross_profit,
                   COUNT(*)                      AS months
              FROM (SELECT revenue, gross_profit
                      FROM mv_monthly_summary
                     WHERE company_id = %s
                     ORDER BY month_start DESC
                     LIMIT 12) t
        """, (company_id,))[0]
        cur.execute("SELECT COUNT(*) FROM anomaly_alerts"
                    " WHERE company_id = %s AND NOT is_resolved", (company_id,))
        open_alerts = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM mv_customer_profitability"
                    " WHERE company_id = %s AND is_whale", (company_id,))
        whales = cur.fetchone()[0]
        forecast = _q(cur, """
            SELECT period_start, predicted, ci_low, ci_high, horizon_months
              FROM forecasts
             WHERE company_id = %s
               AND series = 'revenue'
             ORDER BY horizon_months
             LIMIT 3
        """, (company_id,))
    return ttm, open_alerts, whales, forecast


def _service_lines(conn, company_id):
    with conn.cursor() as cur:
        return _q(cur, """
            SELECT job_type, revenue, margin_pct, job_count,
                   price_vs_benchmark_pct
              FROM mv_job_type_profitability
             WHERE company_id = %s
             ORDER BY revenue DESC
        """, (company_id,))


def _open_anomalies(conn, company_id, limit=8):
    """Top open alerts of the workspace, worst severity first."""
    with conn.cursor() as cur:
        return _q(cur, """
            SELECT a.severity, a.anomaly_type,
                   COALESCE(j.job_number,
                            a.category || ' - ' || TO_CHAR(a.period, 'Mon YYYY'))
                       AS subject,
                   a.metric_value, a.baseline_value
              FROM anomaly_alerts a
              LEFT JOIN jobs j ON j.id = a.job_id
             WHERE a.company_id = %s
               AND NOT a.is_resolved
             ORDER BY CASE a.severity WHEN 'high' THEN 1
                                      WHEN 'medium' THEN 2
                                      ELSE 3 END,
                      a.detected_at DESC
             LIMIT %s
        """, (company_id, limit))


# ---------------------------------------------------------------------------
# rendering
# ---------------------------------------------------------------------------
def _register_fonts():
    """Embed the Bitstream Vera TrueType faces bundled with reportlab
    (freely redistributable; license shipped in reportlab/fonts). Falls back
    to the built-in Helvetica family when the files are unavailable."""
    try:
        if "BriefVera" not in pdfmetrics.getRegisteredFontNames():
            font_dir = os.path.join(
                os.path.dirname(__import__("reportlab").__file__), "fonts")
            pdfmetrics.registerFont(TTFont(
                "BriefVera", os.path.join(font_dir, "Vera.ttf")))
            pdfmetrics.registerFont(TTFont(
                "BriefVera-Bold", os.path.join(font_dir, "VeraBd.ttf")))
            pdfmetrics.registerFont(TTFont(
                "BriefVera-Italic", os.path.join(font_dir, "VeraIt.ttf")))
            registerFontFamily("BriefVera", normal="BriefVera",
                               bold="BriefVera-Bold",
                               italic="BriefVera-Italic",
                               boldItalic="BriefVera-Bold")
        return "BriefVera"
    except Exception:                            # noqa: BLE001 - keep working
        return "Helvetica"


BODY_FONT = None   # resolved once by _styles()


def _styles():
    global BODY_FONT
    if BODY_FONT is None:
        BODY_FONT = _register_fonts()
    ss = getSampleStyleSheet()
    ss.add(ParagraphStyle(
        "BriefTitle", parent=ss["Title"], fontSize=17, leading=21,
        fontName=BODY_FONT, textColor=HEADER_BG, spaceAfter=2))
    ss.add(ParagraphStyle(
        "BriefSub", parent=ss["Normal"], fontSize=9.5, leading=13,
        fontName=BODY_FONT, textColor=colors.HexColor("#5A6B7B")))
    ss.add(ParagraphStyle(
        "SectionHead", parent=ss["Heading2"], fontSize=11.5, leading=14,
        fontName=BODY_FONT, textColor=HEADER_BG, spaceBefore=10,
        spaceAfter=4))
    ss.add(ParagraphStyle(
        "NoData", parent=ss["Normal"], fontSize=9, leading=12,
        fontName=BODY_FONT, textColor=colors.HexColor("#7A6A5A")))
    ss.add(ParagraphStyle(
        "Cell", parent=ss["Normal"], fontSize=8, leading=10,
        fontName=BODY_FONT))
    ss.add(ParagraphStyle(
        "CellHead", parent=ss["Normal"], fontSize=8, leading=10,
        fontName=BODY_FONT, textColor=colors.white))
    return ss


def _table(headers, rows, widths, aligns=None):
    """Build a platypus Table with the shared brief styling. Rows are lists
    of already-formatted strings."""
    data = [[Paragraph(str(h), _STYLES["CellHead"]) for h in headers]]
    for r in rows:
        data.append([Paragraph(str(c), _STYLES["Cell"]) for c in r])
    tbl = Table(data, colWidths=widths, repeatRows=1, hAlign="LEFT")
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
        ("GRID", (0, 0), (-1, -1), 0.4, GRID),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 2.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
    ]
    for i in range(1, len(data)):
        if i % 2 == 0:
            style.append(("BACKGROUND", (0, i), (-1, i), ROW_ALT_BG))
    if aligns:
        for col, al in enumerate(aligns):
            style.append(("ALIGN", (col, 0), (col, -1), al))
    tbl.setStyle(TableStyle(style))
    return tbl


_STYLES = None     # initialized in generate_pdf_brief() (reportlab styles)


def _footer(canvas, doc):
    """Shared page footer: generated-by line + timestamp."""
    canvas.saveState()
    canvas.setFont(BODY_FONT or _register_fonts(), 7)
    canvas.setFillColor(colors.HexColor("#5A6B7B"))
    canvas.drawString(MARGIN, 10 * mm, GENERATED_BY)
    canvas.drawRightString(PAGE_SIZE[0] - MARGIN, 10 * mm,
                           f"Generated {dt.datetime.now():%Y-%m-%d %H:%M}")
    canvas.setStrokeColor(GRID)
    canvas.setLineWidth(0.4)
    canvas.line(MARGIN, 12 * mm, PAGE_SIZE[0] - MARGIN, 12 * mm)
    canvas.restoreState()


def generate_pdf_brief(company_id, conn=None, months=12):
    """Build the board-ready Executive Financial Brief for ONE client
    workspace and return the PDF as bytes.

    company_id : the workspace to report on (every query is scoped to it).
    conn       : optional psycopg2 connection (used read-only, never closed);
                 None opens a short-lived connection to DATABASE_URL/DB.
    months     : how many months of P&L history to include (default 12).

    A workspace with no data produces a valid PDF with informative "no data
    yet" placeholders instead of raising (an unknown company_id renders as
    'Company #<id>')."""
    global _STYLES
    owned = False
    if conn is None:
        conn = psycopg2.connect(DB)
        owned = True
    try:
        company = _company_row(conn, company_id)
        company_name = (company["company_name"] if company
                        else f"Company #{company_id}")
        monthly = _monthly_pnl(conn, company_id, months)
        ttm, open_alerts, whales, forecast = _exec_kpis(conn, company_id)
        lines = _service_lines(conn, company_id)
        alerts = _open_anomalies(conn, company_id)
    finally:
        if owned:
            conn.close()

    _STYLES = _styles()
    ss = _STYLES
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=PAGE_SIZE,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=14 * mm, bottomMargin=16 * mm,
        title=f"Executive Financial Brief - {company_name}",
        author=GENERATED_BY)
    story = []

    # ---------------------------------------------------------------- header
    period_desc = (f"{monthly[-1]['month_start']:%b %Y} - "
                   f"{monthly[0]['month_start']:%b %Y}"
                   if monthly else "no reporting period yet")
    meta_bits = []
    if company:
        if company.get("industry_vertical"):
            meta_bits.append(str(company["industry_vertical"]))
        if company.get("socal_region"):
            meta_bits.append(str(company["socal_region"]))
    meta = " · ".join(meta_bits) or "client workspace"

    story.append(Paragraph(company_name, ss["BriefTitle"]))
    story.append(Paragraph("Executive Financial Brief", ss["SectionHead"]))
    story.append(Paragraph(
        f"{meta} &nbsp;|&nbsp; Reporting period: {period_desc} &nbsp;|&nbsp; "
        f"Generated {dt.date.today():%Y-%m-%d}", ss["BriefSub"]))
    story.append(Spacer(1, 6))

    # -------------------------------------------------------- KPI summary
    ttm_revenue = float(ttm["revenue"] or 0)
    ttm_gp = float(ttm["gross_profit"] or 0)
    ttm_margin = (ttm_gp / ttm_revenue * 100) if ttm_revenue else None
    fc_total = sum(float(f["predicted"] or 0) for f in forecast)
    kpis = [
        ("TTM revenue", _money(ttm_revenue)),
        ("TTM gross profit", f"{_money(ttm_gp)}"
         + (f" ({_pct(ttm_margin)} margin)" if ttm_margin is not None else "")),
        ("Open anomaly alerts", str(open_alerts)),
        ("Whale clients", str(whales)),
        ("Next-3-mo revenue forecast", _money(fc_total) if forecast
         else "no forecast yet"),
    ]
    story.append(_table(["Executive summary", "Value"],
                        [(k, v) for k, v in kpis],
                        [70 * mm, 60 * mm], aligns=["LEFT", "RIGHT"]))
    story.append(Spacer(1, 4))

    # -------------------------------------------------------- monthly P&L
    story.append(Paragraph(f"Monthly P&L (last {months} months)",
                           ss["SectionHead"]))
    if monthly:
        monthly_rows = [(
            m["month_start"].strftime("%b %Y"),
            _money(m["revenue"]),
            _money(m["total_expense"]),
            _money(m["gross_profit"]),
            _pct(m["gross_margin_pct"]),
            str(m["job_completions"] or 0),
        ) for m in monthly]
        story.append(_table(
            ["Month", "Revenue", "Total expense", "Gross profit",
             "Margin", "Completions"],
            monthly_rows,
            [24 * mm, 28 * mm, 28 * mm, 28 * mm, 18 * mm, 20 * mm],
            aligns=["LEFT", "RIGHT", "RIGHT", "RIGHT", "RIGHT", "RIGHT"]))
        story.append(Spacer(1, 2))
        story.append(Paragraph(
            "Cash/ledger basis: every ledger row counts, overhead included "
            "(mv_monthly_summary, scoped to this workspace).",
            ss["BriefSub"]))
    else:
        story.append(Paragraph(
            "No monthly activity yet for this workspace -- load ledger or "
            "labor data (seed_demo.py / ingest_csv.py) and refresh the "
            "analytics to populate this section.", ss["NoData"]))

    # ------------------------------------------------- service-line P&L
    story.append(Paragraph("Service-line profitability", ss["SectionHead"]))
    if lines:
        line_rows = [(
            l["job_type"], _money(l["revenue"]), _pct(l["margin_pct"]),
            str(l["job_count"] or 0),
            (_pct(l["price_vs_benchmark_pct"])
             if l["price_vs_benchmark_pct"] is not None else "n/a"),
        ) for l in lines]
        story.append(_table(
            ["Service line", "Revenue", "Margin %", "Jobs", "vs benchmark"],
            line_rows,
            [46 * mm, 26 * mm, 20 * mm, 16 * mm, 26 * mm],
            aligns=["LEFT", "RIGHT", "RIGHT", "RIGHT", "RIGHT"]))
        story.append(Spacer(1, 2))
        story.append(Paragraph(
            "'vs benchmark' compares the workspace's average quote with the "
            "latest national market_benchmarks row for the service line "
            "(market data is global / shared across workspaces).",
            ss["BriefSub"]))
    else:
        story.append(Paragraph(
            "No service-line activity yet for this workspace.",
            ss["NoData"]))

    # ------------------------------------------------- open anomalies
    story.append(Paragraph(f"Top open anomalies (up to 8)", ss["SectionHead"]))
    if alerts:
        alert_rows = [(
            a["severity"], a["subject"], _money(a["metric_value"]),
            _money(a["baseline_value"]),
        ) for a in alerts]
        story.append(_table(
            ["Severity", "Subject", "Metric", "Baseline"],
            alert_rows,
            [20 * mm, 62 * mm, 24 * mm, 24 * mm],
            aligns=["LEFT", "LEFT", "RIGHT", "RIGHT"]))
    else:
        story.append(Paragraph(
            "No open anomaly alerts for this workspace -- either the books "
            "are clean or detect_anomalies() has not run since the last "
            "data load.", ss["NoData"]))

    # ------------------------------------------------- forecast outlook
    story.append(Paragraph("Forecast outlook (revenue, next 3 months)",
                           ss["SectionHead"]))
    if forecast:
        fc_rows = [(
            f["period_start"].strftime("%b %Y"),
            f"h={f['horizon_months']}",
            _money(f["predicted"]),
            f"{_money(f['ci_low'])} - {_money(f['ci_high'])}",
        ) for f in forecast]
        story.append(_table(
            ["Month", "Horizon", "Predicted", "90% CI"],
            fc_rows,
            [26 * mm, 20 * mm, 30 * mm, 56 * mm],
            aligns=["LEFT", "LEFT", "RIGHT", "RIGHT"]))
        story.append(Spacer(1, 2))
        story.append(Paragraph(
            "seasonal_trend_v1 point forecasts with 90% confidence "
            "intervals (forecast_engine.py, scoped to this workspace). "
            "Positive-only months' lower bounds are clamped at $0.",
            ss["BriefSub"]))
    else:
        story.append(Paragraph(
            "No revenue forecast stored yet for this workspace -- run the "
            "forecast engine (dashboard button or "
            "`python3 forecast_engine.py forecast --company-id "
            f"{company_id}`).", ss["NoData"]))

    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(
        description="Board-ready Executive Financial Brief (PDF) for one "
                    "client workspace")
    ap.add_argument("--company-id", type=int, default=1, dest="company_id",
                    help="client workspace to report on (default 1 = the "
                         "demo workspace)")
    ap.add_argument("--months", type=int, default=12,
                    help="months of P&L history to include (default 12)")
    ap.add_argument("--out", default=None,
                    help="output PDF path (default: "
                         "<slug>_executive_brief_<YYYY-MM>.pdf)")
    args = ap.parse_args()

    pdf = generate_pdf_brief(args.company_id, months=args.months)
    out = args.out
    if not out:
        conn = psycopg2.connect(DB)
        try:
            company = _company_row(conn, args.company_id)
        finally:
            conn.close()
        name = company["company_name"] if company else f"company_{args.company_id}"
        slug = "".join(ch if ch.isalnum() else "_" for ch in name.lower())
        slug = "_".join(part for part in slug.split("_") if part) or "company"
        out = f"{slug}_executive_brief_{dt.date.today():%Y-%m}.pdf"
    with open(out, "wb") as fh:
        fh.write(pdf)
    print(f"Wrote {out} ({len(pdf):,} bytes) for company_id="
          f"{args.company_id} ({args.months} months of P&L).")


if __name__ == "__main__":
    main()
