"""Doc-to-Warehouse — Streamlit review UI.

Upload (or generate) PDFs, run the rule-based extraction pipeline
(pdf_parser -> categorizer -> export_warehouse_csv), review/edit every
transaction inline, and download warehouse-ready CSVs.

Run:  streamlit run app.py --server.port 8603 --server.headless true
"""

from __future__ import annotations

import glob
import logging
import os
import sys
import tempfile

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pdf_parser  # noqa: E402
import export_warehouse_csv as exporter  # noqa: E402
from categorizer import CATEGORIES, categorize_all  # noqa: E402

logger = logging.getLogger("doc_to_warehouse.app")

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
SAMPLE_DIR = os.path.join(PROJECT_DIR, "sample_pdfs")
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")

OVERHEAD_SUBTYPES = ("utilities", "insurance", "rent", "equipment", "other")

st.set_page_config(
    page_title="Doc-to-Warehouse",
    page_icon=":card_file_box:",
    layout="wide",
)

CUSTOM_CSS = """
<style>
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #f8fafc 0%, #eef2f7 100%);
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 14px 16px;
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.06);
    }
    div[data-testid="stMetricLabel"] p { font-size: 0.85rem; color: #475569; }
    div[data-testid="stMetricValue"] { font-size: 1.45rem; }
    .stButton > button { border-radius: 8px; }
    .dw-header {
        padding: 4px 0 2px 0;
        border-bottom: 3px solid #2563eb;
        margin-bottom: 4px;
    }
    .dw-badge-dup {
        background: #fef3c7; color: #92400e; border: 1px solid #f59e0b;
        border-radius: 6px; padding: 0 6px; font-size: 0.8rem; font-weight: 600;
    }
    .dw-badge-review {
        background: #fee2e2; color: #991b1b; border: 1px solid #ef4444;
        border-radius: 6px; padding: 0 6px; font-size: 0.8rem; font-weight: 600;
    }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

def _init_state() -> None:
    defaults = {
        "txns": [],
        "source_label": "",
        "editor_version": 0,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


_init_state()


# ---------------------------------------------------------------------------
# Pipeline helpers
# ---------------------------------------------------------------------------

def run_pipeline(paths, label: str) -> None:
    """Parse every PDF, categorize, and store the result in session state."""
    collected = []
    failures = []
    for path in paths:
        try:
            collected.extend(pdf_parser.extract_transactions(path))
        except Exception as exc:  # keep going; one bad PDF must not kill all
            logger.error("Extraction failed for %s: %s", path, exc)
            failures.append(os.path.basename(path))
    collected = categorize_all(collected)
    st.session_state.txns = collected
    st.session_state.source_label = label
    st.session_state.editor_version += 1
    if failures:
        st.sidebar.error("Failed to parse: " + ", ".join(failures))


def _write_uploads_to_temp(uploads) -> list:
    tmp_dir = tempfile.mkdtemp(prefix="dw_uploads_")
    paths = []
    for upload in uploads or []:
        name = os.path.basename(upload.name or "upload.pdf") or "upload.pdf"
        if not name.lower().endswith(".pdf"):
            name += ".pdf"
        path = os.path.join(tmp_dir, name)
        with open(path, "wb") as fh:
            fh.write(upload.getvalue())
        paths.append(path)
    return paths


def _csv_bytes(which: str) -> bytes:
    """Build a CSV from the CURRENT (possibly edited) session transactions."""
    with tempfile.TemporaryDirectory(prefix="dw_export_") as tmp_dir:
        path = os.path.join(tmp_dir, which)
        if which == exporter.FINANCIAL_LEDGER_FILENAME:
            exporter.export_financial_ledger(st.session_state.txns, path)
        else:
            exporter.export_labor_logs(st.session_state.txns, path)
        with open(path, "rb") as fh:
            return fh.read()


# ---------------------------------------------------------------------------
# Sidebar: input + filters
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("### Input PDFs")
    st.caption("PDFs only (bank statements, invoices, weekly payroll).")

    uploads = st.file_uploader(
        "Upload documents (drag & drop, multiple allowed)",
        type=["pdf"],
        accept_multiple_files=True,
        key="pdf_uploader",
    )
    if st.button("Extract from uploaded PDFs",
                 use_container_width=True,
                 disabled=not uploads):
        run_pipeline(_write_uploads_to_temp(uploads),
                     f"{len(uploads)} uploaded file(s)")

    st.divider()
    if st.button("Load Sample Messy PDFs", use_container_width=True):
        try:
            import generate_sample_pdfs  # Agent 2's generator (optional)

            generate_sample_pdfs.generate_all(SAMPLE_DIR)
            sample_files = sorted(
                glob.glob(os.path.join(SAMPLE_DIR, "*.pdf")))
            if sample_files:
                run_pipeline(sample_files, "sample_pdfs/")
            else:
                st.warning(
                    "Generator ran but sample_pdfs/ contains no PDFs.")
        except ImportError:
            st.warning(
                "generate_sample_pdfs.py not found next to app.py — the "
                "sample generator is not available yet. Upload your own "
                "PDFs instead.")
        except Exception as exc:  # friendly message, never crash the app
            st.error(f"Sample generation failed: {exc}")

    st.divider()
    st.markdown("### Filters")
    show_only_review = st.checkbox("Show only needs_review", value=False)
    show_only_dupes = st.checkbox("Show only duplicates", value=False)
    doc_type_filter = st.multiselect(
        "Document type",
        options=["bank_statement", "invoice", "payroll", "credit_card"],
        default=[],
    )

    st.divider()
    st.caption(
        "Rule-based extraction runs fully offline. Optional GLM fallback "
        "env vars: GLM_API_KEY / OPENAI_API_KEY, GLM_BASE_URL, GLM_MODEL.")


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.markdown('<div class="dw-header">'
            '<h1>Doc-to-Warehouse — Financial Document Extractor</h1>'
            '</div>', unsafe_allow_html=True)
st.caption("PDF documents → normalized transactions → categorization → "
           "warehouse CSVs (financial_ledger.csv + labor_logs.csv). "
           "Review and edit rows below before exporting.")

txns = st.session_state.txns

if not txns:
    st.info(
        "**No transactions loaded yet.**\n\n"
        "1. Drag & drop PDF bank statements, invoices or weekly payroll "
        "summaries into the sidebar uploader, then click "
        "**Extract from uploaded PDFs**; or\n"
        "2. Click **Load Sample Messy PDFs** to generate and parse the "
        "bundled messy sample documents (bank statement, vendor invoices, "
        "payroll week, and a duplicate re-sent invoice); or\n"
        "3. Run the headless CLI: `python3 export_warehouse_csv.py "
        "--pdfs sample_pdfs --out output`.\n\n"
        "Every extracted row can be reviewed and corrected below, then "
        "exported as warehouse-ready CSVs.")
    st.stop()

# ---------------------------------------------------------------------------
# Metrics row
# ---------------------------------------------------------------------------

total_txns = len(txns)
total_revenue = sum(t.get("amount") or 0.0 for t in txns
                    if t.get("direction") == "in")
total_expenses = sum(t.get("amount") or 0.0 for t in txns
                     if t.get("direction") == "out")
n_duplicates = sum(1 for t in txns if t.get("is_duplicate"))
n_review = sum(1 for t in txns if t.get("needs_review"))

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total transactions", total_txns)
c2.metric("Total revenue", f"${total_revenue:,.2f}")
c3.metric("Total expenses", f"${total_expenses:,.2f}")
c4.metric("Flagged duplicates", n_duplicates)
c5.metric("Needs review", n_review)

if st.session_state.source_label:
    st.caption(f"Source: {st.session_state.source_label}")

# ---------------------------------------------------------------------------
# Review table
# ---------------------------------------------------------------------------

filtered = [
    t for t in txns
    if (not show_only_review or t.get("needs_review"))
    and (not show_only_dupes or t.get("is_duplicate"))
    and (not doc_type_filter or t.get("doc_type") in doc_type_filter)
]

if not filtered:
    st.warning("No rows match the current filters.")
    st.stop()


def _flag(t: dict) -> str:
    if t.get("is_duplicate"):
        return "DUP"
    if t.get("needs_review"):
        return "REVIEW"
    return ""


rows = []
row_txns = []  # parallel to df rows; edits map back through this list
for t in filtered:
    rows.append({
        "flag": _flag(t),
        "txn_id": t.get("txn_id", ""),
        "date": t.get("date", ""),
        "vendor": t.get("vendor", ""),
        "description": t.get("description", ""),
        "amount": float(t.get("amount") or 0.0),
        "direction": t.get("direction", "out"),
        "category": t.get("category") or "",
        "overhead_subtype": t.get("overhead_subtype") or "",
        "confidence": float(t.get("confidence") or 0.0),
        "doc_type": t.get("doc_type", ""),
        "source_file": t.get("source_file", ""),
        "page": t.get("page", 0),
    })
    row_txns.append(t)

df = pd.DataFrame(rows)

EDITABLE_COLS = ["date", "vendor", "description", "amount", "direction",
                 "category", "overhead_subtype"]
DISABLED_COLS = ["flag", "txn_id", "confidence", "doc_type", "source_file",
                 "page"]

edited_df = st.data_editor(
    df,
    key=f"txn_editor_{st.session_state.editor_version}",
    hide_index=True,
    use_container_width=True,
    num_rows="fixed",
    column_config={
        "flag": st.column_config.TextColumn(
            "Flag", help="DUP = flagged duplicate, REVIEW = low confidence",
            disabled=True, width="small"),
        "txn_id": st.column_config.TextColumn("Txn ID", disabled=True,
                                              width="small"),
        "date": st.column_config.TextColumn("Date (YYYY-MM-DD)"),
        "vendor": st.column_config.TextColumn("Vendor"),
        "description": st.column_config.TextColumn("Description",
                                                   width="large"),
        "amount": st.column_config.NumberColumn(
            "Amount", format="%.2f", min_value=0.0, step=0.01),
        "direction": st.column_config.SelectboxColumn(
            "Direction", options=["in", "out"]),
        "category": st.column_config.SelectboxColumn(
            "Category", options=list(CATEGORIES)),
        "overhead_subtype": st.column_config.SelectboxColumn(
            "Overhead subtype", options=list(OVERHEAD_SUBTYPES)),
        "confidence": st.column_config.NumberColumn(
            "Confidence", format="%.2f", disabled=True),
        "doc_type": st.column_config.TextColumn("Doc type", disabled=True),
        "source_file": st.column_config.TextColumn("Source file",
                                                   disabled=True),
        "page": st.column_config.NumberColumn("Page", disabled=True),
    },
    disabled=DISABLED_COLS,
)


def _values_equal(a, b) -> bool:
    try:
        if a is None or b is None:
            return (a or "") == (b or "") if isinstance(a, str) or isinstance(b, str) \
                else float(a or 0) == float(b or 0)
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            return abs(float(a) - float(b)) < 1e-9
        return str(a) == str(b)
    except (TypeError, ValueError):
        return str(a) == str(b)


# Apply user edits back onto the session transactions (flag-not-drop).
applied = 0
for idx, new_row in edited_df.iterrows():
    txn = row_txns[idx]
    orig_row = rows[idx]
    for col in EDITABLE_COLS:
        new_val = new_row[col]
        old_val = orig_row[col]
        if _values_equal(old_val, new_val):
            continue
        if col == "amount":
            txn["amount"] = float(new_val or 0.0)
        elif col == "category":
            txn["category"] = str(new_val) if str(new_val) else None
            if txn["category"] != "Overhead":
                txn["overhead_subtype"] = None
        elif col == "overhead_subtype":
            txn["overhead_subtype"] = str(new_val) if str(new_val) else None
        else:
            txn[col] = str(new_val) if new_val is not None else ""
        applied += 1
if applied:
    st.toast(f"Applied {applied} edit(s) to the transaction list.")

# ---------------------------------------------------------------------------
# Exports (always use the CURRENT, possibly edited session data)
# ---------------------------------------------------------------------------

st.divider()
c_ledger, c_labor, c_folder = st.columns(3)
with c_ledger:
    st.download_button(
        "Download financial_ledger.csv",
        data=_csv_bytes(exporter.FINANCIAL_LEDGER_FILENAME),
        file_name=exporter.FINANCIAL_LEDGER_FILENAME,
        mime="text/csv",
        use_container_width=True,
    )
with c_labor:
    st.download_button(
        "Download labor_logs.csv",
        data=_csv_bytes(exporter.LABOR_LOGS_FILENAME),
        file_name=exporter.LABOR_LOGS_FILENAME,
        mime="text/csv",
        use_container_width=True,
    )
with c_folder:
    if st.button("Export to output/ folder", use_container_width=True):
        paths = exporter.export_all(st.session_state.txns, OUTPUT_DIR)
        st.success("Wrote {ledger} and {labor}".format(
            ledger=paths["financial_ledger"], labor=paths["labor_logs"]))

st.caption(
    "Duplicate rows are flagged (DUP) and still exported — fix or confirm "
    "them before loading into the warehouse. Revenue/expense direction is "
    "carried by entry_type; amounts are always positive.")


# ---------------------------------------------------------------------------
# Push Directly to Data Warehouse (suite bridge: PostgreSQL + port 8602)
# ---------------------------------------------------------------------------

st.divider()
st.markdown("### Push Directly to Data Warehouse")
st.caption(
    "Send the reviewed transactions straight into the local PostgreSQL "
    "warehouse (SMB Warehouse), then view the workspace in the Data "
    "Warehouse dashboard. The CSV downloads above keep working as before.")

try:
    import warehouse_bridge  # noqa: E402  (ships next to app.py)
except Exception as exc:  # defensive: never break the rest of the app
    logger.error("warehouse_bridge import failed: %s", exc)
    st.error(f"warehouse_bridge module unavailable: {exc}")
else:
    n_flagged_dupes = sum(1 for t in txns if t.get("is_duplicate"))
    companies = None
    try:
        companies = warehouse_bridge.list_active_companies()
    except Exception as exc:  # DB down / unreachable: show + retry, no crash
        st.error(
            "Cannot reach the Data Warehouse database — is Postgres "
            f"running? ({exc})")
        if st.button("Retry connection", use_container_width=True):
            st.rerun()

    if companies is not None:
        if not companies:
            st.warning(
                "No client workspaces exist in the warehouse yet. Create "
                "one in the Data Warehouse app (sidebar → ➕ New client "
                "workspace), then retry the push.")
        else:
            name_by_id = {c["company_name"]: c["company_id"]
                          for c in companies}
            selected_name = st.selectbox(
                "Client workspace (Data Warehouse)",
                options=list(name_by_id.keys()),
                key="dw_push_workspace",
                help="Transactions are pushed into this company workspace "
                     "in the warehouse (multi-tenant: every row is scoped "
                     "to it).")
            include_dupes = st.checkbox(
                "Include rows flagged as duplicates",
                value=False,
                help="Duplicate rows carry the DUP flag in the review "
                     "grid. Leave this off to push only the unique "
                     "transactions; the warehouse also dedupes exact "
                     "rows it already has.")
            n_pushable = len(txns) - (0 if include_dupes
                                      else n_flagged_dupes)
            if st.button(
                    f"Push {n_pushable} transactions to warehouse",
                    type="primary",
                    disabled=(not txns or n_pushable <= 0),
                    use_container_width=True):
                try:
                    result = warehouse_bridge.push_transactions(
                        st.session_state.txns,
                        name_by_id[selected_name],
                        include_duplicates=include_dupes,
                    )
                except Exception as exc:
                    logger.error("Warehouse push failed: %s", exc)
                    st.error(f"Push failed: {exc}")
                else:
                    created_refs = result.get("created_refs") or {}
                    created_txt = ", ".join(
                        f"{v} {k}" for k, v in sorted(created_refs.items())
                        if v) or "none"
                    row_errors = result.get("errors") or []
                    st.success(
                        f"Pushed **{result.get('ledger_rows', 0)} ledger "
                        f"row(s)** and **{result.get('labor_rows', 0)} "
                        f"labor row(s)** into "
                        f"**{result.get('company_name', 'the workspace')}**"
                        ".\n\n"
                        f"- Skipped duplicates: "
                        f"{result.get('skipped_duplicates', 0)}\n"
                        f"- New anomaly alerts: "
                        f"{result.get('anomalies_new', 0)}\n"
                        f"- Warehouse references created: {created_txt}\n\n"
                        f"[Open the Data Warehouse workspace →]"
                        f"({warehouse_bridge.WAREHOUSE_APP_URL})")
                    if row_errors:
                        with st.expander(
                                f"{len(row_errors)} row(s) could not be "
                                "loaded"):
                            for e in row_errors:
                                st.warning(
                                    f"row {e.get('row')}: {e.get('error')}")
