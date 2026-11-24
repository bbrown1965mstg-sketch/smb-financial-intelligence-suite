"""Warehouse CSV export (financial_ledger.csv + labor_logs.csv).

Per CONTRACT.md §6:

* ``FINANCIAL_LEDGER_HEADERS`` / ``LABOR_LOG_HEADERS`` -- fixed column order.
* Warehouse ``amount`` is ALWAYS positive and formatted to 2 decimal places
  as a plain string (``"1234.56"``, no ``$``, no thousands separators);
  ``entry_type`` carries the direction (``revenue`` / ``expense``).
* Category mapping (FIXED):

  =======================  ===========================  =========================
  extracted_category       entry_type                   warehouse category
  =======================  ===========================  =========================
  Revenue                  revenue                      revenue
  Direct_Materials         expense                      materials
  Subcontractor            expense                      subcontractor
  Marketing                expense                      marketing
  Payroll                  expense                      payroll
  Overhead                 expense                      <overhead_subtype>
                                                        (utilities / insurance /
                                                        rent / equipment /
                                                        other; default "other")
  =======================  ===========================  =========================

  ``extracted_category`` keeps the six-word vocabulary for transparency.
* ``labor_logs`` rows come ONLY from payroll ``line_items`` (one row per
  employee per work date; ``hours`` within 0 < h <= 24 and
  ``hourly_rate >= 0`` -- out-of-range rows are skipped with a warning).
* Duplicate rows are still exported, but with ``needs_review=True``
  (flag-not-drop).

CLI: ``python3 export_warehouse_csv.py --pdfs DIR --out DIR`` runs the whole
headless pipeline (parse -> categorize -> export) over every PDF in DIR.
"""

from __future__ import annotations

import argparse
import csv
import glob
import logging
import os
import sys
from typing import Dict, List

import pdf_parser
from categorizer import CATEGORIES, categorize_all

logger = logging.getLogger("doc_to_warehouse.export")

FINANCIAL_LEDGER_HEADERS = [
    "entry_type", "category", "entry_date", "amount", "description", "vendor",
    "job_id", "customer_id", "extracted_category", "confidence",
    "needs_review", "is_duplicate", "duplicate_of", "source_file",
    "source_page"]

LABOR_LOG_HEADERS = [
    "work_date", "hours", "hourly_rate", "description", "employee_name",
    "employee_id", "job_id", "confidence", "needs_review", "source_file",
    "source_page"]

FINANCIAL_LEDGER_FILENAME = "financial_ledger.csv"
LABOR_LOGS_FILENAME = "labor_logs.csv"


def _map_category(category, overhead_subtype) -> tuple:
    """FIXED mapping -> (entry_type, warehouse category)."""
    if category == "Revenue":
        return "revenue", "revenue"
    if category == "Direct_Materials":
        return "expense", "materials"
    if category == "Subcontractor":
        return "expense", "subcontractor"
    if category == "Marketing":
        return "expense", "marketing"
    if category == "Payroll":
        return "expense", "payroll"
    if category == "Overhead":
        subtype = overhead_subtype or "other"
        return "expense", str(subtype)
    # Unknown/uncategorized: keep the row, conservative warehouse mapping.
    return "expense", "other"


def _fmt_amount(value) -> str:
    """Plain 2dp positive amount string: '1234.56' (no $, no commas)."""
    try:
        amt = abs(float(value))
    except (TypeError, ValueError):
        amt = 0.0
    return f"{amt:.2f}"


def _fmt_conf(value) -> str:
    try:
        conf = float(value)
    except (TypeError, ValueError):
        conf = 0.0
    return f"{conf:.2f}"


def _bool_str(value) -> str:
    return "True" if bool(value) else "False"


def export_financial_ledger(txns: List[dict], out_path: str) -> int:
    """Write the financial ledger CSV. Returns the number of rows written."""
    out_dir = os.path.dirname(os.path.abspath(out_path))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    rows_written = 0
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(FINANCIAL_LEDGER_HEADERS)
        for txn in txns:
            category = txn.get("category")
            entry_type, wh_category = _map_category(
                category, txn.get("overhead_subtype"))
            needs_review = bool(txn.get("needs_review")) or bool(
                txn.get("is_duplicate"))
            writer.writerow([
                entry_type,
                wh_category,
                str(txn.get("date") or ""),
                _fmt_amount(txn.get("amount")),
                str(txn.get("description") or ""),
                str(txn.get("vendor") or ""),
                "",  # job_id: not derivable from source documents
                "",  # customer_id: not derivable from source documents
                str(category) if category else "",
                _fmt_conf(txn.get("confidence")),
                _bool_str(needs_review),
                _bool_str(txn.get("is_duplicate")),
                str(txn.get("duplicate_of") or ""),
                str(txn.get("source_file") or ""),
                str(txn.get("page") if txn.get("page") is not None else ""),
            ])
            rows_written += 1
    logger.info("Wrote %d rows to %s", rows_written, out_path)
    return rows_written


def export_labor_logs(txns: List[dict], out_path: str) -> int:
    """Write labor logs from payroll line_items only. Returns rows written.

    Warehouse CHECK ranges: 0 < hours <= 24, hourly_rate >= 0. Rows outside
    the ranges are skipped (logged) so the CSV never violates them.
    """
    out_dir = os.path.dirname(os.path.abspath(out_path))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    rows_written = 0
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(LABOR_LOG_HEADERS)
        for txn in txns:
            if str(txn.get("doc_type") or "") != "payroll":
                continue
            for item in txn.get("line_items") or []:
                try:
                    hours = float(item.get("hours"))
                    rate = float(item.get("rate"))
                except (TypeError, ValueError):
                    logger.warning("Skipping malformed labor row in %s: %r",
                                   txn.get("source_file"), item)
                    continue
                if not (0.0 < hours <= 24.0) or rate < 0:
                    logger.warning(
                        "Skipping labor row outside warehouse CHECK ranges "
                        "(hours=%s, rate=%s) in %s", hours, rate,
                        txn.get("source_file"))
                    continue
                needs_review = bool(txn.get("needs_review")) or bool(
                    txn.get("is_duplicate"))
                writer.writerow([
                    str(item.get("work_date") or ""),
                    f"{hours:.2f}",
                    f"{rate:.2f}",
                    str(txn.get("description") or ""),
                    str(item.get("employee") or ""),
                    "",  # employee_id: not present in source payroll grid
                    "",  # job_id: not derivable from source documents
                    _fmt_conf(txn.get("confidence")),
                    _bool_str(needs_review),
                    str(txn.get("source_file") or ""),
                    str(txn.get("page") if txn.get("page") is not None else ""),
                ])
                rows_written += 1
    logger.info("Wrote %d labor rows to %s", rows_written, out_path)
    return rows_written


def export_all(txns: List[dict], out_dir: str) -> Dict[str, str]:
    """Export both CSVs into ``out_dir``.

    Returns ``{"financial_ledger": <path>, "labor_logs": <path>}``."""
    os.makedirs(out_dir, exist_ok=True)
    ledger_path = os.path.join(out_dir, FINANCIAL_LEDGER_FILENAME)
    labor_path = os.path.join(out_dir, LABOR_LOGS_FILENAME)
    export_financial_ledger(txns, ledger_path)
    export_labor_logs(txns, labor_path)
    return {"financial_ledger": ledger_path, "labor_logs": labor_path}


# ---------------------------------------------------------------------------
# Headless CLI
# ---------------------------------------------------------------------------

def _find_pdfs(pdfs_dir: str) -> List[str]:
    if not os.path.isdir(pdfs_dir):
        return []
    pdfs = [p for p in glob.glob(os.path.join(pdfs_dir, "*"))
            if p.lower().endswith(".pdf")]
    return sorted(pdfs)


def main(argv=None) -> int:
    """Headless pipeline: parse -> categorize -> export over a PDF folder."""
    parser = argparse.ArgumentParser(
        description="Doc-to-Warehouse: extract, categorize and export PDF "
                    "financial documents to warehouse CSVs.")
    parser.add_argument("--pdfs", required=True,
                        help="Directory containing input PDFs")
    parser.add_argument("--out", required=True,
                        help="Directory for financial_ledger.csv / "
                             "labor_logs.csv")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s %(name)s: %(message)s")

    pdf_paths = _find_pdfs(args.pdfs)
    if not pdf_paths:
        print(f"No PDFs found in {args.pdfs!r}")
        return 1

    all_txns: List[dict] = []
    for pdf_path in pdf_paths:
        try:
            txns = pdf_parser.extract_transactions(pdf_path)
        except Exception as exc:  # never let one bad PDF kill the run
            logger.error("Extraction failed for %s: %s", pdf_path, exc)
            txns = []
        all_txns.extend(txns)
        logger.info("%s -> %d transactions",
                    os.path.basename(pdf_path), len(txns))

    # Categorize once over the FULL set: duplicate detection is cross-file
    # (e.g. a re-sent invoice in a different PDF), so it must see every
    # transaction together, not per-file.
    all_txns = categorize_all(all_txns)

    paths = export_all(all_txns, args.out)
    n_review = sum(1 for t in all_txns if t.get("needs_review"))
    n_dupes = sum(1 for t in all_txns if t.get("is_duplicate"))
    print(f"Parsed {len(pdf_paths)} PDFs -> {len(all_txns)} transactions "
          f"({n_dupes} duplicates flagged, {n_review} need review). "
          f"Wrote {paths['financial_ledger']} and {paths['labor_logs']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
