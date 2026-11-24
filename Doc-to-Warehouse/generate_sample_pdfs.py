#!/usr/bin/env python3
"""Deterministic generator for the Doc-to-Warehouse sample corpus (Agent 2).

Owns (per CONTRACT.md section 1): ``generate_sample_pdfs.py``, ``conftest.py``,
``test_extractor.py``, ``sample_pdfs/`` and ``ground_truth/``.

Generates exactly six deliberately messy sample PDFs (reportlab) plus
``ground_truth/ground_truth.json`` from the single ``GROUND_TRUTH`` dict
defined at module top (format per CONTRACT.md section 4). Every fact
(date, vendor, amount, direction, line item) comes from ``GROUND_TRUTH``;
all money math uses :class:`decimal.Decimal` and is asserted internally
consistent *before* any file is written.

Determinism: no randomness is used anywhere -- layout, text, whitespace
quirks and footnote asterisks are fixed constants, so repeated runs produce
identical documents (module constant ``SEED`` is documentation only).

Usage::

    python3 generate_sample_pdfs.py        # generate + round-trip verify (prints summary)

or programmatically::

    from generate_sample_pdfs import generate_all
    generate_all("sample_pdfs")            # {"files": [...], "ground_truth_path": ...}
"""

import copy
import json
import logging
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Tuple

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import (PageBreak, Paragraph, SimpleDocTemplate,
                                Spacer, Table, TableStyle)

logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SAMPLE_DIR_DEFAULT = "sample_pdfs"
GROUND_TRUTH_DIR = "ground_truth"

# Documentation only: the corpus is deterministic by construction (no
# `random` calls anywhere); this value would be the seed if randomness were
# ever introduced.
SEED = 20260302

# Bank statement opening balance (printed on page 1; reconciles to
# GROUND_TRUTH expected_totals.final_balance).
BEGINNING_BALANCE = Decimal("18250.00")

# The fixed inventory per CONTRACT.md section 3, in ground-truth document
# order (original invoice before its re-send so duplicate detection sees the
# re-send as the later occurrence under any file ordering).
BANK_FILE = "bank_statement_pacific_trust.pdf"
INVOICE_ABC_FILE = "invoice_abc_lumber.pdf"
INVOICE_SIERRA_FILE = "invoice_sierra_supply.pdf"
INVOICE_HILTI_FILE = "invoice_hilti_equipment.pdf"
PAYROLL_FILE = "payroll_week_2026-03-02.pdf"
INVOICE_RESEND_FILE = "invoice_abc_lumber_resend.pdf"
EXPECTED_FILES: Tuple[str, ...] = (
    BANK_FILE,
    INVOICE_ABC_FILE,
    INVOICE_SIERRA_FILE,
    INVOICE_HILTI_FILE,
    PAYROLL_FILE,
    INVOICE_RESEND_FILE,
)


def D(value: Any) -> Decimal:
    """Convert to Decimal via ``str()`` so float round-trips stay penny-exact."""
    return Decimal(str(value))


# ---------------------------------------------------------------------------
# GROUND TRUTH -- single source of truth for BOTH PDF generation and
# ground_truth/ground_truth.json (CONTRACT.md section 4). Amounts are exact
# to the penny. "expected_category" / "expected_overhead_subtype" are the
# one allowed additive extension per the test plan.
# ---------------------------------------------------------------------------
GROUND_TRUTH: Dict[str, Any] = {
    "generated_at": "",  # filled by generate_all() at write time
    "documents": [
        {
            "file": BANK_FILE,
            "doc_type": "bank_statement",
            "transactions": [
                {"date": "2026-03-02", "vendor": "HARBORVIEW HOA",
                 "description": "Deposit - customer payment",
                 "amount": 8400.00, "direction": "in",
                 "expected_category": "Revenue"},
                {"date": "2026-03-02", "vendor": "ABC Lumber Co.",
                 "description": "Check - ABC Lumber Co.",
                 "amount": 2315.40, "direction": "out",
                 "expected_category": "Direct_Materials"},
                {"date": "2026-03-02", "vendor": "Sierra Building Supply",
                 "description": "ACH - Sierra Building Supply",
                 "amount": 1187.25, "direction": "out",
                 "expected_category": "Direct_Materials"},
                {"date": "2026-03-03", "vendor": "Cedar Grove Estates",
                 "description": "Deposit - customer payment",
                 "amount": 6150.00, "direction": "in",
                 "expected_category": "Revenue"},
                {"date": "2026-03-03", "vendor": "BrightWave Digital Advertising",
                 "description": "ACH - BrightWave Digital Advertising platform",
                 "amount": 425.00, "direction": "out",
                 "expected_category": "Marketing"},
                {"date": "2026-03-03", "vendor": "Pacific Gas & Electric",
                 "description": "Wire - Pacific Gas & Electric",
                 "amount": 612.44, "direction": "out",
                 "expected_category": "Overhead",
                 "expected_overhead_subtype": "utilities"},
                {"date": "2026-03-04", "vendor": "ABC Lumber Co.",
                 "description": "Check - ABC Lumber Co.",
                 "amount": 3980.12, "direction": "out",
                 "expected_category": "Direct_Materials"},
                {"date": "2026-03-04", "vendor": "SureGuard Insurance Services",
                 "description": "ACH - SureGuard Insurance Services premium",
                 "amount": 1200.00, "direction": "out",
                 "expected_category": "Overhead",
                 "expected_overhead_subtype": "insurance"},
                {"date": "2026-03-05", "vendor": "Ironclad Subcontractors LLC",
                 "description": "Check - Ironclad Subcontractors LLC",
                 "amount": 4750.00, "direction": "out",
                 "expected_category": "Subcontractor"},
                {"date": "2026-03-05", "vendor": "Harborview HOA",
                 "description": "Deposit - customer payment",
                 "amount": 5230.00, "direction": "in",
                 "expected_category": "Revenue"},
                {"date": "2026-03-05", "vendor": "Pacific Trust Bank",
                 "description": "Bank service charge - Pacific Trust Bank",
                 "amount": 45.00, "direction": "out",
                 "expected_category": "Overhead",
                 "expected_overhead_subtype": "other"},
                {"date": "2026-03-06", "vendor": "Ironclad Subcontractors LLC",
                 "description": "Check - Ironclad Subcontractors LLC",
                 "amount": 3125.00, "direction": "out",
                 "expected_category": "Subcontractor"},
                {"date": "2026-03-06", "vendor": "Hilti Tool Rental",
                 "description": "ACH - Hilti Tool Rental equipment",
                 "amount": 890.50, "direction": "out",
                 "expected_category": "Overhead",
                 "expected_overhead_subtype": "equipment"},
                {"date": "2026-03-06", "vendor": "Cedar Grove Estates",
                 "description": "Deposit - customer payment",
                 "amount": 4120.75, "direction": "in",
                 "expected_category": "Revenue"},
                {"date": "2026-03-06", "vendor": "Silver Creek Concrete Inc",
                 "description": "Check - Silver Creek Concrete Inc",
                 "amount": 1840.00, "direction": "out",
                 "expected_category": "Direct_Materials"},
            ],
            "expected_totals": {
                "total_credits": 23900.75,
                "total_debits": 20370.71,
                "final_balance": 21780.04,
            },
        },
        {
            "file": INVOICE_ABC_FILE,
            "doc_type": "invoice",
            "transactions": [
                {"date": "2026-03-04", "vendor": "ABC Lumber Co.",
                 "invoice_number": "INV-1041",
                 "description": "Materials invoice INV-1041",
                 "amount": 823.66, "direction": "out",
                 "line_items": [
                     {"description": "2x6 Douglas Fir lumber, 8 ft (per stick)",
                      "qty": 12.0, "unit_price": 14.75, "amount": 177.00},
                     {"description": "Plywood sheathing 3/4 in, 4x8 sheet",
                      "qty": 5.0, "unit_price": 89.50, "amount": 447.50},
                     {"description": "Deck screws, 3 in stainless (box of 500)",
                      "qty": 3.0, "unit_price": 42.80, "amount": 128.40},
                     {"description": "Treated 4x4x10 post, ground contact rated "
                      "pressure treated lumber, per each",
                      "qty": 1.0, "unit_price": 9.75, "amount": 9.75},
                 ],
                 "expected_category": "Direct_Materials"},
            ],
            "expected_totals": {"subtotal": 762.65, "tax": 61.01, "total": 823.66},
        },
        {
            "file": INVOICE_SIERRA_FILE,
            "doc_type": "invoice",
            "transactions": [
                {"date": "2026-03-05", "vendor": "Sierra Building Supply",
                 "invoice_number": "INV-2077",
                 "description": "Materials invoice INV-2077",
                 "amount": 707.83, "direction": "out",
                 "line_items": [
                     {"description": "Polymeric joint sand, 50 lb bag",
                      "qty": 40.0, "unit_price": 8.25, "amount": 330.00},
                     {"description": "Landscape fabric, 3 ft x 100 ft roll",
                      "qty": 6.0, "unit_price": 34.60, "amount": 207.60},
                     {"description": "Drainage basin, 12 in grate",
                      "qty": 2.0, "unit_price": 58.90, "amount": 117.80},
                 ],
                 "expected_category": "Direct_Materials"},
            ],
            "expected_totals": {"subtotal": 655.40, "tax": 52.43, "total": 707.83},
        },
        {
            "file": INVOICE_HILTI_FILE,
            "doc_type": "invoice",
            "transactions": [
                {"date": "2026-03-06", "vendor": "Hilti Equipment Rental",
                 "invoice_number": "INV-5532",
                 "description": "Equipment rental invoice INV-5532",
                 "amount": 1654.75, "direction": "out",
                 "line_items": [
                     {"description": "Mini skid steer rental, weekly rate",
                      "qty": 1.0, "unit_price": 1450.00, "amount": 1450.00},
                     {"description": "Concrete saw blade, 14 in",
                      "qty": 3.0, "unit_price": 68.25, "amount": 204.75},
                 ],
                 "expected_category": "Overhead",
                 "expected_overhead_subtype": "equipment"},
            ],
            # Hilti is a rental (service) invoice: no tax line printed.
            "expected_totals": {"subtotal": 1654.75, "tax": 0.00, "total": 1654.75},
        },
        {
            "file": PAYROLL_FILE,
            "doc_type": "payroll",
            "transactions": [
                {"date": "2026-03-06", "vendor": "Bristlecone Landscaping",
                 "description": "Weekly payroll 2026-03-02 to 2026-03-05",
                 "amount": 3218.25, "direction": "out",
                 "line_items": [
                     {"employee": "Maria Alvarez", "work_date": "2026-03-02",
                      "hours": 8.0, "rate": 24.50, "amount": 196.00},
                     {"employee": "Maria Alvarez", "work_date": "2026-03-03",
                      "hours": 8.0, "rate": 24.50, "amount": 196.00},
                     {"employee": "Maria Alvarez", "work_date": "2026-03-04",
                      "hours": 7.5, "rate": 24.50, "amount": 183.75},
                     {"employee": "Maria Alvarez", "work_date": "2026-03-05",
                      "hours": 8.0, "rate": 24.50, "amount": 196.00},
                     {"employee": "DeShawn Carter", "work_date": "2026-03-02",
                      "hours": 8.0, "rate": 28.00, "amount": 224.00},
                     {"employee": "DeShawn Carter", "work_date": "2026-03-03",
                      "hours": 9.0, "rate": 28.00, "amount": 252.00},
                     {"employee": "DeShawn Carter", "work_date": "2026-03-04",
                      "hours": 8.0, "rate": 28.00, "amount": 224.00},
                     {"employee": "DeShawn Carter", "work_date": "2026-03-05",
                      "hours": 8.0, "rate": 28.00, "amount": 224.00},
                     {"employee": "Tuyen Nguyen", "work_date": "2026-03-02",
                      "hours": 8.0, "rate": 22.75, "amount": 182.00},
                     {"employee": "Tuyen Nguyen", "work_date": "2026-03-03",
                      "hours": 8.0, "rate": 22.75, "amount": 182.00},
                     {"employee": "Tuyen Nguyen", "work_date": "2026-03-04",
                      "hours": 8.0, "rate": 22.75, "amount": 182.00},
                     {"employee": "Tuyen Nguyen", "work_date": "2026-03-05",
                      "hours": 6.0, "rate": 22.75, "amount": 136.50},
                     {"employee": "Rosa Delgado", "work_date": "2026-03-02",
                      "hours": 8.0, "rate": 26.25, "amount": 210.00},
                     {"employee": "Rosa Delgado", "work_date": "2026-03-03",
                      "hours": 8.0, "rate": 26.25, "amount": 210.00},
                     {"employee": "Rosa Delgado", "work_date": "2026-03-04",
                      "hours": 8.0, "rate": 26.25, "amount": 210.00},
                     {"employee": "Rosa Delgado", "work_date": "2026-03-05",
                      "hours": 8.0, "rate": 26.25, "amount": 210.00},
                 ],
                 "expected_category": "Payroll"},
            ],
            "expected_totals": {"total_gross": 3218.25},
        },
        {
            # The duplicate-detection probe: same vendor (under its other
            # legal name), same invoice number, same total, different file.
            "file": INVOICE_RESEND_FILE,
            "doc_type": "invoice",
            "transactions": [
                {"date": "2026-03-04", "vendor": "ABC LUMBER SUPPLY INC",
                 "invoice_number": "INV-1041",
                 "description": "Materials invoice INV-1041",
                 "amount": 823.66, "direction": "out",
                 "line_items": [
                     {"description": "2x6 Douglas Fir lumber, 8 ft (per stick)",
                      "qty": 12.0, "unit_price": 14.75, "amount": 177.00},
                     {"description": "Plywood sheathing 3/4 in, 4x8 sheet",
                      "qty": 5.0, "unit_price": 89.50, "amount": 447.50},
                     {"description": "Deck screws, 3 in stainless (box of 500)",
                      "qty": 3.0, "unit_price": 42.80, "amount": 128.40},
                     {"description": "Treated 4x4x10 post, ground contact rated "
                      "pressure treated lumber, per each",
                      "qty": 1.0, "unit_price": 9.75, "amount": 9.75},
                 ],
                 "expected_category": "Direct_Materials"},
            ],
            "expected_totals": {"subtotal": 762.65, "tax": 61.01, "total": 823.66},
        },
    ],
    "expected_duplicates": [
        # vendor_canonical per the CONTRACT.md normalize_vendor rule
        # (lowercase, legal suffixes stripped, whitespace collapsed).
        {"vendor_canonical": "abc lumber", "invoice_number": "INV-1041"},
    ],
    "vendor_alias_groups": [
        ["ABC Lumber Co.", "ABC LUMBER SUPPLY INC"],
    ],
}


# ---------------------------------------------------------------------------
# Presentation layer: how GROUND_TRUTH facts are rendered into messy PDFs.
# Nothing here changes a fact; _validate_ground_truth() cross-checks every
# printed string against GROUND_TRUTH before any file is written.
# ---------------------------------------------------------------------------
# Per bank row: "ref" printed in the Ref column ("" = blank, realistic gap),
# "desc" = list of printed description lines (2 lines = deliberate wrap or
# payee continuation line; asterisks are trailing footnote markers).
BANK_PRESENTATION: List[Dict[str, Any]] = [
    {"ref": "DEP-88134", "desc": ["Deposit - customer payment", "HARBORVIEW HOA"]},
    {"ref": "CHK-1201", "desc": ["Check -  ABC Lumber Co. *"]},
    {"ref": "ACH-44105", "desc": ["ACH - Sierra Building Supply"]},
    {"ref": "DEP-88147", "desc": ["Deposit - customer payment", "Cedar Grove Estates"]},
    {"ref": "ACH-44123", "desc": ["ACH - BrightWave Digital", "Advertising platform *"]},
    {"ref": "WIR-90211", "desc": ["Wire - Pacific Gas & Electric"]},
    {"ref": "CHK-1214", "desc": ["Check - ABC Lumber Co."]},
    {"ref": "ACH-44156", "desc": ["ACH - SureGuard Insurance", "Services premium *"],
     "paren": True},
    {"ref": "CHK-1233", "desc": ["Check - Ironclad Subcontractors LLC"]},
    {"ref": "DEP-88162", "desc": ["Deposit - customer payment", "Harborview HOA"]},
    {"ref": "", "desc": ["Bank service charge - Pacific Trust Bank"]},
    {"ref": "CHK-1240", "desc": ["Check - Ironclad Subcontractors LLC"]},
    {"ref": "ACH-44187", "desc": ["ACH - Hilti Tool Rental equipment *"]},
    {"ref": "DEP-88178", "desc": ["Deposit - customer payment", "Cedar Grove Estates"]},
    {"ref": "CHK-1244", "desc": ["Check - Silver Creek Concrete Inc"]},
]

INVOICE_PRESENTATION: Dict[str, Dict[str, Any]] = {
    INVOICE_ABC_FILE: {
        "vendor_header": "ABC Lumber Co.",
        "address_lines": ["1840 Timberline Road", "Sacramento, CA 95815",
                          "Phone: (916) 555-0184"],
        "meta_lines": ["Invoice #: {inv}", "Invoice Date: {date}",
                       "Bill To: Bristlecone Landscaping"],
        "tax_label": "Sales Tax (8%):",
        "note_lines": ["Thank you for your business. Net 30.",
                       "Please retain this copy for your records."],
        "wrapped_item_index": 3,
    },
    INVOICE_SIERRA_FILE: {
        "vendor_header": "Sierra Building Supply",
        "address_lines": ["2200 Sierra Meadows Drive, Suite B", "Rocklin, CA 95677",
                          "Phone: (916) 555-0139"],
        # Inconsistent spacing around the colon vs the other invoices.
        "meta_lines": ["Invoice #  :  {inv}", "Invoice Date:  {date}",
                       "Bill To: Bristlecone Landscaping"],
        "tax_label": "Sales Tax (8%):",
        "note_lines": ["Delivery charges included. Net 30."],
        "wrapped_item_index": None,
    },
    INVOICE_HILTI_FILE: {
        # All-caps header: vendor case differs from ground truth spelling.
        "vendor_header": "HILTI EQUIPMENT RENTAL",
        "address_lines": ["2450 Industrial Parkway", "West Sacramento, CA 95605",
                          "Phone: (916) 555-0177"],
        "meta_lines": ["Invoice #: {inv}", "Invoice Date: {date}",
                       "Bill To: Bristlecone Landscaping"],
        "tax_label": None,
        "note_lines": ["Rental equipment must be returned within 7 days "
                       "to avoid additional charges."],
        "wrapped_item_index": None,
    },
    INVOICE_RESEND_FILE: {
        # Same company under its other legal name (the vendor-alias probe).
        "vendor_header": "ABC LUMBER SUPPLY INC",
        "address_lines": ["1840 Timberline Road", "Sacramento, CA 95815",
                          "Phone: (916) 555-0184"],
        "meta_lines": ["Invoice #: {inv}", "Invoice Date: {date}",
                       "Bill To: Bristlecone Landscaping"],
        "tax_label": "Sales Tax (8%):",
        "note_lines": ["Re-sent copy of previously emailed invoice.",
                       "Please retain this copy for your records."],
        "wrapped_item_index": 3,
    },
}


# ---------------------------------------------------------------------------
# Formatting helpers (shared by builders and verifier)
# ---------------------------------------------------------------------------
def _money(d: Decimal) -> str:
    """$1,234.56 style with thousands commas."""
    return "${:,.2f}".format(d)


def _paren(d: Decimal) -> str:
    """(1,200.00) style negative - printed WITHOUT a dollar sign."""
    return "({:,.2f})".format(d)


def _mmddyyyy(iso_date: str) -> str:
    """'2026-03-02' -> '03/02/2026' (the only date style printed in the PDFs)."""
    year, month, day = iso_date.split("-")
    return "{:02d}/{:02d}/{}".format(int(month), int(day), year)


def _num(d: Decimal) -> str:
    """Trim trailing '.0': 8.0 -> '8', 7.5 -> '7.5', 12.0 -> '12'."""
    if d == d.to_integral_value():
        return str(int(d))
    return str(d.normalize())


def _clean_desc(text: str) -> str:
    """Collapse whitespace and drop a trailing footnote asterisk."""
    cleaned = " ".join(text.split())
    if cleaned.endswith(" *"):
        cleaned = cleaned[:-2]
    return cleaned


def _esc(text: str) -> str:
    """Escape for reportlab Paragraph XML markup (keeps output ASCII)."""
    return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


# ---------------------------------------------------------------------------
# Internal consistency validation (Decimal) -- runs BEFORE any file write
# ---------------------------------------------------------------------------
def _validate_ground_truth(gt: Dict[str, Any]) -> None:
    """Assert every fact in GROUND_TRUTH is internally consistent.

    Raises AssertionError (with a precise message) if not. Checks:
      * invoice line items sum exactly to subtotal, subtotal+tax == total,
        each line amount == qty * unit_price, txn amount == total;
      * payroll line items sum exactly to TOTAL GROSS, each amount ==
        hours * rate, 0 < hours <= 24, description matches work-date span;
      * bank credits - debits reconcile the running balance from
        BEGINNING_BALANCE to expected_totals.final_balance, printed
        description lines match ground truth, row keys are unique;
      * exactly 20 transactions across the 6 fixed documents.
    """
    files = [doc["file"] for doc in gt["documents"]]
    assert tuple(files) == EXPECTED_FILES, (
        "ground truth documents must be exactly %r, got %r" % (EXPECTED_FILES, files))

    total_txns = 0
    for doc in gt["documents"]:
        for txn in doc["transactions"]:
            total_txns += 1
            amount = D(txn["amount"])
            assert amount == amount.quantize(Decimal("0.01")), (
                "amount %r not penny-exact in %s" % (txn["amount"], doc["file"]))
            assert txn["direction"] in ("in", "out"), txn

        if doc["doc_type"] == "bank_statement":
            txns = doc["transactions"]
            assert len(BANK_PRESENTATION) == len(txns), "bank presentation mismatch"
            # (date, amount, direction) keys must be unique for 1:1 matching.
            keys = [(t["date"], "{:.2f}".format(D(t["amount"])), t["direction"])
                    for t in txns]
            assert len(set(keys)) == len(keys), "bank rows not unique: %r" % keys
            amounts = ["{:.2f}".format(D(t["amount"])) for t in txns]
            assert len(set(amounts)) == len(amounts), "bank amounts not unique"

            balance = BEGINNING_BALANCE
            credits = Decimal("0")
            debits = Decimal("0")
            for idx, txn in enumerate(txns):
                amount = D(txn["amount"])
                if txn["direction"] == "in":
                    balance += amount
                    credits += amount
                else:
                    balance -= amount
                    debits += amount
                pres = BANK_PRESENTATION[idx]
                lines = pres["desc"]
                if len(lines) == 2 and _clean_desc(lines[1]) == " ".join(txn["vendor"].split()):
                    # Payee continuation line: line 1 is the description,
                    # line 2 is the vendor as printed.
                    assert " ".join(lines[0].split()) == " ".join(txn["description"].split()), (
                        "bank row %d desc mismatch: %r vs %r"
                        % (idx, lines[0], txn["description"]))
                    assert " ".join(lines[1].split()) == " ".join(txn["vendor"].split()), (
                        "bank row %d continuation != vendor: %r vs %r"
                        % (idx, lines[1], txn["vendor"]))
                elif len(lines) == 2:
                    # Deliberately wrapped description: joined lines == GT.
                    joined = _clean_desc(" ".join(lines))
                    assert joined == " ".join(txn["description"].split()), (
                        "bank row %d wrapped desc mismatch: %r vs %r"
                        % (idx, joined, txn["description"]))
                else:
                    assert _clean_desc(lines[0]) == " ".join(txn["description"].split()), (
                        "bank row %d desc mismatch: %r vs %r"
                        % (idx, lines[0], txn["description"]))
                assert pres["ref"] == "" or (
                    len(pres["ref"]) in (8, 9) and pres["ref"][:3].isalpha()
                    and pres["ref"][3] == "-"), "bad ref %r" % pres["ref"]

            totals = doc["expected_totals"]
            assert credits == D(totals["total_credits"]), (
                "credits %s != %s" % (credits, totals["total_credits"]))
            assert debits == D(totals["total_debits"]), (
                "debits %s != %s" % (debits, totals["total_debits"]))
            assert balance == D(totals["final_balance"]), (
                "final balance %s != %s" % (balance, totals["final_balance"]))

        elif doc["doc_type"] == "invoice":
            assert len(doc["transactions"]) == 1
            txn = doc["transactions"][0]
            totals = doc["expected_totals"]
            subtotal = sum((D(li["amount"]) for li in txn["line_items"]), Decimal("0"))
            assert subtotal == D(totals["subtotal"]), (
                "%s: line items %s != subtotal %s" % (doc["file"], subtotal, totals["subtotal"]))
            for li in txn["line_items"]:
                assert D(li["qty"]) * D(li["unit_price"]) == D(li["amount"]), (
                    "%s line item qty*price != amount: %r" % (doc["file"], li))
            assert subtotal + D(totals["tax"]) == D(totals["total"]), (
                "%s: subtotal+tax != total" % doc["file"])
            assert D(txn["amount"]) == D(totals["total"]), (
                "%s: txn amount != total" % doc["file"])
            assert txn["description"].endswith(txn["invoice_number"]), (
                "%s: ledger description must carry the invoice number" % doc["file"])

        elif doc["doc_type"] == "payroll":
            assert len(doc["transactions"]) == 1
            txn = doc["transactions"][0]
            items = txn["line_items"]
            assert len(items) == 16, "payroll must have 16 rows, got %d" % len(items)
            gross = sum((D(li["amount"]) for li in items), Decimal("0"))
            for li in items:
                assert D(li["hours"]) * D(li["rate"]) == D(li["amount"]), (
                    "payroll row hours*rate != amount: %r" % li)
                assert Decimal("0") < D(li["hours"]) <= Decimal("24"), li
                assert D(li["rate"]) >= Decimal("0"), li
            totals = doc["expected_totals"]
            assert gross == D(totals["total_gross"]), (
                "payroll items %s != TOTAL GROSS %s" % (gross, totals["total_gross"]))
            assert D(txn["amount"]) == D(totals["total_gross"]), (
                "payroll txn amount != TOTAL GROSS")
            work_dates = sorted(li["work_date"] for li in items)
            expected_desc = "Weekly payroll %s to %s" % (work_dates[0], work_dates[-1])
            assert txn["description"] == expected_desc, (
                "payroll description %r != %r" % (txn["description"], expected_desc))
        else:  # pragma: no cover - corpus only holds the three types above
            raise AssertionError("unexpected doc_type %r" % doc["doc_type"])

    assert total_txns == 20, "expected 20 transactions total, got %d" % total_txns
    dup_numbers = {d["invoice_number"] for d in gt["expected_duplicates"]}
    assert dup_numbers == {"INV-1041"}, gt["expected_duplicates"]


# ---------------------------------------------------------------------------
# Style objects
# ---------------------------------------------------------------------------
_S_BASE = dict(fontName="Helvetica", fontSize=9, leading=10.5, textColor=colors.black)
S_H1 = ParagraphStyle("H1", fontName="Helvetica-Bold", fontSize=15, leading=18)
S_H2 = ParagraphStyle("H2", fontName="Helvetica-Bold", fontSize=11, leading=14)
S_SUB = ParagraphStyle("SUB", fontSize=9.5, leading=12)
S_SUB_BOLD = ParagraphStyle("SUBB", fontName="Helvetica-Bold", fontSize=9.5, leading=12)
S_NOTE = ParagraphStyle("NOTE", fontSize=8.5, leading=11,
                        textColor=colors.Color(0.35, 0.35, 0.35))
S_TOTAL = ParagraphStyle("TOTAL", fontName="Helvetica-Bold", fontSize=12,
                         leading=15, alignment=TA_RIGHT)
S_META_RIGHT = ParagraphStyle("META", fontSize=10, leading=13, alignment=TA_RIGHT)
S_TITLE_CENTER = ParagraphStyle("TITLEC", fontName="Helvetica-Bold", fontSize=13,
                                leading=16, alignment=TA_CENTER)
S_BANK_CELL = ParagraphStyle("BCELL", **_S_BASE)
S_BANK_CELL_R = ParagraphStyle("BCELLR", alignment=TA_RIGHT, **_S_BASE)
S_INV_DESC = ParagraphStyle("INVDESC", **_S_BASE)
S_INV_NUM_R = ParagraphStyle("INVNUMR", alignment=TA_RIGHT, **_S_BASE)

BANK_COL_WIDTHS = [58, 210, 56, 62, 62, 56]          # sums to 504 pt
INVOICE_COL_WIDTHS = [44, 286, 88, 86]               # sums to 504 pt
PAYROLL_COL_WIDTHS = [140, 100, 70, 100, 94]         # sums to 504 pt

_PAGE = letter
_MARGINS = dict(leftMargin=54, rightMargin=54, topMargin=54, bottomMargin=64)


def _footer(left_text: str, pages_total: int):
    """Build an onPage callback drawing consistent 'Page N of M' footers."""
    def _draw(canvas, _doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.Color(0.35, 0.35, 0.35))
        canvas.drawString(54, 40, left_text)
        canvas.drawRightString(558, 40, "Page %d of %d" % (canvas.getPageNumber(), pages_total))
        canvas.restoreState()
    return _draw


def _render(path: str, story: List[Any], footer, title: str, author: str) -> None:
    doc = SimpleDocTemplate(path, pagesize=_PAGE, title=title, author=author, **_MARGINS)
    doc.build(story, onFirstPage=footer, onLaterPages=footer)


# ---------------------------------------------------------------------------
# Bank statement (2 pages)
# ---------------------------------------------------------------------------
def _bank_table(rows: List[Tuple[int, Dict[str, Any], Decimal]]) -> Table:
    """rows: list of (gt_index, txn, running_balance_after)."""
    header = ["Date", "Description", "Ref", "Debit", "Credit", "Balance"]
    data: List[List[Any]] = [header]
    for gt_index, txn, balance in rows:
        pres = BANK_PRESENTATION[gt_index]
        desc_lines = pres["desc"]
        date_s = _mmddyyyy(txn["date"])
        ref_s = pres["ref"]
        amount = D(txn["amount"])
        if pres.get("paren"):
            debit_s = _paren(amount)          # (1,200.00) messiness, no $
        elif txn["direction"] == "out":
            debit_s = _money(amount)
        else:
            debit_s = ""
        credit_s = _money(amount) if txn["direction"] == "in" else ""
        balance_s = _money(balance)
        if len(desc_lines) == 2:
            # Two-line row: every cell is a Paragraph so all first lines
            # share one baseline (extracts as one row + one continuation).
            desc_cell = Paragraph(
                "<br/>".join(_esc(line) for line in desc_lines), S_BANK_CELL)
            row_cells = [Paragraph(_esc(date_s), S_BANK_CELL), desc_cell,
                         Paragraph(_esc(ref_s), S_BANK_CELL),
                         Paragraph(_esc(debit_s), S_BANK_CELL_R),
                         Paragraph(_esc(credit_s), S_BANK_CELL_R),
                         Paragraph(_esc(balance_s), S_BANK_CELL_R)]
        else:
            row_cells = [date_s, desc_lines[0], ref_s, debit_s, credit_s, balance_s]
        data.append(row_cells)

    table = Table(data, colWidths=BANK_COL_WIDTHS, repeatRows=0, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BACKGROUND", (0, 0), (-1, 0), colors.Color(0.90, 0.90, 0.90)),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
        ("ALIGN", (0, 0), (-1, 0), "LEFT"),
        ("ALIGN", (3, 1), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.Color(0.72, 0.72, 0.72)),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.Color(0.955, 0.955, 0.955)]),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
    ]))
    return table


def _bank_story(gt: Dict[str, Any]) -> List[Any]:
    doc = next(d for d in gt["documents"] if d["file"] == BANK_FILE)
    txns = doc["transactions"]
    totals = doc["expected_totals"]

    # Running balance chain (validated against expected_totals upstream).
    rows_page1: List[Tuple[int, Dict[str, Any], Decimal]] = []
    rows_page2: List[Tuple[int, Dict[str, Any], Decimal]] = []
    balance = BEGINNING_BALANCE
    for idx, txn in enumerate(txns):
        amount = D(txn["amount"])
        balance = balance + amount if txn["direction"] == "in" else balance - amount
        (rows_page1 if idx < 8 else rows_page2).append((idx, txn, balance))

    story: List[Any] = [
        Paragraph("PACIFIC TRUST BANK", S_H1),
        Paragraph("Business Checking Statement", S_SUB_BOLD),
        Paragraph("Account: 4471-220988", S_SUB),
        Paragraph("Statement period: 03/01/2026 - 03/31/2026", S_SUB),
        Spacer(1, 10),
        Paragraph("Beginning Balance:  %s" % _money(BEGINNING_BALANCE), S_SUB_BOLD),
        Spacer(1, 6),
        _bank_table(rows_page1),
        Spacer(1, 18),
        Paragraph("Message: Earn 3% cash back on eligible purchases with your "
                  "Pacific Trust Business Card.", S_NOTE),
        Paragraph("* Check images and deposit detail available in online banking.", S_NOTE),
        PageBreak(),
        Paragraph("PACIFIC TRUST BANK - Business Checking (continued)", S_H2),
        Spacer(1, 6),
        _bank_table(rows_page2),
        Spacer(1, 12),
        Paragraph("Ending Balance:  %s" % _money(D(totals["final_balance"])), S_SUB_BOLD),
        Spacer(1, 10),
        Paragraph("Statement Summary", S_SUB_BOLD),
        Paragraph("Total Deposits and Credits:  %s" % _money(D(totals["total_credits"])), S_SUB),
        Paragraph("Total Withdrawals and Debits:  %s" % _money(D(totals["total_debits"])), S_SUB),
        Spacer(1, 10),
        Paragraph("* Check images and deposit detail available in online banking.", S_NOTE),
    ]
    return story


# ---------------------------------------------------------------------------
# Invoices
# ---------------------------------------------------------------------------
def _invoice_items_table(txn: Dict[str, Any], pres: Dict[str, Any]) -> Table:
    data: List[List[Any]] = [["Qty", "Description", "Unit Price", "Amount"]]
    wrap_index = pres.get("wrapped_item_index")
    for i, li in enumerate(txn["line_items"]):
        qty_s = _num(D(li["qty"]))
        unit_s = _money(D(li["unit_price"]))
        amount_s = _money(D(li["amount"]))
        if wrap_index is not None and i == wrap_index:
            # The one deliberately wrapped two-line item description.
            row = [Paragraph(_esc(qty_s), S_BANK_CELL),
                   Paragraph(_esc(li["description"]), S_INV_DESC),
                   Paragraph(_esc(unit_s), S_INV_NUM_R),
                   Paragraph(_esc(amount_s), S_INV_NUM_R)]
        else:
            row = [qty_s, li["description"], unit_s, amount_s]
        data.append(row)

    table = Table(data, colWidths=INVOICE_COL_WIDTHS, repeatRows=1, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BACKGROUND", (0, 0), (-1, 0), colors.Color(0.90, 0.90, 0.90)),
        ("ALIGN", (0, 0), (0, -1), "CENTER"),
        ("ALIGN", (2, 0), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.Color(0.72, 0.72, 0.72)),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.Color(0.955, 0.955, 0.955)]),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
    ]))
    return table


def _invoice_story(gt: Dict[str, Any], file_name: str) -> List[Any]:
    doc = next(d for d in gt["documents"] if d["file"] == file_name)
    txn = doc["transactions"][0]
    totals = doc["expected_totals"]
    pres = INVOICE_PRESENTATION[file_name]

    meta_table = Table(
        [[line.format(inv=txn["invoice_number"], date=_mmddyyyy(txn["date"]))]
         for line in pres["meta_lines"]],
        colWidths=[230], hAlign="RIGHT")
    meta_table.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("ALIGN", (0, 0), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 1),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
    ]))

    left_block = [Paragraph(_esc(pres["vendor_header"]), S_H1)]
    left_block += [Paragraph(_esc(line), S_SUB) for line in pres["address_lines"]]
    header = Table([[left_block, meta_table]], colWidths=[274, 230], hAlign="LEFT")
    header.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
    ]))

    story: List[Any] = [
        header,
        Spacer(1, 14),
        Paragraph("INVOICE", S_TITLE_CENTER),
        Spacer(1, 10),
        _invoice_items_table(txn, pres),
        Spacer(1, 14),
        Paragraph("Subtotal:   %s" % _money(D(totals["subtotal"])), S_SUB),
    ]
    if D(totals["tax"]) != Decimal("0") and pres.get("tax_label"):
        story.append(Paragraph("%s   %s" % (pres["tax_label"], _money(D(totals["tax"]))), S_SUB))
    story.append(Paragraph("TOTAL:   %s" % _money(D(totals["total"])), S_TOTAL))
    story.append(Spacer(1, 16))
    for line in pres["note_lines"]:
        story.append(Paragraph(_esc(line), S_NOTE))
    return story


# ---------------------------------------------------------------------------
# Payroll
# ---------------------------------------------------------------------------
def _payroll_story(gt: Dict[str, Any]) -> List[Any]:
    doc = next(d for d in gt["documents"] if d["file"] == PAYROLL_FILE)
    txn = doc["transactions"][0]
    items = txn["line_items"]

    data: List[List[Any]] = [["Employee", "Work Date", "Hours", "Rate", "Gross"]]
    for li in items:
        data.append([
            li["employee"],
            _mmddyyyy(li["work_date"]),
            _num(D(li["hours"])),
            _money(D(li["rate"])),
            _money(D(li["amount"])),
        ])
    table = Table(data, colWidths=PAYROLL_COL_WIDTHS, repeatRows=1, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BACKGROUND", (0, 0), (-1, 0), colors.Color(0.90, 0.90, 0.90)),
        ("ALIGN", (2, 0), (-1, -1), "RIGHT"),
        ("ALIGN", (1, 1), (1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.Color(0.72, 0.72, 0.72)),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.Color(0.955, 0.955, 0.955)]),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
    ]))

    period_start = min(li["work_date"] for li in items)
    period_end = max(li["work_date"] for li in items)
    story: List[Any] = [
        Paragraph("BRISTLECONE LANDSCAPING", S_H1),
        Paragraph("Weekly Payroll Summary", S_SUB_BOLD),
        Paragraph("Pay Period: %s - %s" % (_mmddyyyy(period_start), _mmddyyyy(period_end)), S_SUB),
        Paragraph("Pay Date: %s" % _mmddyyyy(txn["date"]), S_SUB),
        Spacer(1, 12),
        table,
        Spacer(1, 12),
        Paragraph("TOTAL GROSS:   %s" % _money(D(doc["expected_totals"]["total_gross"])), S_TOTAL),
        Spacer(1, 12),
        Paragraph("Note: Direct deposits post on pay date.", S_NOTE),
    ]
    return story


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def generate_all(out_dir: str = SAMPLE_DIR_DEFAULT) -> Dict[str, Any]:
    """Write the six sample PDFs and ground_truth/ground_truth.json.

    Idempotent: overwrites existing files cleanly. All facts come from
    GROUND_TRUTH; internal consistency is asserted (Decimal) before any
    file is written.

    :param out_dir: directory for the PDFs (absolute, or relative to this
        module's project directory).
    :returns: ``{"files": [pdf paths...], "ground_truth_path": path}``
    """
    gt = copy.deepcopy(GROUND_TRUTH)
    gt["generated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _validate_ground_truth(gt)

    pdf_dir = out_dir if os.path.isabs(out_dir) else os.path.join(PROJECT_ROOT, out_dir)
    gt_dir = os.path.join(PROJECT_ROOT, GROUND_TRUTH_DIR)
    os.makedirs(pdf_dir, exist_ok=True)
    os.makedirs(gt_dir, exist_ok=True)

    jobs = [
        (BANK_FILE, _bank_story(gt), _footer("PACIFIC TRUST BANK  |  Member FDIC", 2),
         "Pacific Trust Bank - Business Checking Statement"),
        (INVOICE_ABC_FILE, _invoice_story(gt, INVOICE_ABC_FILE),
         _footer("ABC Lumber Co.", 1), "ABC Lumber Co. - Invoice INV-1041"),
        (INVOICE_SIERRA_FILE, _invoice_story(gt, INVOICE_SIERRA_FILE),
         _footer("Sierra Building Supply", 1), "Sierra Building Supply - Invoice INV-2077"),
        (INVOICE_HILTI_FILE, _invoice_story(gt, INVOICE_HILTI_FILE),
         _footer("HILTI EQUIPMENT RENTAL", 1), "Hilti Equipment Rental - Invoice INV-5532"),
        (PAYROLL_FILE, _payroll_story(gt),
         _footer("BRISTLECONE LANDSCAPING  |  Payroll", 1),
         "Bristlecone Landscaping - Weekly Payroll 2026-03-02"),
        (INVOICE_RESEND_FILE, _invoice_story(gt, INVOICE_RESEND_FILE),
         _footer("ABC LUMBER SUPPLY INC", 1),
         "ABC Lumber Supply Inc - Invoice INV-1041 (re-sent copy)"),
    ]
    files: List[str] = []
    for file_name, story, footer, title in jobs:
        path = os.path.join(pdf_dir, file_name)
        _render(path, story, footer, title, "Doc-to-Warehouse sample generator")
        files.append(path)

    ground_truth_path = os.path.join(gt_dir, "ground_truth.json")
    with open(ground_truth_path, "w", encoding="utf-8") as fh:
        json.dump(gt, fh, indent=2, ensure_ascii=True)
        fh.write("\n")

    logger.info("generated %d PDFs into %s", len(files), pdf_dir)
    return {"files": files, "ground_truth_path": ground_truth_path}


# ---------------------------------------------------------------------------
# Round-trip verification (pdfplumber). Run from __main__ (prints), or import
# and call for a structured {"ok": bool, "problems": [...], "details": [...]}.
# ---------------------------------------------------------------------------
def verify_samples(ground_truth_path: str = None, sample_dir: str = None) -> Dict[str, Any]:
    """Re-read every generated PDF with pdfplumber and check the round trip.

    For each document: every ground-truth amount (transaction amounts, line
    items, expected totals) and every transaction date string must appear in
    the extracted text; per-document transaction counts must match ground
    truth (bank: unique amounts/refs; invoices: invoice number present once;
    payroll: 4 rows per employee); the bank statement must be exactly 2
    pages; the deliberate wrap/paren/noise messiness must be present.
    """
    import pdfplumber

    if ground_truth_path is None:
        ground_truth_path = os.path.join(PROJECT_ROOT, GROUND_TRUTH_DIR, "ground_truth.json")
    if sample_dir is None:
        sample_dir = os.path.join(PROJECT_ROOT, SAMPLE_DIR_DEFAULT)
    with open(ground_truth_path, "r", encoding="utf-8") as fh:
        gt = json.load(fh)

    problems: List[str] = []
    details: List[str] = []

    def _need(text: str, needle: str, count: int, what: str, doc_file: str,
              at_least: bool = False) -> None:
        found = text.count(needle)
        ok = found >= count if at_least else found == count
        if not ok:
            problems.append("%s: %s -> expected %s%d occurrence(s) of %r, found %d"
                            % (doc_file, what, "at least " if at_least else "", count,
                               needle, found))

    for doc in gt["documents"]:
        file_name = doc["file"]
        path = os.path.join(sample_dir, file_name)
        if not os.path.exists(path):
            problems.append("%s: file missing" % file_name)
            continue
        with pdfplumber.open(path) as pdf:
            pages = pdf.pages
            text = "\n".join((page.extract_text() or "") for page in pages)
            lines = text.splitlines()
        txns = doc["transactions"]
        details.append("%s: pages=%d txns=%d" % (file_name, len(pages), len(txns)))

        if file_name == BANK_FILE and len(pages) != 2:
            problems.append("%s: must be exactly 2 pages, found %d" % (file_name, len(pages)))

        # 1. Every transaction amount appears in the extracted text.
        for idx, txn in enumerate(txns):
            fmt = "{:,.2f}".format(D(txn["amount"]))
            if file_name == BANK_FILE:
                if BANK_PRESENTATION[idx].get("paren"):
                    _need(text, _paren(D(txn["amount"])), 1, "transaction amount", file_name)
                else:
                    _need(text, _money(D(txn["amount"])), 1, "transaction amount", file_name)
            else:
                _need(text, fmt, 1, "transaction amount", file_name, at_least=True)
            # 2. Every transaction date string appears (printed MM/DD/YYYY).
            _need(text, _mmddyyyy(txn["date"]), 1, "transaction date", file_name, at_least=True)

        # Line items / per-document specifics.
        if file_name == BANK_FILE:
            for idx, txn in enumerate(txns):
                pres = BANK_PRESENTATION[idx]
                if pres["ref"]:
                    _need(text, pres["ref"], 1, "row ref", file_name)
            totals = doc["expected_totals"]
            _need(text, _money(BEGINNING_BALANCE), 1, "beginning balance", file_name, at_least=True)
            _need(text, _money(D(totals["total_credits"])), 1, "total credits", file_name,
                  at_least=True)
            _need(text, _money(D(totals["total_debits"])), 1, "total debits", file_name,
                  at_least=True)
            _need(text, _money(D(totals["final_balance"])), 1, "final balance", file_name,
                  at_least=True)
            _need(text, "(1,200.00)", 1, "parenthesized amount", file_name)
            _need(text, "Message: Earn 3% cash back", 1, "memo noise line", file_name)
            _need(text, "Page 1 of 2", 1, "footer", file_name)
            _need(text, "Page 2 of 2", 1, "footer", file_name)
            _need(text, "Beginning Balance", 1, "beginning balance label", file_name)
            _need(text, "Ending Balance", 1, "ending balance label", file_name)
            # Deliberate wraps: both halves present, on different lines.
            for first, second in (("ACH - BrightWave Digital", "Advertising platform *"),
                                  ("ACH - SureGuard Insurance", "Services premium *")):
                first_lines = [ln for ln in lines if first in ln]
                if len(first_lines) != 1 or second in first_lines[0]:
                    problems.append("%s: wrapped description %r not split as expected"
                                    % (file_name, first))
                if not any(second in ln for ln in lines):
                    problems.append("%s: wrapped continuation %r missing" % (file_name, second))
        elif doc["doc_type"] == "invoice":
            txn = doc["transactions"][0]
            _need(text, txn["invoice_number"], 1, "invoice number", file_name)
            for li in txn["line_items"]:
                _need(text, "{:,.2f}".format(D(li["unit_price"])), 1, "unit price",
                      file_name, at_least=True)
                _need(text, "{:,.2f}".format(D(li["amount"])), 1, "line item amount",
                      file_name, at_least=True)
            totals = doc["expected_totals"]
            _need(text, _money(D(totals["subtotal"])), 1, "subtotal", file_name, at_least=True)
            if D(totals["tax"]) != Decimal("0"):
                _need(text, _money(D(totals["tax"])), 1, "tax", file_name, at_least=True)
            _need(text, _money(D(totals["total"])), 1, "total", file_name, at_least=True)
            if file_name in (INVOICE_ABC_FILE, INVOICE_RESEND_FILE):
                head = "Treated 4x4x10 post"
                tail = "per each"
                head_lines = [ln for ln in lines if head in ln]
                if len(head_lines) != 1 or tail in head_lines[0]:
                    problems.append("%s: wrapped item description not split as expected"
                                    % file_name)
                if not any(tail in ln for ln in lines):
                    problems.append("%s: wrapped item continuation missing" % file_name)
            if file_name == INVOICE_RESEND_FILE:
                _need(text, "Re-sent copy", 1, "re-send note", file_name)
        else:  # payroll
            employees = {}
            for li in txns[0]["line_items"]:
                employees[li["employee"]] = employees.get(li["employee"], 0) + 1
            for employee, expected_rows in employees.items():
                _need(text, employee, expected_rows, "employee rows", file_name)
            for li in txns[0]["line_items"]:
                _need(text, _mmddyyyy(li["work_date"]), 1, "work date", file_name, at_least=True)
                _need(text, _money(D(li["amount"])), 1, "gross amount", file_name, at_least=True)
            _need(text, "TOTAL GROSS", 1, "total gross label", file_name)
            _need(text, "{:,.2f}".format(D(doc["expected_totals"]["total_gross"])), 1,
                  "total gross amount", file_name, at_least=True)
            _need(text, "Note: Direct deposits post on pay date.", 1, "memo noise line",
                  file_name)

    details.append("total transactions: %d"
                   % sum(len(d["transactions"]) for d in gt["documents"]))
    return {"ok": not problems, "problems": problems, "details": details}


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    result = generate_all()
    print("Generated sample corpus:")
    for path in result["files"]:
        print("  %s" % path)
    print("  %s" % result["ground_truth_path"])

    print("Verifying round-trip with pdfplumber ...")
    report = verify_samples(result["ground_truth_path"],
                            os.path.join(PROJECT_ROOT, SAMPLE_DIR_DEFAULT))
    for line in report["details"]:
        print("  %s" % line)
    if report["ok"]:
        print("VERIFICATION PASSED: all ground-truth amounts, dates, counts and "
              "messiness markers round-trip through pdfplumber.")
        return 0
    print("VERIFICATION FAILED (%d problem(s)):" % len(report["problems"]))
    for problem in report["problems"]:
        print("  - %s" % problem)
    return 1


if __name__ == "__main__":
    sys.exit(main())
