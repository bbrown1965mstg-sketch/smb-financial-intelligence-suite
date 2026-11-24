"""Rule-based extraction of financial transactions from PDF documents.

Pipeline (per PDF):

1. Load pages with pdfplumber (text + word geometry). If pdfplumber fails or
   yields no text, fall back to pypdf text extraction (no coordinates).
2. Pick a document type for the whole PDF from layout cue scores
   (``detect_doc_type``) and route to a dedicated parser:

   * ``bank_statement`` -- x-coordinate-aware table extraction
     (Date | Description | Ref | Debit | Credit | Balance). Falls back to
     right-to-left text heuristics when no column geometry is available.
     Handles ``$1,234.56`` commas, ``(1,200.00)`` parenthesized negatives,
     trailing footnote asterisks, continuation/memo lines, and cross-checks
     every row against the running balance.
   * ``invoice`` -- header fields (vendor, Invoice #, Invoice Date), an items
     table (Qty | Description | Unit Price | Amount) with wrapped two-line
     descriptions merged, and Subtotal/Tax/TOTAL reconciliation.
   * ``payroll`` -- flat grid (Employee | Work Date | Hours | Rate | Gross)
     rolled up into ONE aggregate transaction with per-employee line items.

3. Optional GLM/LLM fallback (``llm_extract_page``) for pages that yield
   nothing or only low-confidence rows. It is never required: the bundled
   sample PDFs extract 100% with the rule-based engine and no API key.

Every extracted transaction is a plain dict matching CONTRACT.md §5 exactly
(categorizer fields are present but ``None``/``False`` until
``categorizer.categorize_all`` runs). The parser never raises on malformed
content; it logs and skips what it cannot parse.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

try:  # primary extraction engine
    import pdfplumber
except ImportError:  # pragma: no cover - pdfplumber is in requirements.txt
    pdfplumber = None

try:  # LLM fallback transport
    import requests
except ImportError:  # pragma: no cover
    requests = None

logger = logging.getLogger("doc_to_warehouse.pdf_parser")

REVIEW_THRESHOLD = 0.85

DOC_TYPES = ("bank_statement", "invoice", "payroll", "credit_card")

# ---------------------------------------------------------------------------
# Confidence bands (CONTRACT.md §6 guidance)
# ---------------------------------------------------------------------------
CONF_CLEAN = 0.95            # clean structured-table hit
CONF_COMMA_PAREN = 0.90      # comma / parenthesis amount handling involved
CONF_TEXT_FALLBACK = 0.82    # no column geometry available (heuristic path)
CONF_MERGED_CAP = 0.88       # bank description merged from continuation lines
CONF_WRAPPED_CAP = 0.85      # wrapped two-line invoice item description
CONF_MISREAD_CAP = 0.70      # direction contradicts the balance column
CONF_NO_DELTA_PAREN = 0.70   # paren amount with unresolved direction
CONF_AMBIGUOUS = 0.65        # malformed row / balance cross-check failure
CONF_LOW = 0.60              # rows that came from the LLM fallback
CONF_MISSING_TOTAL_CAP = 0.78  # TOTAL line absent (amount derived)
CONF_MISSING_DATE_CAP = 0.80
CONF_NO_INVOICE_NUM = 0.75
CONF_NO_FOOTER_CAP = 0.80    # payroll TOTAL GROSS footer absent

_FOOTNOTE_CHARS = "*\u2020\u2021"  # * † ‡

# ---------------------------------------------------------------------------
# Date normalization
# ---------------------------------------------------------------------------

_MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

_ISO_DATE_RE = re.compile(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b")
_SLASH_DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4}|\d{2})\b")
_MDY_NAME_RE = re.compile(
    r"\b([A-Za-z]{3,9})\.?\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b")
_DMY_NAME_RE = re.compile(
    r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9})\.?,?\s+(\d{4})\b")
# Loose "find any date in a line" pattern (used by the payroll/invoice parsers).
_DATE_FIND_RE = re.compile(
    r"(\d{4}-\d{1,2}-\d{1,2}"
    r"|\d{1,2}/\d{1,2}/\d{2,4}"
    r"|[A-Za-z]{3,9}\.?\s+\d{1,2},?\s+\d{4}"
    r"|\d{1,2}\s+[A-Za-z]{3,9}\.?,?\s+\d{4})")


def _month_num(token: str) -> Optional[int]:
    """Resolve a month name/abbreviation token to 1-12 (or None)."""
    t = token.strip(".").lower()
    if t in _MONTHS:
        return _MONTHS[t]
    if t[:3] in _MONTHS:
        return _MONTHS[t[:3]]
    return None


def _mk_date(year: Any, month: Any, day: Any) -> Optional[str]:
    """Validate y/m/d ints and return YYYY-MM-DD, else None."""
    try:
        y, m, d = int(year), int(month), int(day)
        _dt.date(y, m, d)
        return "%04d-%02d-%02d" % (y, m, d)
    except (ValueError, TypeError):
        return None


def normalize_date(raw: str) -> str:
    """Normalize a date string to ``YYYY-MM-DD`` (``""`` when unparseable).

    Supported: ``YYYY-MM-DD``, ``MM/DD/YYYY``, ``MM/DD/YY`` (pivot: <70 ->
    20xx), ``Mar 4, 2026`` / ``March 4, 2026`` and ``4 Mar 2026``.
    """
    if not raw:
        return ""
    s = str(raw).strip().strip(",;")

    m = _ISO_DATE_RE.search(s)
    if m:
        out = _mk_date(m.group(1), m.group(2), m.group(3))
        if out:
            return out

    m = _SLASH_DATE_RE.search(s)
    if m:
        year = m.group(3)
        if len(year) == 2:
            year = ("20" + year) if int(year) < 70 else ("19" + year)
        out = _mk_date(year, m.group(1), m.group(2))
        if out:
            return out

    m = _MDY_NAME_RE.search(s)
    if m:
        mon = _month_num(m.group(1))
        if mon:
            out = _mk_date(m.group(3), mon, m.group(2))
            if out:
                return out

    m = _DMY_NAME_RE.search(s)
    if m:
        mon = _month_num(m.group(2))
        if mon:
            out = _mk_date(m.group(3), mon, m.group(1))
            if out:
                return out
    return ""


# ---------------------------------------------------------------------------
# Amount parsing
# ---------------------------------------------------------------------------

_AMOUNT_TOKEN_RE = re.compile(
    r"^\(?\s*\$?\s*-?\d[\d,]*(?:\.\d+)?\s*\)?[" + re.escape(_FOOTNOTE_CHARS) + r"]*$")


def parse_amount_token(token: Any) -> Optional[Tuple[Decimal, bool]]:
    """Parse an amount token.

    Accepts ``1234.56``, ``$1,234.56``, ``(1,200.00)`` (negative),
    ``56.78**`` (footnote marks). Returns ``(signed Decimal, is_paren)``
    or ``None`` when the token is not an amount.
    """
    if token is None:
        return None
    s = str(token).strip()
    if not s:
        return None
    s = s.strip(_FOOTNOTE_CHARS).strip()
    paren = s.startswith("(") and s.endswith(")")
    if paren:
        s = s[1:-1]
    s = s.replace("$", "").replace(",", "").replace(" ", "")
    s = s.strip(_FOOTNOTE_CHARS).strip()
    if not _AMOUNT_TOKEN_RE_MATCH(s):
        return None
    try:
        val = Decimal(s)
    except InvalidOperation:
        return None
    if paren and val > 0:
        val = -val
    return val, paren


def _AMOUNT_TOKEN_RE_MATCH(s: str) -> bool:
    return bool(re.fullmatch(r"-?\d+(\.\d+)?", s))


def _money_text_is_dirty(text: str) -> bool:
    """True when amount text required comma/paren/footnote handling."""
    return any(ch in text for ch in (",", "(", ")", "$", "*"))


# ---------------------------------------------------------------------------
# Transaction construction (CONTRACT.md §5 shape)
# ---------------------------------------------------------------------------

def _make_txn(source_file: str, page: int, row_index: int, doc_type: str,
              date: str, vendor: str, description: str, amount: Any,
              direction: str, invoice_number: str = "",
              line_items: Optional[List[Dict]] = None, raw_text: str = "",
              confidence: float = 0.9) -> Dict:
    """Build one §5 transaction dict. Amount is stored as a positive float."""
    try:
        amt = Decimal(str(amount)).quantize(Decimal("0.01"),
                                            rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        amt = Decimal("0.00")
    conf = float(max(0.0, min(1.0, confidence)))
    return {
        "txn_id": f"{source_file}:{page}:{row_index}",
        "source_file": source_file,
        "page": int(page),
        "doc_type": doc_type,
        "date": date or "",
        "vendor": (vendor or "").strip(),
        "description": (description or "").strip(),
        "amount": float(abs(amt)),
        "direction": direction if direction in ("in", "out") else "out",
        "invoice_number": (invoice_number or "").strip(),
        "line_items": line_items or [],
        "raw_text": (raw_text or "")[:300],
        # --- categorizer fields: absent/None at extraction stage -----------
        "category": None,
        "overhead_subtype": None,
        "vendor_normalized": None,
        "confidence": conf,
        "needs_review": conf < REVIEW_THRESHOLD,
        "is_duplicate": False,
        "duplicate_of": None,
    }


# ---------------------------------------------------------------------------
# Document-type detection
# ---------------------------------------------------------------------------

_DOC_TYPE_CUES = {
    "invoice": (
        (r"invoice\s*(?:#|no\.?|number)", 4), (r"\binvoice\b", 2),
        (r"invoice\s*date", 3), (r"unit\s+price", 3), (r"sub\s*-?\s*total", 3),
        (r"bill\s+to", 2), (r"sold\s+to", 1), (r"amount\s+due", 2),
        (r"balance\s+due", 2), (r"\bqty\b", 2), (r"quantity", 2),
    ),
    "payroll": (
        (r"work\s*date", 3), (r"total\s+gross", 3), (r"pay\s*period", 2),
        (r"period\s+ending", 2), (r"week\s+ending", 2),
        (r"employee\s*id", 2), (r"\bemployees?\b", 2), (r"payroll", 2),
        (r"\bhours\b", 1), (r"\brate\b", 1), (r"\bgross\b", 1),
    ),
    "credit_card": (
        (r"credit\s+card", 4), (r"new\s+balance", 3),
        (r"minimum\s+payment", 3), (r"card\s+number", 2),
        (r"statement\s+period", 2), (r"cash\s+advances", 2),
        (r"\bpurchases\b", 2), (r"payment\s+due\s+date", 2),
    ),
    "bank_statement": (
        (r"bank\s+statement", 3), (r"beginning\s+balance", 3),
        (r"ending\s+balance", 2), (r"previous\s+balance", 2),
        (r"account\s+(?:number|summary|activity)", 2), (r"statement", 2),
        (r"\bdeposits?\b", 2), (r"\bwithdrawals?\b", 2),
        (r"checks?\s+paid", 2), (r"member\s+fdic", 2),
        (r"\bbalance\b", 1), (r"\bdebits?\b", 1), (r"\bcredits?\b", 1),
        (r"\bref\b", 1),
    ),
}
_DOC_TYPE_PRIORITY = ("invoice", "payroll", "credit_card", "bank_statement")


def _score_doc_type(text: str) -> Dict[str, int]:
    scores: Dict[str, int] = {}
    for doc_type, cues in _DOC_TYPE_CUES.items():
        s = 0
        for pattern, weight in cues:
            if re.search(pattern, text, re.I):
                s += weight
        scores[doc_type] = s
    return scores


def detect_doc_type(text: str) -> str:
    """Classify page text into one of the four doc_type values.

    Weighted layout-cue scoring; ties resolved by priority
    invoice > payroll > credit_card > bank_statement. Unrecognized text
    defaults to ``bank_statement`` (the most generic financial document).
    """
    scores = _score_doc_type(text or "")
    best = max(_DOC_TYPE_PRIORITY, key=lambda dt: scores.get(dt, 0))
    if scores.get(best, 0) < 3:
        return "bank_statement"
    return best


def _document_doc_type(texts: List[str]) -> str:
    """Doc-level type: sum per-page cue scores across the whole PDF."""
    totals: Dict[str, int] = {dt: 0 for dt in _DOC_TYPE_PRIORITY}
    for text in texts:
        for dt, s in _score_doc_type(text or "").items():
            totals[dt] = totals.get(dt, 0) + s
    best = max(_DOC_TYPE_PRIORITY, key=lambda dt: totals.get(dt, 0))
    if totals.get(best, 0) < 3:
        return "bank_statement"
    return best


# ---------------------------------------------------------------------------
# Shared line utilities
# ---------------------------------------------------------------------------

def _cluster_lines(words: List[Dict], tol: float = 3.0) -> List[Dict]:
    """Group pdfplumber words into visual lines (top coordinate clusters)."""
    ordered = sorted(words, key=lambda w: (w.get("top", 0.0), w.get("x0", 0.0)))
    groups: List[List[Dict]] = []
    for w in ordered:
        if groups and abs(groups[-1][0].get("top", 0.0) - w.get("top", 0.0)) <= tol:
            groups[-1].append(w)
        else:
            groups.append([w])
    lines = []
    for g in groups:
        g.sort(key=lambda w: w.get("x0", 0.0))
        lines.append({
            "top": g[0].get("top", 0.0),
            "words": g,
            "text": " ".join(w.get("text", "") for w in g),
        })
    lines.sort(key=lambda l: l["top"])
    return lines


def _strip_footnotes(text: str) -> str:
    return re.sub(r"[" + re.escape(_FOOTNOTE_CHARS) + r"]+\s*$", "",
                  text.strip()).strip()


# Bank memo/footer noise: never transactions, never merged into descriptions.
_NOISE_PATTERNS = [
    r"^\s*message\s*[:\-]",
    r"\bthank\s+you\b",
    r"^\s*page\s*\d+\s*(?:of\s*\d+)?$",
    r"\bpage\s+\d+\s+of\s+\d+\b",
    r"^\s*beginning\s+balance\b",
    r"^\s*ending\s+balance\b",
    r"^\s*previous\s+balance\b",
    r"^\s*balance\s+forward\b",
    r"^\s*totals?\s*[:\-]?\s*[\d$,.(]*\s*$",
    r"^\s*[-=_*]{3,}\s*$",
    r"^\s*[" + re.escape(_FOOTNOTE_CHARS) + r"]",  # footnote-only line start
    r"^\s*member\s+fdic\b",
    r"^\s*account\s*[:#]|^\s*account\s+(?:number|summary|activity|statement)",
    r"^\s*statement\s+(?:period|date|of\s+account|summary)",
    r"^\s*total\s+(?:deposits|withdrawals|credits|debits)\b",
    r"\(\s*continued\s*\)",
    r"^\s*(?:daily\s+balance|deposits?\s+and\s+other\s+credits|"
    r"withdrawals?\s+and\s+other\s+debits|checks?\s+paid|"
    r"service\s+charges?\s+and\s+fees|other\s+debits|other\s+credits|"
    r"fees?\s+charged|electronic\s+activity)\b",
    r"^\s*\d{1,3}\s*$",
]
_NOISE_RES = [re.compile(p, re.I) for p in _NOISE_PATTERNS]

_BEGIN_BAL_RE = re.compile(
    r"beginning\s+balance|previous\s+balance|balance\s+forward", re.I)


def _is_noise_line(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return True
    return any(rx.search(t) for rx in _NOISE_RES)


def _balance_anchor(text: str) -> Optional[Decimal]:
    """Extract the starting balance from a 'Beginning balance ...' line."""
    if _BEGIN_BAL_RE.search(text or ""):
        for tok in reversed(text.split()):
            parsed = parse_amount_token(tok)
            if parsed is not None:
                return abs(parsed[0])
    return None


# ---------------------------------------------------------------------------
# Payee/description splitting (bank rows)
# ---------------------------------------------------------------------------

# Bank statements in this corpus print the description cell as
# "<type> - <vendor name>[ <extra words>]" and put the payee on a
# continuation line when the cell has no name. Ground truth for the sample
# corpus: description = the FULL merged cell text (row + wrapped
# continuation, footnote marks stripped); vendor = the name portion, i.e.
# the leading capitalized run after "<type> - ", or a standalone name-like
# continuation line ("HARBORVIEW HOA") when the cell has no capitalized run.


def _is_name_like(text: str) -> bool:
    """True when every token starts uppercase/digit (a standalone payee)."""
    tokens = (text or "").strip().split()
    if not 1 <= len(tokens) <= 5:
        return False
    return all(re.match(r"^[A-Z0-9&']", tok) for tok in tokens)


def _caps_run(text: str, limit: int = 6) -> str:
    """The leading run of capitalized tokens of ``text`` (may be empty)."""
    tokens: List[str] = []
    for tok in (text or "").split():
        if not re.match(r"^[A-Z0-9&']", tok):
            break
        tokens.append(tok)
        if len(tokens) >= limit:
            break
    return " ".join(tokens)


def _bank_vendor_and_desc(desc_text: str,
                          conts: List[str]) -> Tuple[str, str, bool]:
    """Resolve (vendor, description, wrapped) for one bank row.

    ``conts`` are the continuation/memo lines collected under the row.
    Wrapped continuation lines (mixed case) merge into the description;
    standalone name-like lines become the vendor when the cell text itself
    carries no capitalized name run.
    """
    clean = re.sub(r"[" + re.escape(_FOOTNOTE_CHARS) + r"]", "", desc_text)
    clean = re.sub(r"\s+", " ", clean).strip(" -\u2013\u2014").strip()
    name_conts: List[str] = []
    wrapped = False
    for cont in conts:
        c = _strip_footnotes(cont)
        c = re.sub(r"\s+", " ", c).strip()
        if not c:
            continue
        if _is_name_like(c):
            name_conts.append(c)
        else:
            clean = (clean + " " + c).strip()
            wrapped = True
    vendor = ""
    m = re.match(r"^(.+?)\s+[-\u2013\u2014]\s+(.+)$", clean)
    if m:
        vendor = _caps_run(m.group(2))
    if not vendor and name_conts:
        vendor = name_conts[0]
    if not vendor:
        vendor = _caps_run(clean)
    return vendor, clean, wrapped


# ---------------------------------------------------------------------------
# Bank statement parsing
# ---------------------------------------------------------------------------

_BANK_HEADER_MAP = {
    "date": "date", "description": "description", "descriptions": "description",
    "ref": "ref", "ref#": "ref", "debit": "debit", "debits": "debit",
    "credit": "credit", "credits": "credit", "balance": "balance",
}


def _bank_column_regions(header_words: List[Dict]) -> Optional[Dict[str, Tuple[float, float]]]:
    """Build column x-regions from a header line, or None if not a header.

    Each column's region starts at its header word's LEFT edge and ends at
    the next header's left edge. Header-center midpoints are unreliable here:
    description cell text often overflows far to the right of its (narrow)
    header word ("Ironclad Subcontractors LLC"), while numeric cells are
    drawn at or right of their own column edge.
    """
    anchors: List[Tuple[float, float, str]] = []
    seen = set()
    for w in header_words:
        t = re.sub(r"[:#]+$", "", w.get("text", "")).strip().lower()
        key = _BANK_HEADER_MAP.get(t)
        if key and key not in seen:
            seen.add(key)
            anchors.append((w["x0"], w["x1"], key))
    if not {"date", "debit", "credit", "balance"} <= seen:
        return None
    anchors.sort()
    bounds: Dict[str, Tuple[float, float]] = {}
    for i, (x0, _x1, key) in enumerate(anchors):
        left = 0.0 if i == 0 else x0
        right = (anchors[i + 1][0] if i + 1 < len(anchors) else float("inf"))
        bounds[key] = (left, right)
    return bounds


def _assign_column(words: List[Dict],
                   bounds: Dict[str, Tuple[float, float]]) -> Dict[str, List[Dict]]:
    cols = {k: [] for k in ("date", "description", "ref", "debit", "credit", "balance")}
    debit_left = bounds.get("debit", (float("inf"), float("inf")))[0]
    for w in words:
        center = (w["x0"] + w["x1"]) / 2.0
        target = None
        for key, (left, right) in bounds.items():
            if left <= center < right:
                target = key
                break
        if target is None:  # gap between defined regions
            target = "description" if center < debit_left else "balance"
        cols[target].append(w)
    return cols


def _first_amount(words: List[Dict]) -> Optional[Tuple[Decimal, bool]]:
    for w in words:
        parsed = parse_amount_token(w.get("text", ""))
        if parsed is not None:
            return parsed
    return None


def _last_amount(words: List[Dict]) -> Optional[Tuple[Decimal, bool]]:
    for w in reversed(words):
        parsed = parse_amount_token(w.get("text", ""))
        if parsed is not None:
            return parsed
    return None


def _interpret_bank_amounts(
        debit: Optional[Tuple[Decimal, bool]],
        credit: Optional[Tuple[Decimal, bool]],
        balance: Optional[Tuple[Decimal, bool]],
        prev_bal: Optional[Decimal],
) -> Optional[Tuple[Decimal, str, Optional[float], Optional[bool]]]:
    """Resolve one bank row's ``(amount, direction, conf_cap, balance_fit)``.

    ``balance_fit`` is True when the chosen interpretation reproduces the
    printed balance, False when it contradicts it, None when unknowable.
    Parenthesized amounts in the Debit-position column are resolved as a
    credit/refund (direction=in) or a negative debit (direction=out) via the
    balance delta; without a delta they default to a refund at low
    confidence.
    """
    candidates = []
    if debit is not None and debit[0] != 0:
        candidates.append(("debit", abs(debit[0]), debit[1]))
    if credit is not None and credit[0] != 0:
        candidates.append(("credit", abs(credit[0]), credit[1]))
    if not candidates:
        return None
    bal = abs(balance[0]) if balance is not None else None

    def fits(direction: str, val: Decimal) -> Optional[bool]:
        if prev_bal is None or bal is None:
            return None
        expected = prev_bal + val if direction == "in" else prev_bal - val
        return abs(expected - bal) <= Decimal("0.01")

    if len(candidates) == 1:
        col, val, paren = candidates[0]
        default_dir = "in" if col == "credit" else "out"
        if paren:
            order = ["in", "out"] if col == "debit" else ["out", "in"]
        else:
            order = ["out", "in"] if col == "debit" else ["in", "out"]
        for d in order:
            f = fits(d, val)
            if f:
                cap = None
                if d != default_dir and not paren:
                    cap = CONF_MISREAD_CAP  # balance says the column was misread
                return val, d, cap, True
        if prev_bal is None or bal is None:
            cap = CONF_NO_DELTA_PAREN if paren else None
            return val, order[0], cap, None
        return val, order[0], CONF_AMBIGUOUS, False

    # Both debit and credit non-zero -> malformed row; try to attribute via
    # the balance delta, otherwise low confidence on the debit candidate.
    for _col, val, _paren in candidates:
        for d in ("in", "out"):
            if fits(d, val):
                return val, d, CONF_AMBIGUOUS, True
    known_balance = prev_bal is not None and bal is not None
    return (candidates[0][1], "out", CONF_AMBIGUOUS,
            False if known_balance else None)


def _bank_new_draft(page_no: int, counter: Dict[str, int], date_str: str,
                    desc_text: str, amount: Decimal, direction: str,
                    confidence: float, raw_text: str) -> Dict:
    counter["n"] += 1
    return {
        "page": page_no,
        "row_index": counter["n"],
        "date": date_str,
        "desc_text": desc_text,
        "amount": amount,
        "direction": direction,
        "confidence": confidence,
        "raw_text": raw_text,
        "conts": [],
    }


def _bank_page_xmode(lines: List[Dict], header_idx: int,
                     bounds: Dict[str, Tuple[float, float]], page_no: int,
                     counter: Dict[str, int], prev_bal: Optional[Decimal],
                     drafts: List[Dict]) -> Optional[Decimal]:
    """Parse one bank page using column geometry. Returns updated prev_bal."""
    for idx, line in enumerate(lines):
        if idx == header_idx:
            continue
        text_all = line["text"]
        if _is_noise_line(text_all):
            anchor = _balance_anchor(text_all)
            if anchor is not None:
                prev_bal = anchor
            continue
        cols = _assign_column(line["words"], bounds)
        date_text = " ".join(w.get("text", "") for w in cols["date"])
        date_str = normalize_date(date_text)
        if not date_str:
            for w in cols["date"]:
                date_str = normalize_date(w.get("text", ""))
                if date_str:
                    break
        if date_str:
            debit = _first_amount(cols["debit"])
            credit = _first_amount(cols["credit"])
            balance = _last_amount(cols["balance"])
            if debit is None and credit is None:
                continue  # date row without amounts: header/subheader remnant
            res = _interpret_bank_amounts(debit, credit, balance, prev_bal)
            if res is None:
                continue
            amount, direction, cap, _fit = res
            if balance is not None:
                prev_bal = abs(balance[0])
            elif prev_bal is not None:
                prev_bal = prev_bal + amount if direction == "in" else prev_bal - amount
            amount_text = " ".join(
                w.get("text", "") for w in cols["debit"] + cols["credit"])
            base = (CONF_COMMA_PAREN if _money_text_is_dirty(amount_text)
                    else CONF_CLEAN)
            conf = base if cap is None else min(base, cap)
            desc_text = _strip_footnotes(
                " ".join(w.get("text", "") for w in cols["description"]))
            drafts.append(_bank_new_draft(
                page_no, counter, date_str, desc_text, amount, direction,
                conf, text_all))
        else:
            has_amounts = bool(cols["debit"] or cols["credit"] or cols["balance"])
            cont_text = _strip_footnotes(
                " ".join(w.get("text", "") for w in cols["description"]))
            if has_amounts:
                continue  # unanchored amounts (totals/subheader) -> not a txn
            if cont_text and drafts:
                drafts[-1]["conts"].append(cont_text)  # continuation/memo line
    return prev_bal


def _bank_page_textmode(text: str, page_no: int, counter: Dict[str, int],
                        prev_bal: Optional[Decimal],
                        drafts: List[Dict]) -> Optional[Decimal]:
    """Right-to-left heuristic parsing for pages without usable geometry."""
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        if _is_noise_line(line):
            anchor = _balance_anchor(line)
            if anchor is not None:
                prev_bal = anchor
            continue
        m = _DATE_FIND_RE.search(line)
        if not m:
            cleaned = _strip_footnotes(line)
            if cleaned and drafts:
                drafts[-1]["conts"].append(cleaned)
            continue
        date_str = normalize_date(m.group(0))
        if not date_str:
            continue
        rest = (line[:m.start()] + " " + line[m.end():])
        tokens = [t for t in rest.split() if t.strip(_FOOTNOTE_CHARS)]
        dec_idx = [i for i, t in enumerate(tokens) if parse_amount_token(t)
                   and "." in t]
        if not dec_idx:
            continue  # date line without printed amounts -> not a transaction
        k = dec_idx[0]
        desc_text = _strip_footnotes(" ".join(tokens[:k]))
        zone = tokens[k:]
        zone_amounts = [(i, parse_amount_token(t)) for i, t in enumerate(zone)
                        if parse_amount_token(t) is not None]
        dec_amounts = [(i, p) for i, p in zone_amounts if "." in zone[i]]
        balance = dec_amounts[-1][1] if len(dec_amounts) >= 2 else None
        amount_toks = [p for _i, p in dec_amounts[:-1]] if len(dec_amounts) >= 2 \
            else [dec_amounts[0][1]]
        debit = amount_toks[0] if amount_toks else None
        credit = amount_toks[1] if len(amount_toks) > 1 else None
        res = _interpret_bank_amounts(debit, credit, balance, prev_bal)
        if res is None:
            continue
        amount, direction, cap, _fit = res
        if balance is not None:
            prev_bal = abs(balance[0])
        elif prev_bal is not None:
            prev_bal = prev_bal + amount if direction == "in" else prev_bal - amount
        base = CONF_TEXT_FALLBACK
        conf = base if cap is None else min(base, cap)
        drafts.append(_bank_new_draft(
            page_no, counter, date_str, desc_text, amount, direction, conf,
            line))
    return prev_bal


def _parse_bank_statement(pages: List[Tuple[int, Any, str]],
                          source_file: str) -> List[Dict]:
    """Parse consecutive bank-statement pages (state flows across pages)."""
    drafts: List[Dict] = []
    prev_bal: Optional[Decimal] = None
    for page_no, plumber_page, text in pages:
        counter = {"n": 0}
        lines = None
        if plumber_page is not None:
            try:
                words = plumber_page.extract_words() or []
            except Exception as exc:  # never raise
                logger.warning("extract_words failed on %s p%s: %s",
                               source_file, page_no, exc)
                words = []
            if words:
                lines = _cluster_lines(words)
        header_idx, bounds = -1, None
        if lines:
            for i, line in enumerate(lines):
                hb = _bank_column_regions(line["words"])
                if hb is not None:
                    header_idx, bounds = i, hb
                    break
        try:
            if lines and bounds is not None:
                prev_bal = _bank_page_xmode(lines, header_idx, bounds,
                                            page_no, counter, prev_bal, drafts)
            else:
                prev_bal = _bank_page_textmode(text, page_no, counter,
                                               prev_bal, drafts)
        except Exception as exc:  # never raise on malformed content
            logger.warning("Bank parse failed on %s p%s: %s",
                           source_file, page_no, exc)

    txns: List[Dict] = []
    for draft in drafts:
        vendor, desc, wrapped = _bank_vendor_and_desc(
            draft["desc_text"], draft["conts"])
        if not desc:
            desc = vendor
        conf = draft["confidence"]
        if wrapped:
            conf = min(conf, CONF_MERGED_CAP)
        txns.append(_make_txn(
            source_file, draft["page"], draft["row_index"], "bank_statement",
            draft["date"], vendor, desc, draft["amount"], draft["direction"],
            raw_text=draft["raw_text"], confidence=conf))
    return txns


# ---------------------------------------------------------------------------
# Invoice parsing
# ---------------------------------------------------------------------------

_INVOICE_NUM_RE = re.compile(
    r"invoice\s*(?:#|no\.?|number)\s*[:#]?\s*([A-Za-z0-9][A-Za-z0-9\-_/]*)",
    re.I)
_ITEMS_HEADER_RE = re.compile(r"^\s*(?:qty|quantity)\b.*\bamount\b", re.I)
_INVOICE_TITLE_RE = re.compile(r"^\s*(?:tax\s+)?invoice\b\s*:?\s*$", re.I)
_INVOICE_NOISE_RE = re.compile(
    r"^\s*(?:bill\s+to|ship\s+to|sold\s+to|notes?\s*:|terms\s*:|thanks?\b|"
    r"thank\s+you\b|please\s+remit|questions?\b|page\s+\d+)", re.I)
_MONEY_LABEL_RES = {
    "subtotal": re.compile(r"^\s*sub\s*-?\s*total\b", re.I),
    "tax": re.compile(r"^\s*(?:sales\s+)?tax\b", re.I),
    "total": re.compile(r"^\s*(?:grand\s+total|total)\b", re.I),
    "due": re.compile(r"^\s*(?:amount|balance)\s+due\b", re.I),
}


def _label_value(line: str) -> Optional[Tuple[Decimal, bool]]:
    """Last numeric token on a label line (Subtotal/Tax/TOTAL/...)."""
    vals = [parse_amount_token(t) for t in line.split()]
    vals = [v for v in vals if v is not None]
    return vals[-1] if vals else None


def _try_parse_item(line: str) -> Optional[Dict]:
    """Parse ``Qty Description ... Unit Price Amount`` (unit price optional)."""
    toks = line.split()
    if len(toks) < 3:
        return None
    qty = parse_amount_token(toks[0])
    if qty is None or qty[0] <= 0 or qty[1]:
        return None
    up = parse_amount_token(toks[-2])
    am = parse_amount_token(toks[-1])
    if am is None:
        return None
    if up is not None:
        desc_toks = toks[1:-2]
        unit_price: Optional[float] = float(abs(up[0]))
    else:
        desc_toks = toks[1:-1]
        unit_price = None
    desc = _strip_footnotes(" ".join(desc_toks)).strip(" -\u2013\u2014")
    if not desc:
        return None
    return {
        "description": desc,
        "qty": float(qty[0]),
        "unit_price": unit_price,
        "amount": float(abs(am[0])),
        "dirty": _money_text_is_dirty(line),
    }


def _parse_invoice_page(text: str, source_file: str, page_no: int,
                        counter: Dict[str, int]) -> List[Dict]:
    """Parse one invoice page into a single ledger transaction."""
    text = text or ""
    invoice_number = ""
    m = _INVOICE_NUM_RE.search(text)
    if m:
        invoice_number = m.group(1).strip()

    invoice_date = ""
    m = re.search(r"invoice\s*date\s*[:#]?\s*(.{0,32})", text, re.I)
    if m:
        invoice_date = normalize_date(m.group(1))
    if not invoice_date:
        for ln in text.splitlines()[:12]:
            invoice_date = normalize_date(ln)
            if invoice_date:
                break

    vendor = ""
    for ln in text.splitlines():
        s = ln.strip()
        if not s or _INVOICE_TITLE_RE.match(s):
            continue
        # The vendor name is the top-of-page company cell; the "Invoice #:"
        # label is drawn on the same visual line to its right -> cut it off.
        cut = re.sub(r"(?i)\s+invoice\s*(?:#|no\.?|number)\s*[:#]?.*$", "", s)
        cut = cut.strip(" -\u2013\u2014|").strip()
        if cut:
            vendor = cut
            break

    items: List[Dict] = []
    totals: Dict[str, Optional[Tuple[Decimal, bool]]] = {
        "subtotal": None, "tax": None, "total": None, "due": None}
    in_items = False
    wrapped = False
    dirty = False
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if _MONEY_LABEL_RES["subtotal"].match(line):
            totals["subtotal"] = _label_value(line)
            in_items = False
            continue
        if _MONEY_LABEL_RES["tax"].match(line):
            totals["tax"] = _label_value(line)
            in_items = False
            continue
        if _MONEY_LABEL_RES["total"].match(line):
            totals["total"] = _label_value(line)
            in_items = False
            continue
        if _MONEY_LABEL_RES["due"].match(line):
            totals["due"] = _label_value(line)
            in_items = False
            continue
        if _INVOICE_NOISE_RE.match(line):
            in_items = False
            continue
        if not in_items and not items and _ITEMS_HEADER_RE.match(line):
            in_items = True
            continue
        if in_items:
            item = _try_parse_item(line)
            if item is not None:
                items.append(item)
                dirty = dirty or item["dirty"]
                continue
            if items:
                # wrapped two-line item description -> merge upward
                cont = _strip_footnotes(line)
                if cont:
                    items[-1]["description"] = (items[-1]["description"] + " "
                                                + cont).strip()
                    wrapped = True
                continue
            in_items = False

    amount_dec: Optional[Decimal] = None
    if totals["total"] is not None:
        amount_dec = abs(totals["total"][0])
    elif totals["due"] is not None:
        amount_dec = abs(totals["due"][0])
    elif totals["subtotal"] is not None:
        amount_dec = abs(totals["subtotal"][0]) + (
            abs(totals["tax"][0]) if totals["tax"] is not None else Decimal("0"))
    elif items:
        amount_dec = Decimal(str(sum(i["amount"] for i in items)))
    if amount_dec is None or amount_dec <= 0:
        return []

    conf = CONF_COMMA_PAREN if (dirty or _money_text_is_dirty(text)) else CONF_CLEAN
    subtotal_mismatch = False
    total_mismatch = False
    if totals["subtotal"] is not None and items:
        items_sum = Decimal(str(sum(i["amount"] for i in items)))
        if abs(items_sum - abs(totals["subtotal"][0])) > Decimal("0.011"):
            subtotal_mismatch = True
    if (totals["total"] is not None and totals["subtotal"] is not None
            and totals["tax"] is not None):
        expected = abs(totals["subtotal"][0]) + abs(totals["tax"][0])
        if abs(expected - abs(totals["total"][0])) > Decimal("0.011"):
            total_mismatch = True
    if subtotal_mismatch or total_mismatch:
        conf = min(conf, CONF_AMBIGUOUS)
    if totals["total"] is None:
        conf = min(conf, CONF_MISSING_TOTAL_CAP)
    if wrapped:
        conf = min(conf, CONF_WRAPPED_CAP)
    if not invoice_number:
        conf = min(conf, CONF_NO_INVOICE_NUM)
    if not invoice_date:
        conf = min(conf, CONF_MISSING_DATE_CAP)
    if subtotal_mismatch or total_mismatch:
        logger.info("Invoice totals mismatch in %s p%s (subtotal_ok=%s "
                    "total_ok=%s)", source_file, page_no,
                    not subtotal_mismatch, not total_mismatch)

    all_text_l = (vendor + " " + " ".join(i["description"] for i in items)).lower()
    if re.search(r"\bequipment\b|\brentals?\b|\btools?\b", all_text_l):
        kind = "Equipment rental"
    else:
        kind = "Materials"
    description = (kind + " invoice " + invoice_number).strip()

    counter["n"] += 1
    line_items = [{"description": i["description"], "qty": i["qty"],
                   "unit_price": i["unit_price"], "amount": i["amount"]}
                  for i in items]
    txn = _make_txn(source_file, page_no, counter["n"], "invoice",
                    invoice_date, vendor, description, amount_dec, "out",
                    invoice_number=invoice_number, line_items=line_items,
                    raw_text="\n".join(text.splitlines()[:6]), confidence=conf)
    return [txn]


# ---------------------------------------------------------------------------
# Payroll parsing
# ---------------------------------------------------------------------------

_PAYROLL_TOTAL_RE = re.compile(
    r"total\s+gross\s*[:#]?\s*\(?\$?\s*(-?[\d,]+(?:\.\d+)?)\s*\)?", re.I)
# Priority-ordered labels for the aggregate transaction date. A specific
# "Pay Date:" wins over a "Pay Period: <start> - <end>" range (whose LAST
# date is used as the fallback period end).
_PERIOD_LABEL_RES = [
    re.compile(r"pay\s*period\s*(?:end|ending|end\s*date)\s*[:#]?\s*([^\n]{0,40})", re.I),
    re.compile(r"(?:period|week)\s+ending\s*[:#]?\s*([^\n]{0,40})", re.I),
    re.compile(r"pay\s*date\s*[:#]?\s*([^\n]{0,40})", re.I),
    re.compile(r"check\s*date\s*[:#]?\s*([^\n]{0,40})", re.I),
    re.compile(r"pay\s*period\s*[:#]?\s*([^\n]{0,40})", re.I),
]
_PAYROLL_HEADER_RE = re.compile(r"\bwork\s*date\b|\bhours\b.*\brate\b", re.I)


def _period_end_date(text: str) -> str:
    for rx in _PERIOD_LABEL_RES:
        m = rx.search(text or "")
        if m:
            dates = [normalize_date(d)
                     for d in _DATE_FIND_RE.findall(m.group(1))]
            dates = [d for d in dates if d]
            if dates:
                return dates[-1]
    return ""


def _try_parse_payroll_row(line: str) -> Optional[Dict]:
    """Parse ``Employee ... Work Date Hours Rate Gross``."""
    m = _DATE_FIND_RE.search(line)
    if not m:
        return None
    before = line[:m.start()].strip(" -\u2013\u2014:")
    if not before or not re.search(r"[A-Za-z]", before):
        return None
    nums = [parse_amount_token(t) for t in line[m.end():].split()]
    nums = [v for v in nums if v is not None]
    if len(nums) < 3:
        return None
    hours, rate, gross = nums[0][0], nums[1][0], nums[2][0]
    if hours <= 0 or hours > 24:
        logger.warning("Payroll row with out-of-range hours ignored: %s", line)
        return None
    return {
        "employee": re.sub(r"\s+", " ", before).strip(),
        "work_date": normalize_date(m.group(0)) or "",
        "hours": float(hours),
        "rate": float(abs(rate)),
        "amount": float(abs(gross)),
        "dirty": _money_text_is_dirty(line),
    }


def _parse_payroll_page(text: str, source_file: str, page_no: int,
                        counter: Dict[str, int]) -> List[Dict]:
    """Parse a payroll grid page into ONE aggregate transaction."""
    text = text or ""
    rows: List[Dict] = []
    total_gross: Optional[Decimal] = None
    wrapped = False
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        m = _PAYROLL_TOTAL_RE.search(line)
        if m:
            try:
                total_gross = abs(Decimal(m.group(1).replace(",", "")))
            except InvalidOperation:
                total_gross = None
            continue
        if _PAYROLL_HEADER_RE.search(line) and not _DATE_FIND_RE.search(line):
            continue  # grid header
        row = _try_parse_payroll_row(line)
        if row is not None:
            rows.append(row)
            continue
        if rows and re.fullmatch(r"[A-Za-z][A-Za-z .'\-]+", line):
            # wrapped employee name continuation
            rows[-1]["employee"] = (rows[-1]["employee"] + " " + line).strip()
            wrapped = True

    if not rows:
        return []

    rows_sum = Decimal(str(sum(r["amount"] for r in rows)))
    if total_gross is not None:
        amount_dec = total_gross
    else:
        amount_dec = rows_sum
    if amount_dec <= 0:
        return []

    dates = sorted(r["work_date"] for r in rows if r["work_date"])
    start = dates[0] if dates else ""
    end = dates[-1] if dates else ""
    txn_date = _period_end_date(text) or end

    conf = CONF_CLEAN
    if _money_text_is_dirty(text):
        conf = CONF_COMMA_PAREN
    if total_gross is not None and abs(rows_sum - total_gross) > Decimal("0.011"):
        conf = min(conf, CONF_AMBIGUOUS)
        logger.info("Payroll rows sum %s != TOTAL GROSS %s in %s p%s",
                    rows_sum, total_gross, source_file, page_no)
    if total_gross is None:
        conf = min(conf, CONF_NO_FOOTER_CAP)
    if wrapped:
        conf = min(conf, CONF_WRAPPED_CAP)
    if not start:
        conf = min(conf, CONF_MISSING_DATE_CAP)

    vendor = ""
    for ln in text.splitlines():
        s = ln.strip()
        if not s:
            continue
        vendor = re.sub(r"(?i)\s*[-\u2013\u2014|]?\s*(?:weekly\s+)?payroll.*$",
                        "", s).strip(" -\u2013\u2014")
        break

    description = f"Weekly payroll {start} to {end}".strip()
    line_items = [{"employee": r["employee"], "work_date": r["work_date"],
                   "hours": r["hours"], "rate": r["rate"],
                   "amount": r["amount"]} for r in rows]
    counter["n"] += 1
    txn = _make_txn(source_file, page_no, counter["n"], "payroll", txn_date,
                    vendor, description, amount_dec, "out",
                    line_items=line_items,
                    raw_text="\n".join(text.splitlines()[:6]), confidence=conf)
    return [txn]


# ---------------------------------------------------------------------------
# Optional GLM/LLM fallback (never required for the sample PDFs)
# ---------------------------------------------------------------------------

_LLM_SYSTEM_PROMPT = (
    "You are a financial document extraction engine. Extract every "
    "transaction/line item from the document text. Respond with ONLY a JSON "
    'object of the form {"transactions": [{"date": "YYYY-MM-DD", "vendor": '
    '"string", "description": "string", "amount": <positive number>, '
    '"direction": "in"|"out", "invoice_number": "string", "line_items": '
    "[{...}]}]}. No prose, no markdown fences."
)


def _strip_json_fences(content: str) -> str:
    s = (content or "").strip()
    s = re.sub(r"^```(?:json)?\s*", "", s)
    s = re.sub(r"\s*```$", "", s)
    return s.strip()


def llm_extract_page(page_text: str, doc_type_hint: str) -> List[Dict]:
    """OPTIONAL GLM fallback via an OpenAI-compatible chat completion.

    Configuration (environment): ``GLM_API_KEY`` or ``OPENAI_API_KEY``,
    ``GLM_BASE_URL`` (default https://open.bigmodel.cn/api/paas/v4),
    ``GLM_MODEL`` (default glm-4-flash). Uses JSON response mode and a 30s
    timeout. Returns ``[]`` when unconfigured or on ANY error, so callers can
    treat it as best-effort. Result rows are marked low-confidence (0.60) and
    needs_review=True; the caller assigns txn_id/source_file/page.
    """
    api_key = os.environ.get("GLM_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if not api_key or not (page_text or "").strip():
        return []
    if requests is None:
        logger.warning("LLM fallback requested but requests is unavailable")
        return []
    base_url = (os.environ.get("GLM_BASE_URL")
                or "https://open.bigmodel.cn/api/paas/v4").rstrip("/")
    model = os.environ.get("GLM_MODEL") or "glm-4-flash"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": _LLM_SYSTEM_PROMPT},
            {"role": "user",
             "content": f"Document type hint: {doc_type_hint}\n\n{page_text}"},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "max_tokens": 2000,
    }
    try:
        resp = requests.post(
            base_url + "/chat/completions",
            json=payload,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=30,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        data = json.loads(_strip_json_fences(content))
        rows = data.get("transactions") or data.get("items") or []
    except Exception as exc:
        logger.warning("LLM fallback failed (ignored): %s", exc)
        return []

    out: List[Dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        parsed = parse_amount_token(str(row.get("amount", "")))
        if parsed is None:
            continue
        direction = str(row.get("direction", "out")).lower()
        line_items = row.get("line_items") if isinstance(
            row.get("line_items"), list) else []
        out.append({
            "txn_id": "",
            "source_file": "",
            "page": 0,
            "doc_type": doc_type_hint if doc_type_hint in DOC_TYPES
            else "bank_statement",
            "date": normalize_date(str(row.get("date", ""))),
            "vendor": str(row.get("vendor", "")).strip(),
            "description": str(row.get("description", "")).strip(),
            "amount": float(abs(parsed[0])),
            "direction": direction if direction in ("in", "out") else "out",
            "invoice_number": str(row.get("invoice_number", "") or "").strip(),
            "line_items": line_items,
            "raw_text": "[LLM fallback] " + (page_text or "")[:200],
            "category": None,
            "overhead_subtype": None,
            "vendor_normalized": None,
            "confidence": CONF_LOW,
            "needs_review": CONF_LOW < REVIEW_THRESHOLD,
            "is_duplicate": False,
            "duplicate_of": None,
        })
    return out


# ---------------------------------------------------------------------------
# Page loading (pdfplumber primary, pypdf fallback)
# ---------------------------------------------------------------------------

def _pypdf_texts(pdf_path: str) -> List[str]:
    try:
        from pypdf import PdfReader
    except ImportError:  # pragma: no cover
        return []
    try:
        reader = PdfReader(pdf_path)
        return [(page.extract_text() or "") for page in reader.pages]
    except Exception as exc:
        logger.warning("pypdf failed on %s: %s", pdf_path, exc)
        return []


def _load_pages(pdf_path: str) -> List[Tuple[int, Any, str]]:
    """Return [(page_no, pdfplumber_page|None, text)] for the document."""
    pages: List[Tuple[int, Any, str]] = []
    if pdfplumber is not None:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for i, page in enumerate(pdf.pages, 1):
                    try:
                        text = page.extract_text() or ""
                    except Exception as exc:
                        logger.warning("extract_text failed on %s p%s: %s",
                                       pdf_path, i, exc)
                        text = ""
                    pages.append((i, page, text))
        except Exception as exc:
            logger.warning("pdfplumber failed on %s: %s", pdf_path, exc)
            pages = []
    if not any(t.strip() for _i, _p, t in pages):
        texts = _pypdf_texts(pdf_path)
        if texts:
            if pages:
                pages = [(i, p, t if t.strip() else texts[i - 1])
                         for i, (p, t) in
                         enumerate(((pg, tx) for _i, pg, tx in pages), 1)]
            else:
                pages = [(i, None, t) for i, t in enumerate(texts, 1)]
    return pages


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def extract_transactions(pdf_path: str) -> List[Dict]:
    """Full pipeline for one PDF: load pages, detect doc type, parse,
    optionally consult the LLM fallback. Returns §5 transaction dicts.
    Never raises on malformed content; unparseable pages are skipped."""
    source_file = os.path.basename(str(pdf_path))
    try:
        pages = _load_pages(pdf_path)
    except Exception as exc:  # never raise
        logger.error("Could not open %s: %s", source_file, exc)
        return []
    if not pages:
        logger.error("No readable pages in %s", source_file)
        return []

    texts = [t for _i, _p, t in pages]
    doc_type = _document_doc_type(texts)
    rule_txns: List[Dict] = []
    try:
        if doc_type == "bank_statement":
            rule_txns = _parse_bank_statement(pages, source_file)
        elif doc_type == "invoice":
            for page_no, _page, text in pages:
                counter = {"n": 0}
                rule_txns.extend(
                    _parse_invoice_page(text, source_file, page_no, counter))
        elif doc_type == "payroll":
            for page_no, _page, text in pages:
                counter = {"n": 0}
                rule_txns.extend(
                    _parse_payroll_page(text, source_file, page_no, counter))
    except Exception as exc:  # never raise
        logger.error("Rule-based parsing failed on %s: %s", source_file, exc)

    # LLM fallback: only for pages that yielded nothing or all-low confidence.
    by_page: Dict[int, List[Dict]] = {}
    for t in rule_txns:
        by_page.setdefault(t["page"], []).append(t)

    final: List[Dict] = []
    for page_no, _page, text in pages:
        rows = by_page.get(page_no, [])
        if not rows or all(r["confidence"] < CONF_LOW for r in rows):
            hint = detect_doc_type(text)
            llm_rows = llm_extract_page(text, hint)
            if llm_rows:
                logger.info("Using LLM fallback for %s p%s (%d rows)",
                            source_file, page_no, len(llm_rows))
                rows = []  # empty page, or replace all-low-confidence rows
                for k, row in enumerate(llm_rows, 1):
                    row["txn_id"] = f"{source_file}:{page_no}:{k}"
                    row["source_file"] = source_file
                    row["page"] = page_no
                    rows.append(row)
        final.extend(rows)
    return final
