"""Deterministic vendor/description-based categorization.

Per CONTRACT.md §6:

* ``CATEGORIES`` -- the six-word vocabulary.
* ``categorize(txn)`` -- fills ``category``, ``overhead_subtype``,
  ``confidence`` (and ``needs_review``) on one transaction.
* ``categorize_all(txns)`` -- categorize + vendor normalization + duplicate
  detection (flag-not-drop).
* ``normalize_vendor(raw)`` -- canonical vendor form for alias matching.
* ``confidence_score(txn, evidence)`` -- combines the categorization evidence
  with any parser extraction confidence already on the txn.

Categorization rules (first match wins; case-insensitive keyword matching on
the normalized vendor + raw vendor + description text):

+----+---------------------------------------+------------------------------+-----------+
| #  | Signal (vendor and/or description)    | Category                     | Subtype   |
+====+=======================================+==============================+===========+
| 1  | doc_type == "payroll", or "payroll"/  | Payroll                      | -         |
|    | payroll-service cues (Paychex/ADP/    |                              |           |
|    | Gusto, "payroll service charge")      |                              |           |
| 2  | direction == "in" (bank deposits /    | Revenue                      | -         |
|    | customer payments) or deposit/        |                              |           |
|    | customer-payment cues in description  |                              |           |
| 3  | Ad platforms: Google, Meta, Facebook, | Marketing                    | -         |
|    | Instagram, LinkedIn, TikTok, ad/ads,  |                              |           |
|    | advertising, billboard, flyer, mailer |                              |           |
| 4  | Explicit subcontractor cues:          | Subcontractor                | -         |
|    | "subcontractor", "sub contractor",    |                              |           |
|    | "subcontract"                         |                              |           |
| 5  | Trade + qualifier: plumbing/electric- | Subcontractor                | -         |
|    | al/roofing/hvac/drywall/painting/     |                              |           |
|    | excavation/concrete/landscaping with  |                              |           |
|    | contractor/services/builders          |                              |           |
| 6  | Building-material supplier cues:      | Direct_Materials             | -         |
|    | lumber, supply/supplies, masonry,     |                              |           |
|    | landscape materials, building         |                              |           |
|    | materials/products, hardware,         |                              |           |
|    | materials, cement, concrete, gravel,  |                              |           |
|    | mulch, paver, nursery, garden,        |                              |           |
|    | home center                           |                              |           |
| 7  | Trade alone: framing, electrician     | Subcontractor                | -         |
| 8  | Overhead cues (subtype detection,     | Overhead                     | utilities |
|    | first hit wins): equipment/rental ->  |                              | insurance |
|    | equipment; insurance/assurance ->     |                              | rent      |
|    | insurance; rent/lease/landlord ->     |                              | equipment |
|    | rent; utilities/power/water/gas/      |                              | other     |
|    | electric/internet/phone -> utilities; |                              |           |
|    | bank service charge/fee/subscription/ |                              |           |
|    | software/office/postage -> other      |                              |           |
| 9  | Nothing matched                       | Overhead (fallback)          | other     |
+----+---------------------------------------+------------------------------+-----------+

Amount magnitude never affects the category. Confidence: keyword hit in the
NORMALIZED VENDOR >= 0.9 (0.95 when the description corroborates);
description-only hit ~0.8; doc_type/direction structural hits 0.95/0.9;
fallback default "Overhead"/"other" with confidence 0.5 and needs_review=True.
The final confidence never exceeds the parser's extraction confidence.
"""

from __future__ import annotations

import logging
import re
from typing import Dict, List

logger = logging.getLogger("doc_to_warehouse.categorizer")

CATEGORIES = ("Revenue", "Direct_Materials", "Subcontractor", "Overhead",
              "Marketing", "Payroll")

DEFAULT_CATEGORY = "Overhead"
DEFAULT_OVERHEAD_SUBTYPE = "other"
FALLBACK_CONFIDENCE = 0.5

# ---------------------------------------------------------------------------
# Vendor normalization
# ---------------------------------------------------------------------------

# Trailing tokens stripped by normalize_vendor (CONTRACT.md §6: "lowercase,
# strip legal suffixes (co, company, inc, llc, ltd, supply, building
# products...), collapse whitespace/punct").
# NOTE: "lumber" is deliberately NOT stripped -- it is the distinguishing
# noun. "ABC Lumber Co." -> "abc lumber" and "ABC LUMBER SUPPLY INC" ->
# "abc lumber" (both "supply" and "inc" strip away once trailing).
SUFFIX_TOKENS = {
    "co", "company", "inc", "incorporated", "llc", "ltd", "limited",
    "corp", "corporation", "plc", "pc", "supply", "supplies", "building",
    "products", "the",
}


def normalize_vendor(raw: str) -> str:
    """Canonical vendor form.

    Exact rule set (applied in order):
      1. lowercase the whole string;
      2. replace every non ``[a-z0-9]`` character (punctuation, ``&``, dots,
         commas) with a space -- "Co." -> "co";
      3. split on whitespace;
      4. repeatedly drop the LAST token while it is in SUFFIX_TOKENS
         (co, company, inc, incorporated, llc, ltd, limited, corp,
         corporation, plc, pc, supply, supplies, building, products, the);
      5. if that would empty the name entirely, keep the un-stripped tokens
         (so "Supply Co" -> "supply co", never "");
      6. re-join with single spaces.

    Examples: "ABC Lumber Co." -> "abc lumber";
    "ABC LUMBER SUPPLY INC" -> "abc lumber" (same canonical form);
    "Sierra Building Products, Co." -> "sierra".
    """
    if not raw:
        return ""
    s = str(raw).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    tokens = s.split()
    stripped = list(tokens)
    while stripped and stripped[-1] in SUFFIX_TOKENS:
        stripped.pop()
    if not stripped:  # never return an empty canonical for a non-empty name
        stripped = tokens
    return " ".join(stripped)


# ---------------------------------------------------------------------------
# Keyword rule tables
# ---------------------------------------------------------------------------

_PAYROLL_SERVICE_RE = re.compile(
    r"\bpayroll\b|\bpaychex\b|\badp\b|\bgusto\b|\bquickbooks\s+payroll\b|"
    r"\bpayroll\s+service\b", re.I)
_REVENUE_DESC_RE = re.compile(
    r"\bdeposit\b|\bcustomer\s+payment\b|\bpayment\s+received\b|"
    r"\bcollections?\b|\breceivable\b|\binvoice\s+payment\b", re.I)

_MARKETING_RE = re.compile(
    r"\bgoogle\b|\bmeta\b|\bfacebook\b|\binstagram\b|\blinkedin\b|"
    r"\btiktok\b|\badwords?\b|\bprint\s+ads?\b|\badvertising\b|"
    r"\badvertisements?\b|\bbillboards?\b|\bflyers?\b|\bmailers?\b|"
    r"\bdirect\s+mail\b|\byard\s+signs?\b|\bradio\s+spot\b|\btv\s+spot\b|"
    r"\bads\b", re.I)

_SUBC_EXPLICIT_RE = re.compile(
    r"\bsub\s*-?\s*contract|\bsubcontractor\b", re.I)
_SUBC_TRADE_QUALIFIER_RE = re.compile(
    r"\b(?:framing|plumbing|electrical|roofing|hvac|drywall|painting|"
    r"excavation|concrete|landscaping)\b[^.]*\b(?:contractors?|services?|"
    r"svc\.?|builders?)\b", re.I)
_SUBC_TRADE_ALONE_RE = re.compile(
    r"\bframing\b|\belectrician\b|\bsub\s*work\b", re.I)

_MATERIALS_RE = re.compile(
    r"\blumber\b|\bsuppl(?:y|ies)\b|\bmasonry\b|\blandscape\s+materials?\b|"
    r"\bbuilding\s+materials?\b|\bbuilding\s+products\b|\bhardware\b|"
    r"\bmaterials?\b|\bcement\b|\bconcrete\b|\bgravel\b|"
    r"\bmulch\b|\bpavers?\b|\bnursery\b|\bgarden\b|\bhome\s+center\b|"
    r"\bsand\b|\bsoil\b|\bstone\b", re.I)

_OVERHEAD_SUBTYPE_RES = (
    ("equipment", re.compile(
        r"\bequipment\s+rental\b|\btool\s+rental\b|\bequipment\b|"
        r"\brentals?\b|\btools?\b", re.I)),
    ("insurance", re.compile(
        r"\binsurance\b|\bassurance\b|\bmutual\b", re.I)),
    ("rent", re.compile(
        r"\brent\b|\blease\b|\blandlord\b|\bproperty\s+manage?ment\b", re.I)),
    ("utilities", re.compile(
        r"\butilities?\b|\bpower\b|\belectric\b|\bgas\b|\bwater\b|"
        r"\bsewer\b|\binternet\b|\bphone\b|\btelephone\b|\bcable\b|"
        r"\benergy\b", re.I)),
)
_OVERHEAD_GENERIC_RE = re.compile(
    r"\bservice\s+charge\b|\bbank\s+(?:fee|charge|service)\b|"
    r"\banalysis\s+fee\b|\bnsf\b|\boverdraft\b|\bwire\s+fee\b|"
    r"\baccount\s+fee\b|\bmaintenance\s+fee\b|\bsubscription\b|"
    r"\bsoftware\b|\boffice\b|\bpostage\b|\bcleaning\b|\btrash\b|"
    r"\bdisposal\b|\bpermits?\b|\blicen[cs]es?\b|\bfees?\b", re.I)


# ---------------------------------------------------------------------------
# Confidence
# ---------------------------------------------------------------------------

def confidence_score(txn: dict, evidence: dict) -> float:
    """Combine categorization evidence with the parser's extraction confidence.

    ``evidence`` keys (all optional):
      * ``matched_field``: "vendor" | "description" | "doc_type" |
        "direction" | "fallback" -- where the winning keyword hit came from;
      * ``corroborated``: bool -- the description ALSO hit the same rule
        (vendor hit + description corroboration -> 0.95);
      * ``extraction_conf``: float -- parser extraction confidence already on
        the txn (if absent it is read from ``txn["confidence"]``).

    Vendor keyword hit >= 0.9, description-only hit ~0.8, structural
    doc_type/direction hits 0.95/0.9, fallback 0.5. The result is capped by
    the extraction confidence so extraction problems are never hidden.
    """
    matched_field = evidence.get("matched_field", "fallback")
    corroborated = bool(evidence.get("corroborated"))

    if matched_field == "doc_type":
        base = 0.95
    elif matched_field == "direction":
        base = 0.90
    elif matched_field == "vendor":
        base = 0.95 if corroborated else 0.90
    elif matched_field == "description":
        base = 0.80
    else:  # fallback
        base = FALLBACK_CONFIDENCE

    extraction_conf = evidence.get("extraction_conf")
    if extraction_conf is None:
        extraction_conf = txn.get("confidence")
    try:
        extraction_conf = float(extraction_conf) if extraction_conf is not None else None
    except (TypeError, ValueError):
        extraction_conf = None
    if extraction_conf is not None:
        base = min(base, extraction_conf)
    return float(max(0.0, min(1.0, base)))


# ---------------------------------------------------------------------------
# Categorization
# ---------------------------------------------------------------------------

def categorize(txn: dict) -> dict:
    """Fill ``category``, ``overhead_subtype``, ``confidence`` (and
    ``needs_review``) on one transaction dict (mutated in place and returned).
    Deterministic; amount magnitude never affects the result."""
    vendor_raw = str(txn.get("vendor") or "")
    description = str(txn.get("description") or "")
    vendor_norm = normalize_vendor(vendor_raw)
    haystack_vendor = (vendor_norm + " " + vendor_raw.lower()).strip()
    haystack_desc = description.lower()
    doc_type = str(txn.get("doc_type") or "")
    direction = str(txn.get("direction") or "")

    def _hit(regex) -> tuple:
        """-> (matched_field, corroborated) or (None, False)."""
        in_vendor = bool(regex.search(haystack_vendor))
        in_desc = bool(regex.search(haystack_desc))
        if in_vendor:
            return "vendor", in_desc
        if in_desc:
            return "description", False
        return None, False

    category = None
    subtype = None
    matched_field, corroborated = "fallback", False

    # 1. Payroll (doc_type structural, or payroll-service cues)
    if doc_type == "payroll":
        category, matched_field, corroborated = "Payroll", "doc_type", False
    else:
        field, corr = _hit(_PAYROLL_SERVICE_RE)
        if field:
            category, matched_field, corroborated = "Payroll", field, corr

    # 2. Revenue (direction structural, or deposit/customer-payment cues)
    if category is None:
        if direction == "in":
            category, matched_field = "Revenue", "direction"
        else:
            field, corr = _hit(_REVENUE_DESC_RE)
            if field:
                category, matched_field, corroborated = "Revenue", field, corr

    # 3. Marketing (ad platforms)
    if category is None:
        field, corr = _hit(_MARKETING_RE)
        if field:
            category, matched_field, corroborated = "Marketing", field, corr

    # 4. Subcontractor (explicit cues) -- checked before materials so that
    #    "subcontractor" outranks any material word in the same string.
    if category is None:
        field, corr = _hit(_SUBC_EXPLICIT_RE)
        if field:
            category, matched_field, corroborated = "Subcontractor", field, corr

    # 5. Subcontractor (trade + qualifier, e.g. "Riverside Concrete
    #    Contractors") -- before materials so trade vendors are not mistaken
    #    for material suppliers.
    if category is None:
        field, corr = _hit(_SUBC_TRADE_QUALIFIER_RE)
        if field:
            category, matched_field, corroborated = "Subcontractor", field, corr

    # 6. Direct_Materials (building-material supplier cues)
    if category is None:
        field, corr = _hit(_MATERIALS_RE)
        if field:
            category, matched_field, corroborated = "Direct_Materials", field, corr

    # 7. Subcontractor (standalone trade words, e.g. "framing")
    if category is None:
        field, corr = _hit(_SUBC_TRADE_ALONE_RE)
        if field:
            category, matched_field, corroborated = "Subcontractor", field, corr

    # 8. Overhead (subtype detection)
    if category is None:
        for name, regex in _OVERHEAD_SUBTYPE_RES:
            field, corr = _hit(regex)
            if field:
                category, subtype = "Overhead", name
                matched_field, corroborated = field, corr
                break
    if category is None:
        field, corr = _hit(_OVERHEAD_GENERIC_RE)
        if field:
            category, subtype = "Overhead", DEFAULT_OVERHEAD_SUBTYPE
            matched_field, corroborated = field, corr

    # 9. Fallback
    if category is None:
        category = DEFAULT_CATEGORY
        subtype = DEFAULT_OVERHEAD_SUBTYPE
        matched_field = "fallback"

    evidence = {"matched_field": matched_field,
                "corroborated": corroborated,
                "extraction_conf": txn.get("confidence")}
    conf = confidence_score(txn, evidence)

    txn["category"] = category
    txn["overhead_subtype"] = subtype if category == "Overhead" else None
    txn["confidence"] = conf
    txn["needs_review"] = bool(txn.get("needs_review")) or conf < 0.85
    if txn["category"] not in CATEGORIES:  # defensive; cannot happen
        txn["category"] = DEFAULT_CATEGORY
    return txn


def categorize_all(txns: List[dict]) -> List[dict]:
    """Categorize every transaction, normalize vendors, and flag duplicates.

    Duplicate rule (CONTRACT.md §6, flag-not-drop): group by
    ``vendor_normalized`` + ``invoice_number`` when an invoice number is
    present, otherwise by ``vendor_normalized`` + ``(amount, date)``. Every
    occurrence AFTER the first gets ``is_duplicate=True`` and
    ``duplicate_of=<first txn_id>``; rows are never dropped.
    """
    seen: Dict[tuple, str] = {}
    for txn in txns:
        categorize(txn)
        vn = normalize_vendor(str(txn.get("vendor") or ""))
        txn["vendor_normalized"] = vn
        inv = str(txn.get("invoice_number") or "").strip()
        if inv:
            key = ("inv", vn, inv.upper())
        else:
            try:
                amount_key = round(float(txn.get("amount") or 0.0), 2)
            except (TypeError, ValueError):
                amount_key = 0.0
            key = ("amt", vn, amount_key, str(txn.get("date") or ""))
        if key in seen:
            txn["is_duplicate"] = True
            txn["duplicate_of"] = seen[key]
            txn["needs_review"] = True
        else:
            seen[key] = str(txn.get("txn_id") or "")
    return txns
