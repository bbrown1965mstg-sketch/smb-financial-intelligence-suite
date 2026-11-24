# Doc-to-Warehouse — Binding Module Contract

Two agents are building this module **in parallel**. This contract is binding.
Do not change schemas, function signatures, file names, or directory names
defined here. If something here is genuinely impossible, say so in your final
report instead of silently deviating.

## 1. File ownership (STRICT — parallel agents, no exceptions)

| Owner      | Files they own (may create/modify)                                        |
|------------|---------------------------------------------------------------------------|
| Agent 1    | `pdf_parser.py`, `categorizer.py`, `export_warehouse_csv.py`, `app.py`, `README.md` |
| Agent 2    | `generate_sample_pdfs.py`, `test_extractor.py`, `conftest.py`, `sample_pdfs/`, `ground_truth/` |
| Coordinator| `CONTRACT.md`, `requirements.txt`, `.gitignore`                            |

**Rules:**
- NEVER delete, move, rename, or edit a file owned by the other agent — even
  if it looks wrong. Report it instead.
- NEVER touch anything under `/Users/Owner/Desktop/GLM-5.3/Data Warehouse/`
  (read-only reference) or any other sibling project directory.
- No database connections. This module is fully standalone (files in → CSVs out).
- Runtime output goes to `output/` (create if missing; never commit-owned).

## 2. Directory layout

```
Doc-to-Warehouse/
  CONTRACT.md  requirements.txt  .gitignore
  pdf_parser.py            # Agent 1
  categorizer.py           # Agent 1
  export_warehouse_csv.py  # Agent 1
  app.py                   # Agent 1  (Streamlit, port 8603)
  README.md                # Agent 1
  generate_sample_pdfs.py  # Agent 2
  test_extractor.py        # Agent 2
  conftest.py              # Agent 2 (sys.path bootstrap + shared fixtures)
  sample_pdfs/             # Agent 2 output: generated messy PDFs
  ground_truth/ground_truth.json   # Agent 2 output: machine-readable truth
  output/                  # runtime CSV exports (both agents may write here via code)
```

## 3. Sample document inventory (FIXED)

Agent 2 generates exactly these into `sample_pdfs/` (reportlab, deliberately
messy: `$1,234.56` commas, `(1,200.00)` parenthesized negatives, wrapped
two-line descriptions, inconsistent spacing/alignment, trailing footnote
asterisks, memo noise lines that are NOT transactions):

1. `bank_statement_pacific_trust.pdf` — 2-page business bank statement.
   Columns: `Date | Description | Ref | Debit | Credit | Balance`.
   Mix of: customer deposits (Revenue), material supplier checks
   (Direct_Materials), subcontractor payments (Subcontractor), utilities +
   insurance + tool rental (Overhead), an ad platform debit (Marketing), a
   bank service charge (Overhead). Running balance column is consistent with
   all debits/credits (parser may use balance deltas as a confidence signal).
2. `invoice_abc_lumber.pdf`, `invoice_sierra_supply.pdf`,
   `invoice_hilti_equipment.pdf` — vendor invoices. Header: vendor name,
   address, `Invoice #:`, `Invoice Date:`. Items table: `Qty | Description |
   Unit Price | Amount`, then `Subtotal`, optional `Tax`, `TOTAL`. One invoice
   has a wrapped two-line item description.
3. `payroll_week_2026-03-02.pdf` — weekly payroll summary, flat grid:
   `Employee | Work Date | Hours | Rate | Gross`, 4 employees × 4 work days
   (16 rows), plus a `TOTAL GROSS:` footer line. Header shows pay-period
   end date.
4. `invoice_abc_lumber_resend.pdf` — a re-send of invoice #1 (same vendor,
   same invoice number, same total, different file). Exists ONLY to prove
   duplicate detection.

## 4. Ground truth format (Agent 2 produces; binding)

`ground_truth/ground_truth.json`:
```json
{
  "generated_at": "ISO timestamp",
  "documents": [
    {
      "file": "bank_statement_pacific_trust.pdf",
      "doc_type": "bank_statement",
      "transactions": [
        {"date": "2026-03-02", "vendor": "HARBORVIEW HOA", "description": "Deposit - customer payment",
         "amount": 8400.00, "direction": "in"}
      ],
      "expected_totals": {"total_credits": 0.00, "total_debits": 0.00, "final_balance": 0.00}
    },
    {
      "file": "invoice_abc_lumber.pdf", "doc_type": "invoice",
      "transactions": [
        {"date": "2026-03-04", "vendor": "ABC Lumber Co.", "invoice_number": "INV-1041",
         "description": "Materials invoice INV-1041", "amount": 1234.56, "direction": "out",
         "line_items": [{"description": "...", "qty": 10.0, "unit_price": 100.00, "amount": 1000.00}]}
      ],
      "expected_totals": {"subtotal": 0.00, "tax": 0.00, "total": 0.00}
    },
    {
      "file": "payroll_week_2026-03-02.pdf", "doc_type": "payroll",
      "transactions": [
        {"date": "2026-03-06", "vendor": "Bristlecone Landscaping", "description": "Weekly payroll 2026-03-02 to 2026-03-05",
         "amount": 0.00, "direction": "out",
         "line_items": [{"employee": "Maria Alvarez", "work_date": "2026-03-02", "hours": 8.0, "rate": 24.50, "amount": 196.00}]}
      ],
      "expected_totals": {"total_gross": 0.00}
    }
  ],
  "expected_duplicates": [{"vendor_canonical": "abc lumber co", "invoice_number": "INV-1041"}],
  "vendor_alias_groups": [["ABC Lumber Co.", "ABC LUMBER SUPPLY INC"]]
}
```
Rules: every transaction in every PDF appears exactly once in ground truth
(with exact `amount` to the penny, `direction` in/out, ISO dates). `qty`/
`unit_price` may be `null` for bank rows. Invoice `description` for the ledger
row should be `"<Materials invoice|Equipment rental invoice> <invoice_number>"`.
Payroll `description`: `"Weekly payroll <start> to <end>"`.
Also embed the same facts as a Python dict `GROUND_TRUTH` in
`generate_sample_pdfs.py` (single source of truth for generation).

## 5. In-memory transaction model (binding)

A plain dict; both agents code against exactly these keys:
```python
{
  "txn_id": str,          # f"{source_file}:{page}:{row_index}" — stable, 1-based row_index
  "source_file": str,     # base filename only
  "page": int,            # 1-based
  "doc_type": str,        # "bank_statement" | "invoice" | "payroll" | "credit_card"
  "date": str,            # "YYYY-MM-DD" or "" if unparseable
  "vendor": str,          # raw payee/payer text
  "description": str,
  "amount": float,        # ALWAYS positive magnitude, rounded to 2dp (parse via Decimal)
  "direction": str,       # "in" | "out"
  "invoice_number": str,  # invoices only, else ""
  "line_items": list,     # dicts; invoices: {description, qty, unit_price, amount}
                          #              payroll: {employee, work_date, hours, rate, amount}
                          #              bank: [] (bank rows are flat)
  "raw_text": str,        # short snippet for the review UI
  # --- filled by categorizer ---
  "category": str|None,        # one of the six vocabulary words below
  "overhead_subtype": str|None,# utilities|insurance|rent|equipment|other (only when category=="Overhead")
  "vendor_normalized": str|None,
  "confidence": float,         # 0.0–1.0
  "needs_review": bool,        # confidence < REVIEW_THRESHOLD
  "is_duplicate": bool,
  "duplicate_of": str|None,    # txn_id of first occurrence, when duplicate
}
```

## 6. Function signatures (binding)

### `pdf_parser.py`
```python
REVIEW_THRESHOLD = 0.85

def extract_transactions(pdf_path: str) -> list[dict]:
    """Full pipeline for one PDF: rule-based extraction → doc-type detection
    → list of transaction dicts (per §5, categorizer fields absent/None).
    Never raises on malformed content; skips unparseable pages."""

def detect_doc_type(text: str) -> str: ...        # one of the four doc_type values

def llm_extract_page(page_text: str, doc_type_hint: str) -> list[dict]:
    """OPTIONAL GLM fallback, requests-based OpenAI-compatible call.
    Config via env: GLM_API_KEY (or OPENAI_API_KEY), GLM_BASE_URL
    (default https://open.bigmodel.cn/api/paas/v4), GLM_MODEL (default glm-4-flash).
    Returns [] when unconfigured or on any error (30s timeout). Used by
    extract_transactions only when a page yields nothing or all-low confidence.
    MUST NOT be required for 100% extraction of the sample PDFs."""
```

### `categorizer.py`
```python
CATEGORIES = ("Revenue", "Direct_Materials", "Subcontractor", "Overhead", "Marketing", "Payroll")

def categorize(txn: dict) -> dict:      # returns txn with category, overhead_subtype, confidence filled
def categorize_all(txns: list[dict]) -> list[dict]:
    # categorize each + normalize vendors + duplicate detection (same
    # vendor_normalized + same invoice_number/amount/date ⇒ later one
    # is_duplicate=True, duplicate_of=<first txn_id>)

def normalize_vendor(raw: str) -> str:
    # canonical: lowercase, strip legal suffixes (co, company, inc, llc, ltd,
    # supply, building products…), collapse whitespace/punct.

def confidence_score(txn: dict, evidence: dict) -> float
```
Confidence guidance: clean structured-table regex hit ≥0.95; paren/comma
handling 0.9; wrapped/merged lines 0.75–0.88; ambiguous or balance
cross-check fails <0.7.

### `export_warehouse_csv.py`
```python
FINANCIAL_LEDGER_HEADERS = [
    "entry_type","category","entry_date","amount","description","vendor",
    "job_id","customer_id","extracted_category","confidence","needs_review",
    "is_duplicate","duplicate_of","source_file","source_page"]
LABOR_LOG_HEADERS = [
    "work_date","hours","hourly_rate","description","employee_name",
    "employee_id","job_id","confidence","needs_review","source_file","source_page"]

def export_financial_ledger(txns: list[dict], out_path: str) -> int   # rows written
def export_labor_logs(txns: list[dict], out_path: str) -> int
def export_all(txns: list[dict], out_dir: str) -> dict                # {"financial_ledger": path, "labor_logs": path}
```
Warehouse mapping (FIXED; warehouse `amount` is ALWAYS positive, `entry_type`
carries direction):
- Revenue → `entry_type=revenue, category=revenue`
- Direct_Materials → `expense/materials`
- Subcontractor → `expense/subcontractor`
- Marketing → `expense/marketing`
- Payroll → `expense/payroll`
- Overhead → `expense/<overhead_subtype>` (subtype defaults to `other`)
`extracted_category` column keeps the six-word vocabulary for transparency.
labor_logs rows come ONLY from payroll `line_items` (one row per employee per
work date; `hours` 0<h≤24, `hourly_rate`≥0). CLI: `python3
export_warehouse_csv.py --pdfs DIR --out DIR` runs the whole pipeline headless.

### `generate_sample_pdfs.py` (Agent 2)
```python
GROUND_TRUTH = { ... }            # per §4 (single source of truth)
def generate_all(out_dir: str = "sample_pdfs") -> dict:  # {"files": [...], "ground_truth_path": ...}
    # writes PDFs + ground_truth/ground_truth.json; idempotent (overwrites cleanly)
if __name__ == "__main__": generate_all()
```

## 7. Acceptance criteria (what the tests lock in)

1. Every transaction in every sample PDF is extracted — 100% of line items,
   no extras. Per-row: date, vendor, description, amount match ground truth.
2. Aggregated dollar totals match ground truth **to the penny**
   (`Decimal(str(x))` comparison on both sides).
3. The re-send invoice is flagged `is_duplicate=True` with `duplicate_of`
   pointing at the original.
4. Vendor aliases normalize to the same canonical form.
5. Known vendors categorize into the expected six-word categories.
6. Exported CSVs: headers exactly per §6, amounts parse as Decimal, all
   `entry_type` amounts positive, labor rows within warehouse CHECK ranges
   (0<h≤24, rate≥0).
7. `pytest` passes fully. Tests run WITHOUT any GLM API key (pure rule-based).
8. Streamlit app boots on port 8603 headless, no import errors.

## 8. Style & environment

- Python 3.9 (system `/usr/bin/python3`). No venv. Deps in `requirements.txt`
  already installed by the coordinator (pdfplumber, pypdf, reportlab,
  streamlit, pandas, pytest, requests).
- Type hints + docstrings on public functions; no prints inside library
  modules (use logging or return values); stdlib `logging` allowed.
- Windows-1252-safe: PDFs are ASCII-only text (reportlab default font).
