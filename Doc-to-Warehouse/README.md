# Doc-to-Warehouse — Financial Document Extractor

Turns messy PDF financial documents (bank statements, vendor invoices,
weekly payroll summaries) into warehouse-ready CSVs — fully offline,
rule-based, no database, no API key required.

## Architecture

```
PDFs in ──► pdf_parser.py ──► categorizer.py ──► export_warehouse_csv.py ──► CSVs out
                │                    │                      │
        extract_transactions   categorize_all       financial_ledger.csv
        detect_doc_type        normalize_vendor     labor_logs.csv
        llm_extract_page       duplicate detection
                │                                          ▲
                └──────────────────► app.py (Streamlit review UI) ─┘
```

1. **`pdf_parser.py`** — rule-based extraction per document type:
   bank statements (x-coordinate-aware column parsing with running-balance
   cross-checks, `$1,234.56` commas, `(1,200.00)` parenthesized negatives,
   continuation/memo-line merging), invoices (header fields + items table
   with wrapped two-line descriptions merged + Subtotal/Tax/TOTAL
   reconciliation), payroll (flat grid rolled into ONE aggregate
   transaction with per-employee line items). Every row carries a
   0–1 `confidence` score; `needs_review` is set below
   `pdf_parser.REVIEW_THRESHOLD` (0.85). `llm_extract_page` is an optional
   GLM fallback for pages that yield nothing or only low-confidence rows —
   the sample PDFs extract 100% without it.
2. **`categorizer.py`** — deterministic keyword categorization into the
   six-word vocabulary (`Revenue`, `Direct_Materials`, `Subcontractor`,
   `Overhead`, `Marketing`, `Payroll`), vendor normalization
   (`normalize_vendor`: lowercase, strip punctuation and trailing legal/
   filler suffix tokens — `co`, `company`, `inc`, `llc`, `ltd`, `supply`,
   `supplies`, `building`, `products`, `corp`, `the`, ... — so
   `ABC Lumber Co.` and `ABC LUMBER SUPPLY INC` both canonicalize to
   `abc lumber`), and duplicate detection (same vendor + invoice number, or
   same vendor + amount + date → later occurrences flagged
   `is_duplicate=True` with `duplicate_of`; rows are never dropped).
3. **`export_warehouse_csv.py`** — fixed warehouse mapping, UTF-8 CSVs,
   plain positive 2dp amounts (`"1234.56"`), `entry_type` carries direction.
   CLI: `python3 export_warehouse_csv.py --pdfs sample_pdfs --out output`.
4. **`app.py`** — Streamlit review UI: upload/generate PDFs, metrics,
   editable transaction grid (category dropdown limited to the six-word
   vocabulary), filters for needs_review/duplicates, CSV downloads from the
   current edited data, and export to `output/`.

## Warehouse mapping (fixed)

| extracted_category | entry_type | warehouse category            |
|--------------------|------------|-------------------------------|
| Revenue            | revenue    | revenue                       |
| Direct_Materials   | expense    | materials                     |
| Subcontractor      | expense    | subcontractor                 |
| Marketing          | expense    | marketing                     |
| Payroll            | expense    | payroll                       |
| Overhead           | expense    | `<overhead_subtype>`: utilities / insurance / rent / equipment / other (default `other`) |

`labor_logs.csv` rows come only from payroll `line_items`
(one row per employee per work date; warehouse CHECK ranges: `0 < hours <= 24`, `hourly_rate >= 0`).

## Run the app

```bash
streamlit run app.py --server.port 8603 --server.headless true
```

(or `python3 -m streamlit run app.py --server.port 8603 --server.headless true`)

## Run the tests

```bash
python3 -m pytest test_extractor.py -v
```

Tests run purely rule-based — no GLM API key needed.

## Sample PDFs

Regenerate the bundled messy samples (bank statement, three vendor
invoices, payroll week, plus a duplicate re-sent invoice) and their ground
truth:

```bash
python3 generate_sample_pdfs.py          # writes sample_pdfs/ + ground_truth/
```

or click **Load Sample Messy PDFs** in the app sidebar.

## Optional GLM API fallback

`extract_transactions` consults the LLM only when a page yields nothing or
only low-confidence rows; it is never required for the samples.

| Env var          | Default                                    |
|------------------|--------------------------------------------|
| `GLM_API_KEY` (or `OPENAI_API_KEY`) | — (unset ⇒ fallback disabled) |
| `GLM_BASE_URL`   | `https://open.bigmodel.cn/api/paas/v4`     |
| `GLM_MODEL`      | `glm-4-flash`                              |
