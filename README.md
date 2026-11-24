KAGGLE DATASET: https://www.kaggle.com/datasets/thebigduce/socal-fintech-capital-and-smb-financial-benchmarks/data

# GLM-5.3 — SMB Financial Intelligence Suite

Fractional-CFO-grade, local-first financial intelligence for small and
mid-size businesses: messy PDF financial documents (bank statements,
vendor invoices, weekly payroll) go in one side and come out as reviewed,
categorized transactions pushed straight into a multi-tenant PostgreSQL
warehouse with row-level security — then a 10-tab analytics dashboard
turns them into charts, anomaly alerts, forecasts, natural-language
queries and an Executive PDF Brief, while a headless watchdog keeps
watching the numbers on cron. The extraction path is 100% offline and
rule-based (no API key, no cloud calls); an optional LLM fallback exists
but is disabled by default. **775 passing tests (776 collected, 1 skip).**

```mermaid
flowchart LR
    PDF["PDFs<br/>bank statements · invoices · payroll"]
    D2W["Doc-to-Warehouse :8603<br/>offline rule-based extractor<br/>confidence 0–1 · DUP / REVIEW flags"]
    GRID["Review grid<br/>editable categories<br/>CSV downloads"]
    PG[("PostgreSQL 17.5 · smb_warehouse<br/>RLS tenancy · materialized views<br/>detect_anomalies · seasonal_trend_v1 forecasts")]
    DW["Data Warehouse :8602<br/>10-tab Streamlit dashboard<br/>Executive PDF Brief · nl_to_sql"]
    WD["SMB-Watchdog<br/>headless CLI · read-only<br/>digest_report.md + .html"]
    DS["Public datasets<br/>Census CBP/ZBP · FRED · BLS OEWS<br/>SBA FOIA · SEC EDGAR / Form D"]
    CSV["output/ CSV exports"]

    PDF --> D2W --> GRID
    GRID -- "Push N transactions (idempotent)" --> PG
    GRID -- "financial_ledger.csv · labor_logs.csv" --> CSV
    PG --> DW
    PG -- "cron · read-only connection" --> WD
    DS -- "harvest scripts" --> PG
```

## Components

| Component | Where | What it is |
|---------------------------|--------------------------------------------|------------|
| PostgreSQL 17.5 | `localhost:5432`, database `smb_warehouse` | Shared multi-tenant warehouse with RLS, 11 core tables + 6 SoCal benchmark tables + `socal_capital_events` + `public_peer_financials`, 8 materialized views + 5 `scoped_*` wrapper views. Binaries: `/Users/Owner/pg`, data dir: `/Users/Owner/pgdata/smb_warehouse`. DSN: `postgresql://postgres:postgres@localhost:5432/smb_warehouse`. Start/stop: `Data Warehouse/start_db.sh` / `stop_db.sh`. |
| Doc-to-Warehouse | `http://localhost:8603` (Streamlit) | Fully offline rule-based PDF extractor: bank statements (x-coordinate column parsing + running-balance cross-checks), vendor invoices, weekly payroll. 0–1 confidence per row, `needs_review` below 0.85, deterministic 6-word categorization, vendor normalization, duplicate detection (`DUP`/`REVIEW` — rows are flagged, never dropped). Optional GLM fallback only for zero/low-confidence pages (sample PDFs extract 100% without it). See `Doc-to-Warehouse/README.md`; binding contract in `Doc-to-Warehouse/CONTRACT.md`. |
| Data Warehouse | `http://localhost:8602` (Streamlit, 10 tabs) | Client-workspace dashboard: charts, job/customer profitability, anomaly alerts, ingest, forecasting (`forecast_engine.py`, model `seasonal_trend_v1`), Executive PDF Brief (`generate_pdf_brief.py`), `nl_to_sql.py` natural-language queries, SoCal benchmark tabs, Docker stack. |
| SMB-Watchdog | headless CLI (no browser) | Read-only cron watchdog: per-company checks on open anomaly alerts, gross margin vs. national benchmark, overtime hot spots + unprofitable whale clients, and 90-day cash runway. Writes `reports/digest_report.md` + `.html`; exits `0` healthy / `2` critical / `3` error. See `SMB-Watchdog/README.md`. |

The bridge between extractor and warehouse is `Doc-to-Warehouse/warehouse_bridge.py`
(`list_active_companies()`, `push_transactions(...)`); it reuses the exporter's
category mapping and delegates all inserts to the warehouse's own `ingest_csv.py`
engine.

## Quick start

```bash
./launch_suite.sh      # starts Postgres (if needed) + both Streamlit apps; idempotent
./stop_suite.sh        # stops both apps + Postgres
```

`launch_suite.sh` prints each component's URL and log path. PIDs and logs
live under `.suite_run/` (`8602.pid`, `8603.pid`, `logs/`). Already-running
components are detected and not double-started — running the launcher
against a live suite is a no-op that still exits 0 with the URLs.

### The end-to-end story (3 steps)

1. **Extract** — open <http://localhost:8603>, drag & drop PDFs (or click
   **Load Sample Messy PDFs**), then **Extract from uploaded PDFs**. Every
   transaction is parsed, categorized and flagged (`DUP` / `REVIEW`).
2. **Review & push** — fix rows inline in the review grid (category
   dropdown per row), pick a **Client workspace (Data Warehouse)**, then
   click **Push N transactions to warehouse**. Rows land in that
   workspace's `financial_ledger` / `labor_logs`; the push is idempotent —
   duplicates are skipped and an exact re-push inserts nothing new — and
   analytics refresh + new anomaly alerts are reported. Follow the
   **Open the Data Warehouse workspace →** link.
3. **Analyze** — open <http://localhost:8602>, select the same client
   workspace in the sidebar and read the charts, job/customer
   profitability, anomaly alerts, forecasts, benchmark tabs — or ask
   questions in plain English via `nl_to_sql` and export the Executive
   PDF Brief.

CSV export works exactly as before: the two download buttons
(`financial_ledger.csv`, `labor_logs.csv`) and **Export to output/ folder**
on port 8603 are untouched — the push panel is an addition, not a
replacement.

### Watchdog on cron

```bash
cd SMB-Watchdog && python3 watchdog.py            # run on demand
```

Cron one-liner (from `SMB-Watchdog/README.md`, which also has a launchd
plist sketch):

```
15 7 * * * cd /Users/Owner/Desktop/GLM-5.3/SMB-Watchdog && /usr/bin/python3 watchdog.py --quiet
```

Per company it checks: (a) open high/medium anomaly alerts, (b) trailing
30-day gross margin by job type vs. the national benchmark target (flag
>5 pp below), (c) top-3 overtime-heavy employees (>184h) plus
unprofitable whale clients, (d) 90-day cash runway (<30d critical,
30–90d warning). Reports go to `reports/digest_report.md` + `.html`;
exit code is `0` healthy, `2` critical, `3` error — easy to alert on.
Connections are read-only (`SET default_transaction_read_only=on`,
verified per connection) and ANSI colors appear only on a TTY.

## Open benchmark datasets (4 packs, CSV + Parquet)

Shipped in the repo for organic benchmarking and teaching use — each pack
carries a `MANIFEST.json` and a data dictionary. Locations:
`Data Warehouse/exports/{smb_benchmark_pack_v1, socal_fintech_benchmark_pack_v2, socal_funding_deals_v1}/`
and `Data Warehouse/peer_financial_ratios_v1/`.

**Totals: 47,539 rows of real public SoCal/SEC data** (47,615 including
the 76-row harvest provenance log) **plus 30,925 rows of deterministic
synthetic demo data — 78,540 rows overall.**

| Pack | Table | Rows × Cols | Provenance |
|------|-------|-------------|------------|
| smb_benchmark_pack_v1 *(synthetic demo, seed 42 — customers/employees ANONYMIZED)* | financial_ledger | 6,373 × 10 | synthetic |
| | labor_logs | 18,504 × 8 | synthetic |
| | jobs | 2,502 × 14 | synthetic |
| | market_benchmarks | 864 × 10 | synthetic |
| | anomaly_alerts | 71 × 14 | synthetic |
| | customers_anonymized | 150 × 6 | synthetic (anonymized) |
| | employees_anonymized | 20 × 7 | synthetic (anonymized) |
| | forecasts | 12 × 12 | synthetic |
| | backtest_results | 4 × 12 | synthetic |
| | mv_monthly_summary | 36 × 15 | synthetic |
| | mv_labor_cost_analysis | 2,195 × 14 | synthetic |
| | mv_customer_profitability | 150 × 15 | synthetic |
| | mv_benchmark_comparison | 36 × 21 | synthetic |
| | mv_job_type_profitability | 8 × 16 | synthetic |
| socal_fintech_benchmark_pack_v2 *(real public data)* | socal_business_patterns | 36,219 × 14 | Census CBP/ZBP 2023 |
| | socal_macro_indicators | 1,238 × 9 | FRED |
| | socal_wage_benchmarks | 48 × 17 | BLS OEWS May-2024 (8 occupations × 6 SoCal regions) |
| | socal_loan_activity | 84 × 13 | SBA FOIA 7(a)/504 |
| | regional_industry_profiles | 36 × 16 | derived (6 verticals × 6 regions) |
| | mv_socal_rate_guidance | 76 × 19 | derived materialized view |
| | socal_harvest_log | 76 × 9 | provenance log of harvest runs |
| socal_funding_deals_v1 *(real public data)* | socal_capital_events | 3,596 × 12 | SEC Form D + SBA SBIR/STTR awards |
| | socal_firm_filings_metadata | 4,925 × 12 | SEC Form D filers |
| | socal_county_quarter_summary | 74 × 7 | derived county-quarter rollup |
| peer_financial_ratios_v1 *(real public data)* | peer_financials | 1,193 × 25 | SEC EDGAR XBRL, 50 public company peers |
| | vertical_quartiles | 50 × 17 | derived vertical quartiles |

## Architecture highlights

- **RLS fail-closed tenancy (real, migration 002).** The `nl2sql_reader`
  role is `NOBYPASSRLS`; 9 `tenant_isolation_*` row-level-security
  policies key on the `app.current_company_id` GUC. No GUC set → 0 rows;
  `WITH CHECK` blocks cross-tenant writes. Proven by
  `test_multitenant_pdf.py`.
- **Anomaly detection rules.** `detect_anomalies()` flags labor overrun
  at >20% over estimate and expense spikes at z > 2 vs. the prior
  6-month mean; `refresh_all_analytics()` recomputes views + alerts.
- **Forecasting.** `forecast_engine.py` fits model `seasonal_trend_v1`,
  upserts forecasts with confidence intervals, and records walk-forward
  `backtest_results` (MAPE / RMSE).
- **Read-only watchdog posture.** Every watchdog connection sets
  `default_transaction_read_only=on` and is verified after set; the
  watchdog can only look, never write.
- **Local-first, privacy-first.** The core pipeline is 100% local and
  offline — no API key required, no cloud calls in the extraction path.
  The only optional outbound piece is the GLM fallback in
  Doc-to-Warehouse, never triggered by the sample PDFs. Say it plainly:
  local-first, works fully offline; optional LLM fallback disabled by
  default.
- **Privacy-safe demo data.** The synthetic pack's customers and
  employees are anonymized; benchmark suppression rules are preserved —
  NULLs in public-source tables are real suppressions, never sentinels.

## Tests

| Module | Tests | How to run |
|--------|-------|------------|
| Doc-to-Warehouse | 59 (49 extractor + 10 e2e bridge) | `cd "Doc-to-Warehouse" && /usr/bin/python3 -m pytest -q` |
| Data Warehouse | 676 | `cd "Data Warehouse" && /usr/bin/python3 -m pytest -q` |
| SMB-Watchdog | 41 | `cd SMB-Watchdog && /usr/bin/python3 -m pytest -q` |
| **Suite total** | **776 collected · 775 passing · 1 skip** | run all three |

The Doc-to-Warehouse tests are fully rule-based (no API key, no
database). The Data Warehouse suite creates its own throwaway databases —
it never touches the live warehouse data.

## Repo layout

```
GLM-5.3/
├── launch_suite.sh / stop_suite.sh   # one-command suite lifecycle
├── Doc-to-Warehouse/                 # :8603 — offline PDF extractor + review UI
│   ├── CONTRACT.md                   # binding module contract
│   └── warehouse_bridge.py           # push bridge → warehouse ingest engine
├── Data Warehouse/                   # :8602 — Streamlit warehouse + analytics
│   ├── start_db.sh / stop_db.sh      # PostgreSQL 17.5 lifecycle
│   ├── forecast_engine.py · nl_to_sql.py · generate_pdf_brief.py
│   ├── exports/                      # 3 open benchmark packs (CSV + Parquet + MANIFEST.json)
│   └── peer_financial_ratios_v1/     # SEC EDGAR XBRL peer-ratios pack
├── SMB-Watchdog/                     # headless cron watchdog (watchdog.py, stdlib + psycopg2)
├── LAUNCH_KIT/                       # launch collateral (Hugging Face dataset card, ...)
├── backup_db.sh / restore_db.sh      # verified pg_dump backup + round-trip restore
└── qa_audit/                         # adversarial / RLS-probe QA scripts + run logs
```

## License

Code is MIT; the four benchmark dataset packs are CC-BY-4.0 with source
attribution preserved per pack. See [LICENSE](LICENSE) for both grants and
`LAUNCH_KIT/HUGGINGFACE_DATASET_CARD.md` for dataset details.
