---
license: cc-by-4.0
task_categories:
- time-series-forecasting
- other
language:
- en
size_categories:
- 10K<n<100K
pretty_name: "SMB Financial Intelligence Benchmark Packs — SoCal Public Data + Synthetic Demo Suite"
tags:
- smb
- finance
- benchmark
- socal
- sec-edgar
- bls
- fred
- census
- sba
- fractional-cfo
configs:
- config_name: smb_benchmark_pack_v1
  description: "Deterministic SYNTHETIC demo SMB simulation (seed 42, painting/remodeling service business) with ANONYMIZED customers/employees — 14 tables, 30,925 rows."
  data_files:
  - split: financial_ledger
    path: "Data Warehouse/exports/smb_benchmark_pack_v1/financial_ledger.csv"
  - split: labor_logs
    path: "Data Warehouse/exports/smb_benchmark_pack_v1/labor_logs.csv"
  - split: jobs
    path: "Data Warehouse/exports/smb_benchmark_pack_v1/jobs.csv"
  - split: market_benchmarks
    path: "Data Warehouse/exports/smb_benchmark_pack_v1/market_benchmarks.csv"
  - split: anomaly_alerts
    path: "Data Warehouse/exports/smb_benchmark_pack_v1/anomaly_alerts.csv"
  - split: customers_anonymized
    path: "Data Warehouse/exports/smb_benchmark_pack_v1/customers_anonymized.csv"
  - split: employees_anonymized
    path: "Data Warehouse/exports/smb_benchmark_pack_v1/employees_anonymized.csv"
  - split: forecasts
    path: "Data Warehouse/exports/smb_benchmark_pack_v1/forecasts.csv"
  - split: backtest_results
    path: "Data Warehouse/exports/smb_benchmark_pack_v1/backtest_results.csv"
  - split: mv_monthly_summary
    path: "Data Warehouse/exports/smb_benchmark_pack_v1/mv_monthly_summary.csv"
  - split: mv_labor_cost_analysis
    path: "Data Warehouse/exports/smb_benchmark_pack_v1/mv_labor_cost_analysis.csv"
  - split: mv_customer_profitability
    path: "Data Warehouse/exports/smb_benchmark_pack_v1/mv_customer_profitability.csv"
  - split: mv_benchmark_comparison
    path: "Data Warehouse/exports/smb_benchmark_pack_v1/mv_benchmark_comparison.csv"
  - split: mv_job_type_profitability
    path: "Data Warehouse/exports/smb_benchmark_pack_v1/mv_job_type_profitability.csv"
- config_name: socal_fintech_benchmark_pack_v2
  description: "REAL harvested public SoCal data: Census CBP/ZBP 2023, FRED, BLS OEWS May-2024, SBA FOIA 7(a)/504 — 7 tables (6 data + 1 provenance log), 37,777 rows."
  data_files:
  - split: socal_business_patterns
    path: "Data Warehouse/exports/socal_fintech_benchmark_pack_v2/socal_business_patterns.csv"
  - split: socal_macro_indicators
    path: "Data Warehouse/exports/socal_fintech_benchmark_pack_v2/socal_macro_indicators.csv"
  - split: socal_wage_benchmarks
    path: "Data Warehouse/exports/socal_fintech_benchmark_pack_v2/socal_wage_benchmarks.csv"
  - split: socal_loan_activity
    path: "Data Warehouse/exports/socal_fintech_benchmark_pack_v2/socal_loan_activity.csv"
  - split: regional_industry_profiles
    path: "Data Warehouse/exports/socal_fintech_benchmark_pack_v2/regional_industry_profiles.csv"
  - split: mv_socal_rate_guidance
    path: "Data Warehouse/exports/socal_fintech_benchmark_pack_v2/mv_socal_rate_guidance.csv"
  - split: socal_harvest_log
    path: "Data Warehouse/exports/socal_fintech_benchmark_pack_v2/socal_harvest_log.csv"
- config_name: socal_funding_deals_v1
  description: "REAL SEC Form D + SBA SBIR/STTR award data for SoCal — 3 tables, 8,595 rows."
  data_files:
  - split: socal_capital_events
    path: "Data Warehouse/exports/socal_funding_deals_v1/socal_capital_events.csv"
  - split: socal_firm_filings_metadata
    path: "Data Warehouse/exports/socal_funding_deals_v1/socal_firm_filings_metadata.csv"
  - split: socal_county_quarter_summary
    path: "Data Warehouse/exports/socal_funding_deals_v1/socal_county_quarter_summary.csv"
- config_name: peer_financial_ratios_v1
  description: "REAL SEC EDGAR XBRL financial ratios for 50 public company peers — 2 tables, 1,243 rows."
  data_files:
  - split: peer_financials
    path: "Data Warehouse/peer_financial_ratios_v1/peer_financials.csv"
  - split: vertical_quartiles
    path: "Data Warehouse/peer_financial_ratios_v1/vertical_quartiles.csv"
---

# SMB Financial Intelligence Benchmark Packs — SoCal Public Data + Synthetic Demo Suite

> **License note:** the dataset packs are licensed under **CC-BY-4.0**
> (confirmed by the owner 2026-11-23; see the repo's `LICENSE` for the
> code-vs-data split). Pack 1 is original synthetic content of this repo;
> packs 2–4 are redistributions of U.S. public data
> (Census/FRED/BLS/SBA/SEC), whose upstream terms should be re-checked
> before any commercial reuse.

## Abstract

Four tabular benchmark packs built around a local-first SMB financial
intelligence suite. One pack (`smb_benchmark_pack_v1`) is a deterministic
**synthetic benchmark** simulation of a painting/remodeling service SMB (full
operational ledger, labor logs, jobs, anomalies, forecasts — customers and
employees anonymized) — it is clearly labeled SYNTHETIC and is intended for
demos, teaching and pipeline tests; it contains **no real-company financial
data** and must not be quoted as market statistics. Three packs are **real,
harvested public data** about Southern California: business patterns (Census
CBP/ZBP 2023), macro indicators (FRED), occupational wages (BLS OEWS
May-2024), SBA 7(a)/504 lending (FOIA release), startup funding events (SEC
Form D + SBA SBIR/STTR awards), and XBRL-derived financial ratios for 50
public company peers (SEC EDGAR).

**Totals (verified on disk):**

| | Tables | Rows |
|---|---|---|
| smb_benchmark_pack_v1 (synthetic, seed 42) | 14 | 30,925 |
| socal_fintech_benchmark_pack_v2 (real) | 7 (6 data + 1 provenance log) | 37,777 |
| socal_funding_deals_v1 (real) | 3 | 8,595 |
| peer_financial_ratios_v1 (real) | 2 | 1,243 |
| **Overall** | **26** | **78,540** |

Real public data: **47,539 data rows** (47,615 including the 76-row harvest
provenance log). Every table ships as **CSV + Parquet**, and every pack
folder carries a `MANIFEST.json` and a data dictionary.

## The 4 packs

| Config | Provenance | Rows | Synthetic vs. real | License note |
|--------|-----------|------|--------------------|--------------|
| `smb_benchmark_pack_v1` | In-repo simulation (deterministic, seed 42; painting/remodeling service business) | 30,925 | SYNTHETIC — customers/employees are ANONYMIZED | Original content; cc-by-4.0 (confirmed) |
| `socal_fintech_benchmark_pack_v2` | Census CBP/ZBP 2023, FRED, BLS OEWS May-2024, SBA FOIA 7(a)/504 | 37,777 (37,701 data + 76 harvest log) | REAL public data | U.S. public data; cc-by-4.0 (confirmed) |
| `socal_funding_deals_v1` | SEC Form D filings + SBA SBIR/STTR awards | 8,595 | REAL public data | Public filings; cc-by-4.0 (confirmed) |
| `peer_financial_ratios_v1` | SEC EDGAR XBRL, 50 public company peers | 1,243 | REAL public data (derived ratios) | Public filings; cc-by-4.0 (confirmed) |

## Tables and columns

### `smb_benchmark_pack_v1` — 14 tables, 30,925 rows (SYNTHETIC, seed 42)

| Table | Rows × Cols | Columns |
|-------|-------------|---------|
| financial_ledger | 6,373 × 10 | id, job_id, customer_id, entry_type, category, entry_date, amount, description, vendor, created_at |
| labor_logs | 18,504 × 8 | id, job_id, employee_id, work_date, hours, hourly_rate, description, created_at |
| jobs | 2,502 × 14 | id, job_number, customer_id, job_type_id, status, job_date, completion_date, quoted_price, estimated_labor_cost, estimated_hours, actual_revenue, notes, created_at, updated_at |
| market_benchmarks | 864 × 10 | id, job_type_id, period, region, benchmark_kind, series_id, avg_price, avg_labor_cost_per_job, avg_margin_pct, source |
| anomaly_alerts | 71 × 14 | id, anomaly_type, job_id, category, period, severity, metric_value, baseline_value, deviation_pct, z_score, details, detected_at, is_resolved, resolved_at |
| customers_anonymized | 150 × 6 | id, name, city, state, customer_since, is_active |
| employees_anonymized | 20 × 7 | id, first_name, last_name, hire_date, hourly_rate, role, is_active |
| forecasts | 12 × 12 | id, series, period_start, model, predicted, ci_low, ci_high, mape, rmse, horizon_months, run_date, details |
| backtest_results | 4 × 12 | id, series, model, fold, train_start, train_end, test_start, test_end, mape, rmse, predictions, created_at |
| mv_monthly_summary | 36 × 15 | month_start, year, month_name, revenue, materials_expense, labor_expense_ledger, equipment_expense, other_expense, total_expense, labor_hours, labor_cost, gross_profit, gross_margin_pct, job_completions, new_customers |
| mv_labor_cost_analysis | 2,195 × 14 | job_id, job_number, job_type, customer_name, status, total_hours, labor_cost, estimated_labor_cost, estimated_hours, labor_variance_pct, labor_cost_per_hour, employee_count, first_work_date, last_work_date |
| mv_customer_profitability | 150 × 15 | customer_id, customer_name, is_active, revenue, job_cost, acquisition_cost, total_cost, gross_margin, margin_pct, job_count, avg_job_value, first_job_date, last_job_date, days_since_last_job, is_whale |
| mv_benchmark_comparison | 36 × 21 | period, year, month_name, internal_labor_rate, internal_labor_hours, internal_labor_yoy_pct, internal_materials_per_job, internal_materials_yoy_pct, market_wage_national(+yoy), market_wage_regional(+yoy), ppi_lumber/ppi_paint/ppi_building_materials(+yoy), labor_inflation_gap_national_pct, labor_inflation_gap_regional_pct, materials_inflation_gap_pct |
| mv_job_type_profitability | 8 × 16 | job_type_id, job_type, revenue, materials_cost, other_cost, labor_cost, total_cost, gross_margin, margin_pct, job_count, avg_job_price, labor_hours, benchmark_period, benchmark_avg_price, benchmark_avg_margin_pct, price_vs_benchmark_pct |

### `socal_fintech_benchmark_pack_v2` — 7 tables, 37,777 rows (REAL)

| Table | Rows × Cols | Columns |
|-------|-------------|---------|
| socal_business_patterns | 36,219 × 14 | id, geo_level, geo_fips, geo_name, region_code, naics_code, naics_label, data_year, establishments, employment, employment_flag, annual_payroll_usd, payroll_per_employee, source |
| socal_macro_indicators | 1,238 × 9 | id, region_code, series_id, series_name, category, period, value, unit, source |
| socal_wage_benchmarks | 48 × 17 | id, region_code, occupation_code, occupation_title, role_label, data_year, hourly_p25/median/p75/p90, annual_p25/median/p75/p90, employment, employment_rse, source |
| socal_loan_activity | 84 × 13 | id, program, fiscal_year, geo_level, geo_fips, geo_name, region_code, loans_count, gross_approval_usd, avg_loan_usd, top_lender, top_lender_loans, source |
| regional_industry_profiles | 36 × 16 | id, vertical_key, vertical_name, region_code, anchor_occupation_code, gross_margin_pct (+p25/p75), payroll_to_revenue_ratio, cac_usd, utilization_target_pct, locality_burden_pct, median_salary_usd, notes, source, created_at |
| mv_socal_rate_guidance | 76 × 19 | region_code, vertical_key, vertical_name, data_year, anchor_occupation_code, role_label, annual_median_salary, locality_burden_pct, utilization_target_pct, gross_margin_pct, hourly_cost, loaded_cost, suggested_billable_rate, cac_usd, payroll_to_revenue_ratio, statewide_billable_rate, rate_vs_statewide_pct, la_billable_rate, rate_vs_la_pct |
| socal_harvest_log | 76 × 9 | id, run_id, source_name, url, status, http_status, rows_loaded, message, fetched_at *(provenance log of each harvest run)* |

### `socal_funding_deals_v1` — 3 tables, 8,595 rows (REAL)

| Table | Rows × Cols | Columns |
|-------|-------------|---------|
| socal_capital_events | 3,596 × 12 | event_id, company_name, city, county, zip_code, sector, event_type, amount_raised, offering_date, agency_or_filing_id, source_url, raw_metadata |
| socal_firm_filings_metadata | 4,925 × 12 | filer_name, cik, accession, form, filed_date, period_of_report, city, state, zip_code, county, total_holdings_value_usd, source_url |
| socal_county_quarter_summary | 74 × 7 | county, quarter_start, quarter_label, deal_count, total_capital_raised, median_deal_size, top_sectors |

### `peer_financial_ratios_v1` — 2 tables, 1,243 rows (REAL)

| Table | Rows × Cols | Columns |
|-------|-------------|---------|
| peer_financials | 1,193 × 25 | record_id, cik, ticker, company_name, hq_state, is_socal, vertical, fiscal_year, fiscal_period, revenue, gross_profit, rd_expense, sga_expense, operating_income, net_income, operating_cash_flow, gross_margin_pct, operating_margin_pct, rd_pct_of_rev, sga_pct_of_rev, source_url, source_form, accession_no, source_tags, derived_q4 |
| vertical_quartiles | 50 × 17 | vertical, fiscal_year, socal_flag, peer_group, n_records, gross_margin p25/median/p75, operating_margin p25/median/p75, rd_pct_of_rev p25/median/p75, sga_pct_of_rev p25/median/p75 |

## Provenance and collection

- **BLS OEWS May-2024** — occupational wage percentiles for 8 occupations ×
  6 SoCal regions (`socal_wage_benchmarks`).
- **FRED** — regional macro indicator series (`socal_macro_indicators`).
- **U.S. Census CBP/ZBP 2023** — county/ZIP business patterns: establishments,
  employment, annual payroll by NAICS (`socal_business_patterns`).
- **SBA FOIA release of 7(a)/504 loan activity** — loan counts and gross
  approvals by geography and fiscal year (`socal_loan_activity`).
- **SEC Form D filings + SBA SBIR/STTR award records** — SoCal capital
  events with filing metadata and source URLs (`socal_funding_deals_v1`).
- **SEC EDGAR XBRL** — income-statement lines and derived ratios for 50
  public peers, with `source_form` / `accession_no` / `source_url` kept on
  every row (`peer_financial_ratios_v1`).
- Harvest scripts live in the source repo; every run is recorded in
  `socal_harvest_log.csv` (source, URL, HTTP status, rows loaded,
  timestamp), so the lineage of each table is reproducible.
- In the synthetic pack, customers and employees are **ANONYMIZED**
  (`customers_anonymized` / `employees_anonymized`); no real person or
  client identifiers are present anywhere in the packs.
- **Suppression rules preserved:** where BLS/Census suppress or withhold a
  value (e.g. `employment_flag`, withheld wage cells), the cell is a true
  NULL — NULLs are never replaced with sentinels such as 0 or -1.

## Intended uses

- Benchmarking SMB margins, billable rates and payroll against regional
  public data (the `mv_socal_rate_guidance` / `regional_industry_profiles`
  tables encode margin → loaded cost → suggested rate math).
- Funding-landscape research: SoCal deal flow by county, sector and
  quarter from public filings.
- Teaching and demo material for multi-tenant financial warehouses,
  anomaly detection (labor overrun / expense spikes) and small-business
  forecasting evaluation (walk-forward backtests ship in pack 1).
- Regression fixtures for ETL pipelines that ingest mixed CSV/Parquet
  financial data.

## Limitations

- Pack 1 is **entirely synthetic** (seed 42, painting/remodeling vertical):
  realistic in shape, not a real company — do not cite its numbers as
  market evidence.
- Geographic scope is **Southern California** (6 regions); national
  figures appear only as benchmark targets.
- BLS OEWS and Census tables inherit **federal suppression rules** —
  expect NULLs where values were withheld.
- Public aggregates and filings describe establishments/filers, not
  client-level confidential financials; Form D `amount_raised` is
  self-reported by issuers.
- Peer ratios are derived from XBRL with a `derived_q4` flag marking
  estimated Q4 figures — re-derive before drawing fine-grained
  conclusions.

## Usage

```python
import pandas as pd

base = "Data Warehouse/exports"

ledger = pd.read_csv(f"{base}/smb_benchmark_pack_v1/financial_ledger.csv")
wages  = pd.read_parquet(f"{base}/socal_fintech_benchmark_pack_v2/socal_wage_benchmarks.parquet")
deals  = pd.read_parquet(f"{base}/socal_funding_deals_v1/socal_capital_events.parquet")
peers  = pd.read_parquet("Data Warehouse/peer_financial_ratios_v1/peer_financials.parquet")
```

DuckDB directly over the folders (no import step):

```python
import duckdb
con = duckdb.connect()

print(con.sql("""
    SELECT occupation_title, region_code, hourly_median
    FROM read_csv_auto('Data Warehouse/exports/socal_fintech_benchmark_pack_v2/socal_wage_benchmarks.csv')
    ORDER BY hourly_median DESC
    LIMIT 10
""").df())

print(con.sql("""
    SELECT vertical, avg(gross_margin_median) AS avg_median_gross_margin
    FROM read_csv_auto('Data Warehouse/peer_financial_ratios_v1/vertical_quartiles.csv')
    GROUP BY vertical
    ORDER BY 2 DESC
""").df())
```

Or via `datasets` once uploaded to the Hub (each table is a named split):

```python
from datasets import load_dataset
ds = load_dataset("<your-username>/smb-financial-benchmark-packs",
                  "smb_benchmark_pack_v1", split="financial_ledger")
```

> **Path note:** the `data_files` globs use the in-repo layout
> (`Data Warehouse/...`, with a space). If you flatten the pack folders to
> the Hub repo root on upload, update the `path` patterns accordingly.
> Parquet mirrors of every CSV are included in each pack folder.

## Citation

Placeholder BibTeX — fill in the author/organization, the final year and
the published URL before release:

```bibtex
@misc{smb_financial_benchmark_packs,
  title  = {SMB Financial Intelligence Benchmark Packs: SoCal Public Data and a Synthetic SMB Demo Suite},
  author = {[FILL IN: author or organization]},
  year   = {[FILL IN: year of release]},
  note   = {One synthetic demo pack (seed 42) plus three real packs harvested from U.S. Census CBP/ZBP 2023, FRED, BLS OEWS May-2024, SBA FOIA 7(a)/504, and SEC EDGAR (Form D + XBRL)},
  url    = {[FILL IN: dataset or repository URL]}
}
```

## Kaggle upload addendum

**What to zip** (from the repo root):

```bash
zip -r smb-financial-benchmark-packs.zip \
  "Data Warehouse/exports/smb_benchmark_pack_v1" \
  "Data Warehouse/exports/socal_fintech_benchmark_pack_v2" \
  "Data Warehouse/exports/socal_funding_deals_v1" \
  "Data Warehouse/peer_financial_ratios_v1"
```

Then save this card as `DATASET_CARD.md` inside the zip (Kaggle surfaces
the dataset description from the card/README), and keep the four
`MANIFEST.json` files and data dictionaries in the zip so provenance
travels with the data.

**Suggested Kaggle dataset title:**
"SMB Financial Benchmark Packs — SoCal Public Data + Synthetic Demo Ledger"

**Suggested Kaggle description blurb:**

> Four tabular benchmark packs for small-business financial analytics: one
> deterministic synthetic SMB ledger (14 tables, 30,925 rows, seed 42,
> anonymized customers/employees) for demos and pipeline tests, plus three
> REAL public-data packs on Southern California — Census CBP/ZBP 2023
> business patterns, FRED macro indicators, BLS OEWS May-2024 wages, SBA
> FOIA 7(a)/504 lending, SEC Form D / SBIR-STTR funding events, and SEC
> EDGAR XBRL peer ratios (50 public peers). 26 tables, 78,540 rows total
> (47,539 real data rows), each table in CSV + Parquet with MANIFEST.json
> files, data dictionaries and a full harvest provenance log. Built as the
> open-data layer of a local-first SMB financial intelligence suite.
