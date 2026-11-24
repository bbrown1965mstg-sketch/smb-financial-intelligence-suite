#!/usr/bin/env python3
"""
export_dataset_pack.py -- export documented, shareable dataset packs.

OUTPUT (stated assumption): the user asked for "/exports/smb_benchmark_pack_v1/";
this is implemented as PROJECT-RELATIVE `exports/smb_benchmark_pack_v1/`
(i.e. `<project folder>/exports/smb_benchmark_pack_v1/`) so the pack travels
with the repo and nothing is written outside the project folder. The SoCal
Finance/Tech benchmark pack v2 goes to `exports/socal_fintech_benchmark_pack_v2/`.

TWO PACKS (--pack {all,v1,socal_v2}, default all):
  v1        the operational warehouse: base tables (jobs, labor_logs, ledger,
            anonymized customers/employees, market_benchmarks incl. the REAL
            BLS/FRED macro rows, anomaly_alerts, forecasts, backtest_results)
            + the 5 KPI materialized views. FILES spec below (untouched).
  socal_v2  the SoCal Finance/Tech benchmark pack: socal_wage_benchmarks,
            socal_macro_indicators, socal_business_patterns,
            socal_loan_activity, regional_industry_profiles,
            socal_harvest_log + mv_socal_rate_guidance (SOCAL_FILES spec
            below; parquet twins for the wage/business/loan tables).

WHAT IT EXPORTS (all via READ-ONLY SELECTs against the live warehouse;
nothing in the DB is modified):
  * The spec'd tables/views as clean UTF-8 CSVs (+ Parquet twins where the
    spec says so).
  * DATA_DICTIONARY.md per pack -- GENERATED FROM THE SPEC below, never
    hand-counted: per file: description, provenance (REAL harvested data vs
    MODELED profiles distinguished explicitly), exact row count, and every
    column with its dtype + one-line description. The DataFrame's columns
    are ASSERTED against the spec at export time so doc and data cannot drift.
  * MANIFEST.json per pack -- pack name, file name, rows, sha256, bytes,
    exported_at, generator versions (pandas/pyarrow/python) + how to
    regenerate the pack.

Idempotent: overwrites its own outputs; creates exports/ if missing.

Regenerate everything with:
    python3 seed_demo.py && python3 fetch_benchmarks.py && \
        python3 harvest_socal_fintech.py && python3 export_dataset_pack.py

Run:  python3 export_dataset_pack.py [--pack {all,v1,socal_v2}]
"""

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
from decimal import Decimal
from pathlib import Path

import pandas as pd
import psycopg2
import pyarrow

DB = os.environ.get("DATABASE_URL",
                    "postgresql://postgres:postgres@localhost:5432/smb_warehouse")
PACK_DIR = Path(__file__).resolve().parent / "exports" / "smb_benchmark_pack_v1"
SOCAL_PACK_DIR = (Path(__file__).resolve().parent / "exports"
                  / "socal_fintech_benchmark_pack_v2")

# ---------------------------------------------------------------------------
# THE SPEC -- single source of truth for every exported file.
# "columns" maps column name -> one-line description, IN OUTPUT ORDER.
# At export time the DataFrame's columns are asserted to match exactly.
# ---------------------------------------------------------------------------
_S = "Synthetic simulation via seed_demo.py (random seed 42, 36 months ending at generation date)"
_S2 = "Model output of forecast_engine.py (seasonal_trend_v1) computed over the simulated history"
_D = "Derived analytics: materialized view built by analytics_views.sql over the tables above"

FILES = {
    "jobs": dict(
        sql="SELECT * FROM jobs ORDER BY id", parquet=True,
        description="Work orders: 2,500+ service jobs across 36 months with status mix, quotes, labor budgets.",
        provenance=_S,
        columns={
            "id": "Surrogate primary key.",
            "job_number": "Human-readable work order id (e.g. JOB-2024-0001); unique business key.",
            "customer_id": "FK to customers.id.",
            "job_type_id": "FK to job_types.id (service line).",
            "status": "One of quoted/scheduled/in_progress/completed/cancelled.",
            "job_date": "Date the job was quoted/scheduled.",
            "completion_date": "Date work completed (NULL unless completed).",
            "quoted_price": "Price quoted to the customer (>= 0); ledger revenue is the source of truth.",
            "estimated_labor_cost": "Labor budget baseline used by the anomaly detector (NULL = no budget).",
            "estimated_hours": "Budgeted labor hours (informational).",
            "actual_revenue": "Final billed amount (informational mirror of the ledger).",
            "notes": "Free text (e.g. '<Job type> job').",
            "created_at": "Row insert timestamp (UTC).",
            "updated_at": "Row last-update timestamp (UTC).",
        }),
    "labor_logs": dict(
        sql="SELECT * FROM labor_logs ORDER BY id", parquet=True,
        description="Timesheets: per-employee, per-job, per-day hour entries with the hourly-rate snapshot at the time of work.",
        provenance=_S,
        columns={
            "id": "Surrogate primary key.",
            "job_id": "FK to jobs.id.",
            "employee_id": "FK to employees.id.",
            "work_date": "Date the hours were worked (weekends nudged to Friday).",
            "hours": "Hours worked that day (0 < h <= 24).",
            "hourly_rate": "Historical rate SNAPSHOT at the time of work (overtime rows carry 1.5x).",
            "description": "Free text (e.g. 'On-site work', 'On-site work (overtime)').",
            "created_at": "Row insert timestamp (UTC).",
        }),
    "financial_ledger": dict(
        sql="SELECT * FROM financial_ledger ORDER BY id", parquet=True,
        description="Single source of truth for money: every revenue and expense entry. Attribution exclusive: job_id XOR customer_id (both NULL = overhead).",
        provenance=_S,
        columns={
            "id": "Surrogate primary key.",
            "job_id": "FK to jobs.id; NULL for overhead/customer-attributed rows.",
            "customer_id": "FK to customers.id for customer-level entries (e.g. acquisition cost); exclusive with job_id.",
            "entry_type": "'revenue' or 'expense' -- the direction; amount is always positive.",
            "category": "materials/labor/equipment/permits/subcontractor/utilities/insurance/marketing/payroll/rent/customer_acquisition/job_billing/job_deposit/other.",
            "entry_date": "Effective date of the entry.",
            "amount": "Positive magnitude; direction lives in entry_type.",
            "description": "Free text (e.g. 'Final billing - JOB-2024-0007').",
            "vendor": "Payee (expense) / payer (revenue).",
            "created_at": "Row insert timestamp (UTC).",
        }),
    "customers_anonymized": dict(
        sql="SELECT id, name, city, state, customer_since, is_active FROM customers ORDER BY id",
        parquet=True,
        description="Customers with PII stripped (email, phone, address, contact_name dropped). Company/account name, city/state and tenure retained for analytics.",
        provenance=_S + "; PII columns removed at export",
        columns={
            "id": "Surrogate primary key (FK target of jobs.customer_id, ledger.customer_id).",
            "name": "Account name (synthetic: households and local businesses).",
            "city": "City (synthetic Missouri towns).",
            "state": "Two-letter state code.",
            "customer_since": "Date the client relationship began (feeds new_customers).",
            "is_active": "Whether the account is currently active.",
        }),
    "employees_anonymized": dict(
        sql="SELECT id, first_name, last_name, hire_date, hourly_rate, role, is_active FROM employees ORDER BY id",
        parquet=True,
        description="Crew members with PII stripped (email, phone dropped). Names/roles/rates retained (synthetic roster).",
        provenance=_S + "; PII columns removed at export",
        columns={
            "id": "Surrogate primary key (FK target of labor_logs.employee_id).",
            "first_name": "First name (synthetic).",
            "last_name": "Last name (synthetic).",
            "hire_date": "Date hired.",
            "hourly_rate": "CURRENT rate; historical cost uses labor_logs.hourly_rate snapshots.",
            "role": "Job title (owner/PM/lead/craftsperson/apprentice).",
            "is_active": "Whether currently employed.",
        }),
    "market_benchmarks": dict(
        sql="SELECT * FROM market_benchmarks ORDER BY id", parquet=True,
        description="External baselines. benchmark_kind='job_market' rows are per-service-line comparables from the simulation; 'wage'/'ppi' rows are REAL public economic series (BLS via FRED). Provenance per row: series_id + source.",
        provenance="MIXED: job_market rows = " + _S
                   + "; wage/ppi rows = REAL BLS/FRED data loaded by fetch_benchmarks.py "
                     "(keyless FRED CSV endpoint; series/source columns record the origin)",
        columns={
            "id": "Surrogate primary key.",
            "job_type_id": "FK to job_types.id for job_market rows; NULL for macro wage/ppi rows.",
            "period": "First day of the month the benchmark applies to.",
            "region": "'national' / 'us-national' / 'us-mo'.",
            "benchmark_kind": "'job_market' (comparables) or 'wage' / 'ppi' (real macro series).",
            "series_id": "BLS/FRED series id for macro rows (e.g. CES2000000008, WPUSI012011); NULL otherwise.",
            "avg_price": "job_market: benchmark avg job price; wage: $/hr; ppi: index level.",
            "avg_labor_cost_per_job": "job_market: benchmark avg labor cost per job; 0 for macro rows.",
            "avg_margin_pct": "job_market: benchmark avg gross margin %; 0 for macro rows.",
            "source": "Human-readable provenance (e.g. 'BLS PPI commodity: softwood lumber, via FRED').",
        }),
    "anomaly_alerts": dict(
        sql="SELECT * FROM anomaly_alerts ORDER BY id", parquet=True,
        description="Anomaly detector output: labor_overrun (job labor cost >20% over estimate) and expense_spike (category month z > 2 vs prior 6 months) alerts with severity and JSONB context.",
        provenance=_S + "; raised by detect_anomalies() in analytics_views.sql",
        columns={
            "id": "Surrogate primary key.",
            "anomaly_type": "'labor_overrun' or 'expense_spike'.",
            "job_id": "Job subject for labor_overrun; NULL for expense_spike.",
            "category": "Ledger expense category for expense_spike; NULL otherwise.",
            "period": "Month start of an expense_spike; NULL for labor_overrun.",
            "severity": "'low' / 'medium' / 'high' (thresholds documented in the README).",
            "metric_value": "Observed value (actual labor cost, or the month's category total).",
            "baseline_value": "Expected value (estimate, or prior-6-month mean).",
            "deviation_pct": "Percent deviation from baseline (NULL when baseline is 0).",
            "z_score": "Standard deviations above the rolling mean (expense spikes only).",
            "details": "JSONB context (job_number/actual/estimate or category/period/mean/stddev).",
            "detected_at": "When the detector raised the alert (UTC).",
            "is_resolved": "Triage flag; open alerts are deduplicated by partial unique indexes.",
            "resolved_at": "When the alert was resolved (NULL while open).",
        }),
    "forecasts": dict(
        sql="SELECT * FROM forecasts ORDER BY series, horizon_months", parquet=True,
        description="seasonal_trend_v1 point forecasts + 90% CI for the four monthly series; upserted per (series, month, model) so it holds the latest run.",
        provenance=_S2,
        columns={
            "id": "Surrogate primary key.",
            "series": "revenue / labor_cost / material_expense / net_cash_flow.",
            "period_start": "First day of the forecast month.",
            "model": "Model id ('seasonal_trend_v1').",
            "predicted": "Point forecast for the month.",
            "ci_low": "90% CI lower bound (clamped >= 0 except net_cash_flow).",
            "ci_high": "90% CI upper bound.",
            "mape": "Backtest accuracy: MAPE for positive series, WAPE for net_cash_flow.",
            "rmse": "Pooled backtest RMSE (also the CI sigma).",
            "horizon_months": "Steps ahead from the training window's last month (1 = next month).",
            "run_date": "Date the engine produced the row.",
            "details": "JSONB model provenance (train window, seasonal factors, drift, sigma).",
        }),
    "backtest_results": dict(
        sql="SELECT * FROM backtest_results ORDER BY series, fold", parquet=True,
        description="Walk-forward validation folds (train 30 months / test 6) with per-month predictions stored as JSONB.",
        provenance=_S2,
        columns={
            "id": "Surrogate primary key.",
            "series": "Validated series (same four as forecasts).",
            "model": "Model id that produced the fold.",
            "fold": "Fold label encoding the window (e.g. train_2023-12_2026-05_test_2026-06_2026-11).",
            "train_start": "First month of the training window.",
            "train_end": "Last month of the training window (no look-ahead beyond this).",
            "test_start": "First held-out month.",
            "test_end": "Last held-out month.",
            "mape": "Fold accuracy (MAPE, or WAPE for net_cash_flow).",
            "rmse": "Fold root mean squared error.",
            "predictions": "JSONB array of per-month {period, predicted, actual, abs_pct_err}.",
            "created_at": "Row write time.",
        }),
    "mv_monthly_summary": dict(
        sql="SELECT * FROM mv_monthly_summary ORDER BY month_start", parquet=False,
        description="Cash P&L per month: revenue, expense buckets, labor hours/cost, completions, new customers.",
        provenance=_D,
        columns={
            "month_start": "First day of the month.",
            "year": "Calendar year.",
            "month_name": "Month name (e.g. 'June').",
            "revenue": "Ledger revenue entries in the month.",
            "materials_expense": "Ledger materials expenses.",
            "labor_expense_ledger": "Ledger 'labor' category (payroll recharge; the CASH view of labor).",
            "equipment_expense": "Ledger equipment rentals.",
            "other_expense": "All other expense categories (permits, rent, marketing, ...).",
            "total_expense": "Sum of all expense entries.",
            "labor_hours": "Hours from labor_logs.",
            "labor_cost": "hours * rate from labor_logs (the COST view of labor).",
            "gross_profit": "revenue - total_expense.",
            "gross_margin_pct": "gross_profit / revenue * 100.",
            "job_completions": "Jobs completed (completion_date in month AND status='completed').",
            "new_customers": "Customers whose customer_since falls in the month.",
        }),
    "mv_job_type_profitability": dict(
        sql="SELECT * FROM mv_job_type_profitability ORDER BY revenue DESC", parquet=False,
        description="One row per service line: revenue, cost buckets, margin, and the latest national market benchmark comparison.",
        provenance=_D,
        columns={
            "job_type_id": "FK to job_types.id.",
            "job_type": "Service line name.",
            "revenue": "Ledger revenue attributed to the type's jobs.",
            "materials_cost": "Ledger materials expenses for those jobs.",
            "other_cost": "Other ledger expenses (equipment, permits, subcontractor, ...).",
            "labor_cost": "Labor cost from labor_logs (ledger 'labor' rows excluded to avoid double counting).",
            "total_cost": "materials + other + labor.",
            "gross_margin": "revenue - total_cost.",
            "margin_pct": "gross_margin / revenue * 100.",
            "job_count": "Jobs of this type.",
            "avg_job_price": "Average quoted price.",
            "labor_hours": "Total logged hours.",
            "benchmark_period": "Month of the latest 'national' job_market benchmark row.",
            "benchmark_avg_price": "Market benchmark average price for the type.",
            "benchmark_avg_margin_pct": "Market benchmark average margin %.",
            "price_vs_benchmark_pct": "Our avg price vs the benchmark (+ = priced above market).",
        }),
    "mv_customer_profitability": dict(
        sql="SELECT * FROM mv_customer_profitability ORDER BY revenue DESC", parquet=False,
        description="One row per customer (incl. zero-job customers): revenue, costs incl. acquisition cost, margin, and the whale flag (top-quintile revenue + negative net margin).",
        provenance=_D + "; account names retained (synthetic), no contact PII",
        columns={
            "customer_id": "FK to customers.id.",
            "customer_name": "Account name (synthetic).",
            "is_active": "Current active flag.",
            "revenue": "Ledger revenue for the customer's jobs.",
            "job_cost": "Job-attributed costs (materials + other + labor).",
            "acquisition_cost": "Customer-attributed ledger expenses (CAC, win-back).",
            "total_cost": "job_cost + acquisition_cost.",
            "gross_margin": "revenue - total_cost (a NET margin).",
            "margin_pct": "gross_margin / revenue * 100.",
            "job_count": "Jobs for the customer (0 = acquired, never booked).",
            "avg_job_value": "revenue / job_count.",
            "first_job_date": "Earliest job date.",
            "last_job_date": "Latest job date.",
            "days_since_last_job": "Days since last_job_date (recency).",
            "is_whale": "Top-quintile revenue AND negative net margin.",
        }),
    "mv_labor_cost_analysis": dict(
        sql="SELECT * FROM mv_labor_cost_analysis ORDER BY labor_variance_pct DESC NULLS LAST",
        parquet=False,
        description="One row per job with logged hours: actual labor vs estimate, variance %, crew size, cost per hour.",
        provenance=_D,
        columns={
            "job_id": "FK to jobs.id.",
            "job_number": "Human-readable job id.",
            "job_type": "Service line.",
            "customer_name": "Account name (synthetic).",
            "status": "Job status.",
            "total_hours": "Sum of logged hours.",
            "labor_cost": "Sum(hours * rate snapshot).",
            "estimated_labor_cost": "Budget baseline (NULL = no estimate).",
            "estimated_hours": "Budgeted hours.",
            "labor_variance_pct": "(actual - estimate) / estimate * 100.",
            "labor_cost_per_hour": "Blended $/hr actually paid.",
            "employee_count": "Distinct crew members who logged hours.",
            "first_work_date": "First logged work date.",
            "last_work_date": "Last logged work date.",
        }),
    "mv_benchmark_comparison": dict(
        sql="SELECT * FROM mv_benchmark_comparison ORDER BY period", parquet=False,
        description="Per month: our internal labor rate / materials-per-job YoY vs the REAL BLS wage and PPI series, plus inflation-gap columns (positive = we inflate faster than the market).",
        provenance=_D + "; market columns come from the real BLS/FRED macro rows",
        columns={
            "period": "First day of the month.",
            "year": "Calendar year.",
            "month_name": "Month name.",
            "internal_labor_rate": "Blended $/hr actually paid (hours-weighted from labor_logs).",
            "internal_labor_hours": "Total logged hours in the month.",
            "internal_labor_yoy_pct": "internal_labor_rate YoY %.",
            "internal_materials_per_job": "Average materials spend per job (ledger).",
            "internal_materials_yoy_pct": "internal_materials_per_job YoY %.",
            "market_wage_national": "BLS CES construction AHE, US national.",
            "market_wage_national_yoy_pct": "Its YoY %.",
            "market_wage_regional": "BLS CES construction AHE, Missouri.",
            "market_wage_regional_yoy_pct": "Its YoY %.",
            "ppi_lumber": "BLS PPI softwood lumber index.",
            "ppi_lumber_yoy_pct": "Its YoY %.",
            "ppi_paint": "BLS PPI paint & coating manufacturing index.",
            "ppi_paint_yoy_pct": "Its YoY %.",
            "ppi_building_materials": "BLS PPI construction materials index.",
            "ppi_building_materials_yoy_pct": "Its YoY %.",
            "labor_inflation_gap_national_pct": "Our labor YoY minus the national wage YoY (positive = margin squeeze).",
            "labor_inflation_gap_regional_pct": "Same vs the Missouri wage.",
            "materials_inflation_gap_pct": "Our materials YoY minus the construction-materials PPI YoY.",
        }),
}

# JSONB columns serialized to clean JSON strings for CSV/Parquet.
JSON_TEXT_COLS = {
    "anomaly_alerts": ["details"],
    "forecasts": ["details"],
    "backtest_results": ["predictions"],
    "regional_industry_profiles": ["notes"],
}

# ---------------------------------------------------------------------------
# SOCAL SPEC -- the SoCal Finance/Tech benchmark pack v2 (same drift-assertion
# mechanism as FILES). Provenance convention:
#   _REAL_HARVEST  = REAL public data harvested by harvest_socal_fintech.py
#                    (each row's `source` column records the exact file URL
#                    + retrieval date)
#   _MODELED_CONST = MODELED baseline constants (industry norms 2025-2026,
#                    NOT scraped) written by the harvester's --sources profiles
# ---------------------------------------------------------------------------
_REAL_HARVEST = ("REAL harvested public data via harvest_socal_fintech.py "
                 "(keyless endpoints: BLS OEWS special-request zips, FRED "
                 "graph CSV, download.bls.gov time-series files, Census CBP/"
                 "ZBP zips, SBA FOIA CSVs); the per-row `source` column "
                 "records the exact file URL and retrieval date")
_MODELED_CONST = ("MODELED baseline constants written by "
                  "harvest_socal_fintech.py --sources profiles (industry "
                  "norms 2025-2026 -- NOT scraped data); median_salary_usd is "
                  "the only harvested input (OEWS annual median of the "
                  "vertical's anchor occupation)")
_MV_SOCAL = ("Derived analytics: materialized view built by analytics_views.sql "
             "over the SoCal tables above (documented rate formula in its "
             "definition)")

SOCAL_FILES = {
    "socal_wage_benchmarks": dict(
        sql="SELECT * FROM socal_wage_benchmarks "
            "ORDER BY region_code, occupation_code, data_year", parquet=True,
        description="REAL BLS OEWS wages per (region, occupation, data_year): "
                    "8 task-facing roles x 6 SoCal regions (May 2024 metro + "
                    "state files; percentiles as published, suppressed cells NULL).",
        provenance=_REAL_HARVEST,
        columns={
            "id": "Surrogate primary key.",
            "region_code": "Fixed 6-region SoCal vocabulary (la_msa, oxnard_ventura_msa, orange_county, san_diego_msa, inland_empire_msa, california_state).",
            "occupation_code": "BLS OEWS occupation code (e.g. 15-1252 Software Developers).",
            "occupation_title": "OEWS title as published.",
            "role_label": "Task-facing role label (e.g. 'Software Engineer', 'Bookkeeper').",
            "data_year": "OEWS survey year (2024).",
            "hourly_p25": "Hourly 25th percentile wage ($/hr); NULL when suppressed.",
            "hourly_median": "Hourly median wage ($/hr); NULL when suppressed.",
            "hourly_p75": "Hourly 75th percentile wage; NULL when suppressed.",
            "hourly_p90": "Hourly 90th percentile wage; NULL when suppressed.",
            "annual_p25": "Annual 25th percentile ($/yr, OEWS rounds to nearest $100); NULL when suppressed.",
            "annual_median": "Annual median wage ($/yr) -- input to the rate-guidance formula.",
            "annual_p75": "Annual 75th percentile; NULL when suppressed.",
            "annual_p90": "Annual 90th percentile; NULL when suppressed.",
            "employment": "Estimated employment in the occupation; NULL when suppressed (never 0/-1 sentinels).",
            "employment_rse": "Relative standard error of employment (%).",
            "source": "Provenance: file description + retrieval date + exact OEWS zip URL (orange_county rows note the statewide proxy).",
        }),
    "socal_macro_indicators": dict(
        sql="SELECT * FROM socal_macro_indicators "
            "ORDER BY region_code, series_id, period", parquet=False,
        description="REAL monthly macro series per SoCal region (CPI incl. "
                    "US-CPI-U proxy rows, employment, interest rates, "
                    "office-lease PPIs); period = first of month, "
                    "sub-monthly series averaged.",
        provenance=_REAL_HARVEST + "; FRED graph CSV + download.bls.gov CPI "
                                     "time-series file (FRED does not mirror the post-2018 metro CPI ids)",
        columns={
            "id": "Surrogate primary key.",
            "region_code": "Fixed 6-region SoCal vocabulary; US/CA baselines live under california_state.",
            "series_id": "Canonical FRED series id (or BLS series id for the metro CPIs loaded from download.bls.gov).",
            "series_name": "Human-readable description incl. proxy notes (e.g. US CPI-U as CA proxy).",
            "category": "cpi / employment / interest_rate / real_estate / ppi.",
            "period": "First day of the calendar month (sub-monthly series averaged).",
            "value": "Series level for the month (> 0).",
            "unit": "Unit as published (e.g. 'Index 1982-84=100, US CPI-U as CA proxy', 'percent', 'thousands of employees').",
            "source": "Provenance: FRED/BLS + retrieval date + exact URL.",
        }),
    "socal_business_patterns": dict(
        sql="SELECT * FROM socal_business_patterns "
            "ORDER BY geo_level, geo_fips, naics_code, data_year", parquet=True,
        description="REAL Census County/ZIP Business Patterns 2023 for the six "
                    "SoCal counties and their ZIPs, NAICS 2022 families 51/52/54 "
                    "(CBP county rows carry noise-adjusted employment + payroll "
                    "x 1000; modern ZBP detail rows are establishments-only).",
        provenance=_REAL_HARVEST + "; CBP cbp23co.zip + ZBP zbp23detail.zip "
                                     "(2022+ CBP publishes NOISE-ADJUSTED values with noise-LEVEL flags G/H/J, not suppression)",
        columns={
            "id": "Surrogate primary key.",
            "geo_level": "'county' (CBP) or 'zip' (ZBP).",
            "geo_fips": "5-char county FIPS (e.g. 06037) or 5-char ZIP.",
            "geo_name": "County name / city+county for zip rows.",
            "region_code": "Fixed region vocabulary mapped from the county; NULL for zip rows that could not be attributed.",
            "naics_code": "NAICS 2022 code with aggregation fillers stripped (2-6 digit, 51/52/54 families only).",
            "naics_label": "Human-readable NAICS description (static label dict; NULL for codes outside it).",
            "data_year": "CBP/ZBP data year (2023).",
            "establishments": "Number of establishments (>= 0).",
            "employment": "CBP mid-March employment (noise-adjusted, as published); NULL for zip rows (modern ZBP publishes no employment).",
            "employment_flag": "Raw CBP EMP_NF noise-level flag (G/H/J) as published.",
            "annual_payroll_usd": "Annual payroll in DOLLARS (file AP is in $1,000, stored x 1000); NULL for zip rows.",
            "payroll_per_employee": "annual_payroll_usd / employment when both are valid (> 0); NULL otherwise.",
            "source": "Provenance: file description + retrieval date + exact census.gov URL.",
        }),
    "socal_loan_activity": dict(
        sql="SELECT * FROM socal_loan_activity "
            "ORDER BY program, fiscal_year, geo_level, geo_fips", parquet=True,
        description="REAL SBA 7(a)/504 loan activity aggregated per (program, "
                    "fiscal_year, geography) from the loan-level FOIA CSVs: "
                    "6 SoCal counties + all-California state rows, last ~6 "
                    "fiscal years, with the top lender per geography/year.",
        provenance=_REAL_HARVEST + "; data.sba.gov FOIA CSVs streamed and "
                                     "aggregated (county = ProjectCounty, lender = BankName for 7a / CDC_Name for 504)",
        columns={
            "id": "Surrogate primary key.",
            "program": "'7a' or '504'.",
            "fiscal_year": "SBA federal fiscal year of approval.",
            "geo_level": "'county' (six SoCal counties) or 'state' (all-California, geo_fips '06').",
            "geo_fips": "5-char county FIPS or '06'.",
            "geo_name": "County name / 'California'.",
            "region_code": "Fixed region vocabulary (california_state for state rows).",
            "loans_count": "Number of loans approved (>= 0).",
            "gross_approval_usd": "Sum of GrossApproval amounts (>= 0).",
            "avg_loan_usd": "gross_approval_usd / loans_count.",
            "top_lender": "Lender with the most loans in that geography/program/year.",
            "top_lender_loans": "That lender's loan count.",
            "source": "Provenance: SBA FOIA resource URL + retrieval date.",
        }),
    "regional_industry_profiles": dict(
        sql="SELECT * FROM regional_industry_profiles "
            "ORDER BY vertical_key, region_code", parquet=False,
        description="MODELED unit economics per Finance/Tech vertical x SoCal "
                    "region (6 x 6 = 36 rows): margin band, payroll ratio, CAC, "
                    "utilization, locality burden + the harvested anchor-occupation "
                    "median salary. The modeling basis is documented per row in notes.",
        provenance=_MODELED_CONST,
        columns={
            "id": "Surrogate primary key.",
            "vertical_key": "Fixed vertical vocabulary (wealth_advisory, b2b_saas, custom_software_ai, fractional_cfo, fintech_payments, managed_it_cyber).",
            "vertical_name": "Human-readable vertical name.",
            "region_code": "Fixed 6-region SoCal vocabulary.",
            "anchor_occupation_code": "BLS OEWS occupation code the vertical's pricing anchors on.",
            "gross_margin_pct": "MODELED target gross margin % (band midpoint).",
            "gross_margin_p25_pct": "MODELED low end of the margin band.",
            "gross_margin_p75_pct": "MODELED high end of the margin band.",
            "payroll_to_revenue_ratio": "MODELED payroll as a fraction of revenue.",
            "cac_usd": "MODELED customer acquisition cost (USD).",
            "utilization_target_pct": "MODELED target billable utilization (share of paid hours billable).",
            "locality_burden_pct": "MODELED regional payroll-burden delta vs the California baseline (negative = cheaper).",
            "median_salary_usd": "Harvested OEWS annual median of the anchor occupation in this region (region -> CA -> LA fallback chain).",
            "notes": "JSONB modeling basis: every constant + where the median salary came from + the rate formula.",
            "source": "Provenance: modeling note + salary source + retrieval date.",
            "created_at": "First write time (upserts do NOT touch it).",
        }),
    "socal_harvest_log": dict(
        sql="SELECT * FROM socal_harvest_log ORDER BY id", parquet=False,
        description="APPEND-ONLY audit trail of every harvest attempt: one row "
                    "per (run, source, url) with ok/failed/cache_hit status, "
                    "HTTP status, rows loaded and a message (incl. the "
                    "2080-hour wage-consistency warnings).",
        provenance=_REAL_HARVEST.replace("public data", "data") + " (this table is the audit log itself)",
        columns={
            "id": "Surrogate primary key.",
            "run_id": "uuid4 hex per harvester invocation.",
            "source_name": "Harvest family: oews / fred / cbp / zbp / sba / profiles.",
            "url": "Exact URL attempted (or served from cache).",
            "status": "'ok' (fresh fetch) / 'cache_hit' / 'failed'.",
            "http_status": "HTTP status of the last attempt (NULL on network failure).",
            "rows_loaded": "Warehouse rows written from this URL in that run.",
            "message": "Row counts, consistency warnings, degradation notes, error text.",
            "fetched_at": "Attempt timestamp (UTC).",
        }),
    "mv_socal_rate_guidance": dict(
        sql="SELECT * FROM mv_socal_rate_guidance "
            "ORDER BY region_code, vertical_key", parquet=False,
        description="Billable-rate guidance per SoCal region x Finance/Tech "
                    "vertical (36 rows): joins the MODELED profiles with the "
                    "REAL OEWS wages into hourly cost -> loaded cost -> "
                    "suggested billable rate, with statewide/LA percent deltas.",
        provenance=_MV_SOCAL + " (modeled inputs: profiles; real inputs: "
                                 "socal_wage_benchmarks)",
        columns={
            "region_code": "Fixed 6-region SoCal vocabulary.",
            "vertical_key": "Fixed vertical vocabulary.",
            "vertical_name": "Human-readable vertical name.",
            "data_year": "OEWS year of the joined wage row (latest per region+occupation).",
            "anchor_occupation_code": "OEWS occupation the vertical anchors on.",
            "role_label": "Task-facing role label from the wage row.",
            "annual_median_salary": "Harvested OEWS annual median used by the formula.",
            "locality_burden_pct": "MODELED regional payroll burden vs CA baseline.",
            "utilization_target_pct": "MODELED target billable utilization.",
            "gross_margin_pct": "MODELED target gross margin %.",
            "hourly_cost": "annual_median_salary / 2080 x (1 + locality_burden/100).",
            "loaded_cost": "hourly_cost / (utilization_target/100).",
            "suggested_billable_rate": "loaded_cost / (1 - gross_margin/100) -- price so the margin target holds at target utilization.",
            "cac_usd": "MODELED customer acquisition cost (pass-through).",
            "payroll_to_revenue_ratio": "MODELED payroll/revenue (pass-through).",
            "statewide_billable_rate": "Same vertical's suggested rate for california_state.",
            "rate_vs_statewide_pct": "(rate / statewide - 1) * 100; positive = pricier than statewide.",
            "la_billable_rate": "Same vertical's suggested rate for la_msa.",
            "rate_vs_la_pct": "(rate / LA - 1) * 100; positive = pricier than LA.",
        }),
}


V1_INTRO = [
    f"Generated **from the export spec** by `export_dataset_pack.py` on {{now}}. "
    "Row counts and column lists below are the values the exporter actually "
    "wrote (the exporter asserts every DataFrame's columns against the spec "
    "before writing, so this document cannot drift from the data).",
    "",
    "Provenance summary: the business tables (jobs, labor_logs, financial_ledger, "
    "customers, employees, anomaly_alerts, job_market benchmark rows) are a "
    "**deterministic synthetic simulation** (`seed_demo.py`, random seed 42, "
    "36 months). The `wage`/`ppi` rows in `market_benchmarks` are **real public "
    "economic data** (BLS series served via FRED's keyless CSV endpoint) loaded "
    "by `fetch_benchmarks.py`; every row records its origin in `series_id` and "
    "`source`. `forecasts` / `backtest_results` are model output of "
    "`forecast_engine.py`; the `mv_*` files are materialized analytics views "
    "over the tables above. Customer/employee PII (emails, phones, addresses, "
    "contact names) was stripped at export; account names are synthetic.",
    "",
    "All CSVs are UTF-8, comma-delimited, header row 1, no index column. "
    "`*.parquet` files are exact twins of the CSVs of the same stem. JSONB "
    "columns are serialized as JSON strings. CSV text has no type info; "
    "dtypes below are as loaded from PostgreSQL (the Parquet twins preserve them).",
]

V1_REGENERATE = (
    "python3 seed_demo.py && python3 fetch_benchmarks.py && "
    "python3 export_dataset_pack.py  "
    "(seed_demo.py rebuilds the deterministic simulation, "
    "fetch_benchmarks.py refreshes the real BLS/FRED macro rows "
    "-- needs internet --, export_dataset_pack.py rewrites this pack). "
    "macro series without internet: the committed offline snapshot "
    "samples/macro_benchmarks_snapshot.csv can be loaded instead "
    "(docker/init_pipeline.py does exactly that)."
)

V2_INTRO = [
    f"Generated **from the export spec** by `export_dataset_pack.py` on {{now}}. "
    "Row counts and column lists below are the values the exporter actually "
    "wrote (the exporter asserts every DataFrame's columns against the spec "
    "before writing, so this document cannot drift from the data).",
    "",
    "PROVENANCE SUMMARY -- REAL vs MODELED (the two data families in this pack "
    "are deliberately distinct):",
    "",
    "* **REAL harvested data** (`socal_wage_benchmarks`, `socal_macro_indicators`, "
    "`socal_business_patterns`, `socal_loan_activity`, `socal_harvest_log`, and "
    "the wage input behind `mv_socal_rate_guidance` / `regional_industry_profiles."
    "median_salary_usd`): public economic data harvested by "
    "`harvest_socal_fintech.py` from keyless public endpoints (BLS OEWS "
    "special-request zips, FRED graph CSVs, download.bls.gov CPI time-series "
    "files, Census CBP/ZBP zips, SBA FOIA CSVs). **Every row carries a `source` "
    "string of the form '<description>, retrieved <date> from <url>'** so any "
    "value can be audited back to the exact file. Known documented proxies: "
    "Orange County wage rows use the California statewide estimate (BLS no "
    "longer publishes the metro division), and the statewide/OC/Ventura CPI "
    "rows use the US CPI-U (CA has no statewide CPI; Ventura/Orange have no "
    "metro CPI series). Failed/degraded sources are visible in "
    "`socal_harvest_log` -- nothing is fabricated.",
    "",
    "* **MODELED baseline constants** (`regional_industry_profiles` minus its "
    "`median_salary_usd`, and the margin/utilization/CAC/burden inputs of "
    "`mv_socal_rate_guidance`): industry-norm modeling assumptions for the six "
    "Finance/Tech verticals (2025-2026 norms), **NOT scraped from any public "
    "file**. The basis for every constant is documented per row in the `notes` "
    "JSONB column.",
    "",
    "All CSVs are UTF-8, comma-delimited, header row 1, no index column. "
    "`*.parquet` files are exact twins of the CSVs of the same stem. JSONB "
    "columns are serialized as JSON strings. CSV text has no type info; "
    "dtypes below are as loaded from PostgreSQL (the Parquet twins preserve them).",
]

V2_REGENERATE = (
    "python3 harvest_socal_fintech.py && python3 export_dataset_pack.py "
    "--pack socal_v2  (the harvest needs internet; --refresh bypasses its "
    "disk cache; the export itself runs offline against the warehouse and "
    "rewrites this pack)"
)


def friendly_dtype(series):
    """Truthful pandas dtype label for the dictionary."""
    base = str(series.dtype)
    if base != "object":
        return base
    nn = series.dropna()
    if nn.empty:
        return "object (empty)"
    if all(isinstance(v, Decimal) for v in nn.head(200)):
        return "decimal(object)"
    if nn.head(200).map(lambda v: isinstance(v, (dict, list))).all():
        return "json(object)"
    return "text(object)"


def fetch_frame(cur, spec):
    """Run the spec's SELECT and return a DataFrame with the exact result
    columns (the spec's drift assertion then pins them)."""
    cur.execute(spec["sql"])
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    return df


def export_pack(cur, files, pack_dir, pack_name, dictionary_title,
                dictionary_intro, regenerate_note):
    """Export one dataset pack (CSVs [+ parquet twins], DATA_DICTIONARY.md,
    MANIFEST.json) with the spec-vs-data drift assertions. Returns nothing;
    prints the per-file summary."""
    print(f"Exporting dataset pack -> {pack_dir}")
    pack_dir.mkdir(parents=True, exist_ok=True)

    manifest_files = []
    dictionary_blocks = []
    warehouse_counts = {}

    for key, spec in files.items():
        df = fetch_frame(cur, spec)

        # ---- DRIFT ASSERTION: data must match the spec exactly ----------
        expected = list(spec["columns"].keys())
        if list(df.columns) != expected:
            raise SystemExit(
                f"DRIFT: {key}: DataFrame columns {list(df.columns)} != spec "
                f"{expected}. Update the COLUMNS spec in export_dataset_pack.py.")
        missing = [c for c in df.columns if c not in spec["columns"]]
        if missing:
            raise SystemExit(f"DRIFT: {key}: columns without descriptions: {missing}")

        # ---- second opinion: row count straight from the warehouse ------
        cur.execute(f"SELECT COUNT(*) FROM ({spec['sql']}) q")
        wh_count = cur.fetchone()[0]
        warehouse_counts[key] = wh_count
        if wh_count != len(df):
            raise SystemExit(f"DRIFT: {key}: fetched {len(df)} rows but warehouse says {wh_count}")

        # ---- JSONB -> clean JSON strings --------------------------------
        for col in JSON_TEXT_COLS.get(key, []):
            df[col] = df[col].map(
                lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v)

        stem = key
        artifacts = []

        csv_path = pack_dir / f"{stem}.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8")
        artifacts.append(csv_path)

        if spec["parquet"]:
            pq_path = pack_dir / f"{stem}.parquet"
            df.to_parquet(pq_path, index=False)
            artifacts.append(pq_path)

        for path in artifacts:
            manifest_files.append({
                "file": path.name,
                "rows": len(df),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "bytes": path.stat().st_size,
            })

        dictionary_blocks.append((key, spec, df, len(df)))
        print(f"  [ok] {key:28} rows={len(df):>6}  "
              f"{'csv+parquet' if spec['parquet'] else 'csv'}")

    # ---------------- DATA_DICTIONARY.md (generated from the spec) -------
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    intro = [line.replace("{now}", now) for line in dictionary_intro]
    lines = [dictionary_title, "", *intro, ""]
    for key, spec, df, nrows in dictionary_blocks:
        lines += [
            f"## `{key}.csv`" + (" / `.parquet`" if spec["parquet"] else ""),
            "",
            f"**Rows:** {nrows}  ",
            f"**Provenance:** {spec['provenance']}  ",
            "",
            spec["description"],
            "",
            "| Column | Dtype | Description |",
            "|---|---|---|",
        ]
        for col, desc in spec["columns"].items():
            lines.append(f"| `{col}` | `{friendly_dtype(df[col])}` | {desc} |")
        lines.append("")

    dict_path = pack_dir / "DATA_DICTIONARY.md"
    dict_path.write_text("\n".join(lines), encoding="utf-8")
    manifest_files.append({
        "file": dict_path.name, "rows": None, "sha256":
            hashlib.sha256(dict_path.read_bytes()).hexdigest(),
        "bytes": dict_path.stat().st_size,
    })
    print(f"  [ok] DATA_DICTIONARY.md written ({len(dictionary_blocks)} file sections)")

    # ---------------- MANIFEST.json ---------------------------------------
    manifest = {
        "pack": pack_name,
        "exported_at": now,
        "generator": {
            "python": sys.version.split()[0],
            "pandas": pd.__version__,
            "pyarrow": pyarrow.__version__,
        },
        "regenerate": regenerate_note,
        "files": manifest_files,
    }
    manifest_path = pack_dir / "MANIFEST.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"  [ok] MANIFEST.json written ({len(manifest_files)} artifacts)")

    # ---------------- final manifest table --------------------------------
    print(f"\n=== Dataset pack manifest ({pack_dir}) ===")
    hdr = f"{'file':34} {'rows':>7}  {'bytes':>10}  sha256 (first 12)"
    print(hdr)
    print("-" * len(hdr))
    for f in manifest_files:
        rows = "" if f["rows"] is None else f"{f['rows']:,}"
        print(f"{f['file']:34} {rows:>7}  {f['bytes']:>10,}  {f['sha256'][:12]}")


def main():
    ap = argparse.ArgumentParser(
        description="Export the documented dataset pack(s): v1 operational "
                    "warehouse and/or v2 SoCal Finance/Tech benchmarks.")
    ap.add_argument("--pack", choices=["all", "v1", "socal_v2"], default="all",
                    help="which pack to write (default: all)")
    args = ap.parse_args()

    conn = psycopg2.connect(DB)
    try:
        cur = conn.cursor()
        if args.pack in ("all", "v1"):
            export_pack(cur, FILES, PACK_DIR, "smb_benchmark_pack_v1",
                        "# SMB Benchmark Pack v1 -- Data Dictionary",
                        V1_INTRO, V1_REGENERATE)
        if args.pack in ("all", "socal_v2"):
            export_pack(cur, SOCAL_FILES, SOCAL_PACK_DIR,
                        "socal_fintech_benchmark_pack_v2",
                        "# SoCal Finance/Tech Benchmark Pack v2 -- Data Dictionary",
                        V2_INTRO, V2_REGENERATE)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
