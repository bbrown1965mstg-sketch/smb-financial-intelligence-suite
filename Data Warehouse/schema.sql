-- =============================================================================
--  SMB FINANCIAL & OPERATIONAL DATA WAREHOUSE
--  File   : schema.sql  (core schema)
--  Target : PostgreSQL 17+  (pure core; NO extensions required)
--  Apply  : FIRST, before analytics_views.sql
-- =============================================================================
--
--  PURPOSE
--    Core relational schema for a MULTI-CLIENT (Fractional CFO) warehouse of
--    small service businesses (SMBs): the `companies` client registry (2.0)
--    scopes the 8 client-data tables (customers, job_types, employees, jobs,
--    financial_ledger, labor_logs, anomaly_alerts, forecasts) by company_id,
--    plus service catalog, crew, work orders, a complete financial ledger,
--    labor time logs, market benchmarks (service-line comparables AND macro
--    wage/PPI series loaded by fetch_benchmarks.py -- GLOBAL, shared across
--    client workspaces), the output table for the anomaly detector, and the
--    two FORECAST output tables (forecasts, backtest_results) written by
--    forecast_engine.py. harvest_socal_fintech.py
--    writes six additional SoCal regional benchmark tables (sections 2.11-2.16:
--    regional_industry_profiles, socal_wage_benchmarks, socal_macro_indicators,
--    socal_business_patterns, socal_loan_activity, socal_harvest_log) over a
--    FIXED 6-region vocabulary; analytics_views.sql builds five operational
--    materialized views plus the SoCal mv_socal_rate_guidance view and the
--    detect_anomalies() / refresh_all_analytics() functions on top of these.
--
--  USAGE
--    psql "postgresql://postgres:postgres@localhost:5432/smb_warehouse" \
--         -v ON_ERROR_STOP=1 -f schema.sql
--    psql "postgresql://postgres:postgres@localhost:5432/smb_warehouse" \
--         -v ON_ERROR_STOP=1 -f analytics_views.sql      -- apply SECOND
--    psql "postgresql://postgres:postgres@localhost:5432/smb_warehouse" \
--         -v ON_ERROR_STOP=1 -f migrations/001_add_companies.sql
--         -- (idempotent; docker/init_pipeline.py applies it on every boot so
--         --  pre-existing container volumes migrate cleanly. On a FRESH boot
--         --  it is a no-op: this file already creates the new shape.)
--
--    Nightly maintenance after data loads (functions live in analytics_views.sql):
--      SELECT refresh_all_analytics();    -- REFRESH ... CONCURRENTLY all matviews
--      SELECT * FROM detect_anomalies();  -- fills anomaly_alerts, returns new rows
--
--  ERD (text diagram; "1 ──< N" marks one-to-many)
--
--    companies 1 ────< customers, job_types, employees, jobs,
--                      financial_ledger, labor_logs, anomaly_alerts, forecasts
--                      (the 8 CLIENT-DATA tables; every row belongs to exactly
--                       one client workspace)
--
--    customers 1 ────< jobs >──── 1 job_types
--        │ 1           │   │
--        │   1 ┌───────┘   └───────────┐ 1  (ledger.job_id nullable)
--        │       │                       │
--        │  labor_logs                 financial_ledger
--        │           │ 1               (job_id NULL = overhead entry)
--        └───────────┼───────────────┐ 1
--                    │               │    (ledger.customer_id nullable:
--               employees 1          │     customer-only cost, e.g. CAC)
--                                    │
--             (financial_ledger.customer_id is exclusive with job_id:
--              an entry attributes EITHER to a job OR to a customer)
--
--    job_types 1 ────< market_benchmarks        (market_benchmarks is GLOBAL:
--                                                market data, shared across
--                                                client workspaces)
--    jobs      1 ────< anomaly_alerts   (ON DELETE SET NULL)
--
--    forecast_engine.py ──< forecasts (company-scoped), backtest_results
--                          (global model audit trail: no company_id)
--
--    harvest_socal_fintech.py ──< regional_industry_profiles   (MODELED vertical
--                                 socal_wage_benchmarks         economics + REAL
--                                 socal_macro_indicators        OEWS/FRED/CBP/
--                                 socal_business_patterns       ZBP/SBA harvests;
--                                 socal_loan_activity           no FKs; every row
--                                 socal_harvest_log             keyed by region_code
--                                                            from the fixed 6-region
--                                                            vocabulary; see 2.11-2.16)
--
--  IDEMPOTENCY
--    Safe to re-run: existing objects are dropped leaf-first (CASCADE), then
--    recreated inside a single transaction. Because the materialized views
--    depend on these tables, re-running this file invalidates the analytics
--    layer -- ALWAYS re-apply analytics_views.sql afterwards.
--
--  CONVENTIONS
--    * Money       = NUMERIC(14,2); percentages = NUMERIC(5,2)/(8,2);
--      hours       = NUMERIC(6,2). Never floating point for money.
--    * Ledger sign convention: financial_ledger.amount is ALWAYS positive;
--      the direction is carried by entry_type ('revenue' increases income,
--      'expense' reduces it). No negative amounts are ever stored.
--    * job_types is a runtime-extensible lookup TABLE, deliberately NOT a
--      PostgreSQL ENUM: new service types can be inserted without DDL/locks.
--    * labor_logs.hourly_rate stores the employee's rate AT THE TIME OF WORK
--      (a snapshot), so historical labor cost stays stable when current rates
--      change. Labor cost is derived (hours * hourly_rate) in views, never stored.
--    * ON DELETE policy: RESTRICT wherever referenced history must survive
--      (customers/job_types -> jobs, employees -> labor_logs, jobs -> labor_logs);
--      SET NULL only where explicitly required (financial_ledger.job_id and
--      anomaly_alerts.job_id -- the ledger/alert rows survive, attribution drops).
--    * No triggers by design: created_at/updated_at are maintained by the
--      application layer.
--    * All timestamps are TIMESTAMPTZ.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1) DROP, leaf-first (CASCADE) -- makes the script safe to re-run.
--    The analytics objects are defined in analytics_views.sql; they are dropped
--    here defensively so the file pair can be re-applied cleanly in order.
-- -----------------------------------------------------------------------------
DROP FUNCTION           IF EXISTS detect_anomalies();                       -- recreated in analytics_views.sql
DROP FUNCTION           IF EXISTS refresh_all_analytics();                  -- recreated in analytics_views.sql
DROP MATERIALIZED VIEW  IF EXISTS mv_monthly_summary          CASCADE;      -- defined in analytics_views.sql
DROP MATERIALIZED VIEW  IF EXISTS mv_customer_profitability   CASCADE;      -- defined in analytics_views.sql
DROP MATERIALIZED VIEW  IF EXISTS mv_labor_cost_analysis      CASCADE;      -- defined in analytics_views.sql
DROP MATERIALIZED VIEW  IF EXISTS mv_job_type_profitability   CASCADE;      -- defined in analytics_views.sql

-- Forecast engine output tables (forecast_engine.py). They carry no FKs in
-- either direction, so they drop cleanly here -- leaf-first, like everything
-- else in this section.
DROP TABLE IF EXISTS forecasts        CASCADE;   -- seasonal_trend_v1 output (forecast_engine.py)
DROP TABLE IF EXISTS backtest_results CASCADE;   -- walk-forward validation output (forecast_engine.py)

-- SoCal regional benchmark tables (harvest_socal_fintech.py output; like the
-- forecast tables they carry no FKs in either direction, so they drop cleanly
-- leaf-first).
DROP TABLE IF EXISTS regional_industry_profiles CASCADE;
DROP TABLE IF EXISTS socal_wage_benchmarks      CASCADE;
DROP TABLE IF EXISTS socal_macro_indicators     CASCADE;
DROP TABLE IF EXISTS socal_business_patterns    CASCADE;
DROP TABLE IF EXISTS socal_loan_activity        CASCADE;
DROP TABLE IF EXISTS socal_harvest_log          CASCADE;

DROP TABLE IF EXISTS anomaly_alerts    CASCADE;   -- depends on jobs, companies
DROP TABLE IF EXISTS market_benchmarks CASCADE;   -- depends on job_types
DROP TABLE IF EXISTS labor_logs        CASCADE;   -- depends on jobs, employees
DROP TABLE IF EXISTS financial_ledger  CASCADE;   -- depends on jobs, customers, companies
DROP TABLE IF EXISTS jobs              CASCADE;   -- depends on customers, job_types, companies
DROP TABLE IF EXISTS employees         CASCADE;   -- depends on companies
DROP TABLE IF EXISTS job_types         CASCADE;   -- depends on companies
DROP TABLE IF EXISTS customers         CASCADE;   -- depends on companies
DROP TABLE IF EXISTS companies         CASCADE;   -- parent of the 8 client tables (drop last, leaf-first)

-- -----------------------------------------------------------------------------
-- 2) TABLES
-- -----------------------------------------------------------------------------

-- 2.0 companies ----------------------------------------------------------------
-- Client registry for the Fractional CFO multi-tenant deployment. Every
-- CLIENT-DATA table (customers, job_types, employees, jobs, financial_ledger,
-- labor_logs, anomaly_alerts, forecasts) carries a company_id FK to this
-- table, so each client workspace sees exactly its own books. MARKET/GLOBAL
-- data (market_benchmarks, backtest_results, the SoCal regional tables and
-- the SoCal Capital / Peer modules) carries NO company_id: it is shared
-- benchmark context across all workspaces.
-- seed_demo.py truncates this table (RESTART IDENTITY) and re-inserts the
-- demo workspace 'BrightPath Works (Demo)' as company #1; existing
-- deployments pick it up idempotently via migrations/001_add_companies.sql
-- (applied by docker/init_pipeline.py on every boot).
CREATE TABLE companies (
    company_id        BIGINT      GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    company_name      TEXT        NOT NULL UNIQUE,
    industry_vertical TEXT,
    socal_region      TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE  companies                    IS 'Client registry (Fractional CFO multi-tenancy). Parent of the 8 client-data tables via company_id FKs; market/global data is shared and carries no company_id. seed_demo.py re-creates the demo workspace on reload.';
COMMENT ON COLUMN companies.company_id         IS 'Surrogate identity key referenced by customers/job_types/employees/jobs/financial_ledger/labor_logs/anomaly_alerts/forecasts.';
COMMENT ON COLUMN companies.company_name       IS 'Client workspace display name (unique).';
COMMENT ON COLUMN companies.industry_vertical  IS 'Free-text vertical label (e.g. home_services_remodeling); informational.';
COMMENT ON COLUMN companies.socal_region       IS 'Optional fixed SoCal region_code from the 6-region vocabulary; NULL when not applicable.';
COMMENT ON COLUMN companies.created_at         IS 'Row creation time (application-maintained; no triggers by design).';

-- The demo client workspace, seeded here so the company_id DEFAULT 1 below has
-- a valid parent on a FRESH boot (identity starts at 1, so this row IS
-- company #1). seed_demo.py truncates companies (RESTART IDENTITY) and
-- re-inserts it on every reload; existing deployments get the same row
-- idempotently from migrations/001_add_companies.sql.
INSERT INTO companies (company_name, industry_vertical, socal_region)
VALUES ('BrightPath Works (Demo)', 'home_services_remodeling', NULL);

-- 2.1 customers ---------------------------------------------------------------
-- SMB service-business clients (of ONE client workspace: company_id).
CREATE TABLE customers (
    id              BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    company_id      BIGINT      NOT NULL DEFAULT 1 CONSTRAINT fk_customers_company_id REFERENCES companies (company_id) ON DELETE RESTRICT,
    name            TEXT        NOT NULL,
    contact_name    TEXT,
    email           TEXT        NOT NULL UNIQUE,
    phone           TEXT,
    address         TEXT,
    city            TEXT,
    state           TEXT,
    customer_since  DATE,
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE  customers             IS 'Clients of the SMB service business, per client workspace (company_id). Deleting a customer with jobs is blocked (RESTRICT) to protect history.';
COMMENT ON COLUMN customers.company_id   IS 'Client workspace this customer belongs to (multi-tenant scope).';
COMMENT ON COLUMN customers.email        IS 'Unique contact email (natural lookup key; globally unique across workspaces by design).';
COMMENT ON COLUMN customers.customer_since IS 'Date the client relationship began; feeds new_customers in mv_monthly_summary (per company).';

-- 2.2 job_types ---------------------------------------------------------------
-- Catalog of service offerings. NOTE: intentionally a lookup TABLE, not a
-- PostgreSQL ENUM, so new service types can be added at runtime with a plain
-- INSERT -- no DDL, no ALTER TYPE, no table locks.
CREATE TABLE job_types (
    id                     BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    company_id             BIGINT       NOT NULL DEFAULT 1 CONSTRAINT fk_job_types_company_id REFERENCES companies (company_id) ON DELETE RESTRICT,
    name                   TEXT         NOT NULL UNIQUE,
    description            TEXT,
    default_hourly_rate    NUMERIC(14,2) NOT NULL,
    typical_duration_hours NUMERIC(6,2),
    is_active              BOOLEAN      NOT NULL DEFAULT TRUE,
    CONSTRAINT chk_job_types_duration_positive
        CHECK (typical_duration_hours IS NULL OR typical_duration_hours > 0)
);

COMMENT ON TABLE  job_types                  IS 'Service catalog (extensible at runtime), per client workspace. Replaces a Postgres ENUM on purpose: add rows, never ALTER TYPE.';
COMMENT ON COLUMN job_types.company_id       IS 'Client workspace owning this service-line catalog entry.';
COMMENT ON COLUMN job_types.default_hourly_rate IS 'Default crew rate for quoting jobs of this type (informational default, overridden per job/employee).';

-- 2.3 employees ---------------------------------------------------------------
CREATE TABLE employees (
    id           BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    company_id   BIGINT      NOT NULL DEFAULT 1 CONSTRAINT fk_employees_company_id REFERENCES companies (company_id) ON DELETE RESTRICT,
    first_name   TEXT        NOT NULL,
    last_name    TEXT        NOT NULL,
    email        TEXT        NOT NULL UNIQUE,
    phone        TEXT,
    hire_date    DATE        NOT NULL,
    hourly_rate  NUMERIC(14,2) NOT NULL,
    role         TEXT,                       -- role/title
    is_active    BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_employees_hourly_rate_nonnegative CHECK (hourly_rate >= 0)
);

COMMENT ON TABLE  employees            IS 'Crew members, per client workspace. hourly_rate is the CURRENT rate; historical cost uses labor_logs.hourly_rate snapshots, so raising this rate never rewrites history.';
COMMENT ON COLUMN employees.company_id  IS 'Client workspace employing this crew member.';
COMMENT ON COLUMN employees.hourly_rate IS 'Current labor rate. NOT used for historical cost: labor_logs.hourly_rate snapshots the rate at the time of work.';

-- 2.4 jobs --------------------------------------------------------------------
-- Individual work orders. estimated_labor_cost is the budget baseline used by
-- detect_anomalies() for labor-overrun detection.
CREATE TABLE jobs (
    id                   BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    company_id           BIGINT      NOT NULL DEFAULT 1 CONSTRAINT fk_jobs_company_id REFERENCES companies (company_id) ON DELETE RESTRICT,
    job_number           TEXT        NOT NULL UNIQUE,
    customer_id          BIGINT      NOT NULL REFERENCES customers (id) ON DELETE RESTRICT,
    job_type_id          BIGINT      NOT NULL REFERENCES job_types (id) ON DELETE RESTRICT,
    status               TEXT        NOT NULL DEFAULT 'quoted'
                         CHECK (status IN ('quoted','scheduled','in_progress','completed','cancelled')),
    job_date             DATE        NOT NULL,
    completion_date      DATE,
    quoted_price         NUMERIC(14,2) NOT NULL,
    estimated_labor_cost NUMERIC(14,2),
    estimated_hours      NUMERIC(6,2),
    actual_revenue       NUMERIC(14,2),
    notes                TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_jobs_quoted_price_nonnegative
        CHECK (quoted_price >= 0),
    CONSTRAINT chk_jobs_completion_after_start
        CHECK (completion_date IS NULL OR completion_date >= job_date),
    CONSTRAINT chk_jobs_estimated_labor_cost_nonnegative
        CHECK (estimated_labor_cost IS NULL OR estimated_labor_cost >= 0),
    CONSTRAINT chk_jobs_estimated_hours_positive
        CHECK (estimated_hours IS NULL OR estimated_hours > 0)
);

COMMENT ON TABLE  jobs                      IS 'Work orders, per client workspace. Deleting a job that still has labor logs is blocked (RESTRICT); ledger rows keep the job_id (SET NULL) so financial history never vanishes.';
COMMENT ON COLUMN jobs.company_id           IS 'Client workspace this work order belongs to (multi-tenant scope).';
COMMENT ON COLUMN jobs.job_number           IS 'Human-readable work order id (e.g. JOB-2024-0001); unique business key.';
COMMENT ON COLUMN jobs.quoted_price         IS 'Price quoted to the customer (>= 0). Reference price; the financial_ledger is the source of truth for recognized revenue.';
COMMENT ON COLUMN jobs.estimated_labor_cost IS 'Labor budget baseline (hours * expected rate) used by detect_anomalies() for overrun detection. NULL/0 = no budget, so the job is skipped by the detector.';
COMMENT ON COLUMN jobs.estimated_hours      IS 'Budgeted labor hours (informational baseline).';
COMMENT ON COLUMN jobs.actual_revenue       IS 'Final billed amount (informational mirror). Analytics treat the financial_ledger revenue entries as the source of truth.';

-- 2.5 financial_ledger --------------------------------------------------------
-- Every revenue/expense entry. amount is always positive; entry_type carries
-- the direction (see header conventions). Attribution is exclusive:
--   job_id set       -> the entry belongs to that job (and its customer);
--   customer_id set  -> customer-level cost/revenue with no job (customer
--                       acquisition cost, retention credits, ...);
--   both NULL        -> general overhead (rent, utilities, insurance, ...).
CREATE TABLE financial_ledger (
    id          BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    company_id  BIGINT      NOT NULL DEFAULT 1 CONSTRAINT fk_financial_ledger_company_id REFERENCES companies (company_id) ON DELETE RESTRICT,
    job_id      BIGINT      REFERENCES jobs (id) ON DELETE SET NULL,
    customer_id BIGINT      REFERENCES customers (id) ON DELETE SET NULL,
    entry_type  TEXT        NOT NULL CHECK (entry_type IN ('revenue','expense')),
    category    TEXT        NOT NULL,
    entry_date  DATE        NOT NULL,
    amount      NUMERIC(14,2) NOT NULL,
    description TEXT,
    vendor      TEXT,                       -- payee (expense) / payer (revenue)
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_ledger_amount_positive CHECK (amount > 0),
    CONSTRAINT chk_ledger_single_attribution
        CHECK (job_id IS NULL OR customer_id IS NULL)
);

COMMENT ON TABLE  financial_ledger       IS 'Single source of truth for revenue and expenses, per client workspace. Sign convention: amount > 0 always; entry_type carries direction. Attribution is exclusive: job_id XOR customer_id (both NULL = overhead). ON DELETE SET NULL keeps rows alive when a job/customer disappears.';
COMMENT ON COLUMN financial_ledger.company_id IS 'Client workspace this entry belongs to (multi-tenant scope); the mv_monthly_summary cash P&L is per workspace.';
COMMENT ON COLUMN financial_ledger.job_id     IS 'Job the entry belongs to. NULL when the entry is overhead or customer-attributed. Attributed entries roll up to jobs/customers/job types in analytics.';
COMMENT ON COLUMN financial_ledger.customer_id IS 'Customer-level entry with no job behind it (customer acquisition cost, win-back campaigns, credits). Exclusive with job_id by CHECK. mv_customer_profitability adds these to the customer''s cost base (whale detection).';
COMMENT ON COLUMN financial_ledger.entry_type IS 'Direction: ''revenue'' (income) or ''expense'' (cost). amount is always positive.';
COMMENT ON COLUMN financial_ledger.amount     IS 'Positive magnitude in the business currency. Direction lives in entry_type, never in the sign.';
COMMENT ON COLUMN financial_ledger.category   IS 'Free-form label, typically: materials, labor, equipment, permits, subcontractor, utilities, insurance, marketing, payroll, rent, other. mv_monthly_summary gives materials/labor/equipment dedicated buckets and folds everything else into an ''other'' bucket.';
COMMENT ON COLUMN financial_ledger.vendor     IS 'Payee for expenses / payer for revenue (nullable).';

-- 2.6 labor_logs --------------------------------------------------------------
-- Hours worked by employees on jobs. Labor cost of a row = hours * hourly_rate
-- and is computed in views (never stored) from the rate SNAPSHOT below.
CREATE TABLE labor_logs (
    id           BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    company_id   BIGINT      NOT NULL DEFAULT 1 CONSTRAINT fk_labor_logs_company_id REFERENCES companies (company_id) ON DELETE RESTRICT,
    job_id       BIGINT      NOT NULL REFERENCES jobs (id) ON DELETE RESTRICT,
    employee_id  BIGINT      NOT NULL REFERENCES employees (id) ON DELETE RESTRICT,
    work_date    DATE        NOT NULL,
    hours        NUMERIC(6,2)  NOT NULL,
    hourly_rate  NUMERIC(14,2) NOT NULL,
    description  TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_labor_logs_hours_range CHECK (hours > 0 AND hours <= 24),
    CONSTRAINT chk_labor_logs_rate_nonnegative CHECK (hourly_rate >= 0)
);

COMMENT ON TABLE  labor_logs             IS 'Time entries, per client workspace. Authoritative source for labor COST in analytics (hours * hourly_rate). Ledger rows with category=''labor'' (e.g. payroll recharges) are excluded from job/customer cost roll-ups to avoid double counting; mv_monthly_summary includes them as the cash view.';
COMMENT ON COLUMN labor_logs.company_id  IS 'Client workspace this time entry belongs to (multi-tenant scope).';
COMMENT ON COLUMN labor_logs.hourly_rate IS 'Snapshot of employees.hourly_rate at the time of work, so historical labor cost stays stable even if the employee''s current rate changes later. Cost = hours * hourly_rate is computed in views, not stored.';

-- 2.7 market_benchmarks -------------------------------------------------------
-- Two kinds of rows (benchmark_kind):
--   job_market (default): industry comparables per JOB TYPE per month --
--     job_type_id set, avg_price / avg_labor_cost_per_job / avg_margin_pct
--     hold the comparable. Analytics use the latest 'national' row per type.
--   wage / ppi: MACRO economic baselines loaded by fetch_benchmarks.py --
--     job_type_id NULL, series_id holds the BLS/FRED series id, avg_price
--     holds the level (wage $/hr or index value), avg_labor_cost_per_job and
--     avg_margin_pct are 0 (unused for macro rows), source describes origin.
--     mv_benchmark_comparison pivots these against internal cost inflation.
CREATE TABLE market_benchmarks (
    id                     BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    job_type_id            BIGINT       REFERENCES job_types (id) ON DELETE RESTRICT,
    period                 DATE         NOT NULL,
    region                 TEXT         NOT NULL DEFAULT 'national',
    benchmark_kind         TEXT         NOT NULL DEFAULT 'job_market'
                           CHECK (benchmark_kind IN ('job_market','wage','ppi')),
    series_id              TEXT,
    avg_price              NUMERIC(14,2) NOT NULL,
    avg_labor_cost_per_job NUMERIC(14,2) NOT NULL,
    avg_margin_pct         NUMERIC(5,2)  NOT NULL,
    source                 TEXT,
    CONSTRAINT uq_market_benchmarks_type_period_region
        UNIQUE (job_type_id, period, region),
    CONSTRAINT chk_market_benchmarks_period_is_month_start
        CHECK (EXTRACT(DAY FROM period) = 1),
    CONSTRAINT chk_market_benchmarks_amounts_nonnegative
        CHECK (avg_price >= 0 AND avg_labor_cost_per_job >= 0)
);

COMMENT ON TABLE  market_benchmarks           IS 'External baselines (GLOBAL -- market data shared across all client workspaces, no company_id). benchmark_kind=''job_market'' rows (job_type_id set) are per-type comparables; ''wage''/''ppi'' rows (job_type_id NULL, series_id set) are macro series from fetch_benchmarks.py (BLS wage data and FRED producer price indices).';
COMMENT ON COLUMN market_benchmarks.job_type_id IS 'Job type for job_market comparables. NULL for macro wage/PPI rows (they describe the whole industry, not a service line).';
COMMENT ON COLUMN market_benchmarks.period    IS 'First day of the month the benchmark applies to (enforced by CHECK).';
COMMENT ON COLUMN market_benchmarks.avg_price IS 'job_market rows: benchmark average job price for the type/period/region. wage rows: hourly wage ($/hr). ppi rows: index level (1982-ish base varies by series).';
COMMENT ON COLUMN market_benchmarks.series_id IS 'BLS/FRED series id for macro rows (e.g. CES2000000008, WPU0822, WPUSI012011). NULL for job_market rows. Deduped with (region, period) by the partial unique index below.';

-- Partial unique index: macro rows dedupe on (series_id, region, period) so
-- fetch_benchmarks.py can upsert idempotently (ON CONFLICT ... DO UPDATE).
-- job_market rows keep the (job_type_id, period, region) constraint above.
CREATE UNIQUE INDEX uq_market_benchmarks_series_period
    ON market_benchmarks (series_id, region, period)
    WHERE series_id IS NOT NULL;

-- 2.8 anomaly_alerts ----------------------------------------------------------
-- Output table of detect_anomalies() (defined in analytics_views.sql).
-- Two alert kinds:
--   labor_overrun  : actual labor cost (from labor_logs) exceeded the job's
--                    estimated_labor_cost by > 20%. job_id points at the job;
--                    category/period stay NULL.
--   expense_spike  : a ledger expense category's month total exceeded the
--                    prior-6-month rolling mean by more than 2 sample stddevs
--                    (z > 2; needs >= 3 prior months). category + period
--                    (month start) name the subject; job_id stays NULL.
CREATE TABLE anomaly_alerts (
    id             BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    company_id     BIGINT      NOT NULL DEFAULT 1 CONSTRAINT fk_anomaly_alerts_company_id REFERENCES companies (company_id) ON DELETE RESTRICT,
    anomaly_type   TEXT        NOT NULL CHECK (anomaly_type IN ('labor_overrun','expense_spike')),
    job_id         BIGINT      REFERENCES jobs (id) ON DELETE SET NULL,
    category       TEXT,
    period         DATE,
    severity       TEXT        NOT NULL CHECK (severity IN ('low','medium','high')),
    metric_value   NUMERIC(14,2),
    baseline_value NUMERIC(14,2),
    deviation_pct  NUMERIC(8,2),
    z_score        NUMERIC(6,2),
    details        JSONB,
    detected_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_resolved    BOOLEAN     NOT NULL DEFAULT FALSE,
    resolved_at    TIMESTAMPTZ,
    CONSTRAINT chk_anomaly_alerts_period_is_month_start
        CHECK (period IS NULL OR EXTRACT(DAY FROM period) = 1),
    CONSTRAINT chk_anomaly_alerts_resolution
        CHECK (is_resolved = FALSE OR resolved_at IS NOT NULL)
);

COMMENT ON TABLE  anomaly_alerts             IS 'Anomaly detector output, per client workspace (detect_anomalies() scans each workspace separately so expense statistics never mix businesses). Open alerts are deduplicated by partial unique indexes (see below); resolving an alert frees the key so the detector may re-flag the same subject later.';
COMMENT ON COLUMN anomaly_alerts.company_id   IS 'Client workspace the alert was raised for.';
COMMENT ON COLUMN anomaly_alerts.anomaly_type   IS '''labor_overrun'' (points at a job via job_id) or ''expense_spike'' (names a ledger category + month via category/period).';
COMMENT ON COLUMN anomaly_alerts.job_id         IS 'Job subject for labor_overrun alerts; NULL for expense_spike. ON DELETE SET NULL so alerts survive job deletion.';
COMMENT ON COLUMN anomaly_alerts.category       IS 'Ledger expense category for expense_spike alerts; NULL for labor_overrun.';
COMMENT ON COLUMN anomaly_alerts.period         IS 'Month start (first of month) of an expense_spike; NULL for labor_overrun.';
COMMENT ON COLUMN anomaly_alerts.severity       IS 'labor_overrun: >50% over estimate = high, >30% = medium, else low. expense_spike: z > 3 = high, z > 2.5 = medium, else low.';
COMMENT ON COLUMN anomaly_alerts.metric_value   IS 'Observed value: actual labor cost (overrun) or the month''s category total (spike).';
COMMENT ON COLUMN anomaly_alerts.baseline_value IS 'Expected value: estimated_labor_cost (overrun) or the prior-6-month mean (spike).';
COMMENT ON COLUMN anomaly_alerts.deviation_pct  IS 'Percent deviation of metric_value from baseline_value (e.g. 35.00 = 35% over). NULL when the baseline is 0.';
COMMENT ON COLUMN anomaly_alerts.z_score        IS '(metric - baseline mean) / stddev for expense spikes; NULL for labor overruns.';
COMMENT ON COLUMN anomaly_alerts.details        IS 'JSONB context for humans: job_number/actual/estimate/pct or category/period/mean/stddev/months counted.';
COMMENT ON COLUMN anomaly_alerts.is_resolved    IS 'Triage flag. Only OPEN alerts are deduplicated; a resolved alert can be re-raised by the detector.';

-- Partial unique indexes: prevent duplicate OPEN alerts without blocking the
-- detector from re-flagging a subject whose previous alert was resolved.
CREATE UNIQUE INDEX uq_anomaly_open_labor_overrun
    ON anomaly_alerts (anomaly_type, company_id, job_id)
    WHERE anomaly_type = 'labor_overrun' AND NOT is_resolved;

CREATE UNIQUE INDEX uq_anomaly_open_expense_spike
    ON anomaly_alerts (anomaly_type, company_id, category, period)
    WHERE anomaly_type = 'expense_spike' AND NOT is_resolved;

COMMENT ON INDEX uq_anomaly_open_labor_overrun  IS 'At most one OPEN labor_overrun alert per (workspace, job). detect_anomalies() targets this predicate in ON CONFLICT for idempotency.';
COMMENT ON INDEX uq_anomaly_open_expense_spike  IS 'At most one OPEN expense_spike alert per (workspace, category, month) -- the workspace dimension is part of the key so two clients can each flag the same category/month. detect_anomalies() targets this predicate in ON CONFLICT for idempotency.';

-- 2.9 forecasts ---------------------------------------------------------------
-- Output table of forecast_engine.py (model 'seasonal_trend_v1'; the exact
-- method is documented in that module's docstring). One row per
-- (company, series, forecast month, model): re-running the engine for a
-- client workspace UPSERTS that workspace's rows in place via
-- ON CONFLICT (company_id, series, period_start, model) DO UPDATE, so the
-- table always holds the LATEST run's numbers per workspace/series/month --
-- never a growing history.
-- Series provenance (identical to the engine's monthly queries, scoped to
-- the workspace's own ledger/labor_logs by company_id):
--   revenue          = financial_ledger 'revenue' sums per month
--   labor_cost       = labor_logs SUM(hours * hourly_rate) per month
--   material_expense = financial_ledger expenses, category='materials', per month
--   net_cash_flow    = monthly (revenue - total expense) from the ledger
CREATE TABLE forecasts (
    id             BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    company_id     BIGINT NOT NULL DEFAULT 1 CONSTRAINT fk_forecasts_company_id REFERENCES companies (company_id) ON DELETE RESTRICT,
    series         TEXT NOT NULL
                   CHECK (series IN ('revenue','labor_cost','material_expense',
                                     'net_cash_flow')),
    period_start   DATE NOT NULL CHECK (EXTRACT(DAY FROM period_start) = 1),
    model          TEXT NOT NULL,
    predicted      NUMERIC(14,2) NOT NULL
                   CHECK (predicted >= 0 OR series = 'net_cash_flow'),
    ci_low         NUMERIC(14,2),
    ci_high        NUMERIC(14,2),
    mape           NUMERIC(8,2),    -- backtest accuracy: MAPE, or WAPE for net_cash_flow
    rmse           NUMERIC(14,2),
    horizon_months INTEGER NOT NULL, -- 1 = next month, 2 = month after, ...
    run_date       DATE NOT NULL DEFAULT CURRENT_DATE,
    details        JSONB,           -- model inputs: train window, seasonal factors, drift, sigma, z (+ company_id/company_name)
    CONSTRAINT uq_forecasts_series_period_model
        UNIQUE (company_id, series, period_start, model)
);

COMMENT ON TABLE  forecasts              IS 'Forecast engine output (forecast_engine.py, model seasonal_trend_v1), per client workspace. Upserted per (company_id, series, period_start, model): the table holds the latest run per workspace/series/month, not a run history. Series sources: revenue (ledger), labor_cost (labor_logs), material_expense (ledger materials), net_cash_flow (monthly revenue - total expense from the ledger) -- all scoped to the workspace by company_id.';
COMMENT ON COLUMN forecasts.company_id   IS 'Client workspace the forecast was produced for (forecast_engine.run_forecasts(company_id=...) forecasts THAT workspace''s series only).';
COMMENT ON COLUMN forecasts.series       IS 'Which monthly series the row forecasts; constrained to the four series forecast_engine.py models.';
COMMENT ON COLUMN forecasts.period_start IS 'First day of the forecast month (enforced by CHECK, mirroring market_benchmarks.period).';
COMMENT ON COLUMN forecasts.model        IS 'Model id, e.g. ''seasonal_trend_v1'' (method documented in forecast_engine.py). Part of the upsert key so a future model version coexists with the old one.';
COMMENT ON COLUMN forecasts.predicted    IS 'Point forecast for the month. Non-negative for revenue/labor_cost/material_expense; only net_cash_flow may be negative (CHECK reflects the cash series'' real negative months).';
COMMENT ON COLUMN forecasts.ci_low       IS 'Lower bound of the 90% confidence interval: predicted - 1.645 * sigma * sqrt(horizon_months); clamped >= 0 for non-negative series (net_cash_flow is not clamped).';
COMMENT ON COLUMN forecasts.ci_high      IS 'Upper bound of the 90% confidence interval: predicted + 1.645 * sigma * sqrt(horizon_months).';
COMMENT ON COLUMN forecasts.mape         IS 'Backtest accuracy OF THE MODEL for this series, from the most recent walk-forward (backtest_results): MAPE (mean absolute percentage error) for the positive-actual series; WAPE (weighted absolute percentage error = SUM|error| / SUM|actual|) for net_cash_flow, whose negative months make MAPE undefined. NULL until a backtest has run.';
COMMENT ON COLUMN forecasts.rmse         IS 'Backtest RMSE (root mean squared error) for this series pooled across walk-forward folds; the same pooled sigma feeds the confidence intervals. NULL until a backtest has run.';
COMMENT ON COLUMN forecasts.horizon_months IS 'Steps ahead from the training window''s last month: 1 = the month right after it, 2 = the month after that, ....';
COMMENT ON COLUMN forecasts.run_date     IS 'Date the engine produced this row (DEFAULT CURRENT_DATE, refreshed on every upsert).';
COMMENT ON COLUMN forecasts.details      IS 'JSONB model provenance: train window, per-calendar-month seasonal factors, deseasonalized level, trend slope/growth, drift blend (model vs market), sigma and its source (backtest vs in-sample), z = 1.645, metric type (MAPE vs WAPE), plus company_id/company_name of the workspace forecasted.';

-- 2.10 backtest_results -------------------------------------------------------
-- GLOBAL model audit trail (no company_id): walk-forward validation folds
-- document how seasonal_trend_v1 performed on the series it was fed; the
-- latest run's folds win the (series, model, fold) upsert. This is
-- deliberately shared across client workspaces -- it is model quality
-- bookkeeping, not client data (a workspace's forecasts reference it only
-- through the pooled sigma/accuracy they carry in their own columns).
-- Walk-forward (expanding-origin) validation output of forecast_engine.py:
-- one row per (series, model, fold). A fold trains seasonal_trend_v1 on the
-- first `train_months` months of a series' history and predicts the next
-- `test_months` months at horizons h = 1..test_months; later folds shift the
-- whole window forward by test_months while a full test window still fits.
-- The fold label encodes the window, e.g.
-- 'train_2023-12_2026-05_test_2026-06_2026-11'. Idempotent: the engine
-- upserts via ON CONFLICT (series, model, fold) DO UPDATE.
CREATE TABLE backtest_results (
    id          BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    series      TEXT NOT NULL,
    model       TEXT NOT NULL,
    fold        TEXT NOT NULL,
    train_start DATE NOT NULL,
    train_end   DATE NOT NULL,
    test_start  DATE NOT NULL,
    test_end    DATE NOT NULL,
    mape        NUMERIC(8,2),   -- WAPE for net_cash_flow (negative months break MAPE)
    rmse        NUMERIC(14,2),
    predictions JSONB,          -- [{period, predicted, actual, abs_pct_err}, ...]
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_backtest_series_model_fold UNIQUE (series, model, fold)
);

COMMENT ON TABLE  backtest_results             IS 'Walk-forward validation output (forecast_engine.py). One row per (series, model, fold); re-running a fold upserts it in place. The dashboard''s backtest accuracy table reads from here, and the pooled fold errors are the sigma behind forecasts'' confidence intervals.';
COMMENT ON COLUMN backtest_results.series      IS 'Monthly series validated (same four as forecasts).';
COMMENT ON COLUMN backtest_results.model       IS 'Model id that produced the fold, e.g. ''seasonal_trend_v1''.';
COMMENT ON COLUMN backtest_results.fold        IS 'Fold label encoding the window, e.g. ''train_2023-12_2026-05_test_2026-06_2026-11'' (inclusive month-start bounds; test follows train with no overlap).';
COMMENT ON COLUMN backtest_results.train_start IS 'First month (month start) of the fold''s training window.';
COMMENT ON COLUMN backtest_results.train_end   IS 'Last month of the training window; the model never saw anything after this month (no look-ahead).';
COMMENT ON COLUMN backtest_results.test_start  IS 'First held-out month (horizon h = 1 measured from train_end).';
COMMENT ON COLUMN backtest_results.test_end    IS 'Last held-out month of the fold.';
COMMENT ON COLUMN backtest_results.mape        IS 'Fold accuracy: MAPE (mean of |error|/|actual| over non-zero-actual test months) for the positive series; WAPE (SUM|error| / SUM|actual|) for net_cash_flow -- its negative months make MAPE undefined, so WAPE is the metric convention for that series across the whole forecast layer.';
COMMENT ON COLUMN backtest_results.rmse        IS 'Fold root mean squared error over the test months.';
COMMENT ON COLUMN backtest_results.predictions IS 'JSONB array of the fold''s per-month predictions: [{"period": "...", "predicted": ..., "actual": ..., "abs_pct_err": ...}]. abs_pct_err is NULL for a zero actual (MAPE is undefined there; WAPE still uses the month''s absolute error).';
COMMENT ON COLUMN backtest_results.created_at  IS 'Row write time (refreshed on every upsert of the fold).';

-- =============================================================================
--  SoCal REGIONAL BENCHMARK TABLES (sections 2.11-2.16)
--  Written exclusively by harvest_socal_fintech.py; read by the
--  mv_socal_rate_guidance materialized view (analytics_views.sql) and the
--  dashboard's "SoCal Benchmarks" tab. They carry NO foreign keys: the join
--  keys are the fixed vocabulary columns below, harvested on a per-source
--  cadence. Every row carries a `source` provenance string of the form
--  "<human description>, retrieved <date> from <url>" so each row can be
--  audited back to the exact file it came from.
--
--  FIXED REGION VOCABULARY (region_code; used consistently across schema,
--  harvester, dashboard and exports):
--    la_msa            Los Angeles-Long Beach-Anaheim, CA MSA            (CBSA 31080)
--    oxnard_ventura_msa Oxnard-Thousand Oaks-Ventura, CA MSA             (CBSA 37100)
--    orange_county     Santa Ana-Anaheim-Irvine, CA Metro Division       (CBSA/MD 36004)
--    san_diego_msa     San Diego-Chula Vista-Carlsbad, CA MSA            (CBSA 41740)
--    inland_empire_msa Riverside-San Bernardino-Ontario, CA MSA         (CBSA 40140)
--    california_state  California statewide                              (FIPS 06)
--
--  FIXED VERTICAL VOCABULARY (vertical_key): wealth_advisory, b2b_saas,
--  custom_software_ai, fractional_cfo, fintech_payments, managed_it_cyber.
-- =============================================================================

-- 2.11 regional_industry_profiles ----------------------------------------------
-- MODELED unit economics for each Finance/Tech vertical x region: ONE row per
-- (vertical_key, region_code) -- 6 x 6 = 36 rows when fully populated.
-- Provenance matters here: gross_margin_pct (with its p25/p75 band),
-- payroll_to_revenue_ratio, cac_usd and utilization_target_pct are MODELED
-- industry-norm constants (documented in the notes JSONB -- they are NOT
-- scraped from any public file). locality_burden_pct is a MODELED regional
-- payroll-burden delta vs the California baseline (workers comp / insurance /
-- locality premium; negative = cheaper than baseline). median_salary_usd is
-- the ONLY harvested input: the OEWS annual median of the vertical's anchor
-- occupation in that region. mv_socal_rate_guidance combines these with the
-- harvested wages into billable-rate guidance.
CREATE TABLE regional_industry_profiles (
    id                       BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    vertical_key             TEXT          NOT NULL
                             CHECK (vertical_key IN ('wealth_advisory','b2b_saas',
                                     'custom_software_ai','fractional_cfo',
                                     'fintech_payments','managed_it_cyber')),
    vertical_name            TEXT          NOT NULL,
    region_code              TEXT          NOT NULL
                             CHECK (region_code IN ('la_msa','oxnard_ventura_msa',
                                     'orange_county','san_diego_msa',
                                     'inland_empire_msa','california_state')),
    anchor_occupation_code   TEXT          NOT NULL,
    gross_margin_pct         NUMERIC(5,2)  NOT NULL,
    gross_margin_p25_pct     NUMERIC(5,2),
    gross_margin_p75_pct     NUMERIC(5,2),
    payroll_to_revenue_ratio NUMERIC(5,4)  NOT NULL,
    cac_usd                  NUMERIC(14,2) NOT NULL,
    utilization_target_pct   NUMERIC(5,2)  NOT NULL,
    locality_burden_pct      NUMERIC(5,2)  NOT NULL,
    median_salary_usd        NUMERIC(14,2),
    notes                    JSONB,
    source                   TEXT          NOT NULL,
    created_at               TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT uq_regional_profiles_vertical_region
        UNIQUE (vertical_key, region_code),
    CONSTRAINT chk_regional_profiles_vertical_key
        CHECK (vertical_key IN ('wealth_advisory','b2b_saas','custom_software_ai',
                                'fractional_cfo','fintech_payments','managed_it_cyber')),
    CONSTRAINT chk_regional_profiles_region_code
        CHECK (region_code IN ('la_msa','oxnard_ventura_msa','orange_county',
                               'san_diego_msa','inland_empire_msa','california_state')),
    CONSTRAINT chk_regional_profiles_payroll_ratio_range
        CHECK (payroll_to_revenue_ratio > 0 AND payroll_to_revenue_ratio < 1),
    CONSTRAINT chk_regional_profiles_cac_nonnegative
        CHECK (cac_usd >= 0),
    CONSTRAINT chk_regional_profiles_utilization_range
        CHECK (utilization_target_pct >= 1 AND utilization_target_pct <= 100),
    CONSTRAINT chk_regional_profiles_locality_burden_range
        CHECK (locality_burden_pct >= -20 AND locality_burden_pct <= 40)
);

COMMENT ON TABLE  regional_industry_profiles IS 'Modeled unit economics per Finance/Tech vertical x SoCal region (harvest_socal_fintech.py --sources profiles). Margin bands / payroll ratio / CAC / utilization are MODELED industry-norm constants (NOT scraped); median_salary_usd is the harvested OEWS annual median of the vertical''s anchor occupation. mv_socal_rate_guidance turns these into billable-rate guidance.';
COMMENT ON COLUMN regional_industry_profiles.vertical_key    IS 'Fixed vertical vocabulary: wealth_advisory, b2b_saas, custom_software_ai, fractional_cfo, fintech_payments, managed_it_cyber.';
COMMENT ON COLUMN regional_industry_profiles.region_code     IS 'Fixed 6-region SoCal vocabulary (see the block comment above sections 2.11-2.16).';
COMMENT ON COLUMN regional_industry_profiles.anchor_occupation_code IS 'BLS OEWS occupation code the vertical''s pricing anchors on (e.g. 15-1252 Software Developers; 13-2011 Accountants; 11-3021 IT Managers). Joined to socal_wage_benchmarks by (region_code, occupation_code).';
COMMENT ON COLUMN regional_industry_profiles.gross_margin_pct IS 'MODELED target gross margin % for the vertical (band mid-point). NOT scraped data.';
COMMENT ON COLUMN regional_industry_profiles.gross_margin_p25_pct IS 'MODELED low end of the vertical''s gross-margin band (p25). NULL allowed.';
COMMENT ON COLUMN regional_industry_profiles.gross_margin_p75_pct IS 'MODELED high end of the vertical''s gross-margin band (p75). NULL allowed.';
COMMENT ON COLUMN regional_industry_profiles.payroll_to_revenue_ratio IS 'MODELED payroll as a fraction of revenue (strictly between 0 and 1).';
COMMENT ON COLUMN regional_industry_profiles.cac_usd         IS 'MODELED customer acquisition cost in USD (>= 0).';
COMMENT ON COLUMN regional_industry_profiles.utilization_target_pct IS 'MODELED target billable utilization (1-100): the share of PAID hours that are actually billable. Drives the loaded-cost step of the rate formula.';
COMMENT ON COLUMN regional_industry_profiles.locality_burden_pct IS 'MODELED regional payroll burden vs the California baseline (workers comp / insurance / locality premium): california_state = 0.0 baseline, negative = cheaper than baseline (e.g. Inland Empire), positive = pricier (e.g. LA). NOT scraped data.';
COMMENT ON COLUMN regional_industry_profiles.median_salary_usd IS 'Harvested OEWS annual median wage of anchor_occupation_code in region_code (data_year recorded in the source string). Derived from socal_wage_benchmarks at load time; NOT a foreign key (the wage table is refreshed on its own cadence).';
COMMENT ON COLUMN regional_industry_profiles.notes           IS 'JSONB modeling basis: constants used, industry-norm citation basis ("Modeled baseline constants; industry norms 2025-2026 -- NOT scraped data") and where median_salary_usd came from.';
COMMENT ON COLUMN regional_industry_profiles.source          IS 'Provenance: modeling note + OEWS vintage + retrieval date + URL.';

-- 2.12 socal_wage_benchmarks ---------------------------------------------------
-- REAL harvested BLS OEWS wages: one row per (region_code, occupation_code,
-- data_year) for the 8 task-facing roles across the 6 regions. Written by
-- harvest_socal_fintech.py from the May 2024 OEWS metro/division and state
-- files (oesm24ma.zip / oesm24st.zip). All-ownership (or Private) rows for
-- NAICS 000000 (all industries). Suppressed OEWS cells ("*" in the source)
-- load as NULL -- a row is still created for every published occupation even
-- when some percentiles are withheld, but any percentile that IS present must
-- be ordered (p25 <= median <= p75 <= p90) and positive.
CREATE TABLE socal_wage_benchmarks (
    id               BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    region_code      TEXT        NOT NULL,
    occupation_code  TEXT        NOT NULL,
    occupation_title TEXT,
    role_label       TEXT        NOT NULL,
    data_year        SMALLINT    NOT NULL,
    hourly_p25       NUMERIC(14,2),
    hourly_median    NUMERIC(14,2),
    hourly_p75       NUMERIC(14,2),
    hourly_p90       NUMERIC(14,2),
    annual_p25       NUMERIC(14,2),
    annual_median    NUMERIC(14,2),
    annual_p75       NUMERIC(14,2),
    annual_p90       NUMERIC(14,2),
    employment       INTEGER,
    employment_rse   NUMERIC(6,2),
    source           TEXT        NOT NULL,
    CONSTRAINT uq_socal_wage_region_occ_year
        UNIQUE (region_code, occupation_code, data_year),
    CONSTRAINT chk_socal_wage_region_code
        CHECK (region_code IN ('la_msa','oxnard_ventura_msa','orange_county',
                               'san_diego_msa','inland_empire_msa','california_state')),
    CONSTRAINT chk_socal_wage_hourly_ordered
        CHECK ((hourly_p25 <= hourly_median OR hourly_p25 IS NULL OR hourly_median IS NULL)
           AND (hourly_median <= hourly_p75 OR hourly_median IS NULL OR hourly_p75 IS NULL)
           AND (hourly_p75 <= hourly_p90 OR hourly_p75 IS NULL OR hourly_p90 IS NULL)),
    CONSTRAINT chk_socal_wage_hourly_positive
        CHECK ((hourly_p25 IS NULL OR hourly_p25 > 0)
           AND (hourly_median IS NULL OR hourly_median > 0)
           AND (hourly_p75 IS NULL OR hourly_p75 > 0)
           AND (hourly_p90 IS NULL OR hourly_p90 > 0)),
    CONSTRAINT chk_socal_wage_annual_ordered
        CHECK ((annual_p25 <= annual_median OR annual_p25 IS NULL OR annual_median IS NULL)
           AND (annual_median <= annual_p75 OR annual_median IS NULL OR annual_p75 IS NULL)
           AND (annual_p75 <= annual_p90 OR annual_p75 IS NULL OR annual_p90 IS NULL)),
    CONSTRAINT chk_socal_wage_annual_positive
        CHECK ((annual_p25 IS NULL OR annual_p25 > 0)
           AND (annual_median IS NULL OR annual_median > 0)
           AND (annual_p75 IS NULL OR annual_p75 > 0)
           AND (annual_p90 IS NULL OR annual_p90 > 0)),
    CONSTRAINT chk_socal_wage_employment_nonnegative
        CHECK (employment IS NULL OR employment >= 0)
);

COMMENT ON TABLE  socal_wage_benchmarks IS 'REAL BLS OEWS wages per (region, occupation, data_year), harvested by harvest_socal_fintech.py from the May 2024 metro/division + state files. Suppressed OEWS cells load as NULL (rows are still created); present percentiles must be ordered and positive. Sourced as published -- wages are NOT recomputed.';
COMMENT ON COLUMN socal_wage_benchmarks.region_code  IS 'Fixed 6-region SoCal vocabulary. Metro/division rows come from oesm24ma.zip (31080/37100/41740/40140 MSA + 36004 MD); california_state rows come from oesm24st.zip (FIPS 06).';
COMMENT ON COLUMN socal_wage_benchmarks.occupation_code IS 'BLS OEWS occupation code (e.g. 15-1252 Software Developers).';
COMMENT ON COLUMN socal_wage_benchmarks.occupation_title IS 'OEWS occupation title as published (e.g. "Software Developers").';
COMMENT ON COLUMN socal_wage_benchmarks.role_label   IS 'Task-facing role label used by the dashboard/export (e.g. "Software Engineer", "Bookkeeper") -- the business name; occupation_code is the statistical name.';
COMMENT ON COLUMN socal_wage_benchmarks.data_year    IS 'OEWS survey year of the source file (2024 for the May 2024 files).';
COMMENT ON COLUMN socal_wage_benchmarks.hourly_p25   IS 'Hourly 25th percentile wage as published ($/hr); NULL when suppressed ("*"). OEWS hourly wages are rounded to the nearest cent.';
COMMENT ON COLUMN socal_wage_benchmarks.annual_median IS 'ANNUAL median wage as published ($/year, rounded to the nearest $100 by OEWS). This is the input to the rate formula in mv_socal_rate_guidance. Annual and hourly are kept AS PUBLISHED -- the harvester never recomputes one from the other (2080-hour convention: annual ~= hourly x 2080 within OEWS rounding; deviations are logged as warnings).';
COMMENT ON COLUMN socal_wage_benchmarks.employment   IS 'OEWS estimated employment in the occupation (>= 0); NULL when suppressed. NULL means "withheld", never 0.';
COMMENT ON COLUMN socal_wage_benchmarks.employment_rse IS 'OEWS relative standard error of employment (%); NULL when suppressed.';
COMMENT ON COLUMN socal_wage_benchmarks.source       IS 'Provenance: "<file description>, retrieved <date> from <url>" (the exact OEWS zip the row came from).';

-- 2.13 socal_macro_indicators --------------------------------------------------
-- Monthly macro series for the SoCal regions (FRED keyless CSV endpoint via
-- harvest_socal_fintech.py): CPI, employment, interest rates, real-estate /
-- office-lease cost PPIs. period is always the FIRST OF THE MONTH; sub-monthly
-- series (weekly mortgage rates, daily-fed-funds averages) are averaged into
-- calendar months before loading, mirroring fetch_benchmarks.py.
--
-- DEDUP KEY NOTE (deliberate deviation from a plain UNIQUE(series_id, period)):
-- the same FRED series CAN legitimately be loaded under more than one
-- region_code. California has no statewide CPI, and Oxnard-Ventura and Orange
-- County have no metro CPI series, so the national CPI-U (CPIAUCSL) is loaded
-- as a documented PROXY under oxnard_ventura_msa and orange_county in addition
-- to california_state. A plain UNIQUE(series_id, period) would collide those
-- proxy rows with the statewide baseline row, so the dedup key includes
-- region_code: UNIQUE (region_code, series_id, period).
CREATE TABLE socal_macro_indicators (
    id          BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    region_code TEXT          NOT NULL,
    series_id   TEXT          NOT NULL,
    series_name TEXT          NOT NULL,
    category    TEXT          NOT NULL,
    period      DATE          NOT NULL,
    value       NUMERIC(18,4) NOT NULL,
    unit        TEXT          NOT NULL,
    source      TEXT          NOT NULL,
    CONSTRAINT uq_socal_macro_region_series_period
        UNIQUE (region_code, series_id, period),
    CONSTRAINT chk_socal_macro_region_code
        CHECK (region_code IN ('la_msa','oxnard_ventura_msa','orange_county',
                               'san_diego_msa','inland_empire_msa','california_state')),
    CONSTRAINT chk_socal_macro_category
        CHECK (category IN ('cpi','employment','interest_rate','real_estate','ppi')),
    CONSTRAINT chk_socal_macro_period_is_month_start
        CHECK (EXTRACT(DAY FROM period) = 1),
    CONSTRAINT chk_socal_macro_value_positive
        CHECK (value > 0)
);

COMMENT ON TABLE  socal_macro_indicators IS 'Monthly macro indicators for the SoCal regions from FRED (CPI, employment, interest rates, real-estate/office-lease PPIs), loaded by harvest_socal_fintech.py. period = first of month; sub-monthly observations averaged into calendar months. Dedup key is (region_code, series_id, period) because one FRED series may serve several regions as a documented proxy (CPIAUCSL for Ventura/Orange County).';
COMMENT ON COLUMN socal_macro_indicators.region_code IS 'Fixed 6-region SoCal vocabulary. california_state rows include US-national baseline series (documented in series_name); proxy rows carry the same series_id as the baseline.';
COMMENT ON COLUMN socal_macro_indicators.series_id   IS 'Canonical FRED series id (e.g. CUURA311SA0, CPIAUCSL, PCU531120531120, SMU06310805500000001).';
COMMENT ON COLUMN socal_macro_indicators.series_name IS 'Human-readable description INCLUDING proxy notes (e.g. "US CPI-U -- California statewide proxy: CA has no statewide CPI").';
COMMENT ON COLUMN socal_macro_indicators.category    IS 'Dashboard grouping: cpi / employment / interest_rate / real_estate / ppi.';
COMMENT ON COLUMN socal_macro_indicators.period      IS 'First day of the calendar month the observation applies to (sub-monthly series averaged; enforced by CHECK).';
COMMENT ON COLUMN socal_macro_indicators.value       IS 'Series level for the month (> 0). Index levels are as published; rates in percent; employment in thousands.';
COMMENT ON COLUMN socal_macro_indicators.unit        IS 'Unit of measure as published, e.g. "Index 1982-84=100" or "Index 1982-84=100, US CPI-U as CA proxy" (california_state/oxnard_ventura_msa/orange_county CPI rows), "percent", "thousands of employees".';
COMMENT ON COLUMN socal_macro_indicators.source      IS 'Provenance: FRED + the underlying agency (BLS/FRB), retrieval date, and the exact fredgraph.csv URL.';

-- 2.14 socal_business_patterns -------------------------------------------------
-- REAL Census County Business Patterns (CBP 2023, cbp23co.zip) and ZIP Business
-- Patterns (ZBP 2023, zbp23detail.zip) rows for the Finance/Tech NAICS sectors
-- (51 Information, 52 Finance & Insurance, 54 Professional/Scientific/Tech
-- Services) in the six SoCal counties and their ZIPs. One row per
-- (geo_level, geo_fips, naics_code, data_year).
--
-- SUPPRESSION RULES (read from the CBP/ZBP record layouts, never guessed):
--   * employment is NULL when the file withholds it (EMPFLAG range flag set, or
--     EMP negative/blank per the layout). NULL means "withheld by CBP noise
--     protection" -- NEVER store the layout's sentinel values (-1/-2/...) or a
--     bogus 0 as employment.
--   * PAYANN is published in THOUSANDS of dollars: annual_payroll_usd stores
--     PAYANN x 1000 (documented conversion). When the payroll flag says
--     withheld (or PAYANN is negative/blank), annual_payroll_usd is NULL.
--   * geo_level='zip' rows whose ZIP could not be attributed to a county carry
--     region_code NULL: they remain queryable statewide (geo_fips = ZIP) but
--     are excluded from per-region dashboards until a zip->county mapping
--     succeeds.
CREATE TABLE socal_business_patterns (
    id                   BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    geo_level            TEXT          NOT NULL,
    geo_fips             TEXT          NOT NULL,
    geo_name             TEXT,
    region_code          TEXT,
    naics_code           TEXT          NOT NULL,
    naics_label          TEXT,
    data_year            SMALLINT      NOT NULL,
    establishments       INTEGER       NOT NULL,
    employment           BIGINT,
    employment_flag      TEXT,
    annual_payroll_usd   NUMERIC(16,2),
    payroll_per_employee NUMERIC(14,2),
    source               TEXT          NOT NULL,
    CONSTRAINT uq_socal_bp_geo_naics_year
        UNIQUE (geo_level, geo_fips, naics_code, data_year),
    CONSTRAINT chk_socal_bp_geo_level
        CHECK (geo_level IN ('county','zip','msa')),
    CONSTRAINT chk_socal_bp_region_code
        CHECK (region_code IS NULL OR region_code IN
               ('la_msa','oxnard_ventura_msa','orange_county','san_diego_msa',
                'inland_empire_msa','california_state')),
    CONSTRAINT chk_socal_bp_establishments_nonnegative
        CHECK (establishments >= 0),
    CONSTRAINT chk_socal_bp_employment_nonnegative
        CHECK (employment IS NULL OR employment >= 0)
);

COMMENT ON TABLE  socal_business_patterns IS 'REAL Census County/ZIP Business Patterns (2023) for Finance/Tech NAICS (51/52/54 families) in the six SoCal counties and their ZIPs, loaded by harvest_socal_fintech.py. Suppressed employment/payroll load as NULL (never sentinel values); PAYANN is converted from thousands to dollars (x 1000).';
COMMENT ON COLUMN socal_business_patterns.geo_level IS '''county'' (CBP county file), ''zip'' (ZBP detail file) or ''msa'' (reserved for future CBSA-level loads).';
COMMENT ON COLUMN socal_business_patterns.geo_fips  IS 'County: 5-char state+county FIPS (e.g. 06037). ZIP: 5-char zero-padded ZIP code.';
COMMENT ON COLUMN socal_business_patterns.geo_name  IS 'County name / NULL for zip rows.';
COMMENT ON COLUMN socal_business_patterns.region_code IS 'Fixed 6-region vocabulary mapped from the county (06037->la_msa, 06059->orange_county, 06111->oxnard_ventura_msa, 06073->san_diego_msa, 06065/06071->inland_empire_msa). NULL for zip rows whose ZIP could not be attributed to a SoCal county (documented degradation: the row stays statewide-queryable but is excluded from per-region views).';
COMMENT ON COLUMN socal_business_patterns.naics_code IS 'NAICS 2022 code with aggregation dashes stripped (e.g. ''51'' sector, ''5132'' 4-digit, ''513210'' 6-digit). Only the Finance/Tech families (51/52/54 prefixes) are kept.';
COMMENT ON COLUMN socal_business_patterns.naics_label IS 'Human-readable NAICS description (static label dict in harvest_socal_fintech.py; NULL for codes outside the dict).';
COMMENT ON COLUMN socal_business_patterns.employment IS 'CBP mid-March employment as published. VERIFIED against the 2020-2023 CBP record layout (county-layout-2020.txt): since 2022 CBP injects noise instead of suppressing, so EMP carries the noise-adjusted employment for every published row and EMP_NF is a NOISE LEVEL flag (G = <2%, H = 2-<5%, J = >=5%), NOT a suppression flag. Negative/blank sentinels (-1/-2 of the pre-2017 layout) are mapped to NULL defensively and never stored. geo_level=''zip'' rows (ZBP detail) carry employment NULL: the modern ZBP detail file publishes only establishments by size class, no employment/payroll.';
COMMENT ON COLUMN socal_business_patterns.employment_flag IS 'Raw CBP EMP_NF noise-level flag as published (G/H/J per the 2020-2023 record layout; documented in the employment column comment). NULL for zip rows (no employment published) or when no flag was set.';
COMMENT ON COLUMN socal_business_patterns.annual_payroll_usd IS 'Annual payroll in DOLLARS: the file''s AP field is in THOUSANDS of dollars (with noise since 2022), so this column = AP x 1000 (documented conversion). NULL for zip rows (ZBP detail publishes no payroll) or when the source value is blank/unparseable.';
COMMENT ON COLUMN socal_business_patterns.payroll_per_employee IS 'annual_payroll_usd / employment, computed ONLY when both are present and employment > 0; NULL otherwise.';
COMMENT ON COLUMN socal_business_patterns.source    IS 'Provenance: "<file description>, retrieved <date> from <url>".';

-- 2.15 socal_loan_activity -----------------------------------------------------
-- REAL SBA 7(a) and 504 loan activity aggregated by harvest_socal_fintech.py
-- from the loan-level FOIA CSVs (data.sba.gov dataset 7a-504-foia): one row per
-- (program, fiscal_year, geo_level, geo_fips). County rows are filtered to the
-- six SoCal counties; ONE california_state row per (program, fiscal_year)
-- aggregates ALL California loans. Rows are upserted idempotently.
CREATE TABLE socal_loan_activity (
    id                 BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    program            TEXT          NOT NULL,
    fiscal_year        SMALLINT      NOT NULL,
    geo_level          TEXT          NOT NULL,
    geo_fips           TEXT          NOT NULL,
    geo_name           TEXT,
    region_code        TEXT,
    loans_count        INTEGER       NOT NULL,
    gross_approval_usd NUMERIC(16,2) NOT NULL,
    avg_loan_usd       NUMERIC(14,2),
    top_lender         TEXT,
    top_lender_loans   INTEGER,
    source             TEXT          NOT NULL,
    CONSTRAINT uq_socal_loan_program_year_geo
        UNIQUE (program, fiscal_year, geo_level, geo_fips),
    CONSTRAINT chk_socal_loan_program
        CHECK (program IN ('7a','504')),
    CONSTRAINT chk_socal_loan_geo_level
        CHECK (geo_level IN ('county','state')),
    CONSTRAINT chk_socal_loan_region_code
        CHECK (region_code IS NULL OR region_code IN
               ('la_msa','oxnard_ventura_msa','orange_county','san_diego_msa',
                'inland_empire_msa','california_state')),
    CONSTRAINT chk_socal_loan_loans_nonnegative
        CHECK (loans_count >= 0),
    CONSTRAINT chk_socal_loan_gross_nonnegative
        CHECK (gross_approval_usd >= 0)
);

COMMENT ON TABLE  socal_loan_activity IS 'Aggregated SBA 7(a) / 504 loan activity per (program, fiscal_year, geography), built by harvest_socal_fintech.py from the loan-level FOIA CSVs on data.sba.gov. County rows cover the six SoCal counties; state rows are all-California. NOT fabricated: when the SBA portal cannot be reached, no rows are written and the failure is logged.';
COMMENT ON COLUMN socal_loan_activity.program            IS 'SBA loan program: ''7a'' (7(a) guaranteed loans) or ''504'' (CDC/504 fixed-asset loans).';
COMMENT ON COLUMN socal_loan_activity.fiscal_year        IS 'SBA federal fiscal year of loan approval (from the file''s ApprovalFiscalYear).';
COMMENT ON COLUMN socal_loan_activity.geo_level          IS '''county'' (one of the six SoCal counties) or ''state'' (all-California aggregate, geo_fips ''06'').';
COMMENT ON COLUMN socal_loan_activity.geo_fips           IS '5-char county FIPS (e.g. 06037) for county rows; ''06'' for state rows.';
COMMENT ON COLUMN socal_loan_activity.region_code        IS 'Fixed 6-region vocabulary mapped from the county; california_state for state rows; NULL only for non-SoCal geographies (not loaded today).';
COMMENT ON COLUMN socal_loan_activity.loans_count        IS 'Number of SBA loans approved in the geography/program/fiscal year (>= 0).';
COMMENT ON COLUMN socal_loan_activity.gross_approval_usd IS 'Sum of loan approval amounts in USD (>= 0).';
COMMENT ON COLUMN socal_loan_activity.avg_loan_usd       IS 'gross_approval_usd / loans_count; NULL when loans_count is 0.';
COMMENT ON COLUMN socal_loan_activity.top_lender         IS 'Lender (as named in the FOIA file) with the MOST loans in that geography/program/fiscal year; NULL when no loans.';
COMMENT ON COLUMN socal_loan_activity.top_lender_loans   IS 'That top lender''s loan count.';
COMMENT ON COLUMN socal_loan_activity.source             IS 'Provenance: the exact FOIA CSV resource URL + retrieval date.';

-- 2.16 socal_harvest_log -------------------------------------------------------
-- APPEND-ONLY audit log of every harvest attempt: one row per
-- (run_id, source_name, url). No unique constraint on purpose -- the same URL
-- may be attempted (and logged) on every run. status: 'ok' (fresh fetch),
-- 'cache_hit' (served from the local disk cache within its TTL) or 'failed'
-- (all retries exhausted). Auditors (Sub-agent QA) can reconstruct exactly
-- which files produced the data and when.
CREATE TABLE socal_harvest_log (
    id          BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    run_id      TEXT        NOT NULL,
    source_name TEXT        NOT NULL,
    url         TEXT        NOT NULL,
    status      TEXT        NOT NULL,
    http_status SMALLINT,
    rows_loaded INTEGER     NOT NULL DEFAULT 0,
    message     TEXT,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_socal_harvest_log_status
        CHECK (status IN ('ok','failed','cache_hit'))
);

COMMENT ON TABLE  socal_harvest_log IS 'Append-only audit trail of harvest_socal_fintech.py: one row per (run, source, url) with ok / failed / cache_hit status, HTTP status, rows loaded and a message. No unique constraint by design (a URL is logged on every run).';
COMMENT ON COLUMN socal_harvest_log.run_id      IS 'uuid4 hex generated per harvester invocation; groups all attempts of one run.';
COMMENT ON COLUMN socal_harvest_log.source_name IS 'Harvest family: oews / fred / cbp / zbp / sba / profiles.';
COMMENT ON COLUMN socal_harvest_log.url         IS 'The exact URL attempted (or served from cache).';
COMMENT ON COLUMN socal_harvest_log.status      IS '''ok'' = fresh network fetch succeeded; ''cache_hit'' = served from .cache within TTL; ''failed'' = all retries exhausted (message explains why).';
COMMENT ON COLUMN socal_harvest_log.http_status IS 'HTTP status of the last attempt (NULL when the network itself failed, e.g. timeout/DNS).';
COMMENT ON COLUMN socal_harvest_log.rows_loaded IS 'Warehouse rows written from this URL in this run (0 for cache_hit on a parse failure or dry-run).';
COMMENT ON COLUMN socal_harvest_log.message     IS 'Human-readable detail: row counts, 2080-consistency warnings, degradation notes, error text.';
COMMENT ON COLUMN socal_harvest_log.fetched_at  IS 'Attempt timestamp (UTC, DEFAULT now()).';

-- -----------------------------------------------------------------------------
-- 3) INDEXES (FK columns are NOT auto-indexed by PostgreSQL; add them here)
-- -----------------------------------------------------------------------------

-- company_id FK indexes: every workspace-scoped query in the dashboard and
-- the analytics layer filters by the active client workspace first.
CREATE INDEX idx_customers_company_id      ON customers (company_id);
CREATE INDEX idx_job_types_company_id      ON job_types (company_id);
CREATE INDEX idx_employees_company_id      ON employees (company_id);
CREATE INDEX idx_jobs_company_id           ON jobs (company_id);
CREATE INDEX idx_ledger_company_id         ON financial_ledger (company_id);
CREATE INDEX idx_labor_logs_company_id     ON labor_logs (company_id);
CREATE INDEX idx_anomaly_alerts_company_id ON anomaly_alerts (company_id);
CREATE INDEX idx_forecasts_company_id      ON forecasts (company_id);

CREATE INDEX idx_jobs_customer_id  ON jobs (customer_id);
CREATE INDEX idx_jobs_job_type_id  ON jobs (job_type_id);
CREATE INDEX idx_jobs_job_date     ON jobs (job_date);
CREATE INDEX idx_jobs_status       ON jobs (status);

-- financial_ledger: monthly reporting, category pivots, job roll-ups
CREATE INDEX idx_ledger_entry_date  ON financial_ledger (entry_date);
CREATE INDEX idx_ledger_category    ON financial_ledger (category);
CREATE INDEX idx_ledger_job_id      ON financial_ledger (job_id);
CREATE INDEX idx_ledger_customer_id ON financial_ledger (customer_id);
CREATE INDEX idx_ledger_type_date   ON financial_ledger (entry_type, entry_date);

-- labor_logs: per-job and per-employee timesheets, monthly labor curves
CREATE INDEX idx_labor_logs_job_id      ON labor_logs (job_id);
CREATE INDEX idx_labor_logs_employee_id ON labor_logs (employee_id);
CREATE INDEX idx_labor_logs_work_date   ON labor_logs (work_date);

-- market_benchmarks: the (job_type_id, period, region) unique constraint already
-- serves job-type lookups; period helps time-range scans.
CREATE INDEX idx_market_benchmarks_period ON market_benchmarks (period);

-- anomaly_alerts: per-job alert history (the partial unique indexes above serve
-- the detector's open-alert lookups).
CREATE INDEX idx_anomaly_alerts_job_id ON anomaly_alerts (job_id);

-- SoCal regional benchmark tables (harvest_socal_fintech.py): the unique
-- constraints above serve the upsert keys; these serve the dashboard's
-- region/year/NAICS/series lookups.
CREATE INDEX idx_regional_profiles_region    ON regional_industry_profiles (region_code);
CREATE INDEX idx_socal_wage_region_year      ON socal_wage_benchmarks (region_code, data_year);
CREATE INDEX idx_socal_wage_occupation       ON socal_wage_benchmarks (occupation_code);
CREATE INDEX idx_socal_macro_region_period   ON socal_macro_indicators (region_code, period);
CREATE INDEX idx_socal_macro_series          ON socal_macro_indicators (series_id);
CREATE INDEX idx_socal_macro_category        ON socal_macro_indicators (category);
CREATE INDEX idx_socal_bp_region_naics       ON socal_business_patterns (region_code, naics_code);
CREATE INDEX idx_socal_bp_geo_year           ON socal_business_patterns (geo_fips, data_year);
CREATE INDEX idx_socal_bp_naics              ON socal_business_patterns (naics_code);
CREATE INDEX idx_socal_loan_region_year      ON socal_loan_activity (region_code, fiscal_year);
CREATE INDEX idx_socal_loan_geo_year         ON socal_loan_activity (geo_fips, fiscal_year);
CREATE INDEX idx_socal_harvest_log_run       ON socal_harvest_log (run_id);
CREATE INDEX idx_socal_harvest_log_source    ON socal_harvest_log (source_name, fetched_at);
CREATE INDEX idx_socal_harvest_log_status    ON socal_harvest_log (status);

COMMIT;

-- =============================================================================
--  End of schema.sql. NOW apply analytics_views.sql (materialized views +
--  detect_anomalies() + refresh_all_analytics()).
-- =============================================================================
