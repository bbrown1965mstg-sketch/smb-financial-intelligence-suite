-- =============================================================================
--  SMB FINANCIAL & OPERATIONAL DATA WAREHOUSE
--  File   : analytics_views.sql  (materialized analytics layer)
--  Target : PostgreSQL 17+  (pure core; NO extensions required)
--  Apply  : SECOND, after schema.sql
-- =============================================================================
--
--  CONTENTS
--    1. mv_job_type_profitability  -- P&L per (company, service line) + deltas
--    2. mv_labor_cost_analysis     -- per-job labor overrun dashboard
--    3. mv_customer_profitability  -- per-customer economics (incl. zero-job)
--    4. mv_monthly_summary         -- monthly P&L + operational KPIs
--    5. detect_anomalies()         -- inserts anomaly_alerts, returns new rows
--    6. mv_benchmark_comparison    -- internal inflation vs BLS/FRED baselines
--                                      (data loaded by fetch_benchmarks.py)
--    7. mv_socal_rate_guidance     -- SoCal Finance/Tech billable-rate guidance
--                                      (data loaded by harvest_socal_fintech.py;
--                                      reads the SoCal tables added to schema.sql)
--    8. refresh_all_analytics()    -- REFRESH ... CONCURRENTLY all six views
--    9. scoped_* wrapper views     -- 5 REGULAR company-scoped views over the
--                                      5 raw SMB matviews, for the AI-SQL read
--                                      role (migrations/002_add_rls.sql): RLS
--                                      cannot be put ON a materialized view, so
--                                      the app.current_company_id GUC predicate
--                                      is baked into these views instead
--                                      (security_barrier; DEFINER semantics --
--                                      nl2sql_reader gets NO grant on the raw
--                                      matviews, only on these views).
--
--  MULTI-TENANT GRAIN (Fractional CFO deployment)
--    Every CLIENT-data matview is scoped by company_id (the companies
--    registry): mv_monthly_summary is one row per (company_id, month),
--    mv_job_type_profitability per (company_id, job_type),
--    mv_customer_profitability per (company_id, customer),
--    mv_labor_cost_analysis per (company_id, job), and the INTERNAL series of
--    mv_benchmark_comparison per (company_id, period). Whales are ranked
--    WITHIN a workspace's own book and detect_anomalies() loops over
--    companies so expense-spike statistics never mix businesses. MARKET
--    columns (mv_benchmark_comparison's market_wage_*/ppi_* baselines, the
--    job_type benchmark lateral in mv_job_type_profitability, and the whole
--    mv_socal_rate_guidance view) are GLOBAL: market_benchmarks and the SoCal
--    tables carry no company_id by design.
--
--  SOURCE-OF-TRUTH CONVENTIONS
--    * Revenue   : financial_ledger entry_type='revenue' rows. jobs.actual_revenue
--                  is informational only and is NOT summed by the views.
--    * Labor cost: labor_logs (hours * hourly_rate snapshot). Ledger rows with
--                  category='labor' (payroll recharges etc.) are EXCLUDED from
--                  the job-type / customer cost roll-ups to avoid double
--                  counting. mv_monthly_summary is the cash P&L and DOES include
--                  them, so its labor figures may differ from labor_logs by design.
--    * Overhead  : ledger rows with job_id AND customer_id NULL are
--                  unattributable; they appear only in mv_monthly_summary,
--                  never in per-type/per-customer rows.
--    * Customer  : ledger rows with customer_id set (job_id NULL by the
--      entries     schema's exclusive-attribution CHECK) are customer-level
--                  costs (customer acquisition cost, win-backs, credits).
--                  They flow into mv_customer_profitability.acquisition_cost
--                  and its NET margin / is_whale flag; per-type rows and the
--                  monthly cash P&L also include them (via category buckets).
--    * Whales    : mv_customer_profitability.is_whale = revenue >= the 80th
--                  percentile OF THAT WORKSPACE'S book AND gross_margin < 0.
--    * Benchmarks: the LATEST market_benchmarks row per job_type with
--                  region='national' (the table default) is used; benchmarks
--                  are global (market data) and shared by all workspaces.
--
--  USAGE
--    psql "postgresql://postgres:postgres@localhost:5432/smb_warehouse" \
--         -v ON_ERROR_STOP=1 -f analytics_views.sql
--
--    psql "..." -c "SELECT * FROM detect_anomalies();"
--    psql "..." -c "SELECT refresh_all_analytics();"
--
--  Materialized views are snapshots: REFRESH them after data changes. The
--  unique indexes created below are REQUIRED for REFRESH MATERIALIZED VIEW
--  CONCURRENTLY (refresh without blocking readers). refresh_all_analytics()
--  relies on them. Each view is keyed by (company_id +) its entity id
--  (job_type_id / job_id / customer_id / month_start / period) rather than a
--  name, because names are not guaranteed unique except for job_types.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 0) Drop existing objects so the file is idempotent.
-- -----------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mv_job_type_profitability CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_labor_cost_analysis    CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_customer_profitability CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_monthly_summary        CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_benchmark_comparison   CASCADE;  -- added later than the four above; must be dropped for the file to be re-runnable
DROP MATERIALIZED VIEW IF EXISTS mv_socal_rate_guidance    CASCADE;  -- needs the schema.sql SoCal tables to exist
-- The scoped_* wrapper views (section 9) depend on the raw matviews above, so
-- the CASCADE drops would remove them anyway on a re-run; the explicit DROPs
-- keep the intent visible and the file order-independent.
DROP VIEW IF EXISTS scoped_job_type_profitability CASCADE;
DROP VIEW IF EXISTS scoped_labor_cost_analysis    CASCADE;
DROP VIEW IF EXISTS scoped_customer_profitability CASCADE;
DROP VIEW IF EXISTS scoped_monthly_summary        CASCADE;
DROP VIEW IF EXISTS scoped_benchmark_comparison   CASCADE;

-- =============================================================================
-- 1) mv_job_type_profitability -- one row per (company_id, job_type)
-- =============================================================================
-- Revenue/expenses are attributed to a service line THROUGH its jobs:
--   revenue        = ledger 'revenue' rows joined to jobs of the type
--                    (LEDGER IS THE SOURCE OF TRUTH, not jobs.actual_revenue).
--   materials_cost = ledger expenses, category = 'materials'.
--   other_cost     = ledger expenses, category <> 'materials' AND <> 'labor'
--                    (equipment, permits, subcontractor, marketing, ... all
--                    land here). Ledger 'labor' rows are excluded on purpose:
--                    labor cost comes from labor_logs (hours * rate snapshots)
--                    so payroll recharges in the ledger are never double counted.
--   labor_cost     = SUM(hours * hourly_rate) from labor_logs for the type's jobs.
-- Overhead ledger rows (job_id NULL) cannot be attributed to a type and are
-- excluded here; they are captured in mv_monthly_summary instead.
-- No status filter is applied: job_count / avg_job_price cover ALL jobs of the
-- type (quoted, completed, cancelled...), giving a volume view of the catalog.
-- Grain: one row per (company_id, job_type) -- each client workspace prices
-- its own service lines; the market benchmark is GLOBAL (no company_id on
-- market_benchmarks) and shared.
--
-- Benchmark comparison: LEFT JOIN LATERAL picks the most recent
-- market_benchmarks row (max period) for the type restricted to region
-- 'national' (the table default -- stated assumption).
CREATE MATERIALIZED VIEW mv_job_type_profitability AS
WITH ledger_by_type AS (
    SELECT
        j.company_id,
        j.job_type_id,
        COALESCE(SUM(fl.amount) FILTER (WHERE fl.entry_type = 'revenue'), 0) AS revenue,
        COALESCE(SUM(fl.amount) FILTER (WHERE fl.entry_type = 'expense'
                                        AND fl.category = 'materials'), 0)   AS materials_cost,
        COALESCE(SUM(fl.amount) FILTER (WHERE fl.entry_type = 'expense'
                                        AND fl.category NOT IN ('materials', 'labor')), 0) AS other_cost
    FROM financial_ledger fl
    JOIN jobs j ON j.id = fl.job_id
    GROUP BY j.company_id, j.job_type_id
),
labor_by_type AS (
    -- Labor cost computed from labor_logs (hours * rate snapshot), NOT from
    -- ledger 'labor' entries (excluded above) to avoid double counting.
    SELECT
        j.company_id,
        j.job_type_id,
        COALESCE(SUM(ll.hours), 0)                 AS labor_hours,
        COALESCE(SUM(ll.hours * ll.hourly_rate), 0) AS labor_cost
    FROM labor_logs ll
    JOIN jobs j ON j.id = ll.job_id
    GROUP BY j.company_id, j.job_type_id
),
jobs_by_type AS (
    SELECT
        company_id,
        job_type_id,
        COUNT(*)           AS job_count,
        AVG(quoted_price)  AS avg_job_price
    FROM jobs
    GROUP BY company_id, job_type_id
)
SELECT
    jt.company_id               AS company_id,
    jt.id                       AS job_type_id,
    jt.name                     AS job_type,
    COALESCE(lb.revenue, 0)::NUMERIC(14,2)      AS revenue,
    COALESCE(lb.materials_cost, 0)::NUMERIC(14,2) AS materials_cost,
    COALESCE(lb.other_cost, 0)::NUMERIC(14,2)   AS other_cost,
    COALESCE(lt.labor_cost, 0)::NUMERIC(14,2)   AS labor_cost,
    (COALESCE(lb.materials_cost, 0) + COALESCE(lb.other_cost, 0)
     + COALESCE(lt.labor_cost, 0))::NUMERIC(14,2)               AS total_cost,
    (COALESCE(lb.revenue, 0) - COALESCE(lb.materials_cost, 0)
     - COALESCE(lb.other_cost, 0) - COALESCE(lt.labor_cost, 0))::NUMERIC(14,2)
                                                                AS gross_margin,
    ROUND((COALESCE(lb.revenue, 0) - COALESCE(lb.materials_cost, 0)
           - COALESCE(lb.other_cost, 0) - COALESCE(lt.labor_cost, 0))
          / NULLIF(lb.revenue, 0) * 100, 2)::NUMERIC(12,2)      AS margin_pct,
    COALESCE(jb.job_count, 0)                                   AS job_count,
    ROUND(jb.avg_job_price, 2)::NUMERIC(14,2)                   AS avg_job_price,
    COALESCE(lt.labor_hours, 0)::NUMERIC(12,2)                  AS labor_hours,
    bench.period                 AS benchmark_period,
    bench.avg_price              AS benchmark_avg_price,
    bench.avg_margin_pct         AS benchmark_avg_margin_pct,
    ROUND((jb.avg_job_price - bench.avg_price)
          / NULLIF(bench.avg_price, 0) * 100, 2)::NUMERIC(12,2) AS price_vs_benchmark_pct
FROM job_types jt
LEFT JOIN ledger_by_type lb ON lb.company_id = jt.company_id
                           AND lb.job_type_id = jt.id
LEFT JOIN labor_by_type  lt ON lt.company_id = jt.company_id
                           AND lt.job_type_id = jt.id
LEFT JOIN jobs_by_type   jb ON jb.company_id = jt.company_id
                           AND jb.job_type_id = jt.id
LEFT JOIN LATERAL (
    -- Latest 'national' benchmark for this service line (stated assumption:
    -- region='national' is the comparison scope; latest period wins). GLOBAL
    -- market data: shared across all client workspaces.
    SELECT mb.period, mb.avg_price, mb.avg_margin_pct
    FROM market_benchmarks mb
    WHERE mb.job_type_id = jt.id
      AND mb.region = 'national'
    ORDER BY mb.period DESC
    LIMIT 1
) bench ON TRUE
WITH DATA;

CREATE UNIQUE INDEX uq_mv_job_type_profitability
    ON mv_job_type_profitability (company_id, job_type_id);

COMMENT ON MATERIALIZED VIEW mv_job_type_profitability IS
    'P&L per (company_id, service line). Revenue = ledger revenue through jobs (source of truth); labor cost = labor_logs hours*rate; ledger ''labor'' rows excluded to avoid double counting; overhead (job_id NULL) excluded here. Benchmark = latest ''national'' row per type (GLOBAL market data, shared across workspaces).';

-- =============================================================================
-- 2) mv_labor_cost_analysis -- one row per (company_id, job) WITH labor logged
-- =============================================================================
-- The labor-overrun dashboard feed. labor_variance_pct compares actual labor
-- cost (labor_logs) against the job's estimated_labor_cost budget baseline;
-- NULLIF guards divide-by-zero (jobs with NULL/0 estimate yield NULL variance).
-- Grain: one row per (company_id, job_id) -- every logged job belongs to
-- exactly one client workspace via jobs.company_id.
-- ORDER BY is a convenience at creation/refresh time; consumers should still
-- ORDER BY explicitly (heap order is not guaranteed after VACUUM etc.).
CREATE MATERIALIZED VIEW mv_labor_cost_analysis AS
SELECT
    j.company_id                           AS company_id,
    j.id                                   AS job_id,
    j.job_number,
    jt.name                                AS job_type,
    c.name                                 AS customer_name,
    j.status,
    COALESCE(SUM(ll.hours), 0)::NUMERIC(12,2)           AS total_hours,
    COALESCE(SUM(ll.hours * ll.hourly_rate), 0)::NUMERIC(14,2) AS labor_cost,
    j.estimated_labor_cost,
    j.estimated_hours,
    ROUND((COALESCE(SUM(ll.hours * ll.hourly_rate), 0) - j.estimated_labor_cost)
          / NULLIF(j.estimated_labor_cost, 0) * 100, 2)::NUMERIC(12,2) AS labor_variance_pct,
    ROUND(COALESCE(SUM(ll.hours * ll.hourly_rate), 0)
          / NULLIF(SUM(ll.hours), 0), 2)::NUMERIC(14,2)                AS labor_cost_per_hour,
    COUNT(DISTINCT ll.employee_id)                       AS employee_count,
    MIN(ll.work_date)                                    AS first_work_date,
    MAX(ll.work_date)                                    AS last_work_date
FROM jobs j
JOIN labor_logs ll ON ll.job_id = j.id
JOIN job_types jt  ON jt.id = j.job_type_id
JOIN customers c   ON c.id = j.customer_id
GROUP BY j.company_id, j.id, jt.name, c.name
ORDER BY labor_variance_pct DESC NULLS LAST
WITH DATA;

CREATE UNIQUE INDEX uq_mv_labor_cost_analysis
    ON mv_labor_cost_analysis (company_id, job_id);

COMMENT ON MATERIALIZED VIEW mv_labor_cost_analysis IS
    'Per-job labor economics (one row per company_id + job): actual vs estimated labor cost, variance %, blended cost/hour, crew size and date span. Dashboard-ordered by variance (worst first).';

-- =============================================================================
-- 3) mv_customer_profitability -- one row per (company_id, customer), INCLUDING
--    zero-job customers (LEFT JOIN LATERAL keeps every customer row), plus
--    WHALE detection (is_whale) computed WITHIN each workspace's own book.
-- =============================================================================
-- Revenue/costs are allocated to customers THROUGH their jobs, plus
-- customer-level ledger entries (financial_ledger.customer_id, e.g. customer
-- acquisition cost):
--   revenue          = ledger 'revenue' rows joined via jobs.customer_id.
--   job_cost         = ledger expenses (category <> 'labor') + labor_logs cost
--                      for the customer's jobs -- consistent with
--                      mv_job_type_profitability (labor from logs, ledger
--                      'labor' rows excluded to avoid double counting).
--   acquisition_cost = ledger expenses attributed DIRECTLY to the customer
--                      (customer_id set, job_id NULL by the schema's exclusive
--                      attribution CHECK), e.g. marketing/CAC spend.
--   total_cost       = job_cost + acquisition_cost, so margin_pct is a NET
--                      customer margin: it prices in what it cost to win them.
--   days_since_last_job is computed against CURRENT_DATE at REFRESH time, so
--   it is as fresh as the last refresh.
--
-- WHALE FLAG: is_whale = revenue >= 80th percentile of customer revenue WITHIN
-- THE SAME company_id (percentile over that workspace's customer rows,
-- zero-revenue included) AND gross_margin < 0. I.e. "a top-quintile revenue
-- account that LOSES money" -- exactly the client that looks like a crown
-- jewel by top-line size while quietly draining margin (big overruns, heavy
-- discounting, expensive to win). Ranking is per workspace: a mid-size
-- account of one client must never be measured against another client's book.
CREATE MATERIALIZED VIEW mv_customer_profitability AS
WITH cust_agg AS (
    SELECT
        c.company_id                           AS company_id,
        c.id                                   AS customer_id,
        c.name                                 AS customer_name,
        c.is_active,
        agg.revenue,
        agg.job_cost,
        agg.acquisition_cost,
        (agg.job_cost + agg.acquisition_cost)::NUMERIC(14,2)         AS total_cost,
        (agg.revenue - agg.job_cost - agg.acquisition_cost)::NUMERIC(14,2)
                                                                     AS gross_margin,
        agg.job_count,
        agg.avg_job_value,
        agg.first_job_date,
        agg.last_job_date
    FROM customers c
    LEFT JOIN LATERAL (
        SELECT
            -- Ledger revenue attributed through this customer's jobs (overhead
            -- revenue with job_id NULL is unattributable and excluded here).
            (SELECT COALESCE(SUM(fl.amount), 0)::NUMERIC(14,2)
               FROM financial_ledger fl
               JOIN jobs j2 ON j2.id = fl.job_id
              WHERE j2.customer_id = c.id
                AND fl.entry_type = 'revenue')                              AS revenue,
            -- Job-attributed costs: ledger expenses (minus 'labor' rows) +
            -- labor cost from labor_logs.
            ((SELECT COALESCE(SUM(fl.amount), 0)
                 FROM financial_ledger fl
                 JOIN jobs j2 ON j2.id = fl.job_id
                WHERE j2.customer_id = c.id
                  AND fl.entry_type = 'expense'
                  AND fl.category <> 'labor')
             + (SELECT COALESCE(SUM(ll.hours * ll.hourly_rate), 0)
                  FROM labor_logs ll
                  JOIN jobs j2 ON j2.id = ll.job_id
                 WHERE j2.customer_id = c.id))::NUMERIC(14,2)               AS job_cost,
            -- Customer-attributed costs with no job behind them (CAC etc.);
            -- the schema CHECK guarantees job_id IS NULL on these rows.
            (SELECT COALESCE(SUM(fl.amount), 0)::NUMERIC(14,2)
               FROM financial_ledger fl
              WHERE fl.customer_id = c.id
                AND fl.entry_type = 'expense')                              AS acquisition_cost,
            (SELECT COUNT(*)::INTEGER FROM jobs j2
              WHERE j2.customer_id = c.id)                                  AS job_count,
            -- "Job value" = quoted price (reference price; consistent with the
            -- job-type view's avg_job_price).
            (SELECT ROUND(AVG(j2.quoted_price), 2)::NUMERIC(14,2)
               FROM jobs j2 WHERE j2.customer_id = c.id)                    AS avg_job_value,
            (SELECT MIN(j2.job_date) FROM jobs j2 WHERE j2.customer_id = c.id) AS first_job_date,
            (SELECT MAX(j2.job_date) FROM jobs j2 WHERE j2.customer_id = c.id) AS last_job_date
    ) agg ON TRUE
),
whale_threshold AS (
    -- 80th percentile of revenue across ALL customers OF EACH WORKSPACE
    -- (zero-revenue included): "high revenue" = top quintile of that
    -- workspace's book, never a blend of two clients.
    SELECT company_id,
           percentile_cont(0.8) WITHIN GROUP (ORDER BY revenue) AS p80
    FROM cust_agg
    GROUP BY company_id
)
SELECT
    ca.company_id,
    ca.customer_id,
    ca.customer_name,
    ca.is_active,
    ca.revenue,
    ca.job_cost,
    ca.acquisition_cost,
    ca.total_cost,
    ca.gross_margin,
    ROUND(ca.gross_margin / NULLIF(ca.revenue, 0) * 100, 2)::NUMERIC(12,2)  AS margin_pct,
    ca.job_count,
    ca.avg_job_value,
    ca.first_job_date,
    ca.last_job_date,
    (CURRENT_DATE - ca.last_job_date)::INTEGER                              AS days_since_last_job,
    (ca.revenue >= wt.p80 AND ca.gross_margin < 0)                          AS is_whale
FROM cust_agg ca
-- Join each workspace's customer rows to ITS OWN threshold row (NOT a bare
-- CROSS JOIN): whale_threshold holds one row PER company_id, so a cross join
-- multiplies every customer by every workspace's p80 -- duplicated
-- (company_id, customer_id) rows that break REFRESH MATERIALIZED VIEW
-- CONCURRENTLY and would rank each client against another client's book as
-- soon as a second workspace exists. (With exactly one company the cross
-- join happened to be correct, which is why the defect stayed latent.)
JOIN whale_threshold wt ON wt.company_id = ca.company_id
WITH DATA;

CREATE UNIQUE INDEX uq_mv_customer_profitability
    ON mv_customer_profitability (company_id, customer_id);

COMMENT ON MATERIALIZED VIEW mv_customer_profitability IS
    'Per-customer economics incl. zero-job customers (LEFT JOIN LATERAL), one row per (company_id, customer). Revenue = ledger through jobs; job_cost = ledger expenses (excl. ''labor'') + labor_logs cost; acquisition_cost = customer-attributed ledger expenses (CAC); total_cost = job_cost + acquisition_cost, so margin_pct is NET of acquisition. is_whale = revenue >= the 80th percentile OF THE WORKSPACE''S OWN book AND gross_margin < 0 (top-quintile revenue account losing money). days_since_last_job is relative to CURRENT_DATE at refresh time.';

-- =============================================================================
-- 4) mv_monthly_summary -- one row per (company_id, calendar month) with
--    activity in THAT workspace
-- =============================================================================
-- This is the cash/ledger P&L (everything in the ledger counts, including
-- overhead rows with job_id NULL), pivoted with FILTER clauses into major
-- expense buckets: materials, labor (ledger), equipment, other. Labor HOURS
-- and the operational labor_cost come from labor_logs for the same month;
-- gross_profit is computed from ledger totals (labor_expense_ledger included),
-- so gross_profit and labor_cost answer different questions by design:
--   * total_expense / gross_profit  = "what the books recorded that month"
--   * labor_cost (logs)             = "what field labor actually cost that month"
-- MULTI-TENANT GRAIN: every source is grouped per (company_id, month), so the
-- month list of one client workspace is derived ONLY from that workspace's
-- own activity (ledger entries, labor logs, job completions, new customers) --
-- months with, say, only a signup are not silently dropped, and one client's
-- quiet month never appears because another client was busy. Months with no
-- activity of any kind for a workspace do not appear for it.
CREATE MATERIALIZED VIEW mv_monthly_summary AS
WITH ledger_monthly AS (
    SELECT
        company_id,
        date_trunc('month', entry_date)::DATE AS month_start,
        SUM(amount) FILTER (WHERE entry_type = 'revenue')             AS revenue,
        SUM(amount) FILTER (WHERE entry_type = 'expense'
                             AND category = 'materials')              AS materials_expense,
        SUM(amount) FILTER (WHERE entry_type = 'expense'
                             AND category = 'labor')                  AS labor_expense_ledger,
        SUM(amount) FILTER (WHERE entry_type = 'expense'
                             AND category = 'equipment')              AS equipment_expense,
        SUM(amount) FILTER (WHERE entry_type = 'expense'
                             AND category NOT IN ('materials', 'labor', 'equipment')) AS other_expense,
        SUM(amount) FILTER (WHERE entry_type = 'expense')             AS total_expense
    FROM financial_ledger
    GROUP BY company_id, date_trunc('month', entry_date)
),
labor_monthly AS (
    SELECT
        company_id,
        date_trunc('month', work_date)::DATE          AS month_start,
        SUM(hours)                                    AS labor_hours,
        SUM(hours * hourly_rate)                      AS labor_cost
    FROM labor_logs
    GROUP BY company_id, date_trunc('month', work_date)
),
completions_monthly AS (
    -- Jobs COMPLETED that month (completion_date in month AND status='completed',
    -- so a job whose completion_date survives a later status change is not counted).
    SELECT
        company_id,
        date_trunc('month', completion_date)::DATE    AS month_start,
        COUNT(*)                                      AS job_completions
    FROM jobs
    WHERE completion_date IS NOT NULL
      AND status = 'completed'
    GROUP BY company_id, date_trunc('month', completion_date)
),
new_customers_monthly AS (
    SELECT
        company_id,
        date_trunc('month', customer_since)::DATE     AS month_start,
        COUNT(*)                                      AS new_customers
    FROM customers
    WHERE customer_since IS NOT NULL
    GROUP BY company_id, date_trunc('month', customer_since)
),
months AS (
    SELECT company_id, month_start FROM ledger_monthly
    UNION
    SELECT company_id, month_start FROM labor_monthly
    UNION
    SELECT company_id, month_start FROM completions_monthly
    UNION
    SELECT company_id, month_start FROM new_customers_monthly
)
SELECT
    m.company_id,
    m.month_start,
    EXTRACT(YEAR  FROM m.month_start)::INTEGER    AS year,
    TO_CHAR(m.month_start, 'FMMonth')             AS month_name,
    COALESCE(lm.revenue, 0)::NUMERIC(14,2)        AS revenue,
    COALESCE(lm.materials_expense, 0)::NUMERIC(14,2)     AS materials_expense,
    COALESCE(lm.labor_expense_ledger, 0)::NUMERIC(14,2)  AS labor_expense_ledger,
    COALESCE(lm.equipment_expense, 0)::NUMERIC(14,2)     AS equipment_expense,
    COALESCE(lm.other_expense, 0)::NUMERIC(14,2)         AS other_expense,
    COALESCE(lm.total_expense, 0)::NUMERIC(14,2)         AS total_expense,
    COALESCE(lo.labor_hours, 0)::NUMERIC(12,2)    AS labor_hours,
    COALESCE(lo.labor_cost, 0)::NUMERIC(14,2)     AS labor_cost,
    (COALESCE(lm.revenue, 0) - COALESCE(lm.total_expense, 0))::NUMERIC(14,2) AS gross_profit,
    ROUND((COALESCE(lm.revenue, 0) - COALESCE(lm.total_expense, 0))
          / NULLIF(lm.revenue, 0) * 100, 2)::NUMERIC(12,2)                   AS gross_margin_pct,
    COALESCE(cm.job_completions, 0)::INTEGER      AS job_completions,
    COALESCE(nc.new_customers, 0)::INTEGER        AS new_customers
FROM months m
LEFT JOIN ledger_monthly        lm ON lm.company_id  = m.company_id
                                  AND lm.month_start = m.month_start
LEFT JOIN labor_monthly         lo ON lo.company_id  = m.company_id
                                  AND lo.month_start = m.month_start
LEFT JOIN completions_monthly   cm ON cm.company_id  = m.company_id
                                  AND cm.month_start = m.month_start
LEFT JOIN new_customers_monthly nc ON nc.company_id  = m.company_id
                                  AND nc.month_start = m.month_start
ORDER BY m.company_id, m.month_start
WITH DATA;

CREATE UNIQUE INDEX uq_mv_monthly_summary
    ON mv_monthly_summary (company_id, month_start);

COMMENT ON MATERIALIZED VIEW mv_monthly_summary IS
    'Monthly P&L per client workspace (one row per company_id + month; all ledger rows incl. overhead) plus operational KPIs from labor_logs. The month grid comes from the workspace''s OWN activity only, so two clients never share or shadow months. gross_profit uses LEDGER expenses (labor_expense_ledger included); labor_cost/labor_hours come from labor_logs -- the two may differ by design (posting timing vs field hours).';

-- =============================================================================
-- 5) detect_anomalies() -- PER-COMPANY (multi-tenant)
-- =============================================================================
-- Scans the warehouse and INSERTs new open alerts into anomaly_alerts.
-- Returns the rows it actually inserted (empty result = nothing new).
--
--  MULTI-TENANT EXECUTION MODEL: the function LOOPs over companies (one
--  iteration per client workspace) and runs the SAME two detections inside
--  each iteration, scoped to that workspace's rows via company_id. This is
--  what keeps the statistics honest: an expense spike is measured against
--  THAT workspace's prior-6-month mean/stddev, never a blend of two
--  businesses, and each client's alerts carry their own company_id. A
--  workspace with no data simply contributes zero iterations of work.
--
--  a) labor_overrun -- completed/in_progress jobs WITH labor logs where actual
--     labor cost (SUM(hours * hourly_rate) from labor_logs) exceeds
--     estimated_labor_cost by more than 20%. Jobs with NULL/zero estimate are
--     skipped. severity: >50% over = high, >30% = medium, else low.
--     metric_value = actual cost; baseline_value = estimate;
--     deviation_pct = overrun percent; z_score = NULL.
--
--  b) expense_spike -- per ledger expense category per month WITHIN the
--     workspace: the month total exceeds the prior-6-month rolling MEAN by
--     more than 2 x STDDEV_SAMP of those months (i.e. z-score > 2, "2 standard
--     deviations above average").
--     Approach (stated): a zero-filled category x month grid (so absent months
--     count as 0 and the frame is exactly the prior 6 calendar months) + a
--     ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING window over month order.
--     Requires >= 3 prior months AND stddev > 0 (a constant history cannot
--     yield a meaningful z-score, so such months are not flagged).
--     z_score = (current - mean) / stddev; severity: z > 3 high, z > 2.5
--     medium, else low. metric_value = month total; baseline_value = prior mean.
--
--  Idempotency / concurrency
--    * Inserts are guarded twice: (1) a NOT EXISTS anti-join against open
--      alerts (avoids churn on re-runs) and (2) ON CONFLICT ... DO NOTHING
--      against the PARTIAL UNIQUE indexes from schema.sql (guards two
--      concurrent calls). The ON CONFLICT WHERE clause must match the partial
--      index predicates for index inference to work; the conflict targets now
--      include company_id, so two workspaces never suppress each other's
--      alerts.
--    * Each company iteration runs as ONE statement (a single WITH query
--      inside RETURN QUERY) -> one consistent snapshot per workspace;
--      RETURNING yields exactly the inserted rows across all iterations.
--    * z_score / deviation_pct are clamped to their column ranges
--      (NUMERIC(6,2) / NUMERIC(8,2)) so extreme outliers cannot abort the run.
--
--  Output columns are prefixed out_ to avoid plpgsql name ambiguity between
--  RETURNS TABLE parameters and identically-named table columns.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION detect_anomalies()
RETURNS TABLE (
    out_alert_id      BIGINT,
    out_anomaly_type  TEXT,
    out_subject       TEXT,      -- job_number or 'category - Mon YYYY'
    out_severity      TEXT,
    out_metric_value  NUMERIC,
    out_baseline_value NUMERIC,
    out_deviation_pct NUMERIC,
    out_z_score       NUMERIC,
    out_details       JSONB
)
LANGUAGE plpgsql
VOLATILE
SET search_path = public
AS $$
DECLARE
    v_company BIGINT;
BEGIN
    -- One pass per client workspace: every statistic below is computed from
    -- THAT company's rows only (company_id = v_company in every source).
    FOR v_company IN SELECT company_id FROM companies ORDER BY company_id LOOP
        RETURN QUERY
        WITH
        -- ------------------------------------------------------------------
        -- (a) labor_overrun (this workspace's jobs + labor logs)
        -- ------------------------------------------------------------------
        labor_actual AS (
            SELECT
                j.id            AS job_id,
                j.job_number,
                j.estimated_labor_cost,
                SUM(ll.hours * ll.hourly_rate) AS actual_labor_cost
            FROM jobs j
            JOIN labor_logs ll ON ll.job_id = j.id
                              AND ll.company_id = v_company
            WHERE j.company_id = v_company
              AND j.status IN ('completed', 'in_progress')
              AND j.estimated_labor_cost IS NOT NULL
              AND j.estimated_labor_cost > 0      -- skip NULL/0-estimate jobs
            GROUP BY j.id, j.job_number, j.estimated_labor_cost
        ),
        overrun_candidates AS (
            SELECT
                la.job_id,
                la.job_number,
                la.estimated_labor_cost,
                la.actual_labor_cost,
                ROUND(LEAST((la.actual_labor_cost - la.estimated_labor_cost)
                            / la.estimated_labor_cost * 100, 999999.99), 2) AS deviation_pct,
                CASE
                    WHEN (la.actual_labor_cost - la.estimated_labor_cost)
                         / la.estimated_labor_cost * 100 > 50 THEN 'high'
                    WHEN (la.actual_labor_cost - la.estimated_labor_cost)
                         / la.estimated_labor_cost * 100 > 30 THEN 'medium'
                    ELSE 'low'
                END AS sev
            FROM labor_actual la
            WHERE la.actual_labor_cost > la.estimated_labor_cost * 1.2   -- >20% overrun
        ),
        ins_overrun AS (
            INSERT INTO anomaly_alerts
                  (anomaly_type, company_id, job_id, severity, metric_value,
                   baseline_value, deviation_pct, z_score, details)
            SELECT
                'labor_overrun',
                v_company,
                oc.job_id,
                oc.sev,
                oc.actual_labor_cost::NUMERIC(14,2),
                oc.estimated_labor_cost::NUMERIC(14,2),
                oc.deviation_pct,
                NULL::NUMERIC(6,2),
                JSONB_BUILD_OBJECT(
                    'job_number',           oc.job_number,
                    'actual_labor_cost',    oc.actual_labor_cost,
                    'estimated_labor_cost', oc.estimated_labor_cost,
                    'overrun_pct',          oc.deviation_pct,
                    'threshold_pct',        20
                )
            FROM overrun_candidates oc
            WHERE NOT EXISTS (          -- belt: skip already-open alerts
                SELECT 1
                FROM anomaly_alerts a
                WHERE a.anomaly_type = 'labor_overrun'
                  AND a.company_id = v_company
                  AND a.job_id = oc.job_id
                  AND a.is_resolved = FALSE
            )
            ON CONFLICT (anomaly_type, company_id, job_id)   -- race-safe vs partial index
                WHERE anomaly_type = 'labor_overrun' AND NOT is_resolved
            DO NOTHING
            RETURNING id, job_id, anomaly_type, category, period, severity,
                      metric_value, baseline_value, deviation_pct, z_score, details
        ),

        -- ------------------------------------------------------------------
        -- (b) expense_spike (this workspace's ledger only)
        -- ------------------------------------------------------------------
        month_bounds AS (
            SELECT
                date_trunc('month', MIN(entry_date))::DATE AS min_month,
                date_trunc('month', MAX(entry_date))::DATE AS max_month
            FROM financial_ledger
            WHERE entry_type = 'expense'
              AND company_id = v_company
        ),
        categories AS (
            SELECT DISTINCT category
            FROM financial_ledger
            WHERE entry_type = 'expense'
              AND company_id = v_company
        ),
        monthly_totals AS (
            SELECT
                category,
                date_trunc('month', entry_date)::DATE AS month_start,
                SUM(amount) AS month_total
            FROM financial_ledger
            WHERE entry_type = 'expense'
              AND company_id = v_company
            GROUP BY 1, 2
        ),
        zero_filled AS (
            -- Zero-fill every category across every month with any expense
            -- activity OF THIS WORKSPACE, so the rolling window below is
            -- exactly the prior 6 CALENDAR months even when a category has no
            -- rows in a month (absent = 0, not missing).
            SELECT
                cat.category,
                gs.month_ts,
                COALESCE(mt.month_total, 0) AS month_total
            FROM categories cat
            CROSS JOIN generate_series(
                (SELECT min_month FROM month_bounds),
                (SELECT max_month FROM month_bounds),
                INTERVAL '1 month'
            ) AS gs(month_ts)
            LEFT JOIN monthly_totals mt
                   ON mt.category = cat.category
                  AND mt.month_start = gs.month_ts::DATE
        ),
        spike_stats AS (
            SELECT
                category,
                month_ts::DATE             AS month_start,
                month_total,
                COUNT(*)         OVER w    AS prior_months,
                AVG(month_total) OVER w    AS prior_mean,
                STDDEV_SAMP(month_total) OVER w AS prior_stddev
            FROM zero_filled
            WINDOW w AS (
                PARTITION BY category
                ORDER BY month_ts
                ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING   -- prior 6 complete months
            )
        ),
        spike_candidates AS (
            SELECT
                ss.category,
                ss.month_start,
                ss.month_total,
                ROUND(ss.prior_mean, 2)    AS prior_mean_r,
                ROUND(ss.prior_stddev, 2)  AS prior_stddev_r,
                ss.prior_months,
                ROUND(GREATEST(LEAST((ss.month_total - ss.prior_mean) / ss.prior_stddev,
                                     9999.99), -9999.99), 2)                AS z_score,
                ROUND(LEAST((ss.month_total - ss.prior_mean)
                            / NULLIF(ss.prior_mean, 0) * 100, 999999.99), 2) AS deviation_pct,
                CASE
                    WHEN (ss.month_total - ss.prior_mean) / ss.prior_stddev > 3   THEN 'high'
                    WHEN (ss.month_total - ss.prior_mean) / ss.prior_stddev > 2.5 THEN 'medium'
                    ELSE 'low'
                END AS sev
            FROM spike_stats ss
            WHERE ss.prior_months >= 3            -- need >= 3 prior months for a valid stddev
              AND ss.prior_stddev IS NOT NULL     -- (needs >= 2; we demand more)
              AND ss.prior_stddev > 0             -- constant history => no meaningful z
              AND (ss.month_total - ss.prior_mean) > 2 * ss.prior_stddev  -- z > 2: ABOVE the rolling mean, not merely above zero
        ),
        ins_spike AS (
            INSERT INTO anomaly_alerts
                  (anomaly_type, company_id, category, period, severity,
                   metric_value, baseline_value, deviation_pct, z_score, details)
            SELECT
                'expense_spike',
                v_company,
                sc.category,
                sc.month_start,
                sc.sev,
                sc.month_total::NUMERIC(14,2),
                sc.prior_mean_r::NUMERIC(14,2),
                sc.deviation_pct,
                sc.z_score,
                JSONB_BUILD_OBJECT(
                    'category',               sc.category,
                    'period',                 TO_CHAR(sc.month_start, 'YYYY-MM'),
                    'month_total',            sc.month_total,
                    'prior_6m_mean',          sc.prior_mean_r,
                    'prior_6m_stddev',        sc.prior_stddev_r,
                    'prior_months_counted',   sc.prior_months,
                    'spike_threshold_multiple', 2
                )
            FROM spike_candidates sc
            WHERE NOT EXISTS (          -- belt: skip already-open alerts
                SELECT 1
                FROM anomaly_alerts a
                WHERE a.anomaly_type = 'expense_spike'
                  AND a.company_id = v_company
                  AND a.category = sc.category
                  AND a.period = sc.month_start
                  AND a.is_resolved = FALSE
            )
            ON CONFLICT (anomaly_type, company_id, category, period)  -- race-safe vs partial index
                WHERE anomaly_type = 'expense_spike' AND NOT is_resolved
            DO NOTHING
            RETURNING id, job_id, anomaly_type, category, period, severity,
                      metric_value, baseline_value, deviation_pct, z_score, details
        )

        -- ------------------------------------------------------------------
        -- Return exactly the rows this iteration inserted.
        -- ------------------------------------------------------------------
        SELECT
            io.id            AS out_alert_id,
            io.anomaly_type  AS out_anomaly_type,
            j.job_number     AS out_subject,
            io.severity      AS out_severity,
            io.metric_value  AS out_metric_value,
            io.baseline_value AS out_baseline_value,
            io.deviation_pct AS out_deviation_pct,
            io.z_score       AS out_z_score,
            io.details       AS out_details
        FROM ins_overrun io
        JOIN jobs j ON j.id = io.job_id
        UNION ALL
        SELECT
            isp.id,
            isp.anomaly_type,
            isp.category || ' - ' || TO_CHAR(isp.period, 'Mon YYYY'),
            isp.severity,
            isp.metric_value,
            isp.baseline_value,
            isp.deviation_pct,
            isp.z_score,
            isp.details
        FROM ins_spike isp;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION detect_anomalies() IS
    'Detects labor overruns (>20% over estimate on completed/in_progress jobs) and expense spikes (month total > 2 standard deviations ABOVE the prior-6-month mean, i.e. z > 2, >= 3 prior months, stddev > 0), PER CLIENT WORKSPACE: a LOOP over companies scopes every statistic to that workspace''s rows so expense-spike statistics never mix businesses, and each inserted alert carries its company_id. Inserts open alerts into anomaly_alerts (idempotent via NOT EXISTS + ON CONFLICT against the per-company partial unique indexes) and returns the newly inserted rows.';

-- =============================================================================
-- 6) mv_benchmark_comparison -- internal inflation vs real-world baselines
-- =============================================================================
-- One row per (company_id, period): the INTERNAL series (left columns) are
-- PER WORKSPACE -- each client's blended labor rate and materials spend are
-- computed from that workspace's own labor_logs / financial_ledger rows via
-- company_id -- while the MARKET baselines (right columns) are GLOBAL: they
-- come from market_benchmarks, which carries no company_id by design and is
-- shared across all client workspaces. Months are derived from internal
-- activity only (so years of pre-business market history are not dumped into
-- the view), per workspace: a workspace's row exists only for months where
-- THAT workspace had activity.
-- Internal columns, computed at refresh time:
--   internal_labor_rate        blended $/hr actually paid (hours-weighted)
--   internal_materials_per_job average materials spend per job with materials
-- Market columns = baselines loaded by fetch_benchmarks.py into
-- market_benchmarks (benchmark_kind 'wage' / 'ppi'; BLS data served via
-- FRED's public CSV endpoint). All *_yoy_pct columns compare against the
-- same calendar month one year earlier (self-join at period - 12 months).
-- The three gap columns are (our YoY - market YoY): POSITIVE means we are
-- inflating FASTER than the market baseline -- the margin-squeeze signal.
-- If a series is missing for a month (fetch lag, holiday no-release), the
-- columns for it are NULL rather than stale -- deliberate.
--
-- Series pinned here (edit BOTH this view and fetch_benchmarks.py together):
--   market_wage_national  CES2000000003        BLS CES avg hourly earnings,
--                                              construction, all employees
--   market_wage_regional  SMU29000002000000003 BLS CES, construction AHE,
--                                              Missouri (state = 'us-mo')
--   ppi_lumber            WPU0822              softwood lumber PPI
--   ppi_paint             PCU325510325510      paint & coating mfg PPI
--   ppi_building_materials WPUSI012011         construction materials PPI
CREATE MATERIALIZED VIEW mv_benchmark_comparison AS
WITH internal_labor AS (
    SELECT company_id,
           date_trunc('month', work_date)::DATE AS period,
           ROUND(SUM(hours * hourly_rate) / NULLIF(SUM(hours), 0), 2)::NUMERIC(14,2) AS blended_rate,
           SUM(hours)                            AS hours
    FROM labor_logs
    GROUP BY company_id, date_trunc('month', work_date)
),
internal_materials AS (
    SELECT company_id,
           date_trunc('month', entry_date)::DATE AS period,
           ROUND(SUM(amount) / NULLIF(COUNT(DISTINCT job_id), 0), 2)::NUMERIC(14,2) AS materials_per_job
    FROM financial_ledger
    WHERE entry_type = 'expense'
      AND category = 'materials'
    GROUP BY company_id, date_trunc('month', entry_date)
),
macro AS (
    -- GLOBAL market baselines: no company dimension by design.
    SELECT period, region, series_id, avg_price
    FROM market_benchmarks
    WHERE benchmark_kind IN ('wage', 'ppi')
),
months AS (
    SELECT company_id, period FROM internal_labor
    UNION
    SELECT company_id, period FROM internal_materials
)
SELECT
    m.company_id,
    m.period,
    EXTRACT(YEAR FROM m.period)::INTEGER          AS year,
    TO_CHAR(m.period, 'FMMon YYYY')               AS month_name,
    -- our numbers (per workspace)
    il.blended_rate                               AS internal_labor_rate,
    il.hours                                      AS internal_labor_hours,
    ROUND(((il.blended_rate  / NULLIF(il12.blended_rate, 0)) - 1) * 100, 2)::NUMERIC(6,2)
                                                  AS internal_labor_yoy_pct,
    im.materials_per_job                          AS internal_materials_per_job,
    ROUND(((im.materials_per_job / NULLIF(im12.materials_per_job, 0)) - 1) * 100, 2)::NUMERIC(6,2)
                                                  AS internal_materials_yoy_pct,
    -- market baselines (levels + YoY; GLOBAL, identical for every workspace)
    wn.avg_price                                  AS market_wage_national,
    ROUND(((wn.avg_price / NULLIF(wn12.avg_price, 0)) - 1) * 100, 2)::NUMERIC(6,2)
                                                  AS market_wage_national_yoy_pct,
    wr.avg_price                                  AS market_wage_regional,
    ROUND(((wr.avg_price / NULLIF(wr12.avg_price, 0)) - 1) * 100, 2)::NUMERIC(6,2)
                                                  AS market_wage_regional_yoy_pct,
    pl.avg_price                                  AS ppi_lumber,
    ROUND(((pl.avg_price / NULLIF(pl12.avg_price, 0)) - 1) * 100, 2)::NUMERIC(6,2)
                                                  AS ppi_lumber_yoy_pct,
    pp.avg_price                                  AS ppi_paint,
    ROUND(((pp.avg_price / NULLIF(pp12.avg_price, 0)) - 1) * 100, 2)::NUMERIC(6,2)
                                                  AS ppi_paint_yoy_pct,
    pb.avg_price                                  AS ppi_building_materials,
    ROUND(((pb.avg_price / NULLIF(pb12.avg_price, 0)) - 1) * 100, 2)::NUMERIC(6,2)
                                                  AS ppi_building_materials_yoy_pct,
    -- gaps: (our YoY - market YoY); positive = we inflate faster
    ROUND(((il.blended_rate  / NULLIF(il12.blended_rate, 0))
         - (wn.avg_price / NULLIF(wn12.avg_price, 0))) * 100, 2)::NUMERIC(8,2)
                                                  AS labor_inflation_gap_national_pct,
    ROUND(((il.blended_rate  / NULLIF(il12.blended_rate, 0))
         - (wr.avg_price / NULLIF(wr12.avg_price, 0))) * 100, 2)::NUMERIC(8,2)
                                                  AS labor_inflation_gap_regional_pct,
    ROUND(((im.materials_per_job / NULLIF(im12.materials_per_job, 0))
         - (pb.avg_price / NULLIF(pb12.avg_price, 0))) * 100, 2)::NUMERIC(8,2)
                                                  AS materials_inflation_gap_pct
FROM months m
LEFT JOIN internal_labor     il   ON il.company_id = m.company_id
                                 AND il.period   = m.period
LEFT JOIN internal_labor     il12 ON il12.company_id = m.company_id
                                 AND il12.period = (m.period - INTERVAL '12 months')::DATE
LEFT JOIN internal_materials im   ON im.company_id = m.company_id
                                 AND im.period   = m.period
LEFT JOIN internal_materials im12 ON im12.company_id = m.company_id
                                 AND im12.period = (m.period - INTERVAL '12 months')::DATE
LEFT JOIN macro wn   ON wn.series_id   = 'CES2000000003'         AND wn.region   = 'us-national'
                        AND wn.period   = m.period
LEFT JOIN macro wn12 ON wn12.series_id = 'CES2000000003'         AND wn12.region = 'us-national'
                        AND wn12.period = (m.period - INTERVAL '12 months')::DATE
LEFT JOIN macro wr   ON wr.series_id   = 'SMU29000002000000003'  AND wr.region   = 'us-mo'
                        AND wr.period   = m.period
LEFT JOIN macro wr12 ON wr12.series_id = 'SMU29000002000000003'  AND wr12.region = 'us-mo'
                        AND wr12.period = (m.period - INTERVAL '12 months')::DATE
LEFT JOIN macro pl   ON pl.series_id   = 'WPU0822'               AND pl.region   = 'us-national'
                        AND pl.period   = m.period
LEFT JOIN macro pl12 ON pl12.series_id = 'WPU0822'               AND pl12.region = 'us-national'
                        AND pl12.period = (m.period - INTERVAL '12 months')::DATE
LEFT JOIN macro pp   ON pp.series_id   = 'PCU325510325510'       AND pp.region   = 'us-national'
                        AND pp.period   = m.period
LEFT JOIN macro pp12 ON pp12.series_id = 'PCU325510325510'       AND pp12.region = 'us-national'
                        AND pp12.period = (m.period - INTERVAL '12 months')::DATE
LEFT JOIN macro pb   ON pb.series_id   = 'WPUSI012011'           AND pb.region   = 'us-national'
                        AND pb.period   = m.period
LEFT JOIN macro pb12 ON pb12.series_id = 'WPUSI012011'           AND pb12.region = 'us-national'
                        AND pb12.period = (m.period - INTERVAL '12 months')::DATE
ORDER BY m.company_id, m.period
WITH DATA;

CREATE UNIQUE INDEX uq_mv_benchmark_comparison
    ON mv_benchmark_comparison (company_id, period);

COMMENT ON MATERIALIZED VIEW mv_benchmark_comparison IS
    'Monthly comparison of internal inflation (blended labor $/hr from labor_logs; materials spend per job from the ledger, both YoY -- PER WORKSPACE via company_id) against GLOBAL real market baselines loaded by fetch_benchmarks.py: BLS construction wage rates (national + Missouri, via FRED) and FRED PPIs (softwood lumber, paint & coatings, construction materials). Gap columns = our YoY minus market YoY; positive = margin squeeze.';

-- =============================================================================
-- 7) mv_socal_rate_guidance -- SoCal Finance/Tech billable-rate guidance
-- =============================================================================
-- ONE row per (region_code, vertical_key): 6 regions x 6 verticals = 36 rows
-- when the profile table is fully populated (rows still appear if the wage
-- join misses -- the rate columns are simply NULL). Joins:
--   regional_industry_profiles  (MODELED vertical economics; written by
--                                harvest_socal_fintech.py --sources profiles)
--   x socal_wage_benchmarks     (REAL harvested OEWS wages) on
--     (region_code, anchor_occupation_code) at the LATEST data_year for that
--     region+occupation pair (MAX(data_year) per key in the latest_wage CTE).
--
-- RATE FORMULA (exactly; every stage is computed from UNROUNDED upstream
-- values -- only the output columns are ROUND(..., 2)):
--   hourly_cost             = annual_median_salary / 2080.0
--                             * (1 + locality_burden_pct / 100)
--     2080 = 40 paid hours x 52 weeks, the standard full-time year used to
--     convert an OEWS annual salary into an hourly wage cost. The locality
--     burden raises (or lowers) that cost vs the California baseline for
--     workers-comp / insurance / locality premiums (MODELED input).
--   loaded_cost             = hourly_cost / (utilization_target_pct / 100)
--     a consultant/employee is paid for ALL hours but only bills
--     utilization_target% of them (bench time, admin, sales eat the rest),
--     so the billable hours must carry the whole wage cost.
--   suggested_billable_rate = loaded_cost / (1 - gross_margin_pct / 100)
--     price so that the vertical's target gross margin holds AT target
--     utilization.
--
-- COMPARISON COLUMNS: statewide_billable_rate / la_billable_rate are the SAME
-- vertical's suggested_billable_rate for region_code 'california_state' /
-- 'la_msa'; rate_vs_statewide_pct / rate_vs_la_pct =
-- (this region's rate / reference rate - 1) * 100, i.e. a PERCENT delta
-- (positive = this region's guidance is pricier than the reference region;
-- NULL when either rate is NULL or the reference is 0).
CREATE MATERIALIZED VIEW mv_socal_rate_guidance AS
WITH latest_wage AS (
    -- newest harvested wage year per (region, occupation): the join key
    SELECT region_code, occupation_code, MAX(data_year) AS data_year
    FROM socal_wage_benchmarks
    GROUP BY region_code, occupation_code
),
guidance AS (
    SELECT
        p.region_code,
        p.vertical_key,
        p.vertical_name,
        lw.data_year,
        p.anchor_occupation_code,
        w.role_label,
        w.annual_median                            AS annual_median_salary,
        p.locality_burden_pct,
        p.utilization_target_pct,
        p.gross_margin_pct,
        ROUND(w.annual_median / 2080.0
              * (1 + p.locality_burden_pct / 100.0), 2)::NUMERIC(14,2)
                                                   AS hourly_cost,
        ROUND(w.annual_median / 2080.0
              * (1 + p.locality_burden_pct / 100.0)
              / (p.utilization_target_pct / 100.0), 2)::NUMERIC(14,2)
                                                   AS loaded_cost,
        ROUND(w.annual_median / 2080.0
              * (1 + p.locality_burden_pct / 100.0)
              / (p.utilization_target_pct / 100.0)
              / (1 - p.gross_margin_pct / 100.0), 2)::NUMERIC(14,2)
                                                   AS suggested_billable_rate,
        p.cac_usd,
        p.payroll_to_revenue_ratio
    FROM regional_industry_profiles p
    LEFT JOIN latest_wage lw
           ON lw.region_code     = p.region_code
          AND lw.occupation_code = p.anchor_occupation_code
    LEFT JOIN socal_wage_benchmarks w
           ON w.region_code     = lw.region_code
          AND w.occupation_code = lw.occupation_code
          AND w.data_year       = lw.data_year
)
SELECT
    g.region_code,
    g.vertical_key,
    g.vertical_name,
    g.data_year,
    g.anchor_occupation_code,
    g.role_label,
    g.annual_median_salary,
    g.locality_burden_pct,
    g.utilization_target_pct,
    g.gross_margin_pct,
    g.hourly_cost,
    g.loaded_cost,
    g.suggested_billable_rate,
    g.cac_usd,
    g.payroll_to_revenue_ratio,
    st.suggested_billable_rate                     AS statewide_billable_rate,
    ROUND((g.suggested_billable_rate
           / NULLIF(st.suggested_billable_rate, 0) - 1) * 100, 2)::NUMERIC(8,2)
                                                   AS rate_vs_statewide_pct,
    la.suggested_billable_rate                     AS la_billable_rate,
    ROUND((g.suggested_billable_rate
           / NULLIF(la.suggested_billable_rate, 0) - 1) * 100, 2)::NUMERIC(8,2)
                                                   AS rate_vs_la_pct
FROM guidance g
LEFT JOIN guidance st ON st.vertical_key = g.vertical_key
                     AND st.region_code  = 'california_state'
LEFT JOIN guidance la ON la.vertical_key = g.vertical_key
                     AND la.region_code  = 'la_msa'
ORDER BY g.region_code, g.vertical_key
WITH DATA;

CREATE UNIQUE INDEX uq_mv_socal_rate_guidance
    ON mv_socal_rate_guidance (region_code, vertical_key);

COMMENT ON MATERIALIZED VIEW mv_socal_rate_guidance IS
    'Billable-rate guidance per SoCal region x Finance/Tech vertical (36 rows): joins the MODELED regional_industry_profiles with the REAL harvested OEWS wages (latest data_year) into hourly_cost = salary/2080 * (1 + locality_burden), loaded_cost = hourly_cost / utilization_target, suggested_billable_rate = loaded_cost / (1 - gross_margin). Comparison columns show the delta of the suggested rate vs the same vertical''s california_state and la_msa guidance (percent). Refreshed by refresh_all_analytics().';

-- =============================================================================
-- 8) refresh_all_analytics() -- REFRESH-friendly helper
-- =============================================================================
-- Refreshes all six materialized views with CONCURRENTLY (non-blocking for
-- readers). CONCURRENTLY requires the unique indexes created above -- they are
-- part of this file on purpose. The views are populated at creation (WITH
-- DATA), so CONCURRENTLY is always valid here. View order does not matter
-- (no view reads another); monthly summary is refreshed last because
-- dashboards typically join it last. Safe to call anytime, e.g. nightly.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION refresh_all_analytics()
RETURNS void
LANGUAGE plpgsql
SET search_path = public
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_job_type_profitability;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_labor_cost_analysis;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_customer_profitability;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_monthly_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_benchmark_comparison;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_socal_rate_guidance;
END;
$$;

COMMENT ON FUNCTION refresh_all_analytics() IS
    'REFRESH MATERIALIZED VIEW CONCURRENTLY on all six analytics views (requires the unique indexes in this file). Safe to call anytime; typical cadence: nightly after data loads and benchmark fetches (fetch_benchmarks.py / harvest_socal_fintech.py), before detect_anomalies().';

-- =============================================================================
-- 9) scoped_* wrapper views -- the company-scoped read surface for the
--    AI-generated-SQL path ("Ask Your Data", nl_to_sql.py)
-- =============================================================================
--  PostgreSQL RLS does NOT apply to materialized views, so tenant isolation
--  for the restricted nl2sql_reader role (migrations/002_add_rls.sql) is
--  delivered by these REGULAR views instead: the app.current_company_id GUC
--  predicate is part of the view definition and is enforced for EVERY query
--  through the view regardless of the caller's privileges. Fail-closed: an
--  unset or empty GUC yields NULL and the view returns zero rows.
--
--  * security_barrier blocks leaky push-down of user-defined functions
--    through the tenant filter.
--  * Default DEFINER semantics: the views execute with their owner's rights,
--    so nl2sql_reader needs NO privilege on the underlying matviews -- and
--    gets none. The raw mv_* views stay permission-denied for the role.
--  * migrations/002_add_rls.sql creates the SAME views with CREATE OR REPLACE
--    (it owns the role + grants); the GRANTs below are guarded so this file
--    also applies cleanly on a fresh boot, where migrations (and therefore
--    the role) have not run yet.
-- -----------------------------------------------------------------------------
CREATE VIEW scoped_monthly_summary WITH (security_barrier) AS
    SELECT * FROM mv_monthly_summary
    WHERE company_id = NULLIF(current_setting('app.current_company_id',
                                              true), '')::bigint;

CREATE VIEW scoped_job_type_profitability WITH (security_barrier) AS
    SELECT * FROM mv_job_type_profitability
    WHERE company_id = NULLIF(current_setting('app.current_company_id',
                                              true), '')::bigint;

CREATE VIEW scoped_customer_profitability WITH (security_barrier) AS
    SELECT * FROM mv_customer_profitability
    WHERE company_id = NULLIF(current_setting('app.current_company_id',
                                              true), '')::bigint;

CREATE VIEW scoped_labor_cost_analysis WITH (security_barrier) AS
    SELECT * FROM mv_labor_cost_analysis
    WHERE company_id = NULLIF(current_setting('app.current_company_id',
                                              true), '')::bigint;

CREATE VIEW scoped_benchmark_comparison WITH (security_barrier) AS
    SELECT * FROM mv_benchmark_comparison
    WHERE company_id = NULLIF(current_setting('app.current_company_id',
                                              true), '')::bigint;

COMMENT ON VIEW scoped_monthly_summary IS
    'Company-scoped read surface over mv_monthly_summary for the AI-SQL path: the app.current_company_id GUC predicate is part of the view definition (fail-closed when unset). The raw matview is NOT granted to nl2sql_reader; this view is.';
COMMENT ON VIEW scoped_job_type_profitability IS
    'Company-scoped read surface over mv_job_type_profitability (same GUC predicate, fail-closed).';
COMMENT ON VIEW scoped_customer_profitability IS
    'Company-scoped read surface over mv_customer_profitability (same GUC predicate, fail-closed).';
COMMENT ON VIEW scoped_labor_cost_analysis IS
    'Company-scoped read surface over mv_labor_cost_analysis (same GUC predicate, fail-closed).';
COMMENT ON VIEW scoped_benchmark_comparison IS
    'Company-scoped read surface over mv_benchmark_comparison (same GUC predicate, fail-closed; the market_wage_*/ppi_* baseline columns are global data visible in every workspace).';

-- Grants to the AI read role -- only when migrations/002_add_rls.sql has
-- already created it (fresh-boot order: this file runs BEFORE migrations, so
-- the role may not exist yet; migration 002 grants unconditionally).
DO $$
DECLARE
    v text;
    granted int := 0;
BEGIN
    IF to_regrole('nl2sql_reader') IS NULL THEN
        RAISE NOTICE 'analytics_views: nl2sql_reader not created yet -- migration 002 will grant the scoped views';
        RETURN;
    END IF;
    FOR v IN SELECT * FROM unnest(ARRAY[
        'scoped_monthly_summary', 'scoped_job_type_profitability',
        'scoped_customer_profitability', 'scoped_labor_cost_analysis',
        'scoped_benchmark_comparison'
    ]) LOOP
        IF NOT has_table_privilege('nl2sql_reader', 'public.' || v,
                                   'SELECT') THEN
            EXECUTE format('GRANT SELECT ON public.%I TO nl2sql_reader', v);
            granted := granted + 1;
        END IF;
    END LOOP;
    RAISE NOTICE 'analytics_views: % new scoped-view grant(s)', granted;
END $$;

COMMIT;

-- =============================================================================
--  End of analytics_views.sql.
--  Typical nightly flow:
--    psql "..." -c "SELECT refresh_all_analytics();"
--    psql "..." -c "SELECT * FROM detect_anomalies();"
--  AI "Ask Your Data" tenant isolation (see section 9 + migrations/002):
--    the nl2sql_reader role reads ONLY the scoped_* views (company-filtered
--    by the app.current_company_id GUC) -- the raw mv_* matviews are
--    permission-denied for it.
-- =============================================================================
