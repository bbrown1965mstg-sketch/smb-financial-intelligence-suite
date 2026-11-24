-- =============================================================================
--  demo_queries.sql -- business questions answered with real SQL
-- =============================================================================
--  Run any of these against the warehouse:
--    psql "postgresql://postgres:postgres@localhost:5432/smb_warehouse"
--  Each block is standalone. Materialized views are SNAPSHOTS -- run
--    SELECT refresh_all_analytics();
--  after new data lands, or they will serve stale numbers.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1) Which service line is most profitable PER HOUR of labor?
--    (revenue view cross-checked against logged hours)
-- ---------------------------------------------------------------------------
SELECT job_type,
       ROUND(revenue / NULLIF(labor_hours, 0), 2)   AS revenue_per_labor_hour,
       margin_pct,
       job_count
FROM   mv_job_type_profitability
ORDER  BY revenue_per_labor_hour DESC;

-- ---------------------------------------------------------------------------
-- 2) Who are my 10 best customers by gross margin?
-- ---------------------------------------------------------------------------
SELECT customer_name, revenue, gross_margin, margin_pct, job_count
FROM   mv_customer_profitability
WHERE  job_count > 0
ORDER  BY gross_margin DESC
LIMIT  10;

-- ---------------------------------------------------------------------------
-- 3) Are we underpricing vs the market? (avg quoted price vs latest benchmark)
-- ---------------------------------------------------------------------------
SELECT job_type, avg_job_price, benchmark_avg_price, price_vs_benchmark_pct
FROM   mv_job_type_profitability
ORDER  BY price_vs_benchmark_pct NULLS LAST;

-- ---------------------------------------------------------------------------
-- 4) Which months lost money, and why?
-- ---------------------------------------------------------------------------
SELECT month_start, revenue, total_expense, gross_profit, gross_margin_pct,
       job_completions
FROM   mv_monthly_summary
WHERE  gross_profit < 0
ORDER  BY month_start;

-- ---------------------------------------------------------------------------
-- 5) Show every OPEN anomaly alert, newest first, with the human-readable
--    context the detector stored in the JSONB details column.
-- ---------------------------------------------------------------------------
SELECT a.detected_at,
       a.anomaly_type,
       COALESCE(j.job_number, a.category || ' - ' || TO_CHAR(a.period, 'Mon YYYY')) AS subject,
       a.severity,
       a.metric_value,
       a.baseline_value,
       a.deviation_pct,
       a.z_score,
       a.details
FROM   anomaly_alerts a
LEFT   JOIN jobs j ON j.id = a.job_id
WHERE  NOT a.is_resolved
ORDER  BY CASE a.severity WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
       a.detected_at DESC;

-- ---------------------------------------------------------------------------
-- 6) Which customers have not ordered in the longest time? (win-back list)
-- ---------------------------------------------------------------------------
SELECT customer_name, job_count, revenue, last_job_date, days_since_last_job
FROM   mv_customer_profitability
WHERE  is_active AND job_count > 0
ORDER  BY days_since_last_job DESC
LIMIT  10;

-- ---------------------------------------------------------------------------
-- 7) Job-level drill-down for one flagged job: what actually went wrong?
--    (replace the job number with any alert subject)
-- ---------------------------------------------------------------------------
SELECT ll.work_date, e.first_name || ' ' || e.last_name AS employee,
       ll.hours, ll.hourly_rate,
       ROUND(ll.hours * ll.hourly_rate, 2) AS cost
FROM   labor_logs ll
JOIN   jobs j  ON j.id = ll.job_id
JOIN   employees e ON e.id = ll.employee_id
WHERE  j.job_number = 'JOB-2026-0112'
ORDER  BY ll.work_date, e.last_name;

-- ---------------------------------------------------------------------------
-- 8) Raw ledger drill-down: everything booked against one job
-- ---------------------------------------------------------------------------
SELECT fl.entry_date, fl.entry_type, fl.category, fl.amount, fl.description, fl.vendor
FROM   financial_ledger fl
JOIN   jobs j ON j.id = fl.job_id
WHERE  j.job_number = 'JOB-2026-0112'
ORDER  BY fl.entry_date;

-- ---------------------------------------------------------------------------
-- 9) Crew utilization: hours and payroll cost per employee this year
-- ---------------------------------------------------------------------------
SELECT e.first_name || ' ' || e.last_name AS employee, e.role,
       SUM(ll.hours)                          AS hours_ytd,
       ROUND(SUM(ll.hours * ll.hourly_rate), 2) AS labor_cost_ytd,
       COUNT(DISTINCT ll.job_id)              AS jobs_touched
FROM   labor_logs ll
JOIN   employees e ON e.id = ll.employee_id
WHERE  ll.work_date >= date_trunc('year', CURRENT_DATE)
GROUP  BY e.first_name, e.last_name, e.role
ORDER  BY labor_cost_ytd DESC;

-- ---------------------------------------------------------------------------
-- 10) Month-over-month revenue growth (window functions 101)
-- ---------------------------------------------------------------------------
SELECT month_start, revenue,
       ROUND(revenue - LAG(revenue) OVER (ORDER BY month_start), 2) AS mom_change,
       ROUND((revenue / NULLIF(LAG(revenue) OVER (ORDER BY month_start), 0) - 1) * 100, 1) AS mom_pct
FROM   mv_monthly_summary
ORDER  BY month_start;

-- ---------------------------------------------------------------------------
-- 11) WHALES: top-quintile revenue accounts that are LOSING money
--     (revenue >= 80th percentile AND gross_margin < 0, acquisition cost
--     included). These look like crown jewels by size and quietly drain you.
-- ---------------------------------------------------------------------------
SELECT customer_name, revenue, job_cost, acquisition_cost, total_cost,
       gross_margin, margin_pct, job_count
FROM   mv_customer_profitability
WHERE  is_whale
ORDER  BY gross_margin;

-- 11b) Where did the whale's money go? (drill into one flagged account)
SELECT j.job_number, jt.name AS job_type, j.status,
       j.quoted_price, j.estimated_labor_cost,
       (SELECT ROUND(SUM(ll.hours * ll.hourly_rate), 2)
          FROM labor_logs ll WHERE ll.job_id = j.id)        AS labor_actual,
       (SELECT ROUND(COALESCE(SUM(fl.amount), 0), 2)
          FROM financial_ledger fl
         WHERE fl.job_id = j.id AND fl.category = 'materials') AS materials
FROM   jobs j
JOIN   job_types jt ON jt.id = j.job_type_id
WHERE  j.customer_id = (SELECT customer_id FROM mv_customer_profitability
                        WHERE is_whale ORDER BY gross_margin LIMIT 1)
ORDER  BY j.quoted_price DESC;

-- ---------------------------------------------------------------------------
-- 12) Are we inflating faster than the real market?
--     Internal wage/materials inflation (from OUR books) vs BLS wage rates
--     and FRED PPIs (loaded by fetch_benchmarks.py). Gap > 0 = margin squeeze.
-- ---------------------------------------------------------------------------
SELECT month_name AS month,
       internal_labor_rate,
       market_wage_national,
       internal_labor_yoy_pct,
       market_wage_national_yoy_pct,
       labor_inflation_gap_national_pct,
       internal_materials_yoy_pct,
       ppi_building_materials_yoy_pct,
       materials_inflation_gap_pct
FROM   mv_benchmark_comparison
ORDER  BY period DESC
LIMIT  12;

-- 12b) Which months squeezed us hardest?
SELECT month_name, labor_inflation_gap_national_pct, materials_inflation_gap_pct
FROM   mv_benchmark_comparison
ORDER  BY (COALESCE(labor_inflation_gap_national_pct, 0)
         + COALESCE(materials_inflation_gap_pct, 0)) DESC
LIMIT  6;
