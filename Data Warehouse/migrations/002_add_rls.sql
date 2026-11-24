-- =============================================================================
--  SMB FINANCIAL & OPERATIONAL DATA WAREHOUSE
--  File   : migrations/002_add_rls.sql
--  Purpose: DATABASE-ENFORCED multi-tenant isolation for the AI-generated-SQL
--           path ("Ask Your Data", nl_to_sql.py):
--
--             1. the restricted LOGIN role `nl2sql_reader` (NOSUPERUSER,
--                NOBYPASSRLS) that nl_to_sql.run_readonly_query() connects as;
--             2. ROW LEVEL SECURITY on the 8 client-data tables + the
--                `companies` registry, keyed on the per-session GUC
--                app.current_company_id (fail-closed: unset/empty GUC -> NULL
--                -> ZERO rows);
--             3. SELECT grants on everything the AI may read (client tables,
--                companies, global/market data) and NOTHING on the 5 raw SMB
--                materialized views;
--             4. the 5 `scoped_*` wrapper views (company-scoped read surface
--                over the raw matviews -- RLS cannot be put ON a materialized
--                view, so the GUC predicate is baked into regular views with
--                security_barrier; default DEFINER semantics mean the reader
--                role needs NO privilege on the underlying matviews).
--
--  WHY RLS + a restricted role?  PostgreSQL RLS does NOT apply to materialized
--  views, and the app's trusted code paths (dashboard tabs, ingest, forecast
--  engine) connect as the `postgres` superuser, which BYPASSES RLS BY DESIGN
--  (they already set company_id explicitly; QA proved that scoping). The
--  STRICT enforcement target is therefore the AI-generated-SQL path: it must
--  run as a restricted role whose every query is tenant-filtered by
--  PostgreSQL itself, even when the generated SQL omits WHERE company_id.
--
--  FAIL-CLOSED SEMANTICS: every policy uses
--      company_id = NULLIF(current_setting('app.current_company_id', true),
--                          '')::bigint
--  so an UNSET or EMPTY GUC yields NULL and the comparison filters out every
--  row (zero rows returned). run_readonly_query() sets the GUC from the
--  caller's company_id (int-coercible, string-interpolated); when the caller
--  passes company_id=None nothing is set and the role session sees nothing.
--  (A garbage non-numeric GUC fails the ::bigint cast -- also fail-closed,
--  with a clean PostgreSQL error instead of silent rows.)
--
--  IDEMPOTENT: safe to re-run on any warehouse state (fresh container boot,
--  persisted volume, or an already-migrated database):
--    * role creation is guarded by a pg_roles check (password is set ONLY at
--      creation so an operator who rotates it -- and mirrors the new password
--      in NL2SQL_DB_PASSWORD -- is not clobbered on every boot; the
--      restriction attributes are re-asserted on EVERY run, never widened);
--    * policies are guarded by pg_policy checks;
--    * grants are guarded by has_table_privilege checks and tolerate missing
--      module tables via to_regclass (a DB built without the SoCal Capital /
--      SEC peer modules still migrates cleanly);
--    * scoped views use CREATE OR REPLACE VIEW.
--
--  docker/init_pipeline.py applies this file on EVERY boot (after schema.sql
--  -> analytics_views.sql -> the module schemas -> 001), fresh or persisted.
--  analytics_views.sql creates the same scoped_* views for the fresh-boot
--  path; this migration (re-)creates them with CREATE OR REPLACE so a
--  persisted volume that predates the views picks them up without ever
--  re-running analytics_views.sql.
--
--  APPLY (live host):
--    psql "postgresql://postgres:postgres@localhost:5432/smb_warehouse" \
--         -v ON_ERROR_STOP=1 -f migrations/002_add_rls.sql
--  Everything runs in ONE transaction: any failure rolls the whole migration
--  back and the database is left untouched.
--
--  PASSWORD: the documented default is 'nl2sql_readonly_local' (a
--  local-only READ role; the credential is public by design -- it grants no
--  write and sees only one workspace at a time). To use a different password,
--  ALTER ROLE nl2sql_reader PASSWORD '...' AND set NL2SQL_DB_PASSWORD in the
--  app's environment (nl_to_sql.py reads NL2SQL_DB_ROLE / NL2SQL_DB_PASSWORD
--  with these defaults); this migration never resets an existing role's
--  password.
--
--  WHAT THE ROLE CAN / CANNOT DO (the grant matrix):
--    CAN SELECT: the 8 client tables + companies (each row filtered by RLS),
--                market_benchmarks, backtest_results, mv_socal_rate_guidance,
--                mv_vertical_financial_quartiles, the 6 SoCal benchmark
--                tables, socal_capital_events, socal_county_capital_summary,
--                public_peer_financials -- ALL the global/market data (not
--                client data), and the 5 scoped_* views.
--    CANNOT:     SELECT the 5 raw SMB matviews (mv_monthly_summary,
--                mv_job_type_profitability, mv_customer_profitability,
--                mv_labor_cost_analysis, mv_benchmark_comparison -- an
--                explicit defensive REVOKE below keeps that true even if a
--                future wildcard grant appears), INSERT/UPDATE/DELETE
--                anything, CREATE anything, BYPASSRLS, or see another
--                workspace's rows -- even without a WHERE clause.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1) the restricted role (idempotent; attributes re-asserted every run)
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'nl2sql_reader') THEN
        CREATE ROLE nl2sql_reader LOGIN PASSWORD 'nl2sql_readonly_local';
    END IF;
END $$;

-- Attribute hardening (idempotent; runs on EVERY application so the role can
-- never silently drift to a more privileged state). LOGIN-only, no object
-- creation, no role management, and -- the point of the whole design -- NO
-- BYPASSRLS.
ALTER ROLE nl2sql_reader WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOBYPASSRLS;

COMMENT ON ROLE nl2sql_reader IS
    'Restricted READ-ONLY role for the AI-generated-SQL path (nl_to_sql.py run_readonly_query). RLS-scoped to the app.current_company_id GUC of its session; fail-closed when the GUC is unset. NO grant on the raw SMB matviews -- those are read only through the scoped_* wrapper views. Documented default password: nl2sql_readonly_local (rotate via ALTER ROLE + NL2SQL_DB_PASSWORD).';

-- -----------------------------------------------------------------------------
-- 2) ROW LEVEL SECURITY on the 8 client tables + the companies registry
--    (9 policies; fail-closed on an unset/empty GUC; superusers and the table
--    owner bypass by design -- the app's trusted paths connect as postgres)
--    DO NOT touch the SoCal/peer module tables: they are global market data.
-- -----------------------------------------------------------------------------

-- customers -------------------------------------------------------------------
ALTER TABLE customers ENABLE ROW LEVEL SECURITY;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policy
                   WHERE polrelid = 'customers'::regclass
                     AND polname  = 'tenant_isolation_customers') THEN
        CREATE POLICY tenant_isolation_customers ON customers
            USING      (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint)
            WITH CHECK (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint);
    END IF;
END $$;

-- job_types -------------------------------------------------------------------
ALTER TABLE job_types ENABLE ROW LEVEL SECURITY;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policy
                   WHERE polrelid = 'job_types'::regclass
                     AND polname  = 'tenant_isolation_job_types') THEN
        CREATE POLICY tenant_isolation_job_types ON job_types
            USING      (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint)
            WITH CHECK (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint);
    END IF;
END $$;

-- employees -------------------------------------------------------------------
ALTER TABLE employees ENABLE ROW LEVEL SECURITY;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policy
                   WHERE polrelid = 'employees'::regclass
                     AND polname  = 'tenant_isolation_employees') THEN
        CREATE POLICY tenant_isolation_employees ON employees
            USING      (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint)
            WITH CHECK (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint);
    END IF;
END $$;

-- jobs ------------------------------------------------------------------------
ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policy
                   WHERE polrelid = 'jobs'::regclass
                     AND polname  = 'tenant_isolation_jobs') THEN
        CREATE POLICY tenant_isolation_jobs ON jobs
            USING      (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint)
            WITH CHECK (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint);
    END IF;
END $$;

-- financial_ledger ------------------------------------------------------------
ALTER TABLE financial_ledger ENABLE ROW LEVEL SECURITY;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policy
                   WHERE polrelid = 'financial_ledger'::regclass
                     AND polname  = 'tenant_isolation_financial_ledger') THEN
        CREATE POLICY tenant_isolation_financial_ledger ON financial_ledger
            USING      (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint)
            WITH CHECK (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint);
    END IF;
END $$;

-- labor_logs ------------------------------------------------------------------
ALTER TABLE labor_logs ENABLE ROW LEVEL SECURITY;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policy
                   WHERE polrelid = 'labor_logs'::regclass
                     AND polname  = 'tenant_isolation_labor_logs') THEN
        CREATE POLICY tenant_isolation_labor_logs ON labor_logs
            USING      (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint)
            WITH CHECK (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint);
    END IF;
END $$;

-- anomaly_alerts --------------------------------------------------------------
ALTER TABLE anomaly_alerts ENABLE ROW LEVEL SECURITY;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policy
                   WHERE polrelid = 'anomaly_alerts'::regclass
                     AND polname  = 'tenant_isolation_anomaly_alerts') THEN
        CREATE POLICY tenant_isolation_anomaly_alerts ON anomaly_alerts
            USING      (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint)
            WITH CHECK (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint);
    END IF;
END $$;

-- forecasts -------------------------------------------------------------------
ALTER TABLE forecasts ENABLE ROW LEVEL SECURITY;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policy
                   WHERE polrelid = 'forecasts'::regclass
                     AND polname  = 'tenant_isolation_forecasts') THEN
        CREATE POLICY tenant_isolation_forecasts ON forecasts
            USING      (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint)
            WITH CHECK (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint);
    END IF;
END $$;

-- companies (the workspace registry itself: a restricted session sees only
-- its OWN registry row -- it cannot enumerate the client list) ----------------
ALTER TABLE companies ENABLE ROW LEVEL SECURITY;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policy
                   WHERE polrelid = 'companies'::regclass
                     AND polname  = 'tenant_isolation_companies') THEN
        CREATE POLICY tenant_isolation_companies ON companies
            USING      (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint)
            WITH CHECK (company_id = NULLIF(current_setting(
                            'app.current_company_id', true), '')::bigint);
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 3) grants -- everything the AI may read (idempotent, has_table_privilege-
--    guarded, tolerant of missing module tables via to_regclass)
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    t text;
    granted int := 0;
BEGIN
    IF NOT has_schema_privilege('nl2sql_reader', 'public', 'USAGE') THEN
        GRANT USAGE ON SCHEMA public TO nl2sql_reader;
        granted := granted + 1;
    END IF;

    -- ALWAYS-present warehouse relations: the 8 client tables + companies +
    -- the global model/benchmark tables (market_benchmarks = the real
    -- BLS/FRED series, backtest_results = the global model audit trail) +
    -- the SoCal benchmark tables + the SoCal rate-guidance matview. Client
    -- tables are RLS-filtered per the session GUC (see section 2); the rest
    -- are GLOBAL market data.
    FOR t IN SELECT * FROM unnest(ARRAY[
        'customers', 'job_types', 'employees', 'jobs',
        'financial_ledger', 'labor_logs', 'anomaly_alerts', 'forecasts',
        'companies',
        'market_benchmarks', 'backtest_results', 'mv_socal_rate_guidance',
        'regional_industry_profiles', 'socal_wage_benchmarks',
        'socal_macro_indicators', 'socal_business_patterns',
        'socal_loan_activity', 'socal_harvest_log'
    ]) LOOP
        IF to_regclass('public.' || t) IS NOT NULL
           AND NOT has_table_privilege('nl2sql_reader', 'public.' || t,
                                       'SELECT') THEN
            EXECUTE format('GRANT SELECT ON public.%I TO nl2sql_reader', t);
            granted := granted + 1;
        END IF;
    END LOOP;

    -- Data-module relations (SoCal Capital & Deals, SEC Peer Financials):
    -- granted when the module's schema exists, skipped when a DB was built
    -- without that module (keeps this migration standalone).
    FOR t IN SELECT * FROM unnest(ARRAY[
        'socal_capital_events', 'socal_county_capital_summary',
        'public_peer_financials', 'mv_vertical_financial_quartiles'
    ]) LOOP
        IF to_regclass('public.' || t) IS NOT NULL
           AND NOT has_table_privilege('nl2sql_reader', 'public.' || t,
                                       'SELECT') THEN
            EXECUTE format('GRANT SELECT ON public.%I TO nl2sql_reader', t);
            granted := granted + 1;
        END IF;
    END LOOP;

    RAISE NOTICE 'nl2sql_reader: % new grant(s)', granted;
END $$;

-- -----------------------------------------------------------------------------
-- 4) the 5 scoped_* wrapper views over the raw SMB matviews
--    RLS cannot be attached to a MATERIALIZED view, so the tenant filter
--    lives in these REGULAR views instead: the GUC predicate is part of the
--    view definition and is enforced for EVERY query through the view
--    regardless of the caller's privileges. Default DEFINER semantics (views
--    execute with their owner's rights) mean nl2sql_reader needs NO grant on
--    the underlying matviews -- and gets none. security_barrier prevents
--    leaky push-down of user-defined functions through the filter.
--    analytics_views.sql creates the same views for the fresh-boot path;
--    CREATE OR REPLACE keeps this migration idempotent on any DB state.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW scoped_monthly_summary WITH (security_barrier) AS
    SELECT * FROM mv_monthly_summary
    WHERE company_id = NULLIF(current_setting('app.current_company_id',
                                              true), '')::bigint;

CREATE OR REPLACE VIEW scoped_job_type_profitability WITH (security_barrier) AS
    SELECT * FROM mv_job_type_profitability
    WHERE company_id = NULLIF(current_setting('app.current_company_id',
                                              true), '')::bigint;

CREATE OR REPLACE VIEW scoped_customer_profitability WITH (security_barrier) AS
    SELECT * FROM mv_customer_profitability
    WHERE company_id = NULLIF(current_setting('app.current_company_id',
                                              true), '')::bigint;

CREATE OR REPLACE VIEW scoped_labor_cost_analysis WITH (security_barrier) AS
    SELECT * FROM mv_labor_cost_analysis
    WHERE company_id = NULLIF(current_setting('app.current_company_id',
                                              true), '')::bigint;

CREATE OR REPLACE VIEW scoped_benchmark_comparison WITH (security_barrier) AS
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

DO $$
DECLARE
    v text;
    granted int := 0;
BEGIN
    FOR v IN SELECT * FROM unnest(ARRAY[
        'scoped_monthly_summary', 'scoped_job_type_profitability',
        'scoped_customer_profitability', 'scoped_labor_cost_analysis',
        'scoped_benchmark_comparison'
    ]) LOOP
        IF to_regclass('public.' || v) IS NOT NULL
           AND NOT has_table_privilege('nl2sql_reader', 'public.' || v,
                                       'SELECT') THEN
            EXECUTE format('GRANT SELECT ON public.%I TO nl2sql_reader', v);
            granted := granted + 1;
        END IF;
    END LOOP;
    RAISE NOTICE 'nl2sql_reader: % new scoped-view grant(s)', granted;
END $$;

-- -----------------------------------------------------------------------------
-- 5) the negative invariant, kept explicit: the restricted role must NEVER
--    hold ANY privilege on the 5 raw SMB matviews (idempotent no-op when
--    nothing was ever granted; repairs a future stray wildcard grant).
--    (Matviews are relations: GRANT/REVOKE address them through the plain
--    relation form -- there is no ON MATERIALIZED VIEW object type in the
--    REVOKE grammar of this PostgreSQL build.)
-- -----------------------------------------------------------------------------
REVOKE ALL PRIVILEGES ON TABLE
    mv_monthly_summary, mv_job_type_profitability,
    mv_customer_profitability, mv_labor_cost_analysis,
    mv_benchmark_comparison
FROM nl2sql_reader;

COMMIT;

-- =============================================================================
--  End of migrations/002_add_rls.sql.
--  Verification hints after applying (as postgres):
--    SELECT rolname, rolsuper, rolcanlogin, rolbypassrls, rolcreaterole,
--           rolcreatedb FROM pg_roles WHERE rolname = 'nl2sql_reader';
--        -- super=f bypassrls=f login=t createrole=f createdb=f
--    SELECT count(*) FROM pg_policy;                       -- 9
--    SELECT c.relname, c.relrowsecurity FROM pg_class c
--      WHERE c.relname IN ('customers','job_types','employees','jobs',
--            'financial_ledger','labor_logs','anomaly_alerts','forecasts',
--            'companies');                                 -- all t
--    SELECT has_table_privilege('nl2sql_reader','mv_monthly_summary','SELECT');
--        -- false (and the other 4 raw matviews too)
--    SELECT has_table_privilege('nl2sql_reader','scoped_monthly_summary',
--                               'SELECT');               -- true
--    Enforcement proof (role session):
--      psql "postgresql://nl2sql_reader:nl2sql_readonly_local@localhost:5432/smb_warehouse"
--      => SET app.current_company_id = '1';
--      => SELECT count(*) FROM jobs;   -- only this workspace's rows
--      => SELECT count(*) FROM jobs WHERE company_id = 2;  -- 0 (RLS ANDs)
--      => RESET app.current_company_id; SELECT count(*) FROM jobs;  -- 0
-- =============================================================================
