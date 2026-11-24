-- ============================================================================
-- peer_ratios_schema.sql
-- Audited Finance & Tech Peer Financial Ratios Dataset (exports/peer_financial_ratios_v1/)
-- Sub-agent 1: SEC XBRL Financial Data Engineer
--
-- STANDALONE MODULE SCHEMA.
--   * Does NOT modify schema.sql, analytics_views.sql, funding_schema.sql,
--     app.py, or export_dataset_pack.py, nor any of their objects.
--   * Safe to re-run (idempotent): CREATE IF NOT EXISTS + MV drop/recreate.
--   * Target: local PostgreSQL 17.5, database smb_warehouse @ localhost:5432.
--
-- Apply with:
--   PGPASSWORD=postgres ~/pg/bin/psql -h localhost -p 5432 -U postgres \
--       -d smb_warehouse -f peer_ratios_schema.sql
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Core fact table: one row per (company, fiscal_year, fiscal_period).
-- Dollar amounts are USD as filed (annual periods on FY rows, ~3-month periods
-- on Q1..Q4 rows; Q4 rows are derived when the filer does not tag a discrete
-- fiscal Q4 — see derived_q4 below and PEER_RATIOS_DATA_DICTIONARY.md).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public_peer_financials (
    record_id            BIGSERIAL    PRIMARY KEY,
    cik                  INTEGER      NOT NULL,
    ticker               TEXT         NOT NULL,
    company_name         TEXT         NOT NULL,
    hq_state             TEXT,                       -- EDGAR business-address state (or country for foreign filers)
    is_socal             BOOLEAN      NOT NULL DEFAULT FALSE,
    vertical             TEXT         NOT NULL
                         CHECK (vertical IN ('Enterprise_SaaS',
                                             'Fintech_Payments',
                                             'Asset_Wealth_Mgmt',
                                             'IT_Consulting_Services',
                                             'Specialty_Finance')),
    fiscal_year          INTEGER      NOT NULL CHECK (fiscal_year BETWEEN 2000 AND 2100),
    fiscal_period        TEXT         NOT NULL CHECK (fiscal_period IN ('FY', 'Q1', 'Q2', 'Q3', 'Q4')),
    revenue              NUMERIC(20, 2),
    gross_profit         NUMERIC(20, 2),
    rd_expense           NUMERIC(20, 2),
    sga_expense          NUMERIC(20, 2),
    operating_income     NUMERIC(20, 2),
    net_income           NUMERIC(20, 2),
    operating_cash_flow  NUMERIC(20, 2),
    gross_margin_pct     NUMERIC(10, 4),
    operating_margin_pct NUMERIC(10, 4),
    rd_pct_of_rev        NUMERIC(10, 4),
    sga_pct_of_rev       NUMERIC(10, 4),
    source_url           TEXT         NOT NULL,     -- SEC EDGAR filing index page for the value's source filing
    source_form          TEXT,                       -- e.g. 10-K, 10-K/A, 10-Q, 10-Q/A
    accession_no         TEXT,                       -- EDGAR accession number of the source filing
    source_tags          TEXT,                       -- audit trail: which XBRL tags / fallback math produced each field
    derived_q4           BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT uq_peer_fin_period     UNIQUE (cik, fiscal_year, fiscal_period),
    -- Audited realism bound for gross margin (Sub-agent 2 QA rule); NULL-safe.
    CONSTRAINT chk_gross_margin_realistic
        CHECK (gross_margin_pct IS NULL OR (gross_margin_pct >= -50.0 AND gross_margin_pct <= 100.0)),
    CONSTRAINT chk_source_url_edgar
        CHECK (source_url LIKE 'https://www.sec.gov/Archives/edgar/data/%')
);

CREATE INDEX IF NOT EXISTS idx_peer_fin_vertical_year ON public_peer_financials (vertical, fiscal_year);
CREATE INDEX IF NOT EXISTS idx_peer_fin_ticker        ON public_peer_financials (ticker, fiscal_year, fiscal_period);
CREATE INDEX IF NOT EXISTS idx_peer_fin_socal         ON public_peer_financials (is_socal) WHERE is_socal;

COMMENT ON TABLE  public_peer_financials IS 'Audited peer financials harvested from SEC EDGAR XBRL (data.sec.gov company-concept API), FY2021-FY2025, 50 Finance/Fintech/SaaS/IT-services tickers incl. Southern California public companies.';
COMMENT ON COLUMN public_peer_financials.vertical        IS 'Peer vertical: Enterprise_SaaS | Fintech_Payments | Asset_Wealth_Mgmt | IT_Consulting_Services | Specialty_Finance';
COMMENT ON COLUMN public_peer_financials.fiscal_period   IS 'FY = annual (10-K) period; Q1..Q4 = ~3-month fiscal quarters (Q4 derived from FY-(Q1+Q2+Q3) unless the filer tags a discrete fiscal Q4)';
COMMENT ON COLUMN public_peer_financials.is_socal        IS 'TRUE when the EDGAR business address is in a Southern California county (Imperial, Kern, Los Angeles, Orange, Riverside, San Bernardino, San Diego, Santa Barbara, Ventura)';
COMMENT ON COLUMN public_peer_financials.gross_margin_pct IS '100 * gross_profit / revenue, bounded to [-50, 100]; NULL when gross profit is not determinable';
COMMENT ON COLUMN public_peer_financials.source_tags     IS 'Audit trail of XBRL tags and fallback math used per field (e.g. gross_profit=Computed:Revenue-CostOfRevenue, sga=SUM:...)';
COMMENT ON COLUMN public_peer_financials.derived_q4      IS 'TRUE when the row is a derived fiscal Q4: FY - (Q1+Q2+Q3), sourced from the annual 10-K';

-- ----------------------------------------------------------------------------
-- Benchmark materialized view: 25th / median / 75th percentile of the four
-- ratio metrics per (vertical, fiscal_year), split into SoCal vs. National
-- peer groups via the socal_flag column (peer_group gives the human label).
-- percentile_cont ignores NULLs; groups whose metric is entirely NULL yield NULL.
-- REFRESH:  REFRESH MATERIALIZED VIEW mv_vertical_financial_quartiles;
--           (CONCURRENTLY is supported via the unique index below)
-- ----------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mv_vertical_financial_quartiles;
CREATE MATERIALIZED VIEW mv_vertical_financial_quartiles AS
SELECT
    vertical,
    fiscal_year,
    is_socal                                                    AS socal_flag,
    CASE WHEN is_socal THEN 'SoCal' ELSE 'National' END         AS peer_group,
    COUNT(*)                                                    AS n_records,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY gross_margin_pct)     AS gross_margin_p25,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY gross_margin_pct)     AS gross_margin_median,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY gross_margin_pct)     AS gross_margin_p75,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY operating_margin_pct) AS operating_margin_p25,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY operating_margin_pct) AS operating_margin_median,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY operating_margin_pct) AS operating_margin_p75,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY rd_pct_of_rev)        AS rd_pct_of_rev_p25,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY rd_pct_of_rev)        AS rd_pct_of_rev_median,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY rd_pct_of_rev)        AS rd_pct_of_rev_p75,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY sga_pct_of_rev)       AS sga_pct_of_rev_p25,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY sga_pct_of_rev)       AS sga_pct_of_rev_median,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY sga_pct_of_rev)       AS sga_pct_of_rev_p75
FROM public_peer_financials
GROUP BY vertical, fiscal_year, is_socal;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_peer_quartiles
    ON mv_vertical_financial_quartiles (vertical, fiscal_year, socal_flag);

COMMENT ON MATERIALIZED VIEW mv_vertical_financial_quartiles IS 'Per-vertical, per-fiscal-year 25th/median/75th percentiles of gross_margin_pct, operating_margin_pct, rd_pct_of_rev, sga_pct_of_rev, split SoCal vs. National (socal_flag/peer_group). Refresh after loading.';
