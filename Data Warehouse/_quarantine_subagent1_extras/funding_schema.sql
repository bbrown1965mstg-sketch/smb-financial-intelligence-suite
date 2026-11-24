-- ============================================================================
-- funding_schema.sql -- SoCal Tech & Finance Capital Flows dataset
-- (Sub-agent 1 module: Capital & Grants Data Harvester)
--
-- STANDALONE schema: touches nothing defined in schema.sql /
-- analytics_views.sql. Safe to apply while other harvest modules are running.
--
-- Objects created:
--   * socal_capital_events            (table)  -- one row per verified capital
--     event: SEC Form D private placements, SBIR Phase I/II awards and STTR
--     awards to companies in the six-county SoCal footprint
--     (Los Angeles, Ventura, Orange, San Diego, Riverside, San Bernardino).
--   * socal_county_capital_summary    (materialized view) -- county x quarter
--     aggregation: deal count, total capital raised, median deal size and the
--     top 3 sectors by total dollars.
--
-- Apply:  psql "postgresql://postgres:postgres@localhost:5432/smb_warehouse" \
--              -f funding_schema.sql
-- Refresh the view after loading data:
--         REFRESH MATERIALIZED VIEW socal_county_capital_summary;
-- (CONCURRENTLY works too -- the view has a unique index on (county, quarter_start).)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Event table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS socal_capital_events (
    event_id            TEXT PRIMARY KEY,
    company_name        TEXT    NOT NULL,
    city                TEXT,
    county              TEXT    NOT NULL CHECK (county IN (
                            'Los Angeles', 'Orange', 'Ventura', 'San Diego',
                            'Riverside', 'San Bernardino')),
    zip_code            TEXT    NOT NULL,
    sector              TEXT    NOT NULL CHECK (sector IN (
                            'Fintech', 'Enterprise_SaaS', 'AI_ML',
                            'Defense_Aerospace_Tech', 'Biotech_Medtech',
                            'Financial_Services')),
    event_type          TEXT    NOT NULL CHECK (event_type IN (
                            'SEC_FORM_D', 'SBIR_PHASE_1', 'SBIR_PHASE_2',
                            'STTR', 'STATE_GRANT')),
    amount_raised       NUMERIC(14,2) NOT NULL CHECK (amount_raised > 0),
    offering_date       DATE    NOT NULL CHECK (offering_date >= DATE '2023-01-01'
                                              AND offering_date <= CURRENT_DATE),
    agency_or_filing_id TEXT    NOT NULL,
    source_url          TEXT    NOT NULL CHECK (source_url LIKE 'https://%'),
    raw_metadata        JSONB   NOT NULL DEFAULT '{}'::jsonb,
    -- A filing/award may only appear once: event_id is derived deterministically
    -- from (event_type, agency_or_filing_id), so the PK and this constraint are
    -- redundant by construction -- the pair documents that guarantee explicitly.
    UNIQUE (event_type, agency_or_filing_id)
);

CREATE INDEX IF NOT EXISTS ix_socal_capital_events_county_date
    ON socal_capital_events (county, offering_date);
CREATE INDEX IF NOT EXISTS ix_socal_capital_events_type
    ON socal_capital_events (event_type);
CREATE INDEX IF NOT EXISTS ix_socal_capital_events_sector
    ON socal_capital_events (sector);
CREATE INDEX IF NOT EXISTS ix_socal_capital_events_zip
    ON socal_capital_events (zip_code);
CREATE INDEX IF NOT EXISTS ix_socal_capital_events_metadata
    ON socal_capital_events USING gin (raw_metadata);

-- ----------------------------------------------------------------------------
-- 2. County x quarter materialized view
--    top_sectors = JSONB array of the 3 highest-dollar sectors for that
--    county-quarter: [{"sector": ..., "deals": n, "total": dollars}, ...]
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS socal_county_capital_summary AS
WITH quarterly AS (
    SELECT
        county,
        date_trunc('quarter', offering_date)::date          AS quarter_start,
        to_char(offering_date, 'YYYY-"Q"Q')                 AS quarter_label,
        count(*)                                            AS deal_count,
        round(sum(amount_raised), 2)                        AS total_capital_raised,
        round(percentile_cont(0.5) WITHIN GROUP
              (ORDER BY amount_raised)::numeric, 2)         AS median_deal_size
    FROM socal_capital_events
    GROUP BY 1, 2, 3
),
sector_totals AS (
    SELECT
        county,
        quarter_start,
        sector,
        deals,
        total,
        row_number() OVER (PARTITION BY county, quarter_start
                           ORDER BY total DESC, sector) AS rn
    FROM (
        SELECT
            county,
            date_trunc('quarter', offering_date)::date AS quarter_start,
            sector,
            count(*)                     AS deals,
            round(sum(amount_raised), 2) AS total
        FROM socal_capital_events
        GROUP BY 1, 2, 3
    ) st
),
top_sectors AS (
    SELECT
        county,
        quarter_start,
        jsonb_agg(jsonb_build_object('sector', sector,
                                     'deals',   deals,
                                     'total',   total)
                  ORDER BY total DESC) AS top_sectors
    FROM sector_totals
    WHERE rn <= 3
    GROUP BY 1, 2
)
SELECT
    q.county,
    q.quarter_start,
    q.quarter_label,
    q.deal_count,
    q.total_capital_raised,
    q.median_deal_size,
    t.top_sectors
FROM quarterly q
LEFT JOIN top_sectors t
       ON t.county = q.county AND t.quarter_start = q.quarter_start;

CREATE UNIQUE INDEX IF NOT EXISTS ux_socal_county_capital_summary
    ON socal_county_capital_summary (county, quarter_start);
