#!/usr/bin/env python3
"""
docker/init_pipeline.py -- idempotent bootstrap pipeline for the Docker stack.

Run by docker-entrypoint.sh BEFORE Streamlit starts. Order:

  0. Apply the two DATA-MODULE schemas (funding_schema.sql,
     peer_ratios_schema.sql) on EVERY boot: on the FRESH path they run right
     after analytics_views.sql so the canonical order holds
     (schema.sql -> analytics_views.sql -> funding_schema.sql ->
     peer_ratios_schema.sql); on the ALREADY-INITIALIZED path they are
     re-applied before the guarded pack load so volumes created before these
     modules existed pick the tables up. Both files are standalone and
     idempotent (CREATE ... IF NOT EXISTS; the peer matview is
     DROP ... IF EXISTS + CREATE), so this is always a no-op / safe refresh.
  0b. Apply migrations/migrations/*.sql (currently
     migrations/001_add_companies.sql -- the multi-tenant companies registry
     -- and migrations/002_add_rls.sql -- the nl2sql_reader AI-read role, the
     row-level-security policies on the 8 client tables + companies, the
     reader grants, and the scoped_* wrapper views over the raw matviews)
     AFTER the schema files on EVERY boot, fresh or persisted. The migrations
     are idempotent by design (CREATE TABLE IF NOT EXISTS, ADD COLUMN IF NOT
     EXISTS, guarded backfill, IF NOT EXISTS indexes, pg_roles/pg_policy/
     has_table_privilege-guarded DO blocks, CREATE OR REPLACE views), so
     pre-existing container volumes migrate cleanly and fresh boots are a
     no-op (schema.sql already creates the new shape; analytics_views.sql
     creates the scoped_* views).
  1. If the warehouse already has data (jobs has rows): print
     "already initialized, skipping seed" and skip steps 2-5 (the pgdata
     volume persists; re-running the pipeline is then a no-op for data) --
     EXCEPT the module schema re-apply, the migration re-apply (step 0b) and
     the pack load in step 6, which are guarded and idempotent.
  2. Apply schema.sql, then analytics_views.sql, via psycopg2 (both are
     idempotent BEGIN/COMMIT scripts; no psql client in the image).
  3. Seed the deterministic 36-month simulation (seed_demo.main(), seed 42)
     as client workspace #1 ('BrightPath Works (Demo)').
  4. OFFLINE-FIRST macro benchmarks: load samples/macro_benchmarks_snapshot.csv
     into market_benchmarks (benchmark_kind wage/ppi) with the SAME upsert
     statement fetch_benchmarks.py uses, so the container works without
     internet. THEN attempt fetch_benchmarks.py for the latest months --
     failures are tolerated with a warning (offline environments).
  5. Run the forecast engine for company 1: backtest FIRST, then forecasts
     (the same order as `python3 forecast_engine.py all`, so CIs use
     out-of-sample sigma).
  6. DATA-MODULE packs: load the two modules' committed export-pack CSVs
     (exports/socal_funding_deals_v1/socal_capital_events.csv ->
     socal_capital_events, peer_financial_ratios_v1/peer_financials.csv ->
     public_peer_financials) when the files exist -- upsert-style
     (ON CONFLICT DO NOTHING), never re-harvesting inside the container.
     A missing pack only degrades that module: the pipeline logs
     "harvest required" and keeps booting (schema-only module). The modules'
     matviews are REFRESHed after a pack load.

What boots with DATA vs SCHEMA-ONLY per module:
  * SMB core (schema.sql tables): ALWAYS with data (seed_demo + snapshot).
  * SoCal Benchmarks (harvest_socal_fintech.py tables):
    SCHEMA-ONLY in the container -- its data comes from its own live
    harvester, which is a HOST-side step (`python3 harvest_socal_fintech.py`);
    no pack is shipped for it in the image. Until it is harvested the
    SoCal Benchmarks tab shows its st.info hint.
  * SoCal Capital & Deals (funding_schema.sql): WITH DATA when
    exports/socal_funding_deals_v1/socal_capital_events.csv is present
    (the Dockerfile COPYs it; 3,596 events) -- else schema-only + log.
  * Public Peer Financials (peer_ratios_schema.sql): WITH DATA when
    peer_financial_ratios_v1/peer_financials.csv is present (1,193 rows) --
    else schema-only + log.

Every step logs a line. Schema/seed failures abort with a non-zero exit so
the container logs make the broken step obvious; module PACK failures are
tolerated with a warning so the container still boots clean.
"""

import csv
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

DB = __import__("os").environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@db:5432/smb_warehouse")

SNAPSHOT = PROJECT_ROOT / "samples" / "macro_benchmarks_snapshot.csv"

# Multi-tenant migrations (applied on EVERY boot, fresh or persisted; each
# file is idempotent by design -- see the file headers). Sorted by name so
# 001 (companies registry), 002 (RLS + nl2sql_reader AI-read role + scoped
# views), ... apply in order.
MIGRATIONS_DIR = PROJECT_ROOT / "migrations"

# The demo client workspace created/ensured by migrations/001 and re-seeded
# by seed_demo.main(); the forecast step below runs for THIS workspace.
DEFAULT_COMPANY_ID = 1

# ---------------------------------------------------------------------------
# data-module schemas + export packs (added by the module-unification pass;
# each module's DATA is owned by its own harvest session -- this pipeline only
# applies their committed schema files and loadable export packs)
# ---------------------------------------------------------------------------
MODULE_SCHEMA_FILES = [
    PROJECT_ROOT / "funding_schema.sql",        # SoCal Capital & Deals module
    PROJECT_ROOT / "peer_ratios_schema.sql",    # Public Peer Financials module
]
FUNDING_PACK_CSV = (PROJECT_ROOT / "exports" / "socal_funding_deals_v1"
                    / "socal_capital_events.csv")
FUNDING_PACK_REGENERATE = "python3 harvest_socal_capital.py && python3 export_socal_capital.py"
PEER_PACK_CSV = (PROJECT_ROOT / "peer_financial_ratios_v1"
                 / "peer_financials.csv")
PEER_PACK_REGENERATE = ("python3 harvest_sec_peer_ratios.py && "
                        "python3 export_peer_ratios.py")

# Column order matches the pack CSV headers exactly (verified against
# funding_schema.sql / peer_ratios_schema.sql); conflict targets are the
# tables' natural uniqueness so re-loading a pack is always a no-op.
FUNDING_INSERT_SQL = """
    INSERT INTO socal_capital_events (event_id, company_name, city, county,
                                      zip_code, sector, event_type,
                                      amount_raised, offering_date,
                                      agency_or_filing_id, source_url,
                                      raw_metadata)
    VALUES %s
    ON CONFLICT (event_id) DO NOTHING
"""
PEER_INSERT_SQL = """
    INSERT INTO public_peer_financials (record_id, cik, ticker, company_name,
                                        hq_state, is_socal, vertical,
                                        fiscal_year, fiscal_period, revenue,
                                        gross_profit, rd_expense, sga_expense,
                                        operating_income, net_income,
                                        operating_cash_flow, gross_margin_pct,
                                        operating_margin_pct, rd_pct_of_rev,
                                        sga_pct_of_rev, source_url,
                                        source_form, accession_no,
                                        source_tags, derived_q4)
    VALUES %s
    ON CONFLICT (cik, fiscal_year, fiscal_period) DO NOTHING
"""

# Identical to fetch_benchmarks.UPSERT_SQL: dedupe on the partial unique index
# (series_id, region, period) WHERE series_id IS NOT NULL.
MACRO_UPSERT_SQL = """
    INSERT INTO market_benchmarks (job_type_id, period, region, benchmark_kind,
                                   series_id, avg_price, avg_labor_cost_per_job,
                                   avg_margin_pct, source)
    VALUES %s
    ON CONFLICT (series_id, region, period) WHERE series_id IS NOT NULL
    DO UPDATE SET avg_price      = EXCLUDED.avg_price,
                  benchmark_kind = EXCLUDED.benchmark_kind,
                  source         = EXCLUDED.source
"""


def log(step, msg):
    print(f"[init_pipeline] {step}: {msg}", flush=True)


def apply_sql_file(conn, path):
    """Execute one of the idempotent BEGIN/COMMIT .sql scripts via psycopg2."""
    sql = path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    log("schema", f"applied {path.name}")


def apply_module_schemas(conn):
    """Apply the data-module schema files (funding_schema.sql, then
    peer_ratios_schema.sql). Both are STANDALONE (they reference nothing from
    schema.sql / analytics_views.sql) and IDEMPOTENT (funding: every object is
    CREATE ... IF NOT EXISTS; peer: CREATE TABLE IF NOT EXISTS + DROP/CREATE
    of its matview), so this is safe on every boot -- fresh or persisted.
    No 'already applied' guard is needed; see the file headers."""
    for path in MODULE_SCHEMA_FILES:
        apply_sql_file(conn, path)
    conn.commit()


def apply_migrations(conn):
    """Apply migrations/*.sql in name order on EVERY boot (step 0b).

    Each migration is IDEMPOTENT by design (CREATE TABLE IF NOT EXISTS, ADD
    COLUMN IF NOT EXISTS, a guarded backfill + SET NOT NULL, IF NOT EXISTS
    indexes), so this is a no-op on an already-migrated database and a clean
    upgrade for persisted volumes that predate the migration. On a FRESH boot
    schema.sql already creates the new shape and the migration is a no-op.
    A failing migration aborts the boot (non-zero exit via the main try/except
    convention: schema-level failures must be loud)."""
    if not MIGRATIONS_DIR.is_dir():
        log("migrations", "no migrations directory -- nothing to apply")
        return
    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not migration_files:
        log("migrations", "no migration files found -- nothing to apply")
        return
    for path in migration_files:
        apply_sql_file(conn, path)
    conn.commit()
    log("migrations", f"applied {len(migration_files)} migration file(s) "
                      "(idempotent)")


def _num(val):
    """CSV cell -> float or None (pack CSVs write empty cells for NULLs)."""
    v = (val or "").strip()
    return float(v) if v else None


def _int(val):
    v = (val or "").strip()
    return int(float(v)) if v else None


def _bool(val):
    return str(val).strip().lower() in ("true", "t", "1", "yes")


def _table_count(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")   # name is a code constant
        return cur.fetchone()[0]


def load_funding_pack(conn):
    """Load exports/socal_funding_deals_v1/socal_capital_events.csv into
    socal_capital_events when the pack exists and the table is still empty
    (the pack IS the harvested data -- the container never re-harvests).
    Then REFRESH the county x quarter matview. Missing pack -> 'harvest
    required' log line, container boots clean without the module's data."""
    try:
        n = _table_count(conn, "socal_capital_events")
        if n:
            log("module-socal-capital",
                f"already loaded ({n} events) -- skipping pack load")
            return 0
        if not FUNDING_PACK_CSV.exists():
            log("module-socal-capital",
                "harvest required -- run "
                f"`{FUNDING_PACK_REGENERATE}` on the host (export pack "
                f"{FUNDING_PACK_CSV.name} not shipped in the image); "
                "booting schema-only for this module")
            return 0
        rows = []
        with FUNDING_PACK_CSV.open(newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                rows.append((
                    r["event_id"], r["company_name"], r["city"] or None,
                    r["county"], r["zip_code"], r["sector"], r["event_type"],
                    float(r["amount_raised"]), r["offering_date"],
                    r["agency_or_filing_id"], r["source_url"],
                    r["raw_metadata"] or "{}"))
        with conn.cursor() as cur:
            execute_values(cur, FUNDING_INSERT_SQL, rows,
                           template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                    "%s, %s, %s::jsonb)",
                           page_size=500)
            cur.execute("REFRESH MATERIALIZED VIEW "
                        "socal_county_capital_summary")
        conn.commit()
        log("module-socal-capital",
            f"loaded {len(rows)} capital events from "
            f"{FUNDING_PACK_CSV.relative_to(PROJECT_ROOT)} (+ county matview "
            "refreshed)")
        return len(rows)
    except Exception as exc:                       # noqa: BLE001
        conn.rollback()
        log("module-socal-capital",
            f"WARNING: pack load failed ({exc}) -- module boots schema-only")
        return 0


def load_peer_pack(conn):
    """Load peer_financial_ratios_v1/peer_financials.csv into
    public_peer_financials when the pack exists and the table is still empty,
    re-sync the record_id sequence, and REFRESH the quartiles matview.
    Missing pack -> 'harvest required' log line, clean schema-only boot."""
    try:
        n = _table_count(conn, "public_peer_financials")
        if n:
            log("module-sec-peer-ratios",
                f"already loaded ({n} rows) -- skipping pack load")
            return 0
        if not PEER_PACK_CSV.exists():
            log("module-sec-peer-ratios",
                "harvest required -- run "
                f"`{PEER_PACK_REGENERATE}` on the host (export pack "
                f"{PEER_PACK_CSV.name} not shipped in the image); "
                "booting schema-only for this module")
            return 0
        rows = []
        with PEER_PACK_CSV.open(newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                rows.append((
                    _int(r["record_id"]), _int(r["cik"]), r["ticker"],
                    r["company_name"], r["hq_state"] or None,
                    _bool(r["is_socal"]), r["vertical"],
                    _int(r["fiscal_year"]), r["fiscal_period"],
                    _num(r["revenue"]), _num(r["gross_profit"]),
                    _num(r["rd_expense"]), _num(r["sga_expense"]),
                    _num(r["operating_income"]), _num(r["net_income"]),
                    _num(r["operating_cash_flow"]),
                    _num(r["gross_margin_pct"]),
                    _num(r["operating_margin_pct"]),
                    _num(r["rd_pct_of_rev"]), _num(r["sga_pct_of_rev"]),
                    r["source_url"], r["source_form"] or None,
                    r["accession_no"] or None, r["source_tags"] or None,
                    _bool(r["derived_q4"])))
        with conn.cursor() as cur:
            execute_values(cur, PEER_INSERT_SQL, rows,
                           template="(" + ", ".join(["%s"] * 25) + ")",
                           page_size=500)
            # rows were inserted WITH their record_ids: re-sync the sequence
            cur.execute(
                "SELECT setval(pg_get_serial_sequence("
                "'public_peer_financials', 'record_id'), "
                "GREATEST((SELECT COALESCE(MAX(record_id), 1) "
                "FROM public_peer_financials), 1))")
            cur.execute("REFRESH MATERIALIZED VIEW "
                        "mv_vertical_financial_quartiles")
        conn.commit()
        log("module-sec-peer-ratios",
            f"loaded {len(rows)} peer financial rows from "
            f"{PEER_PACK_CSV.relative_to(PROJECT_ROOT)} (+ quartiles matview "
            "refreshed)")
        return len(rows)
    except Exception as exc:                       # noqa: BLE001
        conn.rollback()
        log("module-sec-peer-ratios",
            f"WARNING: pack load failed ({exc}) -- module boots schema-only")
        return 0


def load_module_packs(conn):
    """Step 6: data-module export packs (guarded, idempotent, failure-tolerant)."""
    log("module", "loading data-module export packs "
                  "(guarded by file existence; the container never "
                  "re-harvests) ...")
    load_funding_pack(conn)
    load_peer_pack(conn)


def already_initialized(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                    "WHERE table_name = 'jobs')")
        if not cur.fetchone()[0]:
            return False
        cur.execute("SELECT COUNT(*) FROM jobs")
        return cur.fetchone()[0] > 0


def load_snapshot(conn):
    """Load the committed offline snapshot into market_benchmarks (wage/ppi)."""
    if not SNAPSHOT.exists():
        log("snapshot", f"WARNING: {SNAPSHOT.name} missing -- skipping "
                        "(run fetch_benchmarks online instead)")
        return 0
    rows = []
    with SNAPSHOT.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append((None, r["period"], r["region"], r["benchmark_kind"],
                         r["series_id"], float(r["avg_price"]), 0.00, 0.00,
                         r["source"] + " (snapshot)"))
    if not rows:
        log("snapshot", "WARNING: snapshot CSV is empty")
        return 0
    with conn.cursor() as cur:
        execute_values(cur, MACRO_UPSERT_SQL, rows,
                       template="(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                       page_size=500)
    conn.commit()
    log("snapshot", f"upserted {len(rows)} macro benchmark rows "
                    f"(offline snapshot: wage + ppi)")
    return len(rows)


def main():
    log("start", f"DATABASE_URL host = {DB.split('@')[-1]}")
    conn = psycopg2.connect(DB)
    try:
        if already_initialized(conn):
            log("seed", "already initialized, skipping seed")
            # The data-module schemas are standalone + idempotent, so they are
            # (re-)applied on EVERY boot even here -- a persisted volume that
            # predates the modules picks the tables up. Module DATA still gets
            # its guarded, idempotent top-up (empty module tables only).
            apply_module_schemas(conn)
            # Step 0b: the multi-tenant migrations are likewise idempotent and
            # re-applied on every boot, so a persisted volume created before
            # the companies registry existed migrates cleanly in place.
            apply_migrations(conn)
            load_module_packs(conn)
            return

        log("schema", "empty warehouse -- applying schema.sql ...")
        apply_sql_file(conn, PROJECT_ROOT / "schema.sql")
        apply_sql_file(conn, PROJECT_ROOT / "analytics_views.sql")
        # Canonical module-schema position on a fresh boot. Both files are
        # STANDALONE and IDEMPOTENT (funding: every object CREATE ... IF NOT
        # EXISTS; peer: CREATE TABLE IF NOT EXISTS + DROP/CREATE matview), so
        # no 'already applied' guard is needed -- documented boot order:
        # schema.sql -> analytics_views.sql -> funding_schema.sql
        # -> peer_ratios_schema.sql
        apply_module_schemas(conn)
        # Step 0b: migrations after the schema files on every boot (idempotent
        # by design; a no-op on a fresh boot because schema.sql already creates
        # the new shape).
        apply_migrations(conn)

        log("seed", "running seed_demo.main() (deterministic seed 42) ...")
        import seed_demo
        seed_demo.main()
        log("seed", "simulation loaded (36 months, ~2,500 jobs, planted anomalies)")

        load_snapshot(conn)

        log("fetch", "attempting fetch_benchmarks.py (online top-up, latest "
                     "months; tolerated to fail offline) ...")
        import fetch_benchmarks
        try:
            fetch_benchmarks.main()
            log("fetch", "online top-up complete")
        except SystemExit as exc:              # fetch_benchmarks exits 1 when
            log("fetch", f"WARNING: online fetch unavailable (exit {exc.code}) "
                         "-- snapshot data stands")
        except Exception as exc:               # noqa: BLE001 -- offline is fine
            log("fetch", f"WARNING: online fetch failed ({exc}) -- snapshot "
                         "data stands")

        log("forecast", "running forecast_engine for company "
                        f"{DEFAULT_COMPANY_ID}: backtest first, then "
                        "forecasts (same as `forecast_engine.py all "
                        f"--company-id {DEFAULT_COMPANY_ID}`) ...")
        import forecast_engine
        # Pass OUR connection so the engine never depends on its import-time
        # DB default (the functions don't close a caller-supplied connection).
        bt = forecast_engine.run_backtest(conn=conn,
                                          company_id=DEFAULT_COMPANY_ID)
        done_bt = [s for s, v in bt.items() if "error" not in v]
        log("forecast", f"backtest folds written for {len(done_bt)} series")
        fc = forecast_engine.run_forecasts(conn=conn, months_ahead=3,
                                           company_id=DEFAULT_COMPANY_ID)
        n_rows = sum(len(fc[s]) for s in forecast_engine.SERIES)
        n_series = sum(1 for s in forecast_engine.SERIES if fc[s])
        log("forecast", f"upserted {n_rows} forecast rows across {n_series} series")

        # Step 6: data-module export packs (SoCal Capital & Deals + Public
        # Peer Financials; guarded by file existence, never re-harvests).
        load_module_packs(conn)

        log("done", "warehouse ready -- starting the dashboard")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
