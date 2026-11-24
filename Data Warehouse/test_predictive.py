"""
QA suite for the PREDICTIVE layer of the SMB Financial & Operational Data
Warehouse (pricing_simulator.py + forecast_engine.py) -- edge cases beyond the
schema-focused coverage in test_schema.py.

HOW TO RUN
----------
    cd "/Users/Owner/Desktop/GLM-5.3/Data Warehouse"
    python3 -m pytest test_predictive.py -v

or together with the schema suite:

    python3 -m pytest test_schema.py test_predictive.py -v

WHAT IT COVERS
--------------
Part A -- OFFLINE unit tests (no database):
  * simulate_job hand-computed reference scenario (price 10000, hours 100,
    rate 40, ot_pct 0.2, material_ratio 0.25, material_change +0.10, other 5%
    -> labor 4400, materials 2750, other 500, margin 2350 = 23.5%, -6.5pp vs
    the 30% target),
  * validation contract (price 0 / negative hours -> ValueError) and the
    documented clamps (ot_pct 0.9 -> 0.5, material_change_pct -5 -> -1) with
    clamps_applied reporting,
  * zero-materials edge (material_ratio 0 -> margin = price - labor - other),
  * margin_vs_market_pct sign conventions (positive above the market
    benchmark, negative below, None when no market_margin),
  * forecast_engine pure-helper edge cases the scratch tests lean on (WAPE
    with negative actuals, MAPE zero-actual exclusion, degenerate seasonal
    indices, year-crossing month arithmetic, constant-series least squares).

Part B -- SCRATCH-DB integration tests against a SMALL DETERMINISTIC
synthetic history (direct inserts, no CSV files):
  * run_backtest produces exactly one fold per series with correct windows,
    finite metrics and 4 predictions; the stored predictions/actuals/errors
    match HAND-DERIVED values exactly (the fixture is built so the model is
    solvable on paper -- see _seed_synthetic_history for the arithmetic),
  * WAPE convention: the synthetic net_cash_flow series has one negative
    month; the stored metric is WAPE (48.24), NOT the MAPE (50.0) that the
    same pairs would produce -- both recomputed by hand in the test comments,
  * no-look-ahead / macro fallback: the scratch DB deliberately has NO
    market_benchmarks rows; the engine still works (drift falls back to the
    model trend alone) and the forecasts' details JSONB reports
    macro_missing = true with a "no macro rows" note,
  * run_forecasts: horizon dates are the next N month starts, the seasonal
    factor is applied (hand-checked level x factor = predicted), the pooled
    backtest metrics are carried into the forecasts' mape/rmse columns, CI
    ordering holds per series kind (ci_low clamped >= 0 for the non-negative
    series; NOT clamped -- genuinely negative -- for net_cash_flow), and the
    (series, period_start, model) upsert is idempotent (two runs -> still 12
    rows, identical values),
  * empty-series edge: a table/category with no rows (labor_logs empty, no
    'materials' ledger rows) makes the engine SKIP that series gracefully --
    no crash, an {'error': ..., 'months': N} entry in the returned summary
    and zero rows written for it. Pinned actual behaviour (the run_backtest
    docstring used to claim such series are "absent from the dict"; they are
    in fact present with an error entry -- the docstring has been corrected).

Part C -- hygiene:
  * the module patches forecast_engine.DB to a nonexistent database so that
    ANY engine call that forgets to pass the scratch connection fails loudly
    instead of silently writing to the live smb_warehouse,
  * a read-only probe of the live smb_warehouse asserts no QAtest-tagged rows
    ever leaked into its forecasts / backtest_results (skipped when the live
    DB is unreachable; this suite itself never writes there).

SCRATCH-DB PATTERN (same lifecycle as test_schema.py, own database names so
both suites can run in one pytest session without fighting):
  1. DROP DATABASE IF EXISTS <scratch> WITH (FORCE),
  2. CREATE DATABASE <scratch>,
  3. apply schema.sql then analytics_views.sql via subprocess psql
     (-v ON_ERROR_STOP=1),
  4. run the tests against a connection to the scratch DB,
  5. teardown: DROP DATABASE IF EXISTS <scratch> WITH (FORCE) -- even on
     failure, so the server is left exactly as it was found.

Two throwaway databases are used:
  * smb_warehouse_qa_pred        -- the 14-month deterministic seasonal
                                    fixture (all four series populated),
  * smb_warehouse_qa_pred_empty  -- 12 ledger months with revenue + overhead
                                    only (no labor_logs rows, no materials
                                    category) for the empty-series edge.

Connection: DATABASE_URL selects the PARENT database the scratch DBs are
derived from (default the maintenance DB 'postgres' on localhost); the suite
NEVER opens smb_warehouse for writing.

Data hygiene: all fixture parent rows use 'QAtest'-prefixed names/emails/job
numbers. The synthetic ledger deliberately uses the REAL category names
'revenue'/'materials'/'overhead' because forecast_engine's series queries
match on category = 'materials' literally; those rows exist only inside the
self-destructing scratch databases.
"""

import datetime
import math
import os
import shutil
import subprocess
import sys
import urllib.parse
import uuid

import psycopg2
import pytest

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _BASE_DIR)  # project-local imports: forecast_engine, pricing_simulator

import forecast_engine as fe  # noqa: E402

# ---------------------------------------------------------------------------
# LIVE-WRITE GUARD: point the engine's default DB at a database that does not
# exist. Every test in this module passes the scratch connection explicitly
# (run_backtest(conn, ...) / run_forecasts(conn, ...)); if any code path ever
# forgets, psycopg2 fails LOUDLY on the fake DB name instead of silently
# writing forecasts into the live smb_warehouse.
# ---------------------------------------------------------------------------
_GUARD_DB = "postgresql://postgres:postgres@localhost:5432/qa_guard_db_does_not_exist"
fe.DB = _GUARD_DB

from pricing_simulator import simulate_job  # noqa: E402

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/postgres",
)

PRED_SCRATCH_DB = "smb_warehouse_qa_pred"
EMPTY_SCRATCH_DB = "smb_warehouse_qa_pred_empty"

_BASE_DIR_SQL = _BASE_DIR
SCHEMA_SQL_PATH = os.path.join(_BASE_DIR_SQL, "schema.sql")
ANALYTICS_VIEWS_SQL_PATH = os.path.join(_BASE_DIR_SQL, "analytics_views.sql")
PSQL_BIN = shutil.which("psql") or "/Users/Owner/pg/bin/psql"

Z90 = 1.6448536269514722  # fe.Z_90 (duplicated so the tests stay self-evident)


# ---------------------------------------------------------------------------
# Scratch database lifecycle (same pattern as test_schema.py, own DB names)
# ---------------------------------------------------------------------------

def _parent_connect():
    conn_ = psycopg2.connect(DATABASE_URL)
    conn_.autocommit = True  # CREATE/DROP DATABASE cannot run in a transaction
    return conn_


def _drop_scratch(db_name):
    conn_ = _parent_connect()
    try:
        with conn_.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {db_name} WITH (FORCE)")
    finally:
        conn_.close()


def _create_scratch(db_name):
    conn_ = _parent_connect()
    try:
        with conn_.cursor() as cur:
            cur.execute(f"CREATE DATABASE {db_name}")
    finally:
        conn_.close()


def _apply_sql(scratch_url, path):
    proc = subprocess.run(
        [PSQL_BIN, scratch_url, "-X", "-v", "ON_ERROR_STOP=1", "-f", path],
        capture_output=True, text=True, timeout=180,
    )
    if proc.returncode != 0:
        pytest.fail(
            f"psql failed applying {os.path.basename(path)}:\n"
            f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}",
            pytrace=False,
        )


def _scratch_url(db_name):
    parts = urllib.parse.urlsplit(DATABASE_URL)
    return urllib.parse.urlunsplit(
        (parts.scheme, parts.netloc, "/" + db_name, parts.query, parts.fragment)
    )


def _make_scratch(db_name):
    """Create a fresh scratch DB with schema + analytics views applied, and
    verify the multi-tenant fixture workspace.

    Migration 001 made company_id NOT NULL on the client-data tables.
    schema.sql seeds the demo client workspace as company #1
    ('BrightPath Works (Demo)') so the company_id DEFAULT 1 has a valid
    parent; the fixture INSERTs below land in that workspace via the column
    DEFAULT, and the engine writes forecasts with its default company_id=1 --
    the same workspace. (Same documented pattern as test_schema.py's
    _seed_qa_company_defaults.)"""
    _drop_scratch(db_name)
    _create_scratch(db_name)
    url = _scratch_url(db_name)
    _apply_sql(url, SCHEMA_SQL_PATH)
    _apply_sql(url, ANALYTICS_VIEWS_SQL_PATH)
    conn = psycopg2.connect(url)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT company_name FROM companies WHERE company_id = 1")
        row = cur.fetchone()
        assert row is not None and row[0] == "BrightPath Works (Demo)", (
            f"schema.sql must seed the demo workspace as company 1; got {row!r}")
    conn.commit()
    return conn


def fetch_all(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def fetch_one(conn, sql, params=None):
    return fetch_all(conn, sql, params)[0]


def fetch_dicts(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _add_months(month, k):
    """Local copy of the engine's month arithmetic (keeps the fixture
    independent of the code under test)."""
    y, m = month.year, month.month + k
    y += (m - 1) // 12
    m = (m - 1) % 12 + 1
    return datetime.date(y, m, 1)


# ---------------------------------------------------------------------------
# Deterministic synthetic fixture
# ---------------------------------------------------------------------------
# 14 monthly buckets, t = 0..13 -> 2024-01 .. 2025-02 (rows dated on the 15th).
#
#   revenue[t]          = 1000 for January months else 500
#   material_expense[t] = 100  for January months else 200
#   overhead[t]         = 100  except t=13 (Feb 2025) -> 800   (cash-crash month)
#   labor_cost[t]       = hours 20 (Jan) / 10 (else) x $50 -> 1000 / 500
#   net_cash_flow[t]    = revenue - materials - overhead
#                          = [800, 200 x 11, 800, -500]   (one NEGATIVE month)
#
# The shape is chosen so seasonal_trend_v1 is EXACTLY solvable on paper:
#
# BACKTEST fold (train_months=10 -> 2024-01..2024-10, test_months=4):
#   every train calendar month appears once, so each seasonal index absorbs
#   its month completely and the de-seasonalized train series is CONSTANT
#   (e.g. revenue: mean 550, idx[Jan] = 1000/550, idx[other] = 500/550,
#   deseason = 550 everywhere) -> slope 0, no macro in the scratch DB ->
#   drift 0, level = 550. Predictions = level x seasonal[month(h)]:
#     revenue/labor : [550, 550, 1000, 500]  (Jan 2025 forecast uses the
#                     January index -> exactly 1000)
#     material      : [190, 190, 100, 200]
#     net_cash_flow : [260, 260, 800, 200]
#   metrics (hand-derived, see the individual tests):
#     revenue/labor MAPE 5.00, RMSE sqrt(1250) = 35.36
#     material      MAPE 2.50, RMSE sqrt(50)   = 7.07
#     net_cash      WAPE 820/1700 = 48.24 (MAPE would be 50.00),
#                   RMSE sqrt(124300) = 352.56
#
# FORECASTS (fit on all 14 months, months_ahead=3 -> Mar/Apr/May 2025):
#   revenue/labor: level 4000/7 = 571.43, idx[Mar] = 7/8 = 0.875
#                  -> predicted 500.00 for all three horizons
#   material     : level 185.71, idx[Mar] = 14/13 -> predicted 200.00 x3
#   net_cash     : the deseasonalized series is NOT constant (Feb appears
#                  twice with values 200 and -500), so the trend kicks in:
#                  slope = 6600/227.5 = 29.011, model growth 6.8376%/mo ->
#                  WINSORIZED to 5%/mo -> drift 5%/mo (macro missing),
#                  level = 5500/7 = 785.71, idx[Mar] = 28/33
#                  -> predicted 700.00, 735.00, 771.75 exactly.

HISTORY_MONTHS = 14
FIRST_MONTH = datetime.date(2024, 1, 15)


def _fixture_series():
    months = [_add_months(FIRST_MONTH, t) for t in range(HISTORY_MONTHS)]
    rev = [1000.0 if m.month == 1 else 500.0 for m in months]
    mat = [100.0 if m.month == 1 else 200.0 for m in months]
    ovr = [800.0 if t == 13 else 100.0 for t in range(HISTORY_MONTHS)]
    cash = [r - m - o for r, m, o in zip(rev, mat, ovr)]
    labor = [1000.0 if m.month == 1 else 500.0 for m in months]
    return months, {"revenue": rev, "material_expense": mat,
                    "net_cash_flow": cash, "labor_cost": labor}


def _seed_synthetic_history(conn):
    """Insert the deterministic fixture graph + rows and COMMIT."""
    tag = uuid.uuid4().hex[:8]
    months, series = _fixture_series()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO customers (name, email, city) VALUES (%s, %s, %s)"
            " RETURNING id",
            (f"QAtest_pred_{tag} customer", f"QAtest_pred_{tag}@example.test",
             "QA City"),
        )
        customer_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO job_types (name, default_hourly_rate,"
            " typical_duration_hours) VALUES (%s, 50.00, 8.00) RETURNING id",
            (f"QAtest_pred_{tag} type",),
        )
        job_type_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO employees (first_name, last_name, email, hire_date,"
            " hourly_rate) VALUES (%s, %s, %s, %s, 50.00) RETURNING id",
            ("QA", f"Predictive_{tag}", f"QAtest_pred_{tag}.emp@example.test",
             datetime.date(2020, 1, 1)),
        )
        employee_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO jobs (job_number, customer_id, job_type_id, job_date,"
            " quoted_price, status) VALUES (%s, %s, %s, %s, 10000.00,"
            " 'completed') RETURNING id",
            (f"QAtest-PRED-{tag}-JOB", customer_id, job_type_id,
             months[0]),
        )
        job_id = cur.fetchone()[0]

        for t, month in enumerate(months):
            day = month  # 15th of the month
            # Ledger: revenue + materials + overhead (all unattributed
            # overhead-style rows: job_id/customer_id both NULL, which the
            # chk_ledger_single_attribution rule explicitly allows).
            cur.execute(
                "INSERT INTO financial_ledger (entry_type, category, entry_date,"
                " amount, description) VALUES (%s, %s, %s, %s, %s)",
                ("revenue", "QAtest services", day, series["revenue"][t],
                 "QAtest predictive revenue"),
            )
            cur.execute(
                "INSERT INTO financial_ledger (entry_type, category, entry_date,"
                " amount, description) VALUES (%s, %s, %s, %s, %s)",
                ("expense", "materials", day, series["material_expense"][t],
                 "QAtest predictive materials"),
            )
            cur.execute(
                "INSERT INTO financial_ledger (entry_type, category, entry_date,"
                " amount, description) VALUES (%s, %s, %s, %s, %s)",
                ("expense", "overhead", day,
                 800.0 if t == 13 else 100.0,
                 "QAtest predictive overhead"),
            )
            # Labor: one timesheet row per month (hours <= 24 satisfies the
            # labor_logs CHECK; 20h Jan / 10h otherwise x $50 = 1000 / 500).
            cur.execute(
                "INSERT INTO labor_logs (job_id, employee_id, work_date, hours,"
                " hourly_rate, description) VALUES (%s, %s, %s, %s, 50.00, %s)",
                (job_id, employee_id, day,
                 20.0 if month.month == 1 else 10.0,
                 "QAtest predictive labor"),
            )
    conn.commit()
    return {"tag": tag}


def _seed_revenue_only(conn):
    """12 ledger months (2025-01..2025-12) with revenue + overhead rows only:
    labor_logs stays EMPTY and there is no 'materials' category, so the
    labor_cost and material_expense series have no rows at all."""
    months = [_add_months(datetime.date(2025, 1, 15), t) for t in range(12)]
    with conn.cursor() as cur:
        for day in months:
            cur.execute(
                "INSERT INTO financial_ledger (entry_type, category, entry_date,"
                " amount, description) VALUES ('revenue', 'QAtest services',"
                " %s, 500.00, 'QAtest empty-edge revenue')",
                (day,),
            )
            cur.execute(
                "INSERT INTO financial_ledger (entry_type, category, entry_date,"
                " amount, description) VALUES ('expense', 'overhead',"
                " %s, 100.00, 'QAtest empty-edge overhead')",
                (day,),
            )
    conn.commit()


@pytest.fixture(scope="session")
def pred_env():
    """Session-scoped seeded scratch DB + one engine run of each kind.

    The fixture runs run_backtest(train 10 / test 4) and then run_forecasts(
    months_ahead=3) TWICE (idempotency) against smb_warehouse_qa_pred and
    yields {"conn", "bt", "fc1", "fc2"}. Teardown drops the database even if
    a test fails."""
    conn = None
    try:
        conn = _make_scratch(PRED_SCRATCH_DB)
        _seed_synthetic_history(conn)
        bt = fe.run_backtest(conn, train_months=10, test_months=4)
        fc1 = fe.run_forecasts(conn, months_ahead=3)
        fc2 = fe.run_forecasts(conn, months_ahead=3)
        yield {"conn": conn, "bt": bt, "fc1": fc1, "fc2": fc2}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # pragma: no cover
                pass
        try:
            _drop_scratch(PRED_SCRATCH_DB)
        except Exception as exc:  # pragma: no cover - backstop only
            print(f"WARNING: could not drop {PRED_SCRATCH_DB}: {exc!r}")


@pytest.fixture(scope="session")
def empty_env():
    """Session-scoped scratch DB seeded with revenue+overhead ledger rows
    ONLY (no labor_logs rows, no materials category) plus one engine run of
    each kind, for the empty-series edge."""
    conn = None
    try:
        conn = _make_scratch(EMPTY_SCRATCH_DB)
        _seed_revenue_only(conn)
        bt = fe.run_backtest(conn, train_months=8, test_months=4)
        fc = fe.run_forecasts(conn, months_ahead=2)
        yield {"conn": conn, "bt": bt, "fc": fc}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # pragma: no cover
                pass
        try:
            _drop_scratch(EMPTY_SCRATCH_DB)
        except Exception as exc:  # pragma: no cover - backstop only
            print(f"WARNING: could not drop {EMPTY_SCRATCH_DB}: {exc!r}")


# ===========================================================================
# PART A -- offline unit tests (no database)
# ===========================================================================

def test_simulate_job_reference_scenario():
    """Hand-computed reference: price 10000, hours 100 @ $40, 20% OT share,
    materials 25% of price with a +10% supplier swing, other costs 5%.

      labor     = 100 * 40 * (1 + 0.5 * 0.2) = 4000 * 1.1        = 4400
      materials = 10000 * 0.25 * (1 + 0.10)                      = 2750
      other     = 10000 * 0.05                                   = 500
      margin    = 10000 - 4400 - 2750 - 500                      = 2350
      margin_pct= 2350 / 10000 * 100                             = 23.5%
      vs target = 23.5 - 30.0                                    = -6.5 pp
    """
    r = simulate_job(hours=100, base_rate=40, ot_pct=0.2, material_ratio=0.25,
                     material_change_pct=0.10, price=10000.0)
    assert r["labor_cost"] == 4400.0
    assert r["material_cost"] == 2750.0
    assert r["other_cost"] == 500.0
    assert r["total_cost"] == 7650.0
    assert r["gross_margin"] == 2350.0
    assert abs(r["margin_pct"] - 23.5) < 1e-9
    assert abs(r["target_margin_pct"] - 30.0) < 1e-9
    assert abs(r["margin_vs_target_pct"] - (-6.5)) < 1e-9
    assert r["margin_vs_market_pct"] is None
    assert r["clamps_applied"] == []
    # Inputs round-trip post-clamp (nothing clamped here).
    assert r["ot_pct"] == 0.2 and r["material_change_pct"] == 0.10


def test_simulate_job_rejects_nonpositive_price_and_hours():
    """A quote without revenue (price 0) or without work (hours <= 0) is
    meaningless -> ValueError (documented validation contract)."""
    with pytest.raises(ValueError):
        simulate_job(hours=100, base_rate=40, ot_pct=0.2, material_ratio=0.25,
                     material_change_pct=0.0, price=0)
    with pytest.raises(ValueError):
        simulate_job(hours=-5, base_rate=40, ot_pct=0.2, material_ratio=0.25,
                     material_change_pct=0.0, price=10000.0)
    with pytest.raises(ValueError):
        simulate_job(hours=0, base_rate=40, ot_pct=0.2, material_ratio=0.25,
                     material_change_pct=0.0, price=10000.0)


def test_simulate_job_clamps_and_reports():
    """Documented clamps: ot_pct into [0, 0.5], material_change_pct into
    [-1, 3]; every override is reported in clamps_applied."""
    r = simulate_job(hours=100, base_rate=40, ot_pct=0.9, material_ratio=0.25,
                     material_change_pct=0.0, price=10000.0)
    assert r["ot_pct"] == 0.5
    assert r["clamps_applied"] == ["ot_pct clamped 0.9 -> 0.5"]
    # Labor uses the CLAMPED ot_pct: 100*40*(1+0.5*0.5) = 5000, not 5800.
    assert r["labor_cost"] == 5000.0

    r = simulate_job(hours=100, base_rate=40, ot_pct=0.2, material_ratio=0.25,
                     material_change_pct=-5.0, price=10000.0)
    assert r["material_change_pct"] == -1.0
    assert r["clamps_applied"] == ["material_change_pct clamped -5 -> -1"]
    # Materials floor: 10000*0.25*(1 + -1) = 0 (free materials scenario).
    assert r["material_cost"] == 0.0

    # Upper material clamp keeps a +300% lumber-spike scenario expressible.
    r = simulate_job(hours=100, base_rate=40, ot_pct=0.2, material_ratio=0.25,
                     material_change_pct=9.9, price=10000.0)
    assert r["material_change_pct"] == 3.0
    assert r["material_cost"] == 10000.0 * 0.25 * 4.0

    # Both clamps at once -> two entries.
    r = simulate_job(hours=100, base_rate=40, ot_pct=2.0, material_ratio=0.25,
                     material_change_pct=-7.5, price=10000.0)
    assert r["ot_pct"] == 0.5 and r["material_change_pct"] == -1.0
    assert len(r["clamps_applied"]) == 2


def test_simulate_job_zero_materials_edge():
    """material_ratio 0 -> materials cost exactly 0 and the whole quote
    reduces to margin = price - labor - other."""
    r = simulate_job(hours=10, base_rate=50, ot_pct=0.0, material_ratio=0.0,
                     material_change_pct=0.10, price=2000.0)
    assert r["material_cost"] == 0.0
    assert r["labor_cost"] == 500.0
    assert r["other_cost"] == 100.0
    assert r["gross_margin"] == 2000.0 - 500.0 - 100.0
    assert abs(r["margin_pct"] - 70.0) < 1e-9


def test_simulate_job_market_margin_sign_conventions():
    """margin_vs_market_pct is margin_pct - market_margin*100 in percentage
    points: positive when the quote beats the market benchmark, negative when
    it lags, and None when no market_margin is given. market_margin arrives as
    a FRACTION (0.3632 = a 36.32% benchmark margin)."""
    base = dict(hours=10, base_rate=50, ot_pct=0.0, material_ratio=0.30,
                material_change_pct=0.0, price=2000.0)  # margin 40%
    above = simulate_job(**base, market_margin=0.3632)
    assert abs(above["market_margin_pct"] - 36.32) < 1e-9
    assert abs(above["margin_vs_market_pct"] - (40.0 - 36.32)) < 1e-9
    assert above["margin_vs_market_pct"] > 0

    below = simulate_job(**base, market_margin=0.45)
    assert abs(below["margin_vs_market_pct"] - (40.0 - 45.0)) < 1e-9
    assert below["margin_vs_market_pct"] < 0

    exact = simulate_job(**base, market_margin=0.40)
    assert abs(exact["margin_vs_market_pct"]) < 1e-9

    none = simulate_job(**base, market_margin=None)
    assert none["market_margin_pct"] is None
    assert none["margin_vs_market_pct"] is None


def test_forecast_engine_offline_edge_helpers():
    """Pure-helper edge cases the scratch integration tests lean on (no DB):
    WAPE over negative actuals, MAPE zero-actual exclusion, degenerate
    seasonal indices, year-crossing month arithmetic, constant-series OLS."""
    # WAPE exists BECAUSE actuals can be negative: one negative month flips a
    # MAPE sign but WAPE stays a plain magnitude ratio. (Same pairs as the
    # synthetic net_cash_flow fold below.)
    pairs = [(260.0, 200.0), (260.0, 200.0), (800.0, 800.0), (200.0, -500.0)]
    assert abs(fe._wape_pct(pairs) - (820.0 / 1700.0 * 100.0)) < 1e-9
    # Same pairs under MAPE would give 50% -- the convention must NOT silently
    # swap on net_cash_flow.
    assert abs(fe._mape_pct(pairs) - 50.0) < 1e-9
    assert fe.METRIC_FOR["net_cash_flow"] == "WAPE"
    assert fe._metric_pct("net_cash_flow", pairs) == fe._wape_pct(pairs)
    assert fe._metric_pct("revenue", pairs) == fe._mape_pct(pairs)

    # Zero-actual months are excluded from MAPE (undefined) but their absolute
    # error still counts in WAPE's numerator as long as other months give the
    # denominator mass; with ONLY a zero actual, WAPE is None (0 denominator).
    assert fe._mape_pct([(110.0, 0.0)]) is None
    assert fe._wape_pct([(110.0, 0.0)]) is None
    assert abs(fe._wape_pct([(110.0, 0.0), (110.0, 100.0)]) - 120.0) < 1e-9

    # A series whose values are all zero (empty category zero-filled, or a
    # one-sided ledger month coerced to 0) must not divide by zero anywhere.
    idx = fe._seasonal_indices([(1, 0.0), (2, 0.0)])
    assert idx == {m: 1.0 for m in range(1, 13)}

    # Month arithmetic across the year boundary (Dec 2026 -> Feb 2027 is the
    # live warehouse's current forecast horizon).
    assert fe._add_months(datetime.date(2026, 12, 1), 2) == datetime.date(2027, 2, 1)
    assert fe._add_months(datetime.date(2025, 2, 1), -14) == datetime.date(2023, 12, 1)

    # A perfectly flat history has zero slope and reproduces its mean.
    slope, intercept = fe._least_squares(range(6), [10.0] * 6)
    assert slope == 0.0 and intercept == 10.0


# ===========================================================================
# PART B -- scratch-DB integration tests
# ===========================================================================

def test_backtest_synthetic_fold_structure(pred_env):
    """run_backtest on the 14-month fixture (train 10 / test 4): one fold per
    series, correct window label/dates, 4 predictions each, finite metrics,
    and the fold upsert is idempotent."""
    conn, bt = pred_env["conn"], pred_env["bt"]
    assert set(bt) == {"revenue", "labor_cost", "material_expense",
                       "net_cash_flow"}
    for series, info in bt.items():
        assert "error" not in info, f"{series} unexpectedly skipped: {info}"
        assert info["folds"] == 1
        assert info["metric"] == ("WAPE" if series == "net_cash_flow" else "MAPE")
        assert len(info["predictions"]) == 4
        assert math.isfinite(info["mape"]) and math.isfinite(info["rmse"])

    rows = fetch_dicts(
        conn,
        """SELECT series, model, fold, train_start, train_end, test_start,
                  test_end, mape, rmse, predictions
             FROM backtest_results ORDER BY series""",
    )
    assert len(rows) == 4
    fold_label = "train_2024-01_2024-10_test_2024-11_2025-02"
    for r in rows:
        assert r["model"] == fe.MODEL == "seasonal_trend_v1"
        assert r["fold"] == fold_label
        assert str(r["train_start"]) == "2024-01-01"
        assert str(r["train_end"]) == "2024-10-01"
        assert str(r["test_start"]) == "2024-11-01"
        assert str(r["test_end"]) == "2025-02-01"
        assert len(r["predictions"]) == 4
        assert [p["horizon"] for p in r["predictions"]] == [1, 2, 3, 4]
        assert [p["period"] for p in r["predictions"]] == [
            "2024-11-01", "2024-12-01", "2025-01-01", "2025-02-01"]

    # Idempotency: re-running the same fold upserts in place (still 4 rows).
    bt_again = fe.run_backtest(conn, train_months=10, test_months=4)
    assert all("error" not in bt_again[s] for s in bt_again)
    (n,) = fetch_one(conn, "SELECT count(*) FROM backtest_results")
    assert n == 4


def test_backtest_synthetic_fold_exact_values(pred_env):
    """The stored fold predictions equal the HAND-DERIVED closed form (see the
    fixture docstring); since the fixture is exactly solvable this also pins
    the model math (seasonal index -> level -> prediction) end to end."""
    conn = pred_env["conn"]

    def fold_row(series):
        return fetch_one(
            conn,
            "SELECT mape, rmse, predictions FROM backtest_results"
            " WHERE series = %s",
            (series,),
        )

    # revenue & labor_cost share the same 1000-Jan/500-else shape:
    #   train mean 550 -> level 550; Nov/Dec unseen -> idx 1.0 -> 550;
    #   Jan 2025 -> 550 * (1000/550) = 1000; Feb 2025 -> 550 * (500/550) = 500.
    for series in ("revenue", "labor_cost"):
        mape, rmse, preds = fold_row(series)
        assert [float(p["predicted"]) for p in preds] == [550.0, 550.0, 1000.0, 500.0]
        assert [float(p["actual"]) for p in preds] == [500.0, 500.0, 1000.0, 500.0]
        # |550-500|/500 = 10%, perfect months 0%.
        assert [p["abs_pct_err"] for p in preds] == [10.0, 10.0, 0.0, 0.0]
        # MAPE = (0.10 + 0.10 + 0 + 0) / 4 = 5%.
        assert abs(float(mape) - 5.0) < 0.01
        # RMSE = sqrt((50^2 + 50^2 + 0 + 0) / 4) = sqrt(1250) = 35.36.
        assert abs(float(rmse) - round(math.sqrt(1250.0), 2)) < 0.01

    # material_expense: 100-Jan/200-else -> train mean 190 -> level 190;
    # predictions [190, 190, 100, 200] vs actuals [200, 200, 100, 200].
    mape, rmse, preds = fold_row("material_expense")
    assert [float(p["predicted"]) for p in preds] == [190.0, 190.0, 100.0, 200.0]
    assert [float(p["actual"]) for p in preds] == [200.0, 200.0, 100.0, 200.0]
    assert [p["abs_pct_err"] for p in preds] == [5.0, 5.0, 0.0, 0.0]
    # MAPE = (10/200 + 10/200) / 4 = 2.5%; RMSE = sqrt(200/4) = sqrt(50).
    assert abs(float(mape) - 2.5) < 0.01
    assert abs(float(rmse) - round(math.sqrt(50.0), 2)) < 0.01


def test_backtest_wape_convention_net_cash_flow(pred_env):
    """WAPE convention: the synthetic net_cash_flow series has one negative
    month (Feb 2025 = -500). The stored metric MUST be WAPE.

    Fold pairs (predicted, actual):
        (260, 200)  (260, 200)  (800, 800)  (200, -500)
    WAPE = SUM|err| / SUM|actual|
         = (60 + 60 + 0 + 700) / (200 + 200 + 800 + 500) = 820/1700
         = 48.235294...%  -> stored 48.24
    MAPE (the WRONG convention here) would be
         = (60/200 + 60/200 + 0/800 + 700/500) / 4 = (0.3+0.3+0+1.4)/4
         = 50.0%  -- clearly distinguishable from the stored value.
    """
    conn = pred_env["conn"]
    mape, rmse, preds = fetch_one(
        conn,
        "SELECT mape, rmse, predictions FROM backtest_results"
        " WHERE series = 'net_cash_flow'",
    )
    assert [float(p["predicted"]) for p in preds] == [260.0, 260.0, 800.0, 200.0]
    assert [float(p["actual"]) for p in preds] == [200.0, 200.0, 800.0, -500.0]
    # abs_pct_err is per-row |err|/|actual| and is defined for the negative
    # actual too: 700/500 = 140%.
    assert [p["abs_pct_err"] for p in preds] == [30.0, 30.0, 0.0, 140.0]
    assert abs(float(mape) - 48.24) < 0.01          # WAPE = 820/1700
    assert abs(float(mape) - 50.0) > 0.5            # and NOT the MAPE value
    # RMSE = sqrt((60^2 + 60^2 + 0 + 700^2) / 4) = sqrt(124300) = 352.56.
    assert abs(float(rmse) - round(math.sqrt(124300.0), 2)) < 0.01

    # The forecasts' accuracy columns follow the SAME convention (WAPE for
    # net_cash_flow), straight from the pooled fold stats.
    (fc_mape,) = fetch_one(
        conn, "SELECT mape FROM forecasts WHERE series = 'net_cash_flow'"
              " AND horizon_months = 1")
    assert abs(float(fc_mape) - 48.24) < 0.01


def test_backtest_and_forecasts_work_without_macro(pred_env):
    """No-look-ahead / fallback: the scratch DB has ZERO market_benchmarks
    rows, so every fold and forecast must run on the model trend alone
    (documented 'macro_missing' fallback) and still match the hand-derived
    predictions. If the engine accidentally read the LIVE warehouse's macro
    data, the drift would be nonzero and these exact values would break."""
    conn = pred_env["conn"]
    (n_macro,) = fetch_one(conn, "SELECT count(*) FROM market_benchmarks")
    assert n_macro == 0, "fixture must contain no macro rows"

    # Backtest ran fine without macro (exact macro-free predictions).
    _, _, preds = fetch_one(
        conn, "SELECT mape, rmse, predictions FROM backtest_results"
              " WHERE series = 'revenue'")
    assert [float(p["predicted"]) for p in preds] == [550.0, 550.0, 1000.0, 500.0]

    # The forecasts' provenance records the fallback explicitly.
    for series in ("revenue", "labor_cost", "material_expense", "net_cash_flow"):
        (details,) = fetch_one(
            conn,
            "SELECT details FROM forecasts WHERE series = %s"
            " AND horizon_months = 1",
            (series,),
        )
        drift = details["drift"]
        assert drift["macro_missing"] is True
        assert "no macro rows" in drift["macro_note"]
        assert drift["market_annual_pct"] is None
        assert drift["market_monthly_pct"] is None
        # drift = model trend only (no market half).
        assert drift["monthly_pct"] == drift["model_monthly_pct"]
        assert details["sigma_source"] in ("backtest", "in_sample_residuals")


def test_forecasts_horizon_dates_and_values(pred_env):
    """run_forecasts: horizon dates are the next N month starts after the last
    history month (2025-02 -> Mar/Apr/May 2025); the seasonal factor is
    applied (hand-checked level x factor = predicted); the pooled backtest
    metrics ride along in mape/rmse; upsert is idempotent."""
    conn, fc1, fc2 = pred_env["conn"], pred_env["fc1"], pred_env["fc2"]

    (n,) = fetch_one(conn, "SELECT count(*) FROM forecasts")
    assert n == 12, f"expected 12 forecast rows, got {n}"

    rows = fetch_dicts(
        conn,
        """SELECT series, period_start, model, predicted, mape, rmse,
                  horizon_months, details
             FROM forecasts ORDER BY series, horizon_months""",
    )
    for series in ("revenue", "labor_cost", "material_expense", "net_cash_flow"):
        srows = [r for r in rows if r["series"] == series]
        assert [str(r["period_start"]) for r in srows] == [
            "2025-03-01", "2025-04-01", "2025-05-01"]
        assert [r["horizon_months"] for r in srows] == [1, 2, 3]
        assert all(r["model"] == "seasonal_trend_v1" for r in srows)

    # Hand-derived point forecasts (see the fixture docstring):
    #   revenue / labor: level 4000/7 x idx[Mar] 7/8 = 500.00 for all horizons
    #                    (drift 0: flat history, no macro).
    #   material:        level 185.71 x idx[Mar] 14/13 = 200.00 x3.
    #   net_cash:        the double-observed February forces a real trend:
    #                    raw growth 6.8376%/mo WINSORIZED to 5%/mo, level
    #                    5500/7 x idx[Mar] 28/33 = 666.6667 -> 700.00, 735.00,
    #                    771.75 (the winsor clamp BINDS here).
    expected_pred = {"revenue": [500.0, 500.0, 500.0],
                     "labor_cost": [500.0, 500.0, 500.0],
                     "material_expense": [200.0, 200.0, 200.0],
                     "net_cash_flow": [700.0, 735.0, 771.75]}
    for series, exp in expected_pred.items():
        got = [float(r["predicted"]) for r in rows if r["series"] == series]
        assert got == exp, f"{series}: {got} != {exp}"

    # Pooled backtest metrics carried into the forecasts' columns.
    expected_metric = {"revenue": (5.0, round(math.sqrt(1250.0), 2)),
                       "labor_cost": (5.0, round(math.sqrt(1250.0), 2)),
                       "material_expense": (2.5, round(math.sqrt(50.0), 2)),
                       "net_cash_flow": (48.24, round(math.sqrt(124300.0), 2))}
    for series, (m, r) in expected_metric.items():
        srows = [x for x in rows if x["series"] == series]
        assert abs(float(srows[0]["mape"]) - m) < 0.01
        assert abs(float(srows[0]["rmse"]) - r) < 0.01

    # Multi-tenant provenance (migration 001): every forecast row carries the
    # workspace's company_id, and the details JSONB records it plus the
    # workspace's display name.
    (fc_co,) = fetch_one(conn, "SELECT DISTINCT company_id FROM forecasts")
    assert fc_co == 1, f"forecasts must belong to workspace 1, got {fc_co}"
    (co_in_details,) = fetch_one(
        conn, "SELECT details->>'company_id' FROM forecasts"
              " WHERE series = 'revenue' AND horizon_months = 1")
    assert co_in_details == "1"
    (co_name_in_details,) = fetch_one(
        conn, "SELECT details->>'company_name' FROM forecasts"
              " WHERE series = 'revenue' AND horizon_months = 1")
    assert co_name_in_details == "BrightPath Works (Demo)"

    # Seasonal factors as stored in details (revenue fit on all 14 months:
    # Jan idx 1.75, Mar idx 0.875; level 571.43).
    (rev_details,) = fetch_one(
        conn, "SELECT details FROM forecasts WHERE series = 'revenue'"
              " AND horizon_months = 1")
    assert rev_details["seasonal_factors"]["1"] == 1.75
    assert rev_details["seasonal_factors"]["3"] == 0.875
    assert abs(rev_details["level_deseasonalized"] - 571.43) < 0.01

    # Winsorization pin on net_cash_flow: the raw monthly growth exceeded the
    # +/-5%/mo clamp, and the drift used the CLAMPED value.
    (cash_details,) = fetch_one(
        conn, "SELECT details FROM forecasts WHERE series = 'net_cash_flow'"
              " AND horizon_months = 1")
    assert cash_details["trend"]["winsorized_monthly_growth_pct"] == 5.0
    assert cash_details["trend"]["monthly_growth_pct"] > 5.0
    assert cash_details["drift"]["monthly_pct"] == 5.0

    # Upsert idempotency: the second run returns the same 12 keys with the
    # same numbers (run_date may advance; nothing else may drift).
    series_names = ("revenue", "labor_cost", "material_expense", "net_cash_flow")
    keys1 = {(r["series"], str(r["period_start"]))
             for s in series_names for r in fc1[s]}
    assert len(keys1) == 12
    for s in series_names:
        for r2 in fc2[s]:
            r1 = next(r for r in fc1[s]
                      if r["period_start"] == r2["period_start"])
            assert (r1["predicted"], r1["ci_low"], r1["ci_high"],
                    r1["mape"], r1["rmse"], r1["horizon_months"]) == \
                   (r2["predicted"], r2["ci_low"], r2["ci_high"],
                    r2["mape"], r2["rmse"], r2["horizon_months"])
    (n_again,) = fetch_one(conn, "SELECT count(*) FROM forecasts")
    assert n_again == 12


def test_forecasts_ci_ordering_per_series_kind(pred_env):
    """CI contract: ci = predicted +/- 1.645 * sigma * sqrt(h) with sigma =
    the pooled backtest RMSE; ci_low clamped >= 0 for the three non-negative
    series; net_cash_flow is NOT clamped (negative cash projections are real
    and its h>=2 lower bounds are genuinely negative here)."""
    conn = pred_env["conn"]

    # sigma per series recomputed independently from the stored fold's
    # predictions JSONB (single fold -> pooled sigma = fold RMSE unrounded).
    sigma = {}
    for series in ("revenue", "labor_cost", "material_expense", "net_cash_flow"):
        (_, preds) = fetch_one(
            conn, "SELECT mape, predictions FROM backtest_results"
                  " WHERE series = %s", (series,))
        pairs = [(float(p["predicted"]), float(p["actual"])) for p in preds]
        sigma[series] = math.sqrt(
            sum((p - a) ** 2 for p, a in pairs) / len(pairs))

    rows = fetch_dicts(
        conn,
        """SELECT series, predicted, ci_low, ci_high, horizon_months
             FROM forecasts ORDER BY series, horizon_months""",
    )
    assert len(rows) == 12
    for r in rows:
        s = r["series"]
        pred, lo, hi = float(r["predicted"]), float(r["ci_low"]), float(r["ci_high"])
        half = Z90 * sigma[s] * math.sqrt(r["horizon_months"])
        assert lo <= pred <= hi, f"{s} h={r['horizon_months']}: CI ordering broken"
        # Upper side is never clamped -> matches the formula within rounding.
        assert abs(hi - (pred + half)) <= 0.02
        if s in ("revenue", "labor_cost", "material_expense"):
            assert lo >= 0.0, f"{s}: ci_low must be clamped >= 0"
        # net_cash_flow: h=2/h=3 raw lower bounds are negative and must NOT
        # have been clamped (735 - 1.645*sigma*sqrt(2) < 0 for sigma 352.56).
        if s == "net_cash_flow" and r["horizon_months"] >= 2:
            assert lo < 0.0, "net_cash_flow ci_low must not be clamped"
        expected_lo = pred - half
        if s in ("revenue", "labor_cost", "material_expense"):
            expected_lo = max(0.0, expected_lo)
        assert abs(lo - expected_lo) <= 0.02

    # sigma source is recorded as the backtest (a backtest ran before the
    # forecasts in this fixture).
    (src,) = fetch_one(
        conn, "SELECT details->>'sigma_source' FROM forecasts"
              " WHERE series = 'revenue' AND horizon_months = 1")
    assert src == "backtest"


def test_empty_series_skipped_gracefully(empty_env):
    """Empty-series edge: labor_logs has NO rows and the ledger has no
    'materials' category -> the engine must SKIP both series gracefully
    (documented behaviour), not crash:
      * run_backtest: the skipped series come back as {'error': ..., 'months':
        0} entries (they ARE in the dict, contrary to the old docstring's
        'absent from the dict' -- pinned actual behaviour), and NOTHING is
        written for them;
      * run_forecasts: {'error': ...} in metrics, [] rows, zero DB rows.
    The two populated series (revenue, net_cash_flow) still produce their
    fold and forecasts."""
    conn, bt, fc = empty_env["conn"], empty_env["bt"], empty_env["fc"]

    for series in ("labor_cost", "material_expense"):
        info = bt[series]
        assert "error" in info, f"{series} should be skipped, got {info}"
        assert info["months"] == 0
        assert series not in fc or fc[series] == []
        err = fc["metrics"][series]
        assert "error" in err and "12" in err["error"]  # "only 0 months ... >= 12"

    # Populated series ran fine.
    for series in ("revenue", "net_cash_flow"):
        assert "error" not in bt[series]
        assert bt[series]["folds"] == 1
        assert len(fc[series]) == 2

    # Nothing leaked into the engine output tables for the empty series.
    (bt_rows,) = fetch_one(conn, "SELECT count(*) FROM backtest_results")
    assert bt_rows == 2
    (fc_rows,) = fetch_one(conn, "SELECT count(*) FROM forecasts")
    assert fc_rows == 4  # revenue + net_cash_flow x 2 months
    (empty_rows,) = fetch_one(
        conn,
        "SELECT count(*) FROM forecasts WHERE series IN"
        " ('labor_cost', 'material_expense')")
    assert empty_rows == 0

    # The 12-month revenue-only history forecasts cleanly (flat 500 -> Jan and
    # Feb 2026 at 500.00, zero-width CI because the fold RMSE is 0).
    rev = fetch_dicts(
        conn,
        "SELECT period_start, predicted, ci_low, ci_high FROM forecasts"
        " WHERE series = 'revenue' ORDER BY horizon_months")
    assert [str(r["period_start"]) for r in rev] == ["2026-01-01", "2026-02-01"]
    assert [float(r["predicted"]) for r in rev] == [500.0, 500.0]
    assert float(rev[0]["ci_low"]) == float(rev[0]["ci_high"]) == 500.0


# ===========================================================================
# PART C -- hygiene: no QA residue may ever reach the live warehouse
# ===========================================================================

def test_engine_db_guard_is_active():
    """The module-level guard replaced forecast_engine.DB with a nonexistent
    database, so an engine call that forgets the scratch connection fails
    loudly instead of writing to the live smb_warehouse."""
    assert fe.DB == _GUARD_DB
    with pytest.raises(Exception):
        fe._fetch_monthly(None, "revenue")  # conn=None -> connects to the guard DB


def test_no_qa_leak_into_live_warehouse():
    """READ-ONLY probe of the live smb_warehouse: none of this suite's QAtest
    fixture rows may appear in its forecast-engine output tables. This suite
    never writes to smb_warehouse (every engine call gets the scratch
    connection, and the DB default is guarded); this test proves it stayed
    that way. Skipped when the live DB is unreachable."""
    live_url = "postgresql://postgres:postgres@localhost:5432/smb_warehouse"
    try:
        conn = psycopg2.connect(live_url, connect_timeout=3)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"live smb_warehouse not reachable: {exc!r}")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM forecasts WHERE model LIKE 'QAtest%'"
                " OR series LIKE 'QAtest%'")
            (fc_qa,) = cur.fetchone()
            cur.execute(
                "SELECT count(*) FROM backtest_results WHERE model LIKE"
                " 'QAtest%' OR series LIKE 'QAtest%' OR fold LIKE 'QAtest%'")
            (bt_qa,) = cur.fetchone()
        assert fc_qa == 0, f"{fc_qa} QAtest rows leaked into live forecasts"
        assert bt_qa == 0, f"{bt_qa} QAtest rows leaked into live backtest_results"
    finally:
        conn.close()
