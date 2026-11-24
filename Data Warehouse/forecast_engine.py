#!/usr/bin/env python3
"""
forecast_engine.py -- monthly forecasting for the SMB Financial & Operational
Data Warehouse.

MULTI-TENANT: the engine forecasts ONE client workspace at a time --
run_forecasts(company_id=...) / run_backtest(company_id=...) read that
workspace's own monthly series (financial_ledger / labor_logs rows scoped by
company_id) and write forecasts rows carrying that company_id (upserted per
(company_id, series, period_start, model)). backtest_results stays GLOBAL
(model audit trail: walk-forward folds document the model's accuracy, keyed
by series/model/fold; the latest run's folds win the upsert).

Writes two tables defined in schema.sql:
  * forecasts          -- point forecasts + 90% confidence intervals, upserted
                          per (company_id, series, period_start, model)
  * backtest_results   -- walk-forward validation folds, upserted per
                          (series, model, fold)

CLI:  python3 forecast_engine.py forecast|backtest|all [--months 3]
      [--company-id 1]
      ('all' runs the backtest FIRST so the forecasts' confidence intervals
       come from real out-of-sample errors, then the forecasts.)

==============================================================================
MODEL: seasonal_trend_v1 -- documented exactly, implemented in pure Python
(stdlib only; no statsmodels/scipy -- least squares, seasonal indices and
residual sigma are closed-form)
==============================================================================

Per series, given a monthly history y[0..n-1] (calendar months, month starts):

1. MONTHLY HISTORY -- one value per calendar month, months with no activity
   zero-filled between the series' first and last observed month:
     revenue          = ledger revenue sums per month (financial_ledger,
                        entry_type='revenue')
     labor_cost       = SUM(hours * hourly_rate) per month from labor_logs
     material_expense = ledger expenses per month, category='materials'
     net_cash_flow    = monthly (revenue - total expense) from the ledger
                        (a real cash series: months CAN be negative)

2. SEASONAL INDICES -- multiplicative, one per calendar month, computed on the
   TRAIN window only:  seasonal[m] = mean(y over train months with calendar
   month m) / mean(y over the whole train window).  Requires >= 12 train
   months; otherwise every index stays 1.0 (no seasonality assumed) and the
   run notes it in the details JSONB.

3. DE-SEASONALIZE + TREND:  y_deseason[t] = y[t] / seasonal[month(t)], then an
   ordinary least-squares line (slope, intercept) is fit over t = 0..n-1.

4. DRIFT BLENDING -- the monthly growth rate applied forward is
       drift = 0.5 * model_trend_growth + 0.5 * market_growth
   * model_trend_growth = slope / (fitted trend level at the last train
     month), WINSORIZED to +/-5% per month (a noisy small-business trend must
     never compound into an absurd projection).
   * market_growth = the market's annual inflation converted to monthly
     ((1 + annual)^(1/12) - 1), taken from market_benchmarks MACRO rows
     (benchmark_kind 'wage'/'ppi', region 'us-national'):
       labor_cost        -> CES2000000008  (BLS construction wage,
                            production & nonsupervisory employees)
       material_expense  -> WPUSI012011    (PPI: construction materials)
       revenue /
       net_cash_flow     -> average of the wage + construction-materials YoY
   * The annual YoY is (latest macro value / value 12 months earlier - 1).
     Backtest folds compute it AS OF the fold's train_end (latest macro
     period <= train_end) so no future data leaks into a validation fold.
     If macro data is missing the drift falls back to the model trend alone
     and says so in the details JSONB ("macro_missing": true).

5. PREDICTION for horizon h = 1..months_ahead (h = 1 is the month right after
   the train window's last month):
       predicted[h] = level * seasonal[month(h)] * (1 + drift)^h
   where level = the LAST train month's DE-SEASONALIZED value
   (y[n-1] / seasonal[month(n-1)]). Growth is applied ONLY through the drift
   factor, so the trend line drives growth without being double-counted.
   NOTE: level is a single last observation -- deliberately literal; a
   one-month spike in the last month propagates into the forecast (documented
   sensitivity, see README section 11).

6. CONFIDENCE INTERVALS (90%):  ci = predicted +/- 1.645 * sigma * sqrt(h)
   * sigma comes from the BACKTEST when one exists (root mean squared error
     pooled across all of the series' walk-forward fold predictions), else
     from the IN-SAMPLE residuals of the trend x seasonal fit on first run
     (sqrt of mean squared residual, n-1 denominator). The source is recorded
     in details JSONB ("sigma_source": "backtest" | "in_sample_residuals").
   * 1.645 is the two-sided 90% normal quantile; sqrt(h) widens the band
     with horizon.
   * ci_low is clamped >= 0 for the non-negative series (revenue, labor_cost,
     material_expense); net_cash_flow is NOT clamped -- negative cash
     projections are real and meaningful.

7. ACCURACY METRIC CONVENTION (MAPE vs WAPE):
   * revenue / labor_cost / material_expense are positive (or zero) by
     nature -> MAPE = mean(|error| / |actual|) over test months with
     actual > 0 (zero-actual months are excluded -- MAPE is undefined there).
   * net_cash_flow has genuinely negative months, so |error|/|actual| can be
     meaningless or undefined -> WAPE = SUM(|error|) / SUM(|actual|) over all
     test months. WAPE is the convention for net_cash_flow EVERYWHERE in the
     forecast layer (forecasts.mape and backtest_results.mape both hold
     MAPE for the positive series and WAPE for net_cash_flow; the column
     comments say so).

API:
    run_forecasts(conn=None, months_ahead=3, company_id=1)
        -> {"revenue": [...rows...], "labor_cost": [...], "material_expense":
            [...], "net_cash_flow": [...], "metrics": {series: {...}}}
        Rows are the upserted forecasts tuples as dicts (each carries
        company_id). The series history is read from THAT workspace's
        ledger/labor_logs rows only.
    run_backtest(conn=None, train_months=30, test_months=6, company_id=1)
        -> {series: {"mape": .., "rmse": .., "predictions": [...],
                     "folds": .., "metric": "MAPE"|"WAPE"}}
        (mape holds WAPE for net_cash_flow, per the convention above. The
        folds themselves are written to the GLOBAL backtest_results audit
        trail -- no company_id there; the workspace scope only decides whose
        history feeds the folds.)
    Pass your own psycopg2 connection, or None to let the engine open one.

Run:  python3 forecast_engine.py forecast|backtest|all [--months 3]
      [--company-id 1]
"""

import argparse
import datetime as dt
import math
import os

import psycopg2
from psycopg2.extras import Json, execute_values

# Honors DATABASE_URL (used by the Docker stack: postgresql://postgres:postgres@db:5432/...).
# NOTE: tests monkeypatch this module-level DB variable directly (test_predictive.py);
# keep the name `DB`.
DB = os.environ.get("DATABASE_URL",
                    "postgresql://postgres:postgres@localhost:5432/smb_warehouse")

MODEL = "seasonal_trend_v1"
SERIES = ("revenue", "labor_cost", "material_expense", "net_cash_flow")

# Series whose actuals can only be >= 0 (drives both the CI clamp and the
# MAPE-vs-WAPE metric convention; net_cash_flow is the odd one out).
NON_NEGATIVE_SERIES = {"revenue", "labor_cost", "material_expense"}
METRIC_FOR = {s: ("WAPE" if s == "net_cash_flow" else "MAPE") for s in SERIES}

Z_90 = 1.6448536269514722          # two-sided 90% normal quantile (rounded 1.645)
CI_LEVEL_PCT = 90.0
WINSOR_MONTHLY = 0.05              # model trend growth clamped to +/-5%/mo
MIN_TRAIN_MONTHS = 12              # below this: no seasonal indices, no fit

# Macro market_benchmarks rows backing the market half of the drift blend
# (must match the series fetch_benchmarks.py loads; region pinned to
# 'us-national' where those series live).
MACRO_FOR_SERIES = {
    "labor_cost":      ["CES2000000008"],
    "material_expense": ["WPUSI012011"],
    # revenue / net_cash_flow: average of the wage + construction-materials YoY
    "revenue":         ["CES2000000008", "WPUSI012011"],
    "net_cash_flow":   ["CES2000000008", "WPUSI012011"],
}

# ---------------------------------------------------------------------------
# Monthly series queries (month starts, zero-filled between first/last month).
# Every query is scoped to ONE client workspace via company_id (bound as the
# last parameter by _fetch_monthly) -- an engine run forecasts THAT
# workspace's history only, never a blend of two businesses.
# ---------------------------------------------------------------------------
MONTHLY_SQL = {
    "revenue": """
        SELECT date_trunc('month', entry_date)::date AS month_start,
               SUM(amount) AS value
          FROM financial_ledger
         WHERE entry_type = 'revenue'
           AND company_id = %s
         GROUP BY 1 ORDER BY 1""",
    "labor_cost": """
        SELECT date_trunc('month', work_date)::date AS month_start,
               SUM(hours * hourly_rate) AS value
          FROM labor_logs
         WHERE company_id = %s
         GROUP BY 1 ORDER BY 1""",
    "material_expense": """
        SELECT date_trunc('month', entry_date)::date AS month_start,
               SUM(amount) AS value
          FROM financial_ledger
         WHERE entry_type = 'expense' AND category = 'materials'
           AND company_id = %s
         GROUP BY 1 ORDER BY 1""",
    # Cash P&L: revenue minus ALL expenses (overhead included) per month.
    # COALESCE guards one-sided months (e.g. revenue but zero recorded
    # expenses): without it, SUM FILTER yields NULL and NULL - value would
    # coerce to 0.0, recording cash of 0 instead of +revenue.
    "net_cash_flow": """
        SELECT date_trunc('month', entry_date)::date AS month_start,
               COALESCE(SUM(amount) FILTER (WHERE entry_type = 'revenue'), 0)
             - COALESCE(SUM(amount) FILTER (WHERE entry_type = 'expense'), 0) AS value
          FROM financial_ledger
         WHERE company_id = %s
         GROUP BY 1 ORDER BY 1""",
}

# COMPANY_NAME_SQL: the workspace's display name for the forecasts.details
# provenance (company_id + company_name ride along in every upserted row).
COMPANY_NAME_SQL = """
    SELECT company_name FROM companies WHERE company_id = %s
"""

FORECAST_UPSERT_SQL = """
    INSERT INTO forecasts (company_id, series, period_start, model, predicted,
                           ci_low, ci_high, mape, rmse, horizon_months,
                           run_date, details)
    VALUES %s
    ON CONFLICT (company_id, series, period_start, model) DO UPDATE SET
        predicted      = EXCLUDED.predicted,
        ci_low         = EXCLUDED.ci_low,
        ci_high        = EXCLUDED.ci_high,
        mape           = EXCLUDED.mape,
        rmse           = EXCLUDED.rmse,
        horizon_months = EXCLUDED.horizon_months,
        run_date       = EXCLUDED.run_date,
        details        = EXCLUDED.details
"""

BACKTEST_UPSERT_SQL = """
    INSERT INTO backtest_results (series, model, fold, train_start, train_end,
                                  test_start, test_end, mape, rmse, predictions)
    VALUES %s
    ON CONFLICT (series, model, fold) DO UPDATE SET
        train_start = EXCLUDED.train_start,
        train_end   = EXCLUDED.train_end,
        test_start  = EXCLUDED.test_start,
        test_end    = EXCLUDED.test_end,
        mape        = EXCLUDED.mape,
        rmse        = EXCLUDED.rmse,
        predictions = EXCLUDED.predictions,
        created_at  = now()
"""


# ---------------------------------------------------------------------------
# Pure math helpers (unit-testable, no DB, no I/O)
# ---------------------------------------------------------------------------
def _add_months(month, k):
    """First-of-month date + k months (k may be negative)."""
    y, m = month.year, month.month + k
    y += (m - 1) // 12
    m = (m - 1) % 12 + 1
    return dt.date(y, m, 1)


def _least_squares(xs, ys):
    """Ordinary least squares over paired iterables -> (slope, intercept).

    Closed form: slope = Sxy / Sxx, intercept = ybar - slope * xbar. For a
    constant x series (degenerate) returns (0.0, mean(ys))."""
    xs = list(xs)
    ys = list(ys)
    n = len(xs)
    if n == 0 or n != len(ys):
        raise ValueError("least_squares needs equal-length non-empty inputs")
    xbar = sum(xs) / n
    ybar = sum(ys) / n
    sxx = sum((x - xbar) ** 2 for x in xs)
    if sxx == 0:
        return 0.0, ybar
    sxy = sum((x - xbar) * (y - ybar) for x, y in zip(xs, ys))
    slope = sxy / sxx
    return slope, ybar - slope * xbar


def _seasonal_indices(values_by_month):
    """Multiplicative seasonal index per CALENDAR month (1..12).

    values_by_month: iterable of (calendar_month_int, value). Index = that
    calendar month's mean value / the overall mean. Calendar months never
    seen (or an overall mean of 0) get index 1.0. Returns {1..12: float}."""
    buckets = {}
    total, count = 0.0, 0
    for m, v in values_by_month:
        buckets.setdefault(int(m), []).append(float(v))
        total += float(v)
        count += 1
    if count == 0 or total == 0:
        return {m: 1.0 for m in range(1, 13)}
    overall = total / count
    idx = {m: 1.0 for m in range(1, 13)}
    for m, vals in buckets.items():
        idx[m] = (sum(vals) / len(vals)) / overall
    return idx


def _winsorize(x, lo, hi):
    """Clamp x into [lo, hi]."""
    return max(lo, min(hi, x))


def _mape_pct(pairs):
    """MAPE in % over (predicted, actual) pairs, EXCLUDING zero/negative
    actuals (MAPE is undefined there). None if no usable pairs."""
    errs = [abs(p - a) / abs(a) for p, a in pairs if a != 0]
    if not errs:
        return None
    return sum(errs) / len(errs) * 100.0


def _wape_pct(pairs):
    """WAPE in %: SUM(|error|) / SUM(|actual|) over ALL pairs (works with
    negative actuals -- this is why net_cash_flow uses it). None when the
    actuals sum to 0."""
    denom = sum(abs(a) for _, a in pairs)
    if denom == 0:
        return None
    return sum(abs(p - a) for p, a in pairs) / denom * 100.0


def _metric_pct(series, pairs):
    """Series-appropriate accuracy metric in %: MAPE for the positive series
    (zero actuals excluded), WAPE for net_cash_flow (see module docstring)."""
    if METRIC_FOR[series] == "WAPE":
        return _wape_pct(pairs)
    return _mape_pct(pairs)


def _cap_numeric(x, cap=999999.99):
    """Keep a float inside a NUMERIC(8,2) column's range."""
    if x is None:
        return None
    return round(min(float(x), cap), 2)


# ---------------------------------------------------------------------------
# Data access
# ---------------------------------------------------------------------------
def _fetch_monthly(conn, series, company_id=1):
    """Monthly history for one series as a sorted [(month_date, value)] list,
    zero-filled between the series' first and last observed month. Scoped to
    ONE client workspace: only that company_id's ledger/labor rows count."""
    if conn is None:
        conn = psycopg2.connect(DB)
        owned = True
    else:
        owned = False
    try:
        with conn.cursor() as cur:
            cur.execute(MONTHLY_SQL[series], (company_id,))
            rows = cur.fetchall()
    finally:
        if owned:
            conn.close()
    points = {r[0]: float(r[1] or 0.0) for r in rows}
    if not points:
        return []
    first, last = min(points), max(points)
    out, month = [], first
    while month <= last:
        out.append((month, points.get(month, 0.0)))
        month = _add_months(month, 1)
    return out


def _market_inflation_pct(conn, series, as_of=None):
    """Market ANNUAL inflation (%) for a series from market_benchmarks macro
    rows: (latest value / value 12 months earlier - 1) * 100, averaged over
    the series' macro sources (see MACRO_FOR_SERIES).

    as_of=None  -> use the latest macro period overall (run_forecasts).
    as_of=date  -> use the latest macro period <= as_of, so a backtest fold
                   never sees macro data published after its train window
                   (no look-ahead).
    Returns (annual_pct_or_None, [series_id used], human note)."""
    series_ids = MACRO_FOR_SERIES[series]
    yoy_by_series = {}
    notes = []
    owned = False
    if conn is None:
        conn = psycopg2.connect(DB)
        owned = True
    try:
        with conn.cursor() as cur:
            for sid in series_ids:
                sql = """
                    SELECT period, avg_price
                      FROM market_benchmarks
                     WHERE series_id = %s
                       AND region = 'us-national'
                       AND benchmark_kind IN ('wage', 'ppi')
                """
                params = [sid]
                if as_of is not None:
                    sql += " AND period <= %s"
                    params.append(as_of)
                sql += " ORDER BY period"
                cur.execute(sql, params)
                pts = {r[0]: float(r[1]) for r in cur.fetchall()}
                if not pts:
                    notes.append(f"{sid}: no macro rows")
                    continue
                # Latest period that has a value 12 months earlier; walk back
                # a few periods to tolerate a single missing month.
                yoy = None
                periods = sorted(pts)
                for p in reversed(periods[-4:]):
                    base_p = _add_months(p, -12)
                    if base_p in pts and pts[base_p] > 0:
                        yoy = (pts[p] / pts[base_p] - 1.0) * 100.0
                        break
                if yoy is None:
                    notes.append(f"{sid}: no value 12 months earlier")
                else:
                    yoy_by_series[sid] = yoy
    finally:
        if owned:
            conn.close()
    if not yoy_by_series:
        return None, series_ids, "; ".join(notes) or "macro data missing"
    avg = sum(yoy_by_series.values()) / len(yoy_by_series)
    note = "YoY from " + ", ".join(sorted(yoy_by_series))
    if notes:
        note += " (missing: " + "; ".join(notes) + ")"
    return avg, sorted(yoy_by_series), note


def _company_name(conn, company_id):
    """Display name of a client workspace (or None when the id is unknown).
    Recorded in every forecasts.details JSONB for human-readable provenance."""
    owned = False
    if conn is None:
        conn = psycopg2.connect(DB)
        owned = True
    try:
        with conn.cursor() as cur:
            cur.execute(COMPANY_NAME_SQL, (company_id,))
            row = cur.fetchone()
    finally:
        if owned:
            conn.close()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# The model itself
# ---------------------------------------------------------------------------
def _fit_model(train, market_annual_pct):
    """Fit seasonal_trend_v1 on a train window.

    train: sorted [(month_date, value)], len >= MIN_TRAIN_MONTHS.
    market_annual_pct: market annual inflation % (None -> model trend only).
    Returns a dict with seasonal indices, trend, blended drift, level, the
    in-sample residual sigma and per-horizon predictions (horizons start at
    h=1 = the month after the train window)."""
    n = len(train)
    months = [m for m, _ in train]
    values = [v for _, v in train]

    seasonal = _seasonal_indices([(m.month, v) for m, v in train])
    deseason = [v / seasonal[m.month] for m, v in train]

    slope, intercept = _least_squares(range(n), deseason)
    trend_end = intercept + slope * (n - 1)
    if trend_end > 0:
        model_growth = slope / trend_end
        winsorized = _winsorize(model_growth, -WINSOR_MONTHLY, WINSOR_MONTHLY)
        winsor_note = None
    else:
        # A non-positive trend level has no meaningful percentage growth;
        # fall back to zero monthly growth rather than a division blow-up.
        model_growth, winsorized = 0.0, 0.0
        winsor_note = "non-positive trend level: model growth forced to 0"

    if market_annual_pct is not None:
        market_monthly = (1.0 + market_annual_pct / 100.0) ** (1.0 / 12.0) - 1.0
        drift = 0.5 * winsorized + 0.5 * market_monthly
    else:
        market_monthly = None
        drift = winsorized

    # In-sample residual sigma of the trend x seasonal reconstruction.
    residuals = [values[t] - (intercept + slope * t) * seasonal[months[t].month]
                 for t in range(n)]
    sse = sum(r * r for r in residuals)
    sigma_in_sample = math.sqrt(sse / max(n - 1, 1))

    level = deseason[-1]           # last DE-SEASONALIZED train observation
    return {
        "n_months": n,
        "train_start": months[0],
        "train_end": months[-1],
        "seasonal": seasonal,
        "slope": slope,
        "intercept": intercept,
        "model_growth": model_growth,
        "winsorized_growth": winsorized,
        "winsor_note": winsor_note,
        "market_annual_pct": market_annual_pct,
        "market_monthly": market_monthly,
        "drift": drift,
        "level": level,
        "sigma_in_sample": sigma_in_sample,
        "level_month": months[-1],
    }


def _predict_from_fit(fit, months_ahead):
    """Point predictions [(period, h, value)] for h = 1..months_ahead from a
    fitted model: level * seasonal[month] * (1 + drift)^h."""
    out = []
    for h in range(1, months_ahead + 1):
        period = _add_months(fit["level_month"], h)
        pred = (fit["level"] * fit["seasonal"][period.month]
                * (1.0 + fit["drift"]) ** h)
        out.append((period, h, pred))
    return out


# ---------------------------------------------------------------------------
# Backtest sigma / metrics already stored in the DB
# ---------------------------------------------------------------------------
def _pooled_backtest_stats(conn, series, model=MODEL):
    """Pool ALL stored folds of (series, model) into one sigma (= RMSE) and
    one MAPE/WAPE figure. Returns (sigma_or_None, metric_value_or_None,
    rmse_or_None, n_folds). sigma = sqrt(mean squared error over every fold
    prediction); the metric follows the series convention (WAPE for
    net_cash_flow)."""
    if conn is None:
        conn = psycopg2.connect(DB)
        owned = True
    else:
        owned = False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT predictions FROM backtest_results
                    WHERE series = %s AND model = %s""",
                (series, model),
            )
            folds = [r[0] for r in cur.fetchall()]
    finally:
        if owned:
            conn.close()
    if not folds:
        return None, None, None, 0
    pairs, sq_errs = [], []
    for preds in folds:
        for row in preds or []:
            pred, actual = float(row["predicted"]), float(row["actual"])
            pairs.append((pred, actual))
            sq_errs.append((pred - actual) ** 2)
    sigma = math.sqrt(sum(sq_errs) / len(sq_errs))
    return sigma, _metric_pct(series, pairs), sigma, len(folds)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def run_backtest(conn=None, train_months=30, test_months=6, company_id=1):
    """Walk-forward (expanding-origin) validation of seasonal_trend_v1.

    Fold k trains on the series' first train_months + k*test_months months and
    predicts the NEXT test_months months at horizons h = 1..test_months;
    folds advance while a full test window still fits inside the history (with
    the default 30/6 and 36 months of history there is exactly one fold --
    e.g. 'train_2023-12_2026-05_test_2026-06_2026-11'). Macro drift data is
    read AS OF each fold's train_end (no look-ahead).

    company_id scopes the HISTORY fed to the folds: the backtest validates the
    model against THAT client workspace's own monthly series. The folds are
    written to the GLOBAL backtest_results audit trail (no company_id column
    by design -- it documents model accuracy, not client data; the latest
    run's folds win the (series, model, fold) upsert).

    Writes/updates backtest_results (upsert on series+model+fold) and returns
    {series: {"mape", "rmse", "predictions", "folds", "metric"}} with metrics
    POOLED across the series' folds; a series without enough history (and so
    without any fold) comes back as an {"error": ..., "months": N} entry
    instead of metrics, and nothing is written for it."""
    owned = False
    if conn is None:
        conn = psycopg2.connect(DB)
        owned = True

    summary = {}
    try:
        rows_to_write = []
        for series in SERIES:
            history = _fetch_monthly(conn, series, company_id)
            if len(history) < train_months + test_months:
                summary[series] = {"error": "insufficient history for a "
                                   f"{train_months}+{test_months} month fold",
                                   "months": len(history)}
                continue

            fold_rows, pooled_pairs, pooled_sq, pooled_preds = [], [], [], []
            k = 0
            while True:
                train_end_idx = train_months + k * test_months
                if train_end_idx + test_months > len(history):
                    break
                train = history[:train_end_idx]
                test = history[train_end_idx:train_end_idx + test_months]

                market_pct, macro_used, macro_note = _market_inflation_pct(
                    conn, series, as_of=train[-1][0])
                fit = _fit_model(train, market_pct)
                preds = _predict_from_fit(fit, test_months)

                pairs, pred_json, sq = [], [], []
                for (period, h, pred), (act_month, act_val) in zip(preds, test):
                    pred = round(pred, 2)
                    pairs.append((pred, act_val))
                    sq.append((pred - act_val) ** 2)
                    pct_err = (abs(pred - act_val) / abs(act_val) * 100.0
                               if act_val != 0 else None)
                    pred_json.append({
                        "period": period.isoformat(),
                        "predicted": pred,
                        "actual": round(act_val, 2),
                        "abs_pct_err": round(pct_err, 2) if pct_err is not None else None,
                        "horizon": h,
                    })

                fold_label = (f"train_{train[0][0]:%Y-%m}_{train[-1][0]:%Y-%m}"
                              f"_test_{test[0][0]:%Y-%m}_{test[-1][0]:%Y-%m}")
                fold_rows.append((
                    series, MODEL, fold_label,
                    train[0][0], train[-1][0], test[0][0], test[-1][0],
                    _cap_numeric(_metric_pct(series, pairs)),
                    round(math.sqrt(sum(sq) / len(sq)), 2),
                    Json(pred_json),
                ))
                pooled_pairs.extend(pairs)
                pooled_sq.extend(sq)
                pooled_preds.extend(pred_json)
                k += 1

            with conn.cursor() as cur:
                execute_values(
                    cur, BACKTEST_UPSERT_SQL, fold_rows,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            conn.commit()
            pooled_rmse = math.sqrt(sum(pooled_sq) / len(pooled_sq))
            summary[series] = {
                "mape": _cap_numeric(_metric_pct(series, pooled_pairs)),
                "rmse": round(pooled_rmse, 2),
                "predictions": pooled_preds,
                "folds": len(fold_rows),
                "metric": METRIC_FOR[series],
            }
        return summary
    finally:
        if owned:
            conn.close()


def run_forecasts(conn=None, months_ahead=3, company_id=1):
    """Produce and upsert seasonal_trend_v1 forecasts for ONE client
    workspace's series.

    company_id selects the workspace: the monthly history of every series is
    read from THAT company's financial_ledger / labor_logs rows only, and the
    upserted forecasts rows carry that company_id (ON CONFLICT
    (company_id, series, period_start, model) DO UPDATE, so two workspaces'
    forecasts never overwrite each other).

    months_ahead default 3 (~90 days). CIs use the pooled BACKTEST sigma when
    a backtest has been run for the series (any folds in backtest_results --
    the GLOBAL model audit trail), else the in-sample residual sigma of the
    current fit; the forecasts' mape/rmse columns carry the pooled backtest
    accuracy (MAPE, or WAPE for net_cash_flow) or stay NULL when no backtest
    exists yet.

    Upserts forecasts and returns {"revenue": [...], "labor_cost": [...],
    "material_expense": [...], "net_cash_flow": [...], "metrics":
    {series: {...}}}; rows are dicts mirroring the forecasts columns
    (including company_id). The details JSONB of every row records
    company_id/company_name for provenance."""
    owned = False
    if conn is None:
        conn = psycopg2.connect(DB)
        owned = True

    company_name = _company_name(conn, company_id)
    result = {s: [] for s in SERIES}
    metrics = {}
    try:
        for series in SERIES:
            history = _fetch_monthly(conn, series, company_id)
            if len(history) < MIN_TRAIN_MONTHS:
                metrics[series] = {"error": f"only {len(history)} months of "
                                   "history; need >= "
                                   f"{MIN_TRAIN_MONTHS}"}
                continue

            bt_sigma, bt_metric, bt_rmse, bt_folds = _pooled_backtest_stats(
                conn, series)

            market_pct, macro_used, macro_note = _market_inflation_pct(conn, series)
            fit = _fit_model(history, market_pct)
            preds = _predict_from_fit(fit, months_ahead)

            if bt_sigma is not None:
                sigma, sigma_source = bt_sigma, "backtest"
            else:
                sigma = fit["sigma_in_sample"]
                sigma_source = "in_sample_residuals"

            rows = []
            for period, h, pred in preds:
                half = Z_90 * sigma * math.sqrt(h)
                ci_low = pred - half
                ci_high = pred + half
                if series in NON_NEGATIVE_SERIES:
                    ci_low = max(0.0, ci_low)
                rows.append((
                    company_id, series, period, MODEL, round(pred, 2),
                    round(ci_low, 2), round(ci_high, 2),
                    _cap_numeric(bt_metric), _cap_numeric(bt_rmse, 9_999_999_999_999.99),
                    h, dt.date.today(),
                    Json({
                        "model": MODEL,
                        "company_id": company_id,
                        "company_name": company_name,
                        "metric_type": METRIC_FOR[series],
                        "train_window": {
                            "start": fit["train_start"].isoformat(),
                            "end": fit["train_end"].isoformat(),
                            "months": fit["n_months"],
                        },
                        "seasonal_factors": {str(m): round(v, 4)
                                             for m, v in fit["seasonal"].items()},
                        "level_deseasonalized": round(fit["level"], 2),
                        "trend": {
                            "slope_monthly": round(fit["slope"], 2),
                            "monthly_growth_pct": round(fit["model_growth"] * 100, 4),
                            "winsorized_monthly_growth_pct":
                                round(fit["winsorized_growth"] * 100, 4),
                            "winsor_note": fit["winsor_note"],
                        },
                        "drift": {
                            "monthly_pct": round(fit["drift"] * 100, 4),
                            "model_monthly_pct":
                                round(fit["winsorized_growth"] * 100, 4),
                            "market_annual_pct":
                                round(fit["market_annual_pct"], 4)
                                if fit["market_annual_pct"] is not None else None,
                            "market_monthly_pct":
                                round(fit["market_monthly"] * 100, 4)
                                if fit["market_monthly"] is not None else None,
                            "market_series": macro_used,
                            "macro_note": macro_note,
                            "macro_missing": fit["market_annual_pct"] is None,
                        },
                        "sigma": round(sigma, 2),
                        "sigma_source": sigma_source,
                        "z": round(Z_90, 3),
                        "ci_level_pct": CI_LEVEL_PCT,
                        "backtest": None if bt_metric is None else {
                            "folds": bt_folds,
                            "metric": METRIC_FOR[series],
                            "value_pct": _cap_numeric(bt_metric),
                            "rmse": _cap_numeric(bt_rmse, 9_999_999_999_999.99),
                        },
                    }),
                ))

            with conn.cursor() as cur:
                execute_values(
                    cur, FORECAST_UPSERT_SQL, rows,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            conn.commit()

            result[series] = [
                {
                    "company_id": r[0], "series": r[1], "period_start": r[2],
                    "model": r[3], "predicted": float(r[4]),
                    "ci_low": float(r[5]), "ci_high": float(r[6]),
                    "mape": r[7], "rmse": r[8],
                    "horizon_months": r[9], "run_date": r[10],
                }
                for r in rows
            ]
            metrics[series] = {
                "company_id": company_id,
                "company_name": company_name,
                "months_trained": fit["n_months"],
                "train_window": [fit["train_start"].isoformat(),
                                 fit["train_end"].isoformat()],
                "drift_monthly_pct": round(fit["drift"] * 100, 4),
                "market_annual_pct": (round(fit["market_annual_pct"], 2)
                                      if fit["market_annual_pct"] is not None
                                      else None),
                "sigma": round(sigma, 2),
                "sigma_source": sigma_source,
                "metric_type": METRIC_FOR[series],
                "backtest_metric": _cap_numeric(bt_metric),
                "backtest_rmse": _cap_numeric(bt_rmse, 9_999_999_999_999.99),
                "rows_written": len(rows),
            }
        result["metrics"] = metrics
        return result
    finally:
        if owned:
            conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _fmt_money(x):
    return f"{x:,.2f}"


def _fmt_month(m):
    return m.strftime("%b %Y")


def _print_backtest(summary):
    print("\n=== Backtest: seasonal_trend_v1 walk-forward (train 30m / test 6m) ===")
    print(f"{'series':<18}{'metric':<8}{'value %':>9}{'rmse':>16}"
          f"{'folds':>7}  predictions")
    for series, info in summary.items():
        if "error" in info:
            print(f"{series:<18}  skipped: {info['error']} (months={info['months']})")
            continue
        first = info["predictions"][0]
        last = info["predictions"][-1]
        window = f"{first['period']} .. {last['period']}"
        print(f"{series:<18}{info['metric']:<8}{info['mape']:>9.2f}"
              f"{info['rmse']:>16,.2f}{info['folds']:>7}  {window}")
        for p in info["predictions"]:
            err = f"{p['abs_pct_err']:.2f}%" if p["abs_pct_err"] is not None else "n/a"
            print(f"    {p['period']}  h={p['horizon']}  "
                  f"pred={_fmt_money(p['predicted']):>15}  "
                  f"actual={_fmt_money(p['actual']):>15}  err={err}")
    print()


def _print_forecasts(result, months_ahead):
    print(f"=== Forecasts: seasonal_trend_v1, horizon {months_ahead} month(s), 90% CI ===")
    for series in SERIES:
        rows = result.get(series) or []
        m = result["metrics"].get(series, {})
        if not rows:
            print(f"{series:<18}  skipped: {m.get('error', 'no rows')}")
            continue
        sigma_src = {"backtest": "backtest", "in_sample_residuals": "in-sample"}[
            m["sigma_source"]]
        market = (f"{m['market_annual_pct']:.2f}%/yr"
                  if m["market_annual_pct"] is not None else "n/a (macro missing)")
        bt = (f"{m['backtest_metric']:.2f}%" if m["backtest_metric"] is not None
              else "not run")
        print(f"\n{series}  drift={m['drift_monthly_pct']:+.3f}%/mo  "
              f"market={market}  sigma={_fmt_money(m['sigma'])} ({sigma_src})  "
              f"{m['metric_type']}={bt}")
        for r in rows:
            print(f"    {_fmt_month(r['period_start'])}  "
                  f"predicted={_fmt_money(r['predicted']):>15}  "
                  f"CI [{_fmt_money(r['ci_low'])} .. {_fmt_money(r['ci_high'])}]")
    print()


def main():
    ap = argparse.ArgumentParser(
        description="Seasonal-trend forecasting for the SMB warehouse")
    ap.add_argument("command", choices=["forecast", "backtest", "all"],
                    help="forecast = write forecasts; backtest = walk-forward "
                         "validation; all = backtest first, then forecast "
                         "(so CIs use real out-of-sample sigma)")
    ap.add_argument("--months", type=int, default=3,
                    help="months ahead to forecast (default 3 ~ 90 days)")
    ap.add_argument("--company-id", type=int, default=1, dest="company_id",
                    help="client workspace to forecast (default 1 = the demo "
                         "workspace); the engine reads THAT company's "
                         "ledger/labor history and writes company-scoped "
                         "forecast rows")
    args = ap.parse_args()

    if args.command in ("backtest", "all"):
        summary = run_backtest(company_id=args.company_id)
        _print_backtest(summary)
    if args.command in ("forecast", "all"):
        result = run_forecasts(months_ahead=args.months,
                               company_id=args.company_id)
        _print_forecasts(result, args.months)
        n = sum(len(result[s]) for s in SERIES)
        print(f"Forecasts upserted: {n} rows "
              f"({sum(1 for s in SERIES if result[s])}/4 series).")


if __name__ == "__main__":
    main()
