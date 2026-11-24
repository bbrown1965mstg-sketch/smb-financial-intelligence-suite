#!/usr/bin/env python3
"""
fetch_benchmarks.py -- load REAL public economic baselines into market_benchmarks.

Pulls two families of series and stores them as macro rows in
market_benchmarks (benchmark_kind 'wage' / 'ppi', job_type_id NULL,
series_id = the BLS/FRED series id):

  WAGES (BLS Current Employment Statistics, served via FRED)
    CES2000000003         avg hourly earnings, construction, all employees, US
    CES2000000008         ... production and nonsupervisory employees, US
    SMU29000002000000003  ... construction, all employees, Missouri
    SMU29000002000000008  ... construction, production employees, Missouri
  PPIs (BLS Producer Price Index, served via FRED)
    WPU0822               softwood lumber
    PCU325510325510       paint & coating manufacturing
    WPUSI012011           construction materials (special index)
    WPUID612              materials & components for construction (intermediate demand)

NOTE ON SOURCES: BLS's own API (api.bls.gov) is blocked from this machine
(HTTP 403), so both families are fetched from FRED's keyless CSV endpoint
(fred.stlouisfed.org/graph/fredgraph.csv). FRED serves the underlying BLS
series unchanged; the `source` column records the true provenance of every
row. If api.bls.gov becomes reachable, swapping the fetch function for the
BLS JSON API is a one-function change -- the table layout stays identical.

Idempotent: every row is upserted via ON CONFLICT against the partial unique
index (series_id, region, period) WHERE series_id IS NOT NULL, so re-running
refreshes values in place and never duplicates. Only rows within the window
(--years, default 6) are written; missing observations ('.') are skipped and
sub-monthly frequencies (if any) are averaged per calendar month.

Run:  python3 fetch_benchmarks.py [--years 6] [--dry-run]
"""

import argparse
import csv
import datetime as dt
import io
import os
import urllib.request

import psycopg2
from psycopg2.extras import execute_values

# Honors DATABASE_URL (used by the Docker stack: postgresql://postgres:postgres@db:5432/...)
DB = os.environ.get("DATABASE_URL",
                    "postgresql://postgres:postgres@localhost:5432/smb_warehouse")
FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"

# series_id        = the canonical BLS/FRED series id (stored in the table;
#                    must match the ids pinned in mv_benchmark_comparison)
# region           = benchmark region tag (view pins 'us-national' / 'us-mo')
# kind             = 'wage' | 'ppi'  (stored in benchmark_kind)
# description      = human-readable provenance (stored in `source`)
SERIES = [
    dict(series_id="CES2000000003", region="us-national", kind="wage",
         description="BLS CES: avg hourly earnings, construction, all employees (US), via FRED"),
    dict(series_id="CES2000000008", region="us-national", kind="wage",
         description="BLS CES: avg hourly earnings, construction, production & nonsupervisory employees (US), via FRED"),
    dict(series_id="SMU29000002000000003", region="us-mo", kind="wage",
         description="BLS CES: avg hourly earnings, construction, all employees (Missouri), via FRED"),
    dict(series_id="SMU29000002000000008", region="us-mo", kind="wage",
         description="BLS CES: avg hourly earnings, construction, production employees (Missouri), via FRED"),
    dict(series_id="WPU0822", region="us-national", kind="ppi",
         description="BLS PPI commodity: softwood lumber, via FRED"),
    dict(series_id="PCU325510325510", region="us-national", kind="ppi",
         description="BLS PPI industry: paint, coating & adhesive manufacturing, via FRED"),
    dict(series_id="WPUSI012011", region="us-national", kind="ppi",
         description="BLS PPI commodity special index: construction materials, via FRED"),
    dict(series_id="WPUID612", region="us-national", kind="ppi",
         description="BLS PPI intermediate demand: materials & components for construction, via FRED"),
]

UPSERT_SQL = """
    INSERT INTO market_benchmarks (job_type_id, period, region, benchmark_kind,
                                   series_id, avg_price, avg_labor_cost_per_job,
                                   avg_margin_pct, source)
    VALUES %s
    ON CONFLICT (series_id, region, period) WHERE series_id IS NOT NULL
    DO UPDATE SET avg_price      = EXCLUDED.avg_price,
                  benchmark_kind = EXCLUDED.benchmark_kind,
                  source         = EXCLUDED.source
"""


def fetch_series(series_id):
    """Fetch the full history for one FRED series as a CSV, returning a list
    of (date, float value). Raises on HTTP errors."""
    url = FRED_CSV.format(series_id=series_id)
    req = urllib.request.Request(url, headers={"User-Agent": "smb-warehouse-fetcher/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        text = resp.read().decode("utf-8", errors="replace")
    out = []
    for row in csv.reader(io.StringIO(text)):
        if not row or row[0].strip().lower() in ("observation_date", "date"):
            continue
        date_str, val_str = row[0], row[1]
        if val_str.strip() in (".", ""):          # FRED marks missing data '.'
            continue
        try:
            out.append((dt.date.fromisoformat(date_str), float(val_str)))
        except (ValueError, IndexError):
            continue
    return out


def to_monthly(observations):
    """Average sub-monthly observations into calendar-month values
    (first-of-month dates). Monthly series pass through unchanged."""
    buckets = {}
    for d, v in observations:
        key = d.replace(day=1)
        buckets.setdefault(key, []).append(v)
    return {k: sum(v) / len(v) for k, v in buckets.items()}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--years", type=int, default=6,
                    help="how many years of history to load (default 6)")
    ap.add_argument("--dry-run", action="store_true",
                    help="fetch and report, but write nothing")
    args = ap.parse_args()

    # window: first of month, (years-1) months back, aligned to month start
    window_start = (dt.date.today().replace(day=1)
                    - dt.timedelta(days=365 * args.years)).replace(day=1)

    conn = None
    cur = None
    if not args.dry_run:
        conn = psycopg2.connect(DB)
        cur = conn.cursor()

    failed = []
    for spec in SERIES:
        try:
            obs = fetch_series(spec["series_id"])
        except Exception as exc:                    # noqa: BLE001 - report & continue
            print(f"[FAIL] {spec['series_id']:22} {exc}")
            failed.append(spec["series_id"])
            continue

        monthly = {p: v for p, v in to_monthly(obs).items()
                   if p >= window_start and v > 0}  # CHECK: avg_price >= 0
        if not monthly:
            print(f"[WARN] {spec['series_id']:22} no usable rows in window")
            failed.append(spec["series_id"])
            continue

        periods = sorted(monthly)
        latest_p = periods[-1]
        yoy_p = latest_p.replace(year=latest_p.year - 1)
        yoy = (monthly[latest_p] / monthly[yoy_p] - 1) * 100 if yoy_p in monthly else None

        rows = [(None, p, spec["region"], spec["kind"], spec["series_id"],
                 money(v), 0.00, 0.00, spec["description"])
                for p, v in monthly.items()]
        if cur is not None:
            execute_values(cur, UPSERT_SQL, rows,
                           template="(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                           page_size=500)
        print(f"[OK]   {spec['series_id']:22} {len(rows):4} rows  "
              f"{periods[0]}..{periods[-1]}  latest={monthly[latest_p]:.2f}"
              + (f"  YoY={yoy:+.2f}%" if yoy is not None else ""))

    if cur is not None:
        conn.commit()
        conn.close()

    if failed:
        print(f"\nFailed series: {', '.join(failed)}")
        if len(failed) == len(SERIES):
            raise SystemExit(1)                     # nothing fetched at all
    else:
        print("\nAll series upserted into market_benchmarks."
              if cur is not None else "\nDry run complete -- nothing written.")


def money(x):
    return round(x, 2)


if __name__ == "__main__":
    main()
