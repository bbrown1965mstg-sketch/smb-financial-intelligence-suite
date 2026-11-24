#!/usr/bin/env bash
# docker-entrypoint.sh -- container bootstrap for the SMB warehouse dashboard.
#
# Pipeline:
#   1. wait until the Postgres service is reachable (DATABASE_URL, ~30s cap)
#   2. run the idempotent bootstrap pipeline (docker/init_pipeline.py):
#      schema -> views -> data-module schemas (funding_schema.sql +
#      peer_ratios_schema.sql, idempotent) -> seed_demo -> offline benchmark
#      snapshot -> optional online top-up -> forecasts/backtest -> data-module
#      export packs (SoCal capital events + SEC peer financials; guarded by
#      file existence, never re-harvests)
#   3. exec the Streamlit dashboard bound to 0.0.0.0 (REQUIRED for the
#      container to be reachable through the host's 8602 port mapping)
#
# The image carries no psql: every DB touch here is psycopg2/Python.
set -euo pipefail

cd "$(dirname "$0")"

DB_URL="${DATABASE_URL:-postgresql://postgres:postgres@db:5432/smb_warehouse}"
export DATABASE_URL="$DB_URL"

echo "[entrypoint] waiting for the database to accept connections ..."
python3 - "$DB_URL" <<'PY'
import sys, time
import psycopg2

url = sys.argv[1]
deadline = time.time() + 30.0                      # ~30s cap
attempt = 0
while True:
    attempt += 1
    try:
        conn = psycopg2.connect(url, connect_timeout=3)
        conn.close()
        print(f"[entrypoint] database reachable after {attempt} attempt(s).")
        break
    except Exception as exc:                       # noqa: BLE001
        if time.time() >= deadline:
            print(f"[entrypoint] FATAL: database not reachable within 30s: {exc}",
                  file=sys.stderr)
            sys.exit(1)
        print(f"[entrypoint] attempt {attempt}: {exc} -- retrying ...")
        time.sleep(2)
PY

echo "[entrypoint] running the bootstrap pipeline (docker/init_pipeline.py) ..."
python3 docker/init_pipeline.py

echo "[entrypoint] starting Streamlit on 0.0.0.0:8602 ..."
exec streamlit run app.py --server.port 8602 --server.address 0.0.0.0
