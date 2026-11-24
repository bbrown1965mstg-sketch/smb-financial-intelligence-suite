#!/usr/bin/env python3
"""Probe the DB-level fate of hostile SQL if it somehow reached
run_readonly_query (bypassing the parser). Scratch DB, then dropped."""
import datetime
import os
import shutil
import subprocess
import sys

import psycopg2

sys.path.insert(0, "/Users/Owner/Desktop/GLM-5.3/Data Warehouse")
import nl_to_sql  # noqa: E402

BASE = "/Users/Owner/Desktop/GLM-5.3/Data Warehouse"
DB = "smb_warehouse_qa_probe2"
PARENT = "postgresql://postgres:postgres@localhost:5432/postgres"
SCRATCH = f"postgresql://postgres:postgres@localhost:5432/{DB}"
PSQL = shutil.which("psql") or "/Users/Owner/pg/bin/psql"

c = psycopg2.connect(PARENT); c.autocommit = True
with c.cursor() as cur:
    cur.execute(f"DROP DATABASE IF EXISTS {DB} WITH (FORCE)")
    cur.execute(f"CREATE DATABASE {DB}")
c.close()
for f in ("schema.sql", "analytics_views.sql",
          "migrations/001_add_companies.sql", "migrations/002_add_rls.sql"):
    r = subprocess.run([PSQL, SCRATCH, "-X", "-v", "ON_ERROR_STOP=1", "-f",
                        os.path.join(BASE, f)], capture_output=True, text=True)
    assert r.returncode == 0, f"{f}: {r.stderr[-800:]}"

sup = psycopg2.connect(SCRATCH)
with sup.cursor() as cur:
    cur.execute("SELECT count(*) FROM jobs")
    print("jobs (demo co1):", cur.fetchone()[0])
sup.commit()

CASES = [
    ("SET alone", "SET app.current_company_id = '1'"),
    ("multi-statement", "SELECT 1; SET app.current_company_id = '1'"),
    ("COPY TO PROGRAM", "COPY (SELECT 1) TO PROGRAM 'echo hi'"),
    ("CTE insert", "WITH x AS (INSERT INTO jobs (company_id) VALUES (1) RETURNING 1) SELECT * FROM x"),
    ("information_schema", "SELECT table_name FROM information_schema.tables"),
    ("raw matview", "SELECT COUNT(*) FROM mv_monthly_summary"),
    ("bare matview star", "SELECT * FROM mv_benchmark_comparison"),
    ("pg_sleep", "SELECT pg_sleep(0.3), 1"),
    ("cursor statement", "CLOSE ALL"),
    ("listen", "LISTEN chan"),
    ("do block", "DO $$ BEGIN NULL; END $$;"),
    ("prepare", "PREPARE p1 AS SELECT 1"),
    ("vacuum", "VACUUM"),
    ("analyze", "ANALYZE jobs"),
    ("select-into", "SELECT 1 INTO new_t"),
    ("set session char", "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE"),
]
for label, sql in CASES:
    res = nl_to_sql.run_readonly_query(sql, query_timeout=5, company_id=2,
                                       db=SCRATCH)
    print(f"[{label:18}] error={res.get('error')!r} "
          f"rows={len(res.get('rows', []))} cols={res.get('columns')!r}")

with sup.cursor() as cur:
    cur.execute("SELECT count(*) FROM jobs")
    (n,) = cur.fetchone()
sup.commit()
print("jobs after all attempts:", n)
sup.close()
c = psycopg2.connect(PARENT); c.autocommit = True
with c.cursor() as cur:
    cur.execute(f"DROP DATABASE {DB} WITH (FORCE)")
c.close()
print("dropped")
