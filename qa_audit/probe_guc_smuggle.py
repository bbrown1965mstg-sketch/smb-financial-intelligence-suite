#!/usr/bin/env python3
"""Probe two suspected GUC-override bypass vectors:
A. multi-statement smuggle through run_readonly_query (simple query
   protocol): "SET app.current_company_id = '1'; SELECT ..." executed in
   ONE cur.execute() call.
B. set_config('app.current_company_id', ...) in a SELECT list (single
   statement -- parser does not block it today) -- does the RLS policy
   evaluate the GUC per-row (leak) or at statement start (no leak)?
Scratch DB, dropped afterwards."""
import os
import shutil
import subprocess
import sys

import psycopg2

sys.path.insert(0, "/Users/Owner/Desktop/GLM-5.3/Data Warehouse")
import nl_to_sql  # noqa: E402

BASE = "/Users/Owner/Desktop/GLM-5.3/Data Warehouse"
DB = "smb_warehouse_qa_probe3"
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
    cur.execute("INSERT INTO companies (company_name) VALUES"
                " ('Probe Two') RETURNING company_id")
    co2_id = cur.fetchone()[0]
    print(f"probe co2 id = {co2_id}")
    cur.execute("INSERT INTO job_types (company_id, name,"
                " default_hourly_rate) VALUES (1, 'T1', 50.0)"
                " RETURNING id")
    jt1 = cur.fetchone()[0]
    cur.execute("INSERT INTO job_types (company_id, name,"
                " default_hourly_rate) VALUES (%s, 'T2', 60.0)"
                " RETURNING id", (co2_id,))
    jt2 = cur.fetchone()[0]
    cur.execute("INSERT INTO customers (company_id, name, email)"
                " VALUES (1, 'C-one', 'a@b.test') RETURNING id")
    cu1 = cur.fetchone()[0]
    cur.execute("INSERT INTO customers (company_id, name, email)"
                " VALUES (%s, 'C-two', 'c@d.test') RETURNING id", (co2_id,))
    cu2 = cur.fetchone()[0]
    cur.execute("INSERT INTO jobs (company_id, job_number, customer_id,"
                " job_type_id, status, job_date, quoted_price) VALUES"
                " (1, 'CO1-SECRET-1', %s, %s, 'completed',"
                " DATE '2026-01-01', 10),"
                " (1, 'CO1-SECRET-2', %s, %s, 'completed',"
                " DATE '2026-01-02', 11),"
                " (1, 'CO1-SECRET-3', %s, %s, 'completed',"
                " DATE '2026-01-03', 12),"
                " (%s, 'CO2-OK-1', %s, %s, 'completed',"
                " DATE '2026-02-01', 20)",
                (cu1, jt1, cu1, jt1, cu1, jt1, co2_id, cu2, jt2))
sup.commit()
with sup.cursor() as cur:
    cur.execute("SELECT company_id, job_number FROM jobs ORDER BY id")
    print("superuser sees:", cur.fetchall())
sup.commit()

print("\n--- A) executor multi-statement smuggle ---")
res = nl_to_sql.run_readonly_query(
    "SET app.current_company_id = '1'; SELECT company_id, job_number"
    " FROM jobs", company_id=co2_id, db=SCRATCH)
print("smuggle:", res.get("error"), "rows:", res.get("rows"))

print("\n--- B) set_config in SELECT list (role session, GUC=2) ---")
role = psycopg2.connect(nl_to_sql._reader_dsn(SCRATCH))
for label, sql in (
        ("set_config is_local=false",
         "SELECT set_config('app.current_company_id', '1', false),"
         " id, company_id, job_number FROM jobs"),
        ("set_config is_local=true",
         "SELECT set_config('app.current_company_id', '1', true),"
         " id, company_id, job_number FROM jobs"),
        ("plain read after set_config (same txn)",
         "SELECT set_config('app.current_company_id', '1', false)"),
):
    try:
        with role.cursor() as cur:
            cur.execute(f"SET LOCAL app.current_company_id = '{co2_id}'")
            cur.execute(sql)
            rows = cur.fetchall()
        print(f"[{label}]: {len(rows)} row(s) -> {rows[:6]}")
    except Exception as exc:  # noqa: BLE001
        print(f"[{label}]: {type(exc).__name__} {str(exc)[:120]}")
    finally:
        role.rollback()

print("\n--- C) current_setting read (should NOT leak) ---")
try:
    with role.cursor() as cur:
        cur.execute("SET LOCAL app.current_company_id = '2'")
        cur.execute("SELECT current_setting('app.current_company_id', true),"
                    " job_number FROM jobs")
        rows = cur.fetchall()
    print(f"rows: {len(rows)} -> {rows[:6]}")
except Exception as exc:  # noqa: BLE001
    print(f"{type(exc).__name__} {str(exc)[:120]}")
finally:
    role.rollback()
role.close()

c = psycopg2.connect(PARENT); c.autocommit = True
with c.cursor() as cur:
    cur.execute(f"DROP DATABASE {DB} WITH (FORCE)")
c.close()
print("\ndropped")
