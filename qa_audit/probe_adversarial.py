#!/usr/bin/env python3
"""Adversarial RLS enforcement probes on a THROWAWAY two-company scratch DB.

Discovery run for the auditor: learns the EXACT observable behavior of every
bypass attempt before it is pinned in the pytest suite. Creates
smb_warehouse_qa_probe, seeds two companies, runs every probe through a real
nl2sql_reader TCP session (password auth), then DROPS the scratch DB.
"""
import datetime
import os
import shutil
import subprocess
import sys
import time
import urllib.parse

import psycopg2
import psycopg2.errors

sys.path.insert(0, "/Users/Owner/Desktop/GLM-5.3/Data Warehouse")
import nl_to_sql  # noqa: E402

BASE = "/Users/Owner/Desktop/GLM-5.3/Data Warehouse"
DB_NAME = "smb_warehouse_qa_probe"
PARENT = "postgresql://postgres:postgres@localhost:5432/postgres"
SCRATCH = f"postgresql://postgres:postgres@localhost:5432/{DB_NAME}"
PSQL = shutil.which("psql") or "/Users/Owner/pg/bin/psql"


def parent():
    c = psycopg2.connect(PARENT)
    c.autocommit = True
    return c


def drop():
    c = parent()
    try:
        with c.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {DB_NAME} WITH (FORCE)")
    finally:
        c.close()


def apply(path):
    r = subprocess.run([PSQL, SCRATCH, "-X", "-v", "ON_ERROR_STOP=1",
                        "-f", path], capture_output=True, text=True,
                       timeout=180)
    assert r.returncode == 0, f"psql {path}: {r.stderr[-1500:]}"


def probe(label, fn):
    try:
        out = fn()
        print(f"[OK  ] {label}: {out}")
    except Exception as exc:  # noqa: BLE001
        code = getattr(exc, "pgcode", None)
        print(f"[ERR ] {label}: {type(exc).__name__} code={code} "
              f"msg={str(exc)[:150]!r}")
        return (type(exc).__name__, code, str(exc)[:200])


def main():
    drop()
    c = parent()
    try:
        with c.cursor() as cur:
            cur.execute(f"CREATE DATABASE {DB_NAME}")
    finally:
        c.close()
    for f in ("schema.sql", "analytics_views.sql", "migrations/001_add_companies.sql",
              "migrations/002_add_rls.sql"):
        apply(os.path.join(BASE, f))

    sup = psycopg2.connect(SCRATCH)
    with sup.cursor() as cur:
        cur.execute("INSERT INTO companies (company_name, industry_vertical,"
                    " socal_region) VALUES ('QA Probe One', 'roofing',"
                    " 'la_msa') RETURNING company_id")
        co1 = cur.fetchone()[0]
        cur.execute("INSERT INTO companies (company_name, industry_vertical,"
                    " socal_region) VALUES ('QA Probe Two', 'software',"
                    " 'la_msa') RETURNING company_id")
        co2 = cur.fetchone()[0]
        cur.execute("INSERT INTO job_types (company_id, name,"
                    " default_hourly_rate) VALUES (%s, 'P1 Type', 50.0)"
                    " RETURNING id", (co1,))
        jt1 = cur.fetchone()[0]
        cur.execute("INSERT INTO job_types (company_id, name,"
                    " default_hourly_rate) VALUES (%s, 'P2 Type', 70.0)"
                    " RETURNING id", (co2,))
        jt2 = cur.fetchone()[0]
        # distinctive rows: co1 3 jobs, co2 1 job
        cur.execute("INSERT INTO customers (company_id, name, email) VALUES"
                    " (%s, 'P1 Customer A', 'p1a@probe.test') RETURNING id",
                    (co1,))
        cust1 = cur.fetchone()[0]
        cur.execute("INSERT INTO customers (company_id, name, email) VALUES"
                    " (%s, 'P2 Customer Z', 'p2z@probe.test') RETURNING id",
                    (co2,))
        cust2 = cur.fetchone()[0]
        for i in range(3):
            cur.execute("INSERT INTO jobs (company_id, job_number,"
                        " customer_id, job_type_id, status, job_date,"
                        " quoted_price)"
                        " VALUES (%s, %s, %s, %s, 'completed', %s, %s)",
                        (co1, f"P1-JOB-{i}", cust1, jt1,
                         datetime.date(2026, 1, i + 1), 100.0 + i))
        cur.execute("INSERT INTO jobs (company_id, job_number, customer_id,"
                    " job_type_id, status, job_date, quoted_price) VALUES"
                    " (%s, 'P2-JOB-0', %s, %s, 'completed', %s, 999.00)",
                    (co2, cust2, jt2, datetime.date(2026, 2, 1)))
    sup.commit()

    # role session over TCP w/ password auth
    role = psycopg2.connect(nl_to_sql._reader_dsn(SCRATCH), connect_timeout=5)
    with role.cursor() as cur:
        cur.execute("SELECT current_user, inet_server_addr() IS NOT NULL")
        print("role session:", cur.fetchone())

    print(f"\nco1={co1} co2={co2}")

    def guc(v):
        with role.cursor() as cur:
            cur.execute("SET LOCAL app.current_company_id = %s", (v,))

    print("\n--- demanded proof ---")
    probe("GUC=co2 SELECT * FROM jobs", lambda: _rows(role, co2, "SELECT * FROM jobs"))
    probe("GUC=co2 SELECT * FROM companies", lambda: _rows(role, co2, "SELECT * FROM companies"))
    probe("GUC=co1 SELECT * FROM jobs", lambda: _rows(role, co1, "SELECT * FROM jobs"))

    print("\n--- leak attempts (GUC=co2) ---")
    probe("WHERE company_id = co1", lambda: _rows(role, co2, "SELECT id FROM jobs WHERE company_id = 1"))
    probe("WHERE company_id IN (co1,co2)", lambda: _rows(role, co2, f"SELECT company_id FROM jobs WHERE company_id IN (1, {co2})"))
    probe("WHERE customer_id > 0", lambda: _rows(role, co2, "SELECT company_id FROM jobs WHERE customer_id > 0"))
    probe("UNION both ids", lambda: _rows(role, co2, f"SELECT company_id FROM jobs WHERE company_id = {co1} UNION SELECT company_id FROM jobs WHERE company_id = {co2}"))
    probe("subquery IN (SELECT ... WHERE company_id = 1)", lambda: _rows(role, co2, f"SELECT company_id FROM customers WHERE company_id IN (SELECT company_id FROM jobs WHERE company_id = {co1})"))

    print("\n--- no-GUC fail closed ---")
    for t in ("jobs", "customers", "companies", "scoped_monthly_summary"):
        probe(f"no GUC SELECT count(*) FROM {t}", lambda t=t: _count(role, f"SELECT COUNT(*) FROM {t}"))

    print("\n--- GUC manipulation ---")
    probe("GUC='abc'", lambda: _count(role, "SELECT COUNT(*) FROM jobs", guc="abc"))
    probe("GUC=''", lambda: _count(role, "SELECT COUNT(*) FROM jobs", guc=""))
    probe("GUC='99999999999999999999999'", lambda: _count(role, "SELECT COUNT(*) FROM jobs", guc="99999999999999999999999"))
    probe("SET SESSION (not LOCAL) then rollback", lambda: _set_session_probe(role, co2))
    probe("RESET GUC then select", lambda: _reset_probe(role))

    print("\n--- AI-vector parser rejections ---")
    for sql in ("SET app.current_company_id = '1'",
                "WITH x AS (INSERT INTO jobs (company_id) VALUES (1) RETURNING 1) SELECT * FROM x",
                "SELECT * FROM mv_monthly_summary",
                "COPY (SELECT 1) TO PROGRAM 'echo hi'",
                "SELECT table_name FROM information_schema.tables",
                "SELECT 1; SET app.current_company_id = '1'"):
        ok, verdict = nl_to_sql.validate_sql(sql)
        print(f"[{'REJ' if not ok else 'PASS!'}] validate_sql({sql[:60]!r}) -> {verdict if not ok else 'ACCEPTED (BAD)'}")

    print("\n--- raw matview direct (role session) ---")
    probe("role SELECT count(*) FROM mv_monthly_summary", lambda: _count(role, "SELECT COUNT(*) FROM mv_monthly_summary", guc=co2))

    print("\n--- write layering (role) ---")
    before = _count(sup, "SELECT COUNT(*) FROM jobs")
    probe("UPDATE jobs (plain txn)", lambda: _write(role, f"UPDATE jobs SET status='x' WHERE company_id = {co2}", guc=co2))
    probe("DELETE FROM jobs (plain txn)", lambda: _write(role, f"DELETE FROM jobs WHERE company_id = {co2}", guc=co2))
    probe("INSERT INTO jobs (plain txn)", lambda: _write(role, f"INSERT INTO jobs (company_id, job_number) VALUES ({co2}, 'HACK1')", guc=co2))
    print("jobs count unchanged:", _count(sup, "SELECT COUNT(*) FROM jobs") == before)

    def ro_layer():
        role.set_session(readonly=True, autocommit=False)
        try:
            with role.cursor() as cur:
                cur.execute("SET LOCAL app.current_company_id = %s", (str(co2),))
                cur.execute(f"UPDATE jobs SET status='hacked' WHERE company_id = {co2}")
        finally:
            role.rollback()
            role.set_session(readonly=False, autocommit=False)
    probe("UPDATE inside read-only txn", ro_layer)

    def with_check_layer():
        # temporarily widen grants on the SCRATCH db, prove RLS WITH CHECK
        # still blocks cross-tenant writes even for a granted role
        sup2 = psycopg2.connect(SCRATCH)
        sup2.autocommit = True
        with sup2.cursor() as cur:
            cur.execute("GRANT UPDATE ON jobs TO nl2sql_reader")
            cur.execute("GRANT INSERT ON jobs TO nl2sql_reader")
        sup2.close()
        try:
            with role.cursor() as cur:
                cur.execute("SET LOCAL app.current_company_id = %s", (str(co2),))
                cur.execute(f"UPDATE jobs SET company_id = {co1} WHERE company_id = {co2}")
                role.rollback()
                return "HIJACK ALLOWED (BAD)"
        except Exception as exc:  # noqa: BLE001
            role.rollback()
            return f"{type(exc).__name__} code={getattr(exc, 'pgcode', None)} msg={str(exc)[:120]!r}"
        finally:
            sup3 = psycopg2.connect(SCRATCH)
            sup3.autocommit = True
            with sup3.cursor() as cur:
                cur.execute("REVOKE UPDATE, INSERT ON jobs FROM nl2sql_reader")
            sup3.close()
    probe("GRANT UPDATE widened: cross-tenant company_id hijack", with_check_layer)

    def self_update_allowed():
        sup2 = psycopg2.connect(SCRATCH)
        sup2.autocommit = True
        with sup2.cursor() as cur:
            cur.execute("GRANT UPDATE ON jobs TO nl2sql_reader")
        sup2.close()
        try:
            with role.cursor() as cur:
                cur.execute("SET LOCAL app.current_company_id = %s", (str(co2),))
                cur.execute(f"UPDATE jobs SET status = 'cancelled' WHERE company_id = {co2}")
                n = cur.rowcount
            role.rollback()
            return f"self-scoped update rowcount={n} (then rolled back)"
        finally:
            role.rollback()
            sup3 = psycopg2.connect(SCRATCH)
            sup3.autocommit = True
            with sup3.cursor() as cur:
                cur.execute("REVOKE UPDATE ON jobs FROM nl2sql_reader")
            sup3.close()
    probe("GRANT UPDATE widened: self-scoped update (documented semantics)", self_update_allowed)

    print("\n--- timeout via run_readonly_query ---")
    t0 = time.perf_counter()
    res = nl_to_sql.run_readonly_query("SELECT pg_sleep(10), 1", query_timeout=2,
                                       company_id=co2, db=SCRATCH)
    el = time.perf_counter() - t0
    print(f"[TIME] elapsed={el:.2f}s result_error={res.get('error')!r} "
          f"reported_ms={res.get('elapsed_ms')}")

    print("\n--- read-only txn blocks writes via run_readonly_query ---")
    res = nl_to_sql.run_readonly_query(f"UPDATE jobs SET status='x' WHERE company_id = {co2}",
                                       company_id=co2, db=SCRATCH)
    print("UPDATE:", res.get("error"))
    res = nl_to_sql.run_readonly_query(f"INSERT INTO jobs (company_id, job_number) VALUES ({co2}, 'H2')",
                                       company_id=co2, db=SCRATCH)
    print("INSERT:", res.get("error"))
    print("jobs count unchanged:", _count(sup, "SELECT COUNT(*) FROM jobs") == before)

    print("\n--- residual visibility (no GUC role session) ---")
    with role.cursor() as cur:
        cur.execute("SELECT relname FROM pg_class WHERE relnamespace = 'public'::regnamespace"
                    " AND relkind IN ('r','v','m') ORDER BY relname")
        rels = [r[0] for r in cur.fetchall()]
    for rel in rels:
        try:
            n = _count(role, f"SELECT COUNT(*) FROM {rel}")
            print(f"[VIS ] {rel}: {n} row(s)")
        except Exception as exc:  # noqa: BLE001
            print(f"[DENY] {rel}: {type(exc).__name__} {getattr(exc, 'pgcode', '')}")

    role.close()
    sup.close()
    drop()
    print("\nscratch dropped")


def _rows(conn, guc_val, sql):
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute("SET LOCAL app.current_company_id = %s", (str(guc_val),))
        cur.execute(sql)
        rows = cur.fetchall()
    conn.rollback()
    return rows


def _count(conn, sql, guc=None):
    conn.rollback()
    with conn.cursor() as cur:
        if guc is not None:
            cur.execute("SET LOCAL app.current_company_id = %s", (str(guc),))
        cur.execute(sql)
        (n,) = cur.fetchone()
    conn.rollback()
    return n


def _set_session_probe(role, co2):
    role.rollback()
    with role.cursor() as cur:
        cur.execute("SET app.current_company_id = %s", (str(co2),))
        cur.execute("SELECT COUNT(*) FROM jobs")
        n = cur.fetchone()[0]
    role.rollback()
    with role.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM jobs")
        n_after = cur.fetchone()[0]
    role.rollback()
    return f"in-txn={n} after-rollback={n_after}"


def _reset_probe(role):
    role.rollback()
    with role.cursor() as cur:
        cur.execute("SET LOCAL app.current_company_id = '2'")
        cur.execute("SELECT COUNT(*) FROM jobs")
        n_set = cur.fetchone()[0]
        cur.execute("RESET app.current_company_id")
        cur.execute("SELECT COUNT(*) FROM jobs")
        n_reset = cur.fetchone()[0]
    role.rollback()
    return f"set={n_set} after-reset={n_reset}"


def _write(role, sql, guc=None):
    role.rollback()
    try:
        with role.cursor() as cur:
            if guc is not None:
                cur.execute("SET LOCAL app.current_company_id = %s", (str(guc),))
            cur.execute(sql)
            rc = cur.rowcount
            role.rollback()
            return f"EXECUTED rowcount={rc} (BAD)"
    except Exception as exc:  # noqa: BLE001
        role.rollback()
        return (f"{type(exc).__name__} code={getattr(exc, 'pgcode', None)} "
                f"msg={str(exc)[:140]!r}")


if __name__ == "__main__":
    main()
