#!/usr/bin/env python3
"""READ-ONLY catalog-level RLS audit of the LIVE smb_warehouse.

Audit 1 role attrs, 2 the 9 policies (pg_get_expr of USING / WITH CHECK),
3 the full grant matrix incl. negative checks, 4 scoped-view security_barrier
+ GUC predicate + drift vs the SQL files.

Runs ONLY SELECT statements; never writes.
"""
import os
import re
import sys

import psycopg2

LIVE = "postgresql://postgres:postgres@localhost:5432/smb_warehouse"
BASE = "/Users/Owner/Desktop/GLM-5.3/Data Warehouse"

CLIENT_TABLES = ("customers", "job_types", "employees", "jobs",
                 "financial_ledger", "labor_logs", "anomaly_alerts",
                 "forecasts")
RAW_MVS = ("mv_monthly_summary", "mv_customer_profitability",
           "mv_job_type_profitability", "mv_labor_cost_analysis",
           "mv_benchmark_comparison")
SCOPED_VIEWS = ("scoped_monthly_summary", "scoped_customer_profitability",
                "scoped_job_type_profitability", "scoped_labor_cost_analysis",
                "scoped_benchmark_comparison")
GLOBAL_GRANTED = ("market_benchmarks", "backtest_results",
                  "mv_socal_rate_guidance", "mv_vertical_financial_quartiles",
                  "regional_industry_profiles", "socal_wage_benchmarks",
                  "socal_macro_indicators", "socal_business_patterns",
                  "socal_loan_activity", "socal_harvest_log",
                  "socal_capital_events", "socal_county_capital_summary",
                  "public_peer_financials")

fails = []


def check(label, ok, detail=""):
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {label}" + (f"  -- {detail}" if detail else ""))
    if not ok:
        fails.append((label, detail))


def main():
    conn = psycopg2.connect(LIVE, connect_timeout=5)
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()

    # ---- 1) role attributes -------------------------------------------------
    cur.execute("SELECT rolname, rolsuper, rolbypassrls, rolcanlogin, "
                "rolcreatedb, rolcreaterole, rolinherit, rolreplication "
                "FROM pg_roles WHERE rolname = 'nl2sql_reader'")
    row = cur.fetchone()
    check("1a role nl2sql_reader exists", row is not None)
    if row:
        (_, rolsuper, bypass, login, createdb, createrole, inherit,
         repl) = row
        check("1b rolsuper = false", rolsuper is False, f"rolsuper={rolsuper}")
        check("1c rolbypassrls = false", bypass is False, f"={bypass}")
        check("1d rolcanlogin = true", login is True, f"={login}")
        check("1e NOCREATEDB/NOCREATEROLE/NOINHERIT/NOREPLICATION",
              (createdb, createrole, inherit, repl) == (False, False, False,
                                                        False),
              f"createdb={createdb} createrole={createrole} "
              f"inherit={inherit} repl={repl}")

    # ---- 2) RLS enablement + the 9 policies ---------------------------------
    cur.execute(
        "SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity "
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND c.relname IN %s ORDER BY 1",
        (CLIENT_TABLES + ("companies",),))
    got = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    for rel in CLIENT_TABLES + ("companies",):
        rls, force = got.get(rel, (None, None))
        check(f"2a relrowsecurity on {rel}", rls is True, f"={rls}")
    cur.execute("SELECT count(*) FROM pg_policy")
    (n_pol,) = cur.fetchone()
    check("2b exactly 9 policies in pg_policy", n_pol == 9, f"count={n_pol}")
    cur.execute("SELECT polname, polrelid::regclass::text, polcmd, "
                "pg_get_expr(polqual, polrelid), "
                "pg_get_expr(polwithcheck, polrelid) "
                "FROM pg_policy ORDER BY polrelid::regclass::text")
    pols = cur.fetchall()
    expected_pols = {f"tenant_isolation_{t}": t
                     for t in CLIENT_TABLES + ("companies",)}
    for polname, rel, polcmd, using, wcheck in pols:
        check(f"2c policy {polname} on {rel} (cmd {polcmd})",
              expected_pols.get(polname) == rel, f"rel={rel}")
        ok_using = ("company_id = " in using
                    and "NULLIF(current_setting('app.current_company_id'::text"
                        ", true)" in using
                    and "::bigint" in using)
        check(f"2d {polname} USING is fail-closed GUC predicate",
              ok_using, using)
        check(f"2e {polname} WITH CHECK present & identical to USING",
              wcheck == using, wcheck)
    # nothing ELSE in public has RLS policies
    extra = [p for p in pols
             if p[1] not in CLIENT_TABLES + ("companies",)]
    check("2f no policy on any relation outside the 9", not extra,
          str([(p[0], p[1]) for p in extra]))

    # ---- 3) grant matrix ----------------------------------------------------
    def has_priv(rel, priv):
        cur.execute("SELECT has_table_privilege('nl2sql_reader', %s, %s)",
                    (rel, priv))
        return cur.fetchone()[0]

    print("\n--- grant matrix (SELECT) ---")
    for rel in CLIENT_TABLES + ("companies",):
        check(f"3a SELECT TRUE  {rel}", has_priv(rel, "SELECT") is True)
    for rel in GLOBAL_GRANTED:
        check(f"3a SELECT TRUE  {rel}", has_priv(rel, "SELECT") is True)
    for rel in SCOPED_VIEWS:
        check(f"3a SELECT TRUE  {rel}", has_priv(rel, "SELECT") is True)
    for rel in RAW_MVS:
        check(f"3b SELECT FALSE {rel} (raw matview denied)",
              has_priv(rel, "SELECT") is False)
    print("--- negative privileges (write/DDL) ---")
    for rel in CLIENT_TABLES + ("companies",) + SCOPED_VIEWS + RAW_MVS:
        for priv in ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES",
                     "TRIGGER"):
            check(f"3c {priv} FALSE on {rel}", has_priv(rel, priv) is False)
    # schema / create privileges
    cur.execute("SELECT has_schema_privilege('nl2sql_reader', 'public', "
                "'CREATE')")
    check("3d CREATE on schema public denied", cur.fetchone()[0] is False)
    cur.execute("SELECT has_database_privilege('nl2sql_reader', "
                "current_database(), 'CREATE')")
    check("3e CREATE on database denied", cur.fetchone()[0] is False)
    # any OTHER grant at all? (audit for stray privileges)
    cur.execute(
        "SELECT grantee::text, table_name, privilege_type "
        "FROM information_schema.role_table_grants "
        "WHERE grantee = 'nl2sql_reader' ORDER BY table_name, privilege_type")
    grants = cur.fetchall()
    allowed = set(CLIENT_TABLES + ("companies",)) | set(GLOBAL_GRANTED) \
        | set(SCOPED_VIEWS)
    stray = [(g, t, p) for (g, t, p) in grants
             if not (p == "SELECT" and t in allowed)]
    check("3f role holds ONLY SELECT grants on the allowlisted relations",
          not stray, str(stray))
    print(f"    (total grants held: {len(grants)})")

    # ---- 4) scoped views ----------------------------------------------------
    cur.execute(
        "SELECT c.relname, c.relrowsecurity, c.relispopulated, "
        "pg_get_viewdef(c.oid, true), obj_description(c.oid, 'pg_class') "
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND c.relname IN %s ORDER BY 1",
        (SCOPED_VIEWS,))
    views = {r[0]: r for r in cur.fetchall()}
    for v in SCOPED_VIEWS:
        check(f"4a scoped view {v} exists", v in views)
    # security_barrier: pg_get_viewdef includes "WHERE" + barrier shows up in
    # the reloid's _return expression? Actually security_barrier is exposed
    # via pg_class.reloptions for views.
    cur.execute(
        "SELECT c.relname, c.reloptions FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND c.relname IN %s",
        (SCOPED_VIEWS,))
    for rel, opts in cur.fetchall():
        check(f"4b security_barrier=true on {rel}",
              any("security_barrier=true" in (o or "") for o in (opts or [])),
              str(opts))
    for rel, (_, _, _, viewdef, _) in views.items():
        check(f"4c viewdef of {rel} carries the GUC predicate",
              "current_setting('app.current_company_id'::text, true)" in viewdef
              and "NULLIF" in viewdef and "::bigint" in viewdef)
    # drift check: view definitions in live DB vs both SQL files
    def file_viewdefs(path):
        text = open(path, encoding="utf-8").read()
        out = {}
        for m in re.finditer(
                r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)\s+"
                r"WITH\s*\(security_barrier\)\s+AS\s+(.*?);",
                text, re.S | re.I):
            out[m.group(1).lower()] = " ".join(m.group(2).split()).lower()
        return out

    mig = file_viewdefs(os.path.join(BASE, "migrations", "002_add_rls.sql"))
    ana = file_viewdefs(os.path.join(BASE, "analytics_views.sql"))
    for v in SCOPED_VIEWS:
        check(f"4d migrations/002 defines {v}", v in mig)
        check(f"4e analytics_views.sql defines {v}", v in ana)
        if v in mig and v in ana:
            check(f"4f NO drift between 002 and analytics_views for {v}",
                  mig[v] == ana[v],
                  "DIFFERS" if mig[v] != ana[v] else "")
        if v in views:
            # pg_get_viewdef normalizes ('' as ''::text, current_setting arg
            # as 'app.current_company_id'::text) and expands SELECT * into an
            # explicit column list -- normalize both so the SEMANTIC idiom is
            # what gets compared.
            def norm_livedef(s):
                s = re.sub(r"::text", "", s)
                s = " ".join(s.split()).lower()
                return s
            def norm_filedef(s):
                return " ".join(s.split()).lower().replace("::text", "")
            live_def = norm_livedef(views[v][3])
            key_live = ("nullif(current_setting("
                        "'app.current_company_id', true), '')::bigint")
            check(f"4h {v}: LIVE definition uses the exact fail-closed idiom",
                  key_live in live_def, live_def[-160:])
            key_file = ("nullif(current_setting("
                        "'app.current_company_id', true), '')::bigint")
            check(f"4g {v}: file definition uses the exact fail-closed idiom",
                  key_file in norm_filedef(mig.get(v, ""))
                  and key_file in norm_filedef(ana.get(v, "")))

    # live vs file: the view must read FROM its raw matview with the same
    # WHERE predicate (SELECT * is expanded by pg_get_viewdef, so match the
    # FROM target + WHERE clause instead of the head).
    for v in SCOPED_VIEWS:
        raw = "mv_" + v[len("scoped_"):]
        if v in views:
            live_def = " ".join(views[v][3].split()).lower()
            file_def = " ".join((mig.get(v) or ana.get(v) or "").split()).lower()
            check(f"4i {v} reads FROM {raw} (live + files)",
                  f" from {raw} where company_id = nullif(" in live_def
                  and f" from {raw}" in file_def,
                  live_def[-120:])

    conn.close()

    print("\n" + "=" * 72)
    if fails:
        print(f"AUDIT FAILURES: {len(fails)}")
        for label, detail in fails:
            print(f"  FAIL {label}: {detail}")
        return 1
    print("CATALOG AUDIT: ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
