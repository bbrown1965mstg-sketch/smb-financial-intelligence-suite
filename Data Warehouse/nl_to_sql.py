#!/usr/bin/env python3
"""
nl_to_sql.py -- AI "Ask Your Data": turn a plain-English business question
into ONE strictly read-only SQL query for the SMB warehouse.

Pipeline (ask()):
    question
      -> generate_sql()        LLM chat completion (OpenAI-compatible endpoint)
             -> validate_sql() parser-level guardrails   (front line)
      -> run_readonly_query()  restricted role + RLS company GUC +
                               read-only transaction + statement_timeout
                               (DB-level backstop)

Four guardrail layers protect the warehouse:
  1. validate_sql()   parser-level scans: single statement, comment ban,
                      forbidden-keyword word-boundary scan (incl.
                      set_config, the one SELECT-callable GUC writer),
                      FROM/JOIN allowlist, auto LIMIT (details in its
                      docstring).
  2. Single-statement enforcement is part of validate_sql(); nothing with a
     second command or a comment ever reaches the database.
  3. run_readonly_query() executes on its own connection opened with
     set_session(readonly=True) under a SET LOCAL statement_timeout
     (default 5 s): even a hypothetical guardrail bypass cannot commit a
     write or hang the caller.
  4. DATABASE-ENFORCED TENANCY: run_readonly_query() ALWAYS connects as the
     restricted nl2sql_reader role (NOSUPERUSER, NOBYPASSRLS -- created with
     its grants by migrations/002_add_rls.sql), and sets
     SET LOCAL app.current_company_id = '<company_id>' for the active
     workspace. Row-level security policies on the 8 client tables + the
     companies registry filter EVERY row to that workspace -- even when the
     generated SQL has no WHERE company_id clause. The 5 raw SMB matviews are
     NOT granted to the role; the AI reads them only through the scoped_*
     wrapper views (company-filtered by the same GUC predicate, fail-closed
     when unset). company_id=None -> no GUC -> zero client rows returned.

validate_sql(sql) -> (ok, sql_or_reason)
    (True,  final_sql)  SAFE. final_sql is the input (whitespace-stripped,
                        one optional trailing ';' dropped) plus an
                        auto-appended " LIMIT 200" when the statement has no
                        LIMIT clause.
    (False, reason)     REJECTED. reason is a short human-readable cause,
                        e.g. "forbidden keyword: drop", "relation not
                        allowed: pg_catalog.pg_tables", "multi-statement".

Config: .env next to this file (hand-rolled parser -- NO python-dotenv).
Keys:   NL2SQL_BASE_URL, NL2SQL_API_KEY, NL2SQL_MODEL,
        NL2SQL_LLM_TIMEOUT   (seconds for the LLM HTTP call, default 60),
        NL2SQL_QUERY_TIMEOUT (seconds for the SQL statement_timeout, default 5),
        NL2SQL_DB_ROLE       (the restricted role, default nl2sql_reader),
        NL2SQL_DB_PASSWORD   (the role's password, default
                              nl2sql_readonly_local -- the documented
                              migration default; rotate with ALTER ROLE and
                              mirror it here).
Precedence per key: .env value > process environment variable > built-in
default (DEFAULT_CONFIG). PRESETS provides provider shortcuts; save_config()
writes .env atomically (write .env.tmp, then os.replace).

CLI quick test (needs an API key in .env or the environment):
    python3 nl_to_sql.py "Which service line has the best gross margin?"

Importing this module has NO side effects: no network calls, no DB
connections -- everything happens inside the functions.
"""

import os
import re
import sys
import time
import urllib.parse
from pathlib import Path

import psycopg2
import requests

# Honors DATABASE_URL (used by the Docker stack: postgresql://postgres:postgres@db:5432/...)
DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/smb_warehouse")
ENV_PATH = Path(__file__).resolve().parent / ".env"

MAX_ROWS = 500           # client-side cap on returned rows
DEFAULT_LIMIT = 200      # auto-appended LIMIT when the SQL has none

# The restricted DB role the AI-SQL path always connects as (created with its
# RLS policies + grants by migrations/002_add_rls.sql). Defaults here match
# the migration; override via NL2SQL_DB_ROLE / NL2SQL_DB_PASSWORD.
DEFAULT_DB_ROLE = "nl2sql_reader"
DEFAULT_DB_PASSWORD = "nl2sql_readonly_local"

# ---------------------------------------------------------------------------
# configuration (hand-rolled .env -- python-dotenv is not installed here)
# ---------------------------------------------------------------------------
PRESETS = {
    "GLM (Zhipu)":   {"base_url": "https://open.bigmodel.cn/api/paas/v4",
                      "model": "glm-4-flash"},
    "OpenAI":        {"base_url": "https://api.openai.com/v1",
                      "model": "gpt-4o-mini"},
    "Ollama (local)": {"base_url": "http://localhost:11434/v1",
                       "model": "llama3.1"},
    "Custom":        {"base_url": "", "model": ""},
}

DEFAULT_CONFIG = {
    "base_url": PRESETS["GLM (Zhipu)"]["base_url"],
    "api_key": "",
    "model": PRESETS["GLM (Zhipu)"]["model"],
    "llm_timeout": 60,
    "query_timeout": 5,
    "db_role": DEFAULT_DB_ROLE,
    "db_password": DEFAULT_DB_PASSWORD,
}

CONFIG_ENV_KEYS = {
    "base_url": "NL2SQL_BASE_URL",
    "api_key": "NL2SQL_API_KEY",
    "model": "NL2SQL_MODEL",
    "llm_timeout": "NL2SQL_LLM_TIMEOUT",
    "query_timeout": "NL2SQL_QUERY_TIMEOUT",
    "db_role": "NL2SQL_DB_ROLE",
    "db_password": "NL2SQL_DB_PASSWORD",
}


def _parse_env_text(text: str) -> dict:
    """Parse .env-style text into {KEY: value} (no python-dotenv needed).

    Skips blank lines and '#' comments; accepts optional 'export ' prefixes;
    strips ONE pair of surrounding single or double quotes from values.
    """
    out = {}
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = re.sub(r"^export\s+", "", key.strip()).strip()
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
            val = val[1:-1]
        if key:
            out[key] = val
    return out


def load_config(env_path=None) -> dict:
    """Read nl2sql settings. Precedence: .env > os.environ > DEFAULT_CONFIG.

    Returns a dict with keys: base_url, api_key, model, llm_timeout (int
    seconds), query_timeout (seconds), db_role + db_password (the restricted
    AI-SQL role credentials; defaults match migrations/002_add_rls.sql).
    Never raises for a missing/broken .env -- falls back to defaults.
    """
    path = Path(env_path) if env_path else ENV_PATH
    file_vals = {}
    if path.exists():
        try:
            file_vals = _parse_env_text(path.read_text(encoding="utf-8"))
        except OSError:
            file_vals = {}

    cfg = {}
    for cfg_key, env_name in CONFIG_ENV_KEYS.items():
        raw = None
        if str(file_vals.get(env_name, "")).strip() != "":
            raw = file_vals[env_name]
        elif str(os.environ.get(env_name, "")).strip() != "":
            raw = os.environ[env_name]
        else:
            raw = DEFAULT_CONFIG[cfg_key]
        if cfg_key in ("llm_timeout", "query_timeout"):
            try:
                raw = max(float(str(raw).strip()), 0.5)
            except (TypeError, ValueError):
                raw = DEFAULT_CONFIG[cfg_key]
        cfg[cfg_key] = raw
    return cfg


def save_config(cfg: dict, env_path=None) -> Path:
    """Write the config dict to .env (atomic-ish: .env.tmp then os.replace).

    cfg uses the same keys as load_config(): base_url, api_key, model,
    llm_timeout, query_timeout. Returns the path written.
    """
    path = Path(env_path) if env_path else ENV_PATH
    lines = [
        "# nl_to_sql.py settings (written by the dashboard's Ask Your Data tab)",
        "# The API key stays in this local file; requests go only to the",
        "# endpoint configured as NL2SQL_BASE_URL.",
        f"{CONFIG_ENV_KEYS['base_url']}={cfg.get('base_url', '')}",
        f"{CONFIG_ENV_KEYS['api_key']}={cfg.get('api_key', '')}",
        f"{CONFIG_ENV_KEYS['model']}={cfg.get('model', '')}",
        f"{CONFIG_ENV_KEYS['llm_timeout']}={cfg.get('llm_timeout', 60)}",
        f"{CONFIG_ENV_KEYS['query_timeout']}={cfg.get('query_timeout', 5)}",
        "",
    ]
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text("\n".join(lines), encoding="utf-8")
    os.replace(tmp, path)
    return path


# ---------------------------------------------------------------------------
# guardrails (parser level)
# ---------------------------------------------------------------------------
ALLOWED_RELATIONS = {
    # 11 base tables (companies = client-workspace registry of the
    # multi-tenant warehouse)
    "companies", "customers", "job_types", "employees", "jobs",
    "financial_ledger", "labor_logs", "market_benchmarks", "anomaly_alerts",
    "forecasts", "backtest_results",
    # 6 SoCal regional benchmark tables (harvest_socal_fintech.py)
    "regional_industry_profiles", "socal_wage_benchmarks",
    "socal_macro_indicators", "socal_business_patterns",
    "socal_loan_activity", "socal_harvest_log",
    # 2 SoCal capital-flows relations (funding_schema.sql; REAL SEC Form D
    # private placements + SBA SBIR/STTR awards harvested by
    # harvest_socal_capital.py)
    "socal_capital_events", "socal_county_capital_summary",
    # 2 SEC peer-ratios relations (peer_ratios_schema.sql; REAL EDGAR XBRL
    # financials for 50 public peers harvested by
    # harvest_sec_peer_ratios.py)
    "public_peer_financials", "mv_vertical_financial_quartiles",
    # 6 materialized views: the GLOBAL mv_socal_rate_guidance, plus the 5
    # company-scoped wrapper views over the raw SMB matviews (RLS cannot go
    # on a matview, so the AI reads them through the scoped_* views, which
    # carry the app.current_company_id GUC predicate; migrations/002). The
    # RAW mv_* names are NOT allowlisted: nl2sql_reader has no grant on them.
    "scoped_monthly_summary", "scoped_customer_profitability",
    "scoped_job_type_profitability", "scoped_labor_cost_analysis",
    "scoped_benchmark_comparison", "mv_socal_rate_guidance",
}

# Word-boundary, case-insensitive scans on literal-masked SQL. "FOR UPDATE"
# is covered by the plain "update" entry. The pg_* / lo_* entries block
# superuser file/lo helpers callable from a plain SELECT list (the read-only
# transaction would not stop them). "analyze" and "checkpoint" are blocked
# because PostgreSQL EXECUTES them inside read-only transactions (ANALYZE
# rewrites pg_statistic, CHECKPOINT forces a WAL flush); "load", "close",
# "deallocate" and "discard" close the remaining non-SELECT statement verbs
# the read-only transaction would not stop or that would run as no-ops.
# "set_config" is blocked because it is the ONE function callable from a
# plain SELECT (incl. a CTE body, which PostgreSQL evaluates BEFORE the
# main scan) that can WRITE the app.current_company_id tenant GUC -- a
# "WITH flipped AS (SELECT set_config(...)) SELECT ... FROM jobs" statement
# would otherwise re-scope RLS to another workspace mid-query (proven leak;
# the plain keyword "set" cannot catch it because \bset\b does not match
# inside set_config).
FORBIDDEN_KEYWORDS = (
    "insert", "update", "delete", "drop", "alter", "truncate", "create",
    "replace", "grant", "revoke", "copy", "vacuum", "reindex", "comment",
    "lock", "merge", "do", "call", "set", "reset", "prepare", "execute",
    "into", "notify", "listen",
    "analyze", "checkpoint", "load", "close", "deallocate", "discard",
    "set_config",
    "pg_read_file", "pg_read_binary_file", "pg_ls_dir", "lo_import",
    "lo_export",
)

RELATION_RE = re.compile(r"(?i)\b(?:from|join)\s+([a-zA-Z_][\w\.]*)")
LIMIT_RE = re.compile(r"(?i)\blimit\s+\d+\b")
# CTE alias definitions on the literal-masked text: "WITH [RECURSIVE] name AS
# (" and each subsequent ", name AS (" / ") name AS (" of a multi-CTE chain.
# A defined alias may be referenced in the outer FROM/JOIN like a relation;
# its body is still scanned like any other text (keywords + relations), so a
# CTE can never reference or modify anything outside the allowlist.
CTE_NAME_RE = re.compile(
    r"(?i)(?:\bwith\b(?:\s+recursive)?|\))\s*,?\s*([a-zA-Z_]\w*)\s+as\s*\(")
# Clause keywords that end a FROM list (for the comma-join sweep below).
FROM_STOP_RE = re.compile(
    r"(?i)\b(?:where|group\s+by|order\s+by|having|limit|offset|fetch|union|"
    r"intersect|except|join|window|returning|for)\b")


def _mask_literals(text: str) -> str:
    """Mask string literals so keyword scans never match inside them.

    Dollar-quoted bodies ($$...$$ / $tag$...$tag$) are blanked first, then
    single-quoted strings with '' escapes ('it''s' masks as one literal).
    Double quotes are NOT masked (they are identifiers and rejected
    separately).
    """
    masked = re.sub(r"(\$[a-zA-Z_]\w*\$|\$\$).*?\1", " ", text, flags=re.DOTALL)
    masked = re.sub(r"'(?:[^']|'')*'", "''", masked)
    return masked


def _relation_verdict(rel: str, extra_ok=()):
    """Return None when `rel` is allowlisted (or one of the CTE aliases in
    `extra_ok`), else the rejection reason."""
    low = rel.lower().rstrip(".,;:()")
    if low.startswith("public."):
        low = low[len("public."):]
    if low in extra_ok:
        return None
    if not low or "." in low or low not in ALLOWED_RELATIONS:
        return f"relation not allowed: {rel}"
    return None


def validate_sql(sql) -> tuple:
    """Parser-level guardrails. Returns (ok, sql_or_reason); see module doc.

    Checks, in order:
      1. trim + drop ONE optional trailing ';'; any remaining ';' -> reject
         ("multi-statement"); nothing left after the strip -> reject
         ("empty statement").
      2. any '--' or '/*' -> reject ("comments are not allowed").
      2b. any double-quoted identifier -> reject (defense against
          FROM "pg_catalog"."pg_tables" hiding from the plain-token scans).
      3. mask string literals BEFORE scanning (no false positives like
          WHERE name LIKE '%update%').
      4. forbidden-keyword word-boundary scan -> "forbidden keyword: X"
          ("set_config" is in the list: the one GUC-writing function
          callable from a plain SELECT/CTE body -- a "WITH flipped AS
          (SELECT set_config(...)) SELECT ... FROM jobs" statement would
          otherwise re-scope RLS to another workspace before the main
          scan runs; "set" alone cannot catch it, \bset\b does not match
          inside set_config).
      5. FROM/JOIN allowlist scan (plus comma-separated relations inside each
          FROM list): every relation must be in ALLOWED_RELATIONS (a
          'public.' prefix is stripped; any other dotted name is rejected)
          -> "relation not allowed: X". A CTE alias defined by an earlier
          "WITH [RECURSIVE] name AS (...)" (or ", name AS (...)" in a
          multi-CTE chain) may be referenced like a relation; its body is
          scanned by the very same checks, so a CTE adds no new data source.
      6. if no LIMIT clause, " LIMIT 200" is appended to the RETURNED sql
          (validation still ok=True).
    """
    if sql is None:
        return False, "empty statement"
    text = str(sql).strip()
    if not text:
        return False, "empty statement"
    if text.endswith(";"):
        text = text[:-1].rstrip()
    if not text:
        return False, "empty statement"
    if ";" in text:
        return False, "multi-statement"
    if "--" in text or "/*" in text:
        return False, "comments are not allowed"

    masked = _mask_literals(text)
    if '"' in masked:
        return False, "double-quoted identifiers are not allowed"

    for kw in FORBIDDEN_KEYWORDS:
        if re.search(rf"(?i)\b{re.escape(kw)}\b", masked):
            return False, f"forbidden keyword: {kw}"

    # Names bound by WITH ... AS (...): they may be referenced in the outer
    # FROM/JOIN even though they are not warehouse relations. Their bodies
    # were scanned above and their FROMs are scanned below, so this only
    # whitelists the alias reference itself.
    cte_names = {m.group(1).lower() for m in CTE_NAME_RE.finditer(masked)}

    for m in RELATION_RE.finditer(masked):
        verdict = _relation_verdict(m.group(1), extra_ok=cte_names)
        if verdict:
            return False, verdict

    # Comma-separated relations in a FROM list ("FROM jobs, pg_tables")
    # escape the plain FROM-regex above; sweep each FROM clause too.
    for fm in re.finditer(r"(?i)\bfrom\b", masked):
        tail = masked[fm.end():]
        stop = FROM_STOP_RE.search(tail)
        clause = tail[:stop.start()] if stop else tail
        for item in clause.split(","):
            s = item.strip()
            if not s or s.startswith("("):
                continue  # subqueries' inner FROMs are scanned separately
            tok = re.match(r"([a-zA-Z_][\w\.]*)", s)
            if tok:
                verdict = _relation_verdict(tok.group(1), extra_ok=cte_names)
                if verdict:
                    return False, verdict

    if not LIMIT_RE.search(masked):
        text = text + f" LIMIT {DEFAULT_LIMIT}"
    return True, text


# ---------------------------------------------------------------------------
# system prompt (the exact warehouse surface the LLM may query)
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """\
You translate a small-business owner's plain-English question into ONE
read-only PostgreSQL SELECT statement for their analytics warehouse
(database smb_warehouse, PostgreSQL 17). You output SQL and nothing else.

## Multi-tenant context (Fractional CFO warehouse)

This warehouse serves MULTIPLE client businesses at once. The companies table
(company_id, company_name, industry_vertical, socal_region) is the client
registry: every client-data table is scoped by company_id across the 8
client tables (customers, job_types, employees, jobs, financial_ledger,
labor_logs, anomaly_alerts, forecasts), and every client-data analytics view
carries company_id too (scoped_monthly_summary is one row per company_id +
month, scoped_customer_profitability per company_id + customer,
scoped_job_type_profitability per company_id + job_type,
scoped_labor_cost_analysis per company_id + job, and the internal series of
scoped_benchmark_comparison are per company_id + period). Market and
benchmark data is GLOBAL and shared across clients: market_benchmarks,
backtest_results, all socal_* tables, regional_industry_profiles,
socal_capital_events, public_peer_financials and mv_socal_rate_guidance
carry NO company_id. ALWAYS filter client-specific questions by the
company_id given in the user's question context (e.g.
WHERE company_id = 1); only market/benchmark questions may omit it.

ROW-LEVEL SECURITY ENFORCEMENT: the connection runs under row-level
security for your active workspace -- rows from other workspaces are
invisible at the database level even without a WHERE company_id clause
(the scoped_* views carry the same database-side filter). Market/SoCal/
peer data is global and is visible in every workspace. Keep adding the
company_id filter anyway: it makes client-data questions unambiguous and
keeps market baselines separate from client rows.

## Relations you may use (allowlist)

Analytics views -- PREFER these, they are pre-aggregated and fast:

* scoped_monthly_summary -- one row per (company_id, month): company_id,
  month_start, year, month_name, revenue, total_expense, gross_profit,
  gross_margin_pct, job_completions, new_customers (also:
  materials_expense, labor_expense_ledger, equipment_expense, other_expense,
  labor_hours, labor_cost)
* scoped_customer_profitability -- one row per (company_id, customer):
  company_id, customer_name, revenue, job_cost, acquisition_cost,
  total_cost, gross_margin, margin_pct, job_count, is_whale,
  days_since_last_job
* scoped_job_type_profitability -- one row per (company_id, service line):
  company_id, job_type, revenue, materials_cost, other_cost, labor_cost,
  total_cost, gross_margin, margin_pct, job_count, avg_job_price,
  benchmark_avg_price, benchmark_avg_margin_pct, price_vs_benchmark_pct
* scoped_labor_cost_analysis -- one row per (company_id, job with logged
  hours): company_id, job_number, customer_name, job_type, status,
  total_hours, labor_cost, estimated_labor_cost, labor_variance_pct,
  labor_cost_per_hour, employee_count
* scoped_benchmark_comparison -- one row per (company_id, period):
  company_id, period, month_name, internal_labor_rate,
  internal_labor_yoy_pct, market_wage_national_yoy_pct,
  internal_materials_yoy_pct, ppi_building_materials_yoy_pct,
  labor_inflation_gap_national_pct, materials_inflation_gap_pct (internal
  columns are per workspace; the market_wage_*/ppi_* baseline columns are
  global market data)
* mv_socal_rate_guidance -- one row per SoCal region x Finance/Tech vertical
  (36 rows): region_code, vertical_key, vertical_name, data_year, role_label,
  annual_median_salary, hourly_cost, loaded_cost, suggested_billable_rate,
  cac_usd, payroll_to_revenue_ratio, statewide_billable_rate,
  rate_vs_statewide_pct, la_billable_rate, rate_vs_la_pct

SoCal regional benchmark tables (loaded by harvest_socal_fintech.py; region
vocabulary: la_msa, oxnard_ventura_msa, orange_county, san_diego_msa,
inland_empire_msa, california_state):

* socal_wage_benchmarks -- REAL BLS OEWS wages per (region, occupation,
  data_year): role_label, hourly_p25/median/p75/p90, annual_median,
  employment, data_year
* socal_macro_indicators -- monthly macro series per region: series_id,
  series_name, category ('cpi'/'employment'/'interest_rate'/'real_estate'/
  'ppi'), period (month start), value, unit
* socal_business_patterns -- Census CBP/ZBP per (geo_level, geo_fips,
  naics_code, data_year): region_code, naics_label, establishments,
  employment, annual_payroll_usd (geo_level 'county'/'zip'; NAICS 51/52/54)
* socal_loan_activity -- SBA 7(a)/504 lending per (program, fiscal_year,
  geography): loans_count, gross_approval_usd, avg_loan_usd, top_lender
* regional_industry_profiles -- MODELED vertical economics per
  (vertical_key, region_code): gross_margin_pct, payroll_to_revenue_ratio,
  cac_usd, utilization_target_pct, locality_burden_pct, median_salary_usd
* socal_harvest_log -- harvest audit trail: run_id, source_name, url,
  status ('ok'/'failed'/'cache_hit'), rows_loaded, fetched_at

SoCal capital flows (funding_schema.sql; REAL harvested funding events for
the six SoCal counties -- Los Angeles, Orange, Ventura, San Diego, Riverside,
San Bernardino -- loaded by harvest_socal_capital.py):

* socal_capital_events -- one row per verified capital event: company_name,
  city, county, zip_code, sector ('Fintech', 'Enterprise_SaaS', 'AI_ML',
  'Defense_Aerospace_Tech', 'Biotech_Medtech', 'Financial_Services'),
  event_type, amount_raised (USD, always positive), offering_date,
  agency_or_filing_id, source_url. The funding source IS event_type:
  'SEC_FORM_D' = private-placement capital raised (SEC Form D filing);
  'SBIR_PHASE_1' / 'SBIR_PHASE_2' / 'STTR' = federal R&D award money
  (SBA). "Form D vs SBIR" questions filter on this column.
* socal_county_capital_summary -- county x quarter aggregate: county,
  quarter_start, quarter_label, deal_count, total_capital_raised,
  median_deal_size, top_sectors (JSONB array of the 3 highest-dollar
  sectors).

SEC public-peer financials (peer_ratios_schema.sql; REAL EDGAR XBRL data
from data.sec.gov harvested by harvest_sec_peer_ratios.py -- 50
Finance/Fintech/SaaS/IT-services tickers, FY2021-FY2025, including 20
Southern California public companies):

* public_peer_financials -- one row per (company, fiscal_year,
  fiscal_period): ticker, company_name, hq_state, is_socal (TRUE = the
  filer's EDGAR business address is in a SoCal county), vertical
  ('Enterprise_SaaS', 'Fintech_Payments', 'Asset_Wealth_Mgmt',
  'IT_Consulting_Services', 'Specialty_Finance'), fiscal_period ('FY' =
  annual 10-K period; 'Q1'..'Q4' = ~3-month fiscal quarters), revenue,
  gross_profit, rd_expense, sga_expense, operating_income, net_income,
  operating_cash_flow, gross_margin_pct, operating_margin_pct,
  rd_pct_of_rev, sga_pct_of_rev, source_url, source_form.
* mv_vertical_financial_quartiles -- benchmark percentiles per (vertical,
  fiscal_year, peer_group): socal_flag, peer_group ('SoCal'/'National'),
  n_records, and p25/median/p75 for each ratio metric (gross_margin_p25/
  median/p75, operating_margin_*, rd_pct_of_rev_*, sga_pct_of_rev_*).
  This is the source for "how does X compare to public peers" and
  "national median" questions.

Base tables (use only when no matview fits the question):

* companies (company_id, company_name, industry_vertical, socal_region,
             created_at) -- the client-workspace registry
* customers (id, company_id, name, email, phone, city, state, customer_since,
             is_active)
* job_types (id, company_id, name, default_hourly_rate,
             typical_duration_hours, is_active)
* employees (id, company_id, first_name, last_name, email, hourly_rate, role,
             is_active)
* jobs (id, company_id, job_number, customer_id, job_type_id, status,
        job_date, completion_date, quoted_price, estimated_labor_cost,
        estimated_hours)
* financial_ledger (id, company_id, job_id, customer_id, entry_type,
                    category, entry_date, amount, description, vendor)
* labor_logs (id, company_id, job_id, employee_id, work_date, hours,
              hourly_rate, description)
* market_benchmarks (id, job_type_id, period, region, benchmark_kind,
                     series_id, avg_price, avg_labor_cost_per_job,
                     avg_margin_pct, source)  -- GLOBAL market data
* anomaly_alerts (id, company_id, anomaly_type, job_id, category, period,
                  severity, metric_value, baseline_value, deviation_pct,
                  z_score, detected_at, is_resolved)
* forecasts (company_id, series, period_start, model, predicted, ci_low,
             ci_high, horizon_months, mape, rmse, run_date)
* backtest_results (series, model, fold, train_start, train_end, test_start,
                    test_end, mape, rmse)  -- GLOBAL model audit trail

## Business conventions

* financial_ledger.amount is ALWAYS positive; entry_type ('revenue' or
  'expense') carries the direction. Never use negative amounts or signs.
* Labor cost comes from labor_logs: hours * hourly_rate (the rate is a
  per-row snapshot).
* Client-specific questions MUST filter by company_id (the workspace in the
  question's context); market/benchmark questions may omit it.
* "Whale client" = is_whale = TRUE in scoped_customer_profitability
  (top-quintile revenue OF THE WORKSPACE'S OWN book AND negative net
  margin).
* socal_capital_events.amount_raised is ALWAYS positive USD (Form D
  'total_amount_sold' or the SBIR/STTR award amount); funding source =
  event_type ('SEC_FORM_D' vs 'SBIR_*'/'STTR').
* public_peer_financials / mv_vertical_financial_quartiles percentages are
  plain numbers (32.38 means 32.38%); dollar lines are USD as filed.
  For "vs peers" questions use mv_vertical_financial_quartiles (match
  vertical, and peer_group = 'SoCal' vs 'National').
* financial_ledger.category values seen in this warehouse: materials, labor,
  equipment, permits, subcontractor, utilities, insurance, marketing,
  payroll, rent, other.
* anomaly_alerts.is_resolved: FALSE = open alert, TRUE = resolved.
* Money columns are NUMERIC; percentages are plain numbers (35.00 means 35%).
* Filter/group by month using month_start (scoped views), period
  (scoped_benchmark_comparison, market_benchmarks), or
  date_trunc('month', entry_date) on the ledger.

## HARD RULES (violations are auto-rejected before execution)

1. Answer with EXACTLY ONE plain SELECT statement and nothing else -- no
   explanations, no markdown, no prose before or after.
2. No comments of any kind (no --, no /* */).
3. No semicolons anywhere in the statement.
4. Only the relations listed above may appear in FROM or JOIN. Never query
   anything else: no pg_catalog, no information_schema, no functions in FROM,
   no LATERAL joins.
5. LIMIT the result to at most 500 rows.
6. Prefer the scoped_* analytics views over the base tables.
7. Filter dates through the month_start / period columns shown above.
8. Never invent columns or tables; use only the names listed above. If the
   question does not map cleanly, answer with the closest sensible SELECT.
9. Use plain unquoted identifiers (lowercase); no double quotes anywhere.

Answer with the bare SELECT statement only."""


# ---------------------------------------------------------------------------
# LLM client (OpenAI-compatible /chat/completions via requests)
# ---------------------------------------------------------------------------
def _strip_code_fences(text: str) -> str:
    """Return the inner text of a ```sql ... ``` (or ``` ... ```) fence."""
    t = (text or "").strip()
    m = re.search(r"```(?:[a-zA-Z0-9_+\-]*)[ \t]*\r?\n?(.*?)```", t, re.DOTALL)
    if m:
        return m.group(1).strip()
    if t.startswith("```"):                    # unterminated fence
        t = re.sub(r"^```[a-zA-Z0-9_+\-]*[ \t]*\r?\n?", "", t)
        t = re.sub(r"```$", "", t).strip()
    return t


def chat(messages, cfg) -> str:
    """One chat-completion call. Returns the assistant text (fences stripped).

    Raises RuntimeError with a clear message (including a response-text
    excerpt) on HTTP errors, timeouts or malformed payloads.
    """
    base = (cfg.get("base_url") or "").strip().rstrip("/")
    model = (cfg.get("model") or "").strip()
    api_key = (cfg.get("api_key") or "").strip()
    if not base:
        raise RuntimeError("No API base URL configured (NL2SQL_BASE_URL).")
    if not model:
        raise RuntimeError("No model configured (NL2SQL_MODEL).")
    if not api_key:
        raise RuntimeError("No API key configured (NL2SQL_API_KEY).")
    timeout = float(cfg.get("llm_timeout", 60) or 60)

    payload = {"model": model, "messages": messages,
               "temperature": 0, "max_tokens": 800}
    try:
        resp = requests.post(
            f"{base}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}",
                     "Content-Type": "application/json"},
            json=payload, timeout=timeout)
    except requests.exceptions.Timeout as exc:
        raise RuntimeError(f"LLM request timed out after {timeout:g}s "
                           f"({base}): {exc}") from exc
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"LLM request failed ({base}): {exc}") from exc

    if resp.status_code >= 400:
        excerpt = (resp.text or "")[:300].replace("\n", " ")
        raise RuntimeError(f"LLM endpoint returned HTTP {resp.status_code}: "
                           f"{excerpt}")
    try:
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
    except (ValueError, KeyError, IndexError, TypeError) as exc:
        raise RuntimeError(f"Unexpected LLM response shape: "
                           f"{str(resp.text)[:300]}") from exc
    if not isinstance(content, str):
        raise RuntimeError("LLM returned a non-text message content.")
    return _strip_code_fences(content)


# ---------------------------------------------------------------------------
# pipeline
# ---------------------------------------------------------------------------
def generate_sql(question, cfg) -> tuple:
    """question -> (sql | None, attempts, note).

    Calls the LLM (system prompt + the question), validates the answer, and
    on rejection retries ONCE with the guardrail verdict fed back. attempts
    counts LLM calls (1 or 2). note is a short human-readable status (also
    carries the failure reason when sql is None). May raise RuntimeError
    from chat() on transport/HTTP problems.
    """
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",
         "content": f"Business question: {question}\n\n"
                    "Answer with the single SELECT statement only."},
    ]
    sql = None
    reason = ""
    attempts = 0
    for attempt in (1, 2):
        attempts = attempt
        content = chat(messages, cfg)          # may raise RuntimeError
        candidate = (content or "").strip()
        ok, verdict = validate_sql(candidate)
        if ok:
            sql = verdict
            note = ("Accepted after 1 attempt." if attempt == 1 else
                    "Accepted after 1 repair retry (2 attempts).")
            return sql, attempts, note
        reason = verdict
        if attempt == 1:
            messages.append({"role": "assistant", "content": candidate})
            messages.append({"role": "user",
                             "content": f"Your SQL violated the guardrails: "
                                        f"{reason}. Re-issue exactly one "
                                        f"corrected SELECT statement that "
                                        f"follows every hard rule."})
    return None, attempts, (f"Could not produce a valid read-only query "
                            f"after {attempts} attempt(s). Last guardrail "
                            f"verdict: {reason}")


def _reader_dsn(db=None, env_path=None) -> str:
    """DATABASE_URL with the user/password swapped for the AI-SQL role.

    Connects as the restricted nl2sql_reader role (migrations/002_add_rls.sql
    -- NOSUPERUSER, NOBYPASSRLS, SELECT-only grants, RLS-scoped), never as
    the app's superuser. Role/password come from the nl2sql config (same
    .env / env-var precedence as everything else: NL2SQL_DB_ROLE /
    NL2SQL_DB_PASSWORD), defaulting to the documented migration values.
    The database name, host, port and any query parameters of `db` (or
    DB_URL when None) are preserved.
    """
    base = db if db is not None else DB_URL
    cfg = load_config(env_path=env_path)
    role = str(cfg.get("db_role") or DEFAULT_DB_ROLE).strip() or DEFAULT_DB_ROLE
    password = str(cfg.get("db_password") or DEFAULT_DB_PASSWORD)
    parts = urllib.parse.urlsplit(base)
    host = parts.hostname or ""
    port = f":{parts.port}" if parts.port else ""
    netloc = (f"{urllib.parse.quote(role, safe='')}:"
              f"{urllib.parse.quote(password, safe='')}@{host}{port}")
    return urllib.parse.urlunsplit(
        (parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def run_readonly_query(sql, query_timeout=5, company_id=None, db=None) -> dict:
    """Execute one SELECT on a STRICTLY read-only, RLS-SCOPED connection.

    Connection setup: psycopg2.connect(_reader_dsn(db)) -- ALWAYS the
    restricted nl2sql_reader role (NOSUPERUSER, NOBYPASSRLS, SELECT grants
    from migrations/002_add_rls.sql; the raw mv_* matviews are not granted
    to it) -> set_session(readonly=True, autocommit=False) ->
    SET LOCAL app.current_company_id = '<company_id>' (only when
    company_id is not None) -> SET LOCAL statement_timeout = '<N>ms' ->
    execute -> fetch -> ROLLBACK -> close.

    Executor-level tenant-GUC backstop (defense in depth -- the pipeline
    always runs validate_sql() first): multi-statement SQL ("multi-
    statement") and set_config(...) calls ("forbidden keyword:
    set_config") are rejected BEFORE any connection is opened, because
    either could override the app.current_company_id workspace GUC and
    re-scope row-level security to another tenant mid-query.

    TENANT ISOLATION IS DATABASE-ENFORCED: row-level security policies on
    the 8 client tables + companies filter every row to the workspace named
    by the GUC -- even when the SQL has no WHERE company_id clause. The 5
    raw SMB matviews are readable only through the scoped_* wrapper views,
    which carry the same GUC predicate. FAIL-CLOSED: company_id=None sets
    NO GUC, so the role session sees ZERO client rows (market/SoCal/peer
    data is global and stays visible).

    company_id must be int-coercible (int(company_id) is applied first, so a
    bad value raises ValueError before any connection is opened); the coerced
    int is string-interpolated into the SET LOCAL, which is safe because it
    can then only be an integer literal. The app always passes the active
    workspace's id.

    Returns {"columns": [...], "rows": [...], "rowcount": int,
             "elapsed_ms": int} on success (rows capped at 500 client-side;
             "truncated": True marks a cap). On failure returns the same
             dict plus "error": QueryCanceledError -> "query exceeded the
             N-second timeout"; ReadOnlySqlTransaction -> "blocked: the
             connection is read-only"; InsufficientPrivilege -> a clean
             permission-denied message (the role's grant surface); anything
             else -> str(exception).
    """
    started = time.perf_counter()
    result = {"columns": [], "rows": [], "rowcount": 0, "elapsed_ms": None}
    conn = None
    if company_id is not None:
        company_id = int(company_id)   # ValueError on non-int-coercible input

    # Executor-level backstop against the two known tenant-GUC override
    # vectors, so run_readonly_query enforces its own "ONE SELECT" contract
    # even for a caller that forgot validate_sql (the pipeline always
    # applies it first -- this is defense in depth):
    #   1. multi-statement: psycopg2 sends a ';'-separated string through
    #      the simple query protocol, so "SET app.current_company_id = '1';
    #      SELECT ..." would otherwise OVERRIDE the tenant GUC between the
    #      executor's own SET LOCAL and the smuggled read (proven leak);
    #   2. set_config(...): the one SELECT-callable function that writes a
    #      GUC (incl. from a CTE body, which PostgreSQL evaluates BEFORE the
    #      main scan) -- the same override vector in a single statement.
    # String literals are masked first so these scans cannot false-positive
    # on data values. Both rejections happen BEFORE any connection is
    # opened; the wording mirrors validate_sql's verdicts.
    _text = str(sql).strip()
    if _text.endswith(";"):
        _text = _text[:-1].rstrip()
    _masked = _mask_literals(_text)
    if ";" in _masked:
        result["error"] = "multi-statement"
        result["elapsed_ms"] = int(
            round((time.perf_counter() - started) * 1000))
        return result
    if re.search(r"(?i)\bset_config\s*\(", _masked):
        result["error"] = ("forbidden keyword: set_config (a query may not "
                           "change the tenant GUC app.current_company_id)")
        result["elapsed_ms"] = int(
            round((time.perf_counter() - started) * 1000))
        return result

    try:
        conn = psycopg2.connect(_reader_dsn(db), connect_timeout=10)
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            if company_id is not None:
                cur.execute(
                    f"SET LOCAL app.current_company_id = '{int(company_id)}'")
            cur.execute("SET LOCAL statement_timeout = %s",
                        (f"{int(float(query_timeout) * 1000)}ms",))
            cur.execute(sql)
            if cur.description is None:
                fetched = []
                cols = []
            else:
                cols = [d.name for d in cur.description]
                fetched = cur.fetchall()
        conn.rollback()
        rows = [tuple(r) for r in fetched[:MAX_ROWS]]
        result["columns"] = cols
        result["rows"] = rows
        result["rowcount"] = len(rows)
        result["truncated"] = len(fetched) > MAX_ROWS
    except psycopg2.errors.QueryCanceledError:
        result["error"] = (f"query exceeded the "
                           f"{float(query_timeout):g}-second timeout")
    except psycopg2.errors.ReadOnlySqlTransaction:
        result["error"] = "blocked: the connection is read-only"
    except psycopg2.errors.InsufficientPrivilege:
        result["error"] = ("permission denied: the AI read-only role "
                           "(nl2sql_reader) is not allowed to read that "
                           "relation or run that statement -- raw mv_* "
                           "matviews are restricted; use the scoped_* views")
    except Exception as exc:                    # noqa: BLE001
        result["error"] = str(exc)
    finally:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:                   # noqa: BLE001
                pass
            try:
                conn.close()
            except Exception:                   # noqa: BLE001
                pass
    result["elapsed_ms"] = int(round((time.perf_counter() - started) * 1000))
    return result


def ask(question, cfg, company_id=None) -> dict:
    """Full pipeline: generate_sql + run_readonly_query.

    company_id is the ACTIVE client workspace id; it is passed straight
    through to run_readonly_query, which sets the app.current_company_id GUC
    for the RLS policies (database-enforced tenancy). company_id=None
    fail-closes to zero client rows.

    Returns {"sql", "attempts", "note", "columns", "rows", "rowcount",
             "error", "elapsed_ms"}. When generation fails, sql is None and
             error carries the guardrail note; when the LLM transport fails,
             RuntimeError from chat() propagates.
    """
    sql, attempts, note = generate_sql(question, cfg)
    result = {"sql": sql, "attempts": attempts, "note": note,
              "columns": [], "rows": [], "rowcount": 0,
              "error": note if sql is None else None, "elapsed_ms": None}
    if sql is None:
        return result
    res = run_readonly_query(sql, query_timeout=cfg.get("query_timeout", 5),
                             company_id=company_id)
    result.update({k: res[k] for k in ("columns", "rows", "rowcount",
                                       "elapsed_ms", "error")
                   if k in res})
    return result


# ---------------------------------------------------------------------------
# CLI quick test
# ---------------------------------------------------------------------------
def main(argv) -> int:
    cfg = load_config()
    if not (cfg.get("api_key") or "").strip():
        print("No API key configured.")
        print("Add your key to .env next to nl_to_sql.py, e.g.:")
        print("  NL2SQL_API_KEY=your-key-here")
        print("or export NL2SQL_API_KEY, or save it from the dashboard's")
        print("'Ask Your Data' tab (AI provider settings).")
        return 1
    question = " ".join(argv).strip() if argv else input(
        "Business question: ").strip()
    if not question:
        print("No question given.")
        return 1
    try:
        sql, attempts, note = generate_sql(question, cfg)
    except Exception as exc:                    # noqa: BLE001
        print(f"LLM error: {exc}")
        return 1
    print(f"\n[{note}]")
    print("\nSQL:")
    print(sql if sql else "(none)")
    if not sql:
        return 1

    res = run_readonly_query(sql, query_timeout=cfg.get("query_timeout", 5),
                             company_id=1)
    if res.get("error"):
        print(f"\nQuery error: {res['error']}")
        return 1
    print(f"\n{res['rowcount']} row(s) in {res['elapsed_ms']} ms "
          f"(read-only, workspace company_id 1, row-level security):")
    print("  " + " | ".join(res["columns"]))
    for row in res["rows"][:10]:
        print("  " + " | ".join(str(v) for v in row))
    if res.get("truncated"):
        print("  ... (capped at the first 500 rows)")
    elif res["rowcount"] > 10:
        print(f"  ... {res['rowcount'] - 10} more row(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
