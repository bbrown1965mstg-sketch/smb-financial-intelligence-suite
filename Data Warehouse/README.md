# SMB Financial & Operational Data Warehouse

A small-business analytics warehouse built on **PostgreSQL 17.5**, running locally at:

```
postgresql://postgres:postgres@localhost:5432/smb_warehouse
```

It models a service business (think painting / remodeling SMB): customers, work
orders, a crew, a complete financial ledger, timesheets, industry benchmarks —
then layers **materialized analytics views** and an **anomaly detector** on top.
A Streamlit dashboard (10 tabs), a deterministic demo simulation, real BLS/FRED
economic baselines, a forecasting engine and a one-command **Docker stack**
make the whole thing portable and shareable — every script honors the
`DATABASE_URL` environment variable (defaulting to the local URL above).

---

## 1. Architecture

```mermaid
flowchart LR
    subgraph INGEST["Ingestion sources"]
        CSV["CSV uploads<br/>ingest_csv.py"]
        BLS["BLS / FRED macro series<br/>fetch_benchmarks.py"]
        SEED["Deterministic simulation<br/>seed_demo.py seed 42"]
        SFH["SoCal Finance/Tech harvest<br/>harvest_socal_fintech.py<br/>(OEWS, FRED, CBP/ZBP, SBA)"]
        CAPH["SoCal capital harvest<br/>harvest_socal_capital.py<br/>(SEC Form D + SBA SBIR/STTR)"]
        PEERH["SEC EDGAR XBRL harvest<br/>harvest_sec_peer_ratios.py<br/>(50 public peers)"]
    end

    subgraph PG["PostgreSQL: smb_warehouse"]
        TABLES["11 core tables: companies (client-workspace<br/>registry) + jobs, labor_logs,<br/>financial_ledger, customers, employees,<br/>job_types, market_benchmarks (global),<br/>anomaly_alerts, forecasts, backtest_results (global)<br/>+ 6 SoCal benchmark tables<br/>(socal_wage_benchmarks, socal_macro_indicators,<br/>socal_business_patterns, socal_loan_activity,<br/>socal_harvest_log, regional_industry_profiles)"]
        CAPITAL["socal_capital_events<br/>(3,596 REAL funding events)<br/>funding_schema.sql"]
        PEER["public_peer_financials<br/>(1,193 REAL EDGAR rows, 50 tickers)<br/>peer_ratios_schema.sql"]
        MVIEWS["8 materialized views:<br/>mv_monthly_summary,<br/>mv_job_type_profitability,<br/>mv_customer_profitability,<br/>mv_labor_cost_analysis,<br/>mv_benchmark_comparison,<br/>mv_socal_rate_guidance,<br/>socal_county_capital_summary,<br/>mv_vertical_financial_quartiles<br/>+ 5 scoped_* company-filtered wrapper<br/>views over the 5 SMB matviews"]
        RLS["row-level security (migration 002):<br/>nl2sql_reader role, NOBYPASSRLS,<br/>9 tenant_isolation_* policies keyed on<br/>the app.current_company_id GUC (fail-closed)"]
        DET["detect_anomalies()<br/>refresh_all_analytics()"]
        FC["forecast_engine.py<br/>seasonal_trend_v1"]
        TABLES --> MVIEWS
        TABLES --> DET
        MVIEWS --> RLS
        DET --> TABLES
        TABLES --> FC
        FC --> TABLES
        CAPITAL --> MVIEWS
        PEER --> MVIEWS
    end

    subgraph UI["Streamlit dashboard: app.py on port 8602"]
        TABS["Sidebar client-workspace selector<br/>(multi-tenant) + 10 tabs: Overview (+ Executive<br/>PDF Brief), Charts, Anomaly alerts, Ingest CSVs,<br/>Pricing simulator, SoCal Benchmarks (global),<br/>Forecast and cash runway, SoCal Capital and Deals<br/>(global), Public Peer Financials (global),<br/>Ask Your Data (workspace-scoped)"]
    end

    subgraph OUT["Shareable outputs (4 dataset packs, §15)"]
        PACK["exports/smb_benchmark_pack_v1<br/>exports/socal_fintech_benchmark_pack_v2<br/>exports/socal_funding_deals_v1<br/>peer_financial_ratios_v1<br/>CSV + Parquet, dictionaries, manifests"]
    end

    CSV --> TABLES
    BLS --> TABLES
    SEED --> TABLES
    SFH --> TABLES
    CAPH --> CAPITAL
    PEERH --> PEER
    TABLES --> TABS
    CAPITAL --> TABS
    PEER --> TABS
    MVIEWS --> TABS
    MVIEWS --> PACK
    TABLES --> PACK
    CAPITAL --> PACK
    PEER --> PACK
```

---

## 2. What's in this folder

| File | What it is |
|---|---|
| `schema.sql` | Core schema: 11 tables — the `companies` client registry (multi-tenant; seeded with the demo workspace) + the 8 company-scoped client tables + `market_benchmarks` (global) + the `forecasts` / `backtest_results` output tables — keys, constraints, indexes. Apply **first**. |
| `migrations/001_add_companies.sql` | **Multi-tenant migration** (idempotent, applied on every Docker boot): creates `companies`, adds `company_id` (NOT NULL, FK, default 1, indexed) to the 8 client tables, backfills every pre-existing row into the demo workspace, and re-keys per-workspace uniqueness (`forecasts` upsert key + the open-alert partial unique indexes). |
| `migrations/002_add_rls.sql` | **RLS isolation migration** (idempotent, applied on every Docker boot): creates the restricted `nl2sql_reader` LOGIN role (NOSUPERUSER, NOBYPASSRLS), enables **row-level security** on the 8 client tables + `companies` (policies keyed on the per-session `app.current_company_id` GUC, fail-closed when unset), grants the role SELECT on everything the AI may read (client tables, companies, ALL the global/market data) and NOTHING on the 5 raw SMB matviews, and (re)creates the 5 company-scoped `scoped_*` wrapper views. See §13. |
| `analytics_views.sql` | 6 materialized views (5 core KPI views + `mv_socal_rate_guidance`) + `detect_anomalies()` + `refresh_all_analytics()` + the 5 `scoped_*` **company-scoped wrapper views** over the raw matviews (the AI read surface — §13). The 5 core views are **per client workspace** (`company_id` grain); market baselines stay global. Apply **second**. |
| `seed_demo.py` | Deterministic **36-month simulation** loaded as client workspace #1 (`BrightPath Works (Demo)`): ~2,500 jobs, 150 customers, seasonal demand curves, employee overtime rules, customer acquisition costs, and planted anomalies (see §6.1). |
| `fetch_benchmarks.py` | Pulls **real public economic data** — BLS construction wage rates (national + Missouri) and FRED producer price indices (softwood lumber, paint & coatings, construction materials) — into `market_benchmarks` (global market data, shared by all workspaces). Idempotent upserts; no API key needed. |
| `ingest_csv.py` | Config-driven loader for **your own messy CSV exports** (accounting/payroll): JSON column maps, money/date cleaning, FK resolution by natural key, deduplication, row-level error reports. Loads into ONE client workspace (`run_ingest(..., company_id=N)` backfills `company_id` and scopes FK lookups). |
| `forecast_engine.py` | **Monthly forecasting engine** (model `seasonal_trend_v1`, pure Python — no statsmodels/scipy needed): forecasts ONE workspace's series (`run_forecasts(company_id=...)` → `forecasts`, upserted per company) and writes the global `backtest_results` audit trail. CLI + importable API (`--company-id`). See §12. |
| `generate_pdf_brief.py` | **Board-ready Executive Financial Brief (PDF)** for one client workspace: reportlab platypus (no chart images) — executive KPIs, monthly P&L, service-line profitability, top open anomalies, forecast outlook. `generate_pdf_brief(company_id, months=12) -> bytes` + CLI (`--company-id 1 --out brief.pdf`). Powers the Overview tab's **Download Executive PDF Brief** button. |
| `pricing_simulator.py` | **What-if gross-margin math for one job quote** (`simulate_job()`): overtime premiums, material price swings, target/market margin comparisons. Pure, unit-tested; powers the dashboard's Pricing simulator tab. See §12.2. |
| `nl_to_sql.py` | **AI "Ask Your Data" engine** (§13): hand-rolled `.env` config (`load_config`/`save_config`), OpenAI-compatible chat client, the warehouse system prompt, strict read-only SQL guardrails (`validate_sql`), and a read-only query executor (`run_readonly_query`) that ALWAYS connects as the restricted `nl2sql_reader` role and sets the workspace GUC (database-enforced tenancy via row-level security — §13). Powers the dashboard's Ask Your Data tab (§9.10). |
| `app.py` | **Local web dashboard** (Streamlit): a sidebar **Client workspace** selector (multi-tenant — every client-data tab filters by the active `company_id`, and a **➕ New client workspace** form registers new clients), upload CSVs into the active workspace, run Refresh & Detect Anomalies, resolve alerts, explore interactive charts, simulate job pricing, generate forecasts & cash runway, download the **Executive PDF Brief**, and ask the AI analyst questions. `streamlit run app.py --server.port 8602` (see the tab-by-tab tour in §9). |
| `export_dataset_pack.py` | **Shareable dataset pack exporter**: clean CSVs + Parquet twins, PII-anonymized customers/employees, the real BLS/FRED benchmark rows, the 5 KPI views, a spec-generated `DATA_DICTIONARY.md` and a sha256 `MANIFEST.json` into `exports/smb_benchmark_pack_v1/`. See §15. |
| `funding_schema.sql` | **SoCal Capital & Deals module schema** (standalone, idempotent): `socal_capital_events` (one row per verified SEC Form D placement / SBA SBIR/STTR award in the six SoCal counties) + the `socal_county_capital_summary` county × quarter matview. Apply after `analytics_views.sql`. |
| `peer_ratios_schema.sql` | **Public Peer Financials module schema** (standalone, idempotent): `public_peer_financials` (SEC EDGAR XBRL facts + ratios for 50 public peers, FY2021-FY2025, incl. 20 SoCal companies) + the `mv_vertical_financial_quartiles` SoCal-vs-National percentile matview. Apply after `funding_schema.sql`. |
| `harvest_socal_capital.py` / `export_socal_capital.py` | **SoCal capital-flows harvester + exporter** (host-side): pulls real SEC EDGAR Form D filings and SBA SBIR/STTR awards, validates + dedupes into `socal_capital_events`, and writes the `exports/socal_funding_deals_v1/` pack (see §15). |
| `harvest_sec_peer_ratios.py` / `export_peer_ratios.py` | **SEC peer-ratios harvester + exporter** (host-side): pulls company-concept XBRL facts from data.sec.gov for the 50-ticker peer set into `public_peer_financials`, and writes the `peer_financial_ratios_v1/` pack (see §15). |
| `requirements.txt`, `Dockerfile`, `docker-compose.yml`, `docker-entrypoint.sh`, `docker/init_pipeline.py`, `.dockerignore` | **The Docker stack**: `docker compose up --build` gives you Postgres + the dashboard, bootstrapped automatically (schema → analytics views → module schemas → **migrations 001 + 002 (companies registry + RLS role/policies)** → seed → offline benchmark snapshot → forecasts → module export packs). See §3 and §14. |
| `ingest_config.example.json` + `samples/` | A ready-made mapping config and two deliberately messy sample exports (QuickBooks-style accounting, payroll timesheet) to try the ingest flow. `samples/macro_benchmarks_snapshot.csv` is a committed export of the real wage/PPI rows so the Docker stack works fully **offline**. |
| `test_schema.py`, `test_predictive.py`, `test_nl_to_sql.py`, `test_socal_fintech.py`, `test_socal_capital.py`, `test_multitenant_pdf.py` | Self-contained pytest suites (**675 tests + 1 skipped** as of the RLS isolation pass; each creates its own throwaway DBs and never touches your data). See §7. |
| `demo_queries.sql` | Twelve real business questions answered in SQL — a good place to learn. |
| `start_db.sh` / `stop_db.sh` | Start/stop the local Postgres server. |
| `.gitignore` | Keeps secrets (`.env`), exports, uploads and logs out of version control. |

## 3. Quickstart

### 3.1 Docker (one command — recommended for sharing)

```bash
docker compose up --build
```

That single command starts two containers:

- **`db`** — PostgreSQL 17 with a persistent `pgdata` volume. It is exposed on
  **host port 5433** (not 5432) so it cannot collide with a Postgres already
  running on your machine; connect from the host with
  `postgresql://postgres:postgres@localhost:5433/smb_warehouse`.
- **`app`** — the Streamlit dashboard on **http://localhost:8602**.

First start bootstraps everything automatically (core schema → analytics
views → **module schemas** `funding_schema.sql` + `peer_ratios_schema.sql` →
demo seed → offline benchmark snapshot → forecasts → **data-module export
packs**: the SoCal capital events + SEC peer financials CSVs shipped in the
image load into `socal_capital_events` / `public_peer_financials`). Notes:

- **Stop a dashboard already running on port 8602 first** (e.g. a local
  `streamlit run app.py`), or the container's port mapping will conflict.
- Data **persists in the `pgdata` volume** across restarts; the bootstrap is
  idempotent (it skips seeding when the warehouse already has data, but still
  re-applies the idempotent module schemas and tops up empty module tables
  from the packs). To wipe and reseed:
  `docker compose down -v && docker compose up --build`.
- **What boots with data vs schema-only:** the core SMB warehouse and the two
  data modules (SoCal Capital & Deals, Public Peer Financials) always come up
  **with data** — the demo simulation + the two modules' committed export-pack
  CSVs ship in the image. The **SoCal Benchmarks** module boots **schema-only**
  (its tables exist but stay empty): its data is a live multi-source harvest
  owned by `harvest_socal_fintech.py`, which is a HOST-side step
  (`python3 harvest_socal_fintech.py`); until you run it the SoCal Benchmarks
  tab shows its info hint and the other tabs are unaffected.
- **Offline fallback:** the container loads the committed macro snapshot
  `samples/macro_benchmarks_snapshot.csv` first, then tries
  `fetch_benchmarks.py` for the latest months — failing that (no internet) it
  logs a warning and continues with the snapshot data.
- Portability caveat: the compose stack was **statically validated** (YAML
  parse + logic review + entrypoint/pipeline dry-checks); Docker was not
  installed on the machine this project was authored on, so it has not been
  executed end-to-end there. See §14.

### 3.2 Local Mac/Linux (no Docker)

```bash
./start_db.sh                                          # if Postgres isn't running
psql "postgresql://postgres:postgres@localhost:5432/smb_warehouse" \
     -v ON_ERROR_STOP=1 -f schema.sql                  # rebuild core schema
psql "postgresql://postgres:postgres@localhost:5432/smb_warehouse" \
     -v ON_ERROR_STOP=1 -f analytics_views.sql         # rebuild analytics layer
psql "postgresql://postgres:postgres@localhost:5432/smb_warehouse" \
     -v ON_ERROR_STOP=1 -f funding_schema.sql          # SoCal capital module
psql "postgresql://postgres:postgres@localhost:5432/smb_warehouse" \
     -v ON_ERROR_STOP=1 -f peer_ratios_schema.sql      # SEC peer-ratios module
python3 seed_demo.py                                   # load demo data + run detector
python3 fetch_benchmarks.py                            # load REAL wage/PPI baselines
python3 forecast_engine.py all --company-id 1          # backtest + forecasts for one workspace
psql "postgresql://postgres:postgres@localhost:5432/smb_warehouse"   # explore
```

The two module schemas are standalone and idempotent (safe to re-run). Their
DATA comes from their own harvesters — on a host machine run
`python3 harvest_socal_capital.py` (SEC Form D + SBIR/STTR, ~3,596 events) and
`python3 harvest_sec_peer_ratios.py` (50 SEC EDGAR tickers, 1,193 rows); or
load a committed export pack's CSV instead (§15). The module matviews are
refreshed by the harvesters / pack loader.

Every script also honors `DATABASE_URL`, so pointing the whole project at a
different server is one environment variable:

```bash
export DATABASE_URL="postgresql://user:pass@host:5432/smb_warehouse"
```

Nightly-style maintenance once real data flows in:

```sql
SELECT refresh_all_analytics();     -- rebuild all view snapshots (non-blocking)
SELECT * FROM detect_anomalies();   -- writes new anomaly_alerts, returns them
```

Run the test suite anytime (it is fully isolated — see §7):

```bash
python3 -m pytest test_schema.py test_predictive.py test_nl_to_sql.py \
                   test_socal_fintech.py test_socal_capital.py -v
```

For day-to-day use, start the web dashboard instead:

```bash
streamlit run app.py --server.port 8602 --server.address localhost
# open http://localhost:8602
```

## 4. How the data model fits together

```
companies 1 ────< customers, job_types, employees, jobs,
                        financial_ledger, labor_logs,
                        anomaly_alerts, forecasts     (multi-tenant scope)

customers 1 ────< jobs >──── 1 job_types
                    │   │
          1 ┌───────┘   └───────────┐ 1
            │                       │
       labor_logs              financial_ledger
            │ 1                 (job_id NULL = overhead)
       employees 1

job_types 1 ────< market_benchmarks   (GLOBAL: market data, no company_id)
jobs      1 ────< anomaly_alerts
```

- **Multi-tenant by design (Fractional CFO deployment).** The `companies`
  registry scopes the 8 client-data tables by `company_id` (NOT NULL, FK,
  default 1, indexed). The demo workspace `BrightPath Works (Demo)` is
  company #1 — every pre-existing row belongs to it, so analytics are
  unchanged by tenancy. Market/benchmark data (`market_benchmarks`,
  `backtest_results`, the SoCal tables and the two data modules) is GLOBAL
  and shared across workspaces. Existing databases migrate in place via the
  idempotent `migrations/001_add_companies.sql` (applied on every Docker
  boot); fresh boots create the same shape directly in `schema.sql`.
- **`financial_ledger` is the single source of truth for money.** Every revenue
  and expense entry lives there. Sign convention: `amount` is *always positive*;
  the direction is carried by `entry_type` (`revenue`/`expense`). Attribution
  is exclusive: `job_id` set → belongs to a job; `customer_id` set → a
  customer-level entry with no job behind it (customer acquisition cost);
  both NULL → general overhead (rent, insurance, marketing).
- **`labor_logs` is the source of truth for labor cost.** Each row stores the
  employee's hourly rate *at the time of work* (a snapshot), so historical costs
  never change when you give someone a raise. Cost = `hours * hourly_rate`,
  computed in the views — never stored.
- **`jobs.estimated_labor_cost` is the budget baseline** the anomaly detector
  compares actual labor cost against.
- **`job_types` is a plain table, not a Postgres ENUM** — on purpose. Adding a
  new service line is a single `INSERT`, no DDL, no locking.
- Deleting is guarded: you can't delete a customer/employee/job that still has
  history (`ON DELETE RESTRICT`); ledger rows survive job deletion with their
  job reference nulled (`ON DELETE SET NULL`) so the books never lose entries.

## 5. The analytics layer

Materialized views are **stored snapshots** of expensive queries (regular views
re-compute on every read; materialized views compute once and you refresh them).
Each has a unique index, which allows `REFRESH ... CONCURRENTLY` — rebuilding
the snapshot *without* blocking anyone reading it.

| View | Grain | Answers |
|---|---|---|
| `mv_job_type_profitability` | 1 row per (workspace, service line) | Which service makes money? Are we priced above/below market? |
| `mv_labor_cost_analysis` | 1 row per (workspace, job with logged hours) | Which jobs blew their labor budget, worst first |
| `mv_customer_profitability` | 1 row per (workspace, customer; incl. zero-job customers) | Who are my best/worst clients? Who went quiet? **Who are the whales?** |
| `mv_monthly_summary` | 1 row per (workspace, month) | Cash P&L, completions, new customers, labor hours |

All four are **per client workspace** (`company_id` column): months, whales
and benchmarks never mix two clients' books. `mv_benchmark_comparison` is
per (workspace, period) for its INTERNAL series while its market baseline
columns are global; `mv_socal_rate_guidance` is global market data.

**Reconciliation rules** (deliberate, documented in the SQL):
- Revenue = ledger `revenue` rows (not `jobs.actual_revenue`, which is informational).
- Job/customer cost = ledger expenses *except* category `labor` + labor cost from
  `labor_logs` — so payroll recharges in the ledger never double-count field labor.
- `mv_customer_profitability` also adds **customer-attributed ledger expenses**
  (`acquisition_cost`) to the customer's cost base, so its margin is a *net*
  margin: it prices in what it cost to win the account.
- `mv_monthly_summary` is the *cash* P&L and **does** include ledger `labor`
  rows, so its labor figure can differ from `labor_logs` by design.

**Whale detection:** `mv_customer_profitability.is_whale` flags a client with
**top-quintile revenue** (revenue ≥ the 80th percentile across all customers
**of the same workspace** — whales are ranked within one client's own book,
never against another company's)
**but negative net margin** — the crown-jewel account that quietly loses money
once overruns and acquisition cost are counted. Find them with:

```sql
SELECT customer_name, revenue, total_cost, gross_margin, acquisition_cost
FROM   mv_customer_profitability
WHERE  is_whale
ORDER  BY gross_margin;
```

## 6. The anomaly detector

`detect_anomalies()` scans for two patterns **per client workspace** (a
LOOP over `companies` scopes every statistic to that workspace's rows, so
expense-spike statistics never mix two businesses; each alert carries its
`company_id`) and writes open alerts to
`anomaly_alerts` (returning the rows it inserted):

1. **`labor_overrun`** — completed/in-progress jobs whose actual labor cost
   (`labor_logs`) exceeds `estimated_labor_cost` by **>20%**.
   Severity: >50% = high, >30% = medium, else low.
2. **`expense_spike`** — a ledger expense category whose month total is more
   than **2 standard deviations above its prior-6-month rolling mean** (z > 2),
   needing ≥3 prior months and a non-zero stddev. Zero-filling guarantees
   absent months count as 0, so the window is exactly the prior 6 *calendar*
   months. Severity: z > 3 = high, > 2.5 = medium, else low.

Two engineering details worth knowing:
- **Idempotency:** the function uses a `NOT EXISTS` anti-join *plus*
  `ON CONFLICT ... DO NOTHING` against *partial unique indexes* (indexes that
  only cover rows `WHERE NOT is_resolved`). Re-running never creates duplicates;
  once you resolve an alert, the same subject may be flagged again later.
- **One statement, one snapshot:** the whole scan is a single CTE tree inside
  `RETURN QUERY`, so it sees a consistent view of the data mid-run.

In the seeded demo data the detector finds **71 alerts** across three years:
labor overruns (including 5 whale accounts at 150–225% over budget and ~10
planted mid-severity jobs), plus expense spikes — including two planted on
purpose (a lumber-price surge tripling materials in Oct 2026, and a marketing
blitz in Aug 2026). The remainder are genuine statistical findings: spring
materials/permits surges and payroll spikes during peak season — exactly what
a real detector surfaces on a seasonal business.

### 6.1 What the simulation models

`seed_demo.py` (`random.seed(42)` — fully deterministic, idempotent) builds:

- **36 months** (Dec 2023 → today) with per-service-line **seasonal demand
  curves** (exterior painting & decks collapse in winter, kitchen/bath
  remodels peak in winter, flooring bumps before the holidays) and ~1.5%/mo
  growth, scaled to **2,500+ jobs**
- **150 customers** acquired over time with a realistic size skew (a few
  dominant commercial/property accounts, a long tail of one-off households),
  each paying a **customer acquisition cost** booked via
  `financial_ledger.customer_id`
- **20 employees** hired across the years; historical hourly rates modeled
  with ~4%/yr raises (labor_logs rate snapshots), and **overtime rules**:
  hours beyond 8 in a day pay 1.5×. Peak-season jobs schedule longer days, so
  the OT premium is seasonal and estimates carry a matching allowance —
  detected overruns are genuine variance, not a systematic tax
- **5 engineered whales**: top-quintile revenue, ~200% labor overruns, heavy
  materials spend, expensive acquisition → negative net margin
- **Market benchmarks** per service line per month, and monthly overhead
  (rent, utilities, insurance, payroll recharge, marketing)

### 6.2 Real market baselines (`fetch_benchmarks.py`)

`market_benchmarks` holds two kinds of rows (`benchmark_kind`):

- **`job_market`** — per-service-line comparables from the simulation (what
  `mv_job_type_profitability` compares against).
- **`wage` / `ppi`** — **real public economic data** loaded by
  `fetch_benchmarks.py`, 72 months of monthly history per series (8 series ×
  72 months = 576 macro rows, +288 simulated `job_market` rows = 864 total),
  keyed by `series_id` + `region` + `period` (partial unique index →
  idempotent upserts). Currently loaded:

| Kind | Series | Region | What it is |
|---|---|---|---|
| wage | `CES2000000003` | us-national | BLS: avg hourly earnings, construction, all employees |
| wage | `CES2000000008` | us-national | BLS: … production & nonsupervisory employees (trade rates) |
| wage | `SMU29000002000000003` | us-mo | BLS: construction AHE, all employees, Missouri |
| wage | `SMU29000002000000008` | us-mo | BLS: construction AHE, production employees, Missouri |
| ppi | `WPU0822` | us-national | BLS PPI: softwood lumber |
| ppi | `PCU325510325510` | us-national | BLS PPI: paint & coating manufacturing |
| ppi | `WPUSI012011` | us-national | BLS PPI: construction materials (special index) |
| ppi | `WPUID612` | us-national | BLS PPI: materials & components for construction |

Provenance note: BLS's own API is blocked from this machine, so both families
are fetched from **FRED's keyless CSV endpoint** (`fred.stlouisfed.org/graph/
fredgraph.csv`), which serves the underlying BLS series unchanged. The
`source` column records the true origin of every row — and the committed
offline snapshot `samples/macro_benchmarks_snapshot.csv` carries the same
provenance into the Docker stack.

**`mv_benchmark_comparison`** pivots these against our own cost inflation,
one row per month of business activity:

- `internal_labor_rate` / `internal_labor_yoy_pct` — blended $/hr actually
  paid (hours-weighted, from `labor_logs`)
- `internal_materials_per_job` / `internal_materials_yoy_pct` — average
  materials spend per job (from the ledger)
- `market_wage_national` / `market_wage_regional` (+ YoY) — the BLS wage
  baselines
- `ppi_lumber` / `ppi_paint` / `ppi_building_materials` (+ YoY) — the PPI
  baselines
- `labor_inflation_gap_*_pct`, `materials_inflation_gap_pct` — **our YoY
  minus the market's YoY; positive = we are inflating faster than the market
  (margin squeeze)**

Missing months (FRED publication lag) surface as NULLs, never stale values.

## 7. How the tests stay safe

The six suites (`test_schema.py`, `test_predictive.py`, `test_nl_to_sql.py`,
`test_socal_fintech.py`, `test_socal_capital.py`, `test_multitenant_pdf.py`)
create their **own throwaway databases** (e.g. `smb_warehouse_qa_test`,
`smb_warehouse_qa_pred`, `socal_fintech_qa_test`, `smb_warehouse_qa_mt`) from
the schema files + migrations, run **675 tests + 1 skipped** against them, and
drop them afterwards — even when tests fail. Your warehouse and its data are
never touched, so you can run the full suite anytime:

```bash
python3 -m pytest test_schema.py test_predictive.py test_nl_to_sql.py \
                   test_socal_fintech.py test_socal_capital.py \
                   test_multitenant_pdf.py -v
```

Coverage: tables/PKs/FKs exist (including the ledger's exclusive job/customer
attribution), money columns are `NUMERIC(14,2)`, CHECK constraints actually
reject bad data (behavioral tests), the matviews exist / refresh / match
hand-computed numbers, `is_whale` semantics (percentile + negative margin),
`detect_anomalies()` fires on engineered overruns and z > 2 spikes, stays
silent below threshold, and is idempotent; the forecast engine's math,
no-look-ahead backtest and macro fallback are validated on small deterministic
scratch databases; the AI guardrails (`validate_sql`, the restricted
`nl2sql_reader` role, row-level security and the scoped_* views) are tested
without any network access; the multi-tenant suite proves RLS tenant
isolation with two seeded workspaces (role sees only its own rows, `WHERE
company_id=<other>` returns 0 because RLS ANDs with the query, no GUC
fail-closes to zero rows, raw matviews denied, writes blocked).

## 8. Ingesting your own CSVs

`ingest_csv.py` turns messy accounting/payroll exports into clean warehouse
rows. Everything is driven by a **JSON column-mapping config**
(`ingest_config.example.json` is a working example with two file specs):

```json
"column_map": {
  "Date":   {"column": "entry_date", "type": "date", "formats": ["%m/%d/%Y"]},
  "Amount": {"column": "amount", "type": "money_signed",
             "entry_type_from_sign": true},
  "Email":  {"column": "email", "type": "text", "lower": true}
},
"resolve": {
  "job_id": {"table": "jobs", "match_column": "job_number",
             "from_csv": "Job Number", "on_missing": "skip_row"}
},
"dedup": {"mode": "exact"}
```

What the engine does per row:

- **Cleaning** — strips whitespace, parses `$1,234.56` / `(1,289.45)` /
  `-45.5` money, M/D/YYYY-style dates (configurable formats), mixed-case
  emails, `N/A` sentinels.
- **Sign handling** — accounting exports record debits as negatives; the
  warehouse wants positive amounts with `entry_type` carrying direction, so
  `money_signed` + `entry_type_from_sign` does that conversion.
- **FK resolution** — CSVs name things (job numbers, emails, customer names),
  the warehouse wants ids. `resolve` looks each value up by natural key;
  `on_missing` picks the policy: `null`, `skip_row`, `error`, or `create`
  (customers only — the row's name becomes a new customer).
- **Deduplication** — `unique_key` mode upserts on a natural key
  (`ON CONFLICT`); `exact` mode hashes each mapped row, drops in-file
  duplicates and anti-joins rows already in the table. Re-running the same
  import is always a no-op.
- **Safety** — row-level errors are collected and reported with CSV line
  numbers (bad rows are skipped, never fatal); each file commits as its own
  transaction; when any file spec has `"refresh": true`, the loader refreshes
  the views and runs `detect_anomalies()` at the end.

```bash
python3 ingest_csv.py --config ingest_config.example.json [--dry-run]
```

The dashboard's **Ingest CSVs** tab does the same thing with file uploads and
the same presets (§9.4).

## 9. The dashboard: a tour of all ten tabs

```bash
streamlit run app.py --server.port 8602 --server.address localhost
```

Ten tabs, all backed by the same views/functions as the SQL layer. The
sidebar opens with the **Client workspace** selector (multi-tenant): every
client-data tab reads and writes the selected workspace's `company_id`, and
**➕ New client workspace** registers another client via a small inline form
(name required + duplicate-checked; optional industry vertical and SoCal
region). Below it sit **Refresh & Detect Anomalies** (refreshes the six core
views, runs the per-workspace detector, reports new alerts) and a guarded
**Reload demo data** (truncates the warehouse and replays `seed_demo.py` +
`fetch_benchmarks.py` as workspace #1). The three market tabs (SoCal
Benchmarks, SoCal Capital & Deals, Public Peer Financials) are labeled
"Market data — shared across client workspaces".

### 9.1 Overview

KPIs (revenue, gross profit, completions, open alerts) for the last full
month plus the whale count, and the full `mv_monthly_summary` table — the
month-by-month cash P&L — **for the active client workspace**. The
**Download Executive PDF Brief** button generates a board-ready 2–3 page
PDF (`generate_pdf_brief.py`: executive KPIs, monthly P&L, service-line
profitability, top open anomalies, forecast outlook) for the active
workspace. The other KPIs update whenever you click **Refresh & Detect
Anomalies** in the sidebar.

### 9.2 Charts

Interactive Plotly, five panels: grouped monthly P&L bars with a gross-profit
line; gross margin by service line (horizontal bars); the 15 worst labor
overruns; the customer margin scatter (bubbles sized by job count, whales in
red); and two inflation charts comparing our labor/materials YoY against the
real BLS wage and construction-materials PPI baselines (§6.2).

### 9.3 Anomaly alerts

The triage desk. Lists every **open** alert (severity-sorted) with its
subject, metric, baseline, deviation and z-score. Tick the *Resolve?*
checkboxes, optionally type a resolution note (stored in the alert's JSONB
`details`), and click **Resolve selected alerts**. The detector can re-flag a
resolved subject if it recurs — that's the partial-unique-index design from
§6.

### 9.4 Ingest CSVs

Upload one or more CSVs, then either pick a **mapping preset** (QuickBooks-style
accounting export / payroll timesheet) or upload a custom JSON column-mapping
config. Click **Ingest uploads** to run it through `ingest_csv.run_ingest`
(§8): you get rows read / loaded / duplicates-skipped per file, the skipped-row
reasons with line numbers, and — when the config asks for it — an automatic
view refresh + anomaly scan report.

### 9.5 Pricing simulator

What-if gross-margin math for one job quote, prefilled from the selected
service line's live averages (§12.2): move the five sliders (crew hours,
overtime share, material cost change, price adjustment, target margin) and the
tab recomputes the margin, the cost-breakdown bar and the comparison against
the actual service-line averages and the market benchmark.

### 9.6 SoCal Benchmarks

Regional Finance/Tech benchmark data for the six SoCal metros, loaded by
`harvest_socal_fintech.py`: REAL BLS OEWS wages (May 2024), FRED/BLS macro
series (CPI, PPI office leases, employment), Census CBP/ZBP business patterns,
SBA 7(a)/504 lending — plus the modeled vertical economics behind
`mv_socal_rate_guidance` (hourly cost → loaded cost → suggested billable
rate). Pick a metro region and a vertical; every section degrades to an info
hint until the harvest has run. This module boots schema-only in Docker (§3.1).

### 9.7 Forecast & cash runway

Two buttons: **Generate forecasts** (runs `seasonal_trend_v1` 3 months ahead
for all four series, same as the CLI's `forecast` command) and **Run backtest**
(walk-forward validation, train 30 / test 6, same as `backtest`). Below,
pick a series to see actuals + the forecast line with its 90% confidence
band, the cumulative **cash runway** projection, and the backtest accuracy
table (§12.1).

### 9.8 SoCal Capital & Deals

REAL harvested capital events for the six SoCal counties
(`harvest_socal_capital.py` → `funding_schema.sql`): SEC Form D private
placements + SBA SBIR/STTR awards — one row per verified event, each with its
filing/award id and EDGAR source URL. Three filters (county, sector,
funding-source toggle All/Form D/SBIR-STTR) drive everything via bound SQL
parameters: KPI metrics (total capital raised, event count, latest event
date), a capital-by-county horizontal bar, a capital-by-sector bar, a monthly
raised-capital area chart, and a most-recent-first events table (capped at
500 rows). A median-deal-size caption keeps the mega-fund Form D filings in
perspective.

### 9.9 Public Peer Financials

REAL SEC EDGAR XBRL financials for 50 public peers, FY2021-FY2025, including
20 Southern California public companies (`harvest_sec_peer_ratios.py` →
`peer_ratios_schema.sql`). Pick a vertical, a ratio metric (gross margin /
operating margin / R&D % of revenue / SG&A % of revenue) and a fiscal year:
the **SoCal vs National** grouped bar compares the p25/median/p75 quartile
bands from `mv_vertical_financial_quartiles`; a revenue-vs-ratio scatter
highlights the SoCal cohort (log revenue axis); and the sortable company
table shows every company-period row for the vertical with per-filing EDGAR
source links.

### 9.10 Ask Your Data

Ask plain-English business questions; an LLM turns each into one strictly
read-only SQL query that you inspect/edit before it runs, with an auto chart.
Six one-click starter questions are included. Results are **database-enforced
to the active workspace** via row-level security (§13) — the query runs as a
restricted role with your workspace id set at the session level. To enable it
you need an API key — see the setup walkthrough in §13 (AI provider settings,
GLM/Zhipu, OpenAI, Ollama or any OpenAI-compatible endpoint).

## 10. Concepts used here (a quick glossary)

- **Materialized view** — a query whose result is stored on disk; refresh to update.
- **`REFRESH ... CONCURRENTLY`** — rebuild a matview without blocking readers;
  requires a unique index on the view.
- **Window functions** — `AVG(...) OVER (ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)`
  computes a rolling 6-month mean per row; this is how the spike detector works.
- **`STDDEV_SAMP`** — sample standard deviation; needs ≥2 data points, we require ≥3.
- **Partial unique index** — `CREATE UNIQUE INDEX ... WHERE NOT is_resolved`:
  uniqueness enforced only for open alerts.
- **`ON CONFLICT` with index inference** — insert that turns into a no-op when
  it would violate the (partial) unique index; the `WHERE` clause must match
  the index predicate exactly.
- **`FILTER (WHERE ...)`** — conditional aggregation: one pass, many buckets
  (`SUM(amount) FILTER (WHERE entry_type='revenue')`).
- **`LATERAL` join** — a subquery that can reference the row currently being
  joined; used for "latest benchmark per type" and per-customer roll-ups.
- **Identity columns** — `GENERATED BY DEFAULT AS IDENTITY`: auto-numbering PKs.
- **NUMERIC(14,2)** — exact decimal for money (never `float`, which rounds).

## 11. Where things live on this machine

| What | Where |
|---|---|
| Postgres 17.5 binaries | `/Users/Owner/pg` (`psql` etc. symlinked into `~/bin`) |
| Database cluster / data dir | `/Users/Owner/pgdata/smb_warehouse` |
| Server log | `/Users/Owner/pgdata/smb_warehouse.log` |
| Multi-tenant migrations | `<project folder>/migrations/001_add_companies.sql` (companies registry) and `migrations/002_add_rls.sql` (RLS policies + `nl2sql_reader` role + `scoped_*` views); applied by `docker/init_pipeline.py` on every boot |
| AI-SQL read role | DB role `nl2sql_reader` (NOSUPERUSER, NOBYPASSRLS; SELECT grants only). Documented default password: `nl2sql_readonly_local` — override the app side via `NL2SQL_DB_ROLE` / `NL2SQL_DB_PASSWORD` (in `.env` or the environment); rotate the DB side with `ALTER ROLE nl2sql_reader PASSWORD '...'`. Connect: `psql "postgresql://nl2sql_reader:nl2sql_readonly_local@localhost:5432/smb_warehouse"` |
| Python packages (`psycopg2`, `pytest`) | user site-packages (`~/Library/Python/3.9`) |

Connection string for any client (Metabase, pgAdmin, Python, Tableau):
`postgresql://postgres:postgres@localhost:5432/smb_warehouse`

(Docker users: the containerized Postgres is reachable on host port **5433**
instead — see §3.1. Every Python script also accepts a `DATABASE_URL`
environment variable, so the same code runs unchanged against either.)

## 12. Forecasting & pricing simulator

Two pure-Python decision tools (no statsmodels/scipy needed — least squares,
seasonal indices and residual sigma are closed-form) that write into two tables
from `schema.sql`:

- **`forecasts`** — one row per (series, forecast month, model); the engine
  **upserts**, so the table always holds the latest run, never a growing
  history. Columns: `predicted`, `ci_low`/`ci_high` (90% band), `mape`/`rmse`
  (backtest accuracy of the model), `horizon_months`, `run_date` and a
  `details` JSONB carrying the full model provenance (train window, seasonal
  factors, drift blend, sigma and its source).
- **`backtest_results`** — one row per (series, model, fold); fold label
  encodes the window, e.g. `train_2023-12_2026-05_test_2026-06_2026-11`.

### 12.1 Model `seasonal_trend_v1` (`forecast_engine.py`)

Per series (sources: `revenue` = ledger revenue sums; `labor_cost` =
`labor_logs` hours × rate; `material_expense` = ledger `materials` expenses;
`net_cash_flow` = monthly revenue − total expense from the ledger, which can
genuinely be negative):

1. Monthly history, zero-filled between first/last observed month.
2. **Multiplicative seasonal index** per calendar month on the train window:
   month average ÷ overall average (needs ≥ 12 months, else 1.0).
3. De-seasonalize, fit a **least-squares linear trend**.
4. **Drift blending** — monthly growth = ½ × model trend (slope ÷ trend level,
   **winsorized to ±5%/mo**) + ½ × market annual inflation converted to
   monthly (`(1+annual)^(1/12)−1`), from `market_benchmarks` macro rows:
   `labor_cost` → `CES2000000008` (BLS construction wage), `material_expense`
   → `WPUSI012011` (construction-materials PPI), `revenue`/`net_cash_flow` →
   the average of the two. Macro missing → model trend alone, flagged in
   `details`.
5. `predicted = last deseasonalized level × seasonal[month] × (1+drift)^h`
   for h = 1..months_ahead (default 3 ≈ 90 days). Growth is applied only via
   the drift, so the trend is never double-counted.
6. **90% confidence interval:** `predicted ± 1.645 × σ × √h` (1.645 = two-sided
   90% normal quantile; √h widens the band with horizon). **σ** is the
   *out-of-sample* RMSE pooled across the backtest folds when a backtest has
   run; on a first run it falls back to the in-sample residual sigma of the
   trend × seasonal fit (`details.sigma_source` records which). `ci_low` is
   clamped ≥ 0 for the non-negative series; `net_cash_flow` is not clamped —
   a negative cash projection is meaningful.
7. **Accuracy metric convention (MAPE vs WAPE):** the positive series
   (`revenue`, `labor_cost`, `material_expense`) use **MAPE** = mean
   |error|/|actual| over non-zero-actual months. `net_cash_flow` has negative
   months, which make MAPE undefined, so it uses **WAPE** =
   SUM|error| / SUM|actual| — this convention holds everywhere
   (`forecasts.mape` and `backtest_results.mape` both carry MAPE for the
   positive series and WAPE for net cash flow; the column comments say so).

**Backtest** (walk-forward, expanding origin): a fold trains on the series'
first `train_months` (default 30) months and predicts the next `test_months`
(default 6) at horizons h = 1..6; later folds shift forward while a full test
window fits. Macro drift data is read **as of the fold's train end**, so no
future information leaks into a validation fold. With the current 36-month
history the defaults produce exactly one fold per series.

```bash
python3 forecast_engine.py forecast [--months 3]
         [--company-id 1]                        # upsert one workspace's forecasts
python3 forecast_engine.py backtest [--company-id 1]  # walk-forward validation
python3 forecast_engine.py all [--months 3] [--company-id 1]
                                                   # backtest FIRST, then
                                                   # forecasts (CIs then use
                                                   # real out-of-sample sigma)
```

The dashboard's **Forecast & cash runway** tab runs the same functions via
its *Generate forecasts* / *Run backtest* buttons (§9.7). Known caveats worth
knowing: the level is the **single last deseasonalized month**, so a spiky
final month propagates into the forecast; the current month is always
treated as complete (a partial month understates its seasonal index inputs);
and Dec 2023 is a business-startup month, which drags the December seasonal
index low. Re-run `all` after any data reload (the demo reload truncates the
ledger but leaves forecast rows in place until regenerated).

### 12.2 Pricing simulator (`pricing_simulator.py`)

One function, pure math, unit-tested:

```python
from pricing_simulator import simulate_job
simulate_job(hours, base_rate, ot_pct, material_ratio, material_change_pct,
             price, other_cost_ratio=0.05, target_margin=0.30,
             market_margin=None)
```

- `labor = hours × base_rate × (1 + 0.5 × ot_pct)` — the overtime premium
  pays **1.5× on the `ot_pct` share of hours** (`ot_pct = 0.30` → 30% of the
  crew's hours are OT-eligible).
- `materials = price × material_ratio × (1 + material_change_pct)`;
  `other = price × other_cost_ratio` (equipment/permits/disposal, 5% default).
- `gross_margin = price − labor − materials − other`; the result dict also
  gives `margin_pct`, `margin_vs_target_pct` and `margin_vs_market_pct` —
  both comparisons in **percentage points** against the target (a fraction,
  default 0.30) and the market benchmark's `avg_margin_pct` (pass the
  fraction, e.g. 0.3632).
- **Validation/clamps (documented in the module docstring):** `price > 0` and
  `hours > 0` are required (else `ValueError`); `ot_pct` is clamped to
  [0, 0.5] and `material_change_pct` to [−1.0, 3.0]; any clamp applied is
  reported in the returned `clamps_applied` list.

The dashboard's **Pricing simulator** tab prefills the function from live
data per service line (§9.5): avg hours/job (`labor_hours/job_count`), blended
base rate (`labor_cost/labor_hours`, hours-weighted), materials ratio
(`materials_cost/revenue`), the trailing-12-month average quoted price, and
the national benchmark margin — then lets you move five sliders (hours
0.25×–3×, OT 0–50%, materials −30–+100%, price −25–+50%, target margin
15–60%) and shows the margin, the breakdown bar and the actual service-line
averages for comparison.

## 13. Ask Your Data (AI Financial Analyst)

The dashboard's tenth tab (**Ask Your Data**, §9.10) answers business
questions in plain English. An LLM translates each question into **one
strictly read-only SQL query**; you inspect (or edit) the SQL before it runs;
the result renders as a table (capped at 500 rows) plus an automatic chart —
a line chart when the result has a date-like column and numeric columns, a
bar chart when it has a category column and 1–2 numeric columns (skipped for
≤1 or >200 rows). `nl_to_sql.py` is the engine; the tab is only the
interface, and it never touches `qdf()` or any other dashboard code path —
the AI query runs on its own connection.

Because the allowlist now covers the whole warehouse — core SMB tables, the
SoCal benchmark tables, the SoCal capital-flows module
(`socal_capital_events`, `socal_county_capital_summary`) and the SEC
peer-ratios module (`public_peer_financials`,
`mv_vertical_financial_quartiles`) — the analyst can answer **cross-dataset
questions** too, e.g. "compare average deal size in Orange County to the
national p50 revenue for the software vertical" (funding events joined
against peer benchmarks), "which SoCal counties raised the most Form D
capital vs SBIR awards", or "how does our labor cost inflation compare to
public-peer SG&A trends" — always through the same four guardrail layers.

### How it works (generate → inspect/edit → run)

1. **Generate** — your question goes to the configured LLM with a system
   prompt describing the exact warehouse surface: the 11 core tables
   (including the `companies` registry), the 6 SoCal benchmark tables, the
   4 data-module relations (capital events + county summary; peer
   financials + vertical quartiles) and the 6 analytics views — the 5
   company-scoped `scoped_*` wrappers over the raw matviews plus the global
   `mv_socal_rate_guidance` — with their key columns, the sign conventions
   (`amount` always positive, `entry_type` carries direction,
   `socal_capital_events.amount_raised` always positive with `event_type`
   telling Form D raises from SBIR/STTR awards), the whale flag, the ledger
   `category` values — plus hard rules (one plain SELECT, no comments, no
   semicolons, allowlisted relations only, LIMIT ≤ 500, prefer the
   matviews). The model gets **one repair retry**: if its SQL fails a
   guardrail, the exact rejection reason is fed back and it tries again.
2. **Inspect / edit** — the generated SQL appears in the *SQL preview* box.
   Edit it freely: the guardrails are re-applied to whatever finally runs,
   so editing can never add anything dangerous.
3. **Run** — the query executes on a dedicated read-only connection that
   always logs in as the restricted `nl2sql_reader` role with the ACTIVE
   workspace's `company_id` set at the session level (row-level security —
   see the next section); results show as a table and auto-chart. Six
   one-click starter questions cover the most common asks (best-margin
   service line, loss months, whales, materials vs PPI, top customers,
   open alerts).

### The four guardrail layers

1. **Parser-level scan (`validate_sql`)** — string literals are masked first
   (so `WHERE name LIKE '%update%'` is not a false positive), then: a
   forbidden-keyword word-boundary scan (INSERT, UPDATE, DELETE, DROP,
   ALTER, TRUNCATE, CREATE, GRANT, COPY, VACUUM, MERGE, DO, CALL, SET, …),
   and a **FROM/JOIN allowlist**: only the 27 warehouse relations (11 core
   tables incl. the `companies` registry + 6 SoCal benchmark tables + 2
   SoCal capital-flows relations + 2 SEC peer-ratios relations +
   `mv_socal_rate_guidance` + the 5 `scoped_*` views) may be referenced —
   `pg_catalog`, `information_schema`, the raw `mv_*` SMB matviews and
   everything else are rejected (`relation not allowed: X`). Comma-separated
   relations in a FROM list are checked too. If the statement has no LIMIT,
   one is appended automatically.
2. **Single statement** — one optional trailing `;` is tolerated; any other
   semicolon (multi-statement input) or SQL comment (`--`, `/* */`) is
   rejected outright, so "one SELECT" cannot smuggle a second command.
3. **Read-only transaction + 5-second timeout** — `run_readonly_query`
   opens its own connection with `SET SESSION CHARACTERISTICS ... READ
   ONLY` and runs under a `SET LOCAL statement_timeout` (default 5 s). Even
   a hypothetical guardrail bypass cannot commit a write (the transaction
   is rolled back) or hang the dashboard; over-eager queries die with
   "query exceeded the 5-second timeout" instead.
4. **Restricted role + row-level security (database-enforced tenancy)** —
   every AI query runs as the restricted `nl2sql_reader` role
   (`NOSUPERUSER`, `NOBYPASSRLS`, SELECT grants only) with
   `SET LOCAL app.current_company_id = '<active workspace>'`, so PostgreSQL
   itself filters every client-data row to that workspace (layer details in
   the next section).

The layering matters: layer 1 gives fast, explainable rejections; layers 3
and 4 are enforced by PostgreSQL itself. The guardrails apply equally to the
LLM's SQL and to your hand-edited SQL.

### Row-level security: what the AI can and cannot see

This is the piece the parser alone cannot deliver: **tenancy that holds even
when the generated SQL omits the `company_id` filter.** PostgreSQL's
row-level security (RLS) does not apply to materialized views, and the
dashboard's own trusted code paths connect as the `postgres` superuser (which
bypasses RLS by design — they set `company_id` explicitly and are QA-proved
that way). So the enforcement target is the AI-generated-SQL path
(`migrations/002_add_rls.sql`, applied on every boot):

* **The role.** `nl2sql_reader` — a LOGIN role with `NOSUPERUSER
  NOCREATEDB NOCREATEROLE NOBYPASSRLS`. Documented default password:
  `nl2sql_readonly_local` (a local-only read role; rotate with
  `ALTER ROLE ... PASSWORD` and set `NL2SQL_DB_PASSWORD` in the app's
  environment — `nl_to_sql.py` reads `NL2SQL_DB_ROLE` / `NL2SQL_DB_PASSWORD`
  with these defaults). The migration creates it guarded by a `pg_roles`
  check and re-asserts its restriction attributes on every boot; the
  password is set only at creation so a rotation survives reboots.
* **The policies.** Row-level security is enabled on the 8 client tables
  (`customers`, `job_types`, `employees`, `jobs`, `financial_ledger`,
  `labor_logs`, `anomaly_alerts`, `forecasts`) **and** on the `companies`
  registry — 9 policies named `tenant_isolation_<table>`, each
  `USING (company_id = NULLIF(current_setting('app.current_company_id',
  true), '')::bigint)` with the same expression as `WITH CHECK`.
* **Fail-closed.** An unset or empty GUC yields NULL, so a session that
  never sets `app.current_company_id` sees **zero rows** in every client
  table. `run_readonly_query(company_id=None)` does exactly that — the app
  always passes the active workspace, and a non-int-coercible
  `company_id` raises `ValueError` before any connection is opened.
* **RLS ANDs with your WHERE.** With the GUC set to workspace 2, even a
  generated `SELECT ... WHERE company_id = 1` returns 0 rows — the policy
  predicate is ANDed with the query's, so a careless or hostile query
  cannot escape its workspace.
* **The scoped_* views.** RLS cannot be attached to a materialized view, so
  the 5 raw SMB matviews (`mv_monthly_summary`, `mv_job_type_profitability`,
  `mv_customer_profitability`, `mv_labor_cost_analysis`,
  `mv_benchmark_comparison`) are wrapped in 5 regular views
  (`scoped_monthly_summary` … `scoped_benchmark_comparison`) whose
  definition carries the same GUC predicate (`security_barrier`; default
  DEFINER semantics — the role needs no privilege on the raw matviews, and
  gets none: a direct `SELECT` on a raw `mv_*` is permission-denied). The
  allowlist now names the `scoped_*` views instead of the raw matviews.
* **What the AI CAN see:** the active workspace's client rows (and nothing
  else — even the `companies` registry collapses to its own row), plus all
  the global/market data (`market_benchmarks`, `backtest_results`, the
  SoCal benchmark tables, `socal_capital_events`,
  `socal_county_capital_summary`, `public_peer_financials`,
  `mv_vertical_financial_quartiles`, `mv_socal_rate_guidance`) which carries
  no `company_id` and is shared by design.
* **What the AI CANNOT do:** write anything (no INSERT/UPDATE/DELETE
  grants — plus the read-only transaction), create anything, bypass RLS, or
  see another workspace's rows — even without a `WHERE` clause.

Verified end-to-end by the test suites: the multi-tenant suite seeds two
workspaces in a throwaway database and proves the role sees only its own
rows, `WHERE company_id=<other>` returns 0, no GUC returns 0, scoped views
match the raw matviews for the owning workspace, raw matviews are denied and
writes are blocked; the Ask-Your-Data suite pins the fail-closed executor
behavior on the live warehouse.

### Setup: get a GLM (Zhipu) API key

1. Sign in at the Zhipu AI open platform console (open.bigmodel.cn) and
   create an API key.
2. In the dashboard, open **Ask Your Data → AI provider settings**. The
   default **GLM (Zhipu)** preset fills the base URL
   (`https://open.bigmodel.cn/api/paas/v4`) and model (`glm-4-flash`).
3. Paste the key into the **API key** field and click **Save settings to
   .env** — the key is written to the local `.env` file in this folder and
   nowhere else; requests go only to the endpoint chosen above.

Quick test without the dashboard (needs the key in `.env`):

```bash
python3 nl_to_sql.py "Which service line has the best gross margin?"
```

### Switching providers

Pick another preset in **AI provider settings** — OpenAI
(`https://api.openai.com/v1`, `gpt-4o-mini`), a local Ollama
(`http://localhost:11434/v1`, `llama3.1`) or **Custom** (any
OpenAI-compatible `/chat/completions` endpoint) — then **Save settings to
.env** again. Any server that speaks the OpenAI chat-completions protocol
works. The key never leaves this machine except to the endpoint you
configure; there is no other network call in the feature.

### Troubleshooting

- **"query exceeded the 5-second timeout"** — the SQL was too heavy (a base
  table scan instead of a matview). Rephrase toward the pre-aggregated views,
  or narrow the query in the SQL preview. The timeout is a guardrail, not a
  bug.
- **HTTP 401 / "LLM endpoint returned HTTP 401"** — the key is missing,
  empty or wrong for that endpoint. Re-paste it in the settings expander and
  save; make sure the preset matches the key's provider (a Zhipu key only
  works against the Zhipu base URL).
- **"Could not produce a valid read-only query"** — the model's SQL was
  rejected twice; the note shows the exact verdict (e.g. `forbidden keyword:
  insert`). Try a simpler question, or write the SELECT yourself in the SQL
  preview and run it.
- **"relation not allowed: X"** — the SQL referenced a relation outside the
  warehouse allowlist; only the 27 relations listed in §13's system prompt
  are queryable (the raw `mv_*` SMB matviews are among the rejected names —
  the AI reads the `scoped_*` views instead).
- **"permission denied: the AI read-only role …"** — the SQL tried to read a
  relation the `nl2sql_reader` role has no grant on (the raw matviews) or to
  run a non-SELECT statement. That is the row-level-security design working;
  nothing was changed. Use the `scoped_*` views / a plain SELECT.
- **No key yet** — the tab shows an info banner and *Generate SQL* stays
  disabled until a key is saved; everything else in the warehouse keeps
  working.

## 14. Docker deployment (the one-command stack)

| File | Role |
|---|---|
| `Dockerfile` | `python:3.11-slim` image; non-root `appuser`; installs `requirements.txt` (no apt packages needed — SQL is applied via psycopg2, not `psql`); `ENTRYPOINT` is `docker-entrypoint.sh`. |
| `docker-compose.yml` | Two services: `db` (postgres:17-alpine, `pgdata` volume, `pg_isready` healthcheck, **host port 5433 → container 5432**) and `app` (builds the image, `DATABASE_URL=postgresql://postgres:postgres@db:5432/smb_warehouse`, port 8602, `depends_on: db: service_healthy`, restart unless-stopped). |
| `docker-entrypoint.sh` | Waits for the DB (psycopg2 retry loop, ~30 s cap), runs the bootstrap pipeline, then `exec`s `streamlit run app.py --server.port 8602 --server.address 0.0.0.0` (0.0.0.0 is what makes the port mapping reachable). |
| `docker/init_pipeline.py` | **Idempotent bootstrap**. Boot order: `schema.sql` → `analytics_views.sql` → `funding_schema.sql` → `peer_ratios_schema.sql` → **`migrations/001_add_companies.sql` → `migrations/002_add_rls.sql`** (all idempotent and re-applied on every boot, so a persisted volume that predates them picks them up — 001 upgrades pre-existing volumes to the multi-tenant shape in place and 002 creates the `nl2sql_reader` AI-read role, the row-level-security policies, the reader grants and the `scoped_*` wrapper views) → skip everything else if `jobs` already has rows ("already initialized, skipping seed"); otherwise seeds the simulation (`seed_demo.main()`, seed 42), loads the **offline snapshot** `samples/macro_benchmarks_snapshot.csv` into `market_benchmarks` with the exact same upsert `fetch_benchmarks.py` uses, attempts the online `fetch_benchmarks.py` top-up (failure tolerated with a warning), runs the forecast engine (backtest first, then forecasts — same as `forecast_engine.py all`), and finally loads the **data-module export packs**: `exports/socal_funding_deals_v1/socal_capital_events.csv` → `socal_capital_events` (ON CONFLICT DO NOTHING on the event id; empty tables only) and `peer_financial_ratios_v1/peer_financials.csv` → `public_peer_financials` (ON CONFLICT on the company-period key; the `record_id` sequence is re-synced), then REFRESHes both module matviews. A missing pack only logs `module X: harvest required — run <script> on the host` and boots schema-only; the container never re-harvests. Step-by-step log lines throughout. |

Portability details worth knowing:

- Every Python entry point (`app.py`, `seed_demo.py`, `fetch_benchmarks.py`,
  `forecast_engine.py`, `ingest_csv.py`, `nl_to_sql.py`,
  `docker/init_pipeline.py`) reads `DATABASE_URL` and falls back to the
  original local default, so the same files run inside the container and on
  the host.
- `.dockerignore` keeps `uploads/`, logs, caches, test files and `.env*` out
  of the image; `samples/` **is** included (the offline snapshot lives
  there). Under `exports/` only the two data-module pack CSVs are re-included
  (`exports/socal_funding_deals_v1/socal_capital_events.csv`), and at the
  project root only `peer_financial_ratios_v1/peer_financials.csv` — exactly
  the files `docker/init_pipeline.py` can load, so the container boots the
  two data modules with real data while every other pack stays local.
- **Validation caveat:** Docker is not installed on the machine this project
  was authored on, so the stack was validated **statically** — the compose
  file parses cleanly (YAML), the Dockerfile's COPY paths exist, the
  entrypoint's wait/exec logic and the pipeline's idempotency were reviewed
  and the pipeline script was syntax-checked — but it has not been executed
  end-to-end there. On a Docker machine, `docker compose up --build` is the
  only step.

## 15. The shareable dataset packs

The warehouse ships **four** self-contained export packs (CSV + Parquet twins,
data dictionary, sha256 manifest). All four are documented by their own
`MANIFEST.json` / dictionary inside the pack; every pack regenerates from its
owning scripts and stays local (`exports/` is git-ignored; only the two CSVs
`docker/init_pipeline.py` loads enter the Docker image — §14).

| Pack | Path | What's inside | Regenerate with |
|---|---|---|---|
| `smb_benchmark_pack_v1` | `exports/smb_benchmark_pack_v1/` | Core SMB warehouse: `jobs` (2,502), `labor_logs` (18,504), `financial_ledger` (6,373), PII-anonymized `customers`/`employees`, `market_benchmarks` (864 real BLS/FRED rows), `anomaly_alerts` (71), `forecasts` (12), `backtest_results` (4) + the 5 core KPI views. | `python3 export_dataset_pack.py` |
| `socal_fintech_benchmark_pack_v2` | `exports/socal_fintech_benchmark_pack_v2/` | SoCal Finance/Tech benchmarks: `socal_wage_benchmarks` (48), `socal_macro_indicators` (1,238), `socal_business_patterns` (36,219), `socal_loan_activity` (84), `regional_industry_profiles` (36), `socal_harvest_log` (76), `mv_socal_rate_guidance` (36). | `python3 harvest_socal_fintech.py && python3 export_dataset_pack.py --pack socal_v2` |
| `socal_funding_deals_v1` | `exports/socal_funding_deals_v1/` | SoCal capital flows: `socal_capital_events.csv` (3,596 verified events — SEC Form D raises + SBIR/STTR awards, Nov 2023–Nov 2026), `socal_county_quarter_summary` (74), auxiliary `socal_firm_filings_metadata` (4,925 13F-HR rows, not raises). | `python3 harvest_socal_capital.py && python3 export_socal_capital.py` |
| `peer_financial_ratios_v1` | `peer_financial_ratios_v1/` (project root) | SEC EDGAR peer financials: `peer_financials.csv` (1,193 company-periods, 50 tickers FY2021-FY2025, 20 SoCal), `vertical_quartiles.csv` (50 SoCal/National benchmark rows). | `python3 harvest_sec_peer_ratios.py && python3 export_peer_ratios.py` |

The two module packs double as the Docker boot data source: the image carries
`exports/socal_funding_deals_v1/socal_capital_events.csv` and
`peer_financial_ratios_v1/peer_financials.csv`, and
`docker/init_pipeline.py` loads them into the module tables (§14) — so the
container gets the real harvested data without ever re-harvesting.

### 15.1 The core pack in detail (`export_dataset_pack.py`)

```bash
python3 export_dataset_pack.py
```

```bash
python3 export_dataset_pack.py
```

writes a documented, self-contained pack to `exports/smb_benchmark_pack_v1/`
(project-relative) — safe to zip and share:

- **Clean UTF-8 CSVs + Parquet twins** for the big tables: `jobs`,
  `labor_logs`, `financial_ledger`, `customers_anonymized` (PII stripped:
  email/phone/address/contact_name dropped), `employees_anonymized`
  (email/phone dropped), `market_benchmarks` (all 864 rows — the real
  BLS/FRED macro rows keep their `series_id`/`source` provenance),
  `anomaly_alerts`, `forecasts`, `backtest_results`.
- **The 5 KPI materialized views as CSV** — the "anonymized KPI summaries":
  `mv_monthly_summary`, `mv_job_type_profitability`,
  `mv_customer_profitability`, `mv_labor_cost_analysis`,
  `mv_benchmark_comparison`.
- **`DATA_DICTIONARY.md`** — generated **from the export spec inside the
  script, never hand-counted**: for every file, the description, provenance
  (synthetic simulation via `seed_demo.py` seed 42 vs real BLS/FRED via
  `fetch_benchmarks.py`), the exact row count and every column with dtype +
  one-line description. The exporter asserts each DataFrame's columns
  against the spec before writing, so the dictionary cannot drift from the
  data.
- **`MANIFEST.json`** — per file: rows, sha256, size; plus `exported_at`,
  generator versions (python/pandas/pyarrow) and a top-level `regenerate`
  note: `python3 seed_demo.py && python3 fetch_benchmarks.py && python3
  export_dataset_pack.py`.

The exporter reads the live warehouse with **read-only SELECTs only**, is
idempotent (it overwrites its own outputs), and prints a final manifest
table. `exports/` is git-ignored, so packs stay local — except the two
module-pack CSVs the Docker image re-includes (§14).
