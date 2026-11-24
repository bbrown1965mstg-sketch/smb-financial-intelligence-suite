#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
export_peer_ratios.py
=====================
Audited Finance & Tech Peer Financial Ratios Dataset (exports/peer_financial_ratios_v1/)
Sub-agent 1: SEC XBRL Financial Data Engineer.

Exports the verified dataset from the local smb_warehouse PostgreSQL database
into ``exports/peer_financial_ratios_v1/``:

  peer_financials.csv / .parquet        -- public_peer_financials (row-level facts)
  vertical_quartiles.csv / .parquet     -- mv_vertical_financial_quartiles (benchmarks)
  PEER_RATIOS_DATA_DICTIONARY.md        -- column docs + full methodology
  MANIFEST.json                         -- SHA-256 checksums + metadata for every file

The materialized view is refreshed (CONCURRENTLY) before export so the quartiles
always reflect the loaded data. Exports are deterministic (stable column order,
row ordering by ticker/fiscal_year/fiscal_period, no timestamps in data files)
so SHA-256 checksums are reproducible for QA.

Usage:  python3 export_peer_ratios.py
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd

MODULE_ROOT = Path(__file__).resolve().parent
EXPORT_DIR = MODULE_ROOT / "exports" / "peer_financial_ratios_v1"

DB_DEFAULTS = {
    "host": os.environ.get("PGHOST", "localhost"),
    "port": int(os.environ.get("PGPORT", "5432")),
    "dbname": os.environ.get("PGDATABASE", "smb_warehouse"),
    "user": os.environ.get("PGUSER", "postgres"),
    "password": os.environ.get("PGPASSWORD", "postgres"),
}

FINANCIAL_COLUMNS = [
    "record_id", "cik", "ticker", "company_name", "hq_state", "is_socal",
    "vertical", "fiscal_year", "fiscal_period", "revenue", "gross_profit",
    "rd_expense", "sga_expense", "operating_income", "net_income",
    "operating_cash_flow", "gross_margin_pct", "operating_margin_pct",
    "rd_pct_of_rev", "sga_pct_of_rev", "source_url", "source_form",
    "accession_no", "source_tags", "derived_q4",
]

QUARTILE_COLUMNS = [
    "vertical", "fiscal_year", "socal_flag", "peer_group", "n_records",
    "gross_margin_p25", "gross_margin_median", "gross_margin_p75",
    "operating_margin_p25", "operating_margin_median", "operating_margin_p75",
    "rd_pct_of_rev_p25", "rd_pct_of_rev_median", "rd_pct_of_rev_p75",
    "sga_pct_of_rev_p25", "sga_pct_of_rev_median", "sga_pct_of_rev_p75",
]


def db_connect():
    import psycopg2
    return psycopg2.connect(
        "host={host} port={port} dbname={dbname} user={user} password={password}".format(
            **DB_DEFAULTS))


def refresh_mv(conn) -> None:
    # CONCURRENTLY cannot run inside a transaction block: autocommit must be
    # enabled BEFORE any statement opens a transaction on this connection.
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_vertical_financial_quartiles'")
            if cur.fetchone() is None:
                raise SystemExit("mv_vertical_financial_quartiles does not exist -- "
                                 "apply peer_ratios_schema.sql first")
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_vertical_financial_quartiles")
    finally:
        conn.autocommit = False


def load_frames(conn):
    financials = pd.read_sql(
        "SELECT {} FROM public_peer_financials "
        "ORDER BY ticker, fiscal_year, fiscal_period".format(
            ", ".join(FINANCIAL_COLUMNS)),
        conn,
    )
    quartiles = pd.read_sql(
        "SELECT {} FROM mv_vertical_financial_quartiles "
        "ORDER BY vertical, fiscal_year, socal_flag".format(
            ", ".join(QUARTILE_COLUMNS)),
        conn,
    )
    # normalize types for deterministic CSV/Parquet output
    for col in ("is_socal", "derived_q4"):
        financials[col] = financials[col].astype(bool)
    financials["record_id"] = financials["record_id"].astype("int64")
    financials["cik"] = financials["cik"].astype("int64")
    financials["fiscal_year"] = financials["fiscal_year"].astype("int64")
    quartiles["n_records"] = quartiles["n_records"].astype("int64")
    return financials, quartiles


def sha256_of_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_data_dictionary(path: Path, financials: pd.DataFrame,
                          quartiles: pd.DataFrame) -> None:
    tickers = (
        financials.drop_duplicates("ticker")
        [["ticker", "company_name", "vertical", "is_socal", "hq_state"]]
        .sort_values(["vertical", "ticker"])
    )
    ticker_lines = []
    for _, row in tickers.iterrows():
        ticker_lines.append("| {} | {} | {} | {} | {} |".format(
            row["ticker"], row["company_name"], row["vertical"],
            "SoCal" if row["is_socal"] else "National", row["hq_state"] or "?"))

    n_companies = financials["ticker"].nunique()
    n_socal = financials.drop_duplicates("ticker")["is_socal"].sum()
    fy_min, fy_max = int(financials["fiscal_year"].min()), int(financials["fiscal_year"].max())

    content = """# PEER_RATIOS_DATA_DICTIONARY
## Audited Finance & Tech Peer Financial Ratios Dataset — `peer_financial_ratios_v1`

**Source:** SEC EDGAR XBRL, company-concept API (`https://data.sec.gov/api/xbrl/companyconcept/CIK.../us-gaap/<Tag>.json`), harvested with a compliant User-Agent at <= 10 requests/second and cached locally (`data_cache/sec_xbrl/`).
**Scope:** {n_companies} public companies across five Finance & Tech verticals ({n_socal} headquartered in Southern California), fiscal years {fy_min}-{fy_max}, annual (FY) and quarterly (Q1-Q4) periods.
**Schema:** `peer_ratios_schema.sql` (table `public_peer_financials`, materialized view `mv_vertical_financial_quartiles`) in the local `smb_warehouse` PostgreSQL database.
**Generators:** `harvest_sec_peer_ratios.py` (harvest + load), `export_peer_ratios.py` (this export pack).
**Audience note:** This dataset is for peer benchmarking research. Values are as filed with the SEC; nothing here is investment advice.

---

## Files

| File | Rows | Description |
|---|---|---|
| `peer_financials.csv` / `.parquet` | {n_fin_rows} | One row per (company, fiscal_year, fiscal_period): as-filed USD statement lines + derived ratio percentages + EDGAR source links. |
| `vertical_quartiles.csv` / `.parquet` | {n_mv_rows} | 25th / median / 75th percentile of the four ratio metrics per (vertical, fiscal_year), split SoCal vs. National (`socal_flag` / `peer_group`). |
| `MANIFEST.json` | — | SHA-256 checksum, byte size and row count for every file in this pack. |

---

## Table `public_peer_financials` -> `peer_financials.csv`

| Column | Type | Description |
|---|---|---|
| record_id | int | Surrogate key (BIGSERIAL). Stable across re-runs (upsert on cik+fiscal_year+fiscal_period). |
| cik | int | SEC EDGAR Central Index Key. |
| ticker | string | Current EDGAR ticker (e.g. Block is `XYZ`, formerly `SQ`). |
| company_name | string | Registrant name from the EDGAR submissions API. |
| hq_state | string | Business-address state (or country for foreign filers, e.g. Accenture = `Ireland`) from EDGAR. |
| is_socal | bool | TRUE when the EDGAR business address is in a Southern California county (Imperial, Kern, Los Angeles, Orange, Riverside, San Bernardino, San Diego, Santa Barbara, Ventura). |
| vertical | enum | `Enterprise_SaaS` | `Fintech_Payments` | `Asset_Wealth_Mgmt` | `IT_Consulting_Services` | `Specialty_Finance`. |
| fiscal_year | int | Calendar year in which the fiscal year **ends** (SEC frame convention). A "FY2024" row for a January-FYE filer covers Feb 2023-Jan 2024. Note: a minority of January-FYE filers (e.g. ServiceTitan) label that same year one year lower in their own marketing convention. |
| fiscal_period | enum | `FY` (annual 10-K period), or `Q1`..`Q4` (~3-month fiscal quarters; see Q4 derivation below). |
| revenue | numeric | As-filed total revenue, USD (see tag fallback chain). |
| gross_profit | numeric | As-filed GrossProfit, USD, or computed = revenue - cost_of_revenue when the filer reports cost of revenue without a gross-profit subtotal (see audit trail). NULL when neither exists (e.g. most bank filers). |
| rd_expense | numeric | Research & development expense, USD (incl. TechnologyAndDevelopmentExpense fallback). |
| sga_expense | numeric | SG&A expense, USD. For bank filers without an SG&A tag this is **total NoninterestExpense** (the efficiency-ratio denominator) - see audit trail. |
| operating_income | numeric | OperatingIncomeLoss, USD. NULL for most bank/REIT filers (tag not filed). |
| net_income | numeric | NetIncomeLoss (attributable to parent), USD. |
| operating_cash_flow | numeric | Net cash provided by (used in) operating activities, USD. |
| gross_margin_pct | numeric | 100 x gross_profit / revenue. **Audited bound [-50, 100]**: out-of-bound values are NULLed and logged by the harvester. |
| operating_margin_pct | numeric | 100 x operating_income / revenue; NULLed when |value| > 1000 (data-quality guard). |
| rd_pct_of_rev | numeric | 100 x rd_expense / revenue; NULLed when |value| > 1000. |
| sga_pct_of_rev | numeric | 100 x sga_expense / revenue; NULLed when |value| > 1000. For bank filers this approximates 100 x (noninterest expense / total revenue). |
| source_url | string | EDGAR filing index page of the source filing: `https://www.sec.gov/Archives/edgar/data/<cik>/<accession-nodashes>/<accession>-index.htm`. 100% of rows carry a live link. |
| source_form | string | Form type of the source filing: 10-K, 10-K/A, 10-Q or 10-Q/A only. |
| accession_no | string | EDGAR accession number of the source filing. |
| source_tags | string | Per-row audit trail of the XBRL tags / fallback math used, e.g. `revenue=Revenues; gross_profit=Computed:revenue-cost_of_revenue; sga=SUM:SellingAndMarketingExpense+GeneralAndAdministrativeExpense; q4=Derived:FY-(Q1+Q2+Q3)`. |
| derived_q4 | bool | TRUE when the row is a derived fiscal Q4: FY - (Q1 + Q2 + Q3) per field (discrete fiscal Q4s are rarely XBRL-tagged). Sourced from the annual 10-K. |

---

## Materialized view `mv_vertical_financial_quartiles` -> `vertical_quartiles.csv`

Grouping: (vertical, fiscal_year, socal_flag). Percentiles use
`percentile_cont` (linear interpolation, NULLs ignored) inside the view
definition, so QA can re-derive every number with an identical query.

| Column | Description |
|---|---|
| vertical, fiscal_year | Peer vertical and fiscal year (end-year convention). |
| socal_flag | TRUE = SoCal peer group, FALSE = National peer group (direct comparison within the same vertical-year). |
| peer_group | Human-readable label: `SoCal` / `National`. |
| n_records | Rows behind the group (public_peer_financials). |
| gross_margin_p25 / _median / _p75 | 25th / 50th / 75th percentile of gross_margin_pct. |
| operating_margin_p25 / _median / _p75 | Same for operating_margin_pct. |
| rd_pct_of_rev_p25 / _median / _p75 | Same for rd_pct_of_rev. |
| sga_pct_of_rev_p25 / _median / _p75 | Same for sga_pct_of_rev. |

Percentiles are NULL when no row in the group has the metric (e.g. banks have
NULL gross profit, so their gross-margin percentiles are NULL).

---

## Methodology

### 1. XBRL tag fallback chains (priority order; lower wins for the same period)
- **revenue**: `Revenues` > (bank fallback, synthesized: `InterestIncomeExpenseNet` + `NoninterestIncome`) > `RevenueFromContractWithCustomerExcludingAssessedTax` > `RevenueFromContractWithCustomerIncludingAssessedTax` > `SalesRevenueNet` > `TotalRevenue` > `RevenuesNetOfInterestExpense` > `TotalInvestmentIncome` (BDC/REIT).
  - The bank fallback exists because bank filers tag net interest income and
    noninterest income but no total; using the contract-revenue tag alone would
    capture only fee income and understate revenue.
- **cost_of_revenue**: `CostOfRevenue` > `CostOfGoodsAndServicesSold` > `CostOfGoodsSold` > `CostOfServices` > `CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization`.
- **gross_profit**: `GrossProfit`, else **computed = revenue - cost_of_revenue** (filers that report cost of revenue without an explicit gross-profit subtotal).
- **rd_expense**: `ResearchAndDevelopmentExpense` > `ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost` > `TechnologyAndDevelopmentExpense`.
- **sga_expense**: `SellingGeneralAndAdministrativeExpense` > `NoninterestExpense` (bank filers) > **computed = `SellingAndMarketingExpense` + `GeneralAndAdministrativeExpense`**.
- **operating_income**: `OperatingIncomeLoss`. **net_income**: `NetIncomeLoss`.
- **operating_cash_flow**: `NetCashProvidedByUsedInOperatingActivities` > `NetCashProvidedByUsedInOperatingActivitiesContinuingOperations`.

### 2. Restatement / amendment deduplication
Only `10-K`, `10-K/A`, `10-Q`, `10-Q/A` filings are considered (no 8-K/S-1/424B
values). For each (period, tag), the kept value is chosen by: primary tag >
fallback tag; then amended form (10-K/A beats 10-K) > original; then latest
filed date; then highest accession number. Comparative restatements published
in later filings therefore override the original numbers.

### 3. Period classification and fiscal labeling
- Durations of 330-380 days = annual; 80-100 days = quarterly; 6-/9-month YTD
  durations are excluded.
- Each quarter is assigned to the annual (10-K) period that contains it; the
  quarter index is the day-proportional position within that annual period
  (robust for 13-week / 4-4-5 calendars, e.g. Adobe quarters ending ~Mar 1).
- `fiscal_year` = calendar year of the annual period END date.

### 4. Derived fiscal Q4
Filers do not tag discrete fiscal Q4s. When no discrete Q4 fact exists, each
field is derived as **Q4 = FY - (Q1 + Q2 + Q3)** (from the deduplicated,
restatement-consistent values) and flagged `derived_q4 = TRUE`, with the
annual 10-K as source_url. Rows are emitted only if the derived revenue is
positive; otherwise the Q4 row is skipped and logged.

### 5. Audited realism bounds (QA)
- `gross_margin_pct` enforced to **[-50%, 100%]** both in the harvester (NULL +
  warning) and via a CHECK constraint in `peer_ratios_schema.sql`.
- Other ratios beyond +/-1000% are NULLed as data-quality anomalies.
- Rows without any revenue value are dropped (revenue anchors three of the
  four ratios); the harvest report logs every such exclusion.

### 6. SoCal classification
`is_socal` is derived from the EDGAR **business address** (city + state):
TRUE when state = CA and the city lies in one of the nine Southern California
counties (Imperial, Kern, Los Angeles, Orange, Riverside, San Bernardino, San
Diego, Santa Barbara, Ventura). Companies whose registrant address moved out
of the region (e.g. Green Dot: Pasadena, CA -> Austin, TX) are classified by
their current EDGAR address.

---

## Company-to-vertical mapping ({n_companies} companies)

| Ticker | Company | Vertical | Peer group | EDGAR HQ |
|---|---|---|---|---|
{ticker_table}

### Vertical definitions
- **Enterprise_SaaS**: subscription/subscription-like software platforms (incl. ad-tech platform TTD and banking-software vendor MLNK).
- **Fintech_Payments**: digital payments, consumer fintech and banking-infrastructure providers.
- **Asset_Wealth_Mgmt**: asset managers, wealth platforms and financial advisory firms (incl. investment-banking advisory HLI).
- **IT_Consulting_Services**: IT services, consulting and professional-services firms.
- **Specialty_Finance**: non-bank lenders, mortgage originators/servicers, BDCs, and regional/community banks (EWBC, CATY, BANC, HOPE, CVBF, HAFC) grouped here for peer benchmarking.

---

## Caveats for analysts
1. Banks and BDCs have NULL gross_profit / operating_income (their XBRL has no
   such subtotals); their `sga_pct_of_rev` is the noninterest-expense ratio.
2. `fiscal_period = Q4` rows are derived (see above); `FY` rows are as filed.
3. Newer public companies (e.g. TTAN, a January-FYE filer that IPO'd Dec 2024)
   have fewer fiscal years. Companies delisted during 2025 (MLNK, GHLD, COOP)
   contribute FY2021-FY2024 only. ARCC (BDC) revenue is GrossInvestmentIncomeOperating.
4. BLK rows use BlackRock's successor registrant CIK (2012383); its XBRL history
   starts with the reorganized entity, so earlier quarters are absent.
5. Percentages are stored as percent units (50.0 = 50%), rounded to 4 decimals.
6. To refresh after re-harvesting:
   `python3 harvest_sec_peer_ratios.py && python3 export_peer_ratios.py`.

*Generated {generated_at} — harvest log: warnings and exclusions are printed by `harvest_sec_peer_ratios.py`.*
""".format(
        n_companies=n_companies,
        n_socal=int(n_socal),
        fy_min=fy_min, fy_max=fy_max,
        n_fin_rows=len(financials),
        n_mv_rows=len(quartiles),
        ticker_table="\n".join(ticker_lines),
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    )
    path.write_text(content, encoding="utf-8")


def write_manifest(path: Path, entries: List[Dict], financials: pd.DataFrame,
                   quartiles: pd.DataFrame) -> None:
    manifest = {
        "dataset": "peer_financial_ratios_v1",
        "description": ("Audited peer financial ratios harvested from SEC EDGAR "
                        "XBRL (company-concept API) for 50 Finance/Fintech/SaaS/"
                        "IT-services companies incl. Southern California public "
                        "companies, FY2021-FY2025, annual + quarterly periods."),
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "https://data.sec.gov/api/xbrl/companyconcept/",
        "sec_compliance": {"user_agent": "PeerRatiosResearch/1.0", "max_requests_per_second": 10,
                           "local_cache": "data_cache/sec_xbrl/"},
        "schema": "peer_ratios_schema.sql",
        "generator_scripts": ["harvest_sec_peer_ratios.py", "export_peer_ratios.py"],
        "row_counts": {
            "public_peer_financials": int(len(financials)),
            "mv_vertical_financial_quartiles": int(len(quartiles)),
            "companies": int(financials["ticker"].nunique()),
            "socal_companies": int(financials.drop_duplicates("ticker")["is_socal"].sum()),
        },
        "qa": {
            "gross_margin_pct_bound": [-50.0, 100.0],
            "rows_with_revenue_null": int(financials["revenue"].isna().sum()),
            "rows_with_source_url": int(financials["source_url"].str.startswith(
                "https://www.sec.gov/Archives/edgar/data/").sum()),
            "dedup_rule": "latest amended/annual 10-K/10-Q value per (period, tag)",
        },
        "files": entries,
    }
    path.write_text(json.dumps(manifest, indent=2, sort_keys=False) + "\n",
                    encoding="utf-8")


def main() -> int:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    conn = db_connect()
    try:
        refresh_mv(conn)
        financials, quartiles = load_frames(conn)
    finally:
        conn.close()

    if financials.empty:
        raise SystemExit("public_peer_financials is empty -- run harvest_sec_peer_ratios.py first")

    outputs = []

    fin_csv = EXPORT_DIR / "peer_financials.csv"
    financials.to_csv(fin_csv, index=False)
    outputs.append((fin_csv, len(financials), "company-period financial facts + ratios (CSV)"))

    fin_parquet = EXPORT_DIR / "peer_financials.parquet"
    financials.to_parquet(fin_parquet, index=False, engine="pyarrow")
    outputs.append((fin_parquet, len(financials), "same as peer_financials.csv (Parquet)"))

    q_csv = EXPORT_DIR / "vertical_quartiles.csv"
    quartiles.to_csv(q_csv, index=False)
    outputs.append((q_csv, len(quartiles), "vertical-year SoCal/National ratio quartiles (CSV)"))

    q_parquet = EXPORT_DIR / "vertical_quartiles.parquet"
    quartiles.to_parquet(q_parquet, index=False, engine="pyarrow")
    outputs.append((q_parquet, len(quartiles), "same as vertical_quartiles.csv (Parquet)"))

    dictionary = EXPORT_DIR / "PEER_RATIOS_DATA_DICTIONARY.md"
    write_data_dictionary(dictionary, financials, quartiles)

    entries = []
    for path, rows, desc in outputs:
        entries.append({
            "name": path.name,
            "sha256": sha256_of_file(path),
            "bytes": path.stat().st_size,
            "rows": rows,
            "description": desc,
        })
    entries.append({
        "name": dictionary.name,
        "sha256": sha256_of_file(dictionary),
        "bytes": dictionary.stat().st_size,
        "rows": None,
        "description": "column documentation + full harvest/export methodology",
    })

    manifest_path = EXPORT_DIR / "MANIFEST.json"
    write_manifest(manifest_path, entries, financials, quartiles)

    print("exported {} financial rows ({} companies, {} SoCal) and {} quartile rows".format(
        len(financials), financials["ticker"].nunique(),
        financials.drop_duplicates("ticker")["is_socal"].sum(), len(quartiles)))
    for e in entries:
        print("  {:<32} {:>10} bytes  sha256={}".format(
            e["name"], e["bytes"], e["sha256"][:16] + "..."))
    print("export dir: {}".format(EXPORT_DIR))
    return 0


if __name__ == "__main__":
    sys.exit(main())
