# SMB-Watchdog

Headless **Executive Watchdog & Morning Digest CLI**. Connects READ-ONLY to an
existing PostgreSQL SMB data warehouse, runs four executive risk checks per
company, prints a color-coded terminal summary, and writes stable-filename
digest reports (`reports/digest_report.md`, `reports/digest_report.html`) that
cron consumers pick up after every run. Single file, Python 3.9, stdlib +
`psycopg2-binary` only (no other third-party dependencies).

## One-line setup guide

```bash
python3 watchdog.py    # (1) RUN ON DEMAND: python3 watchdog.py ; (2) CRONTAB: add "15 7 * * * cd /Users/Owner/Desktop/GLM-5.3/SMB-Watchdog && /usr/bin/python3 watchdog.py --quiet" via crontab -e ; (3) LAUNCHD: save the plist sketch below to ~/Library/LaunchAgents/com.smb-watchdog.plist and run launchctl load ~/Library/LaunchAgents/com.smb-watchdog.plist
```

launchd plist sketch (place at `~/Library/LaunchAgents/com.smb-watchdog.plist`,
then `launchctl load ~/Library/LaunchAgents/com.smb-watchdog.plist`;
unload with `launchctl unload`):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.smb-watchdog</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/bin/python3</string>
    <string>/Users/Owner/Desktop/GLM-5.3/SMB-Watchdog/watchdog.py</string>
    <string>--quiet</string>
  </array>
  <key>StartCalendarInterval</key><dict><key>Hour</key><integer>7</integer><key>Minute</key><integer>15</integer></dict>
  <key>StandardOutPath</key><string>/Users/Owner/Desktop/GLM-5.3/SMB-Watchdog/reports/watchdog_last_run.log</string>
  <key>StandardErrorPath</key><string>/Users/Owner/Desktop/GLM-5.3/SMB-Watchdog/reports/watchdog_last_run.err</string>
</dict>
</plist>
```

## The four checks (run per company, looping over ALL rows of `companies`)

| # | Check | Source | Escalation |
|---|-------|--------|------------|
| (a) | Open critical/warning anomaly alerts | `anomaly_alerts` (`is_resolved=false`, severity `high`/`medium`), joined to `jobs` for labor overruns, `details` JSONB parsed for context | any `high` -> CRITICAL; `medium` -> WARNING |
| (b) | Trailing 30-day gross margin vs target, by job type | `financial_ledger` -> `jobs` -> `job_types` (rolling 30 days; overhead entries excluded); target = latest national `job_market` row in `market_benchmarks` | margin more than **5 percentage points** below target -> WARNING flag |
| (c) | Overtime-heavy employees & whale clients | `labor_logs`/`employees` top-3 by trailing-30d hours; `mv_customer_profitability` whales | hours > **184 h** (1.15 x 160) or whale `gross_margin < 0` -> WARNING |
| (d) | 90-day cash runway | ledger cash proxy, `mv_monthly_summary` burn basis, `forecasts` context | runway < 30 d -> CRITICAL; 30-90 d -> WARNING; > 90 d or net-positive -> HEALTHY |

A failing query for one check never aborts the run: the check is recorded as an
`ERROR` row in the digest and the scan continues.

## CLI flags

| Flag | Meaning |
|------|---------|
| `--dsn URL` | PostgreSQL connection string. Default: `postgresql://postgres:postgres@localhost:5432/smb_warehouse`, overridable via env `SMB_WATCHDOG_DSN` (env beats built-in default; `--dsn` beats env). |
| `--output-dir DIR` | Where `digest_report.md` / `digest_report.html` are written. Default: `<script dir>/reports` (stable regardless of the invoking cwd). |
| `--max-alerts-per-table N` | Row cap per digest table; extra rows summarized in a note (default 10). |
| `--no-html` | Skip the HTML report. |
| `--no-md` | Skip the Markdown report. |
| `--quiet` | Suppress all terminal output; reports are still written (cron friendly). Errors still go to stderr. |
| `--version` | Print version. |

ANSI colors are used **only** when stdout is a TTY, so redirected/cron output
stays clean.

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | Healthy — no critical alerts across scanned companies (warnings allowed). |
| `2` | At least one open critical (`severity='high'`) anomaly alert exists across scanned companies. Per spec, exit 2 is keyed to open severity=`high` anomaly alerts; other CRITICAL check statuses (e.g. runway < 30 days) are displayed prominently but do not change the exit code. |
| `3` | Operational error — warehouse unreachable, companies registry unreadable, report write failure, or every alert check failing (query catastrophe). |

## Read-only posture

Every connection runs `SET default_transaction_read_only = on` immediately
after connect and the setting is re-verified via
`current_setting('default_transaction_read_only')` before any scan. The tool
issues **SELECT statements only** — no INSERT/UPDATE/DELETE, no DDL, no
functions that write.

## Interpreted semantics (also embedded in every report's footnote)

- "Dropped >5% below target" (check b) = actual margin < target − **5.0
  percentage points** (not a 5% relative decline).
- Margin window (check b) = rolling `entry_date >= CURRENT_DATE - 30`; types
  with zero revenue in the window are not evaluated; overhead ledger entries
  without job attribution are excluded from per-type margins.
- Overtime (check c) = trailing-30-day hours > **184 h** (1.15 × 160 h
  full-time month); top 3 employees listed regardless.
- Whales (check c) = `is_whale = true` rows in `mv_customer_profitability`
  (lifetime aggregates); unprofitable = `gross_margin < 0`; if no whale rows
  exist, the 3 lowest-margin customers with revenue > 0 are shown as
  informational context.
- Cash-on-hand proxy (check d) = lifetime `SUM(revenue) - SUM(expense)` from
  `financial_ledger` — the warehouse has no bank/balance table, so runway is a
  proxy, not a bank reconciliation.
- Burn basis (check d) = average monthly net cash flow over the 3 most recent
  months present in `mv_monthly_summary`; if that average is >= 0 the check
  reports "positive cash flow — no burn" and no runway figure is computed.
- Forecasts (check d) = `net_cash_flow` rows with `period_start` in (current
  month, today + 90 days]; latest `run_date` wins per period; context only.
- Severity mapping: `high` -> CRITICAL, `medium` -> WARNING, `low` counted as
  context but not listed.

## Requirements

- Python 3.9+
- `psycopg2-binary` (import name `psycopg2`)
- Network reachability to the warehouse DSN
