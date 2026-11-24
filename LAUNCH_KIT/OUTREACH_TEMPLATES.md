# OUTREACH_TEMPLATES.md — GLM-5.3 SMB Financial Intelligence Suite

Working-copy rules for anyone using this file:

- Every `[BRACKETED]` token is a placeholder. Fill it or delete the line.
- All numbers (775 tests, ≈57,539 rows, >20% labor overrun, z>2, >5 pp, >184h, 90-day runway, 0/2/3 exit codes) are verified product facts. Do not embellish, round up, or add stats, testimonials, customers, or URLs that are not already here. Link placeholder: `[REPO_URL]`.
- Never promise outcomes you cannot verify. Describe what the suite produces, not what it guarantees.

---

## 1. Cold email — SoCal fractional CFOs, bookkeepers, service-business owners (Ventura / LA / Orange County)

**Subject line options (pick one, keep it accurate):**

1. `A profitability & labor-leak audit on the exports you already have`
2. `Your QuickBooks exports, bank PDFs, payroll — one local read`
3. `[TRADE] shops in [CITY]: where is labor leaking?`

**Body (~136 words, under the 150-word cap) — send exactly ONE variant; the variant is one line:**

```text
Hi [FIRST_NAME],

I build a local financial intelligence suite for SoCal service businesses, and I'm [PILOT VARIANT LINE] profitability & labor-leak audit.

You send files you already have — QuickBooks exports, bank statement PDFs, a payroll summary. Everything is processed locally, never uploaded, and deleted whenever you say.

You get back:
- A short profitability read by job and customer
- Top labor leaks — overtime-heavy roles and overruns vs. estimates
- Anomalies worth a second look

If that's useful, reply "audit" and I'll send the one-page list of exactly what to export. If not, no hard feelings and nothing more from me.

—[YOUR_NAME], [ROLE], [CITY]

PS — I also maintain free public benchmark datasets for SoCal service businesses: ~57.5k rows of real Census, BLS, FRED, SBA and SEC data.
They're public — yours whether or not we ever work together.
```

**Variant table — only ONE line changes:**

| Variant marker | Replace `[PILOT VARIANT LINE]` with | Implied ask |
|---|---|---|
| **[FREE PILOT VARIANT]** | `offering a free` | No charge; ask for honest feedback and a referral only if they found it useful |
| **[$500 PILOT VARIANT]** | `running a $500-flat` | $500 flat fee; same deliverables as listed above |

Notes:

- Single CTA only ("audit" reply). Do not add a second CTA, calendar link, or attachment to the first email.
- Plain-professional tone; no fake urgency, no guilt lines, no unproven claims ("cut costs 30%" style promises are banned).
- Signature block must carry a real identity and a physical postal address (see Section 5).

---

## 2. LinkedIn DM (≤90 words; this version is 85)

```text
Hi [FIRST_NAME] — saw you're running finance for [COMPANY TYPE] shops around [AREA]. Quick question: when you need job-level profitability or overtime answers, how long does it take to pull numbers together from QuickBooks, statements, and payroll?

I ask because I built a local tool that turns those exact PDFs into board-ready profitability and labor-leak reads — all offline, nothing uploaded.

Happy to show it in 15 minutes, or just send you two sample PDFs from the demo workspace. No pitch unless you ask for one.
```

Personalize `[COMPANY TYPE]` and `[AREA]` to something visible on their profile or site. If you cannot personalize two fields, skip the send.

---

## 3. Follow-up cadence (3 touches, then stop permanently)

| Touch | Timing | Channel | Exact text |
|---|---|---|---|
| 1 | Day 0 | Email or DM | Template from Section 1 or 2 |
| 2 | Day 4 | Reply in the same thread (one line) | `Floating this back up — a one-word reply closes the loop: "audit" and I'll send the export list, "pass" and this is my last note.` |
| 3 | Day 10 | Email (value-add close) | `Last one from me. Offer: I'll pull the BLS OEWS wage bands for [TRADE] in [COUNTY] from the public dataset and show where your payroll sits against the local band — free, about five minutes, no data needed from you. Want it? One word back and I'll send it. Either way, the benchmark datasets are public at [REPO_URL], and I won't email again.` |

Cadence rules:

- Three touches maximum, then silence forever. Any opt-out ("pass", "stop", "unsubscribe") ends the sequence immediately, even on touch 1.
- Touch 3's benchmark insight offer is a real, verifiable offer (BLS OEWS data is in the free public datasets). Do not invent a specific insight stat before pulling the data.
- Never re-open a closed thread to "just check in".

---

## 4. Launch announcements (ready to paste)

Common rules for all posts: lead with or include the disclosure line **"I built this; feedback welcome."** Link `[REPO_URL]` directly — no URL shorteners. Re-read each venue's current self-promotion rules the day you post.

### 4.1 Show HN

**Title (≤80 chars):**

```text
Show HN: Local-first financial intelligence suite for SMBs (PDFs → Postgres)
```

**Text post:**

```markdown
Hi HN — I built a 3-part, local-first financial intelligence suite for small service businesses: [REPO_URL]

**What it is**

1. **Doc-to-Warehouse** (Streamlit, :8603): drop in messy bank statements, vendor invoices, and weekly payroll PDFs. A fully offline rule-based extractor parses them with running-balance cross-checks, scores every row 0–1 confidence, flags DUP/REVIEW, categorizes into a 6-label vocabulary (Revenue, Direct_Materials, Subcontractor, Overhead, Marketing, Payroll), normalizes vendors, and pushes cleaned rows into Postgres after a human review grid.

2. **Data Warehouse** (:8602): a 10-tab Streamlit app on top of the warehouse — job/customer profitability, anomaly detection (labor overruns >20% over estimate, expense spikes z>2), seasonal_trend_v1 forecasting with walk-forward backtests, SoCal market benchmarks, natural-language-to-SQL, and a one-click Executive PDF Board Brief.

3. **SMB-Watchdog**: a headless read-only CLI for cron/launchd. Four checks per client (open critical/warning anomalies; trailing-30-day margin vs. benchmark, flagged at a >5 pp gap; top-3 overtime-heavy employees >184h and unprofitable whale clients; 90-day cash runway), writes digest_report.md/.html, exits 0/2/3 so automation can branch on it.

**Why I built it:** the numbers a fractional CFO needs are usually trapped in PDFs and a handful of disconnected tools, and owners reasonably don't want financial documents shipped to a hosted API.

**Architecture notes**

- The extraction path is 100% local: no API key, no cloud in the pipeline. There is an optional LLM fallback in the extractor; it's disabled by default and never triggers on the sample documents.
- Multi-tenancy is real row-level security in PostgreSQL 17.5: 9 tenant_isolation_* RLS policies, fail-closed. Tenant scope rides on session-level GUCs set per transaction, so a forgotten app-layer filter can't leak another tenant's rows — the database refuses first.

**Honest limitations:** the parsers are deterministic, not magic. Unfamiliar statement layouts fall back to REVIEW flags for the human grid rather than guessing, and confidence scores are heuristics, not ground truth. The optional LLM fallback exists because some layouts genuinely need it — treat "offline-only" as true of the default configuration.

Also bundled: ≈57,539 rows of real public SoCal/SEC data (Census CBP/ZBP, BLS OEWS, FRED, SBA FOIA, SEC Form D + SBIR/STTR, SEC EDGAR XBRL for 50 peers) plus a synthetic demo SMB pack, so the whole suite runs without your own data. 775 tests passing. Repo: [REPO_URL]

Explicitly asking for feedback on: parser design (deterministic-first vs. LLM-first), the RLS policy set, the watchdog exit-code contract (0/2/3), and whether the Executive PDF Board Brief is genuinely useful or a gimmick.
```

### 4.2 Reddit

One post per subreddit, each with the subreddit-specific angle below. Post from your real account, include the disclosure line, no shorteners.

#### r/dataengineering (technical angle)

**Title:**

```text
I built an offline PDF→Postgres pipeline: deterministic parsers, per-row confidence, RLS-multi-tenant warehouse — notes inside
```

**Body:**

```markdown
I built this; feedback welcome. Repo: [REPO_URL] (direct link, no shorteners)

Problem: SMB financial docs (bank statements, vendor invoices, weekly payroll) arrive as messy PDFs, and owners don't want them shipped to a hosted extraction API.

What I landed on:

- Deterministic rule-based parsers with running-balance cross-checks: the statement's own arithmetic has to reconcile; mismatches flag REVIEW instead of guessing. Every extracted row gets a 0–1 confidence score, and DUP/REVIEW flags route rows through a human review grid before anything reaches the warehouse.
- Categorization is a fixed 6-word vocabulary (Revenue, Direct_Materials, Subcontractor, Overhead, Marketing, Payroll) — a small label space keeps the review grid fast and the rollups stable.
- Rows land in PostgreSQL 17.5 (smb_warehouse) behind 9 tenant_isolation_* RLS policies, fail-closed. Tenant scope rides on session-level GUCs set per transaction (SET LOCAL), so a web-app pool that forgets an app-layer filter still can't cross tenants — the database refuses first.
- Downstream reads are matviews refreshed on push (profitability rollups, anomaly candidates), so the Streamlit layer (:8602, 10 tabs) doesn't recompute on every page load.
- A headless watchdog CLI runs the same checks unattended with exit codes 0/2/3, so cron/launchd can branch without parsing output.
- 775 pytest tests passing.

The trade-off I'd like opinions on: deterministic parsers are predictable but brittle on unfamiliar layouts, so I fall back to human REVIEW instead of guessing. There's an optional LLM fallback for genuinely odd layouts — disabled by default, never triggered by the sample docs. Would you keep it at all, or rip it out for pure auditability?

Happy to share the RLS policy set or parser test corpus if useful.
```

#### r/smallbusiness (plain-English angle)

**Title:**

```text
I built an open-source local tool that turns your bank statement and payroll PDFs into job profitability numbers — nothing gets uploaded
```

**Body:**

```markdown
Disclosure: I built this; feedback welcome. It's open source: [REPO_URL]

The short version for business owners:

- You drop in PDFs you already have: bank statements, vendor invoices, weekly payroll summaries.
- It reads them offline, on your own computer — no cloud, no API keys, nothing uploaded. It checks each statement's own math, sorts every line into 6 buckets (Revenue, Direct_Materials, Subcontractor, Overhead, Marketing, Payroll), and flags anything suspicious instead of guessing.
- You confirm the flagged rows in a simple review grid, click push once, and get dashboards: which jobs and customers actually make money, which are running more than 20% over labor estimates, where overtime is concentrated, and a 90-day cash runway check.
- One click produces an "Executive PDF Board Brief" if you take numbers to a bank or lender, and a scheduled background check writes a daily digest file (digest_report.md / .html) with anything critical.

Privacy, stated plainly: the core pipeline is 100% local — your financial documents never leave your machine. An optional AI fallback exists in the code but is off by default and isn't needed for the sample documents.

It ships with ~57.5k rows of real public SoCal benchmark data (Census, BLS wage data, FRED, SBA, SEC) plus a synthetic demo company, so you can run the whole thing before touching your own files.

Genuinely looking for feedback from anyone running a service business: what's missing from the daily digest? What would make the board PDF actually useful to your lender?
```

#### r/analytics (decision-focused angle)

**Title:**

```text
Decision-focused SMB analytics: 30-day margin vs. benchmark, overtime concentration, 90-day runway, board-ready PDF output
```

**Body:**

```markdown
Disclosure: I built this; feedback welcome. Repo: [REPO_URL]

I built the analytics layer around the four questions a fractional CFO actually asks, rather than pretty-but-unreadable dashboards:

1. Margin drift: trailing-30-day margin vs. SoCal benchmark, flagged when the gap exceeds 5 percentage points.
2. Labor overruns: jobs running >20% over labor estimate, plus expense spikes at z>2.
3. Overtime concentration: top-3 overtime-heavy employees (>184h) and unprofitable whale clients.
4. Runway: 90-day cash runway.

All four run two ways: headless (SMB-Watchdog CLI, exit codes 0/2/3 for automation, writes digest_report.md/.html) and interactive (10-tab app: profitability, anomalies, seasonal_trend_v1 forecasting with walk-forward backtests, SoCal benchmarks, NL-to-SQL, one-click Executive PDF Board Brief).

Forecasting note: seasonal_trend_v1 is evaluated walk-forward, not single-split — I'd appreciate a critique from anyone doing SMB-level forecasting on whether that's the right rigor/complexity trade-off.

Benchmarks come from ≈57.5k rows of real public SoCal/SEC data (Census CBP/ZBP, BLS OEWS, FRED, SBA FOIA, SEC Form D + SBIR/STTR, SEC EDGAR XBRL). The extraction pipeline never touches the network.

What decision checks would you add for a 10–50 person service business?
```

### 4.3 LinkedIn post (first-person founder voice, ≤1300 chars)

```text
I'm releasing GLM-5.3: a local-first financial intelligence suite for SoCal service businesses.

The numbers that matter are trapped in PDFs — statements, invoices, payroll — scattered across disconnected tools. So I built what I wished existed:

1) Doc-to-Warehouse: drop in messy PDFs; a fully offline rule-based extractor checks the math, scores each row 0–1 confidence, flags suspicious rows for human review, and pushes clean data into your own Postgres. No cloud, no API keys — documents never leave your machine.

2) Data Warehouse: 10 dashboards — job/customer profitability, anomaly alerts, SoCal benchmarks, forecasting, and a one-click Executive PDF Board Brief.

3) SMB-Watchdog: a scheduled read-only check writing a daily digest — margin drift, overtime concentration, 90-day cash runway.

Honest note: an optional AI fallback exists — disabled by default, never needed for the sample docs. I'd rather be exact about privacy than impressive.

Also published: ~57.5k rows of real public SoCal/SEC benchmark data — run the suite before touching your own files.

Repo: [REPO_URL] — 775 passing tests, real row-level security for multi-client work.

Feedback welcome, especially from bookkeepers and fractional CFOs.

#FractionalCFO #SmallBusiness #DataEngineering #FinancialAnalytics
```

### 4.4 X/Twitter

**Single post (≤280 chars):**

```text
GLM-5.3 is live: local-first financial intelligence for SMBs.

Bank/invoice/payroll PDFs → offline rule-based extraction → your own Postgres → profitability dashboards → scheduled watchdog digest.

100% local pipeline, no API keys, free public SoCal/SEC benchmarks.

[REPO_URL]
```

**Optional 3-tweet thread:**

```text
1/3 — GLM-5.3: local-first financial intelligence for SMBs.

Part 1, Doc-to-Warehouse: bank/invoice/payroll PDFs → offline rule-based extraction → 0–1 confidence → DUP/REVIEW flags → clean push into your own Postgres behind 9 fail-closed RLS policies. No API key, no cloud.
```

```text
2/3 — Part 2, Data Warehouse: 10 dashboards on your own Postgres. Job/customer profitability, anomaly alerts (labor >20% over estimate, expense spikes z>2), SoCal benchmarks, seasonal_trend_v1 forecasts with walk-forward backtests, one-click Executive PDF Board Brief.
```

```text
3/3 — Part 3, SMB-Watchdog: read-only CLI for cron. 4 checks per client: open anomalies, 30-day margin vs. benchmark, top-3 overtime-heavy employees + whale clients, 90-day cash runway. Writes digest_report.md/.html, exits 0/2/3. Free public SoCal/SEC benchmarks: [REPO_URL]
```

---

## 5. Compliance & etiquette notes

- **Community rules first.** Read each subreddit's current self-promotion rules before posting; some require prior participation or restrict launch posts. Show HN is the right venue for a launch. Always include the disclosure line ("I built this; feedback welcome") and never use URL shorteners.
- **Email lists.** Never email purchased, rented, or scraped lists. Only contact people you can personalize to — real name, real role, a genuine reason you're reaching them. One CTA per message.
- **CAN-SPAM basics (US commercial email).** Accurate subject lines; your real identity and business; a physical postal address in the footer; honor opt-outs promptly — a "pass"/"unsubscribe"/silence after the final touch ends contact permanently.
- **Claims discipline.** Describe outputs (profitability read, anomaly list, digest, board PDF), never guaranteed outcomes ("find $10k in leaks", "cut overtime 25%"). Confidence scores are heuristics — say so if asked. Represent privacy as "100% local in the default configuration": the optional LLM fallback exists but is disabled by default and never triggered by the sample docs.
- **Cadence discipline.** Stick to the 3-touch plan; no fake urgency, no guilt lines, no follow-ups past day 10.
