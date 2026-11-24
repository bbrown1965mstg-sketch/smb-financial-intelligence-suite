# DEMO_SCRIPT.md — GLM-5.3 SMB Financial Intelligence Suite (3-minute screen recording)

Runtime **3:00** · VO pace ~150 wpm (~438 words total) · Record at **1920×1080** (or higher), 30/60 fps · All `[BRACKETS]` are placeholders · Link placeholder: `[REPO_URL]`.

Golden rules while recording:

- Say **"demo workspace"** whenever referring to the data. No customer names, no invented metrics on screen — the only numbers visible are the app's own (confidence scores, DUP/REVIEW flags, thresholds, exit codes).
- Use the exact on-screen UI strings quoted below; if the UI string differs on the day, match what's on screen and update this script.
- Don't claim outcomes ("saves $X") — show artifacts (dashboards, Board Brief PDF, digest file).

---

## PRE-FLIGHT CHECKLIST

- [ ] Run `./launch_suite.sh` from the repo root; confirm both Streamlit apps are up (Doc-to-Warehouse on `:8603`, Data Warehouse on `:8602`) and Postgres `smb_warehouse` is reachable on `localhost:5432`.
- [ ] Sample PDFs staged, or confirmed that **"Load Sample Messy PDFs"** loads in `:8603` and **"Extract from uploaded PDFs"** completes cleanly.
- [ ] Rehearse the two state-changing clicks (push, Board Brief) at least once before recording; know where the generated PDF opens.
- [ ] Warehouse in a clean demo state — if reusing a pushed batch, verify idempotency beforehand so the re-push shows zero new rows, not duplicates.
- [ ] Terminal font size bumped up (≥18pt or zoomed) for the watchdog segment; pre-type `python3 watchdog.py`.
- [ ] `reports/` directory clean of previous runs, so the new `digest_report.md` / `.html` is the only output shown.
- [ ] Browser zoom ~125–150%; full-screen the window; hide the bookmarks bar.
- [ ] Close personal windows/tabs; enable Do Not Disturb (no notification pop-ups mid-take).
- [ ] Screen recorder set to 1080p+, mic level checked; record a 5-second test clip and check audio + framing.
- [ ] End-card asset ready: "100% local · free SoCal/SEC benchmark datasets · `[REPO_URL]`".

---

## BEATS TABLE

| # | Time | Screen | Action | VO words (~150 wpm) |
|---|---|---|---|---|
| 1 | 0:00–0:20 | Browser — Doc-to-Warehouse landing | Show the empty upload state; brief 4-tool montage only if pre-cut B-roll exists | 51 |
| 2 | 0:20–1:10 | Doc-to-Warehouse `:8603` | Click **"Load Sample Messy PDFs"** → **"Extract from uploaded PDFs"** → show confidence scores + DUP/REVIEW flags → fix one row inline in the review grid → pick client workspace → click **"Push N transactions to warehouse"** → click **"Open the Data Warehouse workspace →"** | 119 |
| 3 | 1:10–2:10 | Data Warehouse `:8602` | Select demo workspace → profitability charts → anomaly alerts → SoCal benchmark tab (brief) → click **Executive PDF Board Brief** → hold on the generated PDF (wow beat) | 149 |
| 4 | 2:10–2:45 | Terminal (SMB-Watchdog) | Run `python3 watchdog.py` → show color-coded summary + `digest_report.md` path; overlay cron one-liner | 83 |
| 5 | 2:45–3:00 | End card | Show repo URL + datasets note | 36 |

Total ≈ 438 VO words ≈ 2:55 of speech — leaves ~5s of silent holds (Board Brief, digest output) for the viewer to read.

---

## FULL SCRIPT (VO + actions)

### Beat 1 — Hook · 0:00–0:20 · 51 words

**On screen:** Doc-to-Warehouse empty upload state. (Optional: 1–2s montage of a bank PDF, a dashboard, the Board Brief, a terminal.)

**VO:**
> Every month, the numbers that decide whether a service business makes money are trapped in PDFs — bank statements, vendor invoices, payroll summaries — plus four disconnected tools. This suite reads all of it, fully locally. No cloud, no API keys. Let me show you, end to end, in three minutes.

### Beat 2 — Doc-to-Warehouse · 0:20–1:10 · 119 words

**Actions:**
1. Open `localhost:8603`. Drag in messy PDFs — or click **"Load Sample Messy PDFs"**.
2. Click **"Extract from uploaded PDFs"**. Let the confidence column and DUP/REVIEW flags render.
3. In the review grid, fix one row inline (edit the vendor name; show it normalizing).
4. Pick the client workspace from the selector.
5. Click **"Push N transactions to warehouse"**; watch the confirmation.
6. Click **"Open the Data Warehouse workspace →"**.

**VO:**
> Doc-to-Warehouse runs on port 8603. I'll click "Load Sample Messy PDFs" — bank statements, vendor invoices, weekly payroll. Then "Extract from uploaded PDFs". Everything runs offline, rule-based, with running-balance cross-checks: the parser checks each statement's own arithmetic.
>
> Every row gets a confidence score from zero to one, and anything questionable is flagged DUP or REVIEW. Nothing reaches the warehouse on trust — this review grid is the human gate. I'll fix this vendor inline, pick the client workspace, and push.
>
> The button reads "Push N transactions to warehouse" — N is whatever passed review. The push is idempotent: re-push the same batch, and nothing new gets inserted. Now the link at the bottom: "Open the Data Warehouse workspace →".

### Beat 3 — Data Warehouse · 1:10–2:10 · 149 words

**Actions:**
1. On `:8602`, select the demo workspace in the workspace selector.
2. Show the profitability tab: job/customer charts, the six-bucket rollup.
3. Show the anomaly alerts (one critical/warning is enough — don't narrate each).
4. Flip to the SoCal benchmark tab for ~5 seconds only.
5. Click **Executive PDF Board Brief**; let it generate; **hold 4+ seconds** on the finished PDF.

**VO:**
> This is the Data Warehouse on port 8602 — ten tabs, all reading the same Postgres this demo workspace just filled. Here's the profitability view: jobs and customers ranked by margin, with direct materials, subcontractor, and overhead all in a six-word vocabulary, so every dollar lands in exactly one bucket.
>
> The anomaly detector watches two things: labor overruns beyond twenty percent over estimate, and expense spikes beyond two standard deviations. Anything critical shows here as an alert.
>
> One tab over: SoCal market benchmarks, built from real public data — Census, BLS wage statistics, FRED, SBA, and SEC filings. About fifty-seven and a half thousand rows, all public.
>
> And this is the one I show every owner: one click — Executive PDF Board Brief. A board-ready PDF — margins, anomalies, runway — formatted for a bank or board meeting. The deliverable that usually takes an afternoon to assemble by hand.

### Beat 4 — SMB-Watchdog · 2:10–2:45 · 83 words

**Actions:**
1. Switch to the terminal (large font). Run `python3 watchdog.py`.
2. Let the color-coded summary print; point out the `digest_report.md` path.
3. Overlay (lower third) the cron one-liner: `0 7 * * * cd [WATCHDOG_DIR] && python3 watchdog.py >> watchdog.log 2>&1`

**VO:**
> Last piece: SMB-Watchdog, a headless, read-only CLI. In the terminal, one command: python3 watchdog.py.
>
> Four checks per client: open critical and warning anomalies; trailing thirty-day margin against benchmark — flagged when the gap exceeds five percentage points; the top three overtime-heavy employees over 184 hours and unprofitable whale clients; and ninety-day cash runway.
>
> It writes digest_report.md and an HTML version, and exits zero, two, or three. Exit code two means warning — so one cron line gives any client an automated daily guard.

### Beat 5 — Close · 2:45–3:00 · 36 words

**On screen:** End card — "100% local · free SoCal/SEC benchmark datasets · [REPO_URL]" (hold 5s).

**VO:**
> Everything you just saw ran on one laptop, offline. The suite, the source, and the free SoCal and SEC benchmark datasets are at [REPO_URL]. If you want numbers like these for your clients, it's all there.

---

## ON-SCREEN TEXT (lower thirds)

| Beat | Lower third text |
|---|---|
| 2 | `Doc-to-Warehouse — offline rule-based extraction · localhost:8603` |
| 3 | `Data Warehouse — 10 tabs · localhost:8602` |
| 4 | `SMB-Watchdog — read-only CLI · exit 0 / 2 / 3` |
| 4 (overlay) | `0 7 * * * cd [WATCHDOG_DIR] && python3 watchdog.py >> watchdog.log 2>&1` |
| 5 (end card) | `100% local · free SoCal/SEC benchmark datasets · [REPO_URL]` |

---

## TITLE / THUMBNAIL OPTIONS

| # | Title | Thumbnail text (large) |
|---|---|---|
| 1 | Board-Ready Numbers From Messy PDFs — 100% Local | `PDFs → BOARD PDF` / `100% LOCAL` |
| 2 | A Fractional CFO Stack That Never Touches the Cloud | `NO CLOUD. NO API KEYS.` / `3-MIN DEMO` |
| 3 | Bank Statement PDFs → Postgres → Board Brief, Offline | `9 RLS POLICIES. 0 UPLOADS.` / `FULL WALKTHROUGH` |

---

## EDITING NOTES

- If the Board Brief render takes more than ~5 seconds, cut the wait in editing; keep at least 4 seconds of the finished PDF on screen.
- Keep the watchdog terminal output on screen long enough to read the exit-code line; don't scroll past it while narrating.
- If a take slips past 3:00, trim from Beat 3's benchmark tab (VO already covers it) — never from the Board Brief hold or the exit-code line.
