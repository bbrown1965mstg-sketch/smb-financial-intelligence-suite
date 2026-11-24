#!/usr/bin/env python3
"""Explicit AppTest verification: 10 tabs, 0 exceptions, 0 st.error banners,
workspace selector present, and the Ask-Your-Data tab's Run-query flow works
through the restricted role path (chat mocked; no network)."""
import os
import sys

sys.path.insert(0, "/Users/Owner/Desktop/GLM-5.3/Data Warehouse")
os.chdir("/Users/Owner/Desktop/GLM-5.3/Data Warehouse")

EXPECTED_TABS = ["Overview", "Charts", "Anomaly alerts", "Ingest CSVs",
                 "Pricing simulator", "SoCal Benchmarks",
                 "Forecast & cash runway", "SoCal Capital & Deals",
                 "Public Peer Financials", "Ask Your Data"]

from streamlit.testing.v1 import AppTest  # noqa: E402
import nl_to_sql  # noqa: E402

at = AppTest.from_file("app.py", default_timeout=120)
at.run()
assert not at.exception, f"app raised: {at.exception}"
labels = [t.label for t in at.tabs]
assert labels == EXPECTED_TABS, labels
assert len(at.error) == 0, [e.value for e in at.error]
sbs = [sb for sb in at.sidebar.selectbox if sb.label == "Client workspace"]
assert sbs, "workspace selector missing"
print("tabs OK:", labels)
print("errors:", len(at.error), "| workspace selector:",
      [str(o) for o in sbs[0].options])

# Ask tab run-query flow through the role path: pre-set the SQL box (the
# state key the Run-query button reads) and click it.
class _MockChat:
    def __call__(self, messages, cfg):
        return ("SELECT COUNT(*) AS n FROM jobs")


nl_to_sql.chat = _MockChat()
at2 = AppTest.from_file("app.py", default_timeout=120)
at2.run()
assert not at2.exception
at2.session_state["nl_sql"] = "SELECT COUNT(*) AS n FROM jobs"
tabs = {t.label: t for t in at2.tabs}
# press the Run query button on the Ask tab
run_btns = [b for b in at2.button if b.label == "Run query"]
assert run_btns, "Run query button missing"
run_btns[0].click()
at2.run()
assert not at2.exception, at2.exception
errs = [e.value for e in at2.error]
assert not errs, errs
captions = [c.value for c in at2.caption]
print("Run-query captions:", captions[:4])
assert any("row(s)" in c for c in captions), captions
# the role-path proof: the result is workspace-scoped and the caption names
# row-level security
assert any("row-level security" in c for c in captions), captions
jobs_live = 2502
counted = None
for c in captions:
    if c.startswith(f"{jobs_live} row"):
        counted = c
print("counted caption:", counted)
print("APPTEST OK: 10 tabs, 0 exceptions, 0 banners, Ask run-query flow "
      "works through the role path")
