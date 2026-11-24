"""Doc-to-Warehouse -> Data Warehouse bridge (push reviewed transactions).

Connects the PDF extractor's reviewed/approved transactions to the local
PostgreSQL warehouse (``smb_warehouse``) that powers the "Data Warehouse"
dashboard (Streamlit on port 8602). Public API:

* ``DB_DSN``                 -- connection string (env-overridable).
* ``list_active_companies()`` -- client workspaces for the push panel.
* ``push_transactions(...)``  -- push transactions into ``financial_ledger``
  and ``labor_logs`` for ONE client workspace, then run the warehouse's
  post-load cadence.

How a push works (delegation, not re-implementation):

1. Duplicate-flagged rows are skipped unless ``include_duplicates=True``.
2. Ledger rows are built with the FIXED mapping imported from
   ``export_warehouse_csv.py`` (``_map_category`` / ``_fmt_amount`` -- the
   single source of truth shared with the CSV export): warehouse ``amount``
   is ALWAYS positive, ``entry_type`` carries the direction. Ledger rows are
   NOT job/customer attributed -- the canonical exporter derives no
   attribution from source documents (both columns stay NULL = overhead /
   general), so the push mirrors the CSV path exactly.
3. ``labor_logs`` rows come ONLY from payroll ``line_items`` (one row per
   employee per work date, same CHECK-range filters as the exporter:
   ``0 < hours <= 24``, ``hourly_rate >= 0``). They need ``job_id`` and
   ``employee_id`` FKs, so deterministic warehouse references are
   resolve-or-created FIRST, SCOPED TO THE COMPANY:
     * employees keyed by a deterministic email ``<slug>-c<company_id>@
       docbridge.example.com`` (the ingest engine resolves employees by
       email; ``employees.email`` is globally UNIQUE, so the company id is
       baked into the address);
     * one job per source document, ``DOC-C<company_id>-<SLUG>`` (the
       engine resolves jobs by ``job_number``, also globally UNIQUE);
     * one job_type + one customer per workspace (both required NOT NULL
       FK targets of ``jobs``), company-scoped names/emails.
   All inserts are ``INSERT ... ON CONFLICT ... DO NOTHING`` + a
   company-scoped ``SELECT`` -- idempotent, no ALTER/DROP of anything.
4. The actual data inserts are delegated to ``Data Warehouse/ingest_csv.py``
   (``run_ingest``) via a generated JSON config + two temp CSVs. That
   inherits, unchanged, the warehouse's own rules: company_id backfill into
   EVERY record (rows can never land unscoped), workspace-scoped FK
   resolution, and ``dedup: {"mode": "exact"}`` anti-join (in-file AND vs
   table) -- so pushing the SAME transactions again inserts 0 new rows.
   Rows the engine cannot load (unparseable/empty dates, zero amounts --
   ``chk_ledger_amount_positive`` -- out-of-range hours, unresolved FKs)
   come back as entries in the result's ``errors`` list instead of failing
   the whole push.
5. After a successful load the engine runs the documented post-load cadence
   ``refresh_all_analytics()`` THEN ``detect_anomalies()`` (order per the
   comment on ``refresh_all_analytics()`` in ``analytics_views.sql``:
   "typical cadence: nightly after data loads ..., BEFORE
   detect_anomalies()"). The functions themselves are called -- no
   hand-rolled REFRESH statements. The number of new anomaly rows
   ``detect_anomalies()`` inserted is captured in the result.

Failure policy: nothing is swallowed. Database-down, unknown ``company_id``
and file-level ingest failures raise; row-level problems are reported in
``result["errors"]``. Connections are short-lived: commit on success,
rollback + close on failure. Stdlib ``logging`` only, no prints.
"""

from __future__ import annotations

import csv
import logging
import os
import re
import shutil
import sys
import tempfile
from typing import Dict, List, Tuple

import psycopg2

_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)

# Canonical mapping -- single source of truth shared with the CSV export.
import export_warehouse_csv as exporter  # noqa: E402  (path set above)

# The warehouse's ingestion engine lives in the sibling "Data Warehouse"
# directory. APPEND (never insert at 0) so nothing there can shadow this
# project's modules or the standard library.
_SUITE_ROOT = os.path.dirname(_PROJECT_DIR)
_WAREHOUSE_DIR = os.path.join(_SUITE_ROOT, "Data Warehouse")
if os.path.isdir(_WAREHOUSE_DIR) and _WAREHOUSE_DIR not in sys.path:
    sys.path.append(_WAREHOUSE_DIR)

try:
    import ingest_csv  # type: ignore  # noqa: E402  (Data Warehouse/ingest_csv.py)
except ImportError as _exc:  # pragma: no cover - the suite layout is fixed
    ingest_csv = None
    _INGEST_IMPORT_ERROR = _exc
else:
    _INGEST_IMPORT_ERROR = None

logger = logging.getLogger("doc_to_warehouse.warehouse_bridge")

#: Connection string for the warehouse. Honors ``WAREHOUSE_DATABASE_URL``
#: then ``DATABASE_URL``, else the local default (matches Data Warehouse's
#: own default DSN).
DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/smb_warehouse"
DB_DSN = (os.environ.get("WAREHOUSE_DATABASE_URL")
          or os.environ.get("DATABASE_URL")
          or DEFAULT_DSN)

#: The Data Warehouse dashboard (no per-company URL routing exists there, so
#: the direct link is the plain root URL).
WAREHOUSE_APP_URL = "http://localhost:8602"

# Deterministic reference rows created for labor_logs FKs. The generated
# emails / job numbers embed the company id because employees.email,
# jobs.job_number, job_types.name and customers.email are globally UNIQUE
# while the references must stay scoped to one client workspace.
_BRIDGE_EMAIL_DOMAIN = "docbridge.example.com"
_JOB_TYPE_NAME_TMPL = "Doc-to-Warehouse Import (company {cid})"
_JOB_TYPE_DESC = ("Service line created by the Doc-to-Warehouse push bridge "
                  "for jobs imported from PDF documents.")
_CUSTOMER_NAME_TMPL = "Doc-to-Warehouse Import (company {cid})"


# ---------------------------------------------------------------------------
# deterministic reference derivation
# ---------------------------------------------------------------------------
def _slug_lower(text) -> str:
    """Lowercase alnum slug of ``text`` (mirrors ingest_csv._slug_email)."""
    return re.sub(r"[^a-z0-9]+", "", str(text or "").lower())[:24]


def employee_email_for(employee_name, company_id: int) -> str:
    """Deterministic, company-scoped email for a payroll employee.

    The ingest engine resolves ``labor_logs.employee_id`` by
    ``lower(email) = lower(value) AND company_id = <workspace>``; baking the
    company id into the address keeps workspaces isolated even though
    ``employees.email`` is globally UNIQUE."""
    slug = _slug_lower(employee_name) or "unknown"
    return f"{slug}-c{int(company_id)}@{_BRIDGE_EMAIL_DOMAIN}"


def job_number_for(source_file, company_id: int) -> str:
    """Deterministic, company-scoped ``job_number`` for one source document.

    One warehouse job per PDF: the ingest engine resolves
    ``labor_logs.job_id`` by ``lower(job_number) AND company_id``; the
    company id prefix keeps the globally-UNIQUE ``job_number`` namespaced."""
    stem = os.path.splitext(os.path.basename(str(source_file or "")))[0]
    slug = re.sub(r"[^A-Za-z0-9]+", "", stem)[:24].upper() or "DOCS"
    return f"DOC-C{int(company_id)}-{slug}"


def _split_employee_name(employee_name) -> Tuple[str, str]:
    """Split 'Maria Alvarez' into (first, last); never returns empty parts."""
    parts = str(employee_name or "").strip().split()
    if not parts:
        return "Unknown", "Unknown"
    if len(parts) == 1:
        return parts[0], parts[0]
    return parts[0], " ".join(parts[1:])


# ---------------------------------------------------------------------------
# reference ensure (INSERT ... ON CONFLICT DO NOTHING; never ALTER/DROP)
# ---------------------------------------------------------------------------
def _ensure_job_type_and_customer(cur, company_id: int) -> Dict[str, int]:
    """Resolve-or-create the job_type + customer that imported jobs need."""
    created = {"job_types": 0, "customers": 0}

    jt_name = _JOB_TYPE_NAME_TMPL.format(cid=company_id)
    cur.execute("""
        INSERT INTO job_types (company_id, name, description,
                               default_hourly_rate)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (name) DO NOTHING
    """, (company_id, jt_name, _JOB_TYPE_DESC, 0.00))
    created["job_types"] += cur.rowcount
    cur.execute("""
        SELECT id FROM job_types
         WHERE lower(name) = %s AND company_id = %s
         LIMIT 1
    """, (jt_name.lower(), company_id))
    row = cur.fetchone()
    if row is None:
        raise ValueError(
            f"job_type {jt_name!r} exists in another workspace "
            f"(name is globally unique) -- refusing to attach company "
            f"{company_id} jobs to it")
    job_type_id = row[0]

    cust_name = _CUSTOMER_NAME_TMPL.format(cid=company_id)
    cust_email = f"docbridge-c{int(company_id)}@{_BRIDGE_EMAIL_DOMAIN}"
    cur.execute("""
        INSERT INTO customers (company_id, name, email)
        VALUES (%s, %s, %s)
        ON CONFLICT (email) DO NOTHING
    """, (company_id, cust_name, cust_email))
    created["customers"] += cur.rowcount
    cur.execute("""
        SELECT id FROM customers
         WHERE lower(email) = %s AND company_id = %s
         LIMIT 1
    """, (cust_email.lower(), company_id))
    row = cur.fetchone()
    if row is None:
        raise ValueError(
            f"customer email {cust_email!r} exists in another workspace "
            f"(email is globally unique) -- refusing to attach company "
            f"{company_id} jobs to it")
    customer_id = row[0]

    return {"job_type_id": job_type_id, "customer_id": customer_id,
            "created": created}


def _ensure_reference_rows(conn, company_id: int,
                           employees_needed: Dict[str, dict],
                           job_specs: Dict[str, dict]) -> Dict[str, int]:
    """Resolve-or-create employees / jobs (scoped to the company) for the
    labor rows about to be pushed. Idempotent: re-pushing the same
    documents creates nothing. Returns counts of rows actually created."""
    created = {"employees": 0, "jobs": 0, "job_types": 0, "customers": 0}
    with conn.cursor() as cur:
        if job_specs:
            refs = _ensure_job_type_and_customer(cur, company_id)
            created["job_types"] += refs["created"]["job_types"]
            created["customers"] += refs["created"]["customers"]
            for source_file, spec in sorted(job_specs.items()):
                cur.execute("""
                    INSERT INTO jobs (company_id, job_number, customer_id,
                                      job_type_id, job_date, quoted_price,
                                      notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_number) DO NOTHING
                """, (company_id, spec["job_number"], refs["customer_id"],
                      refs["job_type_id"], spec["job_date"], 0.00,
                      f"Created by the Doc-to-Warehouse push bridge from "
                      f"source document {source_file!r}."))
                created["jobs"] += cur.rowcount
                cur.execute("""
                    SELECT id FROM jobs
                     WHERE lower(job_number) = %s AND company_id = %s
                     LIMIT 1
                """, (spec["job_number"].lower(), company_id))
                if cur.fetchone() is None:
                    raise ValueError(
                        f"job_number {spec['job_number']!r} exists in "
                        f"another workspace -- refusing to attach company "
                        f"{company_id} labor rows to it")
        for email, spec in sorted(employees_needed.items()):
            cur.execute("""
                INSERT INTO employees (company_id, first_name, last_name,
                                       email, hire_date, hourly_rate)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (email) DO NOTHING
            """, (company_id, spec["first_name"], spec["last_name"], email,
                  spec["hire_date"], spec["hourly_rate"]))
            created["employees"] += cur.rowcount
            cur.execute("""
                SELECT id FROM employees
                 WHERE lower(email) = %s AND company_id = %s
                 LIMIT 1
            """, (email.lower(), company_id))
            if cur.fetchone() is None:
                raise ValueError(
                    f"employee email {email!r} exists in another workspace "
                    f"-- refusing to attach company {company_id} labor rows "
                    f"to it")
    logger.info("Bridge reference ensure (company %s): created %s",
                company_id, {k: v for k, v in created.items() if v})
    return created


# ---------------------------------------------------------------------------
# row building (mirrors export_warehouse_csv, shared mapping)
# ---------------------------------------------------------------------------
def _split_pushable(txns: List[dict],
                    include_duplicates: bool) -> Tuple[List[dict], int]:
    """Split txns into the pushable set and the number of flagged duplicates
    left out."""
    pushable = [t for t in txns
                if include_duplicates or not t.get("is_duplicate")]
    return pushable, len(txns) - len(pushable)


def _build_ledger_rows(pushable: List[dict]) -> List[Dict[str, str]]:
    """One financial_ledger CSV row per transaction.

    entry_type/category come from ``exporter._map_category`` (the FIXED
    mapping); amounts are ALWAYS positive 2dp (``exporter._fmt_amount``),
    ``entry_type`` carries direction. job_id/customer_id are NOT derived
    from source documents (NULL = general attribution), exactly like the
    canonical CSV export."""
    rows = []
    for txn in pushable:
        entry_type, wh_category = exporter._map_category(
            txn.get("category"), txn.get("overhead_subtype"))
        rows.append({
            "EntryType": entry_type,
            "Category": wh_category,
            "Date": str(txn.get("date") or ""),
            "Amount": exporter._fmt_amount(txn.get("amount")),
            "Description": str(txn.get("description") or ""),
            "Vendor": str(txn.get("vendor") or ""),
        })
    return rows


def _build_labor_rows(pushable: List[dict], company_id: int
                      ) -> Tuple[List[Dict[str, str]], Dict[str, dict],
                                 Dict[str, dict], List[dict]]:
    """Labor CSV rows from payroll line_items ONLY (same filters as
    ``exporter.export_labor_logs``: ``0 < hours <= 24``, ``rate >= 0``).

    Also derives the deterministic employee/job references the rows need:
    ``employees_needed`` (email -> name/hire-date/rate spec) and
    ``job_specs`` (source_file -> job_number/job_date). Rows outside the
    CHECK ranges are skipped and returned as ``problems`` (reported, never
    silently dropped)."""
    labor_rows: List[Dict[str, str]] = []
    employees_needed: Dict[str, dict] = {}
    job_specs: Dict[str, dict] = {}
    problems: List[dict] = []

    for txn in pushable:
        if str(txn.get("doc_type") or "") != "payroll":
            continue
        source_file = str(txn.get("source_file") or "")
        for item in txn.get("line_items") or []:
            try:
                hours = float(item.get("hours"))
                rate = float(item.get("rate"))
            except (TypeError, ValueError):
                problems.append({
                    "file": source_file,
                    "error": "skipped malformed labor line item "
                             f"{item!r} (hours/rate not numeric)",
                })
                logger.warning("Skipping malformed labor row in %s: %r",
                               source_file, item)
                continue
            if not (0.0 < hours <= 24.0) or rate < 0:
                problems.append({
                    "file": source_file,
                    "error": f"skipped labor line item {item!r} outside "
                             f"warehouse CHECK ranges (hours={hours}, "
                             f"rate={rate})",
                })
                logger.warning(
                    "Skipping labor row outside warehouse CHECK ranges "
                    "(hours=%s, rate=%s) in %s", hours, rate, source_file)
                continue

            employee_name = str(item.get("employee") or "").strip()
            email = employee_email_for(employee_name, company_id)
            work_date = str(item.get("work_date") or "")
            job_number = job_number_for(source_file, company_id)

            labor_rows.append({
                "WorkDate": work_date,
                "Hours": f"{hours:.2f}",
                "Rate": f"{rate:.2f}",
                "Description": str(txn.get("description") or ""),
                "EmployeeEmail": email,
                "JobNumber": job_number,
            })

            first_name, last_name = _split_employee_name(employee_name)
            spec = employees_needed.setdefault(email, {
                "first_name": first_name,
                "last_name": last_name,
                "hire_date": work_date,
                "hourly_rate": rate,
            })
            # ISO dates compare correctly as strings; earliest work date is
            # the deterministic hire_date, the max seen rate the current rate.
            if work_date and (not spec["hire_date"]
                              or work_date < spec["hire_date"]):
                spec["hire_date"] = work_date
            spec["hourly_rate"] = max(float(spec["hourly_rate"]), rate)

            if source_file and source_file not in job_specs:
                job_specs[source_file] = {
                    "job_number": job_number,
                    "job_date": work_date,
                }
            elif source_file:
                jd = job_specs[source_file]["job_date"]
                if work_date and (not jd or work_date < jd):
                    job_specs[source_file]["job_date"] = work_date

    # hire_date / job_date are NOT NULL: fall back to a stable default when
    # a source row carried no usable date.
    for spec in employees_needed.values():
        if not spec["hire_date"]:
            spec["hire_date"] = "1970-01-01"
    for spec in job_specs.values():
        if not spec["job_date"]:
            spec["job_date"] = "1970-01-01"

    return labor_rows, employees_needed, job_specs, problems


# ---------------------------------------------------------------------------
# CSV + config generation for the ingest engine
# ---------------------------------------------------------------------------
_LEDGER_CONFIG = {
    "name": "Doc-to-Warehouse push: financial_ledger",
    "target": "financial_ledger",
    "column_map": {
        "EntryType": {"column": "entry_type", "type": "text",
                      "required": True},
        "Category": {"column": "category", "type": "text",
                     "required": True},
        "Date": {"column": "entry_date", "type": "date",
                 "formats": ["%Y-%m-%d"], "required": True},
        "Amount": {"column": "amount", "type": "money", "required": True},
        "Description": {"column": "description", "type": "text"},
        "Vendor": {"column": "vendor", "type": "text"},
    },
    "dedup": {"mode": "exact"},   # anti-join: in-file AND vs table
    "refresh": False,
}

_LABOR_CONFIG = {
    "name": "Doc-to-Warehouse push: labor_logs",
    "target": "labor_logs",
    "column_map": {
        "WorkDate": {"column": "work_date", "type": "date",
                     "formats": ["%Y-%m-%d"], "required": True},
        "Hours": {"column": "hours", "type": "hours", "required": True},
        "Rate": {"column": "hourly_rate", "type": "float",
                 "required": True},
        "Description": {"column": "description", "type": "text"},
    },
    "resolve": {
        "employee_id": {"table": "employees", "match_column": "email",
                        "from_csv": "EmployeeEmail",
                        "on_missing": "skip_row"},
        "job_id": {"table": "jobs", "match_column": "job_number",
                   "from_csv": "JobNumber", "on_missing": "skip_row"},
    },
    "dedup": {"mode": "exact"},   # anti-join: in-file AND vs table
    "refresh": False,
}


def _write_csv(path: str, rows: List[Dict[str, str]]) -> None:
    """Write one UTF-8 CSV (header = the column_map keys of its config)."""
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(list(rows[0].keys()))
        for row in rows:
            writer.writerow([row[k] for k in rows[0].keys()])


def _build_ingest_config(run_dir: str, ledger_rows: List[Dict[str, str]],
                         labor_rows: List[Dict[str, str]]) -> dict:
    """JSON config for ``ingest_csv.run_ingest``: one file spec per
    non-empty target; ``refresh: true`` rides on the LAST file so the
    engine runs the post-load cadence exactly once, after all loads."""
    files = []
    if ledger_rows:
        path = os.path.join(run_dir, exporter.FINANCIAL_LEDGER_FILENAME)
        _write_csv(path, ledger_rows)
        spec = dict(_LEDGER_CONFIG)
        spec["path"] = path
        files.append(spec)
    if labor_rows:
        path = os.path.join(run_dir, exporter.LABOR_LOGS_FILENAME)
        _write_csv(path, labor_rows)
        spec = dict(_LABOR_CONFIG)
        spec["path"] = path
        files.append(spec)
    if files:
        files[-1]["refresh"] = True
    return {"files": files}


# ---------------------------------------------------------------------------
# public API
# ---------------------------------------------------------------------------
def list_active_companies() -> List[dict]:
    """The client workspaces ("active" = every existing companies row; the
    table has no is_active column), ordered by company_name.

    Returns dicts with ``company_id``, ``company_name``,
    ``industry_vertical``, ``socal_region``. Raises on DB-down (the caller
    -- the Streamlit panel -- shows the error + a retry)."""
    conn = psycopg2.connect(DB_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT company_id, company_name, industry_vertical,
                       socal_region
                  FROM companies
                 ORDER BY company_name
            """)
            rows = cur.fetchall()
    finally:
        conn.close()
    companies = [
        {"company_id": int(r[0]),
         "company_name": r[1] or "",
         "industry_vertical": r[2],
         "socal_region": r[3]}
        for r in rows
    ]
    logger.info("list_active_companies: %d workspace(s)", len(companies))
    return companies


def push_transactions(txns: List[dict], company_id: int, *,
                      include_duplicates: bool = False) -> dict:
    """Push reviewed/approved transactions into the warehouse for ONE
    client workspace, then run the warehouse post-load cadence
    (``refresh_all_analytics()`` -> ``detect_anomalies()``).

    Skips rows with ``is_duplicate=True`` unless ``include_duplicates``.
    Raises ``ValueError`` on empty ``txns`` / unknown ``company_id`` and
    propagates DB-down / connection errors. Row-level problems (empty
    dates, zero amounts, out-of-range hours) never abort the push -- they
    are returned in ``errors``.

    Returns a result dict with at least::

        ledger_rows             int  -- financial_ledger rows inserted
        labor_rows              int  -- labor_logs rows inserted
        skipped_duplicates      int  -- flagged dupes not pushed + rows the
                                        exact-mode anti-join deduped (already
                                        in the warehouse)
        anomalies_new           int  -- new anomaly_alerts rows from
                                        detect_anomalies()
        refreshed               bool -- refresh+anomaly cadence ran
        errors                  list -- {"file", "row"?, "error"} entries
        (plus company_id, company_name, pushed_txns,
         skipped_flagged_duplicates, already_in_warehouse, created_refs)
    """
    if not txns:
        raise ValueError("push_transactions: txns is empty -- nothing to "
                         "push")
    if ingest_csv is None:  # pragma: no cover - fixed suite layout
        raise RuntimeError(
            "the Data Warehouse ingestion engine (ingest_csv.py) could not "
            f"be imported: {_INGEST_IMPORT_ERROR}")
    try:
        cid = int(company_id)
    except (TypeError, ValueError):
        raise ValueError(f"push_transactions: invalid company_id "
                         f"{company_id!r}")
    if cid <= 0:
        raise ValueError(f"push_transactions: invalid company_id {cid}")

    pushable, flagged_skipped = _split_pushable(txns, include_duplicates)
    ledger_rows = _build_ledger_rows(pushable)
    labor_rows, employees_needed, job_specs, problems = \
        _build_labor_rows(pushable, cid)

    # ---- short-lived connection #1: validate workspace + ensure refs ----
    conn = psycopg2.connect(DB_DSN)     # DB-down raises straight through
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT company_name FROM companies WHERE company_id = %s
            """, (cid,))
            row = cur.fetchone()
        if row is None:
            conn.rollback()
            raise ValueError(
                f"push_transactions: unknown company_id {cid} -- no such "
                f"client workspace in the companies table")
        company_name = row[0]

        created_refs: Dict[str, int] = {"employees": 0, "jobs": 0,
                                        "job_types": 0, "customers": 0}
        if labor_rows:
            # Resolve-or-create the FK targets FIRST so the ingest engine's
            # workspace-scoped lookups (employees by email, jobs by
            # job_number) match them.
            created_refs = _ensure_reference_rows(
                conn, cid, employees_needed, job_specs)
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        finally:
            conn.close()
        raise
    conn.close()

    # ---- delegation: the warehouse's own ingestion engine ----
    run_dir = tempfile.mkdtemp(prefix="dw_push_")
    try:
        config = _build_ingest_config(run_dir, ledger_rows, labor_rows)
        if config["files"]:
            # run_ingest backfills company_id into EVERY record, scopes all
            # FK lookups to the workspace, dedups with the exact-mode
            # anti-join (idempotent re-push) and, because the last file has
            # refresh: true, runs refresh_all_analytics() ->
            # detect_anomalies() once after the loads.
            summary = ingest_csv.run_ingest(config, db=DB_DSN,
                                            company_id=cid)
        else:
            summary = {"results": [], "refreshed": False, "new_alerts": 0}
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)

    # ---- summarize (never swallow: file failures raise, rows report) ----
    ledger_loaded = 0
    labor_loaded = 0
    engine_skipped = 0
    errors: List[dict] = list(problems)
    for res in summary.get("results", []):
        if res.get("error"):
            raise RuntimeError(
                f"warehouse ingest failed for {res.get('file')!r} and was "
                f"rolled back (other file's rows, if any, may have "
                f"committed): {res['error']}")
        if res.get("target") == "financial_ledger":
            ledger_loaded += int(res.get("loaded") or 0)
        elif res.get("target") == "labor_logs":
            labor_loaded += int(res.get("loaded") or 0)
        engine_skipped += int(res.get("skipped_duplicates") or 0)
        for row_err in res.get("errors", []):
            errors.append({"file": res.get("file"),
                           "row": row_err.get("row"),
                           "error": row_err.get("error")})

    result = {
        "company_id": cid,
        "company_name": company_name,
        "pushed_txns": len(pushable),
        "ledger_rows": ledger_loaded,
        "labor_rows": labor_loaded,
        "skipped_duplicates": flagged_skipped + engine_skipped,
        "skipped_flagged_duplicates": flagged_skipped,
        "already_in_warehouse": engine_skipped,
        "anomalies_new": int(summary.get("new_alerts") or 0),
        "refreshed": bool(summary.get("refreshed")),
        "errors": errors,
        "created_refs": created_refs,
    }
    logger.info(
        "push_transactions company=%s (%s): %d ledger + %d labor rows "
        "inserted, %d duplicates skipped, %d new anomalies, %d error row(s)",
        cid, company_name, ledger_loaded, labor_loaded,
        result["skipped_duplicates"], result["anomalies_new"], len(errors))
    return result
