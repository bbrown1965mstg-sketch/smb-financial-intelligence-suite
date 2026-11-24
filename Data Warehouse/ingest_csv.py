#!/usr/bin/env python3
"""
ingest_csv.py -- load messy accounting / payroll CSV exports into smb_warehouse.

MULTI-TENANT: every load targets ONE client workspace. run_ingest(...,
company_id=N) backfills record["company_id"] = N for all 6 supported tables
in finalize_record (so the REQUIRED checks are satisfied by the backfill and
rows can never land unscoped), FK lookups (jobs by job_number, customers by
name, employees by email, job_types by name) are scoped to the same workspace
so a CSV row can never silently attach to another client's parents, and the
on_missing=create customer insert carries the workspace too. Dashboard note:
the Ingest CSVs tab passes the ACTIVE workspace's id.

Driven by a JSON config (see ingest_config.example.json) that maps each CSV's
columns onto warehouse tables. The engine handles the reality of SMB exports:
"$1,234.56" and "(128.45)" money, M/D/YYYY dates, mixed-case emails, repeated
rows, missing foreign keys, and sign-based direction (QuickBooks debits are
negative; the warehouse stores positive amounts + entry_type).

Config schema (one file = one target table):

{
  "files": [
    {
      "name":       "QuickBooks export",
      "path":       "samples/accounting_export.csv",   # relative to this file
      "target":     "financial_ledger",                # see SUPPORTED_TABLES
      "delimiter":  ",",
      "encoding":   "utf-8-sig",
      "skip_rows":  0,                                  # junk preamble rows
      "column_map": {                                   # CSV header -> target
        "Date":    {"column": "entry_date", "type": "date",
                    "formats": ["%m/%d/%Y", "%Y-%m-%d"]},
        "Amount":  {"column": "amount", "type": "money_signed",
                    "entry_type_from_sign": true},
        "Email":   {"column": "email", "type": "text", "lower": true},
        "Notes":   {"column": "description", "type": "text", "null_if": ["N/A"]}
      },
      "constants": {"entry_type": "expense"},           # fixed values (mapped values win)
      "resolve": {                                      # FK lookup: db column spec
        "job_id": {"table": "jobs", "match_column": "job_number",
                   "from_csv": "Job Number", "on_missing": "null"},
        "customer_id": {"table": "customers", "match_column": "name",
                        "from_csv": "Customer", "on_missing": "create"}
      },
      "dedup": {"mode": "unique_key", "keys": ["email"], "on_conflict": "skip"},
                 # or {"mode": "exact"} -- anti-join identical rows (in-file + vs table)
      "refresh": true                                   # refresh views + detect anomalies after
    }
  ]
}

Types: text (trim/collapse/lower/upper), date (formats list, else auto-try),
money (positive), money_signed (sign -> entry_type when entry_type_from_sign),
int, float, hours (0 < x <= 24), bool.

on_missing policies for FK resolution: "null" (only for nullable columns),
"skip_row", "error", "create" (customers only: row provides the name).

Run:  python3 ingest_csv.py --config ingest_config.example.json [--dry-run]
      [--company-id 1]
"""

import argparse
import datetime as dt
import hashlib
import json
import os
import re
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

# Honors DATABASE_URL (used by the Docker stack: postgresql://postgres:postgres@db:5432/...)
DB = os.environ.get("DATABASE_URL",
                    "postgresql://postgres:postgres@localhost:5432/smb_warehouse")

SUPPORTED_TABLES = {
    "customers", "job_types", "employees", "jobs",
    "financial_ledger", "labor_logs",
}

# Columns that must be present (after mapping/constants/resolution) per table.
REQUIRED = {
    "customers":        ["name", "email"],
    "job_types":        ["name", "default_hourly_rate"],
    "employees":        ["first_name", "last_name", "email", "hire_date", "hourly_rate"],
    "jobs":             ["job_number", "customer_id", "job_type_id", "job_date", "quoted_price"],
    "financial_ledger": ["entry_type", "category", "entry_date", "amount"],
    "labor_logs":       ["job_id", "employee_id", "work_date", "hours", "hourly_rate"],
}

DATE_FORMATS = [
    "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%d-%b-%Y", "%b %d, %Y",
    "%m-%d-%Y", "%Y/%m/%d", "%d.%m.%Y",
]

RESOLVABLE_TABLES = {"jobs": "job_number", "customers": "name",
                     "employees": "email", "job_types": "name"}

# Python-side backfill for NOT NULL columns that have DB defaults -- CSV rows
# that don't map them get the same value the database would have applied.
TABLE_DEFAULTS = {
    "customers":        {"is_active": True},
    "job_types":        {"is_active": True},
    "employees":        {"is_active": True},
    "jobs":             {"status": "quoted"},
    "financial_ledger": {},
    "labor_logs":       {},
}


class RowError(Exception):
    """A row-level problem: the row is skipped and reported, not fatal."""


# ---------------------------------------------------------------------------
# value coercion
# ---------------------------------------------------------------------------
def _clean_text(raw, spec):
    s = "" if raw is None else str(raw)
    s = re.sub(r"\s+", " ", s).strip()
    if s.lower() in [x.lower() for x in spec.get("null_if", [])]:
        return None
    if spec.get("lower"):
        s = s.lower()
    elif spec.get("upper"):
        s = s.upper()
    return s or None


def _parse_money(raw):
    """'$1,234.56' -> 1234.56 ; '(1,234.56)' -> -1234.56 ; '-$45' -> -45.0"""
    s = "" if raw is None else str(raw).strip()
    if s.lower() in ("", "n/a", "na", "-", "--"):
        raise RowError(f"not a number: {raw!r}")
    neg = s.startswith("(") and s.endswith(")")
    s = re.sub(r"[()\s$€£]", "", s).replace(",", "")
    try:
        val = float(s)
    except ValueError:
        raise RowError(f"not a number: {raw!r}")
    return -val if neg else val


def _parse_date(raw, formats):
    s = "" if raw is None else str(raw).strip()
    if s.lower() in ("", "n/a", "na", "-"):
        raise RowError(f"not a date: {raw!r}")
    for fmt in (formats or []):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    for fmt in DATE_FORMATS:
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise RowError(f"unrecognized date: {raw!r}")


def _parse_bool(raw):
    s = str(raw).strip().lower()
    if s in ("true", "t", "yes", "y", "1"):
        return True
    if s in ("false", "f", "no", "n", "0"):
        return False
    raise RowError(f"not a boolean: {raw!r}")


def coerce(raw, spec):
    """Apply one column_map spec to a raw CSV value; returns coerced value
    or raises RowError."""
    kind = spec.get("type", "text")
    if kind == "text":
        return _clean_text(raw, spec)
    if kind in ("money", "money_signed"):
        val = _parse_money(raw)
        if val == 0:
            raise RowError("zero amount")
        return round(val, 2)
    if kind == "date":
        return _parse_date(raw, spec.get("formats"))
    if kind == "int":
        try:
            return int(float(str(raw).strip()))
        except (ValueError, TypeError):
            raise RowError(f"not an integer: {raw!r}")
    if kind in ("float", "hours"):
        val = _parse_money(raw)
        if kind == "hours" and not (0 < val <= 24):
            raise RowError(f"hours out of range (0,24]: {val}")
        return round(val, 2)
    if kind == "bool":
        return _parse_bool(raw)
    raise RowError(f"unknown type {kind!r}")


# ---------------------------------------------------------------------------
# ingest engine
# ---------------------------------------------------------------------------
class Ingestor:
    def __init__(self, conn, company_id=1):
        self.conn = conn
        # Every FK lookup and created reference is scoped to this client
        # workspace so an ingest never attaches rows to another client's
        # parents (multi-tenant safety; see module docstring).
        self.company_id = int(company_id)
        self.lookup_cache = {}
        self.created_refs = {"customers": 0, "job_types": 0, "employees": 0}

    def resolve_fk(self, db_col, spec, record, row_no):
        """Resolve one FK: look up table.match_column for the value the CSV
        provided, following on_missing policy. Returns the id (or None)."""
        table = spec["table"]
        match_column = spec["match_column"]
        raw = record.get(f"__resolve_{db_col}")
        if raw in (None, ""):
            return None
        cache_key = (table, match_column, str(raw).lower())
        if cache_key in self.lookup_cache:
            hit = self.lookup_cache[cache_key]
        else:
            hit = self._lookup(table, match_column, raw)
            self.lookup_cache[cache_key] = hit
        if hit is not None:
            return hit
        policy = spec.get("on_missing", "skip_row")
        if policy == "null":
            return None
        if policy == "create":
            if table != "customers":
                raise RowError(f"create policy unsupported for {table}")
            return self._create_customer(raw)
        if policy == "error":
            raise RowError(f"{table} {match_column}={raw!r} not found")
        raise RowError(f"{table} {match_column}={raw!r} not found (row skipped)")

    def _lookup(self, table, match_column, value):
        # Workspace-scoped: only the active company's rows are resolvable
        # (tables and match_column come from the code's own RESOLVABLE_TABLES
        # paths, never user input).
        with self.conn.cursor() as cur:
            cur.execute(f'SELECT id FROM {table} WHERE lower({match_column}) = %s'
                        f' AND company_id = %s LIMIT 1',
                        (str(value).strip().lower(), self.company_id))
            r = cur.fetchone()
        return r[0] if r else None

    def _create_customer(self, name):
        """Create a minimal customer from a name (on_missing: create) inside
        the active client workspace."""
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO customers (company_id, name, email)
                VALUES (%s, %s, %s)
                ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
            """, (self.company_id, name.strip(), _slug_email(name)))
            cid = cur.fetchone()[0]
        self.created_refs["customers"] += 1
        self.lookup_cache[("customers", "name", name.strip().lower())] = cid
        return cid


def _slug_email(name):
    slug = re.sub(r"[^a-z0-9]+", "", name.lower())[:24] or "unknown"
    return f"{slug}@imported.example.com"


def build_record(row, file_spec, ing, row_no):
    """Turn one CSV dict into a target-record dict, or raise RowError."""
    record = {}
    # 1) mapped columns
    for csv_col, spec in (file_spec.get("column_map") or {}).items():
        raw = row.get(csv_col)
        if spec.get("column", "").startswith("__"):
            continue
        val = coerce(raw, spec)
        if spec.get("type") == "money_signed" and spec.get("entry_type_from_sign"):
            record["entry_type"] = "revenue" if val > 0 else "expense"
            val = abs(val)
        record[spec["column"]] = val
        if spec.get("required") and val in (None, ""):
            raise RowError(f"missing required value for {csv_col!r}")
    # 2) constants (mapped values win)
    for k, v in (file_spec.get("constants") or {}).items():
        record.setdefault(k, v)
    # 3) FK resolution inputs (raw CSV values carried aside)
    for db_col, spec in (file_spec.get("resolve") or {}).items():
        record[f"__resolve_{db_col}"] = _clean_text(row.get(spec.get("from_csv")), {})
    return record


def finalize_record(record, file_spec, ing, row_no):
    """Resolve FKs, backfill table defaults, validate required columns.

    Multi-tenancy: the client workspace is backfilled here (record
    ["company_id"] = the ingest's company_id) for ALL 6 supported tables, so
    every loaded row lands in the active workspace and the NOT NULL company_id
    column is satisfied without any config change. A company_id mapped
    explicitly in the CSV/column_map would still win (the backfill uses
    setdefault)."""
    for db_col, spec in (file_spec.get("resolve") or {}).items():
        record[db_col] = ing.resolve_fk(db_col, spec, record, row_no)
        del record[f"__resolve_{db_col}"]
    record.setdefault("company_id", ing.company_id)
    for k, v in TABLE_DEFAULTS.get(file_spec["target"], {}).items():
        record.setdefault(k, v)
    missing = [c for c in REQUIRED.get(file_spec["target"], []) if record.get(c) in (None, "")]
    if missing:
        raise RowError(f"missing required columns: {', '.join(missing)}")
    return record


def ingest_file(cur, ing, file_spec, base_dir, dry_run=False):
    """Ingest one file; returns a summary dict."""
    path = (base_dir / file_spec["path"]) if not Path(file_spec["path"]).is_absolute() \
        else Path(file_spec["path"])
    target = file_spec["target"]
    if target not in SUPPORTED_TABLES:
        raise ValueError(f"unsupported target table {target!r}")

    with open(path, newline="", encoding=file_spec.get("encoding", "utf-8-sig")) as fh:
        reader = csv_reader(fh, file_spec)
        rows = list(reader)
    for _ in range(file_spec.get("skip_rows", 0)):
        if rows:
            rows.pop(0)
    if len(rows) > 200_000:
        raise ValueError(f"{path}: {len(rows)} rows exceeds the 200k safety cap")

    summary = {"file": file_spec.get("name", str(path)), "path": str(path),
               "target": target, "rows_read": len(rows), "loaded": 0,
               "skipped_duplicates": 0, "errors": [], "created_refs": {}}

    records = []
    for i, row in enumerate(rows, start=2):        # +header line
        try:
            rec = build_record(row, file_spec, ing, i)
            rec = finalize_record(rec, file_spec, ing, i)
            records.append(rec)
        except RowError as exc:
            summary["errors"].append({"row": i, "error": str(exc)})
        except Exception as exc:                    # noqa: BLE001 - keep going
            summary["errors"].append({"row": i, "error": f"unexpected: {exc}"})

    dedup = file_spec.get("dedup", {})
    mode = dedup.get("mode", "exact")
    if not records:                                 # every row errored
        return summary

    if dry_run:
        summary["loaded"] = len(records)
        summary["skipped_duplicates"] = "n/a (dry run)"
        summary["created_refs"] = dict(ing.created_refs)  # what WOULD be created
        return summary

    if mode == "unique_key":
        keys = dedup["keys"]
        action = "DO NOTHING" if dedup.get("on_conflict", "skip") == "skip" \
            else "DO UPDATE SET " + ", ".join(
                f"{c} = EXCLUDED.{c}" for c in records[0]
                if c not in ("id",) and not c.startswith("__"))
        cols = sorted({k for r in records for k in r})
        rows_t = [tuple(r.get(c) for c in cols) for r in records]
        before = _count(cur, target)
        execute_values(cur, f"""
            INSERT INTO {target} ({', '.join(cols)}) VALUES %s
            ON CONFLICT ({', '.join(keys)}) {action}
        """, rows_t, page_size=500)
        summary["loaded"] = _count(cur, target) - before
        summary["skipped_duplicates"] = len(rows_t) - summary["loaded"]
        if dedup.get("on_conflict") == "update":
            summary["skipped_duplicates"] = 0
    else:                                           # exact anti-join dedup
        cols = sorted({k for r in records for k in r})
        # in-file duplicates first (hash of the full mapped row)
        seen, uniq_records = set(), []
        for r in records:
            key = tuple(r.get(c) for c in cols)
            h = hashlib.md5(repr(key).encode()).hexdigest()
            if h in seen:
                summary["skipped_duplicates"] += 1
                continue
            seen.add(h)
            uniq_records.append(r)
        stage = f"temp_stage_{abs(hash(file_spec.get('name', 'f'))) % 100000}"
        cur.execute(f"CREATE TEMP TABLE {stage} (LIKE {target} INCLUDING DEFAULTS) "
                    f"ON COMMIT DROP")
        cur.execute(f"ALTER TABLE {stage} DROP COLUMN IF EXISTS id")
        cur.execute(f"ALTER TABLE {stage} DROP COLUMN IF EXISTS created_at")
        cur.execute(f"ALTER TABLE {stage} DROP COLUMN IF EXISTS updated_at")
        insert_cols = [c for c in cols if c not in ("id", "created_at", "updated_at")]
        execute_values(cur, f"INSERT INTO {stage} ({', '.join(insert_cols)}) VALUES %s",
                       [tuple(r.get(c) for c in insert_cols) for r in uniq_records],
                       page_size=500)
        before = _count(cur, target)
        match_cols = dedup.get("match_columns", insert_cols)
        conditions = " AND ".join(
            f"t.{c} IS NOT DISTINCT FROM s.{c}" for c in match_cols)
        cur.execute(f"""
            INSERT INTO {target} ({', '.join(insert_cols)})
            SELECT {', '.join(f's.{c}' for c in insert_cols)}
            FROM {stage} s
            WHERE NOT EXISTS (
                SELECT 1 FROM {target} t WHERE {conditions})
        """)
        loaded = _count(cur, target) - before
        summary["loaded"] = loaded
        summary["skipped_duplicates"] += len(uniq_records) - loaded

    summary["created_refs"] = dict(ing.created_refs)
    return summary


def csv_reader(fh, file_spec):
    import csv as _csv
    return _csv.DictReader(fh, delimiter=file_spec.get("delimiter", ","))


def _count(cur, table):
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return cur.fetchone()[0]


def run_ingest(config, db=DB, dry_run=False, company_id=1):
    """Run a full ingest config (dict or path to JSON) into ONE client
    workspace. Returns a summary dict with per-file results; commits
    everything or nothing (per-file rollback on hard failure).

    company_id (default 1 = the demo workspace) is backfilled into every
    loaded record of all 6 supported tables (see finalize_record) and scopes
    the FK lookups / created references (see Ingestor). The dashboard's Ingest
    CSVs tab passes the active client workspace's id."""
    if isinstance(config, (str, Path)):
        cfg_path = Path(config).resolve()
        base_dir = cfg_path.parent
        config = json.loads(cfg_path.read_text())
    else:
        base_dir = Path.cwd()
    conn = psycopg2.connect(db)
    results, refreshed, new_alerts = [], False, 0
    try:
        ing = Ingestor(conn, company_id=company_id)
        for file_spec in config.get("files", []):
            try:
                with conn.cursor() as cur:
                    res = ingest_file(cur, ing, file_spec, base_dir, dry_run)
                if dry_run:
                    # Dry run must write NOTHING. resolve_fk's "create" policy
                    # inserts customers during record building (before the
                    # dedup/insert stage the dry run skips), so discard the
                    # transaction instead of committing it.
                    conn.rollback()
                else:
                    conn.commit()
                results.append(res)
                refreshed = refreshed or bool(file_spec.get("refresh"))
            except Exception as exc:            # noqa: BLE001 - file-level rollback
                conn.rollback()
                results.append({"file": file_spec.get("name", "?"),
                                "target": file_spec.get("target"),
                                "error": f"file failed, rolled back: {exc}",
                                "rows_read": 0, "loaded": 0,
                                "skipped_duplicates": 0, "errors": []})
        if refreshed and not dry_run:
            with conn.cursor() as cur:
                cur.execute("SELECT refresh_all_analytics();")
                cur.execute("SELECT * FROM detect_anomalies();")
                new_alerts = len(cur.fetchall())
            conn.commit()
    finally:
        conn.close()
    return {"results": results, "refreshed": refreshed, "new_alerts": new_alerts}


def print_summary(summary):
    for res in summary["results"]:
        print(f"\n--- {res.get('file')}  ->  {res.get('target')}")
        if res.get("error"):
            print(f"    ERROR: {res['error']}")
            continue
        print(f"    rows read: {res['rows_read']}  loaded: {res['loaded']}  "
              f"duplicates skipped: {res['skipped_duplicates']}")
        if res.get("created_refs"):
            refs = {k: v for k, v in res["created_refs"].items() if v}
            if refs:
                print(f"    created references: {refs}")
        for e in res.get("errors", []):
            print(f"    row {e['row']}: {e['error']}")
    if summary.get("refreshed"):
        print(f"\nviews refreshed; detect_anomalies() raised "
              f"{summary['new_alerts']} new alert(s)")


def main():
    ap = argparse.ArgumentParser(description="CSV -> smb_warehouse ingestion")
    ap.add_argument("--config", required=True, help="JSON ingest config path")
    ap.add_argument("--db", default=DB)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--company-id", type=int, default=1, dest="company_id",
                    help="client workspace to load into (default 1 = the "
                         "demo workspace)")
    args = ap.parse_args()
    print_summary(run_ingest(args.config, db=args.db, dry_run=args.dry_run,
                             company_id=args.company_id))


if __name__ == "__main__":
    main()
