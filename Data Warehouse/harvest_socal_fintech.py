#!/usr/bin/env python3
"""
harvest_socal_fintech.py -- harvest REAL SoCal Finance/Tech benchmark data
into the six SoCal regional benchmark tables (schema.sql sections 2.11-2.16)
and refresh the derived analytics layer afterwards (manually: apply
analytics_views.sql or call refresh_all_analytics()).

SIX HARVEST FAMILIES (--sources, run in this canonical order):

  oews     REAL BLS OEWS wages (May 2024 special-request zips) for 8 task-facing
           roles x SoCal regions -> socal_wage_benchmarks.
           * oesm24ma.zip: metro areas 31080 la_msa, 37100 oxnard_ventura_msa,
             41740 san_diego_msa, 40140 inland_empire_msa (rows are
             cross-ownership, NAICS 000000 = all industries).
           * oesm24st.zip: California (AREA 06) -> california_state.
           * ORANGE COUNTY NOTE (verified 2026-11-21): the Santa
             Ana-Anaheim-Irvine Metro Division (36004) is NOT published anywhere
             in the May 2024 OEWS files (BLS discontinued metro-division
             estimates; verified in oesm24ma.zip, oesm24all.zip and the May 2023
             metro zip). orange_county rows are therefore loaded from the
             California statewide file as a DOCUMENTED PROXY (source string says
             so explicitly) -- mirroring the CPIAUCSL CPI proxy for regions
             without their own CPI series.
  fred     REAL macro indicators via FRED's keyless CSV endpoint
           (fred.stlouisfed.org/graph/fredgraph.csv?id=<id>) -> socal_macro_
           indicators. Sub-monthly series are averaged per calendar month
           (like fetch_benchmarks.py); '.'-marked missing observations are
           skipped. California has no statewide CPI and Oxnard-Ventura /
           Orange County have no metro CPI series, so the US CPI-U (CPIAUCSL)
           is ALSO loaded as a documented proxy under those region codes.
           METRO CPI NOTE (verified 2026-11-21): FRED only mirrors the
           PRE-2018 BLS metro CPI ids -- CUURA311SA0 (LA) ends 2017-11 and
           CUURA424SA0 (San Diego) ends 1986-11, and the Riverside one has
           the old Dec 2017 base. The three current SoCal metro CPIs are
           therefore loaded straight from BLS's public time-series file
           (download.bls.gov/pub/time.series/cu/cu.data.1.AllItems):
           CUURS49ASA0 la_msa, CUURS49ESA0 san_diego_msa, CUURS49CSA0
           inland_empire_msa.
  cbp      REAL Census County Business Patterns 2023 (cbp23co.zip) for the six
           SoCal counties, NAICS 51/52/54 families -> socal_business_patterns
           (geo_level 'county'). The CBP 2023 file is NAICS 2017-coded
           (verified in the data itself: software publishers appear as
           511210, the 2017 code); publishing-family rows (511x) are ALSO
           mirrored to their NAICS 2022 equivalents (511 -> 513,
           5112 -> 5132 Software Publishers) per the Census 2017->2022
           publishing-family concordance, so the task-vocabulary code 5132
           is queryable. VERIFIED against the CBP record layout: CBP publishes
           NOISE-ADJUSTED employment/payroll for every row and the _nf fields
           are noise-LEVEL flags (G <2%, H 2-<5%, J >=5%), NOT suppression
           flags; the file is comma-delimited WITH a header row. AP is annual
           payroll in THOUSANDS of dollars -> stored x 1000.
  zbp      REAL Census ZIP Business Patterns 2023 (zbp23detail.zip) for the
           six SoCal counties' ZIPs -> socal_business_patterns (geo_level
           'zip'). The modern ZBP detail file publishes only establishments
           by size class (no employment/payroll -> those columns stay NULL)
           and -- conveniently -- carries city/state/COUNTY-NAME columns, so
           ZIPs are attributed to counties straight from the source file (no
           external zip->county dataset needed).
  sba      REAL SBA 7(a)/504 loan activity, aggregated from the loan-level
           FOIA CSVs discovered on data.sba.gov/dataset/7a-504-foia ->
           socal_loan_activity. Files can be 100-400MB: downloaded STREAMING
           into the disk cache and parsed incrementally with the csv module.
           data.sba.gov can be flaky (CloudFront): the dataset page discovery
           falls back to the web.archive.org snapshot, and every failure is
           logged + degraded gracefully (other sources still complete). Rows
           are NEVER fabricated.
  profiles MODELED unit economics for the 6 Finance/Tech verticals x 6 regions
           -> regional_industry_profiles (exactly 36 rows). Margin bands,
           payroll ratio, CAC, utilization and locality burden are MODELED
           industry-norm constants (documented in notes JSONB -- NOT scraped
           data); median_salary_usd is the ONLY harvested input (OEWS annual
           median of the vertical's anchor occupation, region ->
           california_state -> la_msa fallback chain; a cited static fallback
           applies only if the whole wage harvest failed).

INFRASTRUCTURE
  * Pure urllib (no third-party HTTP client); UA "smb-warehouse-fetcher/1.0".
  * Disk cache in <project>/.cache/socal_harvest/: key = sha256(url), raw
    bytes in <key>.bin + a <key>.json sidecar {url, fetched_at, content_type}.
    TTLs: OEWS/CBP/ZBP 30 days, FRED 1 day, SBA 7 days, HTML pages 7 days.
    --refresh bypasses the cache entirely.
  * Rate-limit backoff: on HTTP 429/5xx/timeout, retry up to 4 times with
    exponential backoff 2s -> 8s -> 30s -> 60s, honoring Retry-After. 404s
    fail immediately (they are deterministic). A network failure for one
    source never aborts the others.
  * Every (source attempt, url) is appended to socal_harvest_log with the
    run id, status ok/failed/cache_hit, http status, rows loaded and a
    message (includes the 2080-hour wage-consistency warnings).
  * Every warehouse row's `source` string = "<human description>,
    retrieved <date> from <url>" so any row can be audited back to the
    exact file it came from.

Run:
    python3 harvest_socal_fintech.py                     # all six sources
    python3 harvest_socal_fintech.py --sources oews,fred --dry-run
    python3 harvest_socal_fintech.py --refresh           # bypass disk cache
"""

import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import os
import re
import shutil
import time
import urllib.error
import urllib.request
import uuid
import zipfile
from collections import defaultdict
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Honors DATABASE_URL (used by the Docker stack: postgresql://postgres:postgres@db:5432/...)
DB = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/smb_warehouse")

PROJECT_DIR = Path(__file__).resolve().parent
CACHE_DIR = PROJECT_DIR / ".cache" / "socal_harvest"
USER_AGENT = "smb-warehouse-fetcher/1.0"
RETRIEVED = dt.date.today().isoformat()      # stamped into every `source` string

# Disk-cache TTLs in days, per source family (see module docstring).
TTL_DAYS = {"oews": 30, "cbp": 30, "zbp": 30, "fred": 1, "sba": 7, "html": 7}

# ---------------------------------------------------------------------------
# FIXED VOCABULARY (must match the CHECK constraints in schema.sql)
# ---------------------------------------------------------------------------
REGIONS = {          # region_code -> (human name, source geography)
    "la_msa":            "Los Angeles-Long Beach-Anaheim, CA MSA",
    "oxnard_ventura_msa": "Oxnard-Thousand Oaks-Ventura, CA MSA",
    "orange_county":     "Santa Ana-Anaheim-Irvine, CA Metro Division (Orange County)",
    "san_diego_msa":     "San Diego-Chula Vista-Carlsbad, CA MSA",
    "inland_empire_msa": "Riverside-San Bernardino-Ontario, CA MSA",
    "california_state":  "California statewide",
}
REGION_CODES = list(REGIONS)

# OEWS MSA codes (from oesm24ma.zip AREA column) -> region_code
OEWS_MSA_TO_REGION = {
    "31080": "la_msa",
    "37100": "oxnard_ventura_msa",
    "41740": "san_diego_msa",
    "40140": "inland_empire_msa",
}

# Six SoCal counties: 5-char FIPS -> (region_code, county name)
COUNTIES = {
    "06037": ("la_msa", "Los Angeles County"),
    "06059": ("orange_county", "Orange County"),
    "06111": ("oxnard_ventura_msa", "Ventura County"),
    "06073": ("san_diego_msa", "San Diego County"),
    "06065": ("inland_empire_msa", "Riverside County"),
    "06071": ("inland_empire_msa", "San Bernardino County"),
}
# ZBP cty_name column values (upper-case county names) -> 5-char county FIPS
ZBP_COUNTY_FIPS = {
    "LOS ANGELES": "06037",
    "ORANGE": "06059",
    "VENTURA": "06111",
    "SAN DIEGO": "06073",
    "RIVERSIDE": "06065",
    "SAN BERNARDINO": "06071",
}

# Task-facing role label -> BLS OEWS occupation code (all verified present in
# the May 2024 files). The dashboard/export show role_label; occupation_code
# is the statistical join key.
ROLES = [
    ("Software Engineer", "15-1252"),   # Software Developers
    ("Data Analyst",      "15-2051"),   # Data Scientists (OEWS title)
    ("Financial Analyst", "13-2051"),   # Financial and Investment Analysts
    ("Accountant/CPA",    "13-2011"),   # Accountants and Auditors
    ("IT Manager",        "11-3021"),   # Computer and Information Systems Managers
    ("Bookkeeper",        "43-3031"),   # Bookkeeping, Accounting, and Auditing Clerks
    ("Compliance Officer", "13-1041"),  # Compliance Officers
    ("Sales Engineer",    "41-9031"),   # Sales Engineers
]
ROLE_LABEL_BY_CODE = dict((code, label) for label, code in ROLES)
OEWS_OCC_CODES = set(ROLE_LABEL_BY_CODE)

# Six Finance/Tech verticals: modeled constants (industry norms 2025-2026 --
# NOT scraped data; documented again in the notes JSONB written per row).
#   margin, p25, p75 : gross margin % band (mid, low, high)
#   payroll_ratio    : payroll as a fraction of revenue
#   cac              : customer acquisition cost, USD
#   utilization      : target billable utilization % of paid hours
#   anchor           : the vertical's anchor OEWS occupation code
VERTICALS = [
    dict(key="wealth_advisory",   name="Boutique Wealth/Advisory Firm",
         margin=42, p25=36, p75=48, payroll_ratio=0.35, cac=3500,
         utilization=75, anchor="13-2051"),
    dict(key="b2b_saas",          name="B2B SaaS Startup",
         margin=78, p25=70, p75=84, payroll_ratio=0.55, cac=12000,
         utilization=65, anchor="15-1252"),
    dict(key="custom_software_ai", name="Custom Software/AI Agency",
         margin=55, p25=45, p75=65, payroll_ratio=0.60, cac=8000,
         utilization=80, anchor="15-1252"),
    dict(key="fractional_cfo",    name="Fractional CFO/Accounting Practice",
         margin=65, p25=55, p75=75, payroll_ratio=0.45, cac=2500,
         utilization=75, anchor="13-2011"),
    dict(key="fintech_payments",  name="Fintech/Payments Company",
         margin=62, p25=52, p75=72, payroll_ratio=0.50, cac=15000,
         utilization=70, anchor="15-1252"),
    dict(key="managed_it_cyber",  name="Managed IT/Cybersecurity Provider",
         margin=48, p25=40, p75=56, payroll_ratio=0.58, cac=1800,
         utilization=78, anchor="11-3021"),
]

# MODELED regional payroll-burden delta vs the California baseline
# (workers comp / insurance / locality premium; negative = cheaper).
REGION_LOCALITY_BURDEN = {
    "california_state": 0.0,
    "orange_county": 4.0,
    "la_msa": 6.0,
    "san_diego_msa": 3.0,
    "oxnard_ventura_msa": 2.0,
    "inland_empire_msa": -2.0,
}

# Static salary fallback, cited as "fallback pending OEWS" -- used ONLY when
# the whole wage harvest failed (socal_wage_benchmarks has no row for the
# vertical's anchor occupation in any region). Reasonable 2024-era California
# medians; explicitly labeled modeled fallback in notes/source.
SALARY_FALLBACK = {
    "15-1252": 150000, "15-2051": 120000, "13-2051": 115000,
    "13-2011": 99000,  "11-3021": 195000, "43-3031": 52000,
    "13-1041": 85000,  "41-9031": 110000,
}

# NAICS 2022 labels for the levels this harvester keeps (51/52/54 families).
# Codes outside this dict get naics_label NULL (documented degradation).
NAICS_LABELS = {
    "51": "Information",
    # -- NAICS 2017 labels: the CBP 2023 source file's actual vintage. The
    # source file is 2017-coded (software publishers = 511210), so these keys
    # cover the as-published rows.
    "511": "Publishing Industries (except Internet)",
    "5111": "Newspaper, Periodical, Book, and Directory Publishers",
    "511110": "Newspaper Publishers",
    "511120": "Periodical Publishers",
    "511130": "Book Publishers",
    "511140": "Directory Publishers",
    "511191": "Greeting Card Publishers",
    "511199": "All Other Publishers",
    "5112": "Software Publishers",
    "51121": "Software Publishers",
    "511210": "Software Publishers",
    "515": "Broadcasting (except Internet)",
    "5151": "Radio and Television Broadcasting",
    "5152": "Cable and Other Subscription Programming",
    "517": "Telecommunications",
    "518": "Data Processing, Hosting, and Related Services",
    "5182": "Data Processing, Hosting, and Related Services",
    "518210": "Data Processing, Hosting, and Related Services",
    "519": "Other Information Services",
    # -- NAICS 2022 labels for the mirrored 511x -> 513x rows (Census
    # 2017->2022 publishing-family concordance: 513 = Publishing Industries,
    # 5132 = Software Publishers; 518210 stays Data Processing/Hosting under
    # the restructured 2022 subsector 518):
    "513": "Publishing Industries",
    "5131": "Newspaper, Periodical, Book, and Directory Publishers",
    "513110": "Newspaper Publishers",
    "513120": "Periodical Publishers",
    "513130": "Book Publishers",
    "513140": "Directory Publishers",
    "5132": "Software Publishers",
    "51321": "Software Publishers",
    "513210": "Software Publishers",
    "52": "Finance and Insurance",
    "521": "Monetary Authorities-Central Bank",
    "522": "Credit Intermediation and Related Activities",
    "5223": "Activities Related to Credit Intermediation",
    "522310": "Direct Credit Institutions Uninsured by Federal Deposit Insurance",
    "523": "Securities, Commodity Contracts, and Other Financial Investments and Related Activities",
    "524": "Insurance Carriers and Related Activities",
    "525": "Funds, Trusts, and Other Financial Vehicles",
    "54": "Professional, Scientific, and Technical Services",
    "541": "Professional, Scientific, and Technical Services",
    "5412": "Accounting, Tax Preparation, Bookkeeping, and Payroll Services",
    "5413": "Architectural, Engineering, and Related Services",
    "5414": "Specialized Design Services",
    "5415": "Computer Systems Design and Related Services",
    "541511": "Custom Computer Programming Services",
    "541512": "Computer Systems Design Services",
    "5416": "Management, Scientific, and Technical Consulting Services",
    "5417": "Scientific Research and Development Services",
    "5418": "Advertising, Public Relations, and Related Services",
    "5419": "Other Professional, Scientific, and Technical Services",
}

# ---------------------------------------------------------------------------
# disk cache + resilient HTTP
# ---------------------------------------------------------------------------

class HttpError(Exception):
    """HTTP-level failure with the final status code attached."""

    def __init__(self, code, text):
        super().__init__(f"HTTP {code}: {text}")
        self.code = code


def _cache_paths(url):
    key = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{key}.bin", CACHE_DIR / f"{key}.json"


def _fetch_with_backoff(url, timeout):
    """One fresh network fetch with the documented retry policy: on 429/5xx/
    timeout/URLError, retry up to 4 times, sleeping 2s -> 8s -> 30s -> 60s
    (Retry-After overrides the sleep when present). 404 and other 4xx raise
    immediately (deterministic failures are not retried). Returns
    (response_obj, status, is_stream_done) -- caller reads the body."""
    delays = [2, 8, 30, 60]
    last_exc = None
    for attempt, delay in enumerate([0] + delays):        # first try, then 4 retries
        if attempt > 0:
            time.sleep(delay)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            return urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError as exc:
            code = exc.code
            if code == 429 or 500 <= code <= 599:
                last_exc = exc
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                if retry_after and retry_after.strip().isdigit():
                    time.sleep(min(int(retry_after.strip()), 120))
                    continue
                continue                                   # backoff loop handles the sleep
            raise HttpError(code, url) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_exc = exc
            continue
    # all retries exhausted
    raise last_exc if last_exc else HttpError(0, url)


def fetch_cached(url, ttl_days, refresh=False, timeout=120):
    """GET `url` through the disk cache. Returns (payload_bytes, meta, status)
    where status is 'ok' (fresh fetch) or 'cache_hit'. Raises HttpError on
    deterministic failures and the last network exception after retries."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    blob, meta_path = _cache_paths(url)
    if not refresh and blob.exists() and meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text())
            fetched = dt.datetime.fromisoformat(meta["fetched_at"])
            if dt.datetime.now(dt.timezone.utc) - fetched < dt.timedelta(days=ttl_days):
                return blob.read_bytes(), meta, "cache_hit"
        except (ValueError, KeyError, json.JSONDecodeError):
            pass                                           # corrupt sidecar -> refetch
    with _fetch_with_backoff(url, timeout) as resp:
        payload = resp.read()
        meta = {
            "url": url,
            "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "content_type": resp.headers.get("Content-Type"),
            "http_status": resp.status,
        }
    # atomic cache write (temp file in the same dir, then rename)
    tmp = blob.with_suffix(".tmp")
    tmp.write_bytes(payload)
    os.replace(tmp, blob)
    meta_tmp = meta_path.with_suffix(".tmp")
    meta_tmp.write_text(json.dumps(meta))
    os.replace(meta_tmp, meta_path)
    return payload, meta, "ok"


def fetch_cached_stream(url, ttl_days, refresh=False, timeout=180):
    """Like fetch_cached but STREAMS the body to the cache blob (for the
    100-400MB SBA CSVs -- never held in memory whole). Returns the blob path."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    blob, meta_path = _cache_paths(url)
    if not refresh and blob.exists() and meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text())
            fetched = dt.datetime.fromisoformat(meta["fetched_at"])
            if dt.datetime.now(dt.timezone.utc) - fetched < dt.timedelta(days=ttl_days):
                return blob, meta, "cache_hit"
        except (ValueError, KeyError, json.JSONDecodeError):
            pass
    tmp = blob.with_suffix(".tmp")
    try:
        with _fetch_with_backoff(url, timeout) as resp:
            meta = {
                "url": url,
                "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "content_type": resp.headers.get("Content-Type"),
                "http_status": resp.status,
            }
            with open(tmp, "wb") as out:
                shutil.copyfileobj(resp, out, length=8 * 1024 * 1024)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise
    os.replace(tmp, blob)
    meta_tmp = meta_path.with_suffix(".tmp")
    meta_tmp.write_text(json.dumps(meta))
    os.replace(meta_tmp, meta_path)
    return blob, meta, "ok"


def unzip_cached(payload, subdir_name):
    """Extract a cached zip into a FRESH per-run directory under the cache
    (downloaded archives are untrusted data: each lives in its own new,
    empty directory). Returns the extraction dir; the caller removes it."""
    workdir = CACHE_DIR / f"_unzip_{uuid.uuid4().hex[:12]}" / subdir_name
    workdir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        zf.extractall(workdir)
    return workdir


# ---------------------------------------------------------------------------
# harvest audit log (append-only, autocommit so logs survive source failures)
# ---------------------------------------------------------------------------

class HarvestLog:
    """Writes socal_harvest_log rows: one per (run_id, source_name, url).
    Uses its own autocommit connection so audit rows survive even when a
    later source's transaction fails. In --dry-run mode, prints only."""

    def __init__(self, run_id, dry_run):
        self.run_id = run_id
        self.dry_run = dry_run
        self.conn = None
        if not dry_run:
            self.conn = psycopg2.connect(DB)
            self.conn.autocommit = True

    def log(self, source, url, status, http_status, rows_loaded, message):
        message = (message or "")[:2000]
        print(f"    [{status}] {source} rows={rows_loaded} "
              f"http={http_status} :: {message[:160]}")
        if self.conn is None:
            return
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO socal_harvest_log
                       (run_id, source_name, url, status, http_status,
                        rows_loaded, message)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (self.run_id, source, url, status, http_status,
                 rows_loaded, message))

    def close(self):
        if self.conn is not None:
            self.conn.close()


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------

def src(desc, url):
    """Provenance string: '<human description>, retrieved <date> from <url>'."""
    return f"{desc}, retrieved {RETRIEVED} from {url}"


def num_or_none(text):
    """OEWS/CB/ZBP cells: '$'/','-stripped float, or None for the suppression
    markers ('*', '**', '#', 'NA', 'N', '') used by the agencies."""
    if text is None:
        return None
    s = str(text).strip().replace("$", "").replace(",", "")
    if s in ("", "*", "**", "#", "NA", "N", "S", ".", "(S)"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def to_monthly(observations):
    """Average sub-monthly observations into calendar-month values (first-of-
    month dates). Monthly series pass through unchanged (fetch_benchmarks.py
    convention)."""
    buckets = defaultdict(list)
    for d, v in observations:
        buckets[d.replace(day=1)].append(v)
    return {k: sum(v) / len(v) for k, v in buckets.items()}


def execute_upsert(cur, sql, rows, template, page_size=500):
    if rows:
        execute_values(cur, sql, rows, template=template, page_size=page_size)


# ---------------------------------------------------------------------------
# source: oews
# ---------------------------------------------------------------------------

OEWS_DATA_YEAR = 2024
OEWS_URLS = {
    "metro": "https://www.bls.gov/oes/special-requests/oesm24ma.zip",
    "state": "https://www.bls.gov/oes/special-requests/oesm24st.zip",
}
# Columns read from the combined OEWS *_dl.xlsx files (verified layout:
# AREA, AREA_TITLE, AREA_TYPE, PRIM_STATE, NAICS, OWN_CODE, OCC_CODE, OCC_TITLE,
# TOT_EMP, EMP_PRSE, H_PCT25..H_PCT90, A_PCT25..A_PCT90).
OEWS_COLS = ["AREA", "AREA_TITLE", "NAICS", "OWN_CODE", "OCC_CODE", "OCC_TITLE",
             "TOT_EMP", "EMP_PRSE", "H_PCT25", "H_MEDIAN", "H_PCT75", "H_PCT90",
             "A_PCT25", "A_MEDIAN", "A_PCT75", "A_PCT90"]

WAGE_UPSERT = """
    INSERT INTO socal_wage_benchmarks (region_code, occupation_code,
        occupation_title, role_label, data_year,
        hourly_p25, hourly_median, hourly_p75, hourly_p90,
        annual_p25, annual_median, annual_p75, annual_p90,
        employment, employment_rse, source)
    VALUES %s
    ON CONFLICT (region_code, occupation_code, data_year) DO UPDATE SET
        occupation_title = EXCLUDED.occupation_title,
        role_label       = EXCLUDED.role_label,
        hourly_p25       = EXCLUDED.hourly_p25, hourly_median = EXCLUDED.hourly_median,
        hourly_p75       = EXCLUDED.hourly_p75, hourly_p90    = EXCLUDED.hourly_p90,
        annual_p25       = EXCLUDED.annual_p25, annual_median = EXCLUDED.annual_median,
        annual_p75       = EXCLUDED.annual_p75, annual_p90    = EXCLUDED.annual_p90,
        employment       = EXCLUDED.employment, employment_rse = EXCLUDED.employment_rse,
        source           = EXCLUDED.source
"""


def _pick_ownership(rows):
    """For one (AREA, OCC_CODE, NAICS) pick the right ownership row: the
    combined dl files carry cross-ownership rows (OWN_CODE '1235' = 'All');
    if an area ever publishes both an All row and a Private row, All wins."""
    by_pref = {}
    for _, r in rows.iterrows():
        own = str(r.get("OWN_CODE", "")).strip()
        title = str(r.get("OWN_TITLE", "")).strip().lower()
        if own == "1235" or title.startswith("all"):
            pref = 0
        elif own == "4" or title.startswith("private"):
            pref = 1
        else:
            pref = 2
        by_pref.setdefault(pref, r)
    return by_pref[min(by_pref)] if by_pref else None


def _wage_row_from(frame_row, region_code, source_desc, warnings):
    """Turn one OEWS row into the socal_wage_benchmarks upsert tuple, keeping
    wages AS PUBLISHED. Runs the 2080-hour consistency check: OEWS annual
    wages ~= hourly x 2080 (rounded to the nearest $100); deviations > $120
    are collected as warnings (they never change the stored values)."""
    h = [num_or_none(frame_row.get(c)) for c in
         ("H_PCT25", "H_MEDIAN", "H_PCT75", "H_PCT90")]
    a = [num_or_none(frame_row.get(c)) for c in
         ("A_PCT25", "A_MEDIAN", "A_PCT75", "A_PCT90")]
    h_med, a_med = h[1], a[1]
    if h_med is not None and a_med is not None:
        drift = abs(a_med - h_med * 2080)
        if drift > 120:
            warnings.append(
                f"{region_code}/{frame_row['OCC_CODE']}: annual median "
                f"${a_med:,.0f} vs hourly x 2080 ${h_med * 2080:,.0f} "
                f"(drift ${drift:,.0f})")
    emp = num_or_none(frame_row.get("TOT_EMP"))
    emp = int(emp) if emp is not None and emp >= 0 else None
    rse = num_or_none(frame_row.get("EMP_PRSE"))
    occ = str(frame_row.get("OCC_CODE", "")).strip()
    occ_title = (frame_row.get("OCC_TITLE") or "").strip() or None
    return (
        region_code, occ, occ_title, ROLE_LABEL_BY_CODE.get(occ, occ),
        OEWS_DATA_YEAR,
        h[0], h[1], h[2], h[3],
        a[0], a[1], a[2], a[3],
        emp, rse,
        src(source_desc, OEWS_URLS["metro"] if region_code != "california_state"
            and region_code != "orange_county" else OEWS_URLS["state"]),
    )


def harvest_oews(cur, log):
    """Download + parse the May 2024 OEWS metro and state files and upsert
    socal_wage_benchmarks. See the module docstring for the Orange County
    proxy note (the MD 36004 estimate does not exist in the 2024 files)."""
    warnings = []
    rows = []

    # ---- metro file: the four MSA estimates -------------------------------
    payload, meta, status = fetch_cached(OEWS_URLS["metro"], TTL_DAYS["oews"])
    workdir = unzip_cached(payload, "oesm24ma")
    try:
        msa_path = next(workdir.rglob("MSA_M2024_dl.xlsx"))
        df = pd.read_excel(msa_path, sheet_name=0, dtype=str, usecols=OEWS_COLS)
        df["AREA"] = df["AREA"].str.strip().str.zfill(5)
        df["NAICS"] = df["NAICS"].str.strip()
        df["OCC_CODE"] = df["OCC_CODE"].str.strip()
        keep = df[df["AREA"].isin(OEWS_MSA_TO_REGION)
                  & (df["NAICS"] == "000000")
                  & df["OCC_CODE"].isin(OEWS_OCC_CODES)]
        for area, grp in keep.groupby("AREA"):
            region = OEWS_MSA_TO_REGION[area]
            for occ, occ_grp in grp.groupby("OCC_CODE"):
                picked = _pick_ownership(occ_grp)
                if picked is None:
                    continue
                desc = (f"BLS OEWS May {OEWS_DATA_YEAR} metro estimates "
                        f"({REGIONS[region]})")
                rows.append(_wage_row_from(picked, region, desc, warnings))
    finally:
        shutil.rmtree(workdir.parent, ignore_errors=True)
    metro_status, metro_http = status, meta.get("http_status")

    # ---- state file: California statewide ---------------------------------
    payload, meta, status = fetch_cached(OEWS_URLS["state"], TTL_DAYS["oews"])
    workdir = unzip_cached(payload, "oesm24st")
    try:
        st_path = next(workdir.rglob("state_M2024_dl.xlsx"))
        df = pd.read_excel(st_path, sheet_name=0, dtype=str, usecols=OEWS_COLS)
        df["AREA"] = df["AREA"].str.strip().str.zfill(2)
        df["NAICS"] = df["NAICS"].str.strip()
        df["OCC_CODE"] = df["OCC_CODE"].str.strip()
        keep = df[(df["AREA"] == "06") & (df["NAICS"] == "000000")
                  & df["OCC_CODE"].isin(OEWS_OCC_CODES)]
        for occ, occ_grp in keep.groupby("OCC_CODE"):
            picked = _pick_ownership(occ_grp)
            if picked is None:
                continue
            rows.append(_wage_row_from(
                picked, "california_state",
                f"BLS OEWS May {OEWS_DATA_YEAR} state estimates (California)",
                warnings))
    finally:
        shutil.rmtree(workdir.parent, ignore_errors=True)
    state_status, state_http = status, meta.get("http_status")

    # ---- orange_county proxy rows (documented; MD 36004 not published) ----
    proxy_desc = (f"BLS OEWS May {OEWS_DATA_YEAR} California statewide values "
                  f"used as documented PROXY for Orange County: the Santa "
                  f"Ana-Anaheim-Irvine Metro Division (36004) is not "
                  f"published in the May 2024 OEWS files")
    for row in [r for r in rows if r[0] == "california_state"]:
        proxy = list(row)
        proxy[0] = "orange_county"
        proxy[15] = src(proxy_desc, OEWS_URLS["state"])
        rows.append(tuple(proxy))

    if log.conn is not None and rows:
        execute_upsert(cur, WAGE_UPSERT, rows,
                       template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                "%s, %s, %s, %s, %s, %s)")
    msg = (f"{len(rows)} wage rows loaded "
           f"(4 MSAs + CA statewide + orange_county statewide-proxy; "
           f"MD 36004 not published in May 2024)")
    if warnings:
        msg += f"; 2080-consistency warnings: " + "; ".join(warnings[:6])
    for status_, http_, url in [
            (metro_status, metro_http, OEWS_URLS["metro"]),
            (state_status, state_http, OEWS_URLS["state"])]:
        log.log("oews", url, status_, http_, len(rows) if status_ == "ok" else 0,
                msg if status_ == "ok" else "OEWS file failed")
    return len(rows)


# ---------------------------------------------------------------------------
# source: fred
# ---------------------------------------------------------------------------

FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
CPI_PROXY_NOTE = ("US CPI-U used as documented proxy: CA has no statewide CPI, "
                  "and Oxnard-Ventura / Orange County have no metro CPI series")

# Current SoCal METRO CPIs, loaded straight from BLS's public time-series
# file (FRED does not mirror the post-2018 BLS metro CPI ids -- see the
# module docstring). Series ids + base months verified against
# download.bls.gov/pub/time.series/cu/cu.series on 2026-11-21.
BLS_CPI_URL = "https://download.bls.gov/pub/time.series/cu/cu.data.1.AllItems"
BLS_CPI_SERIES = [
    ("CUURS49ASA0", "la_msa", "Index 1982-84=100",
     "CPI-U All Items, Los Angeles-Long Beach-Anaheim, CA (NSA, BLS)"),
    ("CUURS49ESA0", "san_diego_msa", "Index 1982-84=100",
     "CPI-U All Items, San Diego-Carlsbad, CA (NSA, bimonthly, BLS)"),
    ("CUURS49CSA0", "inland_empire_msa", "Index Dec 2017=100",
     "CPI-U All Items, Riverside-San Bernardino-Ontario, CA (NSA, BLS)"),
]

# Every series: (series_id, region_code, category, unit, series_name).
# region 'california_state' rows include US-national baselines (documented in
# series_name). The optional/attempt list at the bottom tries once each and
# logs 404s without failing the run.
FRED_SERIES = [
    # CPI-U statewide baseline + documented proxies for regions without
    # their own metro series
    ("CPIAUCSL", "california_state", "cpi",
     "Index 1982-84=100, US CPI-U as CA proxy",
     "US CPI-U, All Items (US) -- " + CPI_PROXY_NOTE),
    ("CPIAUCSL", "oxnard_ventura_msa", "cpi",
     "Index 1982-84=100, US CPI-U as CA proxy",
     "US CPI-U, All Items (US) -- " + CPI_PROXY_NOTE),
    ("CPIAUCSL", "orange_county", "cpi",
     "Index 1982-84=100, US CPI-U as CA proxy",
     "US CPI-U, All Items (US) -- " + CPI_PROXY_NOTE),
    # Interest rates (US national baselines under california_state)
    ("PRIME", "california_state", "interest_rate", "percent",
     "Bank Prime Loan Rate (US, published by FRED only on rate-change "
     "dates -- step series; months without a Fed move have no observation)"),
    ("FEDFUNDS", "california_state", "interest_rate", "percent",
     "Effective Federal Funds Rate (US)"),
    ("MORTGAGE30US", "california_state", "interest_rate", "percent",
     "30-Year Fixed Rate Mortgage Average (US, weekly averaged to monthly)"),
    # Real estate / office-lease cost (US national PPI industry series;
    # base months verified on fred.stlouisfed.org 2026-11-21)
    ("PCU531120531120", "california_state", "real_estate", "Index Dec 1995=100",
     "PPI Lessors of Nonresidential Buildings (US) -- office-landlord price "
     "index, the closest official office-lease-cost proxy"),
    ("PCU531210531210", "california_state", "real_estate", "Index Dec 2003=100",
     "PPI Offices of Real Estate Agents and Brokers (US)"),
    # Employment (thousands; US/CA baselines under california_state)
    ("USFIRE", "california_state", "employment", "thousands of employees",
     "Financial activities employment, US (CES)"),
    ("USPBS", "california_state", "employment", "thousands of employees",
     "Professional and business services employment, US (CES)"),
    ("USINFO", "california_state", "employment", "thousands of employees",
     "Information employment, US (CES)"),
    ("CAINFON", "california_state", "employment", "thousands of employees",
     "Information employment, California (CES)"),
    ("CAFIREN", "california_state", "employment", "thousands of employees",
     "Financial activities employment, California (CES)"),
    ("SMU06310805500000001", "la_msa", "employment", "thousands of employees",
     "Financial activities employment, Los Angeles-Long Beach-Anaheim, CA MSA (CES)"),
    ("SMU06310805000000001", "la_msa", "employment", "thousands of employees",
     "Information employment, Los Angeles-Long Beach-Anaheim, CA MSA (CES)"),
    ("SMU06310806000000001", "la_msa", "employment", "thousands of employees",
     "Professional and business services employment, Los Angeles-Long "
     "Beach-Anaheim, CA MSA (CES)"),
]

# Optional regional series (verified 404 in pre-flight -- FRED does not
# mirror the current BLS ids; one attempt each, log and move on).
FRED_OPTIONAL = [
    ("CUURS49ASEHA", "la_msa", "cpi", "Index 1982-84=100",
     "CPI Rent of Primary Residence, Los Angeles-Long Beach-Anaheim, CA (NSA)"),
    ("SMU06417405500000001", "san_diego_msa", "employment", "thousands of employees",
     "Financial activities employment, San Diego-Chula Vista-Carlsbad, CA MSA (CES)"),
    ("SMU06401405500000001", "inland_empire_msa", "employment", "thousands of employees",
     "Financial activities employment, Riverside-San Bernardino-Ontario, CA MSA (CES)"),
    ("SMU06371005500000001", "oxnard_ventura_msa", "employment", "thousands of employees",
     "Financial activities employment, Oxnard-Thousand Oaks-Ventura, CA MSA (CES)"),
]
OPTIONAL_KEYS = {(s[0], s[1]) for s in FRED_OPTIONAL}

MACRO_UPSERT = """
    INSERT INTO socal_macro_indicators (region_code, series_id, series_name,
        category, period, value, unit, source)
    VALUES %s
    ON CONFLICT (region_code, series_id, period) DO UPDATE SET
        series_name = EXCLUDED.series_name,
        category    = EXCLUDED.category,
        value       = EXCLUDED.value,
        unit        = EXCLUDED.unit,
        source      = EXCLUDED.source
"""


def _fred_parse_series(payload):
    """Parse a fredgraph.csv body -> [(date, value)] skipping '.' gaps."""
    out = []
    text = payload.decode("utf-8", errors="replace")
    for row in csv.reader(io.StringIO(text)):
        if not row or row[0].strip().lower() in ("observation_date", "date"):
            continue
        if row[1].strip() in (".", ""):          # FRED marks missing data '.'
            continue
        try:
            out.append((dt.date.fromisoformat(row[0].strip()), float(row[1])))
        except (ValueError, IndexError):
            continue
    return out


def _fred_load_one(cur, log, series_id, region, category, unit, name,
                   window_start, dry_rows):
    """Fetch+upsert one (series, region) pair. Returns rows written."""
    url = FRED_CSV.format(series_id=series_id)
    try:
        payload, meta, status = fetch_cached(url, TTL_DAYS["fred"])
    except HttpError as exc:
        label = ("optional series" if (series_id, region) in OPTIONAL_KEYS
                 else "series")
        log.log("fred", url, "failed", exc.code, 0,
                f"{label} unavailable: {exc}")
        return 0
    except Exception as exc:                      # noqa: BLE001 - network death
        log.log("fred", url, "failed", None, 0, f"network failure: {exc}")
        return 0
    monthly = {p: v for p, v in to_monthly(_fred_parse_series(payload)).items()
               if p >= window_start and v > 0}    # CHECK: value > 0
    if not monthly:
        log.log("fred", url, status, meta.get("http_status"), 0,
                "no usable observations in window")
        return 0
    rows = [(region, series_id, name, category,
             p.isoformat(), round(v, 4), unit,
             src(f"FRED series {series_id} ({name}); underlying agency: BLS/FRB",
                 url))
            for p, v in sorted(monthly.items())]
    dry_rows.extend(rows)
    periods = sorted(monthly)
    log.log("fred", url, status, meta.get("http_status"), len(rows),
            f"{len(rows)} monthly rows {periods[0]}..{periods[-1]}")
    return len(rows)


def _bls_cpi_load(cur, log, window_start, dry_rows):
    """Load the three current SoCal metro CPI series from BLS's public
    tab-delimited time-series file (FRED doesn't mirror the post-2018
    metro CPI ids). File layout (verified 2026-11-21): whitespace-padded
    tab-separated columns series_id | year | period (M01..M13) | value |
    footnote_codes; bimonthly series leave non-publishing months EMPTY (no
    '.' markers here)."""
    try:
        payload, meta, status = fetch_cached(BLS_CPI_URL, TTL_DAYS["fred"])
    except Exception as exc:                       # noqa: BLE001 - degrade
        log.log("fred", BLS_CPI_URL, "failed", None, 0,
                f"BLS CPI file unreachable: {exc}")
        return 0
    want = {sid: (region, unit, name)
            for sid, region, unit, name in BLS_CPI_SERIES}
    rows = []
    per_series = defaultdict(int)
    for line in payload.decode("utf-8", errors="replace").splitlines()[1:]:
        fields = [f.strip() for f in line.split("\t")]
        if len(fields) < 4 or fields[0] not in want:
            continue
        series_id, year_s, period, value_s = fields[0], fields[1], fields[2], fields[3]
        if not (year_s.isdigit() and period.startswith("M")):
            continue
        month = int(period[1:])
        if not 1 <= month <= 12:                   # M13 = annual average: skip
            continue
        try:
            value = float(value_s)
        except ValueError:
            continue                               # empty (bimonthly gap) / junk
        if value <= 0:                             # CHECK: value > 0
            continue
        period_date = dt.date(int(year_s), month, 1)
        if period_date < window_start:
            continue
        region, unit, name = want[series_id]
        rows.append((region, series_id, name, "cpi",
                     period_date.isoformat(), round(value, 4), unit,
                     src(f"BLS CPI time-series file, series {series_id} ({name})",
                         BLS_CPI_URL)))
        per_series[series_id] += 1
    if log.conn is not None and rows:
        dry_rows.extend(rows)
    breakdown = ", ".join(f"{sid}={per_series[sid]}" for sid, _, _, _ in BLS_CPI_SERIES)
    log.log("fred", BLS_CPI_URL, status, meta.get("http_status"), len(rows),
            f"metro CPI rows from BLS time-series file ({breakdown})")
    return len(rows)


def harvest_fred(cur, log, years):
    """Fetch all pinned FRED series (+ one attempt on the optional regional
    ones, + the three current metro CPIs from BLS's time-series file) into
    socal_macro_indicators."""
    window_start = (dt.date.today().replace(day=1)
                    - dt.timedelta(days=365 * years)).replace(day=1)
    rows_written = 0
    dry_rows = []
    for series_id, region, category, unit, name in FRED_SERIES + FRED_OPTIONAL:
        rows_written += _fred_load_one(cur, log, series_id, region, category,
                                       unit, name, window_start, dry_rows)
    rows_written += _bls_cpi_load(cur, log, window_start, dry_rows)
    if log.conn is not None and dry_rows:
        execute_upsert(cur, MACRO_UPSERT, dry_rows,
                       template="(%s, %s, %s, %s, %s, %s, %s, %s)")
    return rows_written


# ---------------------------------------------------------------------------
# source: cbp (county) and zbp (zip)
# ---------------------------------------------------------------------------

CBP_URL = "https://www2.census.gov/programs-surveys/cbp/datasets/2023/cbp23co.zip"
ZBP_URL = "https://www2.census.gov/programs-surveys/cbp/datasets/2023/zbp23detail.zip"
CBP_YEAR = 2023
CBP_DATA_YEAR = 2023

# VERIFIED record layout (records-layouts/2020_record_layouts/county-layout-
# 2020.txt, which the Census page documents as applying through 2023):
#   comma-delimited WITH a quoted header row; fields:
#   fipstate, fipscty, naics, emp_nf, emp, qp1_nf, qp1, ap_nf, ap, est,
#   n<5 .. n1000_4.
#   * emp/ap are PUBLISHED WITH NOISE for every row (since 2022 CBP injects
#     noise instead of suppressing); _nf fields are noise-LEVEL flags
#     (G <2%, H 2-<5%, J >=5%) stored verbatim in employment_flag.
#   * AP is annual payroll in $1,000 -> stored x 1000.
#   * NAICS codes use trailing '-' (2-digit) or '/' (3-6 digit) fillers.
BUSINESS_UPSERT = """
    INSERT INTO socal_business_patterns (geo_level, geo_fips, geo_name,
        region_code, naics_code, naics_label, data_year, establishments,
        employment, employment_flag, annual_payroll_usd,
        payroll_per_employee, source)
    VALUES %s
    ON CONFLICT (geo_level, geo_fips, naics_code, data_year) DO UPDATE SET
        geo_name            = EXCLUDED.geo_name,
        region_code         = EXCLUDED.region_code,
        naics_label         = EXCLUDED.naics_label,
        establishments      = EXCLUDED.establishments,
        employment          = EXCLUDED.employment,
        employment_flag     = EXCLUDED.employment_flag,
        annual_payroll_usd  = EXCLUDED.annual_payroll_usd,
        payroll_per_employee = EXCLUDED.payroll_per_employee,
        source              = EXCLUDED.source
"""


def _naics_keep(code):
    """Strip the trailing '-'/'/' fillers and decide whether the row is in
    scope: NAICS 2022 families 51 / 52 / 54 at any published level (2-6
    digit). Returns the cleaned code or None."""
    code = str(code).strip().strip("-/")
    if not code or len(code) < 2 or len(code) > 6:
        return None
    if code[:2] in ("51", "52", "54"):
        return code
    return None


def _naics2022_mirror(code):
    """NAICS 2017 -> 2022 crosswalk for the publishing family. The CBP 2023
    source file is NAICS 2017-coded (verified in the data: software
    publishers appear as 511210), but the task vocabulary and downstream
    analytics use the 2022 code 5132 (Software Publishers). Census's
    2017->2022 concordance maps the publishing family mechanically:
    511 -> 513 (5112 -> 5132, 511210 -> 513210, ...), so every kept code
    starting with '511' additionally gets a mirrored row under the 2022
    code. Returns the mirrored code, or None for other families."""
    if code.startswith("511"):
        return "513" + code[3:]
    return None


def _int_or_none(text):
    """CBP numerics: blank/unparseable/negative -> NULL (never a sentinel)."""
    v = num_or_none(text)
    return int(v) if v is not None else None


def _bp_row(geo_level, geo_fips, geo_name, region_code, naics_code, est, emp,
            emp_flag, ap_thousands, source_desc):
    """Assemble one socal_business_patterns upsert tuple (shared by CBP and
    ZBP; PAYANN/AP x 1000 conversion happens here, payroll/employee only when
    both are valid)."""
    payroll = ap_thousands * 1000.0 if ap_thousands is not None else None
    per_emp = (round(payroll / emp, 2)
               if payroll is not None and emp is not None and emp > 0 else None)
    return (geo_level, geo_fips, geo_name, region_code, naics_code,
            NAICS_LABELS.get(naics_code), CBP_DATA_YEAR, est, emp, emp_flag,
            payroll, per_emp, src(source_desc,
                                  CBP_URL if geo_level == "county" else ZBP_URL))


def harvest_cbp(cur, log):
    """County Business Patterns 2023 for the six SoCal counties."""
    payload, meta, status = fetch_cached(CBP_URL, TTL_DAYS["cbp"])
    if status == "failed":
        return 0
    rows = []
    skipped_other_naics = 0
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        inner = next(n for n in zf.namelist() if n.endswith(".txt"))
        with zf.open(inner) as fh:
            rdr = csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8"))
            for row in rdr:
                if row.get("fipstate") != "06":
                    continue
                county_fips = "06" + str(row.get("fipscty", "")).strip().zfill(3)
                if county_fips not in COUNTIES:
                    continue
                code = _naics_keep(row.get("naics", ""))
                if code is None:
                    skipped_other_naics += 1
                    continue
                region_code, county_name = COUNTIES[county_fips]
                est = _int_or_none(row.get("est"))
                if est is None or est < 0:
                    est = 0
                emp = _int_or_none(row.get("emp"))
                emp = None if (emp is None or emp < 0) else emp
                ap = num_or_none(row.get("ap"))
                ap = None if (ap is None or ap < 0) else ap
                emp_flag = (row.get("emp_nf") or "").strip() or None
                rows.append(_bp_row(
                    "county", county_fips, county_name, region_code, code,
                    est, emp, emp_flag, ap,
                    f"US Census County Business Patterns {CBP_YEAR}, county file "
                    f"(NAICS 2017 codes as published, noise-adjusted "
                    f"employment/payroll) -- {county_name}"))
                # NAICS 2017 -> 2022 publishing-family mirror (511x -> 513x)
                # so the task-vocabulary code 5132 Software Publishers is
                # queryable alongside the as-published 5112 rows.
                mirror = _naics2022_mirror(code)
                if mirror:
                    rows.append(_bp_row(
                        "county", county_fips, county_name, region_code,
                        mirror, est, emp, emp_flag, ap,
                        f"US Census County Business Patterns {CBP_YEAR}, county "
                        f"file, NAICS 2017->2022 crosswalk (Census publishing-"
                        f"family concordance 511->513, 5112->5132 Software "
                        f"Publishers; source file is NAICS 2017-coded) -- "
                        f"{county_name}"))
    if log.conn is not None and rows:
        execute_upsert(cur, BUSINESS_UPSERT, rows,
                       template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                "%s, %s, %s)")
    log.log("cbp", CBP_URL, status, meta.get("http_status"), len(rows),
            f"{len(rows)} county rows for 6 SoCal counties, NAICS 51/52/54 "
            f"(incl. NAICS 2017->2022 publishing mirrors 511x->513x so 5132 "
            f"Software Publishers is queryable; {skipped_other_naics} "
            f"out-of-scope NAICS rows in scope counties skipped); AP stored "
            f"x 1000 as dollars; noise flags kept verbatim in employment_flag")
    return len(rows)


def harvest_zbp(cur, log):
    """ZIP Business Patterns 2023 for ZIPs in the six SoCal counties. ZIP ->
    county attribution uses the file's OWN city/state/cty_name columns (the
    modern ZBP detail layout includes them), so no external zip->county
    dataset is required; the modern ZBP detail file publishes NO employment
    or payroll (those columns stay NULL)."""
    payload, meta, status = fetch_cached(ZBP_URL, TTL_DAYS["zbp"])
    if status == "failed":
        return 0
    rows = []
    zips_seen = set()
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        inner = next(n for n in zf.namelist() if n.endswith(".txt"))
        with zf.open(inner) as fh:
            rdr = csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8"))
            for row in rdr:
                if row.get("stabbr") != "CA":
                    continue
                county_fips = ZBP_COUNTY_FIPS.get(
                    (row.get("cty_name") or "").strip().upper())
                if county_fips is None:
                    continue                     # non-SoCal CA zip
                code = _naics_keep(row.get("naics", ""))
                if code is None:
                    continue
                zip_code = str(row.get("zip", "")).strip().zfill(5)
                region_code, county_name = COUNTIES[county_fips]
                est = _int_or_none(row.get("est"))
                if est is None or est < 0:
                    est = 0
                city = (row.get("city") or "").strip().title()
                rows.append(_bp_row(
                    "zip", zip_code, f"{city}, CA ({county_name})", region_code,
                    code, est, None, None, None,
                    f"US Census ZIP Business Patterns {CBP_YEAR}, zip-by-NAICS "
                    f"detail file (establishments only; county attribution "
                    f"from the file's cty_name column)"))
                # Same NAICS 2017 -> 2022 publishing-family mirror as CBP.
                mirror = _naics2022_mirror(code)
                if mirror:
                    rows.append(_bp_row(
                        "zip", zip_code, f"{city}, CA ({county_name})",
                        region_code, mirror, est, None, None, None,
                        f"US Census ZIP Business Patterns {CBP_YEAR}, "
                        f"zip-by-NAICS detail file, NAICS 2017->2022 crosswalk "
                        f"(511->513, 5112->5132 Software Publishers; "
                        f"establishments only)"))
                zips_seen.add(zip_code)
    if log.conn is not None and rows:
        execute_upsert(cur, BUSINESS_UPSERT, rows,
                       template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                "%s, %s, %s)")
    log.log("zbp", ZBP_URL, status, meta.get("http_status"), len(rows),
            f"{len(rows)} zip rows across {len(zips_seen)} SoCal ZIPs "
            f"(region mapped from the file's own cty_name column; incl. NAICS "
            f"2017->2022 publishing mirrors 511x->513x; ZBP detail publishes "
            f"establishments only -- employment/payroll NULL)")
    return len(rows)


# ---------------------------------------------------------------------------
# source: sba
# ---------------------------------------------------------------------------

SBA_DATASET_URL = "https://data.sba.gov/dataset/7a-504-foia"
SBA_ARCHIVE_URL = "https://web.archive.org/web/2026/" + SBA_DATASET_URL
SBA_FISCAL_YEARS = 6          # keep the most recent 6 fiscal years

LOAN_UPSERT = """
    INSERT INTO socal_loan_activity (program, fiscal_year, geo_level, geo_fips,
        geo_name, region_code, loans_count, gross_approval_usd, avg_loan_usd,
        top_lender, top_lender_loans, source)
    VALUES %s
    ON CONFLICT (program, fiscal_year, geo_level, geo_fips) DO UPDATE SET
        geo_name          = EXCLUDED.geo_name,
        region_code       = EXCLUDED.region_code,
        loans_count       = EXCLUDED.loans_count,
        gross_approval_usd = EXCLUDED.gross_approval_usd,
        avg_loan_usd      = EXCLUDED.avg_loan_usd,
        top_lender        = EXCLUDED.top_lender,
        top_lender_loans  = EXCLUDED.top_lender_loans,
        source            = EXCLUDED.source
"""


def _sba_resource_urls(page_html):
    """Pull the FOIA CSV resource URLs out of the dataset page HTML, grouped
    by program; within a program prefer the 'FY..._Present' window with the
    newest as-of stamp (the current full file)."""
    found = defaultdict(list)
    for m in re.findall(r'href="(https://data\.sba\.gov/sites/[^"]*FOIA[^"]*\.csv)"',
                        page_html, re.I):
        url = m.replace("&amp;", "&")
        low = url.lower()
        program = "7a" if "_7a" in low else ("504" if "_504" in low else None)
        if program:
            asof = 0
            m2 = re.search(r"asof_(\d+)", low)
            if m2:
                asof = int(m2.group(1))
            present = "present" in low
            found[program].append((present, asof, url))
    picked = {}
    for program, cands in found.items():
        # sort: 'Present' windows first, then newest as-of stamp, then name
        cands.sort(key=lambda c: (c[0], c[1], c[2]), reverse=True)
        picked[program] = cands[0][2]
    return picked


def _sba_lender_field(program):
    """7(a) FOIA rows name the lender in BankName; 504 rows in CDC_Name (the
    certified development company is the 504 lending entity)."""
    return "BankName" if program == "7a" else "CDC_Name"


def harvest_sba(cur, log):
    """Aggregate SBA 7(a)/504 FOIA loan-level CSVs into socal_loan_activity.
    Streams the big CSVs through the disk cache and parses incrementally.
    Any failure logs 'failed' + degrades gracefully (never fabricates rows)."""
    # 1) discover the resource CSV URLs (live page first, archive fallback)
    urls = None
    for disco_url in (SBA_DATASET_URL, SBA_ARCHIVE_URL):
        try:
            payload, meta, status = fetch_cached(disco_url, TTL_DAYS["html"])
            urls = _sba_resource_urls(payload.decode("utf-8", errors="replace"))
            log.log("sba", disco_url, status, meta.get("http_status"), 0,
                    f"dataset page fetched; resources found: "
                    f"{ {k: v.rsplit('/', 1)[-1] for k, v in urls.items()} }")
            if urls.get("7a") or urls.get("504"):
                break
        except Exception as exc:               # noqa: BLE001 - degrade below
            log.log("sba", disco_url, "failed", None, 0,
                    f"dataset page unreachable ({exc}); "
                    f"{'trying archive.org fallback' if disco_url == SBA_DATASET_URL else 'no source left'}")
            urls = None
    if not urls:
        print("  [DEGRADED] SBA: data.sba.gov unreachable and archive.org has "
              "no snapshot; other sources completed. No socal_loan_activity "
              "rows were written (never fabricated).")
        return 0

    # 2) stream each program's CSV and aggregate per (program, fy, geo)
    #    buckets: ('county', fips) and ('state', '06')
    agg_count = defaultdict(int)
    agg_sum = defaultdict(float)
    lender_counts = defaultdict(lambda: defaultdict(int))
    parse_notes = []
    for program in ("7a", "504"):
        url = urls.get(program)
        if not url:
            parse_notes.append(f"{program}: no resource URL discovered")
            continue
        try:
            blob, meta, status = fetch_cached_stream(url, TTL_DAYS["sba"])
        except Exception as exc:               # noqa: BLE001
            log.log("sba", url, "failed", None, 0,
                    f"{program} CSV unreachable after retries: {exc}")
            continue
        lender_field = _sba_lender_field(program)
        rows_seen = rows_kept = 0
        with open(blob, "r", encoding="utf-8", errors="replace", newline="") as fh:
            rdr = csv.DictReader(fh)
            for row in rdr:
                rows_seen += 1
                if (row.get("ProjectState") or "").strip().upper() != "CA":
                    continue
                try:
                    amount = float(str(row.get("GrossApproval") or "0")
                                   .replace(",", "").strip())
                    fy = int(float(str(row.get("ApprovalFY") or "0").strip()))
                except ValueError:
                    parse_notes.append(f"{program}: unparseable amount/FY row skipped")
                    continue
                if fy <= 0:
                    continue
                rows_kept += 1
                lender = (row.get(lender_field) or "").strip() or "Unknown"
                # all-California state aggregate
                skey = (program, fy, "state", "06")
                agg_count[skey] += 1
                agg_sum[skey] += amount
                lender_counts[skey][lender] += 1
                # SoCal county aggregate (unmatched counties only feed the
                # state row -- they are still CA loans)
                county_fips = ZBP_COUNTY_FIPS.get(
                    (row.get("ProjectCounty") or "").strip().upper())
                if county_fips:
                    ckey = (program, fy, "county", county_fips)
                    agg_count[ckey] += 1
                    agg_sum[ckey] += amount
                    lender_counts[ckey][lender] += 1
        log.log("sba", url, status, meta.get("http_status"), 0,
                f"{program}: {rows_seen} loan rows streamed, {rows_kept} "
                f"California rows aggregated (lender field {lender_field})")

    # 3) keep the most recent ~6 fiscal years present in the data
    if not agg_count:
        print("  [DEGRADED] SBA: no rows parsed. No socal_loan_activity rows "
              "were written (never fabricated).")
        return 0
    top_fys = sorted({fy for (_, fy, _, _) in agg_count}, reverse=True)[:SBA_FISCAL_YEARS]
    rows = []
    for (program, fy, geo_level, geo_fips), count in sorted(agg_count.items()):
        if fy not in top_fys:
            continue
        total = round(agg_sum[(program, fy, geo_level, geo_fips)], 2)
        lenders = lender_counts[(program, fy, geo_level, geo_fips)]
        top_lender = sorted(lenders.items(), key=lambda kv: (-kv[1], kv[0]))[0]
        if geo_level == "county":
            region_code, geo_name = COUNTIES[geo_fips]
        else:
            region_code, geo_name = "california_state", "California"
        rows.append((program, fy, geo_level, geo_fips, geo_name, region_code,
                     count, total, round(total / count, 2),
                     top_lender[0], top_lender[1],
                     src(f"SBA FOIA 7(a)/504 loan-level file (aggregated; "
                         f"lender field {_sba_lender_field(program)})",
                         urls[program])))
    if log.conn is not None and rows:
        execute_upsert(cur, LOAN_UPSERT, rows,
                       template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    msg = (f"{len(rows)} aggregated rows (programs 7a/504, fiscal years "
           f"{min(top_fys)}-{max(top_fys)}, 6 counties + CA state)")
    if parse_notes:
        msg += "; notes: " + "; ".join(parse_notes[:5])
    log.log("sba", SBA_DATASET_URL, "ok", None, len(rows), msg)
    return len(rows)


# ---------------------------------------------------------------------------
# source: profiles (MODELED vertical economics; needs wages first)
# ---------------------------------------------------------------------------

PROFILE_UPSERT = """
    INSERT INTO regional_industry_profiles (vertical_key, vertical_name,
        region_code, anchor_occupation_code, gross_margin_pct,
        gross_margin_p25_pct, gross_margin_p75_pct, payroll_to_revenue_ratio,
        cac_usd, utilization_target_pct, locality_burden_pct,
        median_salary_usd, notes, source)
    VALUES %s
    ON CONFLICT (vertical_key, region_code) DO UPDATE SET
        vertical_name          = EXCLUDED.vertical_name,
        anchor_occupation_code = EXCLUDED.anchor_occupation_code,
        gross_margin_pct       = EXCLUDED.gross_margin_pct,
        gross_margin_p25_pct   = EXCLUDED.gross_margin_p25_pct,
        gross_margin_p75_pct   = EXCLUDED.gross_margin_p75_pct,
        payroll_to_revenue_ratio = EXCLUDED.payroll_to_revenue_ratio,
        cac_usd                = EXCLUDED.cac_usd,
        utilization_target_pct = EXCLUDED.utilization_target_pct,
        locality_burden_pct    = EXCLUDED.locality_burden_pct,
        median_salary_usd      = EXCLUDED.median_salary_usd,
        notes                  = EXCLUDED.notes,
        source                 = EXCLUDED.source
"""


def _median_salary(cur, occupation_code, region_code):
    """Salary derivation chain (documented in notes JSONB): the harvested
    OEWS annual median for the anchor occupation in THIS region (latest
    data_year) -> california_state -> la_msa -> static fallback pending OEWS.
    Returns (salary, provenance_description)."""
    chain = [region_code, "california_state", "la_msa"]
    for probe_region in chain:
        cur.execute(
            """SELECT annual_median, data_year
                 FROM socal_wage_benchmarks
                WHERE region_code = %s AND occupation_code = %s
                  AND annual_median IS NOT NULL
                ORDER BY data_year DESC LIMIT 1""",
            (probe_region, occupation_code))
        hit = cur.fetchone()
        if hit:
            salary, year = hit
            note = (f"harvested OEWS May {year} annual median "
                    f"({ROLE_LABEL_BY_CODE.get(occupation_code, occupation_code)}, "
                    f"{REGIONS[probe_region]}"
                    f"{' -- regional fallback' if probe_region != region_code else ''}"
                    f"{' -- statewide proxy' if probe_region == 'california_state' and region_code == 'orange_county' else ''})")
            return float(salary), note
    fb = SALARY_FALLBACK.get(occupation_code)
    if fb:
        return float(fb), "static fallback pending OEWS (modeled CA-level median)"
    return None, "unavailable"


def load_profiles(cur, log):
    """Write the 6 verticals x 6 regions MODELED economics (36 rows),
    upserting idempotently. Margin bands / payroll ratio / CAC / utilization /
    locality burden are industry-norm constants; the only harvested input is
    median_salary_usd (see _median_salary)."""
    rows = []
    for vert in VERTICALS:
        for region in REGION_CODES:
            if cur is not None:
                salary, salary_note = _median_salary(cur, vert["anchor"], region)
            else:                               # --dry-run: no DB to read wages from
                fb = SALARY_FALLBACK.get(vert["anchor"])
                salary = float(fb) if fb else None
                salary_note = "dry-run salary lookup skipped (static fallback)"
            burden = REGION_LOCALITY_BURDEN[region]
            notes = {
                "modeling_basis": ("Modeled baseline constants; industry norms "
                                   "2025-2026 -- NOT scraped data"),
                "gross_margin_pct": {
                    "value": vert["margin"], "p25": vert["p25"], "p75": vert["p75"],
                    "basis": "modeled band midpoint; p25/p75 published alongside"},
                "payroll_to_revenue_ratio": {"value": vert["payroll_ratio"],
                                             "basis": "modeled"},
                "cac_usd": {"value": vert["cac"], "basis": "modeled"},
                "utilization_target_pct": {"value": vert["utilization"],
                                           "basis": "modeled share of paid hours billable"},
                "locality_burden_pct": {"value": burden,
                                        "basis": "MODELED regional payroll burden vs "
                                                 "CA baseline (workers comp/insurance/"
                                                 "locality premium); 0.0 = baseline"},
                "median_salary_usd": {"value": salary, "source": salary_note},
                "rate_formula": ("hourly_cost = salary/2080 * (1 + locality_burden/"
                                 "100); loaded_cost = hourly_cost/(utilization/100); "
                                 "suggested_billable_rate = loaded_cost/"
                                 "(1 - gross_margin/100) -- see mv_socal_rate_guidance"),
            }
            source_desc = (
                f"Modeled baseline constants for {vert['name']} "
                f"(industry norms 2025-2026, NOT scraped data); median salary "
                f"{salary_note}"
                + (f", retrieved {RETRIEVED} from {OEWS_URLS['state']} / {OEWS_URLS['metro']}"
                   if "fallback pending OEWS" not in salary_note else ""))
            rows.append((vert["key"], vert["name"], region, vert["anchor"],
                         vert["margin"], vert["p25"], vert["p75"],
                         vert["payroll_ratio"], vert["cac"], vert["utilization"],
                         burden, salary, json.dumps(notes), source_desc))
    if log.conn is not None:
        execute_upsert(cur, PROFILE_UPSERT, rows,
                       template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    log.log("profiles", OEWS_URLS["metro"], "ok", None, len(rows),
            "36 modeled vertical x region profiles loaded; median_salary_usd "
            "derived from socal_wage_benchmarks (region -> CA -> LA chain)")
    return len(rows)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

ARGS_YEARS = 6          # --years default (window used by harvest_fred)
CANONICAL_ORDER = ["oews", "fred", "cbp", "zbp", "sba", "profiles"]


def main():
    ap = argparse.ArgumentParser(
        description="Harvest REAL SoCal Finance/Tech benchmark data "
                    "(OEWS wages, FRED macro, Census CBP/ZBP business "
                    "patterns, SBA loans) + write the MODELED vertical "
                    "profiles. See the module docstring.")
    ap.add_argument("--sources", default="oews,fred,cbp,zbp,sba,profiles",
                    help="comma-separated subset of: oews,fred,cbp,zbp,sba,"
                         "profiles (default: all)")
    ap.add_argument("--years", type=int, default=6,
                    help="FRED history window in years (default 6)")
    ap.add_argument("--refresh", action="store_true",
                    help="bypass the disk cache (force fresh downloads)")
    ap.add_argument("--dry-run", action="store_true",
                    help="fetch and parse, but write nothing to the database")
    args = ap.parse_args()

    wanted = [s.strip().lower() for s in args.sources.split(",") if s.strip()]
    unknown = [s for s in wanted if s not in CANONICAL_ORDER]
    if unknown:
        raise SystemExit(f"Unknown source(s): {unknown}. "
                         f"Valid: {CANONICAL_ORDER}")
    order = [s for s in CANONICAL_ORDER if s in wanted]

    run_id = uuid.uuid4().hex
    print(f"=== SoCal Finance/Tech harvest run {run_id} ===")
    print(f"    sources: {order}  window: {args.years}y  "
          f"refresh: {args.refresh}  dry-run: {args.dry_run}")

    log = HarvestLog(run_id, args.dry_run)
    conn = None
    cur = None
    if not args.dry_run:
        conn = psycopg2.connect(DB)
        conn.autocommit = False
        cur = conn.cursor()

    summary = {}
    try:
        for source in order:
            print(f"  -- {source} --")
            rows = 0
            try:
                if source == "oews":
                    rows = harvest_oews(cur, log)
                elif source == "fred":
                    rows = harvest_fred(cur, log, args.years)
                elif source == "cbp":
                    rows = harvest_cbp(cur, log)
                elif source == "zbp":
                    rows = harvest_zbp(cur, log)
                elif source == "sba":
                    rows = harvest_sba(cur, log)
                elif source == "profiles":
                    rows = load_profiles(cur, log)
                if conn is not None:
                    conn.commit()          # one transaction per source
            except Exception as exc:       # noqa: BLE001 - never abort siblings
                if conn is not None:
                    conn.rollback()
                log.log(source, "-", "failed", None, 0,
                        f"source aborted: {type(exc).__name__}: {exc}")
                print(f"  [ABORTED] {source}: {exc}")
            summary[source] = rows

        print("\n=== Summary (rows loaded per source) ===")
        for source in order:
            print(f"  {source:10} {summary.get(source, 0):>6}")
        if args.dry_run:
            print("  (dry run: nothing was written)")
        else:
            print(f"\nAudit trail: socal_harvest_log run_id={run_id}")
            print("Refresh analytics with: "
                  "psql \"$DATABASE_URL\" -c 'SELECT refresh_all_analytics();'")
    finally:
        log.close()
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
