#!/usr/bin/env python3
"""
harvest_socal_capital.py -- harvest REAL Southern-California private-capital
and federal R&D funding events into socal_capital_events (funding_schema.sql)
and refresh socal_county_capital_summary.

STANDALONE MODULE (Sub-agent 1: Capital & Grants Data Harvester). It shares no
code or tables with schema.sql / analytics_views.sql / harvest_socal_fintech.py.

SOURCES (all real, public, free; every row keeps a verifiable source_url)
  sbir        SBIR.gov open award data -- the official bulk CSV
              https://data.www.sbir.gov/mod_awarddatapublic_no_abstract/award_
              data_no_abstract.csv (the www.sbir.gov/api/awards.json endpoint
              is documented at https://www.sbir.gov/api but has been "under
              maintenance" since 2026-10 and returns {"message":"Forbidden"},
              so the bulk file -- the same data the API serves -- is used
              instead). SBIR Phase I / Phase II / STTR awards to companies
              with a California ZIP that maps to one of the six SoCal
              counties. 219,780 award rows, 44,826 of them California.
  edgar-formd SEC EDGAR Form D private-placement filings. The EDGAR
              full-text search API (https://efts.sec.gov/LATEST/search-index)
              is queried per SoCal mailing city (forms=D, filed-date window);
              every hit's primary_doc.xml is fetched from
              https://www.sec.gov/Archives/edgar/data/... and the ISSUER
              ADDRESS inside the XML -- not the search hit -- decides whether
              the filing is SoCal (state=CA + ZIP -> SoCal county). Only
              filings whose industry group / keywords map onto the dataset's
              six Tech & Finance sectors are kept. Form D/A amendments are
              collapsed to the latest filing per (CIK, first-sale month).
  edgar-13f   SEC EDGAR 13F-HR filings by investment managers located in LA /
              Ventura / Orange / San Diego counties (same FTS city technique,
              forms=13F-HR). These are heldINGS disclosures, not raises, so
              they are stored as auxiliary FIRM METADATA in
              data_cache/capital/edgar_13f_metadata.json and exported as
              socal_firm_filings_metadata.csv -- they are NOT inserted into
              socal_capital_events (whose event_type enum covers raises and
              grants only). NOTE: SEC Form ADV filings live in the IAPD
              system, not EDGAR, and have no public bulk API -- they are out
              of scope for this harvest and documented in the data dictionary.
  grants      California state grants via data.ca.gov (California Grants
              Portal Grant Awards). PROBE ONLY: the portal publishes no
              recipient ZIP/city (county-served only) and is dominated by
              government/nonprofit service grants, so it cannot satisfy the
              strict ZIP-level SoCal + sector requirements -- 0 rows are
              loaded and the probe is logged honestly rather than fabricating
              locations or sectors. event_type STATE_GRANT stays supported by
              the schema for a future source that can meet the bar.

INFRASTRUCTURE
  * Disk cache data_cache/capital/: every HTTP response (FTS page, Form D
    XML, 13F XML, bulk CSV, census crosswalk) is cached by URL; reruns are
    offline-fast and --refresh bypasses the cache.
  * Zip->county: US Census 2010 ZCTA-to-county relationship file
    (www2.census.gov/geo/docs/maps-data/data/rel/zcta_county_rel_10.txt),
    ZCTA attributed to the county holding the largest share of its
    population (ZPOPPCT); a curated static fallback covers major cities if
    the census file is unavailable. SoCal = Los Angeles, Orange, Ventura,
    San Diego, Riverside, San Bernardino counties, ZCTAs 90000-93599.
  * Retry/backoff on every request (429/403/5xx/timeout: exponential 2s->
    30s, honoring Retry-After; 404s are deterministic and fail fast). SEC
    traffic is throttled to <10 req/s across worker threads.
  * Strict normalization: ZIPs collapse to 5 digits, amounts strip $/commas,
    dates accept ISO / partial "YYYY-MM" / "YYYY" / "MM/DD/YYYY".
  * Load dedupe: event_id = f"{prefix}-{sha12(event_type|filing_id)}"; the
    table's PRIMARY KEY and UNIQUE(event_type, agency_or_filing_id) make a
    duplicate insert a no-op. Form D/A supersession collapses one offering
    to one row (the most recent filing wins).
  * The materialized view is refreshed at the end of a successful load.

Run:
    python3 harvest_socal_capital.py                       # all sources
    python3 harvest_socal_capital.py --sources sbir        # one source
    python3 harvest_socal_capital.py --refresh             # bypass disk cache
    python3 harvest_socal_capital.py --rebuild             # truncate + reload
    python3 harvest_socal_capital.py --limit 40 --no-load  # smoke run
"""

import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import logging
import os
import re
import sys
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import psycopg2
import requests
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/smb_warehouse")

PROJECT_DIR = Path(__file__).resolve().parent
CACHE_DIR = PROJECT_DIR / "data_cache" / "capital"
LOG_PATH = CACHE_DIR / "run_log.jsonl"
SUMMARY_PATH = CACHE_DIR / "last_run_summary.json"
THIRTEEN_F_METADATA_PATH = CACHE_DIR / "edgar_13f_metadata.json"
CENSUS_PATH = CACHE_DIR / "census" / "zcta_county_rel_10.txt"
CENSUS_URL = ("https://www2.census.gov/geo/docs/maps-data/data/rel/"
              "zcta_county_rel_10.txt")
SBIR_CSV_PATH = CACHE_DIR / "sbir_bulk" / "award_data_no_abstract.csv"
SBIR_CSV_URL = ("https://data.www.sbir.gov/mod_awarddatapublic_no_abstract/"
                "award_data_no_abstract.csv")

# Dataset window: rolling three years back from the harvest date.
WINDOW_END = dt.date.today()
WINDOW_START = WINDOW_END - dt.timedelta(days=3 * 365)

TARGET_COUNTIES = ("Los Angeles", "Orange", "Ventura", "San Diego",
                   "Riverside", "San Bernardino")
TARGET_COUNTY_FIPS = {
    "Los Angeles": "06037",
    "Orange": "06059",
    "Ventura": "06111",
    "San Diego": "06073",
    "Riverside": "06065",
    "San Bernardino": "06071",
}

SECTORS = ("Fintech", "Enterprise_SaaS", "AI_ML", "Defense_Aerospace_Tech",
           "Biotech_Medtech", "Financial_Services")
EVENT_TYPES = ("SEC_FORM_D", "SBIR_PHASE_1", "SBIR_PHASE_2", "STTR",
               "STATE_GRANT")

EVENT_ID_PREFIX = {
    "SEC_FORM_D": "FORMD",
    "SBIR_PHASE_1": "SBIR-P1",
    "SBIR_PHASE_2": "SBIR-P2",
    "STTR": "STTR",
    "STATE_GRANT": "GRANT",
}

FTS_BASE = "https://efts.sec.gov/LATEST/search-index"
EDGAR_ARCHIVES = "https://www.sec.gov/Archives/edgar/data"
SBIR_AWARDS_PORTAL_URL = "https://www.sbir.gov/awards"
GRANTS_PORTAL_SEARCH = ("https://data.ca.gov/api/3/action/package_search"
                        "?q=California%20Grants%20Portal%20Grant%20Awards"
                        "&rows=10")

SEC_UA = ("SoCalCapitalResearch/1.0 (private-capital dataset harvest; "
          "contact: research@example.org)")
SBIR_UA = "Mozilla/5.0 (compatible; socal-capital-research/1.0)"
CENSUS_UA = "Mozilla/5.0 (compatible; socal-capital-research/1.0)"
CA_DATA_UA = "Mozilla/5.0 (compatible; socal-capital-research/1.0)"

# ---------------------------------------------------------------------------
# Logging / run journal
# ---------------------------------------------------------------------------

log = logging.getLogger("socal_capital")
_RUN_ID = f"{dt.datetime.now():%Y%m%dT%H%M%S}"
_LOG_LOCK = threading.Lock()


def journal(step, status, message, **extra):
    """Append one structured entry to data_cache/capital/run_log.jsonl."""
    entry = {"run_id": _RUN_ID,
             "ts": dt.datetime.now().isoformat(timespec="seconds"),
             "step": step, "status": status, "message": message}
    entry.update(extra)
    try:
        with _LOG_LOCK:
            with open(LOG_PATH, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError:
        pass


# ---------------------------------------------------------------------------
# HTTP: disk cache + retry/backoff + polite rate limiting
# ---------------------------------------------------------------------------

class RateLimiter:
    """Token interval limiter shared across worker threads."""

    def __init__(self, min_interval):
        self.min_interval = min_interval
        self._lock = threading.Lock()
        self._next_ok = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            delta = self._next_ok - now
            self._next_ok = max(now, self._next_ok) + self.min_interval
        if delta > 0:
            time.sleep(delta)


RATE = RateLimiter(0.12)          # <= ~8 req/s across all threads
RETRY_DELAYS = (2, 4, 8, 16, 30)
_RETRYABLE = {403, 408, 425, 429, 500, 502, 503, 504}


def _cache_file(url, cache_path, content):
    if cache_path is not None:
        return Path(cache_path)
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()
    suffix = ".json"
    if ".xml" in url.lower():
        suffix = ".xml"
    elif url.lower().endswith(".csv") or ".csv" in url.lower():
        suffix = ".csv"
    elif url.lower().endswith(".txt"):
        suffix = ".txt"
    return CACHE_DIR / "http" / f"{digest}{suffix}"


def fetch(url, *, cache_path=None, headers=None, refresh=False,
          timeout=60, session=None):
    """GET url with disk caching, polite throttling and retry/backoff.

    Returns the raw response text. Cached responses are served without any
    network traffic; --refresh (or refresh=True) bypasses the cache.
    """
    dest = _cache_file(url, cache_path, None)
    if not refresh and dest.exists():
        return dest.read_text(encoding="utf-8", errors="replace")

    dest.parent.mkdir(parents=True, exist_ok=True)
    hdrs = {"User-Agent": SEC_UA, "Accept-Encoding": "gzip"}
    if headers:
        hdrs.update(headers)
    sess = session or requests.Session()

    last_err = None
    for attempt, delay in enumerate(RETRY_DELAYS):
        try:
            RATE.wait()
            resp = sess.get(url, headers=hdrs, timeout=timeout)
            if resp.status_code == 200:
                text = resp.text
                dest.write_text(text, encoding="utf-8")
                journal("http", "ok", url[:180], bytes=len(text))
                return text
            if resp.status_code == 404:
                raise RuntimeError(f"HTTP 404 (deterministic): {url}")
            if resp.status_code in _RETRYABLE:
                wait_for = delay
                ra = resp.headers.get("Retry-After")
                if ra and ra.isdigit():
                    wait_for = max(delay, int(ra))
                last_err = f"HTTP {resp.status_code}"
                journal("http", "retry", url[:180], http_status=resp.status_code,
                        attempt=attempt + 1, sleep=wait_for)
                time.sleep(wait_for)
                continue
            raise RuntimeError(f"HTTP {resp.status_code}: {url}")
        except requests.RequestException as exc:
            last_err = f"{type(exc).__name__}: {exc}"
            journal("http", "retry", url[:180], attempt=attempt + 1,
                    sleep=delay, error=str(exc)[:200])
            time.sleep(delay)
    raise RuntimeError(f"download failed after retries ({last_err}): {url}")


# ---------------------------------------------------------------------------
# Zip -> county mapping (Census ZCTA crosswalk, curated fallback)
# ---------------------------------------------------------------------------

# Offline fallback: curated high-confidence ZIPs for the six counties. Used
# only when the census crosswalk cannot be downloaded AND is not cached; the
# census file (cached under data_cache/capital/census/) is the authority.
FALLBACK_ZIP_COUNTY = {
    # --- Los Angeles -------------------------------------------------------
    **{z: "Los Angeles" for z in (
        "90001 90002 90003 90004 90005 90006 90007 90008 90011 90012 90013 "
        "90015 90016 90017 90018 90019 90020 90021 90022 90023 90024 90025 "
        "90026 90027 90028 90029 90031 90032 90033 90034 90035 90036 90037 "
        "90039 90041 90042 90043 90044 90045 90046 90047 90049 90056 90057 "
        "90058 90059 90061 90062 90063 90064 90065 90066 90067 90068 90071 "
        "90077 90089 90201 90210 90211 90212 90230 90232 90240 90241 90242 "
        "90245 90247 90248 90249 90250 90254 90255 90260 90262 90265 90266 "
        "90272 90274 90275 90277 90278 90280 90290 90292 90293 90301 90302 "
        "90303 90304 90305 90401 90402 90403 90404 90405 90501 90502 90503 "
        "90504 90505 90506 90601 90602 90603 90604 90605 90606 90638 90640 "
        "90650 90660 90670 90701 90703 90706 90707 90712 90713 90715 90716 "
        "90717 90723 90731 90732 90744 90745 90746 90747 90748 90802 90803 "
        "90804 90805 90806 90807 90808 90810 90812 90813 90814 90815 91001 "
        "91006 91007 91011 91016 91017 91020 91024 91030 91040 91042 91101 "
        "91103 91104 91105 91106 91107 91108 91201 91202 91203 91204 91205 "
        "91206 91207 91208 91210 91214 91301 91302 91303 91304 91306 91307 "
        "91311 91321 91322 91324 91325 91326 91330 91331 91335 91336 91342 "
        "91343 91344 91345 91350 91351 91354 91355 91356 91364 91367 91380 "
        "91381 91384 91387 91390 91401 91402 91403 91405 91406 91411 91423 "
        "91436 91501 91502 91503 91504 91505 91506 91601 91602 91604 91605 "
        "91606 91607 91702 91706 91711 91722 91723 91724 91731 91732 91733 "
        "91740 91741 91744 91745 91746 91747 91748 91750 91754 91755 91756 "
        "91765 91766 91767 91768 91770 91775 91776 91780 91789 91790 91791 "
        "91792 93532 93534 93535 93543 93544 93550 93551 93552 93553 93591"
    ).split()},
    # --- Ventura -----------------------------------------------------------
    **{z: "Ventura" for z in (
        "91320 91360 91362 93001 93003 93004 93010 93012 93013 93015 93021 "
        "93023 93024 93030 93031 93032 93033 93035 93036 93060 93061 93063 "
        "93064 93065"
    ).split()},
    # --- Orange ------------------------------------------------------------
    **{z: "Orange" for z in (
        "90620 90621 90623 90630 90720 90740 90742 90743 92602 92603 92604 "
        "92606 92610 92612 92614 92618 92620 92625 92626 92627 92629 92630 "
        "92637 92646 92647 92648 92649 92651 92656 92657 92658 92660 92661 "
        "92662 92663 92672 92673 92675 92683 92688 92691 92692 92694 92697 "
        "92698 92701 92702 92703 92704 92705 92706 92707 92708 92780 92781 "
        "92782 92799 92801 92802 92803 92804 92805 92806 92807 92808 92821 "
        "92823 92831 92832 92833 92834 92835 92840 92841 92843 92844 92845 "
        "92865 92866 92867 92868 92869 92886 92887"
    ).split()},
    # --- San Diego ---------------------------------------------------------
    **{z: "San Diego" for z in (
        "91901 91902 91906 91910 91911 91912 91913 91914 91915 91916 91917 "
        "91932 91934 91935 91941 91942 91945 91950 91951 91962 91977 91978 "
        "92003 92004 92007 92008 92009 92010 92011 92014 92019 92020 92021 "
        "92024 92025 92026 92027 92028 92029 92036 92037 92040 92051 92052 "
        "92054 92056 92057 92058 92060 92064 92065 92067 92069 92071 92075 "
        "92078 92079 92081 92083 92084 92086 92093 92101 92102 92103 92104 "
        "92105 92106 92107 92108 92109 92110 92111 92113 92114 92115 92116 "
        "92117 92118 92119 92120 92121 92122 92123 92124 92126 92127 92128 "
        "92129 92130 92131 92154 92173 92182"
    ).split()},
    # --- Riverside ---------------------------------------------------------
    **{z: "Riverside" for z in (
        "92201 92203 92210 92211 92220 92222 92223 92225 92234 92235 92240 "
        "92241 92256 92260 92262 92263 92264 92270 92274 92501 92503 92504 "
        "92505 92506 92507 92508 92509 92521 92530 92531 92532 92543 92544 "
        "92545 92548 92551 92553 92555 92557 92562 92563 92564 92570 92571 "
        "92572 92581 92582 92583 92584 92585 92586 92587 92590 92591 92592 "
        "92860 92877 92878 92879 92880 92881 92882 92883"
    ).split()},
    # --- San Bernardino ----------------------------------------------------
    **{z: "San Bernardino" for z in (
        "91708 91709 91710 91729 91730 91737 91739 91758 91761 91762 91763 "
        "91764 91784 91785 91786 92252 92277 92278 92284 92301 92307 92308 "
        "92311 92312 92313 92314 92315 92316 92317 92322 92323 92324 92335 "
        "92336 92337 92345 92346 92350 92352 92356 92363 92369 92371 92372 "
        "92373 92374 92376 92377 92382 92392 92393 92394 92397 92399 92401 "
        "92402 92403 92404 92405 92406 92407 92408 92410 92411"
    ).split()},
}

_ZIP_MAP_LOCK = threading.Lock()
_ZIP_MAP = None
_ZIP_MAP_SOURCE = None


def load_zip_county_map(refresh=False):
    """Return ({zip5: county}, source) using the Census ZCTA crosswalk.

    The census file attributes each ZCTA to the county holding the largest
    share of its population (ZPOPPCT) -- this is the documented rule for
    split ZCTAs such as 91361 (Westlake Village, LA/Ventura).
    """
    global _ZIP_MAP, _ZIP_MAP_SOURCE
    with _ZIP_MAP_LOCK:
        if _ZIP_MAP is not None and not refresh:
            return _ZIP_MAP, _ZIP_MAP_SOURCE
        try:
            text = fetch(CENSUS_URL, cache_path=CENSUS_PATH,
                         headers={"User-Agent": CENSUS_UA},
                         refresh=refresh, timeout=120)
        except Exception as exc:
            log.warning("census crosswalk unavailable (%s); using "
                        "curated fallback map", exc)
            _ZIP_MAP = dict(FALLBACK_ZIP_COUNTY)
            _ZIP_MAP_SOURCE = "curated_fallback"
            return _ZIP_MAP, _ZIP_MAP_SOURCE

        mapping = {}
        reader = csv.DictReader(io.StringIO(text))
        best = {}
        for row in reader:
            geoid = (row.get("GEOID") or "").strip()
            zcta = (row.get("ZCTA5") or "").strip()
            if len(zcta) != 5 or not geoid:
                continue
            try:
                pop_pct = float(row.get("ZPOPPCT") or 0)
            except ValueError:
                pop_pct = 0.0
            cur = best.get(zcta)
            if cur is None or pop_pct > cur[1]:
                best[zcta] = (geoid, pop_pct)
        fips_to_name = {v: k for k, v in TARGET_COUNTY_FIPS.items()}
        for zcta, (geoid, _pct) in best.items():
            # keep a ZCTA only when its GLOBAL dominant county (largest
            # population share across all counties, not merely among target
            # counties) is one of the six -- sliver fragments (e.g. 89019:
            # 2.6% in San Bernardino, 97.4% in Nevada) and boundary ZCTAs
            # dominated by a neighboring county (e.g. 93225 Frazier Park:
            # 93.4% Kern) are excluded, and the 90000-93599 SoCal band holds.
            county = fips_to_name.get(geoid)
            if county is None or not ("90000" <= zcta <= "93599"):
                continue
            mapping[zcta] = county
        if not mapping:
            raise RuntimeError("census crosswalk parsed to zero SoCal rows")
        _ZIP_MAP = mapping
        _ZIP_MAP_SOURCE = "census_zcta_county_rel_10"
        journal("zipmap", "ok", f"{len(mapping)} SoCal zips mapped from census")
        return _ZIP_MAP, _ZIP_MAP_SOURCE


def zip_to_county(zip5):
    """Map a 5-digit ZIP to one of the six SoCal counties, or None."""
    mapping, _src = load_zip_county_map()
    return mapping.get(zip5)


def in_socal(zip5):
    return zip_to_county(zip5) is not None


# ---------------------------------------------------------------------------
# Strict normalizers (all pure functions -- unit-test friendly)
# ---------------------------------------------------------------------------

def normalize_zip(raw):
    """Return a 5-digit ZIP string or None (accepts 9-digit, ints, padding)."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    s = s.split("-")[0]
    s = re.sub(r"\D", "", s)
    if len(s) == 9:
        s = s[:5]
    if len(s) != 5 or s.startswith("00000"):
        return None
    return s


def normalize_amount(raw):
    """Parse a dollar amount from '$1,234,567.89', '189993.0000', etc."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.upper() in {"N/A", "NA", "NONE", "NULL", "-", "."}:
        return None
    s = s.replace("$", "").replace(",", "").strip()
    if not re.fullmatch(r"-?\d+(\.\d+)?", s):
        return None
    val = float(s)
    if val <= 0:
        return None
    return val


def normalize_date(raw):
    """Parse a date; returns (date, precision) with precision in
    {'day','month','year'} or (None, None). Accepts ISO, partial ISO and
    US slash formats, and ISO datetimes."""
    if raw is None:
        return None, None
    s = str(raw).strip()
    if not s:
        return None, None
    s = s.split("T")[0].split(" ")[0]
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        try:
            return dt.date(int(m[1]), int(m[2]), int(m[3])), "day"
        except ValueError:
            return None, None
    m = re.fullmatch(r"(\d{4})-(\d{2})", s)
    if m:
        try:
            return dt.date(int(m[1]), int(m[2]), 1), "month"
        except ValueError:
            return None, None
    m = re.fullmatch(r"(\d{4})", s)
    if m:
        return dt.date(int(m[1]), 1, 1), "year"
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        try:
            return dt.date(int(m[3]), int(m[1]), int(m[2])), "day"
        except ValueError:
            return None, None
    m = re.fullmatch(r"(\d{1,2})-(\d{1,2})-(\d{4})", s)
    if m:
        try:
            return dt.date(int(m[3]), int(m[1]), int(m[2])), "day"
        except ValueError:
            return None, None
    return None, None


def clean_city(raw):
    if raw is None:
        return None
    s = re.sub(r"\s+", " ", str(raw)).strip().title()
    return s or None


def clean_company(raw):
    if raw is None:
        return None
    s = re.sub(r"\s+", " ", str(raw)).strip().rstrip(",")
    return s or None


def event_id_for(event_type, agency_or_filing_id):
    """Deterministic event id -- deduplication is built into the PK."""
    digest = hashlib.sha1(
        f"{event_type}|{agency_or_filing_id}".encode("utf-8")).hexdigest()
    return f"{EVENT_ID_PREFIX[event_type]}-{digest[:12]}"


def validate_event_record(rec):
    """Full-row validation applied before any DB insert. Returns None if the
    record is valid, else a short reason string."""
    for field in ("event_id", "company_name", "county", "zip_code", "sector",
                  "event_type", "amount_raised", "offering_date",
                  "agency_or_filing_id", "source_url"):
        if rec.get(field) in (None, ""):
            return f"missing {field}"
    if rec["county"] not in TARGET_COUNTIES:
        return "county out of scope"
    if rec["sector"] not in SECTORS:
        return "sector out of enum"
    if rec["event_type"] not in EVENT_TYPES:
        return "event_type out of enum"
    if not isinstance(rec["amount_raised"], (int, float)) or rec["amount_raised"] <= 0:
        return "amount invalid"
    if not (WINDOW_START - dt.timedelta(days=60) <= rec["offering_date"]
            <= WINDOW_END + dt.timedelta(days=1)):
        return "date out of window"
    if not str(rec["source_url"]).startswith("https://"):
        return "source_url not https"
    return None


# ---------------------------------------------------------------------------
# Sector classification
# ---------------------------------------------------------------------------

_AI_KW = (r"\bartificial intelligence\b", r"\bmachine learning\b",
          r"\bdeep learning\b", r"\bneural network", r"\bcomputer vision\b",
          r"\bnatural language\b", r"\blarge language\b", r"\bllm\b",
          r"\bgenerative ai\b", r"\breinforcement learning\b", r"\bgen ai\b",
          r"\bai\b")
_FINTECH_KW = (r"\bfintech\b", r"\bpayment", r"\bbanking\b", r"\blending\b",
               r"\binsurtech\b", r"\bblockchain\b", r"\bcrypto",
               r"\bdigital wallet\b", r"\bmoney transfer\b", r"\bremittance\b",
               r"\bfinancial technology\b", r"\btrading platform\b")
_DEFENSE_KW = (r"\bdefense\b", r"\bmilitary\b", r"\bwarfighter\b",
               r"\bmissile\b", r"\bradar\b", r"\bsonar\b", r"\bsatellite\b",
               r"\bspacecraft\b", r"\baerospace\b", r"\buav\b", r"\buas\b",
               r"\bmunitions\b", r"\bhypersonic\b", r"\belectronic warfare\b",
               r"\bair force\b", r"\bnavy\b", r"\barmy\b", r"\bspace flight\b",
               r"\bpropulsion\b", r"\borbital\b", r"\bsatcom\b")
_BIOTECH_KW = (r"\bbiotech", r"\bbiomedical\b", r"\btherapeutic", r"\bdrug\b",
               r"\bpharma", r"\bclinical\b", r"\bdiagnostic", r"\bmedical\b",
               r"\bgenomic", r"\bgenome\b", r"\bprotein\b", r"\bvaccine\b",
               r"\bcell therapy\b", r"\bgene therapy\b", r"\bsurgical\b",
               r"\bimplant\b", r"\bbiosensor\b", r"\bpatient\b",
               r"\bbiomarker\b", r"\bimmuno", r"\bcancer\b", r"\btumor\b")
_SAAS_KW = (r"\bsoftware\b", r"\bsaas\b", r"\bplatform\b", r"\bcloud\b",
            r"\bapi\b", r"\benterprise\b", r"\bcybersecurity\b", r"\bcyber\b",
            r"\biot\b", r"\banalytics\b", r"\bdevops\b", r"\bsemiconductor\b",
            r"\bphotonics?\b", r"\bsensors?\b", r"\brobot", r"\blidar\b",
            r"\bquantum\b", r"\boptical\b", r"\bfpga\b", r"\bbattery\b",
            r"\bmaterials?\b", r"\benergy\b", r"\bsolar\b", r"\b5g\b",
            r"\bnetwork\b", r"\bcomputing\b")
_FINADV_KW = (r"\basset management\b", r"\bwealth management\b",
              r"\binvestment management\b", r"\bhedge fund\b",
              r"\bprivate equity\b", r"\bfund\b", r"\bbroker[- ]dealer\b",
              r"\bcapital markets\b", r"\bfinancial services\b",
              r"\badvisory\b", r"\bcapital management\b")

_KEYWORD_SCORES = (
    ("AI_ML", _AI_KW),
    ("Fintech", _FINTECH_KW),
    ("Biotech_Medtech", _BIOTECH_KW),
    ("Defense_Aerospace_Tech", _DEFENSE_KW),
    ("Financial_Services", _FINADV_KW),
    ("Enterprise_SaaS", _SAAS_KW),
)


def _kw_score(text):
    text = (text or "").lower()
    return {name: sum(1 for pat in pats if re.search(pat, text))
            for name, pats in _KEYWORD_SCORES}


def classify_sector_sbir(agency, branch, title, company):
    """Sector for an SBIR/STTR award: keyword score on title+company, with
    agency defaults (DoD/NASA -> defense, HHS -> biotech, NSF/DOE/DOC ->
    generic tech) for zero-signal awards."""
    scores = _kw_score(f"{title} {company} {branch}")
    order = {name: idx for idx, (name, _pats) in enumerate(_KEYWORD_SCORES)}
    best = max(scores, key=lambda s: (scores[s], -order[s]))
    if scores[best] > 0:
        return best
    agency_u = (agency or "").upper()
    if ("DEFENSE" in agency_u or "NASA" in agency_u or "SPACE" in agency_u
            or "ARMY" in agency_u):
        return "Defense_Aerospace_Tech"
    if "HEALTH" in agency_u or "NIH" in agency_u:
        return "Biotech_Medtech"
    return "Enterprise_SaaS"


# Form D industryGroup -> dataset sector. None = not a Tech & Finance sector
# (real estate, restaurants, ...): such filings are dropped, keeping the
# dataset's scope honest. 'manufacturing' survives only via keyword override.
FORMD_INDUSTRY_MAP = {
    "aerospace and defense": "Defense_Aerospace_Tech",
    "banking and financial services": "Financial_Services",
    "financial services": "Financial_Services",
    "investment banking": "Financial_Services",
    "other finance": "Financial_Services",
    "insurance": "Financial_Services",
    "pooled investment fund": "Financial_Services",
    "biotechnology": "Biotech_Medtech",
    "medical devices and equipment": "Biotech_Medtech",
    "healthcare": "Biotech_Medtech",
    "other healthcare": "Biotech_Medtech",
    "software": "Enterprise_SaaS",
    "internet": "Enterprise_SaaS",
    "it services": "Enterprise_SaaS",
    "computers": "Enterprise_SaaS",
    "electronics": "Enterprise_SaaS",
    "semiconductors": "Enterprise_SaaS",
    "telecommunications": "Enterprise_SaaS",
    "other technology": "Enterprise_SaaS",
    "business services": "Enterprise_SaaS",
    "commercial services": "Enterprise_SaaS",
    "education": "Enterprise_SaaS",
}


def classify_sector_formd(industry_group, company_name):
    """Sector for a Form D filing from its industryGroup + issuer name."""
    base = None
    if industry_group:
        norm = re.sub(r"\s+", " ",
                      industry_group.lower().replace("&", "and")).strip()
        base = FORMD_INDUSTRY_MAP.get(norm)
        if base is None and norm == "manufacturing":
            scores = _kw_score(company_name or "")
            if scores["Defense_Aerospace_Tech"] > 0:
                base = "Defense_Aerospace_Tech"
        if base is None:
            return None
    else:
        return None

    scores = _kw_score(f"{company_name} {industry_group}")
    if base in ("Enterprise_SaaS", "Defense_Aerospace_Tech") and \
            scores["AI_ML"] > 0:
        return "AI_ML"
    # fintech-software signals on the issuer NAME override both the generic
    # tech bucket and the generic finance bucket (banks stay Financial_
    # Services -- 'banking' is deliberately absent from this list).
    if base == "Enterprise_SaaS":
        if any(re.search(p, (company_name or "").lower()) for p in
               (r"\bpayment", r"\bfintech\b", r"\bcrypto", r"\bblockchain\b",
                r"\binsurtech\b", r"\blending\b", r"\bdigital wallet\b",
                r"\bmoney transfer\b", r"\bfinancial technology\b")):
            return "Fintech"
    if base == "Financial_Services":
        # a payments/crypto/blockchain issuer is fintech even under a finance
        # industry label -- but a fund with 'Lending' in its name is just a
        # fund (direct-lending funds stay Financial_Services)
        if any(re.search(p, (company_name or "").lower()) for p in
               (r"\bpayment", r"\bfintech\b", r"\bcrypto", r"\bblockchain\b",
                r"\binsurtech\b", r"\bdigital wallet\b",
                r"\bmoney transfer\b", r"\bfinancial technology\b")):
            return "Fintech"
    return base


# ---------------------------------------------------------------------------
# SBIR / STTR harvester (official bulk CSV)
# ---------------------------------------------------------------------------

_AGENCY_SHORT = {
    "Department of Defense": "DOD", "Department of the Army": "DOD",
    "Department of the Navy": "DOD", "Department of the Air Force": "DOD",
    "National Aeronautics and Space Administration": "NASA",
    "National Science Foundation": "NSF",
    "Department of Health and Human Services": "HHS",
    "Department of Energy": "DOE", "Department of Commerce": "DOC",
    "Department of Agriculture": "USDA", "Department of Education": "ED",
    "Department of Transportation": "DOT", "Department of Homeland Security":
    "DHS", "Environmental Protection Agency": "EPA",
}


def _sbir_event_type(phase, program):
    prog = (program or "").strip().lower()
    ph = (phase or "").strip().lower()
    if prog == "sttr":
        return "STTR"
    if "phase iii" in ph or "phase 3" in ph:
        return None
    if "phase ii" in ph or "phase 2" in ph:
        return "SBIR_PHASE_2"
    if "phase i" in ph or "phase 1" in ph:
        return "SBIR_PHASE_1"
    return None


def _file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def harvest_sbir(refresh=False, limit=None):
    """Yield normalized SBIR/STTR event records from the official bulk CSV."""
    log.info("SBIR source: official bulk award CSV")
    csv_local = Path(SBIR_CSV_PATH)
    if not csv_local.exists():
        fetch(SBIR_CSV_URL, cache_path=SBIR_CSV_PATH,
              headers={"User-Agent": SBIR_UA}, refresh=refresh, timeout=300)
    bulk_sha = _file_sha256(csv_local)

    stats = {"rows": 0, "california": 0, "in_window": 0, "socal": 0,
             "kept": 0, "no_amount": 0, "no_zip": 0, "phase_skipped": 0}
    records = []
    with open(csv_local, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            stats["rows"] += 1
            if (row.get("State") or "").strip() != "California":
                continue
            stats["california"] += 1

            award_date, date_prec = normalize_date(row.get("Proposal Award Date"))
            if award_date is None:
                award_date, date_prec = normalize_date(row.get("Date of Notification"))
            if award_date is None:
                continue
            if not (WINDOW_START <= award_date <= WINDOW_END):
                continue
            stats["in_window"] += 1

            ev_type = _sbir_event_type(row.get("Phase"), row.get("Program"))
            if ev_type is None:
                stats["phase_skipped"] += 1
                continue

            zip5 = normalize_zip(row.get("Zip"))
            if zip5 is None:
                stats["no_zip"] += 1
                continue
            county = zip_to_county(zip5)
            if county is None:
                continue
            stats["socal"] += 1

            amount = normalize_amount(row.get("Award Amount"))
            if amount is None:
                stats["no_amount"] += 1
                continue

            agency = (row.get("Agency") or "").strip()
            agency_short = _AGENCY_SHORT.get(agency, re.sub(r"\W+", "",
                                                             agency)[:6] or "US")
            tracking = (row.get("Agency Tracking Number") or "").strip()
            contract = (row.get("Contract") or "").strip()
            filing_id = f"{agency_short}-{tracking or contract}"
            if not tracking and not contract:
                continue

            title = (row.get("Award Title") or "").strip()
            company = clean_company(row.get("Company"))
            sector = classify_sector_sbir(agency, row.get("Branch"), title,
                                          company)
            rec = {
                "event_type": ev_type,
                "company_name": company,
                "city": clean_city(row.get("City")),
                "county": county,
                "zip_code": zip5,
                "sector": sector,
                "amount_raised": round(amount, 2),
                "offering_date": award_date,
                "agency_or_filing_id": filing_id,
                "source_url": SBIR_AWARDS_PORTAL_URL,
                "raw_metadata": {
                    "source": "SBIR.gov official award data (bulk CSV, "
                              "no-abstract edition)",
                    "bulk_file_url": SBIR_CSV_URL,
                    "bulk_file_sha256": bulk_sha,
                    "award_title": title,
                    "agency": agency,
                    "branch": (row.get("Branch") or "").strip() or None,
                    "program": (row.get("Program") or "").strip() or None,
                    "phase": (row.get("Phase") or "").strip() or None,
                    "contract": contract or None,
                    "solicitation_number": (row.get("Solicitation Number")
                                            or "").strip() or None,
                    "topic_code": (row.get("Topic Code") or "").strip() or None,
                    "pi_name": (row.get("PI Name") or "").strip() or None,
                    "uei": (row.get("UEI") or "").strip() or None,
                    "duns": (row.get("Duns") or "").strip() or None,
                    "date_precision": date_prec,
                    "date_basis": "proposal_award_date",
                    "verification": ("searchable at " + SBIR_AWARDS_PORTAL_URL
                                     + " by company / tracking number"),
                },
            }
            rec["event_id"] = event_id_for(ev_type, filing_id)
            reason = validate_event_record(rec)
            if reason:
                journal("sbir", "dropped", reason,
                        filing_id=filing_id[:40])
                continue
            records.append(rec)
            stats["kept"] += 1
            if limit and len(records) >= limit:
                break
    stats["zip_map_source"] = load_zip_county_map()[1]
    journal("sbir", "done", "harvest complete", **stats)
    log.info("SBIR: %s kept of %s SoCal / %s CA / %s total rows | %s",
             stats["kept"], stats["socal"], stats["california"], stats["rows"],
             stats)
    return records


# ---------------------------------------------------------------------------
# SEC EDGAR harvester (Form D raises + 13F-HR firm metadata)
# ---------------------------------------------------------------------------

# SoCal mailing cities used as EDGAR full-text search probes. Issuer location
# is ALWAYS re-verified from the filing XML (state=CA + SoCal ZIP), so false
# phrase hits are harmless -- only city names missing from this list can
# cause missed filings (documented limitation).
SOCAL_CITIES = (
    # Los Angeles County
    "Agoura Hills", "Alhambra", "Arcadia", "Artesia", "Azusa", "Baldwin Park",
    "Bell", "Bell Gardens", "Bellflower", "Beverly Hills", "Burbank",
    "Calabasas", "Carson", "Cerritos", "Claremont", "Commerce", "Compton",
    "Covina", "Culver City", "Diamond Bar", "Downey", "Duarte", "El Monte",
    "El Segundo", "Gardena", "Glendale", "Glendora", "Hawthorne",
    "Hermosa Beach", "Huntington Park", "Inglewood", "Industry", "Irwindale",
    "La Canada Flintridge", "La Mirada", "La Puente", "La Verne", "Lakewood",
    "Lancaster", "Lawndale", "Lomita", "Long Beach", "Los Angeles", "Lynwood",
    "Malibu", "Manhattan Beach", "Maywood", "Monrovia", "Montebello",
    "Monterey Park", "Norwalk", "Palmdale", "Palos Verdes Estates",
    "Paramount", "Pasadena", "Pico Rivera", "Pomona", "Rancho Palos Verdes",
    "Redondo Beach", "Rosemead", "San Dimas", "San Fernando", "San Gabriel",
    "San Marino", "Santa Clarita", "Santa Fe Springs", "Santa Monica",
    "Sierra Madre", "Signal Hill", "South El Monte", "South Gate",
    "South Pasadena", "Temple City", "Torrance", "Vernon", "Walnut",
    "West Covina", "West Hollywood", "Westlake Village", "Whittier",
    "Canoga Park", "Chatsworth", "Encino", "Granada Hills", "Mission Hills",
    "Newhall", "North Hollywood", "Northridge", "Panorama City",
    "Porter Ranch", "Reseda", "Sherman Oaks", "Studio City", "Sunland",
    "Sylmar", "Tarzana", "Toluca Lake", "Topanga", "Tujunga", "Valencia",
    "Van Nuys", "Venice", "Woodland Hills", "Winnetka", "Marina del Rey",
    "Pacific Palisades", "San Pedro", "Wilmington", "Harbor City",
    "Palos Verdes", "Playa del Rey", "Castaic", "Agua Dulce", "Littlerock",
    # Ventura County
    "Camarillo", "Fillmore", "Moorpark", "Newbury Park", "Ojai", "Oxnard",
    "Port Hueneme", "Santa Paula", "Simi Valley", "Thousand Oaks", "Ventura",
    "Oak View", "Somis",
    # Orange County
    "Aliso Viejo", "Anaheim", "Brea", "Buena Park", "Costa Mesa", "Cypress",
    "Dana Point", "Fountain Valley", "Fullerton", "Garden Grove",
    "Huntington Beach", "Irvine", "Laguna Beach", "Laguna Hills",
    "Laguna Niguel", "Laguna Woods", "Lake Forest", "La Palma",
    "Los Alamitos", "Mission Viejo", "Newport Beach", "Orange", "Placentia",
    "Rancho Santa Margarita", "San Clemente", "San Juan Capistrano",
    "Santa Ana", "Seal Beach", "Stanton", "Tustin", "Villa Park",
    "Westminster", "Yorba Linda", "Foothill Ranch", "Ladera Ranch",
    "Coto de Caza", "Trabuco Canyon", "Newport Coast", "Corona del Mar",
    "Sunset Beach", "Rossmoor", "Silverado",
    # San Diego County
    "Carlsbad", "Chula Vista", "Coronado", "Del Mar", "El Cajon", "Encinitas",
    "Escondido", "Imperial Beach", "La Jolla", "La Mesa", "Lemon Grove",
    "National City", "Oceanside", "Poway", "San Diego", "San Marcos",
    "Santee", "Solana Beach", "Vista", "Rancho Bernardo", "Rancho Santa Fe",
    "Spring Valley", "Lakeside", "Alpine", "Ramona", "Fallbrook", "Bonita",
    "Cardiff", "Cardiff by the Sea", "Carmel Valley", "Mira Mesa",
    "Sorrento Valley", "Pacific Beach", "Ocean Beach", "Point Loma",
    "University City", "Clairemont", "Kearny Mesa", "Scripps Ranch",
    "Carmel Mountain", "4S Ranch", "Julian", "Jamul", "Borrego Springs",
    # Riverside County
    "Blythe", "Calimesa", "Canyon Lake", "Cathedral City", "Coachella",
    "Corona", "Desert Hot Springs", "Eastvale", "Hemet", "Idyllwild",
    "Indian Wells", "Indio", "Jurupa Valley", "Lake Elsinore", "La Quinta",
    "Menifee", "Moreno Valley", "Murrieta", "Norco", "Palm Desert",
    "Palm Springs", "Perris", "Rancho Mirage", "Riverside", "San Jacinto",
    "Temecula", "Wildomar", "Banning", "Beaumont", "Thermal", "Winchester",
    "Thousand Palms", "Bermuda Dunes", "Corona del Mar",
    # San Bernardino County
    "Adelanto", "Apple Valley", "Barstow", "Big Bear Lake", "Big Bear City",
    "Bloomington", "Chino", "Chino Hills", "Colton", "Crestline",
    "Grand Terrace", "Hesperia", "Highland", "Lake Arrowhead", "Loma Linda",
    "Lucerne Valley", "Mentone", "Montclair", "Needles", "Ontario",
    "Rancho Cucamonga", "Redlands", "Rialto", "San Bernardino",
    "Twentynine Palms", "Upland", "Victorville", "Wrightwood", "Yucaipa",
    "Yucca Valley", "Joshua Tree", "Running Springs", "Blue Jay",
    "Phelan", "Pinon Hills", "Oak Hills", "Newberry Springs",
)
# de-duplicate while preserving order
SOCAL_CITIES = list(dict.fromkeys(SOCAL_CITIES))


class EdgarHarvester:
    """EDGAR full-text search + primary-doc fetch/parse."""

    def __init__(self, refresh=False, limit=None, forms=None,
                 date_start=None, date_end=None):
        self.refresh = refresh
        self.limit = limit
        self.forms = forms
        self.date_start = (date_start or WINDOW_START).isoformat()
        self.date_end = (date_end or WINDOW_END).isoformat()
        self.hits = {}          # adsh -> hit metadata (deduped across cities)
        self._city_stats = {}

    # -- full-text search ---------------------------------------------------

    def _fts_url(self, city, from_off):
        q = requests.utils.quote(f'"{city}"')
        return (f"{FTS_BASE}?q={q}&forms={self.forms}"
                f"&dateRange=custom&startdt={self.date_start}"
                f"&enddt={self.date_end}&from={from_off}")

    def search_city(self, city):
        """Collect every filing hit for one city phrase (paginated)."""
        from_off = 0
        total = None
        collected = 0
        while True:
            text = fetch(self._fts_url(city, from_off), refresh=self.refresh)
            data = json.loads(text)
            hits_block = data.get("hits", {})
            total = hits_block.get("total", {}).get("value", 0)
            page = hits_block.get("hits", [])
            for hit in page:
                src = hit.get("_source", {})
                adsh = src.get("adsh")
                if not adsh:
                    continue
                if adsh not in self.hits:
                    self.hits[adsh] = {
                        "adsh": adsh,
                        "doc_file": (hit.get("_id") or "").split(":", 1)[1]
                        if ":" in (hit.get("_id") or "") else None,
                        "ciks": src.get("ciks") or [],
                        "display_names": src.get("display_names") or [],
                        "file_date": src.get("file_date"),
                        "form": src.get("form"),
                        "file_type": src.get("file_type"),
                        "biz_locations": src.get("biz_locations") or [],
                        "biz_states": src.get("biz_states") or [],
                    }
            collected += len(page)
            if not page or collected >= min(total, 10000):
                break
            from_off += 100
            if from_off >= 10000:
                journal("edgar-fts", "warn",
                        f"hit cap 10000 reached for {city}")
                break
        self._city_stats[city] = collected
        journal("edgar-fts", "ok", f'"{city}"', hits=collected, total=total)
        time.sleep(0.12)

    def run_search(self):
        for city in SOCAL_CITIES:
            for attempt in range(3):
                try:
                    self.search_city(city)
                    break
                except Exception as exc:
                    if attempt == 2:
                        journal("edgar-fts", "failed", f"{city}: {exc}")
                        log.warning("FTS query failed for %s: %s", city, exc)
                    else:
                        time.sleep(2 + 4 * attempt)
                        journal("edgar-fts", "retry", f"{city}: {exc}",
                                attempt=attempt + 1)
        journal("edgar-fts", "done", "all city queries complete",
                unique_filings=len(self.hits))

    # -- primary document fetch/parse ---------------------------------------

    def _filing_dir(self, adsh, cik):
        return f"{EDGAR_ARCHIVES}/{int(cik)}/{adsh.replace('-', '')}"

    def _resolve_primary_doc(self, adsh, cik, doc_file, kind):
        """Return candidate filenames for the primary structured document,
        best first. doc_file (the file the full-text search matched) is
        tried first; when it is wrong (e.g. the match landed on an exhibit),
        the filing's index.json is consulted."""
        names = []
        if doc_file:
            names.append(doc_file)
        idx_url = f"{self._filing_dir(adsh, cik)}/index.json"
        try:
            data = json.loads(fetch(idx_url, refresh=self.refresh))
            all_names = [item.get("name", "") for item in
                         data.get("directory", {}).get("item", [])]
        except Exception:
            all_names = []
        if kind == "formd":
            primary = [n for n in all_names
                       if re.search(r"formd.*\.xml$", n, re.I)]
            generic = [n for n in all_names
                       if n.lower() in ("primary_doc.xml", "formd.xml")]
        else:
            primary = [n for n in all_names
                       if re.search(r"13fhr.*primary.*\.xml$|13f-?hr\.xml$",
                                    n, re.I)]
            generic = [n for n in all_names
                       if n.lower().endswith(".xml")
                       and "infotable" not in n.lower()
                       and "parallel" not in n.lower()]
        out = list(dict.fromkeys(names + primary + generic))
        return out or [doc_file] if doc_file else out

    @staticmethod
    def _local(tag):
        return tag.rsplit("}", 1)[-1]

    @classmethod
    def _first(cls, elem, localname):
        for child in elem.iter():
            if cls._local(child.tag) == localname:
                return child
        return None

    @classmethod
    def _text(cls, elem, localname):
        node = cls._first(elem, localname)
        if node is None:
            return None
        return (node.text or "").strip() or None

    def parse_formd(self, adsh, hit):
        cik = hit["ciks"][0] if hit["ciks"] else None
        if cik is None:
            return None, "no CIK"
        root = None
        for doc in self._resolve_primary_doc(adsh, cik, hit["doc_file"],
                                             "formd"):
            try:
                xml_url = f"{self._filing_dir(adsh, cik)}/{doc}"
                cand = ET.fromstring(fetch(xml_url, refresh=self.refresh))
            except Exception as exc:
                return None, f"xml fetch/parse failed: {exc}"
            # modern X0708 schema: <edgarSubmission><primaryIssuer>...
            # legacy pre-2021 schema: <issuer>... -- accept either
            if (self._first(cand, "primaryIssuer") is not None
                    or self._first(cand, "issuer") is not None) and \
                    self._first(cand, "offeringData") is not None:
                root = cand
                break
        if root is None:
            return None, "not a formd document"

        issuer = (self._first(root, "primaryIssuer")
                  or self._first(root, "issuer"))
        offering = self._first(root, "offeringData")

        issuer_name = clean_company(
            self._text(issuer, "entityName")
            or self._text(issuer, "issuerName"))
        addr = self._first(issuer, "issuerAddress")
        city = clean_city(self._text(addr, "city"))
        state = ((self._text(addr, "stateOrCountry")
                  or self._text(addr, "state")) or "").upper()
        zip5 = normalize_zip(self._text(addr, "zipCode"))
        if state != "CA":
            return None, "issuer state not CA"
        if zip5 is None:
            return None, "issuer zip missing"
        county = zip_to_county(zip5)
        if county is None:
            return None, "zip not in SoCal counties"

        # <dateOfFirstSale><value>2024-03-15</value></dateOfFirstSale>
        # (modern) or <dateOfFirstSale>2024-03-15</dateOfFirstSale> (legacy)
        fs_node = self._first(offering, "dateOfFirstSale")
        fs_raw = None
        if fs_node is not None:
            val = self._first(fs_node, "value")
            fs_raw = (val.text if val is not None else fs_node.text)
        fs_raw = (fs_raw or "").strip() or None
        first_sale, fs_prec = normalize_date(fs_raw)
        file_date, _ = normalize_date(hit.get("file_date"))
        if first_sale is not None and first_sale > WINDOW_END:
            return None, "first sale after window"
        if first_sale is not None and first_sale < WINDOW_START:
            return None, "first sale before window"
        if first_sale is None:
            if file_date is None:
                return None, "no usable date"
            offering_date, date_basis = file_date, "filed_date"
        elif fs_prec == "day":
            offering_date, date_basis = first_sale, "first_sale_date"
        elif fs_prec == "month":
            offering_date, date_basis = first_sale, "first_sale_month_start"
        else:
            return None, "year-only first sale"

        # amounts: <offeringSalesAmounts><totalAmountSold>750000
        # </totalAmountSold><totalOfferingAmount>Indefinite|number</...>
        sales = self._first(offering, "offeringSalesAmounts") or offering
        total_sold = normalize_amount(self._text(sales, "totalAmountSold"))
        total_planned = normalize_amount(
            self._text(sales, "totalOfferingAmount"))
        amount = total_sold or total_planned
        if amount is None:
            return None, "no positive amount"

        # <industryGroup><industryGroupType>Software</industryGroupType>...
        ig_node = self._first(offering, "industryGroup")
        industry = None
        if ig_node is not None:
            industry = (self._text(ig_node, "industryGroupType")
                        or (ig_node.text or "").strip() or None)
        sector = classify_sector_formd(industry, issuer_name)
        if sector is None:
            return None, f"industry outside dataset scope ({industry})"

        # exemptions metadata: <federalExemptionsExclusions><item>...
        exemptions = None
        ex_node = self._first(offering, "federalExemptionsExclusions")
        if ex_node is not None:
            items = [(item.text or "").strip() for item in ex_node.iter()
                     if self._local(item.tag) == "item"]
            exemptions = ", ".join(t for t in items if t) or None

        filing_id = adsh
        rec = {
            "event_type": "SEC_FORM_D",
            "company_name": issuer_name,
            "city": city,
            "county": county,
            "zip_code": zip5,
            "sector": sector,
            "amount_raised": round(amount, 2),
            "offering_date": offering_date,
            "agency_or_filing_id": filing_id,
            "source_url": f"{self._filing_dir(adsh, cik)}-index.htm",
            "raw_metadata": {
                "source": "SEC EDGAR Form D filing (issuer XML)",
                "accession": adsh,
                "cik": int(cik),
                "form": hit.get("form"),
                "filed_date": hit.get("file_date"),
                "date_of_first_sale": fs_raw,
                "date_precision": fs_prec or None,
                "date_basis": date_basis,
                "total_amount_sold": total_sold,
                "total_offering_amount_planned": total_planned,
                "industry_group": industry,
                "federal_exemptions": exemptions,
                "minimum_investment": self._text(offering,
                                                 "minimumInvestmentAccepted"),
                "revenue_range": self._text(offering, "revenueRange"),
                "investment_fund_flag": self._text(offering,
                                                   "investmentFundFlag"),
                "edgar_filer_names": hit.get("display_names"),
                "amount_basis": ("total_amount_sold"
                                 if total_sold else "total_offering_amount"),
                **({"amount_outlier_note":
                    f"self-reported totalAmountSold >= ${amount/1e9:.0f}B -- "
                    "as-filed value, issuer scale errors are common in Form "
                    "D; kept verbatim, flagged for review"}
                   if amount > 25_000_000_000 else {}),
            },
        }
        rec["event_id"] = event_id_for("SEC_FORM_D", filing_id)
        reason = validate_event_record(rec)
        if reason:
            return None, reason
        # bookkeeping for amendment collapse
        rec["_cik"] = int(cik)
        rec["_file_date"] = hit.get("file_date") or ""
        return rec, None

    def parse_13f(self, adsh, hit):
        cik = hit["ciks"][0] if hit["ciks"] else None
        if cik is None:
            return None
        root = None
        for doc in self._resolve_primary_doc(adsh, cik, hit["doc_file"],
                                             "13f"):
            try:
                xml_url = f"{self._filing_dir(adsh, cik)}/{doc}"
                cand = ET.fromstring(fetch(xml_url, refresh=self.refresh))
            except Exception:
                return None
            if self._first(cand, "filingManager") is not None:
                root = cand
                break
        if root is None:
            return None

        # X0202 schema: filer address lives in
        # formData/coverPage/filingManager/{name, address}
        fm = self._first(root, "filingManager")
        addr = self._first(fm, "address") if fm is not None else None
        if addr is None:
            return None
        city = clean_city(self._text(addr, "city"))
        state = (self._text(addr, "stateOrCountry") or "").upper()
        zip5 = normalize_zip(self._text(addr, "zipCode"))
        if state != "CA" or zip5 is None:
            return None
        county = zip_to_county(zip5)
        if county is None or county not in ("Los Angeles", "Ventura",
                                            "Orange", "San Diego"):
            return None

        period_raw = (self._text(root, "periodOfReport")
                      or self._text(root, "reportCalendarOrQuarter"))
        period, _prec = normalize_date(period_raw) if period_raw else \
            (None, None)
        value_total = None
        tv_node = self._first(root, "tableValueTotal")
        if tv_node is not None:
            value_total = normalize_amount(tv_node.text)
        name = self._text(fm, "name")
        if not name:
            name = hit["display_names"][0] if hit["display_names"] else None
            if name:
                name = re.sub(r"\s*\(CIK[^)]*\)\s*$", "", name).strip()
        return {
            "filer_name": name,
            "cik": int(cik),
            "accession": adsh,
            "form": hit.get("form"),
            "filed_date": hit.get("file_date"),
            "period_of_report": str(period) if period else None,
            "city": city,
            "state": state,
            "zip_code": zip5,
            "county": county,
            "total_holdings_value_usd": value_total,
            "source_url": f"{self._filing_dir(adsh, cik)}-index.htm",
        }

    def fetch_and_parse(self, parser_kind):
        """Threaded fetch+parse over the deduped filing hit set."""
        results, skipped = [], {}
        cands = list(self.hits.items())
        if self.limit:
            cands = cands[: self.limit]
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {}
            for adsh, hit in cands:
                # cheap pre-filter on the search metadata
                states = [s.upper() for s in hit["biz_states"]]
                if "CA" not in states:
                    skipped.setdefault("state_not_ca", 0)
                    skipped["state_not_ca"] += 1
                    continue
                futures[pool.submit(self.parse_formd if parser_kind == "formd"
                                    else self.parse_13f, adsh, hit)] = adsh
            for fut in as_completed(futures):
                adsh = futures[fut]
                try:
                    if parser_kind == "formd":
                        rec, reason = fut.result()
                        if rec is None:
                            skipped.setdefault(reason.split(" (")[0], 0)
                            skipped[reason.split(" (")[0]] += 1
                        else:
                            results.append(rec)
                    else:
                        rec = fut.result()
                        if rec is None:
                            skipped.setdefault("13f filtered", 0)
                            skipped["13f filtered"] += 1
                        else:
                            results.append(rec)
                except Exception as exc:
                    journal("edgar-parse", "error", f"{adsh}: {exc}")
                    skipped.setdefault("parse_exception", 0)
                    skipped["parse_exception"] += 1
        return results, skipped


def collapse_formd_amendments(records):
    """One row per offering: group Form D/A chains by (CIK, first-sale
    month) and keep the most recently filed accession -- the amendment's
    cumulative 'total amount sold' supersedes the original filing (avoids
    double-counting the same raise)."""
    def key(r):
        d = r["offering_date"]
        if r["raw_metadata"].get("date_basis") == "first_sale_month_start":
            return f"m{d:%Y-%m}"
        if r["raw_metadata"].get("date_basis") == "first_sale_date":
            return f"m{d:%Y-%m}"
        if r["raw_metadata"].get("date_basis") == "filed_date":
            return f"f{r['raw_metadata']['filed_date'][:7]}"
        return f"x{d.isoformat()}"

    keep = {}
    for r in records:
        k = (r["_cik"], key(r))
        cur = keep.get(k)
        rank_new = (r["_file_date"], r["agency_or_filing_id"])
        if cur is None:
            keep[k] = r
        else:
            rank_cur = (cur["_file_date"], cur["agency_or_filing_id"])
            if rank_new > rank_cur:
                keep[k] = r
    collapsed = list(keep.values())
    journal("edgar-formd", "dedupe",
            "amendment collapse", before=len(records), after=len(collapsed))
    for r in collapsed:
        r["raw_metadata"]["amendment_collapsed"] = True
    return collapsed


def harvest_edgar_formd(refresh=False, limit=None):
    log.info("EDGAR Form D source: full-text search over %s SoCal cities",
             len(SOCAL_CITIES))
    harv = EdgarHarvester(refresh=refresh, limit=limit, forms="D")
    harv.run_search()
    results, skipped = harv.fetch_and_parse("formd")
    results = collapse_formd_amendments(results)
    journal("edgar-formd", "done", "harvest complete", kept=len(results),
            skipped=skipped)
    log.info("EDGAR Form D: %s kept, skipped: %s", len(results), skipped)
    return results


def harvest_edgar_13f(refresh=False, limit=None):
    log.info("EDGAR 13F-HR source: firm metadata (auxiliary export)")
    harv = EdgarHarvester(refresh=refresh, limit=limit, forms="13F-HR")
    harv.run_search()
    results, skipped = harv.fetch_and_parse("13f")
    payload = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "source": "SEC EDGAR 13F-HR filings (filer address from primary doc)",
        "note": ("Auxiliary firm metadata -- 13F is a holdings disclosure, "
                 "not a capital raise, so these records are NOT written to "
                 "socal_capital_events; they are exported as "
                 "socal_firm_filings_metadata.csv. Counties limited to LA/"
                 "Ventura/Orange/San Diego per dataset scope."),
        "record_count": len(results),
        "records": results,
    }
    THIRTEEN_F_METADATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    THIRTEEN_F_METADATA_PATH.write_text(
        json.dumps(payload, indent=1), encoding="utf-8")
    journal("edgar-13f", "done", "metadata written", kept=len(results),
            skipped=skipped, path=str(THIRTEEN_F_METADATA_PATH))
    log.info("EDGAR 13F-HR: %s firm records (skipped %s)", len(results),
             skipped)
    return results


# ---------------------------------------------------------------------------
# California state grants (probe only -- honest zero)
# ---------------------------------------------------------------------------

def harvest_state_grants(refresh=False):
    """Probe data.ca.gov for California Grants Portal award data.

    The portal publishes county-served (not recipient ZIP) rows dominated by
    government/nonprofit service grants, so it cannot meet the strict
    ZIP-level SoCal + Tech & Finance sector requirements. Rather than
    fabricate ZIPs or sectors, zero rows are loaded; the probe outcome is
    logged and surfaced in the run summary."""
    stats = {"probe": "california-grants-portal", "datasets_found": 0,
             "rows_examined": 0, "loaded": 0,
             "reason": ("portal data has no recipient ZIP/city and is not "
                        "business-sector scoped; strict zip+sector "
                        "requirements not satisfiable -- 0 rows loaded")}
    try:
        text = fetch(GRANTS_PORTAL_SEARCH,
                     headers={"User-Agent": CA_DATA_UA}, refresh=refresh)
        results = json.loads(text).get("result", {}).get("results", [])
        stats["datasets_found"] = len(results)
        for ds in results[:3]:
            for res in ds.get("resources", [])[:1]:
                stats.setdefault("resource_urls", []).append(res.get("url"))
                csv_url = res.get("url")
                if not csv_url:
                    continue
                try:
                    data = fetch(csv_url, headers={"User-Agent": CA_DATA_UA},
                                 refresh=refresh, timeout=120)
                    rows = list(csv.DictReader(io.StringIO(data)))
                    stats["rows_examined"] += len(rows)
                    cols = set(rows[0].keys()) if rows else set()
                    stats.setdefault("columns", sorted(cols))
                except Exception as exc:
                    stats.setdefault("resource_errors", []).append(str(exc)[:120])
    except Exception as exc:
        stats["error"] = str(exc)[:200]
    journal("grants", "done", "probe complete", **stats)
    log.info("STATE_GRANT probe: %s datasets / %s rows examined, 0 loaded",
             stats["datasets_found"], stats["rows_examined"])
    return []


# ---------------------------------------------------------------------------
# Database loading
# ---------------------------------------------------------------------------

INSERT_SQL = """
INSERT INTO socal_capital_events
    (event_id, company_name, city, county, zip_code, sector, event_type,
     amount_raised, offering_date, agency_or_filing_id, source_url,
     raw_metadata)
VALUES %s
ON CONFLICT (event_id) DO UPDATE SET
    company_name    = EXCLUDED.company_name,
    city            = EXCLUDED.city,
    county          = EXCLUDED.county,
    zip_code        = EXCLUDED.zip_code,
    sector          = EXCLUDED.sector,
    event_type      = EXCLUDED.event_type,
    amount_raised   = EXCLUDED.amount_raised,
    offering_date   = EXCLUDED.offering_date,
    source_url      = EXCLUDED.source_url,
    raw_metadata    = EXCLUDED.raw_metadata
"""


def ensure_schema(conn):
    """Apply funding_schema.sql if the event table does not exist yet.

    Looks in the project directory first, then a backup copy inside the
    module's own cache dir (survives unrelated cleanups of the workspace)."""
    cur = conn.cursor()
    cur.execute("SELECT to_regclass('public.socal_capital_events')")
    if cur.fetchone()[0] is None:
        candidates = [PROJECT_DIR / "funding_schema.sql",
                      CACHE_DIR / "funding_schema_backup.sql"]
        sql = None
        for cand in candidates:
            if cand.exists():
                sql = cand.read_text(encoding="utf-8")
                journal("db", "ok", f"schema restored from {cand.name}")
                break
        if sql is None:
            raise RuntimeError(
                "socal_capital_events missing and funding_schema.sql not "
                f"found (looked in {[str(c) for c in candidates]})")
        cur.execute(sql)
        conn.commit()
        journal("db", "ok", "funding_schema.sql applied by harvester")
        log.info("funding_schema.sql applied (socal_capital_events was missing)")


def load_events(records, *, rebuild=False):
    """Insert records (dedupe-safe) and refresh the summary view."""
    conn = psycopg2.connect(DB_DSN)
    try:
        ensure_schema(conn)
        cur = conn.cursor()
        if rebuild:
            cur.execute("TRUNCATE TABLE socal_capital_events")
            conn.commit()
            journal("db", "ok", "socal_capital_events truncated (--rebuild)")
        cur.execute("SELECT event_id FROM socal_capital_events")
        existing = {r[0] for r in cur.fetchall()}

        rows = []
        for r in records:
            rows.append((
                r["event_id"], r["company_name"], r["city"], r["county"],
                r["zip_code"], r["sector"], r["event_type"],
                r["amount_raised"], r["offering_date"],
                r["agency_or_filing_id"], r["source_url"],
                json.dumps(r["raw_metadata"], ensure_ascii=False)))
        new_rows = [row for row in rows if row[0] not in existing]
        updated_ct = len(rows) - len(new_rows)
        if rows:
            execute_values(cur, INSERT_SQL, rows, page_size=500)
            conn.commit()
        cur.execute("REFRESH MATERIALIZED VIEW socal_county_capital_summary")
        conn.commit()
        journal("db", "done", "load + MV refresh complete",
                attempted=len(records), inserted=len(new_rows),
                refreshed=updated_ct)
        log.info("DB: inserted %s, refreshed %s of %s; MV refreshed",
                 len(new_rows), updated_ct, len(records))
        return len(new_rows)
    finally:
        conn.close()


def db_counts():
    conn = psycopg2.connect(DB_DSN)
    try:
        cur = conn.cursor()
        out = {}
        cur.execute("""SELECT event_type, count(*), sum(amount_raised)
                       FROM socal_capital_events GROUP BY 1 ORDER BY 1""")
        out["by_event_type"] = {t: {"deals": n, "total": float(s or 0)}
                                for t, n, s in cur.fetchall()}
        cur.execute("""SELECT county, count(*), sum(amount_raised)
                       FROM socal_capital_events GROUP BY 1 ORDER BY 2 DESC""")
        out["by_county"] = {c: {"deals": n, "total": float(s or 0)}
                            for c, n, s in cur.fetchall()}
        cur.execute("SELECT count(*) FROM socal_capital_events")
        out["total_events"] = cur.fetchone()[0]
        return out
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Harvest the SoCal Tech & Finance Capital Flows dataset")
    parser.add_argument("--sources",
                        default="sbir,edgar-formd,edgar-13f,grants",
                        help="comma list: sbir,edgar-formd,edgar-13f,grants")
    parser.add_argument("--refresh", action="store_true",
                        help="bypass the disk cache")
    parser.add_argument("--rebuild", action="store_true",
                        help="truncate socal_capital_events before loading")
    parser.add_argument("--no-load", action="store_true",
                        help="harvest only; skip DB insert")
    parser.add_argument("--limit", type=int, default=None,
                        help="cap filings per source (smoke runs)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level.upper(),
                        format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S", stream=sys.stderr)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    journal("run", "start", f"sources={sources} refresh={args.refresh}",
            window=f"{WINDOW_START}..{WINDOW_END}")
    log.info("Dataset window: %s .. %s", WINDOW_START, WINDOW_END)
    mapping, map_source = load_zip_county_map(refresh=args.refresh)
    log.info("ZIP map: %s zips from %s", len(mapping), map_source)

    summary = {"run_id": _RUN_ID, "window": [str(WINDOW_START), str(WINDOW_END)],
               "zip_map_source": map_source, "sources": {}}

    for source in sources:
        if source == "sbir":
            records = harvest_sbir(refresh=args.refresh, limit=args.limit)
        elif source == "edgar-formd":
            records = harvest_edgar_formd(refresh=args.refresh,
                                          limit=args.limit)
        elif source == "edgar-13f":
            harvest_edgar_13f(refresh=args.refresh, limit=args.limit)
            summary["sources"]["edgar-13f"] = {
                "records_metadata_file": str(THIRTEEN_F_METADATA_PATH.name)}
            continue
        elif source == "grants":
            records = harvest_state_grants(refresh=args.refresh)
        else:
            log.warning("unknown source %s -- skipped", source)
            continue

        summary["sources"][source] = {
            "harvested_records": len(records),
            "event_type_breakdown":
                {t: sum(1 for r in records if r["event_type"] == t)
                 for t in EVENT_TYPES},
        }
        if not args.no_load:
            inserted = load_events(records, rebuild=args.rebuild)
            args.rebuild = False        # only the first load truncates
            summary["sources"][source]["inserted"] = inserted

    if not args.no_load:
        summary["db_counts"] = db_counts()
    SUMMARY_PATH.write_text(json.dumps(summary, indent=1), encoding="utf-8")
    journal("run", "done", "harvest finished",
            total_events=summary.get("db_counts", {}).get("total_events"))
    print(json.dumps(summary, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
