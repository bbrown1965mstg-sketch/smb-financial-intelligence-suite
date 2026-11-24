#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
harvest_sec_peer_ratios.py
==========================
Audited Finance & Tech Peer Financial Ratios Dataset (exports/peer_financial_ratios_v1/)
Sub-agent 1: SEC XBRL Financial Data Engineer.

Harvests real XBRL financial statements from the SEC EDGAR company-concept API
(data.sec.gov) for 50 representative public Finance / Fintech / SaaS / IT
Services companies -- including Southern California public companies -- over the
last five fiscal years (FY2021-FY2025, annual + quarterly periods), and loads
them into the ``public_peer_financials`` table in the local ``smb_warehouse``
PostgreSQL database.

SEC compliance
--------------
* Identifying User-Agent header (SEC fair-access policy).
* Hard rate limiter at 10 requests / second.
* All HTTP responses cached locally as JSON under ``data_cache/sec_xbrl/``
  (keyed by URL hash), including 404 misses, so re-runs make zero requests
  for previously seen URLs.

Methodology (documented in exports/peer_financial_ratios_v1/PEER_RATIOS_DATA_DICTIONARY.md)
-------------------------------------------------------------------------------------------
* XBRL tag fallback chains (priority order):
    revenue          : Revenues > [bank fallback: InterestIncomeExpenseNet +
                       NoninterestIncome, synthesized] >
                       RevenueFromContractWithCustomer{Excluding,Including}AssessedTax
                       > SalesRevenueNet > TotalRevenue > RevenuesNetOfInterestExpense
                       > TotalInvestmentIncome > GrossInvestmentIncomeOperating (BDCs)
    cost_of_revenue  : CostOfRevenue > CostOfGoodsAndServicesSold > CostOfGoodsSold
                       > CostOfServices > CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization
    gross_profit     : GrossProfit, else computed = revenue - cost_of_revenue
    rd_expense       : ResearchAndDevelopmentExpense
                       > ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost
                       > TechnologyAndDevelopmentExpense
    sga_expense      : SellingGeneralAndAdministrativeExpense, else NoninterestExpense
                       (bank filers: total noninterest expense / efficiency-ratio
                       denominator), else
                       computed = SellingAndMarketingExpense + GeneralAndAdministrativeExpense
    operating_income : OperatingIncomeLoss (NULL for most bank filers)
    net_income       : NetIncomeLoss
    operating_cash_flow: NetCashProvidedByUsedInOperatingActivities
                       > NetCashProvidedByUsedInOperatingActivitiesContinuingOperations
* Restatement / amendment dedup: only 10-K, 10-K/A, 10-Q, 10-Q/A forms are
  considered; for each (period, tag) the latest amended / latest filed value
  wins (amended form > original, then latest filed date, then accession no).
* Data-coherence guards: the bank revenue synthesis is skipped for any period
  where a component (InterestIncomeExpenseNet / NoninterestIncome) is negative
  (merger charges netted into noninterest income), and a derived fiscal Q4 is
  only emitted when Q1-Q3 revenue is 40-105% of the annual revenue (rejects
  annual-vs-quarterly source mixing).
* Period classification by duration: annual = 330-380 days, quarter = 80-100
  days (6- and 9-month YTD durations are excluded). Fiscal year / quarter
  labels are derived from the containing annual (10-K) period end date.
* Discrete fiscal Q4 is rarely XBRL-tagged; when absent it is derived as
  FY - (Q1 + Q2 + Q3) per field (flagged ``derived_q4 = TRUE``, sourced from
  the annual 10-K).
* gross_margin_pct is enforced to the audited realistic bound [-50, 100]:
  values outside the bound are NULLed and logged. Rows without a revenue
  value are dropped (revenue anchors three of the four ratio metrics).

Usage
-----
  python3 harvest_sec_peer_ratios.py                    # full run + DB upsert
  python3 harvest_sec_peer_ratios.py --dry-run          # harvest, no DB write
  python3 harvest_sec_peer_ratios.py --tickers TTAN,PCOR,BL
  python3 harvest_sec_peer_ratios.py --refresh-cache    # ignore cached copies
  python3 harvest_sec_peer_ratios.py --list-tickers

The pure helper functions (dedupe_entries, compute_gross_profit, compute_sga,
compute_margins, pick_quarter_label, containing_annual, classify_duration,
is_socal_location) are import-safe for pytest (no network / DB side effects).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

MODULE_ROOT = Path(__file__).resolve().parent
CACHE_DIR = MODULE_ROOT / "data_cache" / "sec_xbrl"
LOG = logging.getLogger("harvest_sec_peer_ratios")

SEC_UA = (
    "PeerRatiosResearch/1.0 (SMB Warehouse peer-benchmark research pipeline; "
    "contact: research@smbwarehouse.example)"
)
SEC_REQUEST_TIMEOUT = 30
SEC_MAX_RETRIES = 3
DEFAULT_RATE_PER_SEC = 10.0

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{:010d}.json"
COMPANY_CONCEPT_URL = "https://data.sec.gov/api/xbrl/companyconcept/CIK{:010d}/us-gaap/{}.json"

# Fallback CIKs for tickers that may be missing from company_tickers.json
# (e.g. recently delisted names whose historical filings remain on EDGAR).
# CIKs verified via EDGAR company search / full-text search.
CIK_OVERRIDES: Dict[str, Tuple[int, str]] = {
    "COOP": (933136, "Mr. Cooper Group Inc."),        # acquired by Rocket 2025; conformed name now a merger shell
    "MLNK": (1834494, "MeridianLink, Inc."),          # taken private (Centerbridge) 2025
    "GHLD": (1821160, "Guild Holdings Co"),
    "XYZ": (1512673, "Block, Inc."),
}

FISCAL_YEAR_START = 2021
FISCAL_YEAR_END = 2025
ANNUAL_MIN_DAYS, ANNUAL_MAX_DAYS = 330, 380
QUARTER_MIN_DAYS, QUARTER_MAX_DAYS = 80, 100

ALLOWED_FORMS = {"10-K", "10-K/A", "10-Q", "10-Q/A"}

VERTICALS = (
    "Enterprise_SaaS",
    "Fintech_Payments",
    "Asset_Wealth_Mgmt",
    "IT_Consulting_Services",
    "Specialty_Finance",
)

# (ticker, vertical) -- 50 tickers across the five Finance & Tech verticals.
ALL_TICKERS: List[Tuple[str, str]] = [
    # --- Enterprise_SaaS (incl. SoCal: TTAN Glendale, PCOR Carpinteria, BL Woodland Hills,
    #     LZ Glendale, MLNK Costa Mesa, TTD Ventura, TDC San Diego) ---
    ("TTAN", "Enterprise_SaaS"),   # ServiceTitan (Glendale, CA)
    ("PCOR", "Enterprise_SaaS"),   # Procore Technologies (Carpinteria, CA)
    ("BL",   "Enterprise_SaaS"),   # BlackLine (Woodland Hills, CA)
    ("LZ",   "Enterprise_SaaS"),   # LegalZoom (Glendale, CA)
    ("MLNK", "Enterprise_SaaS"),   # MeridianLink (Costa Mesa, CA)
    ("TTD",  "Enterprise_SaaS"),   # The Trade Desk (Ventura, CA)
    ("TDC",  "Enterprise_SaaS"),   # Teradata (San Diego, CA)
    ("CRM",  "Enterprise_SaaS"),   # Salesforce (San Francisco, CA)
    ("ADBE", "Enterprise_SaaS"),   # Adobe (San Jose, CA)
    ("INTU", "Enterprise_SaaS"),   # Intuit (Mountain View, CA)
    ("NOW",  "Enterprise_SaaS"),   # ServiceNow (Santa Clara, CA)
    ("TYL",  "Enterprise_SaaS"),   # Tyler Technologies (Plano, TX)
    # --- Fintech_Payments ---
    ("XYZ",  "Fintech_Payments"),  # Block / Square (San Francisco, CA)
    ("PYPL", "Fintech_Payments"),  # PayPal (San Jose, CA)
    ("AFRM", "Fintech_Payments"),  # Affirm (San Francisco, CA)
    ("SOFI", "Fintech_Payments"),  # SoFi Technologies (San Francisco, CA)
    ("UPST", "Fintech_Payments"),  # Upstart (San Mateo, CA)
    ("GDOT", "Fintech_Payments"),  # Green Dot (Austin, TX; formerly Pasadena, CA)
    ("DAVE", "Fintech_Payments"),  # Dave Inc (Los Angeles, CA)
    ("TREE", "Fintech_Payments"),  # LendingTree (Charlotte, NC)
    ("MQ",   "Fintech_Payments"),  # Marqeta (Oakland, CA)
    ("FIS",  "Fintech_Payments"),  # Fidelity National Information Services (Jacksonville, FL)
    # --- Asset_Wealth_Mgmt (incl. SoCal: ARES & HLI Los Angeles, LPLA San Diego) ---
    ("BLK",  "Asset_Wealth_Mgmt"),  # BlackRock (New York, NY)
    ("SCHW", "Asset_Wealth_Mgmt"),  # Charles Schwab (Westlake, TX / San Francisco, CA)
    ("ARES", "Asset_Wealth_Mgmt"),  # Ares Management (Los Angeles, CA)
    ("HLNE", "Asset_Wealth_Mgmt"),  # Hamilton Lane (Conshohocken, PA)
    ("LPLA", "Asset_Wealth_Mgmt"),  # LPL Financial Holdings (San Diego, CA)
    ("HLI",  "Asset_Wealth_Mgmt"),  # Houlihan Lokey (Los Angeles, CA)
    ("BEN",  "Asset_Wealth_Mgmt"),  # Franklin Resources (San Mateo, CA)
    # --- IT_Consulting_Services (incl. SoCal: RGP Irvine, KFY Los Angeles) ---
    ("ACN",  "IT_Consulting_Services"),  # Accenture (Dublin, Ireland)
    ("EPAM", "IT_Consulting_Services"),  # EPAM Systems (Newtown, PA)
    ("CTSH", "IT_Consulting_Services"),  # Cognizant (Teaneck, NJ)
    ("IBM",  "IT_Consulting_Services"),  # IBM (Armonk, NY)
    ("KD",   "IT_Consulting_Services"),  # Kyndryl (New York, NY)
    ("IT",   "IT_Consulting_Services"),  # Gartner (Stamford, CT)
    ("RGP",  "IT_Consulting_Services"),  # Resources Connection (Irvine, CA)
    ("KFY",  "IT_Consulting_Services"),  # Korn Ferry (Los Angeles, CA)
    # --- Specialty_Finance (incl. SoCal: PFSI & PMT Westlake Village, ARCC LA,
    #     EWBC Pasadena, CATY/BANC/HOPE/HAFC LA, CVBF Ontario, GHLD San Diego) ---
    ("PFSI", "Specialty_Finance"),  # PennyMac Financial Services (Westlake Village, CA)
    ("PMT",  "Specialty_Finance"),  # PennyMac Mortgage Investment Trust (Westlake Village, CA)
    ("ARCC", "Specialty_Finance"),  # Ares Capital (Los Angeles, CA)
    ("EWBC", "Specialty_Finance"),  # East West Bancorp (Pasadena, CA)
    ("CATY", "Specialty_Finance"),  # Cathay General Bancorp (Los Angeles, CA)
    ("BANC", "Specialty_Finance"),  # Banc of California (Los Angeles, CA)
    ("HOPE", "Specialty_Finance"),  # Hope Bancorp (Los Angeles, CA)
    ("CVBF", "Specialty_Finance"),  # CVB Financial (Ontario, CA)
    ("GHLD", "Specialty_Finance"),  # Guild Holdings (San Diego, CA)
    ("HAFC", "Specialty_Finance"),  # Hanmi Financial (Los Angeles, CA)
    ("RKT",  "Specialty_Finance"),  # Rocket Companies (Detroit, MI)
    ("COOP", "Specialty_Finance"),  # Mr. Cooper Group (Dallas, TX)
    ("OMF",  "Specialty_Finance"),  # OneMain Holdings (Indianapolis, IN)
]

# XBRL concept fallback chains, in priority order (lower index wins for the
# same period). Keys map to public_peer_financials columns.
# "revenue" contains a synthesized pseudo-tag (bank fallback): banks that do
# not tag a total revenue get revenue = InterestIncomeExpenseNet +
# NoninterestIncome (priority 1, above the fee-only contract-revenue tag).
# "sga_expense": NoninterestExpense is the bank-filer equivalent (efficiency
# ratio denominator) used when no explicit SG&A tag exists.
BANK_REVENUE_COMPONENTS = ("InterestIncomeExpenseNet", "NoninterestIncome")
BANK_REVENUE_TAG_DESC = "Computed:InterestIncomeExpenseNet+NoninterestIncome"
BANK_REVENUE_PRIORITY = 1

CONCEPT_CHAINS: Dict[str, List[str]] = {
    "revenue": [
        "Revenues",                                             # 0
        "__BANK_COMPUTED_REVENUE__",                            # 1 (synthesized)
        "RevenueFromContractWithCustomerExcludingAssessedTax",  # 2
        "RevenueFromContractWithCustomerIncludingAssessedTax",  # 3
        "SalesRevenueNet",                                      # 4
        "TotalRevenue",                                         # 5
        "RevenuesNetOfInterestExpense",                         # 6
        "TotalInvestmentIncome",                                # 7 (REITs)
        "GrossInvestmentIncomeOperating",                       # 8 (BDCs: total investment income)
    ],
    "cost_of_revenue": [
        "CostOfRevenue",
        "CostOfGoodsAndServicesSold",
        "CostOfGoodsSold",
        "CostOfServices",
        "CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization",
    ],
    "gross_profit": ["GrossProfit"],
    "rd_expense": [
        "ResearchAndDevelopmentExpense",
        "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost",
        "TechnologyAndDevelopmentExpense",
    ],
    "sga_expense": [
        "SellingGeneralAndAdministrativeExpense",  # 0
        "NoninterestExpense",                      # 1 (bank filers: total noninterest expense)
    ],
    "sga_marketing": ["SellingAndMarketingExpense"],
    "sga_general": ["GeneralAndAdministrativeExpense"],
    "operating_income": ["OperatingIncomeLoss"],
    "net_income": ["NetIncomeLoss"],
    "operating_cash_flow": [
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    ],
}

# Southern California counties (Census region): Imperial, Kern, Los Angeles,
# Orange, Riverside, San Bernardino, San Diego, Santa Barbara, Ventura.
SOCAL_CITIES = frozenset({
    # Los Angeles County
    "los angeles", "la", "city of los angeles", "santa monica", "glendale", "burbank",
    "pasadena", "woodland hills", "calabasas", "agoura hills", "west hills", "canoga park",
    "chatsworth", "northridge", "granada hills", "mission hills", "north hollywood",
    "studio city", "sherman oaks", "van nuys", "valley village", "encino", "tarzana",
    "reseda", "winnetka", "porter ranch", "sun valley", "sunland", "tujunga",
    "la crescenta", "montrose", "eagle rock", "highland park", "glassell park", "echo park",
    "silver lake", "los feliz", "hollywood", "west hollywood", "w hollywood", "beverly hills",
    "century city", "westwood", "brentwood", "pacific palisades", "malibu", "topanga",
    "marina del rey", "venice", "playa del rey", "westchester", "playa vista",
    "culver city", "el segundo", "manhattan beach", "hermosa beach", "redondo beach",
    "torrance", "palos verdes estates", "rancho palos verdes", "palos verdes",
    "rolling hills", "rolling hills estates", "lomita", "san pedro", "wilmington",
    "harbor city", "gardena", "lawndale", "hawthorne", "inglewood", "lennox", "compton",
    "carson", "long beach", "signal hill", "lakewood", "bellflower", "norwalk", "downey",
    "paramount", "south gate", "lynwood", "cerritos", "artesia", "hawaiian gardens",
    "bell", "maywood", "huntington park", "vernon", "east los angeles", "boyle heights",
    "arcadia", "monrovia", "duarte", "sierra madre", "south pasadena", "alhambra",
    "monterey park", "san gabriel", "rosemead", "el monte", "south el monte", "temple city",
    "san marino", "altadena", "la canada flintridge", "la canada", "azusa", "covina",
    "west covina", "glendora", "san dimas", "la verne", "claremont", "pomona", "walnut",
    "diamond bar", "whittier", "pico rivera", "santa fe springs", "montebello", "monterey park",
    "santa clarita", "valencia", "stevenson ranch", "newhall", "canyon country", "saugus",
    "castaic", "lancaster", "palmdale", "quartz hill", "el monte",
    # Ventura County
    "ventura", "san buenaventura", "oxnard", "camarillo", "thousand oaks", "westlake village",
    "westlake vlg", "newbury park", "moorpark", "simi valley", "santa paula", "fillmore",
    "port hueneme", "carpinteria", "oak view", "ojai", "somis",
    # Santa Barbara County
    "santa barbara", "goleta", "isla vista", "summerland", "montecito", "buellton",
    "solvang", "santa maria", "lompoc", "orcutt", "guadalupe",
    # Kern County
    "bakersfield", "tehachapi", "taft", "delano", "wasco", "shafter", "arvin",
    "california city", "ridgecrest", "rosamond", "mojave",
    # Orange County
    "irvine", "newport beach", "newport coast", "costa mesa", "santa ana", "tustin",
    "orange", "fullerton", "anaheim", "garden grove", "huntington beach", "fountain valley",
    "westminster", "seal beach", "los alamitos", "cypress", "la palma", "buena park",
    "placentia", "yorba linda", "villa park", "brea", "rancho santa margarita",
    "mission viejo", "lake forest", "aliso viejo", "laguna hills", "laguna woods",
    "laguna beach", "dana point", "san clemente", "san juan capistrano", "coto de caza",
    "ladera ranch",
    # Riverside County
    "riverside", "corona", "norco", "moreno valley", "perris", "hemet", "temecula",
    "murrieta", "menifee", "lake elsinore", "wildomar", "eastvale", "jurupa valley",
    "palm springs", "palm desert", "rancho mirage", "indian wells", "la quinta",
    "cathedral city", "coachella", "indio", "desert hot springs", "beaumont", "banning",
    # San Bernardino County
    "san bernardino", "ontario", "rancho cucamonga", "fontana", "upland", "montclair",
    "chino", "chino hills", "riverside", "redlands", "loma linda", "colton", "rialto",
    "victorville", "hesperia", "apple valley", "yucca valley", "twentynine palms",
    "big bear lake", "lake arrowhead", "highland", "grand terrace", "barstow",
    # San Diego County
    "san diego", "carlsbad", "oceanside", "vista", "san marcos", "escondido", "encinitas",
    "solana beach", "del mar", "la jolla", "poway", "rancho bernardo", "rancho santa fe",
    "santee", "el cajon", "la mesa", "spring valley", "chula vista", "national city",
    "imperial beach", "coronado", "ramona", "jamul", "bonita",
    # Imperial County
    "el centro", "calexico", "holtville", "brawley", "imperial",
})

# --------------------------------------------------------------------------- #
# SEC client: caching + rate limiting
# --------------------------------------------------------------------------- #

class RateLimiter:
    """Token-style limiter enforcing a minimum interval between requests."""

    def __init__(self, max_per_sec: float = DEFAULT_RATE_PER_SEC):
        self._min_interval = 1.0 / max_per_sec if max_per_sec > 0 else 0.0
        self._last = 0.0

    def wait(self) -> None:
        if self._min_interval <= 0:
            return
        now = time.monotonic()
        delta = now - self._last
        if delta < self._min_interval:
            time.sleep(self._min_interval - delta)
        self._last = time.monotonic()


class SecClient:
    """Rate-limited SEC EDGAR/data.sec.gov JSON fetcher with a local JSON cache."""

    def __init__(self, cache_dir: Path = CACHE_DIR,
                 rate_per_sec: float = DEFAULT_RATE_PER_SEC,
                 refresh_cache: bool = False):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.limiter = RateLimiter(rate_per_sec)
        self.refresh_cache = refresh_cache
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": SEC_UA,
            "Accept": "application/json",
        })
        self.stats = {"hits": 0, "fetches": 0, "misses404": 0, "errors": 0}

    def _cache_path(self, url: str) -> Path:
        digest = hashlib.md5(url.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{digest}.json"

    def fetch_json(self, url: str) -> Optional[dict]:
        """Return parsed JSON for ``url`` or None on 404/irrecoverable error.

        Cached responses (including 404 misses) are served without a request
        unless refresh_cache is set.
        """
        cache_path = self._cache_path(url)
        if not self.refresh_cache and cache_path.exists():
            try:
                envelope = json.loads(cache_path.read_text(encoding="utf-8"))
                if envelope.get("status") == 200:
                    self.stats["hits"] += 1
                    return envelope.get("data")
                if envelope.get("status") == 404:
                    self.stats["hits"] += 1
                    self.stats["misses404"] += 1
                    return None
            except (ValueError, OSError):
                pass  # corrupt cache entry -> refetch

        backoff = 2.0
        for attempt in range(1, SEC_MAX_RETRIES + 1):
            self.limiter.wait()
            self.stats["fetches"] += 1
            try:
                resp = self.session.get(url, timeout=SEC_REQUEST_TIMEOUT)
                if resp.status_code == 200:
                    data = resp.json()
                    self._write_cache(cache_path, url, 200, data)
                    return data
                if resp.status_code == 404:
                    self.stats["misses404"] += 1
                    self._write_cache(cache_path, url, 404, None)
                    return None
                LOG.warning("HTTP %s for %s (attempt %d/%d)",
                            resp.status_code, url, attempt, SEC_MAX_RETRIES)
            except requests.RequestException as exc:
                LOG.warning("Request error for %s (attempt %d/%d): %s",
                            url, attempt, SEC_MAX_RETRIES, exc)
            if attempt < SEC_MAX_RETRIES:
                time.sleep(backoff)
                backoff *= 2.0
        self.stats["errors"] += 1
        return None

    @staticmethod
    def _write_cache(path: Path, url: str, status: int, data) -> None:
        envelope = {
            "url": url,
            "status": status,
            "fetched_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "data": data,
        }
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(envelope), encoding="utf-8")
        tmp.replace(path)

    def close(self) -> None:
        self.session.close()


# --------------------------------------------------------------------------- #
# Pure helper functions (unit-testable; no I/O)
# --------------------------------------------------------------------------- #

FORM_RANK = {"10-K": 1, "10-Q": 1, "10-K/A": 2, "10-Q/A": 2}


def classify_duration(start_iso: str, end_iso: str) -> Optional[str]:
    """Classify an XBRL duration period as 'annual', 'quarter' or None.

    annual  = 330-380 days (10-K fiscal year, incl. 52/53-week years)
    quarter = 80-100 days  (10-Q three-month periods; excludes 6/9-month YTD)
    """
    try:
        days = (date.fromisoformat(end_iso) - date.fromisoformat(start_iso)).days
    except (TypeError, ValueError):
        return None
    if ANNUAL_MIN_DAYS <= days <= ANNUAL_MAX_DAYS:
        return "annual"
    if QUARTER_MIN_DAYS <= days <= QUARTER_MAX_DAYS:
        return "quarter"
    return None


def _entry_sort_key(entry: dict) -> Tuple[int, int, int, int]:
    """Dedup priority: primary tag first, then amended form, then latest filed,
    then highest accession number. Lower tuple wins."""
    try:
        filed = int(str(entry.get("filed", "")).replace("-", "")) or 0
    except ValueError:
        filed = 0
    accn = str(entry.get("accn", "")).replace("-", "")
    try:
        accn_int = int(accn) if accn else 0
    except ValueError:
        accn_int = 0
    return (
        int(entry.get("tag_priority", 0)),
        -FORM_RANK.get(entry.get("form", ""), 0),
        -filed,
        -accn_int,
    )


def dedupe_entries(entries: List[dict]) -> Dict[str, Dict[Tuple[str, str], dict]]:
    """Keep one value per period, applying the restatement dedup rule:
    only 10-K/10-K/A/10-Q/10-Q/A entries, latest amended / latest filed wins.

    ``entries`` are dicts with keys: start, end, val, form, filed, accn,
    tag_priority (and optionally tag). Returns
    ``{"annual": {(start, end): entry}, "quarter": {(start, end): entry}}``.
    """
    best: Dict[Tuple[str, str], dict] = {}
    for entry in entries:
        form = entry.get("form")
        if form not in FORM_RANK:
            continue
        kind = classify_duration(entry.get("start"), entry.get("end"))
        if kind is None:
            continue
        key = (entry["start"], entry["end"])
        current = best.get(key)
        if current is None or _entry_sort_key(entry) < _entry_sort_key(current):
            best[key] = entry

    out: Dict[str, Dict[Tuple[str, str], dict]] = {"annual": {}, "quarter": {}}
    for (start, end), entry in best.items():
        out[classify_duration(start, end)][(start, end)] = entry
    return out


def pick_quarter_label(q_end: date, ann_start: date, ann_end: date) -> Tuple[int, str]:
    """Map a quarter end date inside a fiscal year to (fiscal_year, 'Q1'..'Q4').

    The fiscal year is labeled by the annual (10-K) period end year; the quarter
    index is the day-proportional position of the quarter end within the annual
    period (quarter length = annual duration / 4, rounded half-up), which is
    robust for 13-week / 4-4-5 fiscal calendars whose quarter ends drift across
    month boundaries.
    """
    duration = (ann_end - ann_start).days
    if duration <= 0:
        return ann_end.year, "Q4"
    days_in = (q_end - ann_start).days + 1
    idx = min(4, max(1, (days_in * 4 + duration // 2) // duration))
    return ann_end.year, "Q{}".format(idx)


def containing_annual(q_end: date,
                      annuals: List[Tuple[date, date]]) -> Optional[Tuple[date, date]]:
    """Return the annual (start, end) period that contains ``q_end``, i.e. the
    one with the smallest end date >= q_end whose start <= q_end."""
    best: Optional[Tuple[date, date]] = None
    for a_start, a_end in annuals:
        if a_start <= q_end <= a_end:
            if best is None or a_end < best[1]:
                best = (a_start, a_end)
    return best


def compute_gross_profit(revenue: Optional[float],
                         gross_tag_value: Optional[float],
                         cost_tag_value: Optional[float]
                         ) -> Tuple[Optional[float], Optional[str]]:
    """XBRL fallback math for gross profit.

    Prefer the explicit GrossProfit tag; otherwise compute
    GrossProfit = Revenue - CostOfRevenue for filers that report cost of
    revenue without an explicit gross-profit subtotal. Returns (value, tag_desc).
    """
    if gross_tag_value is not None:
        return gross_tag_value, "GrossProfit"
    if revenue is not None and cost_tag_value is not None:
        return revenue - cost_tag_value, "Computed:revenue-cost_of_revenue"
    return None, None


def compute_sga(sga_value: Optional[float],
                marketing_value: Optional[float],
                general_value: Optional[float]) -> Tuple[Optional[float], Optional[str]]:
    """XBRL fallback math for SG&A: prefer the explicit
    SellingGeneralAndAdministrativeExpense tag; otherwise sum the split
    SellingAndMarketingExpense + GeneralAndAdministrativeExpense lines."""
    if sga_value is not None:
        return sga_value, "SellingGeneralAndAdministrativeExpense"
    if marketing_value is not None and general_value is not None:
        return marketing_value + general_value, \
            "SUM:SellingAndMarketingExpense+GeneralAndAdministrativeExpense"
    return None, None


def compute_margins(revenue: Optional[float],
                    gross_profit: Optional[float],
                    rd: Optional[float],
                    sga: Optional[float],
                    operating_income: Optional[float],
                    gross_margin_bound: Tuple[float, float] = (-50.0, 100.0),
                    extreme_ratio_abs: float = 1000.0
                    ) -> Tuple[Optional[float], Optional[float], Optional[float],
                               Optional[float], List[str]]:
    """Compute the four ratio metrics (percent units) from USD components.

    * gross_margin_pct is enforced to the audited realistic bound
      ``gross_margin_bound`` (default [-50, 100]): out-of-bound values become
      NULL and a warning is returned.
    * Other ratios beyond +/- ``extreme_ratio_abs`` percent are NULLed with a
      warning (implies a data-quality anomaly).
    Returns (gross_margin_pct, operating_margin_pct, rd_pct_of_rev,
             sga_pct_of_rev, warnings).
    """
    warnings: List[str] = []

    def _pct(numerator: Optional[float]) -> Optional[float]:
        if revenue is None or revenue <= 0 or numerator is None:
            return None
        return 100.0 * float(numerator) / float(revenue)

    gross_margin_pct = _pct(gross_profit)
    if gross_margin_pct is not None and not (
            gross_margin_bound[0] <= gross_margin_pct <= gross_margin_bound[1]):
        warnings.append(
            "gross_margin_pct {:.2f}% outside {} -> NULLed".format(
                gross_margin_pct, gross_margin_bound))
        gross_margin_pct = None

    operating_margin_pct = _pct(operating_income)
    if operating_margin_pct is not None and abs(operating_margin_pct) > extreme_ratio_abs:
        warnings.append("operating_margin_pct {:.2f}% implausible -> NULLed"
                        .format(operating_margin_pct))
        operating_margin_pct = None

    rd_pct_of_rev = _pct(rd)
    if rd_pct_of_rev is not None and abs(rd_pct_of_rev) > extreme_ratio_abs:
        warnings.append("rd_pct_of_rev {:.2f}% implausible -> NULLed".format(rd_pct_of_rev))
        rd_pct_of_rev = None

    sga_pct_of_rev = _pct(sga)
    if sga_pct_of_rev is not None and abs(sga_pct_of_rev) > extreme_ratio_abs:
        warnings.append("sga_pct_of_rev {:.2f}% implausible -> NULLed".format(sga_pct_of_rev))
        sga_pct_of_rev = None

    return gross_margin_pct, operating_margin_pct, rd_pct_of_rev, sga_pct_of_rev, warnings


def is_socal_location(state: Optional[str], city: Optional[str]) -> bool:
    """TRUE when the EDGAR business address is a Southern California city."""
    if not state or not city:
        return False
    if str(state).strip().upper() != "CA":
        return False
    normalized = " ".join(str(city).split()).replace(".", "").replace(",", "").lower()
    return normalized in SOCAL_CITIES


def filing_index_url(cik: int, accession_no: str) -> str:
    """Canonical EDGAR filing index page for an accession number:
    https://www.sec.gov/Archives/edgar/data/<cik>/<accn-nodashes>/<accn-dashes>-index.htm
    """
    return "https://www.sec.gov/Archives/edgar/data/{cik}/{nodash}/{dashed}-index.htm".format(
        cik=int(cik), nodash=accession_no.replace("-", ""), dashed=accession_no)


# --------------------------------------------------------------------------- #
# Harvest pipeline
# --------------------------------------------------------------------------- #

def load_ticker_map(client: SecClient) -> Dict[str, Tuple[int, str]]:
    """ticker -> (cik, official registrant title) from EDGAR's master list."""
    data = client.fetch_json(TICKERS_URL) or {}
    mapping: Dict[str, Tuple[int, str]] = {}
    for item in data.values():
        ticker = str(item.get("ticker", "")).upper().strip()
        if ticker:
            mapping[ticker] = (int(item["cik_str"]), str(item.get("title", "")))
    for ticker, override in CIK_OVERRIDES.items():
        mapping.setdefault(ticker, override)
    return mapping


def fetch_concept_entries(client: SecClient, cik: int, tag: str,
                          tag_priority: int) -> List[dict]:
    """Fetch one us-gaap concept for a CIK and normalize its USD duration facts
    into entry dicts (with tag + tag_priority for dedup prioritization)."""
    payload = client.fetch_json(COMPANY_CONCEPT_URL.format(cik, tag))
    if not payload:
        return []
    units = payload.get("units") or {}
    usd_rows = units.get("USD") or []
    entries: List[dict] = []
    for row in usd_rows:
        if "start" not in row or "end" not in row:
            continue  # instant (balance-sheet) facts are not used
        entries.append({
            "start": row["start"],
            "end": row["end"],
            "val": row.get("val"),
            "form": row.get("form"),
            "filed": row.get("filed"),
            "accn": row.get("accn"),
            "tag": tag,
            "tag_priority": tag_priority,
        })
    return entries


def build_period_maps(deduped: Dict[str, Dict[Tuple[str, str], dict]],
                      annual_periods: List[Tuple[date, date]]
                      ) -> Dict[str, Dict[Tuple[int, str], dict]]:
    """Map deduped entries to (fiscal_year, period_label) keys: annuals keyed
    by end-year, quarters assigned to their containing annual period."""
    annuals: Dict[int, dict] = {}
    for (start_s, end_s), entry in deduped["annual"].items():
        fy = date.fromisoformat(end_s).year
        current = annuals.get(fy)
        if current is None or _entry_sort_key(entry) < _entry_sort_key(current):
            annuals[fy] = entry

    quarters: Dict[Tuple[int, str], dict] = {}
    for (start_s, end_s), entry in deduped["quarter"].items():
        q_end = date.fromisoformat(end_s)
        candidate = containing_annual(q_end, annual_periods)
        if candidate is None:
            continue
        fy, qlabel = pick_quarter_label(q_end, candidate[0], candidate[1])
        key = (fy, qlabel)
        current = quarters.get(key)
        if current is None or _entry_sort_key(entry) < _entry_sort_key(current):
            quarters[key] = entry
    return {"annual": annuals, "quarter": quarters}


def resolve_field_maps(client: SecClient, cik: int
                       ) -> Tuple[Dict[str, Dict[str, Dict[Tuple[int, str], dict]]],
                                  List[Tuple[date, date]]]:
    """Fetch + dedupe every concept chain for a company; keep the per-period
    best entry across the tag chain (priority, then amended form, then latest
    filed).

    Returns:
      field_maps: {field: {"annual"|"quarter": {(fy, period_label): entry}}}
      annuals: sorted list of all (start, end) annual periods (any year)
    """
    field_maps: Dict[str, Dict[str, Dict[Tuple[int, str], dict]]] = {}
    annual_periods: List[Tuple[date, date]] = []

    for field, chain in CONCEPT_CHAINS.items():
        entries: List[dict] = []
        for priority, tag in enumerate(chain):
            if tag == "__BANK_COMPUTED_REVENUE__":
                continue  # synthesized below from BANK_REVENUE_COMPONENTS
            entries.extend(fetch_concept_entries(client, cik, tag, priority))

        if field == "revenue":
            # Bank fallback: banks rarely tag total revenue. Synthesize
            # revenue = InterestIncomeExpenseNet + NoninterestIncome per
            # period where both components exist AND are non-negative
            # (negative components signal pathological filer tagging, e.g.
            # merger charges netted into noninterest income -- BANC FY2023).
            comp_dedup = {
                t: dedupe_entries(fetch_concept_entries(client, cik, t, 99))
                for t in BANK_REVENUE_COMPONENTS
            }
            nii, non_interest = (comp_dedup[BANK_REVENUE_COMPONENTS[0]],
                                 comp_dedup[BANK_REVENUE_COMPONENTS[1]])
            for kind in ("annual", "quarter"):
                for key, e1 in nii[kind].items():
                    e2 = non_interest[kind].get(key)
                    if (e2 is None or e1.get("val") is None or e2.get("val") is None):
                        continue
                    v1, v2 = float(e1["val"]), float(e2["val"])
                    if v1 < 0 or v2 < 0:
                        continue
                    entries.append({
                        "start": e1["start"], "end": e1["end"],
                        "val": v1 + v2,
                        "form": e1.get("form"), "filed": e1.get("filed"),
                        "accn": e1.get("accn"),
                        "tag": BANK_REVENUE_TAG_DESC,
                        "tag_priority": BANK_REVENUE_PRIORITY,
                    })

        deduped = dedupe_entries(entries)

        annuals = {}
        quarters: Dict[Tuple[int, str], dict] = {}
        for (start_s, end_s), entry in deduped["annual"].items():
            fy = date.fromisoformat(end_s).year
            annuals[fy] = entry
            annual_periods.append(
                (date.fromisoformat(start_s), date.fromisoformat(end_s)))

        for (start_s, end_s), entry in deduped["quarter"].items():
            q_end = date.fromisoformat(end_s)
            candidate = containing_annual(q_end, annual_periods)
            if candidate is None:
                continue
            fy, q_label = pick_quarter_label(q_end, candidate[0], candidate[1])
            key = (fy, q_label)
            current = quarters.get(key)
            if current is None or _entry_sort_key(entry) < _entry_sort_key(current):
                quarters[key] = entry

        field_maps[field] = {"annual": annuals, "quarter": quarters}

    return field_maps, sorted(set(annual_periods))


# Fields eligible for derived fiscal-Q4 math: Q4 = FY - (Q1 + Q2 + Q3).
DERIVED_Q4_FIELDS = (
    "revenue", "gross_profit", "rd_expense", "sga_expense",
    "operating_income", "net_income", "operating_cash_flow",
)


def build_company_rows(ticker: str, vertical: str, cik: int,
                       company_name: str, hq_state: Optional[str],
                       hq_city: Optional[str],
                       field_maps: Dict[str, Dict[str, Dict[Tuple[int, str], dict]]],
                       annuals: List[Tuple[date, date]],
                       warnings: List[str]) -> List[dict]:
    """Assemble one dict per (fiscal_year, fiscal_period) row, applying the
    gross-profit / SG&A fallback math, the audited margin bounds, and the
    derived-Q4 math. Rows without a revenue value are dropped."""
    rows: List[dict] = []
    socal = is_socal_location(hq_state, hq_city)

    def annual_value(field: str, fy: int) -> Optional[dict]:
        return field_maps.get(field, {}).get("annual", {}).get(fy)

    def quarter_value(field: str, fy: int, ql: str) -> Optional[dict]:
        return field_maps.get(field, {}).get("quarter", {}).get((fy, ql))

    def resolve_components(fy: int, ql: str) -> Optional[dict]:
        if ql == "FY":
            getter = lambda f: annual_value(f, fy)  # noqa: E731
        else:
            getter = lambda f: quarter_value(f, fy, ql)  # noqa: E731

        rev_entry = getter("revenue")
        if rev_entry is None or rev_entry.get("val") is None:
            return None
        revenue = float(rev_entry["val"])

        gp_entry = getter("gross_profit")
        cost_entry = getter("cost_of_revenue")
        gross_profit, gp_desc = compute_gross_profit(
            revenue,
            float(gp_entry["val"]) if gp_entry and gp_entry.get("val") is not None else None,
            float(cost_entry["val"]) if cost_entry and cost_entry.get("val") is not None else None,
        )

        rd_entry = getter("rd_expense")
        rd = float(rd_entry["val"]) if rd_entry and rd_entry.get("val") is not None else None

        sga_entry = getter("sga_expense")
        mkt_entry = getter("sga_marketing")
        gen_entry = getter("sga_general")
        sga, sga_desc = compute_sga(
            float(sga_entry["val"]) if sga_entry and sga_entry.get("val") is not None else None,
            float(mkt_entry["val"]) if mkt_entry and mkt_entry.get("val") is not None else None,
            float(gen_entry["val"]) if gen_entry and gen_entry.get("val") is not None else None,
        )
        if sga is not None and sga_entry is not None:
            # report the actual tag that supplied the value (SG&A tag, or
            # NoninterestExpense for bank filers)
            sga_desc = sga_entry.get("tag") or sga_desc

        op_entry = getter("operating_income")
        ni_entry = getter("net_income")
        ocf_entry = getter("operating_cash_flow")
        operating_income = float(op_entry["val"]) if op_entry and op_entry.get("val") is not None else None
        net_income = float(ni_entry["val"]) if ni_entry and ni_entry.get("val") is not None else None
        ocf = float(ocf_entry["val"]) if ocf_entry and ocf_entry.get("val") is not None else None

        return {
            "revenue": revenue,
            "revenue_tag": rev_entry.get("tag"),
            "gross_profit": gross_profit,
            "gross_profit_desc": gp_desc,
            "rd_expense": rd,
            "rd_tag": rd_entry.get("tag") if rd_entry else None,
            "sga_expense": sga,
            "sga_desc": sga_desc,
            "operating_income": operating_income,
            "net_income": net_income,
            "operating_cash_flow": ocf,
            "ocf_desc": ocf_entry.get("tag") if ocf_entry else None,
            "revenue_entry": rev_entry,
            "derived_q4": False,
        }

    def make_row(fy: int, ql: str, comps: dict) -> dict:
        rev_entry = comps["revenue_entry"]
        gm, om, rd_pct, sga_pct, warns = compute_margins(
            comps["revenue"], comps["gross_profit"], comps["rd_expense"],
            comps["sga_expense"], comps["operating_income"])
        for w in warns:
            warnings.append("{} FY{} {}: {}".format(ticker, fy, ql, w))

        audit_parts = ["revenue={}".format(comps.get("revenue_tag"))]
        if comps.get("gross_profit_desc"):
            audit_parts.append("gross_profit={}".format(comps["gross_profit_desc"]))
        if comps.get("rd_tag"):
            audit_parts.append("rd={}".format(comps["rd_tag"]))
        if comps.get("sga_desc"):
            audit_parts.append("sga={}".format(comps["sga_desc"]))
        if comps.get("operating_income") is not None:
            audit_parts.append("operating_income=OperatingIncomeLoss")
        if comps.get("net_income") is not None:
            audit_parts.append("net_income=NetIncomeLoss")
        if comps.get("operating_cash_flow") is not None:
            audit_parts.append("ocf={}".format(
                comps.get("ocf_desc") or "NetCashProvidedByUsedInOperatingActivities"))
        if comps.get("derived_q4"):
            audit_parts.append("q4=Derived:FY-(Q1+Q2+Q3)")

        accn = rev_entry.get("accn")
        return {
            "cik": cik,
            "ticker": ticker,
            "company_name": company_name,
            "hq_state": hq_state,
            "is_socal": socal,
            "vertical": vertical,
            "fiscal_year": fy,
            "fiscal_period": ql,
            "revenue": comps["revenue"],
            "gross_profit": comps["gross_profit"],
            "rd_expense": comps["rd_expense"],
            "sga_expense": comps["sga_expense"],
            "operating_income": comps["operating_income"],
            "net_income": comps["net_income"],
            "operating_cash_flow": comps["operating_cash_flow"],
            "gross_margin_pct": gm,
            "operating_margin_pct": om,
            "rd_pct_of_rev": rd_pct,
            "sga_pct_of_rev": sga_pct,
            "source_url": filing_index_url(cik, accn) if accn else None,
            "source_form": rev_entry.get("form"),
            "accession_no": accn,
            "source_tags": "; ".join(audit_parts),
            "derived_q4": bool(comps.get("derived_q4")),
        }

    for fy in range(FISCAL_YEAR_START, FISCAL_YEAR_END + 1):
        # Annual (FY) row
        comps_fy = resolve_components(fy, "FY")
        if comps_fy is not None:
            rows.append(make_row(fy, "FY", comps_fy))

        # Quarters Q1..Q3 as filed
        for ql in ("Q1", "Q2", "Q3"):
            comps = resolve_components(fy, ql)
            if comps is not None:
                rows.append(make_row(fy, ql, comps))

        # Coherence context: Q1-Q3 vs the annual revenue.
        q_comps = [resolve_components(fy, q) for q in ("Q1", "Q2", "Q3")]
        have_context = comps_fy is not None and not any(c is None for c in q_comps)
        ratio_ok, implied_q4 = True, None
        if have_context:
            sum_q1_q3_rev = sum(c["revenue"] for c in q_comps)
            ratio = (sum_q1_q3_rev / comps_fy["revenue"]) if comps_fy["revenue"] else 0.0
            # Plausibility guard: three fiscal quarters should be 40-105% of
            # the annual revenue. A far-off ratio means annual and quarterly
            # values came from different XBRL sources (restatement/source
            # mixing, e.g. BANC tagging Revenues annually but not quarterly).
            ratio_ok = 0.40 <= ratio <= 1.05
            implied_q4 = comps_fy["revenue"] - sum_q1_q3_rev

        # Q4: use a discrete tagged fiscal Q4 if one exists AND it is coherent
        # with the annual and Q1-Q3 values (within max(2% of FY, $1M));
        # otherwise derive each field as FY - (Q1 + Q2 + Q3).
        comps_q4_real = resolve_components(fy, "Q4")
        if comps_q4_real is not None:
            tol = max(0.02 * abs(comps_fy["revenue"]), 1_000_000.0) if have_context else None
            if tol is None or (ratio_ok and
                               abs(comps_q4_real["revenue"] - implied_q4) <= tol):
                rows.append(make_row(fy, "Q4", comps_q4_real))
                continue
            warnings.append("{} FY{} Q4: tagged Q4 incoherent with FY-Q1..Q3 "
                            "(derived instead)".format(ticker, fy))
        if not have_context:
            warnings.append("{} FY{} Q4: FY or Q1-Q3 components unavailable (skipped)"
                            .format(ticker, fy))
            continue
        if not ratio_ok:
            warnings.append(
                "{} FY{} Q4: Q1-Q3 revenue is {:.0%} of FY revenue "
                "(inconsistent sources) (skipped)".format(
                    ticker, fy, (sum(c["revenue"] for c in q_comps) / comps_fy["revenue"])
                    if comps_fy["revenue"] else 0.0))
            continue
        derived = {}
        for field in DERIVED_Q4_FIELDS:
            vals = [comps_fy[field]] + [c[field] for c in q_comps]
            derived[field] = (vals[0] - sum(vals[1:])) if all(v is not None for v in vals) else None
        if derived["revenue"] is None or derived["revenue"] <= 0:
            warnings.append("{} FY{} Q4: derived revenue unavailable (skipped)"
                            .format(ticker, fy))
            continue
        comps_q4 = {
            "revenue": derived["revenue"],
            "revenue_tag": comps_fy["revenue_tag"],
            "gross_profit": derived["gross_profit"],
            "gross_profit_desc": comps_fy["gross_profit_desc"],
            "rd_expense": derived["rd_expense"],
            "rd_tag": comps_fy["rd_tag"],
            "sga_expense": derived["sga_expense"],
            "sga_desc": comps_fy["sga_desc"],
            "operating_income": derived["operating_income"],
            "net_income": derived["net_income"],
            "operating_cash_flow": derived["operating_cash_flow"],
            "ocf_desc": comps_fy["ocf_desc"],
            "revenue_entry": comps_fy["revenue_entry"],
            "derived_q4": True,
        }
        rows.append(make_row(fy, "Q4", comps_q4))

    return rows


def fetch_company_header(client: SecClient, cik: int,
                         fallback_name: str) -> Tuple[str, Optional[str], Optional[str]]:
    """Return (company_name, hq_state, hq_city) from the EDGAR submissions API."""
    submissions = client.fetch_json(SUBMISSIONS_URL.format(cik)) or {}
    name = submissions.get("name") or fallback_name
    addresses = submissions.get("addresses") or {}
    business = addresses.get("business") or {}
    return name, business.get("stateOrCountry"), business.get("city")


def upsert_rows(rows: List[dict], db_config: dict, tickers: List[str]) -> int:
    """Load rows into public_peer_financials. Rows for every ticker being
    loaded are deleted first so a re-run is authoritative (no stale rows from
    earlier harvest logic survive); returns number of rows loaded."""
    import psycopg2
    from psycopg2.extras import execute_values

    dsn = (
        "host={host} port={port} dbname={dbname} user={user} password={password}".format(
            **db_config)
    )
    columns = [
        "cik", "ticker", "company_name", "hq_state", "is_socal", "vertical",
        "fiscal_year", "fiscal_period", "revenue", "gross_profit", "rd_expense",
        "sga_expense", "operating_income", "net_income", "operating_cash_flow",
        "gross_margin_pct", "operating_margin_pct", "rd_pct_of_rev",
        "sga_pct_of_rev", "source_url", "source_form", "accession_no",
        "source_tags", "derived_q4",
    ]
    update_set = ", ".join(
        "{c} = EXCLUDED.{c}".format(c=c)
        for c in columns if c not in ("cik", "fiscal_year", "fiscal_period"))

    tuples = [
        (
            r["cik"], r["ticker"], r["company_name"], r["hq_state"], r["is_socal"],
            r["vertical"], r["fiscal_year"], r["fiscal_period"],
            r["revenue"], r["gross_profit"], r["rd_expense"], r["sga_expense"],
            r["operating_income"], r["net_income"], r["operating_cash_flow"],
            None if r["gross_margin_pct"] is None else round(r["gross_margin_pct"], 4),
            None if r["operating_margin_pct"] is None else round(r["operating_margin_pct"], 4),
            None if r["rd_pct_of_rev"] is None else round(r["rd_pct_of_rev"], 4),
            None if r["sga_pct_of_rev"] is None else round(r["sga_pct_of_rev"], 4),
            r["source_url"], r["source_form"], r["accession_no"], r["source_tags"],
            r["derived_q4"],
        )
        for r in rows
    ]

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM public_peer_financials WHERE ticker = ANY(%s)",
                        (tickers,))
            sql = (
                "INSERT INTO public_peer_financials ({cols}) VALUES %s "
                "ON CONFLICT (cik, fiscal_year, fiscal_period) DO UPDATE SET {upd}"
            ).format(cols=", ".join(columns), upd=update_set)
            execute_values(cur, sql, tuples, page_size=500)
        conn.commit()
    finally:
        conn.close()
    return len(tuples)


def refresh_materialized_view(db_config: dict) -> None:
    import psycopg2
    dsn = "host={host} port={port} dbname={dbname} user={user} password={password}".format(
        **db_config)
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_vertical_financial_quartiles")
    finally:
        conn.close()


def default_db_config() -> dict:
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": os.environ.get("PGDATABASE", "smb_warehouse"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", "postgres"),
    }


def process_ticker(ticker: str, vertical: str, cik_map: Dict[str, Tuple[int, str]],
                   client: SecClient, warnings: List[str]) -> Tuple[int, Optional[dict]]:
    """Harvest one ticker; returns (row_count, summary_dict)."""
    if ticker not in cik_map:
        warnings.append("{}: ticker not found in EDGAR company_tickers.json -> skipped"
                        .format(ticker))
        return 0, None
    cik, title = cik_map[ticker]
    company_name, hq_state, hq_city = fetch_company_header(client, cik, title)
    if ticker in CIK_OVERRIDES:
        # delisted registrants: EDGAR's conformed name may be a post-merger
        # shell (e.g. COOP's CIK is now a Rocket merger sub) -> keep the
        # registrant name at listing
        company_name = CIK_OVERRIDES[ticker][1]

    field_maps, annuals = resolve_field_maps(client, cik)
    rows = build_company_rows(ticker, vertical, cik, company_name, hq_state, hq_city,
                              field_maps, annuals, warnings)

    n_fy = sum(1 for r in rows if r["fiscal_period"] == "FY")
    n_q = len(rows) - n_fy
    n_derived = sum(1 for r in rows if r["derived_q4"])
    gp_computed = sum(1 for r in rows
                      if "gross_profit=Computed:" in (r["source_tags"] or ""))
    sga_sum = sum(1 for r in rows
                  if "sga=SUM:" in (r["source_tags"] or ""))
    summary = {
        "ticker": ticker,
        "cik": cik,
        "company": company_name,
        "hq": "{}, {}".format(hq_city or "?", hq_state or "?"),
        "socal": is_socal_location(hq_state, hq_city),
        "rows": len(rows),
        "fy_rows": n_fy,
        "q_rows": n_q,
        "derived_q4": n_derived,
        "gp_computed": gp_computed,
        "sga_sum": sga_sum,
        "rows_obj": rows,
    }
    return len(rows), summary


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--tickers", default=None,
                        help="comma-separated subset of tickers (default: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="harvest and report, but do not write to Postgres")
    parser.add_argument("--refresh-cache", action="store_true",
                        help="bypass the local JSON cache (still writes fresh copies)")
    parser.add_argument("--rate", type=float, default=DEFAULT_RATE_PER_SEC,
                        help="max SEC requests per second (default 10)")
    parser.add_argument("--list-tickers", action="store_true",
                        help="print the configured ticker list and exit")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    if args.list_tickers:
        for ticker, vertical in ALL_TICKERS:
            print("{:<6} {}".format(ticker, vertical))
        print("total: {}".format(len(ALL_TICKERS)))
        return 0

    targets = ALL_TICKERS
    if args.tickers:
        wanted = {t.strip().upper() for t in args.tickers.split(",") if t.strip()}
        targets = [(t, v) for (t, v) in ALL_TICKERS if t in wanted]
        if not targets:
            LOG.error("no configured tickers matched %s", args.tickers)
            return 2

    client = SecClient(rate_per_sec=args.rate, refresh_cache=args.refresh_cache)
    warnings: List[str] = []
    all_rows: List[dict] = []
    summaries: List[dict] = []

    LOG.info("loading EDGAR ticker map ...")
    cik_map = load_ticker_map(client)
    LOG.info("ticker map entries: %d", len(cik_map))

    try:
        for idx, (ticker, vertical) in enumerate(targets, start=1):
            t0 = time.monotonic()
            try:
                count, summary = process_ticker(ticker, vertical, cik_map, client, warnings)
            except Exception:  # keep harvesting other companies on one-off failures
                LOG.exception("failed to harvest %s", ticker)
                warnings.append("{}: harvest error (see log) -> skipped".format(ticker))
                continue
            if summary is None:
                continue
            summaries.append(summary)
            all_rows.extend(summary.pop("rows_obj"))
            LOG.info("[%d/%d] %-5s %-22s rows=%2d (FY=%d Q=%d derivedQ4=%d) hq=%-28s socal=%s %.1fs",
                     idx, len(targets), summary["ticker"], summary["company"][:22],
                     summary["rows"], summary["fy_rows"], summary["q_rows"],
                     summary["derived_q4"], summary["hq"], summary["socal"],
                     time.monotonic() - t0)
    finally:
        client.close()

    # ---------------------------- coverage report --------------------------- #
    socal_rows = sum(1 for r in all_rows if r["is_socal"])
    derived_rows = sum(1 for r in all_rows if r["derived_q4"])
    gp_computed_rows = sum(1 for r in all_rows
                           if "gross_profit=Computed:" in (r["source_tags"] or ""))
    null_rev = sum(1 for r in all_rows if r["revenue"] is None)
    companies = {r["ticker"] for r in all_rows}

    print("\n================ HARVEST COVERAGE REPORT ================")
    print("configured tickers          : {}".format(len(targets)))
    print("companies with >=1 row      : {}".format(len(companies)))
    print("total rows                  : {}".format(len(all_rows)))
    print("  SoCal rows                : {}".format(socal_rows))
    print("  derived Q4 rows           : {}".format(derived_rows))
    print("  gross_profit computed     : {}".format(gp_computed_rows))
    print("  rows w/ NULL revenue      : {} (should be 0)".format(null_rev))
    print("cache: hits={} fetches={} 404s={} errors={}".format(
        client.stats["hits"], client.stats["fetches"],
        client.stats["misses404"], client.stats["errors"]))
    if warnings:
        print("\nwarnings ({}):".format(len(warnings)))
        for w in warnings:
            print("  - " + w)

    print("\nper-company summary:")
    for s in summaries:
        print("  {:<6} {:<2} rows={:<3} FY={:<2} Q={:<3} dQ4={:<2} gpComputed={:<2} sgaSum={:<2} {}".format(
            s["ticker"], "CA" if s["socal"] else "--", s["rows"], s["fy_rows"],
            s["q_rows"], s["derived_q4"], s["gp_computed"], s["sga_sum"], s["company"][:40]))

    if args.dry_run:
        print("\nDRY RUN: no database writes performed.")
        return 0

    if not all_rows:
        LOG.error("nothing harvested; aborting without DB write")
        return 1

    db_config = default_db_config()
    inserted = upsert_rows(all_rows, db_config, [t for t, _ in targets])
    refresh_materialized_view(db_config)
    print("\nupserted {} rows into public_peer_financials; refreshed "
          "mv_vertical_financial_quartiles.".format(inserted))
    return 0


if __name__ == "__main__":
    sys.exit(main())
