#!/usr/bin/env python3
"""
seed_demo.py -- 36-month multi-year simulation for smb_warehouse.

Simulates a growing service business (painting / remodeling SMB) across three
full years of seasonality, loaded as client workspace #1 of the multi-tenant
(Fractional CFO) warehouse.

  * 8 service lines with PER-TYPE MONTHLY SEASONAL DEMAND CURVES (exterior
    painting and decks collapse in winter; kitchen/bath remodels peak in
    winter; flooring bumps before the holidays), plus ~1.5%/month growth
  * 150 customers acquired over time (CAC attributed per customer via
    financial_ledger.customer_id), with realistic size skew (a few dominant
    commercial/property accounts, a long tail of one-off households)
  * 20 employees hired across the years, historical rates modeled with ~4%/yr
    raises, and OVERTIME RULES: hours beyond 8 in a DAY pay 1.5x. Peak-season
    jobs schedule longer days, so the OT premium is seasonal; estimates carry
    a matching seasonal allowance, so detected overruns are genuine variance
  * 2,500+ jobs (age-appropriate status mix), labor_logs for every worked job,
    financial_ledger with revenue/deposits, per-job expenses, monthly overhead
  * 5 engineered WHALE clients: top-quintile revenue, deeply negative net
    margin (labor blowouts + heavy materials spend + expensive acquisition)
    -- the prey of mv_customer_profitability.is_whale
  * planted anomalies: ~10 extra labor-overrun jobs, a lumber-price surge
    tripling October 2026 materials, and a marketing blitz in August 2026

Idempotent: truncates all tables (RESTART IDENTITY) before loading.
Run:  python3 seed_demo.py          (deterministic: random.seed(42))

Multi-tenancy: the simulation belongs to ONE client workspace -- the demo
company 'BrightPath Works (Demo)' (industry_vertical
'home_services_remodeling', socal_region NULL). The TRUNCATE list includes
companies (RESTART IDENTITY), so a reload always recreates it as company #1;
every client-table INSERT (job_types, employees, customers, jobs, labor_logs,
financial_ledger) sets company_id EXPLICITLY (documented choice: explicit
column values captured from the freshly inserted workspace row rather than a
backfill UPDATE, so the seed can never silently write unscoped rows).
market_benchmarks stays GLOBAL (no company_id).
"""

import datetime as dt
import os
import random

import psycopg2
from psycopg2.extras import execute_values

# Honors DATABASE_URL (used by the Docker stack: postgresql://postgres:postgres@db:5432/...)
DB = os.environ.get("DATABASE_URL",
                    "postgresql://postgres:postgres@localhost:5432/smb_warehouse")
rng = random.Random(42)

TODAY = dt.date.today()
CUR_MONTH = TODAY.replace(day=1)
N_MONTHS = 36                                        # 2023-12 .. 2026-11
TARGET_JOBS = 2500

# Multi-tenancy: the demo simulation is client workspace #1. The companies
# row is (re)created below with this exact name; every client-table INSERT
# carries the workspace's company_id explicitly (read back from the fresh
# INSERT -- identity 1 on a freshly truncated table).
DEMO_COMPANY_NAME = "BrightPath Works (Demo)"
DEMO_COMPANY_ID = 1            # expected id after TRUNCATE ... RESTART IDENTITY;
                               # main() verifies the actual row id at runtime


def u(a, b):
    return rng.uniform(a, b)


def pick(seq):
    return rng.choice(seq)


def money(x):
    return round(x, 2)


def month_starts():
    return [(CUR_MONTH - dt.timedelta(days=30 * i)).replace(day=1)
            for i in range(N_MONTHS - 1, -1, -1)]


MONTHS = month_starts()

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------
# Seasonal demand factor per calendar month (Jan..Dec), ~1.0 annual average.
JOB_TYPES = [
    # name, bill rate, typical duration h, seasonal factors Jan..Dec, popularity weight
    ("Interior Painting",  55.00, 24, [0.8, 1.1, 1.4, 1.5, 1.4, 1.1, 0.9, 1.0, 1.3, 1.3, 0.9, 0.6], 0.14),
    ("Exterior Painting",  58.00, 40, [0.1, 0.2, 0.5, 1.2, 1.7, 1.9, 1.8, 1.6, 1.2, 0.6, 0.2, 0.1], 0.12),
    ("Kitchen Remodel",    85.00, 120, [1.4, 1.5, 1.4, 1.2, 1.0, 0.8, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3], 0.10),
    ("Bathroom Remodel",   82.00, 80, [1.3, 1.4, 1.4, 1.2, 1.0, 0.9, 0.9, 1.0, 1.0, 1.1, 1.2, 1.3], 0.11),
    ("Deck & Fence Build", 70.00, 60, [0.1, 0.2, 0.6, 1.5, 1.9, 1.8, 1.6, 1.4, 0.9, 0.4, 0.1, 0.1], 0.10),
    ("Flooring Install",   65.00, 32, [1.0, 1.0, 1.1, 1.1, 1.1, 1.0, 1.0, 1.0, 1.1, 1.3, 1.4, 1.1], 0.12),
    ("Handyman Services",  48.00, 6,  [0.9, 0.9, 1.0, 1.0, 1.1, 1.1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.2], 0.20),
    ("Landscaping",        52.00, 20, [0.1, 0.2, 0.9, 1.5, 1.8, 1.7, 1.6, 1.5, 1.2, 0.7, 0.2, 0.1], 0.11),
]
PRICE_RANGE = {
    1: (1800, 6500), 2: (3500, 12000), 3: (15000, 42000), 4: (8000, 22000),
    5: (4500, 16000), 6: (2200, 9000), 7: (250, 1500), 8: (1200, 8000),
}  # informational only -- quoting derives from hours x bill rate x margin
REMODELISH = {3, 4, 5}
CREW_SIZE = {1: (1, 3), 2: (1, 3), 3: (2, 4), 4: (2, 4), 5: (2, 3),
             6: (1, 3), 7: (1, 2), 8: (2, 3)}

# Quoting model (keeps crew-hours in balance with the 20-person roster):
#   est_hours  = typical_duration_hours x size multiplier (0.5-1.5x normal,
#                1.8-2.6x whale jobs)
#   quoted     = est_hours x bill rate x margin multiplier (~1.5-2.1x)
#   est_labor  = est_hours x the actual crew's blended rate x (0.95-1.05)
# so labor COST (what the detector compares) and labor HOURS (what the roster
# can physically supply) both derive from the same hour estimate.

EMPLOYEES = [
    # first, last, role, rate as of the end of the simulation, hire year
    ("Marcus", "Webb",     "Owner / Project Manager", 95.00, 2018),
    ("Dana",   "Ortiz",    "Project Manager",         72.00, 2020),
    ("Eli",    "Thompson", "Lead Carpenter",          44.00, 2019),
    ("Priya",  "Raman",    "Lead Painter",            40.00, 2020),
    ("Kofi",   "Mensah",   "Lead Remodeler",          41.00, 2022),
    ("Chris",  "Yun",      "Carpenter",               36.00, 2021),
    ("Tom",    "Garrett",  "Flooring Technician",     32.00, 2021),
    ("Walt",   "Freeman",  "Handyman",                33.00, 2021),
    ("Jake",   "Simmons",  "Painter",                 29.00, 2022),
    ("Nora",   "Blake",    "Painter",                 28.00, 2022),
    ("Ivy",    "Chen",     "Landscape Technician",    26.00, 2023),
    ("Sam",    "Whitaker", "Carpenter",               35.00, 2023),
    ("Luis",   "Ortega",   "Painter",                 27.00, 2023),
    ("Beth",   "Holloway", "Project Coordinator",     31.00, 2024),
    ("Ryan",   "Duffy",    "Painter",                 26.00, 2024),
    ("Sofia",  "Marino",   "Designer / PM",           48.00, 2024),
    ("Amber",  "Cole",     "Apprentice",              20.00, 2025),
    ("Dan",    "Pierce",   "Landscape Technician",    24.00, 2025),
    ("Carmen", "Ruiz",     "Painter",                 27.00, 2025),
    ("Tessa",  "Nguyen",   "Apprentice",              19.00, 2026),
]
RAISE_PER_YEAR = 1.04          # ~4% annual raises, back-dated from "now"


def rate_at(emp_idx, work_date):
    """Historical hourly rate: base rate discounted back by years since hire."""
    hire_year = EMPLOYEES[emp_idx][4]
    years = max(0.0, (dt.date(work_date.year, 12, 1)
                      - dt.date(hire_year, 7, 1)).days / 365.25)
    return money(EMPLOYEES[emp_idx][3] / (RAISE_PER_YEAR ** years))


LAST_NAMES = [
    "Adams", "Baker", "Carter", "Diaz", "Evans", "Foster", "Garcia", "Hughes",
    "Ito", "Jensen", "Klein", "Larsen", "Meyer", "Novak", "Olsen", "Price",
    "Quinn", "Reyes", "Sato", "Tucker", "Vargas", "Watson", "Xu", "Young",
    "Zimmerman", "Barnes", "Caldwell", "Draper", "Ellison", "Franklin",
    "Grant", "Hoffman", "Irwin", "Jacobs", "Kaur", "Lowell", "Marsh", "Nolan",
    "Odom", "Pierce", "Ramirez", "Slater", "Turner", "Vaughn", "Walsh",
    "Yates", "Zhang", "Boone", "Crane", "Dwyer",
]
FIRST_NAMES = [
    "Aaron", "Bella", "Carlos", "Dana", "Elena", "Felix", "Grace", "Hassan",
    "Ingrid", "Jamal", "Kira", "Liam", "Maya", "Nathan", "Olivia", "Pedro",
    "Rachel", "Stefan", "Tara", "Umar", "Vera", "Wesley", "Ximena", "Yusuf",
    "Zoe", "Arthur", "Bianca", "Caleb", "Delia", "Ernest", "Faye", "Gavin",
]
COMMERCIAL_BASES = [
    "Cedar Ridge Apartments", "Silver Lake Bistro", "Brookfield Dental",
    "Summit Physical Therapy", "Hillcrest Animal Clinic", "Old Town Marketplace",
    "Lakeside Yoga Studio", "Riverton Brew Co", "Hartley & Sons Realty",
    "Fairview Montessori School", "Bellweather Condos", "Maple Street Cafe",
    "Northside Auto Detail", "Prairie Wind Brewery", "Stonebridge Law Group",
    "Redwood Dental Studio", "Lakeview Pharmacy", "Harbor Point Marina",
    "Union Street Barber", "Falcon Ridge Golf Club", "Copper Kettle Catering",
    "Blue Door Boutique", "Granite Peak Outfitters", "Willow Creek Vet Clinic",
    "Ironworks Fitness", "Meadowlark Florist", "Pinehurst Realty Group",
    "Cascade Coffee Roasters", "Juniper Day Spa", "Oak & Iron Furniture",
    "Bright Harbor Cleaning", "Tandem Bikes & Repair", "Meridian Insurance",
    "Harvest Table Restaurant", "Pinnacle Roofing Supply", "Casa Verde Nursery",
    "Serene Waters Pools", "Vantage Point Photography", "Lakeshore Laundromat",
    "Twin Pines Mobile Park",
]
CITY_STATE = [
    ("Oakdale", "MO"), ("Riverton", "MO"), ("Maplewood", "MO"),
    ("Cedar Springs", "MO"), ("Brookfield", "MO"), ("Fairview", "MO"),
    ("Lakewood", "MO"), ("Hillcrest", "MO"),
]
STREETS = ["Oak", "Maple", "Cedar", "Elm", "Birch", "Walnut", "Ash", " Willow"]

# Whales: 5 engineered accounts -- big-ticket remodels, blown budgets,
# expensive to win. One whale job per listed month (5 jobs each).
WHALE_MONTHS = {           # whale idx -> [month indices of their big jobs]
    0: [6, 12, 18, 24, 30],
    1: [7, 13, 19, 25, 29],
    2: [8, 14, 20, 26, 31],
    3: [9, 15, 21, 27, 32],
    4: [10, 16, 22, 28, 33],
}
WHALE_HOURS = (2.0, 2.8)       # est hours vs typical duration
WHALE_FACTOR = (2.4, 3.2)      # labor cost vs estimate
WHALE_MAT = (0.55, 0.75)       # materials as share of quoted price

# Extra planted labor overruns sprinkled through the eligible population.
# Chosen so the detected severity spreads across low/medium/high.
PLANTED_FACTORS = [1.87, 1.38, 1.62, 1.28, 1.52, 1.78, 1.42, 1.36, 1.70, 1.29]

# ---------------------------------------------------------------------------
# Overtime rule: hours beyond 8 in a DAY pay 1.5x. Peak-season jobs schedule
# fewer-but-longer days (≈10h vs ≈7h), so the OT premium is seasonal and
# bounded (<= ~15% of a job's labor cost), and estimates carry a matching
# seasonal allowance (PEAK_ALLOWANCE). Detected overruns are genuine variance,
# not a systematic tax on every job.
# ---------------------------------------------------------------------------
DAILY_OT_THRESHOLD = 8.0
PEAK_ALLOWANCE = 1.10


def add_labor(job_id, emp_idx, work_date, hours, desc, rows):
    """Append a labor log for `hours` on work_date; hours beyond 8 in the day
    are overtime at 1.5x the historical rate (rate_at)."""
    base = rate_at(emp_idx, work_date)
    regular = min(hours, DAILY_OT_THRESHOLD)
    ot = hours - regular
    if regular > 1e-9:
        rows.append((job_id, emp_idx + 1, work_date, money(regular), base, desc))
    if ot > 1e-9:
        rows.append((job_id, emp_idx + 1, work_date, money(ot),
                     money(base * 1.5), desc + " (overtime)"))


def workday(d):
    """Nudge weekend work dates back to Friday (crews mostly work weekdays)."""
    while d.weekday() >= 5:
        d -= dt.timedelta(days=1)
    return d


def main():
    conn = psycopg2.connect(DB)
    conn.autocommit = False
    cur = conn.cursor()

    # ---------------------------------------------------------------- clean
    # companies is included (RESTART IDENTITY): a reload always recreates the
    # demo workspace as company #1 (see DEMO_COMPANY_ID above).
    cur.execute("""
        TRUNCATE anomaly_alerts, market_benchmarks, labor_logs,
                 financial_ledger, jobs, employees, job_types, customers,
                 companies
        RESTART IDENTITY CASCADE
    """)

    # --------------------------------------------------------- demo company
    # Client workspace #1: the simulation's own business. Inserted by NAME
    # (the identity sequence yields 1 on the freshly truncated table); the
    # actual id is read back and used for every subsequent INSERT, and the
    # sequence is re-synced so a later workspace creation cannot collide.
    cur.execute("""
        INSERT INTO companies (company_name, industry_vertical, socal_region)
        VALUES (%s, %s, %s)
        RETURNING company_id
    """, (DEMO_COMPANY_NAME, "home_services_remodeling", None))
    company_id = cur.fetchone()[0]
    if company_id != DEMO_COMPANY_ID:
        raise SystemExit(
            f"seed_demo: the demo workspace must be company_id="
            f"{DEMO_COMPANY_ID} after TRUNCATE ... RESTART IDENTITY, got "
            f"{company_id}")
    cur.execute(
        "SELECT setval(pg_get_serial_sequence('companies', 'company_id'), %s)",
        (company_id,))

    # ------------------------------------------------------------- job types
    execute_values(cur, """
        INSERT INTO job_types (company_id, name, description, default_hourly_rate,
                               typical_duration_hours)
        VALUES %s
    """, [(company_id, n,
           f"{n} services for residential and light-commercial clients.",
           r, d) for n, r, d, _f, _w in JOB_TYPES])

    # ------------------------------------------------------------- employees
    emp_rows = []
    for i, (fn, ln, role, rate, hy) in enumerate(EMPLOYEES):
        emp_rows.append((company_id, fn, ln,
                         f"{fn.lower()}.{ln.lower()}@brightpathworks.com",
                         f"(555) 20{i:02d}-{rng.randint(1000, 9999)}",
                         dt.date(hy, rng.randint(1, 12), rng.randint(1, 28)),
                         rate, role))
    execute_values(cur, """
        INSERT INTO employees (company_id, first_name, last_name, email, phone,
                               hire_date, hourly_rate, role) VALUES %s
    """, emp_rows)

    # ------------------------------------------------------------- customers
    # 150 customers acquired across the window, with size skew handled at job
    # assignment time (weights + caps), not here.
    cust_specs = []                     # (name, contact, is_commercial, since_idx)
    used_names = set()

    def unique_name(base):
        n = base
        k = 2
        while n in used_names:
            n = f"{base} ({k})"
            k += 1
        used_names.add(n)
        return n

    # month-by-month acquisition plan: 24 legacy customers, then ~4/month
    since_plan = [9, 8, 7] + [4] * 31 + [2]
    commercial_bases = COMMERCIAL_BASES[:]
    rng.shuffle(commercial_bases)
    commercial_i = 0
    first_i = 0
    for mi, count in enumerate(since_plan):
        for _ in range(count):
            is_commercial = commercial_i < 55 and (mi < 6 or rng.random() < 0.35)
            if is_commercial:
                base = commercial_bases[commercial_i % len(commercial_bases)]
                commercial_i += 1
                suffix = pick([" LLC", " Inc", " Group", " Co", ""])
                name = unique_name(base + suffix)
                contact = f"{pick(FIRST_NAMES)} {pick(LAST_NAMES)}"
            else:
                if first_i < len(LAST_NAMES):
                    name = unique_name(f"{LAST_NAMES[first_i]} Residence")
                    first_i += 1
                else:
                    name = unique_name(f"{pick(FIRST_NAMES)} & "
                                       f"{pick(LAST_NAMES)} Residence")
                contact = f"{pick(FIRST_NAMES)} {pick(LAST_NAMES)}"
            cust_specs.append((name, contact, is_commercial, mi))

    cust_rows = []
    for idx, (name, contact, is_commercial, mi) in enumerate(cust_specs):
        slug = "".join(ch for ch in name.lower() if ch.isalnum())[:20]
        city, state = pick(CITY_STATE)
        day = rng.randint(1, 26)
        since = MONTHS[mi] + dt.timedelta(days=day)
        cust_rows.append((
            name, contact,
            f"{slug}.{idx}@example.com",
            f"(555) 3{rng.randint(10, 99)}-{rng.randint(1000, 9999)}",
            f"{rng.randint(100, 9800)} {pick(STREETS)} {'Ave' if is_commercial else 'St'}, "
            f"{city}, {state}",
            city, state, since,
        ))
    execute_values(cur, """
        INSERT INTO customers (company_id, name, contact_name, email, phone,
                               address, city, state, customer_since, is_active)
        VALUES %s
    """, [(company_id, r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], True)
          for r in cust_rows])

    # fetch customer ids back (insertion order == cust_specs order)
    cur.execute("SELECT id, name, customer_since FROM customers ORDER BY id")
    cust_by_id = {cid: (name, since) for cid, name, since in cur.fetchall()}
    cust_ids = list(cust_by_id)
    whale_ids = set(cust_ids[:5])      # first 5 customers become the whales

    # customer size weights + caps (dominant accounts vs long tail)
    size_w, size_cap = {}, {}
    for cid in cust_ids:
        mi = cust_specs[cid - 1][3]          # acquisition month
        is_com = cust_specs[cid - 1][2]
        if cid in whale_ids:
            size_w[cid], size_cap[cid] = 0.0, 0       # only their whale jobs
        elif mi < 3 and rng.random() < 0.12:
            size_w[cid], size_cap[cid] = u(8.0, 12.0), 45   # property/HOA accounts
        else:
            size_w[cid] = u(1.5, 4.5) if is_com else u(0.6, 1.6)
            size_cap[cid] = 30 if is_com else 12

    # ------------------------------------------------------------------ jobs
    # Monthly job counts: seasonal mix x growth, scaled to hit TARGET_JOBS.
    def season_mix(mi):
        mth = MONTHS[mi].month - 1
        return sum(w * f[mth] for _n, _r, _d, f, w in JOB_TYPES)

    growth = [1.015 ** mi for mi in range(N_MONTHS)]
    raw = [season_mix(mi) * growth[mi] for mi in range(N_MONTHS)]
    base_scale = TARGET_JOBS / sum(raw)
    plan = [max(6, round(base_scale * r)) for r in raw]
    while sum(plan) < TARGET_JOBS:                     # rounding safety net
        plan[max(range(N_MONTHS), key=lambda i: raw[i])] += 1

    whale_plan = {}                    # month idx -> [(whale_cust, job type)]
    for w_idx, mis in WHALE_MONTHS.items():
        w_cid = cust_ids[w_idx]
        for mi in mis:
            whale_plan.setdefault(mi, []).append((w_cid, pick([3, 4])))

    jobs_rows = []                     # (jn, cust_id, type, status, date, completion,
                                       #  quoted, est_labor, est_hours, notes)
    job_kind = {}                      # jn -> ('whale'|'normal', factor, mat_ratio|None)
    crew_of, crew_rate_of = {}, {}     # jn -> crew (emp idx list), blended rate
    seq_by_year = {}
    pools = []                         # per-month (ids, weights) for assignment
    job_count = {cid: 0 for cid in cust_ids}
    for mi in range(N_MONTHS):
        mstart = MONTHS[mi]
        pool = [(cid, size_w[cid]) for cid in cust_ids
                if (cust_by_id[cid][1].year, cust_by_id[cid][1].month)
                <= (mstart.year, mstart.month)
                and cid not in whale_ids]
        ids = [p[0] for p in pool]
        wts = [p[1] for p in pool]
        for _ in range(plan[mi]):
            if mi in whale_plan and whale_plan[mi]:
                w_cid, jt = whale_plan[mi].pop(0)
                is_whale_job = True
            else:
                w_cid = rng.choices(ids, weights=wts)[0]
                job_count[w_cid] += 1
                # heavy users hit their cap: fall back to a lighter customer
                tries = 0
                while job_count[w_cid] > size_cap[w_cid] and tries < 20:
                    w_cid = rng.choices(ids, weights=wts)[0]
                    job_count[w_cid] += 1
                    tries += 1
                jt = rng.choices(
                    list(range(1, 9)),
                    weights=[w for _n, _r, _d, f, w in JOB_TYPES])[0]
                mth = mstart.month - 1
                while JOB_TYPES[jt - 1][3][mth] < 0.2:      # no winter decks
                    jt = rng.choices(
                        list(range(1, 9)),
                        weights=[w for _n, _r, _d, f, w in JOB_TYPES])[0]
                is_whale_job = False

            age = N_MONTHS - 1 - mi
            if age >= 2:
                status = "completed" if rng.random() < 0.90 else "cancelled"
            elif age == 1:
                status = "completed" if rng.random() < 0.78 else (
                    "cancelled" if rng.random() < 0.35 else "in_progress")
            else:
                status = pick(["quoted", "scheduled", "in_progress"])
            if is_whale_job:
                status = "completed"

            day = rng.randint(1, 18 if age == 0 else 27)
            job_date = mstart + dt.timedelta(days=day)
            completion = None
            if status == "completed":
                # whale jobs run long (big remodels); everyone else 4-40 days
                span_days = rng.randint(40, 75) if is_whale_job else rng.randint(4, 40)
                completion = job_date + dt.timedelta(days=span_days)
                if completion >= TODAY:
                    completion = TODAY - dt.timedelta(days=1)

            # hours-based estimating (see header note): hours from the type's
            # typical duration, price from hours x bill rate x margin
            dft_rate, typ_h = JOB_TYPES[jt - 1][1], JOB_TYPES[jt - 1][2]
            if is_whale_job:
                est_hours = money(typ_h * u(*WHALE_HOURS))
                margin_mult = u(1.7, 2.1)
            else:
                # avg ~0.8x typical duration keeps roster utilization ~65% so
                # overtime only emerges in true peak season, not every week
                est_hours = money(max(1.0, typ_h * u(0.4, 1.2)))
                margin_mult = u(1.5, 2.1)
            quoted = money(est_hours * dft_rate * margin_mult * (1.003 ** mi))

            # estimate labor cost from the crew that will actually do the work
            hires = [i for i, e in enumerate(EMPLOYEES)
                     if dt.date(e[4], 1, 1) <= job_date]
            if not hires:
                hires = [0]
            crew_lo, crew_hi = CREW_SIZE[jt]
            crew = rng.sample(hires, min(rng.randint(crew_lo, crew_hi), len(hires)))
            avg_rate = sum(rate_at(i, job_date) for i in crew) / len(crew)
            peak_job = JOB_TYPES[jt - 1][3][job_date.month - 1] >= 1.3
            est_labor = money(est_hours * avg_rate * u(0.95, 1.05)
                              * (PEAK_ALLOWANCE if peak_job else 1.0))

            year = job_date.year
            seq_by_year[year] = seq_by_year.get(year, 0) + 1
            jn = f"JOB-{year}-{seq_by_year[year]:04d}"

            if is_whale_job:
                job_kind[jn] = ("whale", u(*WHALE_FACTOR), u(*WHALE_MAT))
            else:
                job_kind[jn] = ("normal", None, None)
            crew_of[jn] = crew
            crew_rate_of[jn] = avg_rate

            jobs_rows.append((
                jn, w_cid, jt, status, job_date, completion, quoted,
                est_labor, est_hours,
                f"{JOB_TYPES[jt - 1][0]} job",
            ))

    execute_values(cur, """
        INSERT INTO jobs (company_id, job_number, customer_id, job_type_id,
                          status, job_date, completion_date, quoted_price,
                          estimated_labor_cost, estimated_hours, notes)
        VALUES %s
    """, [(company_id,) + row for row in jobs_rows])

    cur.execute("SELECT id, job_number FROM jobs")
    job_ids = {jn: jid for jid, jn in cur.fetchall()}
    job_meta = {j[0]: j for j in jobs_rows}

    # --------------------------------- planted overruns (non-whale, eligible)
    eligible = sorted(jn for jn, j in job_meta.items()
                      if j[3] in ("completed", "in_progress")
                      and job_kind[jn][0] == "normal")
    for pos, fac in zip(
            [round(len(eligible) * f) for f in
             [0.04, 0.11, 0.18, 0.25, 0.32, 0.40, 0.47, 0.55, 0.63, 0.70]],
            PLANTED_FACTORS):
        job_kind[eligible[pos]] = ("normal", fac, None)

    # ------------------------------------------------------------ labor logs
    labor_rows = []
    for jn, j in job_meta.items():
        if j[3] not in ("completed", "in_progress"):
            continue
        jt_id, jdate, completion = j[2], j[4], j[5]
        kind, factor, _mat = job_kind[jn]
        crew = crew_of[jn]
        avg_rate = crew_rate_of[jn]

        target_cost = j[7] * (factor if factor is not None else u(0.82, 1.10))
        total_hours = round(max(1.0, target_cost / avg_rate) * 4) / 4

        end = completion or min(
            jdate + dt.timedelta(days=int(JOB_TYPES[jt_id - 1][2] * 0.6) + 6),
            TODAY - dt.timedelta(days=1))
        end = max(end, jdate)
        span = (end - jdate).days + 1
        # peak season: longer member-days -> daily overtime kicks in.
        # The whole crew works the same days; each member logs their own hours.
        peak = JOB_TYPES[jt_id - 1][3][jdate.month - 1] >= 1.3
        day_len = 9.5 if peak else 6.5                  # hours per member-day
        n_days = min(max(2, int(total_hours / (len(crew) * day_len)) + 1), span)

        work_days = sorted(rng.sample(range(span), n_days))
        day_shares = [rng.random() + 0.35 for _ in range(n_days)]
        norm = total_hours / sum(day_shares)
        for k, wd in enumerate(work_days):
            wdate = workday(jdate + dt.timedelta(days=wd))
            wdate = max(wdate, jdate)
            day_h = day_shares[k] * norm
            # each crew member takes a slice of the day (own workday, own rate)
            slices = [u(0.7, 1.3) for _ in crew]
            snorm = day_h / sum(slices)
            for i, e in enumerate(crew):
                h = round(slices[i] * snorm * 4) / 4
                if h > 0:
                    add_labor(job_ids[jn], e, wdate, min(h, 12.0),
                              "On-site work", labor_rows)

    execute_values(cur, """
        INSERT INTO labor_logs (company_id, job_id, employee_id, work_date, hours,
                                hourly_rate, description) VALUES %s
    """, [(company_id,) + row for row in labor_rows])

    # monthly labor cost from logs (drives the payroll ledger entries)
    cur.execute("""SELECT date_trunc('month', work_date)::date,
                          SUM(hours * hourly_rate) FROM labor_logs GROUP BY 1""")
    monthly_labor = {m: float(v) for m, v in cur.fetchall()}

    # -------------------------------------------------------- financial ledger
    surge_month = MONTHS[N_MONTHS - 2]          # last complete month
    blitz_month = MONTHS[32]                    # Aug of the final year
    ledger_rows = []
    for jn, j in job_meta.items():
        jid, jt_id, status = job_ids[jn], j[2], j[3]
        cust_id, quoted, jdate, cdate = j[1], float(j[6]), j[4], j[5]
        kind, _factor, mat_ratio = job_kind[jn]

        if status == "completed":
            ledger_rows.append((jid, None, "revenue", "job_billing",
                                cdate or (jdate + dt.timedelta(days=7)),
                                money(quoted * u(0.97, 1.06)),
                                f"Final billing - {jn}", None))
        elif status in ("scheduled", "in_progress"):
            ledger_rows.append((jid, None, "revenue", "job_deposit", jdate,
                                money(quoted * u(0.30, 0.40)),
                                f"Deposit - {jn}", None))

        if status in ("completed", "in_progress"):
            surge = jdate.replace(day=1) == surge_month
            mat_mult = 2.6 if surge else 1.0
            m_share = mat_ratio if mat_ratio is not None else u(0.16, 0.30)
            if quoted > 800 or rng.random() < 0.85:
                ledger_rows.append((jid, None, "expense", "materials", jdate,
                                    money(quoted * m_share * mat_mult),
                                    "Materials & supplies",
                                    pick(["TriCity Building Supply",
                                          "Metro Lumber",
                                          "Heartland Hardware Dist."])))
            if surge:                            # extra surge restock
                ledger_rows.append((jid, None, "expense", "materials", jdate,
                                    money(quoted * u(0.12, 0.22)),
                                    "Lumber & coating price-surge restock",
                                    "TriCity Building Supply"))
            if rng.random() < 0.45:
                ledger_rows.append((jid, None, "expense", "equipment", jdate,
                                    money(quoted * u(0.02, 0.05)),
                                    "Equipment rental", "ProRent Tools"))
            if jt_id in REMODELISH and rng.random() < 0.7:
                ledger_rows.append((jid, None, "expense", "permits", jdate,
                                    money(u(250, 900)),
                                    "City permits & inspections",
                                    "City of Oakdale"))
            if jt_id in REMODELISH and rng.random() < 0.25:
                ledger_rows.append((jid, None, "expense", "subcontractor", jdate,
                                    money(quoted * u(0.06, 0.14)),
                                    "Trade subcontractor",
                                    pick(["Bright Spark Electric LLC",
                                          "AquaPro Plumbing"])))

    # monthly overhead (job_id & customer_id NULL)
    for mstart in MONTHS:
        ledger_rows.append((None, None, "expense", "rent",
                            mstart + dt.timedelta(days=1), 4200.00,
                            "Shop & yard rent", "Oakdale Property Mgmt"))
        ledger_rows.append((None, None, "expense", "utilities",
                            mstart + dt.timedelta(days=8), money(u(450, 900)),
                            "Utilities", "City Power & Gas"))
        ledger_rows.append((None, None, "expense", "insurance",
                            mstart + dt.timedelta(days=5), 1450.00,
                            "Liability & vehicle insurance", "Midwest Mutual"))
        lm = monthly_labor.get(mstart, 0)
        ledger_rows.append((None, None, "expense", "labor",
                            mstart + dt.timedelta(days=27), money(lm * 1.12)
                            if lm else 2500.00,
                            "Payroll recharge (wages + burden)",
                            "Payroll Processing Co"))
        if mstart == blitz_month:
            ledger_rows.append((None, None, "expense", "marketing",
                                mstart + dt.timedelta(days=12), 9800.00,
                                "Summer campaign blitz (radio + mailers)",
                                "AdVantage Media"))
        else:
            ledger_rows.append((None, None, "expense", "marketing",
                                mstart + dt.timedelta(days=12),
                                money(u(700, 1800)), "Local advertising",
                                pick(["AdVantage Media", "DoorVault Mailers",
                                      "Google Ads"])))

    # customer acquisition costs (customer-attributed, job_id NULL)
    for idx, (name, _contact, is_com, mi) in enumerate(cust_specs):
        cid = cust_ids[idx]
        since = cust_by_id[cid][1]
        if cid in whale_ids:
            amt = u(2500, 4200)
        elif is_com:
            amt = u(800, 2500)
        else:
            amt = u(250, 900)
        ledger_rows.append((None, cid, "expense", "customer_acquisition",
                            since + dt.timedelta(days=rng.randint(1, 10)),
                            money(amt), "Customer acquisition cost",
                            pick(["AdVantage Media", "Google Ads",
                                  "DoorVault Mailers", "Referral program"])))
        # occasional win-back spend on established accounts
        if cid not in whale_ids and mi < 20 and rng.random() < 0.06:
            wb = min(mi + rng.randint(12, 20), N_MONTHS - 1)
            ledger_rows.append((None, cid, "expense", "customer_acquisition",
                                MONTHS[wb] + dt.timedelta(days=rng.randint(1, 10)),
                                money(u(400, 1200)), "Win-back campaign",
                                "DoorVault Mailers"))

    execute_values(cur, """
        INSERT INTO financial_ledger (company_id, job_id, customer_id,
                                      entry_type, category, entry_date,
                                      amount, description, vendor)
        VALUES %s
    """, [(company_id,) + row for row in ledger_rows])

    # ------------------------------------------------------ market benchmarks
    # Base price = typical hours x bill rate x ~1.8 margin, consistent with
    # how the simulation quotes jobs.
    bench_rows = []
    for jt_i, (name, rate, dur, _f, _w) in enumerate(JOB_TYPES, start=1):
        bp = dur * rate * 1.8
        for mi in range(N_MONTHS):
            drift = 1.003 ** mi
            bench_rows.append((jt_i, MONTHS[mi], "national",
                               money(bp * drift * u(0.96, 1.04)),
                               money(dur * 34.0 * drift * u(0.9, 1.1)),
                               round(u(30, 38), 2), "Regional Trade Composite"))
    execute_values(cur, """
        INSERT INTO market_benchmarks (job_type_id, period, region, avg_price,
              avg_labor_cost_per_job, avg_margin_pct, source) VALUES %s
    """, bench_rows)

    # ------------------------------------------------- refresh + detect
    cur.execute("ANALYZE")
    cur.execute("SELECT refresh_all_analytics()")
    cur.execute("SELECT * FROM detect_anomalies()")
    alerts = cur.fetchall()
    conn.commit()

    # ------------------------------------------------------------- summary
    for tbl in ("companies", "jobs", "financial_ledger", "labor_logs",
                "market_benchmarks", "customers", "employees"):
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        print(f"{tbl:>18}: {cur.fetchone()[0]}")
    print(f"{'workspace':>18}: {DEMO_COMPANY_NAME} (company_id="
          f"{DEMO_COMPANY_ID}, all client rows scoped to it)")

    ot_q = """
        SELECT e.first_name || ' ' || e.last_name,
               ROUND(SUM(ll.hours * ll.hourly_rate), 2) AS ot_cost,
               ROUND(SUM(ll.hours), 2) AS ot_hours
        FROM labor_logs ll
        JOIN employees e ON e.id = ll.employee_id
        WHERE ll.hourly_rate > e.hourly_rate * 1.2
        GROUP BY 1 ORDER BY ot_cost DESC LIMIT 5"""
    cur.execute(ot_q)
    print("\nTop overtime earners (1.5x beyond 8h/day):")
    for r in cur.fetchall():
        print(f"   {r[0]:18} OT hours={r[2]:>8}  OT cost=${r[1]:>10}")

    print(f"\n detect_anomalies() raised {len(alerts)} new alert(s):")
    for a in sorted(alerts, key=lambda x: ({"high": 0, "medium": 1, "low": 2}[x[3]], x[1])):
        print(f"   [{a[3]:6}] {a[1]:14} {a[2]:34} metric={a[4]} "
              f"baseline={a[5]} dev%={a[6]} z={a[7]}")

    cur.execute("""SELECT customer_name, revenue, total_cost, gross_margin,
                          acquisition_cost, job_count
                   FROM mv_customer_profitability WHERE is_whale
                   ORDER BY gross_margin""")
    whales = cur.fetchall()
    print(f"\n WHALE clients (top-quintile revenue, negative net margin): "
          f"{len(whales)}")
    for r in whales:
        print(f"   {r[0]:34} rev={r[1]:>12} cost={r[2]:>12} margin={r[3]:>12} "
              f"({r[4]:>9} CAC)  jobs={r[5]}")

    cur.execute("""SELECT job_type, revenue, margin_pct, job_count
                   FROM mv_job_type_profitability ORDER BY revenue DESC LIMIT 8""")
    print("\n mv_job_type_profitability:")
    for r in cur.fetchall():
        print(f"   {r[0]:22} rev={r[1]:>13} margin={str(r[2]):>7}%  jobs={r[3]}")

    cur.execute("""SELECT month_start, revenue, total_expense, gross_profit,
                          job_completions
                   FROM mv_monthly_summary ORDER BY month_start DESC LIMIT 6""")
    print("\n mv_monthly_summary (most recent months):")
    for r in cur.fetchall():
        print(f"   {r[0]}  rev={r[1]:>12} exp={r[2]:>12} profit={r[3]:>12}  "
              f"completions={r[4]}")

    conn.close()
    print("\nDone. 36-month simulation loaded.")


if __name__ == "__main__":
    main()
