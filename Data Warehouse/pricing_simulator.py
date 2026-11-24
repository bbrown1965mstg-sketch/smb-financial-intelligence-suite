#!/usr/bin/env python3
"""
pricing_simulator.py -- what-if gross-margin math for a single job quote.

PURE MATH: no database, no I/O, fully unit-testable (the QA suite in
test_schema.py exercises it with hand-computed fixtures). The dashboard's
"Pricing simulator" tab pre-fills its arguments from live warehouse data
(mv_job_type_profitability, labor_logs, financial_ledger) and calls
simulate_job() -- the SAME function a script would call, so dashboard and
CLI stay consistent.

The simulator answers one question: "if I quote THIS job at THIS price with
THIS crew mix, what gross margin do I keep?" -- including the two levers that
quietly eat SMB margins: overtime premiums and material price swings.

def simulate_job(hours, base_rate, ot_pct, material_ratio, material_change_pct,
                 price, other_cost_ratio=0.05, target_margin=0.30,
                 market_margin=None) -> dict

METHOD (documented exactly)
---------------------------
  labor     = hours * base_rate * (1 + 0.5 * ot_pct)
              The overtime premium applies 1.5x pay to the ot_pct SHARE of
              total hours: 0.8 hours at base rate + 0.2 hours at 1.5x rate
              = hours * rate * (1 + 0.5 * ot_pct). ot_pct=0.30 means "30% of
              the crew's hours are overtime-eligible".
  materials = price * material_ratio * (1 + material_change_pct)
              material_ratio is materials cost as a fraction of price
              (e.g. 0.30 = materials usually run 30% of the quote);
              material_change_pct is a supplier-price swing, e.g. -0.10
              (10% cheaper) or +0.50 (lumber surge).
  other     = price * other_cost_ratio       (equipment, permits, disposal,
              subcontractor margin -- default 5% of price)
  gross_margin = price - labor - materials - other
  margin_pct   = gross_margin / price        (returned as a PERCENT, e.g. 35.2)
  margin_vs_target_pct = margin_pct - target_margin*100
              expressed in PERCENTAGE POINTS (e.g. +5.2 means the quote keeps
              5.2pp more than the target margin); target_margin is a FRACTION
              (default 0.30 = 30%).
  margin_vs_market_pct = margin_pct - market_margin*100
              same percentage-point convention against the market benchmark's
              avg_margin_pct for the service line (pass the fraction, e.g.
              0.3632 for a 36.32% benchmark); None when market_margin is None.

VALIDATION AND CLAMPS (documented behaviour)
--------------------------------------------
  * price > 0 and hours > 0 are REQUIRED: a non-positive price or a
    non-positive hours value raises ValueError (a quote without work or
    without revenue is meaningless, and dividing by zero price would poison
    every downstream percentage).
  * ot_pct       clamped to [0, 0.5]    (50% of hours on overtime is the
                  practical ceiling; negative overtime is meaningless).
  * material_change_pct clamped to [-1.0, 3.0]  (materials can drop at most
                  to free (-100%) and the clamp keeps a +300% lumber-spike
                  scenario expressible while bounding nonsense inputs).
  * material_ratio / other_cost_ratio / target_margin / market_margin are NOT
    clamped -- they are analyst inputs whose odd values should surface in the
    numbers rather than be silently corrected. (material_ratio < 0 or
    other_cost_ratio < 0 simply produce negative costs; target_margin is used
    only for the comparison columns.)
  * Clamped inputs are reported: the returned dict carries a "clamps_applied"
    list, e.g. ["ot_pct clamped 0.80 -> 0.50"] -- so a caller can tell when
    the simulator overrode it.

Returned dict keys
------------------
  hours, base_rate, ot_pct (clamped), material_ratio, material_change_pct
  (clamped), price, other_cost_ratio, target_margin, market_margin,
  labor_cost, material_cost, other_cost, total_cost, gross_margin,
  margin_pct (percent), target_margin_pct, margin_vs_target_pct (pp),
  market_margin_pct (percent or None), margin_vs_market_pct (pp or None),
  clamps_applied (list of strings, empty when nothing was clamped)
"""

OT_CLAMP = (0.0, 0.5)
MATERIAL_CHANGE_CLAMP = (-1.0, 3.0)


def _clamp(value, lo, hi):
    return max(lo, min(hi, value))


def simulate_job(hours, base_rate, ot_pct, material_ratio, material_change_pct,
                 price, other_cost_ratio=0.05, target_margin=0.30,
                 market_margin=None):
    """Simulate one job quote's gross margin. See the module docstring for the
    exact method, clamps and the returned keys."""
    hours = float(hours)
    price = float(price)
    if price <= 0:
        raise ValueError(f"price must be > 0, got {price!r}")
    if hours <= 0:
        raise ValueError(f"hours must be > 0, got {hours!r}")

    clamps = []
    ot_pct_in = float(ot_pct)
    ot_pct = _clamp(ot_pct_in, *OT_CLAMP)
    if ot_pct != ot_pct_in:
        clamps.append(f"ot_pct clamped {ot_pct_in:.4g} -> {ot_pct:.4g}")

    change_in = float(material_change_pct)
    material_change_pct = _clamp(change_in, *MATERIAL_CHANGE_CLAMP)
    if material_change_pct != change_in:
        clamps.append(
            f"material_change_pct clamped {change_in:.4g} -> {material_change_pct:.4g}")

    base_rate = float(base_rate)
    material_ratio = float(material_ratio)
    other_cost_ratio = float(other_cost_ratio)
    target_margin = float(target_margin)
    market_margin = None if market_margin is None else float(market_margin)

    # Labor: OT premium = 1.5x pay for the ot_pct share of hours, i.e. an
    # effective (1 + 0.5 * ot_pct) multiplier on the whole hours base.
    labor_cost = hours * base_rate * (1.0 + 0.5 * ot_pct)
    material_cost = price * material_ratio * (1.0 + material_change_pct)
    other_cost = price * other_cost_ratio

    total_cost = labor_cost + material_cost + other_cost
    gross_margin = price - total_cost
    margin_pct = gross_margin / price * 100.0

    target_margin_pct = target_margin * 100.0
    market_margin_pct = None if market_margin is None else market_margin * 100.0

    return {
        # inputs (post-clamp, so the dict round-trips)
        "hours": hours,
        "base_rate": base_rate,
        "ot_pct": ot_pct,
        "material_ratio": material_ratio,
        "material_change_pct": material_change_pct,
        "price": price,
        "other_cost_ratio": other_cost_ratio,
        "target_margin": target_margin,
        "market_margin": market_margin,
        # breakdown
        "labor_cost": labor_cost,
        "material_cost": material_cost,
        "other_cost": other_cost,
        "total_cost": total_cost,
        "gross_margin": gross_margin,
        # comparisons (percentage points)
        "margin_pct": margin_pct,
        "target_margin_pct": target_margin_pct,
        "margin_vs_target_pct": margin_pct - target_margin_pct,
        "market_margin_pct": market_margin_pct,
        "margin_vs_market_pct": (None if market_margin_pct is None
                                 else margin_pct - market_margin_pct),
        "clamps_applied": clamps,
    }


if __name__ == "__main__":
    # Tiny smoke example (not a test -- the tests live in test_schema.py):
    # 40h job at $52/hr blended, 20% OT hours, materials at 30% of quote with
    # a +10% supplier swing, quoted at $9,000, vs a 36.32% market margin.
    import json

    demo = simulate_job(hours=40, base_rate=52, ot_pct=0.20,
                        material_ratio=0.30, material_change_pct=0.10,
                        price=9000, market_margin=0.3632)
    print(json.dumps({k: (round(v, 4) if isinstance(v, float) else v)
                      for k, v in demo.items()}, indent=2))
