"""
analysis/impact_estimator.py — Attach ₹ figures to every gap and waste signal.

Translates creative fatigue, angle gaps, format gaps, and refresh cycle issues
into monthly INR impact estimates so D2C founders think in rupees, not abstract
scores.

  run(brand_name, competitor_names, daily_spend_inr=None) → dict

CLI: python -m analysis.impact_estimator --brand "Mamaearth" --competitors "Plum,WOW Skin Science"
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import datetime
from pathlib import Path

from config import (
    ESTIMATED_DAILY_SPEND_PER_AD,
    FATIGUE_AD_MIN_DAYS,
    PROC_DIR,
    REFRESH_BENCHMARK_DAYS,
    SPRINT_PRICE_INR,
    get_connection,
)
from scrapers.utils import safe_brand_slug

logger = logging.getLogger(__name__)

# Andromeda decay: fatigued ads waste ~30-50% of budget; we use 35% (conservative)
_FATIGUE_WASTE_FRACTION = 0.35

# Refresh cycle: performance decay fraction per extra day beyond benchmark
_REFRESH_DECAY_FRACTION = 0.20

# Conservative multiplier for gap opportunity cost
_GAP_OPPORTUNITY_MULTIPLIER = 0.5


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def run(
    brand_name: str,
    competitor_names: list[str],
    daily_spend_inr: float | None = None,
) -> dict:
    """
    Estimate ₹ impact of creative gaps, fatigue, and missed opportunities.

    Parameters
    ----------
    daily_spend_inr : float or None
        Total daily ad spend in ₹.  If None, estimated from active ad count.
    """
    brand_row = _fetch_brand(brand_name)
    if not brand_row:
        raise ValueError(f"Brand '{brand_name}' not found in DB.")

    brand_id = brand_row["id"]
    client_ads = _fetch_active_ads(brand_id)
    active_count = len(client_ads)

    # ── Spend estimation ─────────────────────────────────────────────────────
    spend_info = estimate_daily_spend(active_count, daily_spend_inr)
    daily_spend = spend_info["amount"]
    daily_per_ad = daily_spend / active_count if active_count else ESTIMATED_DAILY_SPEND_PER_AD
    monthly_spend = daily_spend * 30

    # ── Load upstream data ───────────────────────────────────────────────────
    fatigue_data = _load_json(brand_name, "fatigue")
    intel_data = _load_json(brand_name, "category_intelligence")

    # ── Fatigue waste ────────────────────────────────────────────────────────
    fatigue_waste = calculate_fatigue_waste(fatigue_data, daily_per_ad)

    # ── Refresh cycle waste ──────────────────────────────────────────────────
    refresh_waste = calculate_refresh_waste(
        fatigue_data, daily_per_ad, active_count,
    )

    # ── Gap-based opportunity costs ──────────────────────────────────────────
    gaps = _build_gaps(intel_data, fatigue_data, client_ads, brand_row)
    angle_opp, format_opp, per_gap = _price_gaps(
        gaps, intel_data, fatigue_data, daily_per_ad,
    )

    total_waste = (
        fatigue_waste
        + angle_opp
        + format_opp
        + refresh_waste
    )

    # ── ROI of sprint ────────────────────────────────────────────────────────
    monthly_savings = total_waste
    payback_days = (
        round(SPRINT_PRICE_INR / (monthly_savings / 30), 1)
        if monthly_savings > 0 else 0.0
    )

    result = {
        "brand_name": brand_name,
        "generated_at": datetime.utcnow().isoformat(),
        "daily_spend": spend_info,
        "monthly_spend_estimate": round(monthly_spend, 0),
        "waste_breakdown": {
            "creative_fatigue_waste_monthly": round(fatigue_waste, 0),
            "angle_gap_opportunity_cost_monthly": round(angle_opp, 0),
            "format_gap_opportunity_cost_monthly": round(format_opp, 0),
            "refresh_cycle_waste_monthly": round(refresh_waste, 0),
            "total_estimated_monthly_waste": round(total_waste, 0),
        },
        "per_gap_impact": per_gap,
        "roi_of_sprint": {
            "sprint_cost": SPRINT_PRICE_INR,
            "estimated_monthly_savings": round(monthly_savings, 0),
            "payback_period_days": payback_days,
        },
    }

    _write_processed(brand_name, result)
    logger.info(
        "Impact estimate for '%s': ₹%.0f/month total waste, payback %.1f days",
        brand_name, total_waste, payback_days,
    )
    return result


# ══════════════════════════════════════════════════════════════════════════════
# Spend estimation
# ══════════════════════════════════════════════════════════════════════════════

def estimate_daily_spend(
    active_ad_count: int,
    daily_spend_inr: float | None = None,
) -> dict:
    """Return ``{"amount": float, "is_estimated": bool}``."""
    if daily_spend_inr is not None:
        return {"amount": float(daily_spend_inr), "is_estimated": False}
    estimated = active_ad_count * ESTIMATED_DAILY_SPEND_PER_AD
    return {"amount": float(estimated), "is_estimated": True}


# ══════════════════════════════════════════════════════════════════════════════
# Fatigue waste
# ══════════════════════════════════════════════════════════════════════════════

def calculate_fatigue_waste(
    fatigue_data: dict | None,
    daily_per_ad: float,
) -> float:
    """
    Monthly waste from ads running 30+ days (Andromeda decay).

    ``fatigued_count × daily_per_ad × 0.35 × 30``
    """
    if not fatigue_data:
        return 0.0
    fatigued_count = len(fatigue_data.get("critical_ads", []))
    return fatigued_count * daily_per_ad * _FATIGUE_WASTE_FRACTION * 30


# ══════════════════════════════════════════════════════════════════════════════
# Refresh cycle waste
# ══════════════════════════════════════════════════════════════════════════════

def calculate_refresh_waste(
    fatigue_data: dict | None,
    daily_per_ad: float,
    active_count: int,
) -> float:
    """
    Monthly waste from running ads beyond the optimal refresh window.

    If avg duration > 14 days:
        ``(avg_duration - REFRESH_BENCHMARK) × daily_per_ad × active_ads × 0.20 × 30 / avg_duration``
    """
    if not fatigue_data:
        return 0.0

    avg_duration = (
        fatigue_data
        .get("fatigue_index", {})
        .get("avg_duration", 0.0)
    )
    if avg_duration <= 14 or avg_duration == 0:
        return 0.0

    excess_days = avg_duration - REFRESH_BENCHMARK_DAYS
    return (
        excess_days
        * daily_per_ad
        * active_count
        * _REFRESH_DECAY_FRACTION
        * 30
        / avg_duration
    )


# ══════════════════════════════════════════════════════════════════════════════
# Gap builder (mirrors audit_generator._build_gaps logic)
# ══════════════════════════════════════════════════════════════════════════════

def _build_gaps(
    intel_data: dict | None,
    fatigue_data: dict | None,
    client_ads: list[dict],
    brand_row: dict,
) -> list[dict]:
    """Build creative gaps list from category intel + fatigue data."""
    intel_data = intel_data or {}
    fatigue_data = fatigue_data or {}
    gaps: list[dict] = []

    category = (brand_row.get("category") or "this category").title()

    # ── Angle gaps ───────────────────────────────────────────────────────────
    trigger_analysis = intel_data.get("trigger_analysis", {})
    client_triggers = set(
        fatigue_data.get("hook_diversity", {}).get("triggers_used", [])
    )
    profitable_triggers = trigger_analysis.get("by_profitable_only", {})
    total_pt = sum(profitable_triggers.values()) or 1

    for trigger, count in profitable_triggers.items():
        if trigger not in client_triggers:
            pct = round(count / total_pt * 100, 1)
            if pct >= 5:
                win_rate = trigger_analysis.get(
                    "profitable_rate_by_trigger", {}
                ).get(trigger, 0)
                gaps.append({
                    "type": "ANGLE GAP",
                    "title": f"Zero {trigger.replace('_', ' ').title()} Creatives",
                    "competitor_usage_pct": pct,
                    "win_rate": win_rate,
                })

    # ── Format gaps ──────────────────────────────────────────────────────────
    format_analysis = intel_data.get("format_analysis", {})
    active_ads = [a for a in client_ads if a.get("is_active")]
    total_client = len(active_ads) or 1
    client_fmt_counts = Counter(
        a.get("creative_type", "unknown") for a in active_ads
    )

    for fmt, fdata in format_analysis.items():
        if fmt == "unknown":
            continue
        client_pct = round(client_fmt_counts.get(fmt, 0) / total_client * 100, 1)
        winner_pct = fdata.get("winner_pct", 0)
        if client_pct == 0 and winner_pct >= 10:
            gaps.append({
                "type": "FORMAT GAP",
                "title": f"No {fmt.title()} Ads",
                "competitor_usage_pct": fdata.get("total_pct", 0),
                "win_rate": fdata.get("win_rate", 0),
            })

    # ── Hook structure gaps ──────────────────────────────────────────────────
    hook_analysis = intel_data.get("hook_structure_analysis", {})
    client_hooks = set(
        fatigue_data.get("hook_diversity", {}).get("hook_structures_used", [])
    )
    profitable_hooks = hook_analysis.get("by_profitable_only", {})
    total_hc = sum(profitable_hooks.values()) or 1

    for hook, count in profitable_hooks.items():
        if hook not in client_hooks:
            pct = round(count / total_hc * 100, 1)
            if pct >= 10:
                hook_win_rate = hook_analysis.get(
                    "profitable_rate_by_hook", {}
                ).get(hook, 0)
                gaps.append({
                    "type": "HOOK STRUCTURE GAP",
                    "title": f"No '{hook.replace('_', ' ').title()}' Hooks",
                    "competitor_usage_pct": pct,
                    "win_rate": hook_win_rate,
                })

    type_order = {"ANGLE GAP": 0, "FORMAT GAP": 1, "HOOK STRUCTURE GAP": 2}
    gaps.sort(key=lambda g: type_order.get(g["type"], 9))
    return gaps


# ══════════════════════════════════════════════════════════════════════════════
# Gap pricing
# ══════════════════════════════════════════════════════════════════════════════

def _price_gaps(
    gaps: list[dict],
    intel_data: dict | None,
    fatigue_data: dict | None,
    daily_per_ad: float,
) -> tuple[float, float, list[dict]]:
    """
    Attach ₹ impact to each gap.

    Returns (angle_total, format_total, per_gap_list).
    """
    intel_data = intel_data or {}
    fatigue_data = fatigue_data or {}

    avg_client_duration = (
        fatigue_data
        .get("fatigue_index", {})
        .get("avg_duration", 10.0)
    ) or 10.0

    avg_winner_duration = (
        intel_data
        .get("duration_analysis", {})
        .get("profitable_ads", {})
        .get("avg", 0)
    ) or 21.0

    angle_total = 0.0
    format_total = 0.0
    per_gap: list[dict] = []

    for gap in gaps:
        win_rate = gap.get("win_rate", 0)
        gap_type = gap["type"]

        # Extra days a winning ad from this gap would have run
        duration_gain = max(avg_winner_duration - avg_client_duration, 0)

        # Impact = duration_gain × daily_per_ad × 0.5 (conservative)
        # scaled by win_rate / 100 to weight higher-value gaps
        rate_factor = (win_rate / 100) if win_rate else 0.1
        monthly_impact = (
            duration_gain
            * daily_per_ad
            * _GAP_OPPORTUNITY_MULTIPLIER
            * rate_factor
            * 30
            / max(avg_winner_duration, 1)
        )
        monthly_impact = round(monthly_impact, 0)

        # Confidence
        if win_rate >= 40:
            confidence = "high"
        elif win_rate >= 20:
            confidence = "medium"
        else:
            confidence = "low"

        per_gap.append({
            "gap_type": gap_type,
            "gap_title": gap["title"],
            "estimated_monthly_impact_inr": monthly_impact,
            "confidence": confidence,
        })

        if gap_type == "ANGLE GAP":
            angle_total += monthly_impact
        elif gap_type in ("FORMAT GAP", "HOOK STRUCTURE GAP"):
            format_total += monthly_impact

    return angle_total, format_total, per_gap


# ══════════════════════════════════════════════════════════════════════════════
# Data loaders
# ══════════════════════════════════════════════════════════════════════════════

def _load_json(brand_name: str, suffix: str) -> dict | None:
    """Load a processed JSON file by brand name and suffix."""
    safe = brand_name.lower().replace(" ", "_")
    path = PROC_DIR / f"{safe}_{suffix}.json"
    if not path.exists():
        slug = safe_brand_slug(brand_name)
        path = PROC_DIR / f"{slug}_{suffix}.json"
    if not path.exists():
        logger.debug("No %s data found for '%s'", suffix, brand_name)
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to load %s for '%s': %s", suffix, brand_name, exc)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# DB helpers
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_brand(name: str) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id, name, category, is_client FROM brands WHERE name = ?",
            (name,),
        ).fetchone()
    return dict(row) if row else None


def _fetch_active_ads(brand_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM ads WHERE brand_id = ? AND is_active = 1",
            (brand_id,),
        ).fetchall()
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# Output
# ══════════════════════════════════════════════════════════════════════════════

def _write_processed(brand_name: str, data: dict) -> Path:
    PROC_DIR.mkdir(parents=True, exist_ok=True)
    slug = safe_brand_slug(brand_name)
    path = PROC_DIR / f"{slug}_impact_estimate.json"
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info("Impact estimate -> %s", path)
    return path


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def _cli() -> None:
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s -- %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        prog="python -m analysis.impact_estimator",
        description="Estimate ₹ impact of creative gaps and fatigue waste.",
    )
    parser.add_argument("--brand", required=True, help="Client brand name")
    parser.add_argument("--competitors", required=True,
                        help="Comma-separated competitor names")
    parser.add_argument("--daily-spend", type=float, default=None,
                        help="Total daily ad spend in ₹ (estimated if omitted)")
    args = parser.parse_args()

    competitors = [c.strip() for c in args.competitors.split(",") if c.strip()]
    result = run(args.brand, competitors, daily_spend_inr=args.daily_spend)

    wb = result["waste_breakdown"]
    roi = result["roi_of_sprint"]
    ds = result["daily_spend"]

    est_tag = " (estimated)" if ds["is_estimated"] else ""
    print(f"\n  Brand: {result['brand_name']}")
    print(f"  Daily spend: ₹{ds['amount']:,.0f}{est_tag}")
    print(f"  Monthly spend: ₹{result['monthly_spend_estimate']:,.0f}")
    print(f"\n  Waste breakdown:")
    print(f"    Fatigue waste:       ₹{wb['creative_fatigue_waste_monthly']:,.0f}/mo")
    print(f"    Angle gap cost:      ₹{wb['angle_gap_opportunity_cost_monthly']:,.0f}/mo")
    print(f"    Format gap cost:     ₹{wb['format_gap_opportunity_cost_monthly']:,.0f}/mo")
    print(f"    Refresh cycle waste: ₹{wb['refresh_cycle_waste_monthly']:,.0f}/mo")
    print(f"    TOTAL:               ₹{wb['total_estimated_monthly_waste']:,.0f}/mo")

    if result["per_gap_impact"]:
        print(f"\n  Per-gap impact:")
        for g in result["per_gap_impact"]:
            print(f"    {g['gap_title']}: ₹{g['estimated_monthly_impact_inr']:,.0f}/mo "
                  f"[{g['confidence']}]")

    print(f"\n  Sprint ROI:")
    print(f"    Sprint cost:      ₹{roi['sprint_cost']:,}")
    print(f"    Monthly savings:  ₹{roi['estimated_monthly_savings']:,.0f}")
    print(f"    Payback:          {roi['payback_period_days']} days")


if __name__ == "__main__":
    _cli()
