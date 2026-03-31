"""
analysis/competitor_deep_dive.py — Per-competitor breakdowns with detailed
winner ad dissections and competitive landscape summary.

  run(brand_name, competitor_names) → dict
      • Loads ads + analyses per competitor from DB
      • Builds per-competitor profiles: format mix, velocity, top winners
      • Generates data-backed "why_it_works" explanations (no LLM)
      • Writes data/processed/{brand_slug}_competitor_deep_dive.json

CLI: python -m analysis.competitor_deep_dive --brand "Mamaearth" --competitors "Plum,WOW Skin Science"
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import datetime
from pathlib import Path

from config import (
    PROC_DIR,
    PROFITABLE_AD_MIN_DAYS,
    get_connection,
)
from scrapers.utils import safe_brand_slug

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def run(brand_name: str, competitor_names: list[str]) -> dict:
    """
    Build per-competitor deep-dive intelligence.

    Returns the full deep-dive dict and writes JSON to processed dir.
    """
    if not competitor_names:
        raise ValueError("At least one competitor name is required.")

    category_intel = _load_category_intel(brand_name)

    per_competitor: dict[str, dict] = {}

    for comp_name in competitor_names:
        profile = _build_competitor_profile(comp_name, category_intel)
        per_competitor[comp_name] = profile
        if profile["active_ads"] == 0:
            logger.info(
                "Competitor '%s' has 0 active ads — skipping detailed analysis",
                comp_name,
            )

    landscape = _build_landscape_summary(per_competitor)

    result = {
        "brand_name": brand_name,
        "generated_at": datetime.utcnow().isoformat(),
        "competitive_landscape": landscape,
        "per_competitor": per_competitor,
    }

    _write_processed(brand_name, result)
    logger.info(
        "Competitor deep dive built for '%s': %d competitors analysed",
        brand_name, len(per_competitor),
    )
    return result


# ══════════════════════════════════════════════════════════════════════════════
# Per-competitor profile
# ══════════════════════════════════════════════════════════════════════════════

def _build_competitor_profile(comp_name: str, category_intel: dict | None) -> dict:
    """Build a full profile for a single competitor."""
    brand_row = _fetch_brand(comp_name)
    if not brand_row:
        logger.warning("Competitor '%s' not found in DB — returning empty profile", comp_name)
        return _empty_profile()

    brand_id = brand_row["id"]
    ads = _fetch_ads(brand_id)
    ad_ids = [a["id"] for a in ads]
    analyses = _fetch_analyses(ad_ids) if ad_ids else []
    analysis_by_ad_id = {a["ad_id"]: a for a in analyses}

    active_ads = [a for a in ads if a.get("is_active")]
    profitable_ads = [
        a for a in ads
        if a.get("duration_days") is not None
        and a["duration_days"] >= PROFITABLE_AD_MIN_DAYS
    ]

    format_mix = _compute_format_mix(active_ads)
    velocity = compute_creative_velocity(ads)
    dominant_trigger = _dominant_value(analyses, "psychological_trigger")
    dominant_hook = _dominant_value(analyses, "hook_structure")

    # Top 5 winners by duration
    profitable_sorted = sorted(
        profitable_ads,
        key=lambda a: a.get("duration_days") or 0,
        reverse=True,
    )
    top_winners = [
        _build_winner_detail(ad, analysis_by_ad_id.get(ad["id"]), category_intel)
        for ad in profitable_sorted[:5]
    ]

    total = len(active_ads)
    profitable_count = len(profitable_ads)

    return {
        "active_ads": total,
        "profitable_ads": profitable_count,
        "win_rate": round(profitable_count / total * 100, 1) if total else 0.0,
        "format_mix": format_mix,
        "dominant_trigger": dominant_trigger,
        "dominant_hook_structure": dominant_hook,
        "creative_velocity_per_week": velocity,
        "top_winners": top_winners,
    }


def _empty_profile() -> dict:
    return {
        "active_ads": 0,
        "profitable_ads": 0,
        "win_rate": 0.0,
        "format_mix": {},
        "dominant_trigger": None,
        "dominant_hook_structure": None,
        "creative_velocity_per_week": 0.0,
        "top_winners": [],
    }


# ══════════════════════════════════════════════════════════════════════════════
# Winner detail builder
# ══════════════════════════════════════════════════════════════════════════════

def _build_winner_detail(
    ad: dict,
    analysis: dict | None,
    category_intel: dict | None,
) -> dict:
    """Build a detailed breakdown dict for a single winning ad."""
    ad_copy = ad.get("ad_copy") or ""
    hook_text = ad_copy.split("\n")[0].strip()[:200] if ad_copy else ""

    trigger = (analysis or {}).get("psychological_trigger") or "unknown"
    hook_structure = (analysis or {}).get("hook_structure") or "unknown"
    copy_tone = (analysis or {}).get("copy_tone")
    visual_layout = (analysis or {}).get("visual_layout")

    # Extract fields from analysis_json
    spoken_hook = None
    effectiveness_score = None
    analysis_json_raw = (analysis or {}).get("analysis_json")
    if analysis_json_raw:
        try:
            aj = json.loads(analysis_json_raw) if isinstance(analysis_json_raw, str) else analysis_json_raw
            spoken_hook = aj.get("spoken_hook")
            effectiveness_score = aj.get("effectiveness_score")
        except (json.JSONDecodeError, TypeError):
            pass

    duration = ad.get("duration_days") or 0

    why = build_why_it_works(
        trigger=trigger,
        hook_structure=hook_structure,
        duration_days=duration,
        category_intel=category_intel,
    )

    return {
        "ad_library_id": ad.get("ad_library_id", ""),
        "duration_days": duration,
        "creative_type": ad.get("creative_type"),
        "full_hook_text": hook_text,
        "full_ad_copy": ad_copy[:500],
        "psychological_trigger": trigger,
        "hook_structure": hook_structure,
        "copy_tone": copy_tone,
        "visual_layout": visual_layout,
        "cta_type": ad.get("cta_type"),
        "spoken_hook": spoken_hook,
        "effectiveness_score": effectiveness_score,
        "why_it_works": why,
        "thumbnail_url": ad.get("thumbnail_url"),
    }


# ══════════════════════════════════════════════════════════════════════════════
# "Why it works" generator (no LLM — pure data)
# ══════════════════════════════════════════════════════════════════════════════

def build_why_it_works(
    trigger: str | None,
    hook_structure: str | None,
    duration_days: int,
    category_intel: dict | None,
) -> str:
    """
    Generate a 2-3 sentence data-backed explanation for why an ad works.

    Uses trigger win rates, hook structure win rates, and duration averages
    from category_intel. Falls back to duration-only explanation when
    category_intel is unavailable.
    """
    parts: list[str] = []

    trigger_rate = None
    hook_rate = None
    avg_duration = None

    if category_intel:
        trigger_rates = (
            category_intel
            .get("trigger_analysis", {})
            .get("profitable_rate_by_trigger", {})
        )
        trigger_rate = trigger_rates.get(trigger) if trigger else None

        hook_rates = (
            category_intel
            .get("hook_structure_analysis", {})
            .get("profitable_rate_by_hook", {})
        )
        hook_rate = hook_rates.get(hook_structure) if hook_structure else None

        avg_duration = (
            category_intel
            .get("duration_analysis", {})
            .get("all_ads", {})
            .get("avg")
        )

    # Sentence 1: trigger + hook
    if trigger and trigger_rate is not None and hook_structure and hook_rate is not None:
        parts.append(
            f"This ad uses a {trigger.replace('_', ' ')} trigger "
            f"({trigger_rate:.0f}% win rate in category) with a "
            f"{hook_structure.replace('_', ' ')} hook structure "
            f"({hook_rate:.0f}% win rate)."
        )
    elif trigger and trigger_rate is not None:
        parts.append(
            f"This ad uses a {trigger.replace('_', ' ')} trigger "
            f"({trigger_rate:.0f}% win rate in category)."
        )
    elif trigger:
        parts.append(
            f"This ad uses a {trigger.replace('_', ' ')} trigger."
        )

    # Sentence 2: duration vs average
    if avg_duration and avg_duration > 0 and duration_days > 0:
        multiplier = duration_days / avg_duration
        parts.append(
            f"At {duration_days} days running, it's {multiplier:.1f}x the "
            f"category average duration of {avg_duration:.0f} days, indicating "
            f"strong creative-market fit."
        )
    elif duration_days > 0:
        parts.append(
            f"Running for {duration_days} days indicates sustained performance "
            f"above the profitability threshold."
        )

    return " ".join(parts) if parts else "Insufficient data for analysis."


# ══════════════════════════════════════════════════════════════════════════════
# Creative velocity
# ══════════════════════════════════════════════════════════════════════════════

def compute_creative_velocity(ads: list[dict]) -> float:
    """
    Estimate new ads per week from start_date distribution.

    Groups ads by ISO year-month, counts per month, then converts
    the average monthly rate to weekly.
    """
    months: Counter = Counter()
    for ad in ads:
        sd = ad.get("start_date")
        if not sd:
            continue
        # start_date is YYYY-MM-DD
        month_key = sd[:7]  # "YYYY-MM"
        months[month_key] += 1

    if not months:
        return 0.0

    avg_per_month = sum(months.values()) / len(months)
    return round(avg_per_month / 4.33, 1)  # ~4.33 weeks per month


# ══════════════════════════════════════════════════════════════════════════════
# Competitive landscape summary
# ══════════════════════════════════════════════════════════════════════════════

def _build_landscape_summary(per_competitor: dict[str, dict]) -> dict:
    """Aggregate metrics across all competitors."""
    if not per_competitor:
        return {
            "total_competitor_ads": 0,
            "total_profitable_across_competitors": 0,
            "avg_competitor_ad_count": 0.0,
            "most_aggressive_competitor": None,
            "dominant_format_across_competitors": None,
            "dominant_trigger_across_competitors": None,
            "creative_velocity_leader": None,
        }

    total_ads = sum(c["active_ads"] for c in per_competitor.values())
    total_profitable = sum(c["profitable_ads"] for c in per_competitor.values())
    n = len(per_competitor)

    # Most aggressive = most active ads
    most_aggressive_name = max(
        per_competitor, key=lambda k: per_competitor[k]["active_ads"]
    )
    ma = per_competitor[most_aggressive_name]
    most_aggressive = {
        "name": most_aggressive_name,
        "active_ads": ma["active_ads"],
        "profitable_pct": ma["win_rate"],
    }

    # Dominant format across competitors
    all_formats: Counter = Counter()
    for comp in per_competitor.values():
        for fmt, data in comp.get("format_mix", {}).items():
            all_formats[fmt] += data.get("count", 0)
    dominant_format = all_formats.most_common(1)[0][0] if all_formats else None

    # Dominant trigger across competitors
    trigger_counts: Counter = Counter()
    for comp in per_competitor.values():
        t = comp.get("dominant_trigger")
        if t:
            trigger_counts[t] += 1
    dominant_trigger = trigger_counts.most_common(1)[0][0] if trigger_counts else None

    # Velocity leader
    velocity_leader_name = max(
        per_competitor, key=lambda k: per_competitor[k]["creative_velocity_per_week"]
    )
    vl = per_competitor[velocity_leader_name]
    velocity_leader = {
        "name": velocity_leader_name,
        "estimated_new_per_week": vl["creative_velocity_per_week"],
    }

    return {
        "total_competitor_ads": total_ads,
        "total_profitable_across_competitors": total_profitable,
        "avg_competitor_ad_count": round(total_ads / n, 1),
        "most_aggressive_competitor": most_aggressive,
        "dominant_format_across_competitors": dominant_format,
        "dominant_trigger_across_competitors": dominant_trigger,
        "creative_velocity_leader": velocity_leader,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _compute_format_mix(ads: list[dict]) -> dict:
    """Count and percentage for each creative_type."""
    counts = Counter(a.get("creative_type") or "unknown" for a in ads)
    total = len(ads) or 1
    return {
        fmt: {"count": c, "pct": round(c / total * 100, 1)}
        for fmt, c in counts.most_common()
    }


def _dominant_value(analyses: list[dict], field: str) -> str | None:
    """Return the most common non-null value for *field* across analyses."""
    values = [a[field] for a in analyses if a.get(field)]
    if not values:
        return None
    return Counter(values).most_common(1)[0][0]


def _load_category_intel(brand_name: str) -> dict | None:
    """Load category_intelligence JSON if available."""
    slug = safe_brand_slug(brand_name)
    # category_intel uses lowercase + underscore, not the safe slug
    safe_name = brand_name.lower().replace(" ", "_")
    path = PROC_DIR / f"{safe_name}_category_intelligence.json"
    if not path.exists():
        # Try slug-based name as fallback
        path = PROC_DIR / f"{slug}_category_intelligence.json"
    if not path.exists():
        logger.debug("No category intel found for '%s'", brand_name)
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        logger.info("Loaded category intel from %s", path)
        return data
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to load category intel: %s", exc)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# DB helpers
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_brand(name: str) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id, name, is_client FROM brands WHERE name = ?",
            (name,),
        ).fetchone()
    return dict(row) if row else None


def _fetch_ads(brand_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM ads WHERE brand_id = ?", (brand_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def _fetch_analyses(ad_ids: list[int]) -> list[dict]:
    if not ad_ids:
        return []
    placeholders = ",".join("?" * len(ad_ids))
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT * FROM ad_analysis WHERE ad_id IN ({placeholders})", ad_ids
        ).fetchall()
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# Output
# ══════════════════════════════════════════════════════════════════════════════

def _write_processed(brand_name: str, data: dict) -> Path:
    PROC_DIR.mkdir(parents=True, exist_ok=True)
    slug = safe_brand_slug(brand_name)
    path = PROC_DIR / f"{slug}_competitor_deep_dive.json"
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info("Competitor deep dive -> %s", path)
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
        prog="python -m analysis.competitor_deep_dive",
        description="Per-competitor deep dive with winner ad dissections.",
    )
    parser.add_argument("--brand", required=True, help="Client brand name")
    parser.add_argument("--competitors", required=True,
                        help="Comma-separated competitor names")
    args = parser.parse_args()

    competitors = [c.strip() for c in args.competitors.split(",") if c.strip()]
    result = run(args.brand, competitors)

    ls = result["competitive_landscape"]
    print(f"\n  Landscape: {ls['total_competitor_ads']} competitor ads, "
          f"{ls['total_profitable_across_competitors']} profitable")
    if ls.get("most_aggressive_competitor"):
        ma = ls["most_aggressive_competitor"]
        print(f"  Most aggressive: {ma['name']} ({ma['active_ads']} ads, "
              f"{ma['profitable_pct']}% win rate)")
    if ls.get("creative_velocity_leader"):
        vl = ls["creative_velocity_leader"]
        print(f"  Velocity leader: {vl['name']} ({vl['estimated_new_per_week']}/wk)")

    for name, profile in result["per_competitor"].items():
        print(f"\n  {name}:")
        print(f"    Active: {profile['active_ads']}, "
              f"Profitable: {profile['profitable_ads']} "
              f"({profile['win_rate']}%)")
        print(f"    Trigger: {profile['dominant_trigger']}, "
              f"Hook: {profile['dominant_hook_structure']}")
        print(f"    Velocity: {profile['creative_velocity_per_week']}/wk")
        for w in profile["top_winners"][:3]:
            print(f"    Winner: {w['ad_library_id']} — {w['duration_days']}d — "
                  f"{w['full_hook_text'][:60]}...")

    slug = safe_brand_slug(args.brand)
    print(f"\nOutput: {PROC_DIR / slug}_competitor_deep_dive.json")


if __name__ == "__main__":
    _cli()
