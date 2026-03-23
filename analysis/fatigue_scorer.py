"""
analysis/fatigue_scorer.py

Computes creative fatigue for a CLIENT brand, benchmarked against competitors.

fatigue_score 0–100 — higher is worse.

Signal components and their maximum contribution:
  critical_ads_penalty         40 pts  (ads running 30+ days, 10 pts each, cap 40)
  warning_ads_penalty          15 pts  (ads running 14–29 days, 3 pts each, cap 15)
  format_concentration_penalty 25 pts  (single-format dominance > 60%)
  count_deficit_penalty        10 pts  (client has < 50% of avg competitor ad count)
  recency_penalty              10 pts  (days since last new creative launched)

  run(brand_name, competitor_names) → dict
      • Populates the waste_reports table
      • Writes data/processed/{brand_name}_fatigue.json
      • Returns the fatigue data dict
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from config import (
    FATIGUE_AD_MIN_DAYS,
    PROC_DIR,
    PROFITABLE_AD_MIN_DAYS,
    VALID_CREATIVE_TYPES,
    get_connection,
)

logger = logging.getLogger(__name__)

_WARNING_MIN_DAYS  = 14   # ads in 14–29 days range = warning zone
_CRITICAL_MIN_DAYS = FATIGUE_AD_MIN_DAYS   # 30 days
_CRITICAL_PTS_PER_AD   = 10
_CRITICAL_CAP          = 40
_WARNING_PTS_PER_AD    = 3
_WARNING_CAP           = 15
_CONCENTRATION_CAP     = 25
_CONCENTRATION_TRIGGER = 0.60   # dominant format > 60% → penalty kicks in
_COUNT_DEFICIT_PTS     = 10
_RECENCY_CAP           = 10
_RECENCY_THRESHOLD     = 21    # days; beyond this, recency penalty grows


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def run(brand_name: str, competitor_names: list[str]) -> dict:
    """
    Score creative fatigue for *brand_name* (client), benchmarked against
    *competitor_names*.

    Returns the full fatigue data dict.
    """
    all_names  = [brand_name] + list(competitor_names)
    brand_rows = _fetch_brands(all_names)
    name_to_id = {b["name"]: b["id"] for b in brand_rows}

    client_id = name_to_id.get(brand_name)
    if client_id is None:
        raise ValueError(f"Brand '{brand_name}' not found in DB. Run ingest first.")

    # ── Client brand analysis ─────────────────────────────────────────────────
    client_ads        = _fetch_active_ads(client_id)
    critical_ads      = _ads_in_range(client_ads, _CRITICAL_MIN_DAYS, None)
    warning_ads       = _ads_in_range(client_ads, _WARNING_MIN_DAYS, _CRITICAL_MIN_DAYS)
    format_mix        = _format_mix(client_ads)
    days_since_new    = _days_since_last_new_creative(client_ads)

    # ── Competitor benchmarking ────────────────────────────────────────────────
    competitor_ad_counts: list[int] = []
    for name in competitor_names:
        bid = name_to_id.get(name)
        if bid:
            competitor_ad_counts.append(len(_fetch_active_ads(bid)))

    competitor_avg_count = (
        round(sum(competitor_ad_counts) / len(competitor_ad_counts), 1)
        if competitor_ad_counts else 0.0
    )

    # ── Score components ───────────────────────────────────────────────────────
    critical_penalty      = _critical_penalty(critical_ads)
    warning_penalty       = _warning_penalty(warning_ads)
    concentration_penalty = _concentration_penalty(format_mix, len(client_ads))
    count_deficit_penalty = _count_deficit_penalty(len(client_ads), competitor_avg_count)
    recency_penalty       = _recency_penalty(days_since_new)

    fatigue_score = round(
        min(
            100,
            critical_penalty
            + warning_penalty
            + concentration_penalty
            + count_deficit_penalty
            + recency_penalty,
        ),
        1,
    )

    breakdown = {
        "critical_ads_penalty":         critical_penalty,
        "warning_ads_penalty":          warning_penalty,
        "format_concentration_penalty": concentration_penalty,
        "count_deficit_penalty":        count_deficit_penalty,
        "recency_penalty":              recency_penalty,
    }

    recommendations = _build_recommendations(
        fatigue_score=fatigue_score,
        critical_ads=critical_ads,
        warning_ads=warning_ads,
        format_mix=format_mix,
        total_ads=len(client_ads),
        days_since_new=days_since_new,
        client_count=len(client_ads),
        competitor_avg=competitor_avg_count,
    )

    result = {
        "brand_name":               brand_name,
        "generated_at":             datetime.utcnow().isoformat(),
        "fatigue_score":            fatigue_score,
        "score_interpretation":     _interpret_score(fatigue_score),
        "score_breakdown":          breakdown,
        "client_ad_count":          len(client_ads),
        "competitor_avg_ad_count":  competitor_avg_count,
        "days_since_last_new_creative": days_since_new,
        "format_mix":               format_mix,
        "critical_ads": [_ad_summary(a) for a in critical_ads],
        "warning_ads":  [_ad_summary(a) for a in warning_ads],
        "recommendations":          recommendations,
    }

    _persist_waste_report(brand_name, client_id, result)
    _write_processed(brand_name, result)

    logger.info(
        "Fatigue score for '%s': %.1f/100 (%s) — %d critical, %d warning ads",
        brand_name, fatigue_score, _interpret_score(fatigue_score),
        len(critical_ads), len(warning_ads),
    )
    return result


# ══════════════════════════════════════════════════════════════════════════════
# Signal computation
# ══════════════════════════════════════════════════════════════════════════════

def _ads_in_range(ads: list[dict], min_days: int, max_days: Optional[int]) -> list[dict]:
    results = []
    for ad in ads:
        d = ad.get("duration_days")
        if d is None:
            continue
        if d >= min_days and (max_days is None or d < max_days):
            results.append(ad)
    return results


def _format_mix(ads: list[dict]) -> dict:
    total  = len(ads) or 1
    counts = Counter(ad.get("creative_type") or "unknown" for ad in ads)
    return {
        fmt: {
            "count": counts.get(fmt, 0),
            "pct":   round(counts.get(fmt, 0) / total * 100, 1),
        }
        for fmt in VALID_CREATIVE_TYPES
    }


def _days_since_last_new_creative(ads: list[dict]) -> Optional[int]:
    """
    How many days ago was the most recently launched ad?
    Uses start_date. Returns None if no start dates available.
    """
    dates: list[date] = []
    for ad in ads:
        raw = ad.get("start_date")
        if not raw:
            continue
        try:
            dates.append(date.fromisoformat(raw))
        except ValueError:
            continue

    if not dates:
        return None

    newest = max(dates)
    return (date.today() - newest).days


# ── Penalty calculators ────────────────────────────────────────────────────────

def _critical_penalty(critical_ads: list[dict]) -> float:
    return min(_CRITICAL_PTS_PER_AD * len(critical_ads), _CRITICAL_CAP)


def _warning_penalty(warning_ads: list[dict]) -> float:
    return min(_WARNING_PTS_PER_AD * len(warning_ads), _WARNING_CAP)


def _concentration_penalty(format_mix: dict, total_ads: int) -> float:
    """
    Penalty when a single format accounts for > 60% of active ads.
    Scales linearly from 0 at 60% dominance to full 25 pts at 100% dominance.
    """
    if total_ads == 0:
        return 0.0
    max_pct = max(
        (v["pct"] / 100 for v in format_mix.values()),
        default=0.0,
    )
    if max_pct <= _CONCENTRATION_TRIGGER:
        return 0.0
    ratio = (max_pct - _CONCENTRATION_TRIGGER) / (1.0 - _CONCENTRATION_TRIGGER)
    return round(ratio * _CONCENTRATION_CAP, 1)


def _count_deficit_penalty(client_count: int, competitor_avg: float) -> float:
    """
    Full penalty (10 pts) if client runs fewer than 50% of competitor average.
    Scaled proportionally between 50% and 100%.
    """
    if competitor_avg == 0 or client_count == 0:
        return 0.0
    ratio = client_count / competitor_avg
    if ratio >= 1.0:
        return 0.0
    if ratio <= 0.5:
        return float(_COUNT_DEFICIT_PTS)
    # Linear between 0.5 and 1.0
    return round((1.0 - ratio) / 0.5 * _COUNT_DEFICIT_PTS, 1)


def _recency_penalty(days_since_new: Optional[int]) -> float:
    """
    No penalty if last new creative launched within _RECENCY_THRESHOLD days.
    Full 10 pts if 30+ days since new creative.
    """
    if days_since_new is None:
        return 5.0   # unknown → half penalty as conservative estimate
    if days_since_new <= _RECENCY_THRESHOLD:
        return 0.0
    over = days_since_new - _RECENCY_THRESHOLD
    return round(min(over / 30.0, 1.0) * _RECENCY_CAP, 1)


# ── Score interpretation ───────────────────────────────────────────────────────

def _interpret_score(score: float) -> str:
    if score < 20:
        return "healthy"
    if score < 40:
        return "mild_fatigue"
    if score < 60:
        return "moderate_fatigue"
    if score < 80:
        return "high_fatigue"
    return "critical_fatigue"


# ── Recommendations ────────────────────────────────────────────────────────────

def _build_recommendations(
    fatigue_score:    float,
    critical_ads:     list[dict],
    warning_ads:      list[dict],
    format_mix:       dict,
    total_ads:        int,
    days_since_new:   Optional[int],
    client_count:     int,
    competitor_avg:   float,
) -> list[dict]:
    """
    Returns a list of recommendation dicts:
      {"priority": "high"|"medium"|"low", "signal": str, "action": str}
    """
    recs: list[dict] = []

    if critical_ads:
        recs.append({
            "priority": "high",
            "signal": f"{len(critical_ads)} ad(s) running {_CRITICAL_MIN_DAYS}+ days without refresh.",
            "action": f"Immediately retire or refresh: {', '.join(a['ad_library_id'] for a in critical_ads[:5])}.",
        })

    if warning_ads:
        recs.append({
            "priority": "medium",
            "signal": f"{len(warning_ads)} ad(s) in the {_WARNING_MIN_DAYS}–{_CRITICAL_MIN_DAYS - 1} day warning zone.",
            "action": "Schedule creative refresh within the next 7 days.",
        })

    if total_ads > 0:
        dominant = max(format_mix.items(), key=lambda kv: kv[1]["pct"])
        if dominant[1]["pct"] > 60:
            recs.append({
                "priority": "medium",
                "signal": f"Format concentration: {dominant[1]['pct']:.0f}% of ads are {dominant[0]}.",
                "action": f"Introduce at least one non-{dominant[0]} format this sprint (video or carousel recommended).",
            })

    if competitor_avg > 0 and client_count < competitor_avg * 0.5:
        recs.append({
            "priority": "medium",
            "signal": f"Running {client_count} ads vs competitor avg of {competitor_avg:.0f}.",
            "action": "Increase active ad count to at least match the competitive baseline.",
        })

    if days_since_new is not None and days_since_new > _RECENCY_THRESHOLD:
        recs.append({
            "priority": "high" if days_since_new > 30 else "medium",
            "signal": f"Last new creative launched {days_since_new} days ago.",
            "action": f"Launch a fresh creative immediately — the {_RECENCY_THRESHOLD}-day refresh window has passed.",
        })

    if fatigue_score >= 60 and not recs:
        recs.append({
            "priority": "high",
            "signal": f"Overall fatigue score is {fatigue_score}/100.",
            "action": "Full creative refresh sprint required. Brief the creative team this week.",
        })

    return recs


# ══════════════════════════════════════════════════════════════════════════════
# DB helpers
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_brands(names: list[str]) -> list[dict]:
    if not names:
        return []
    placeholders = ",".join("?" * len(names))
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT id, name FROM brands WHERE name IN ({placeholders})", names
        ).fetchall()
    return [dict(r) for r in rows]


def _fetch_active_ads(brand_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM ads WHERE brand_id = ? AND is_active = 1", (brand_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def _persist_waste_report(brand_name: str, brand_id: int, result: dict) -> None:
    diversity_score = max(
        0.0,
        round(
            100 - result["fatigue_score"]
            - result["score_breakdown"]["format_concentration_penalty"],
            1,
        ),
    )
    with get_connection() as conn:
        conn.execute(
            """INSERT INTO waste_reports (
                   client_brand_id, creative_diversity_score, format_mix_json,
                   avg_refresh_days, fatigue_flags_json, recommendations_json
               ) VALUES (?, ?, ?, ?, ?, ?)""",
            (
                brand_id,
                diversity_score,
                json.dumps(result["format_mix"]),
                result.get("days_since_last_new_creative"),
                json.dumps(result["critical_ads"] + result["warning_ads"]),
                json.dumps(result["recommendations"]),
            ),
        )
    logger.info("Waste report persisted for '%s'", brand_name)


# ══════════════════════════════════════════════════════════════════════════════
# Output helpers
# ══════════════════════════════════════════════════════════════════════════════

def _ad_summary(ad: dict) -> dict:
    return {
        "ad_library_id": ad.get("ad_library_id"),
        "creative_type": ad.get("creative_type"),
        "duration_days": ad.get("duration_days"),
        "start_date":    ad.get("start_date"),
        "cta_type":      ad.get("cta_type"),
    }


def _write_processed(brand_name: str, data: dict) -> Path:
    PROC_DIR.mkdir(parents=True, exist_ok=True)
    safe = brand_name.lower().replace(" ", "_")
    path = PROC_DIR / f"{safe}_fatigue.json"
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Fatigue report -> %s", path)
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
        prog="python -m analysis.fatigue_scorer",
        description="Compute creative fatigue score for a brand, benchmarked against competitors.",
    )
    parser.add_argument("--brand", required=True, help="Client brand name")
    parser.add_argument("--competitors", default="",
                        help="Comma-separated competitor names (auto-detected if omitted)")
    args = parser.parse_args()

    competitors = [c.strip() for c in args.competitors.split(",") if c.strip()]

    if not competitors:
        with get_connection() as conn:
            brand_row = conn.execute(
                "SELECT id FROM brands WHERE name = ?", (args.brand,)
            ).fetchone()
            if brand_row:
                comp_rows = conn.execute(
                    "SELECT b.name FROM competitor_sets cs "
                    "JOIN brands b ON b.id = cs.competitor_brand_id "
                    "WHERE cs.client_brand_id = ?",
                    (brand_row["id"],),
                ).fetchall()
                competitors = [r["name"] for r in comp_rows]
                if competitors:
                    logger.info("Auto-detected competitors: %s", competitors)

    result = run(args.brand, competitors)

    score = result["fatigue_score"]
    interp = result["score_interpretation"]
    print(f"\n  Fatigue score: {score}/100 ({interp})")
    print(f"  Active ads: {result['client_ad_count']} "
          f"(competitor avg: {result['competitor_avg_ad_count']})")
    print(f"  Critical ads (30+ days): {len(result['critical_ads'])}")
    print(f"  Warning ads (14-29 days): {len(result['warning_ads'])}")

    bd = result["score_breakdown"]
    print(f"\n  Breakdown:")
    for k, v in bd.items():
        print(f"    {k}: {v}")

    if result["recommendations"]:
        print(f"\n  Recommendations:")
        for r in result["recommendations"]:
            print(f"    [{r['priority'].upper()}] {r['signal']}")
            print(f"      -> {r['action']}")

    safe = args.brand.lower().replace(" ", "_")
    print(f"\nOutput: {PROC_DIR / safe}_fatigue.json")


if __name__ == "__main__":
    _cli()
