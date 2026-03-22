"""
analysis/category_intel.py

Aggregates creative intelligence across all brands in a competitor set
and surfaces actionable patterns and white-space opportunities.

  run(brand_name, competitor_names) → dict
      • Reads ad_analysis + ads from DB for all brands
      • Identifies trigger prevalence, format trends, and winner patterns
      • Surfaces underused angles (triggers with 0 profitable ads = opportunity)
      • Writes data/processed/{brand_name}_category_intelligence.json
      • Returns the intel dict
"""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from config import PROC_DIR, PROFITABLE_AD_MIN_DAYS, PSYCHOLOGICAL_TRIGGERS, get_connection

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def run(brand_name: str, competitor_names: list[str]) -> dict:
    """
    Build category-level intelligence from *brand_name* + *competitor_names*.

    Returns the intel dict and writes category_intelligence.json.
    """
    all_names  = [brand_name] + list(competitor_names)
    brand_rows = _fetch_brands(all_names)
    name_to_id = {b["name"]: b["id"] for b in brand_rows}

    if not brand_rows:
        raise ValueError("None of the requested brands are in the DB. Run ingest first.")

    brand_ids = list(name_to_id.values())

    # Load raw data
    all_ads       = _fetch_ads(brand_ids)
    all_analyses  = _fetch_analyses([a["id"] for a in all_ads])
    ad_id_to_ad   = {a["id"]: a for a in all_ads}

    # Separate profitable vs all ads
    profitable_ad_ids = {
        r["ad_id"] for r in all_analyses if r.get("is_profitable")
    }
    # Also classify by duration directly (analysis rows may be partial)
    profitable_ads = [
        a for a in all_ads
        if a["id"] in profitable_ad_ids
        or (a.get("duration_days") is not None and a["duration_days"] >= PROFITABLE_AD_MIN_DAYS)
    ]

    # ── Trigger analysis ────────────────────────────────────────────────────
    trigger_analysis   = _trigger_analysis(all_analyses, profitable_ads, all_ads)

    # ── Format analysis ─────────────────────────────────────────────────────
    format_analysis    = _format_analysis(all_ads, profitable_ads)

    # ── Duration analysis ───────────────────────────────────────────────────
    duration_analysis  = _duration_analysis(all_ads, profitable_ads)

    # ── CTA analysis ─────────────────────────────────────────────────────────
    cta_analysis       = _cta_analysis(all_ads, profitable_ads)

    # ── Per-brand summary ─────────────────────────────────────────────────────
    per_brand = _per_brand_summary(brand_rows, all_ads, profitable_ads, name_to_id)

    # ── Patterns and opportunities ────────────────────────────────────────────
    patterns      = _derive_patterns(trigger_analysis, format_analysis, duration_analysis, cta_analysis)
    opportunities = _derive_opportunities(trigger_analysis, format_analysis)

    intel = {
        "brand_name":              brand_name,
        "competitors_analysed":    competitor_names,
        "generated_at":            datetime.utcnow().isoformat(),
        "total_ads_in_universe":   len(all_ads),
        "profitable_ads_in_universe": len(profitable_ads),
        "profitable_rate":         round(len(profitable_ads) / len(all_ads) * 100, 1) if all_ads else 0,
        "trigger_analysis":        trigger_analysis,
        "format_analysis":         format_analysis,
        "duration_analysis":       duration_analysis,
        "cta_analysis":            cta_analysis,
        "per_brand_summary":       per_brand,
        "patterns":                patterns,
        "opportunities":           opportunities,
    }

    _write_processed(brand_name, intel)
    logger.info(
        "Category intel built for '%s': %d ads, %d profitable, %d patterns, %d opportunities",
        brand_name, len(all_ads), len(profitable_ads), len(patterns), len(opportunities),
    )
    return intel


# ══════════════════════════════════════════════════════════════════════════════
# Trigger analysis
# ══════════════════════════════════════════════════════════════════════════════

def _trigger_analysis(
    all_analyses:   list[dict],
    profitable_ads: list[dict],
    all_ads:        list[dict],
) -> dict:
    """
    For each psychological trigger:
      - Count across ALL analysed ads (prevalence)
      - Count in profitable ads only
      - Compute profitable_rate (% of ads with this trigger that are profitable)
      - Flag underused triggers (those not used by any profitable ad)
    """
    profitable_ad_ids = {a["id"] for a in profitable_ads}

    # Trigger counts across all analysed ads
    all_triggers     = [r["psychological_trigger"] for r in all_analyses if r.get("psychological_trigger")]
    profitable_triggers = [
        r["psychological_trigger"]
        for r in all_analyses
        if r.get("psychological_trigger") and r.get("ad_id") in profitable_ad_ids
    ]

    all_trigger_counts        = Counter(all_triggers)
    profitable_trigger_counts = Counter(profitable_triggers)

    # Profitable rate per trigger
    profitable_rate: dict[str, float] = {}
    for trigger, total_count in all_trigger_counts.items():
        p_count = profitable_trigger_counts.get(trigger, 0)
        profitable_rate[trigger] = round(p_count / total_count * 100, 1)

    # Underused: in PSYCHOLOGICAL_TRIGGERS but not appearing in any profitable ad
    used_in_profitable = set(profitable_trigger_counts.keys())
    underused = [t for t in PSYCHOLOGICAL_TRIGGERS if t not in used_in_profitable]

    return {
        "by_prevalence":          dict(all_trigger_counts.most_common()),
        "by_profitable_only":     dict(profitable_trigger_counts.most_common()),
        "profitable_rate_by_trigger": dict(
            sorted(profitable_rate.items(), key=lambda x: x[1], reverse=True)
        ),
        "underused_angles":        underused,
        "total_ads_with_trigger":  len(all_triggers),
        "trigger_coverage_pct":    round(len(all_triggers) / len(all_analyses) * 100, 1)
                                   if all_analyses else 0,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Format analysis
# ══════════════════════════════════════════════════════════════════════════════

def _format_analysis(all_ads: list[dict], profitable_ads: list[dict]) -> dict:
    """
    For each creative format:
      - Total count + % of universe
      - Winner count + % of winners
      - Win rate (% of that format's ads that are profitable)
    """
    all_formats        = Counter(a.get("creative_type") or "unknown" for a in all_ads)
    profitable_formats = Counter(a.get("creative_type") or "unknown" for a in profitable_ads)

    total       = len(all_ads)   or 1
    total_p     = len(profitable_ads) or 1
    result: dict = {}

    for fmt in set(list(all_formats.keys()) + list(profitable_formats.keys())):
        total_count  = all_formats.get(fmt, 0)
        winner_count = profitable_formats.get(fmt, 0)
        result[fmt] = {
            "total_count":        total_count,
            "total_pct":          round(total_count  / total   * 100, 1),
            "winner_count":       winner_count,
            "winner_pct":         round(winner_count / total_p * 100, 1),
            "win_rate":           round(winner_count / total_count * 100, 1)
                                  if total_count else 0.0,
        }

    # Sort by win_rate descending
    return dict(sorted(result.items(), key=lambda kv: kv[1]["win_rate"], reverse=True))


# ══════════════════════════════════════════════════════════════════════════════
# Duration analysis
# ══════════════════════════════════════════════════════════════════════════════

def _duration_analysis(all_ads: list[dict], profitable_ads: list[dict]) -> dict:
    all_durations  = [a["duration_days"] for a in all_ads if a.get("duration_days") is not None]
    p_durations    = [a["duration_days"] for a in profitable_ads if a.get("duration_days") is not None]

    def _stats(durations: list[int]) -> dict:
        if not durations:
            return {"count": 0, "avg": 0, "min": 0, "max": 0, "median": 0}
        s = sorted(durations)
        n = len(s)
        return {
            "count":  n,
            "avg":    round(sum(s) / n, 1),
            "min":    s[0],
            "max":    s[-1],
            "median": s[n // 2],
        }

    return {
        "all_ads":        _stats(all_durations),
        "profitable_ads": _stats(p_durations),
    }


# ══════════════════════════════════════════════════════════════════════════════
# CTA analysis
# ══════════════════════════════════════════════════════════════════════════════

def _cta_analysis(all_ads: list[dict], profitable_ads: list[dict]) -> dict:
    all_ctas        = Counter(a.get("cta_type") for a in all_ads if a.get("cta_type"))
    profitable_ctas = Counter(a.get("cta_type") for a in profitable_ads if a.get("cta_type"))

    total_p = len(profitable_ads) or 1
    return {
        "all_ctas":        dict(all_ctas.most_common(10)),
        "profitable_ctas": dict(profitable_ctas.most_common(10)),
        "top_winner_cta":  profitable_ctas.most_common(1)[0][0] if profitable_ctas else None,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Per-brand summary
# ══════════════════════════════════════════════════════════════════════════════

def _per_brand_summary(
    brand_rows:     list[dict],
    all_ads:        list[dict],
    profitable_ads: list[dict],
    name_to_id:     dict,
) -> dict:
    profitable_by_brand: Counter = Counter(a.get("brand_id") for a in profitable_ads)
    total_by_brand:      Counter = Counter(a.get("brand_id") for a in all_ads)

    summary: dict = {}
    for brand in brand_rows:
        bid  = brand["id"]
        name = brand["name"]
        tc   = total_by_brand.get(bid, 0)
        pc   = profitable_by_brand.get(bid, 0)
        summary[name] = {
            "total_ads":      tc,
            "profitable_ads": pc,
            "win_rate":       round(pc / tc * 100, 1) if tc else 0.0,
        }

    return summary


# ══════════════════════════════════════════════════════════════════════════════
# Patterns and opportunities
# ══════════════════════════════════════════════════════════════════════════════

def _derive_patterns(
    trigger_analysis:  dict,
    format_analysis:   dict,
    duration_analysis: dict,
    cta_analysis:      dict,
) -> list[str]:
    """
    Synthesise data into plain-English statements for LLM context and client reports.
    Only emits a statement when the underlying signal is strong enough to be actionable.
    """
    patterns: list[str] = []

    # ── Trigger dominance ─────────────────────────────────────────────────────
    by_profitable = trigger_analysis.get("by_profitable_only", {})
    total_p_triggers = sum(by_profitable.values()) or 1

    top_two = list(by_profitable.items())[:2]
    if len(top_two) >= 2:
        combined_pct = round(
            (top_two[0][1] + top_two[1][1]) / total_p_triggers * 100
        )
        if combined_pct >= 50:
            patterns.append(
                f"{combined_pct}% of profitable ads use '{top_two[0][0]}' or "
                f"'{top_two[1][0]}' triggers — these are the proven angles in this category."
            )

    # Best profitable rate trigger
    p_rate = trigger_analysis.get("profitable_rate_by_trigger", {})
    if p_rate:
        best_trigger, best_rate = next(iter(p_rate.items()))
        if best_rate >= 40:
            patterns.append(
                f"'{best_trigger}' has the highest win rate at {best_rate:.0f}% — "
                "ads using this angle are significantly more likely to run 21+ days."
            )

    # ── Format over-performance ───────────────────────────────────────────────
    for fmt, fdata in format_analysis.items():
        if fmt == "unknown":
            continue
        total_pct  = fdata["total_pct"]
        winner_pct = fdata["winner_pct"]
        if winner_pct > 0 and winner_pct >= total_pct * 1.5 and total_pct >= 5:
            patterns.append(
                f"{fmt.title()} format is over-performing: {winner_pct:.0f}% of winners "
                f"vs only {total_pct:.0f}% of total ads — disproportionately high win rate."
            )

    # ── Duration gap ────────────────────────────────────────────────────────────
    avg_all = duration_analysis.get("all_ads", {}).get("avg", 0)
    avg_p   = duration_analysis.get("profitable_ads", {}).get("avg", 0)
    if avg_p > avg_all * 1.5 and avg_p > 0:
        patterns.append(
            f"Profitable ads run {avg_p:.0f} days on average vs {avg_all:.0f} days for all ads — "
            "winners stay active significantly longer than the baseline."
        )

    # ── CTA dominance ────────────────────────────────────────────────────────
    top_cta = cta_analysis.get("top_winner_cta")
    if top_cta:
        p_ctas = cta_analysis.get("profitable_ctas", {})
        total_p_cta = sum(p_ctas.values()) or 1
        top_pct = round(p_ctas.get(top_cta, 0) / total_p_cta * 100)
        if top_pct >= 40:
            patterns.append(
                f"'{top_cta}' is the dominant CTA among winners ({top_pct}% of profitable ads)."
            )

    return patterns


def _derive_opportunities(trigger_analysis: dict, format_analysis: dict) -> list[str]:
    """
    Identify angles and formats that are:
      a) Completely unused by competitors in profitable ads (underused trigger)
      b) Present in the universe at low volume but with high win rate (format opportunity)
    """
    opportunities: list[str] = []

    # ── Underused triggers ────────────────────────────────────────────────────
    for trigger in trigger_analysis.get("underused_angles", []):
        opportunities.append(
            f"'{trigger}' trigger: zero competitor ads using this angle profitably — "
            "potential first-mover differentiation in this category."
        )

    # Triggers used, but with very low profitable rate despite being used
    p_rate = trigger_analysis.get("profitable_rate_by_trigger", {})
    for trigger, rate in p_rate.items():
        if rate == 0 and trigger not in trigger_analysis.get("underused_angles", []):
            opportunities.append(
                f"'{trigger}' is being used but has 0% win rate — "
                "either the execution is weak or the angle is mismatched to this audience."
            )

    # ── Format opportunities ─────────────────────────────────────────────────
    for fmt, fdata in format_analysis.items():
        if fmt == "unknown":
            continue
        total_pct = fdata["total_pct"]
        win_rate  = fdata["win_rate"]
        # Low usage (< 20%) but decent win rate (> 25%) = underutilised
        if total_pct < 20 and win_rate >= 25:
            opportunities.append(
                f"{fmt.title()} format is underutilised ({total_pct:.0f}% of ads) "
                f"but achieves a {win_rate:.0f}% win rate — scaling it could unlock significant returns."
            )

    return opportunities


# ══════════════════════════════════════════════════════════════════════════════
# DB helpers
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_brands(names: list[str]) -> list[dict]:
    if not names:
        return []
    placeholders = ",".join("?" * len(names))
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT id, name, is_client FROM brands WHERE name IN ({placeholders})", names
        ).fetchall()
    return [dict(r) for r in rows]


def _fetch_ads(brand_ids: list[int]) -> list[dict]:
    if not brand_ids:
        return []
    placeholders = ",".join("?" * len(brand_ids))
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT * FROM ads WHERE brand_id IN ({placeholders})", brand_ids
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
    safe = brand_name.lower().replace(" ", "_")
    path = PROC_DIR / f"{safe}_category_intelligence.json"
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Category intelligence → %s", path)
    return path
