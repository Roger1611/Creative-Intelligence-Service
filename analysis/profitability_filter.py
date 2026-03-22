"""
analysis/profitability_filter.py

Flags ads as profitable winners and produces a ranked competitor intelligence
summary.

Rule: ad running >= PROFITABLE_AD_MIN_DAYS (21 days) = probable winner.
No brand funds a losing creative for 3 weeks.

  run(brand_name, competitor_names) → dict
      • Flags is_profitable in ad_analysis for every ad across the competitor set
      • Returns a ranked summary dict
      • Writes data/processed/{brand_name}_profitable_ads_summary.json
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Optional

from config import PROC_DIR, PROFITABLE_AD_MIN_DAYS, get_connection

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def run(brand_name: str, competitor_names: list[str]) -> dict:
    """
    Evaluate profitable ads across *brand_name* and all *competitor_names*.

    Steps:
      1. Load all ads from DB for each brand.
      2. Upsert is_profitable flag into ad_analysis (creates row if missing).
      3. Build ranked winner list per competitor, sorted by duration_days desc.
      4. Compute cross-competitor patterns.
      5. Write profitable_ads_summary.json.

    Returns the summary dict.
    """
    all_names  = [brand_name] + list(competitor_names)
    brand_rows = _fetch_brands(all_names)
    name_to_id = {b["name"]: b["id"] for b in brand_rows}

    if brand_name not in name_to_id:
        raise ValueError(f"Brand '{brand_name}' not found in DB. Run ingest first.")

    per_brand: dict[str, dict] = {}

    for name in all_names:
        bid = name_to_id.get(name)
        if bid is None:
            logger.warning("'%s' not in DB — skipping profitability pass.", name)
            continue

        ads      = _fetch_ads(bid)
        winners  = [ad for ad in ads if _is_profitable(ad)]
        losers   = [ad for ad in ads if not _is_profitable(ad)]

        # Persist profitability flags
        _upsert_profitability(winners, is_profitable=True)
        _upsert_profitability(losers,  is_profitable=False)

        profitable_pct = round(len(winners) / len(ads) * 100, 1) if ads else 0.0

        per_brand[name] = {
            "brand_id":        bid,
            "total_ads":       len(ads),
            "profitable_ads":  len(winners),
            "profitable_pct":  profitable_pct,
            "ranked_winners":  _rank_winners(winners),
        }

        logger.info(
            "'%s': %d/%d ads profitable (%.1f%%)",
            name, len(winners), len(ads), profitable_pct,
        )

    cross_patterns = _cross_competitor_patterns(per_brand, competitor_names)

    summary = {
        "generated_at":          datetime.utcnow().isoformat(),
        "client_brand":          brand_name,
        "profitability_threshold_days": PROFITABLE_AD_MIN_DAYS,
        "brands":                per_brand,
        "cross_competitor_patterns": cross_patterns,
    }

    _write_summary(brand_name, summary)
    return summary


# ══════════════════════════════════════════════════════════════════════════════
# Per-brand helpers
# ══════════════════════════════════════════════════════════════════════════════

def _is_profitable(ad: dict) -> bool:
    duration = ad.get("duration_days")
    return duration is not None and duration >= PROFITABLE_AD_MIN_DAYS


def _rank_winners(winners: list[dict]) -> list[dict]:
    """Sort by duration_days descending. Longer run = more proven."""
    ranked = sorted(winners, key=lambda a: a.get("duration_days") or 0, reverse=True)
    return [
        {
            "ad_library_id": a["ad_library_id"],
            "duration_days": a.get("duration_days"),
            "creative_type": a.get("creative_type"),
            "cta_type":      a.get("cta_type"),
            "ad_copy":       (a.get("ad_copy") or "")[:200],  # truncate for summary
            "start_date":    a.get("start_date"),
            "thumbnail_url": a.get("thumbnail_url"),
            "image_path":    a.get("image_path"),
        }
        for a in ranked
    ]


def _upsert_profitability(ads: list[dict], is_profitable: bool) -> None:
    """
    Create or update the is_profitable flag in ad_analysis for each ad.
    Only touches is_profitable — leaves other analysis columns untouched.
    """
    flag = int(is_profitable)
    with get_connection() as conn:
        for ad in ads:
            ad_id = ad.get("id")
            if not ad_id:
                continue
            existing = conn.execute(
                "SELECT id FROM ad_analysis WHERE ad_id = ?", (ad_id,)
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE ad_analysis SET is_profitable = ? WHERE ad_id = ?",
                    (flag, ad_id),
                )
            else:
                conn.execute(
                    "INSERT INTO ad_analysis (ad_id, is_profitable) VALUES (?, ?)",
                    (ad_id, flag),
                )


# ══════════════════════════════════════════════════════════════════════════════
# Cross-competitor patterns
# ══════════════════════════════════════════════════════════════════════════════

def _cross_competitor_patterns(
    per_brand: dict[str, dict],
    competitor_names: list[str],
) -> dict:
    """
    Aggregate winner data across all competitors (excludes the client brand).
    """
    all_winners: list[dict] = []
    for name in competitor_names:
        bdata = per_brand.get(name, {})
        all_winners.extend(bdata.get("ranked_winners", []))

    if not all_winners:
        return {}

    durations = [w["duration_days"] for w in all_winners if w.get("duration_days") is not None]
    formats   = Counter(w.get("creative_type") for w in all_winners if w.get("creative_type"))
    ctas      = Counter(w.get("cta_type")      for w in all_winners if w.get("cta_type"))

    most_durable = max(all_winners, key=lambda w: w.get("duration_days") or 0)

    # Format win rate: winners-in-format / all-formats-in-winners
    total_winners = len(all_winners)
    format_win_pct = {
        fmt: round(cnt / total_winners * 100, 1)
        for fmt, cnt in formats.items()
    }

    return {
        "total_winners_across_competitors": total_winners,
        "avg_winner_duration_days":         round(sum(durations) / len(durations), 1) if durations else 0,
        "max_winner_duration_days":         max(durations) if durations else 0,
        "most_durable_ad": {
            "ad_library_id": most_durable["ad_library_id"],
            "duration_days": most_durable.get("duration_days"),
            "creative_type": most_durable.get("creative_type"),
        },
        "winner_format_distribution":     dict(formats.most_common()),
        "winner_format_pct":              format_win_pct,
        "winner_cta_distribution":        dict(ctas.most_common()),
    }


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


def _fetch_ads(brand_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM ads WHERE brand_id = ?", (brand_id,)
        ).fetchall()
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# Output
# ══════════════════════════════════════════════════════════════════════════════

def _write_summary(brand_name: str, data: dict) -> Path:
    PROC_DIR.mkdir(parents=True, exist_ok=True)
    safe = brand_name.lower().replace(" ", "_")
    path = PROC_DIR / f"{safe}_profitable_ads_summary.json"
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Profitable ads summary → %s", path)
    return path
