"""
feedback/loop.py — Use imported performance data to score and improve
generated creative concepts.

Month 2+ feature. Requires performance_data rows linked to creative_concepts.
"""

import logging
from typing import Optional

from config import get_connection

logger = logging.getLogger(__name__)


def run(brand_name: str) -> dict:
    """
    Analyse performance data for concepts belonging to *brand_name* and
    return a feedback summary.

    Returns:
        {
            "top_performing_angles": [...],
            "underperforming_formats": [...],
            "recommended_next_batch_weights": {...},
        }
    """
    brand = _fetch_brand(brand_name)
    if not brand:
        raise ValueError(f"Brand '{brand_name}' not found.")

    perf_rows = _fetch_performance(brand["id"])
    if not perf_rows:
        logger.info("No performance data yet for '%s' — feedback loop skipped.", brand_name)
        return {}

    summary = _analyse(perf_rows)
    logger.info("Feedback loop complete for '%s'", brand_name)
    return summary


# ── Internal ───────────────────────────────────────────────────────────────────

def _fetch_brand(name: str) -> Optional[dict]:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM brands WHERE name = ?", (name,)).fetchone()
    return dict(row) if row else None


def _fetch_performance(brand_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """SELECT pd.*, cc.psychological_angle, cc.creative_type
               FROM performance_data pd
               JOIN creative_concepts cc ON cc.id = pd.creative_concept_id
               WHERE cc.client_brand_id = ?
               AND pd.roas IS NOT NULL""",
            (brand_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def _analyse(rows: list[dict]) -> dict:
    from collections import defaultdict

    angle_roas:  dict = defaultdict(list)
    format_roas: dict = defaultdict(list)

    for row in rows:
        if row.get("psychological_angle") and row.get("roas") is not None:
            angle_roas[row["psychological_angle"]].append(row["roas"])
        if row.get("creative_type") and row.get("roas") is not None:
            format_roas[row["creative_type"]].append(row["roas"])

    def _avg(lst):
        return round(sum(lst) / len(lst), 2) if lst else 0.0

    angle_avgs  = {k: _avg(v) for k, v in angle_roas.items()}
    format_avgs = {k: _avg(v) for k, v in format_roas.items()}

    sorted_angles  = sorted(angle_avgs.items(),  key=lambda x: x[1], reverse=True)
    sorted_formats = sorted(format_avgs.items(), key=lambda x: x[1], reverse=True)

    top_angles   = [a for a, _ in sorted_angles[:3]]
    under_formats = [f for f, roas in sorted_formats if roas < 1.5]

    # Weight next batch toward top-performing angles
    total_roas = sum(angle_avgs.values()) or 1
    weights = {
        angle: round(roas / total_roas, 2)
        for angle, roas in angle_avgs.items()
    }

    return {
        "top_performing_angles":          top_angles,
        "underperforming_formats":         under_formats,
        "angle_avg_roas":                 angle_avgs,
        "format_avg_roas":                format_avgs,
        "recommended_next_batch_weights": weights,
    }
