"""
feedback/loop.py - Performance feedback loop for concept generation.

Analyses real performance data across all clients in a category to find:
- Which psychological angles get the best ROAS / CTR
- Which hook structures stop the scroll most effectively
- Which formats (static/carousel/video) perform best per category
- Generates a "winning patterns" summary that gets injected into future
  concept_generation prompts

This is the compounding moat: after 2--3 months with multiple clients in the
same category, concept generation is informed by real performance data, not
just competitor observation.

CLI: python -m feedback.loop --category skincare
     python -m feedback.loop --brand "Mamaearth"   (single-brand analysis)
"""

import argparse
import json
import logging
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

from config import PROC_DIR, PSYCHOLOGICAL_TRIGGERS, get_connection
from scrapers.utils import safe_brand_slug

logger = logging.getLogger(__name__)

# Minimum sample size before we trust a pattern
_MIN_SAMPLES = 3


# -- Public API ----------------------------------------------------------------

def run(
    category: str | None = None,
    brand_name: str | None = None,
) -> dict:
    """
    Analyse performance data and generate a winning-patterns summary.

    Provide *category* to analyse all clients in that category, or
    *brand_name* for a single brand. Returns the full feedback dict
    and saves it to data/processed/.
    """
    if not category and not brand_name:
        raise ValueError("Provide --category or --brand")

    conn = get_connection()
    try:
        perf_rows = _fetch_performance(conn, category=category,
                                       brand_name=brand_name)
        if not perf_rows:
            label = category or brand_name
            logger.info("No performance data for '%s' - feedback loop skipped.",
                        label)
            return {"status": "no_data"}

        angle_analysis = _analyse_angles(perf_rows)
        hook_analysis = _analyse_hooks(conn, perf_rows)
        format_analysis = _analyse_formats(perf_rows)
        winning_patterns = _build_winning_patterns(
            angle_analysis, hook_analysis, format_analysis,
            category=category, brand_name=brand_name,
        )
        weights = _compute_next_batch_weights(angle_analysis)

        result = {
            "generated_at": datetime.now().isoformat(),
            "scope": {"category": category, "brand": brand_name},
            "sample_size": len(perf_rows),
            "angle_analysis": angle_analysis,
            "hook_analysis": hook_analysis,
            "format_analysis": format_analysis,
            "winning_patterns": winning_patterns,
            "next_batch_weights": weights,
        }

    finally:
        conn.close()

    # Save to processed dir
    label = category or safe_brand_slug(brand_name or "unknown")
    _save_json(label, result)

    logger.info("Feedback loop complete: %d rows analysed, patterns saved",
                len(perf_rows))
    return result


# -- Data fetching -------------------------------------------------------------

def _fetch_performance(
    conn,
    category: str | None = None,
    brand_name: str | None = None,
) -> list[dict]:
    """Fetch performance rows joined with concept + ad metadata."""

    if brand_name:
        rows = conn.execute(
            """
            SELECT pd.ctr, pd.cpa, pd.roas, pd.impressions, pd.spend,
                   cc.psychological_angle, cc.hook_text, cc.body_script,
                   a.creative_type, a.ad_copy,
                   b.name AS brand_name, b.category
            FROM performance_data pd
            LEFT JOIN creative_concepts cc ON cc.id = pd.creative_concept_id
            LEFT JOIN ads a ON a.id = pd.ad_id
            LEFT JOIN brands b ON (
                b.id = cc.client_brand_id OR b.id = a.brand_id
            )
            WHERE b.name = ? AND b.is_client = 1
            """,
            (brand_name,),
        ).fetchall()
    elif category:
        rows = conn.execute(
            """
            SELECT pd.ctr, pd.cpa, pd.roas, pd.impressions, pd.spend,
                   cc.psychological_angle, cc.hook_text, cc.body_script,
                   a.creative_type, a.ad_copy,
                   b.name AS brand_name, b.category
            FROM performance_data pd
            LEFT JOIN creative_concepts cc ON cc.id = pd.creative_concept_id
            LEFT JOIN ads a ON a.id = pd.ad_id
            LEFT JOIN brands b ON (
                b.id = cc.client_brand_id OR b.id = a.brand_id
            )
            WHERE b.category = ? AND b.is_client = 1
            """,
            (category,),
        ).fetchall()
    else:
        return []

    return [dict(r) for r in rows]


# -- Angle analysis ------------------------------------------------------------

def _analyse_angles(rows: list[dict]) -> dict:
    """ROAS- and CTR-weighted scoring per psychological angle."""
    angle_metrics: dict[str, list[dict]] = defaultdict(list)

    for r in rows:
        angle = r.get("psychological_angle")
        if not angle:
            continue
        angle_metrics[angle].append({
            "roas": r.get("roas"),
            "ctr": r.get("ctr"),
            "cpa": r.get("cpa"),
            "spend": r.get("spend"),
            "impressions": r.get("impressions"),
        })

    results = {}
    for angle in PSYCHOLOGICAL_TRIGGERS:
        data = angle_metrics.get(angle, [])
        n = len(data)
        if n == 0:
            results[angle] = {
                "sample_size": 0, "avg_roas": None, "avg_ctr": None,
                "avg_cpa": None, "total_spend": 0, "verdict": "no_data",
            }
            continue

        roas_vals = [d["roas"] for d in data if d["roas"] is not None]
        ctr_vals = [d["ctr"] for d in data if d["ctr"] is not None]
        cpa_vals = [d["cpa"] for d in data if d["cpa"] is not None]
        spend_vals = [d["spend"] for d in data if d["spend"] is not None]

        avg_roas = _safe_avg(roas_vals)
        avg_ctr = _safe_avg(ctr_vals)
        avg_cpa = _safe_avg(cpa_vals)
        total_spend = sum(spend_vals)

        # Verdict
        if n < _MIN_SAMPLES:
            verdict = "insufficient_data"
        elif avg_roas is not None and avg_roas >= 3.0:
            verdict = "top_performer"
        elif avg_roas is not None and avg_roas >= 1.5:
            verdict = "solid"
        elif avg_roas is not None and avg_roas >= 1.0:
            verdict = "break_even"
        else:
            verdict = "underperforming"

        results[angle] = {
            "sample_size": n,
            "avg_roas": avg_roas,
            "avg_ctr": avg_ctr,
            "avg_cpa": avg_cpa,
            "total_spend": round(total_spend, 2),
            "verdict": verdict,
        }

    return results


# -- Hook structure analysis ---------------------------------------------------

def _analyse_hooks(conn, rows: list[dict]) -> dict:
    """Classify hook structures and find which patterns get the highest CTR."""
    hook_metrics: dict[str, list[float]] = defaultdict(list)

    for r in rows:
        hook = r.get("hook_text") or _extract_hook(r.get("ad_copy") or "")
        if not hook:
            continue
        structure = _classify_hook_structure(hook)
        ctr = r.get("ctr")
        if ctr is not None:
            hook_metrics[structure].append(ctr)

    results = {}
    for structure, ctrs in hook_metrics.items():
        if len(ctrs) < _MIN_SAMPLES:
            continue
        results[structure] = {
            "sample_size": len(ctrs),
            "avg_ctr": round(sum(ctrs) / len(ctrs), 3),
            "max_ctr": round(max(ctrs), 3),
            "min_ctr": round(min(ctrs), 3),
        }

    # Sort by avg CTR descending
    results = dict(sorted(results.items(),
                          key=lambda x: x[1]["avg_ctr"], reverse=True))
    return results


def _classify_hook_structure(hook: str) -> str:
    """Classify a hook into a structural category."""
    hook_lower = hook.lower().strip()

    if hook_lower.endswith("?"):
        return "question"
    if re.match(r"^\d", hook_lower):
        return "number_lead"
    if any(hook_lower.startswith(w) for w in
           ("stop", "wait", "don't", "never", "warning")):
        return "pattern_interrupt"
    if any(w in hook_lower for w in ("you ", "your ", "you're")):
        return "direct_address"
    if any(w in hook_lower for w in
           ("secret", "nobody", "hidden", "truth", "real reason")):
        return "curiosity_gap"
    if any(w in hook_lower for w in
           ("before", "after", "transformation", "results", "changed")):
        return "transformation"
    if any(w in hook_lower for w in
           ("review", "testimonial", "said", "told me")):
        return "social_proof_lead"
    if any(w in hook_lower for w in
           ("last chance", "limited", "hurry", "only", "ending")):
        return "urgency_lead"
    if any(w in hook_lower for w in
           ("doctor", "expert", "dermatologist", "study", "research")):
        return "authority_lead"
    return "bold_claim"


def _extract_hook(ad_copy: str) -> str:
    """Pull the first line/sentence from ad copy as the hook."""
    if not ad_copy:
        return ""
    # First line or first sentence (up to 80 chars)
    first_line = ad_copy.split("\n")[0].strip()
    if len(first_line) > 80:
        # Try sentence boundary
        m = re.match(r"^(.{10,80}?[.!?])\s", first_line)
        if m:
            return m.group(1)
        return first_line[:80]
    return first_line


# -- Format analysis -----------------------------------------------------------

def _analyse_formats(rows: list[dict]) -> dict:
    """Performance breakdown by creative format."""
    fmt_metrics: dict[str, list[dict]] = defaultdict(list)

    for r in rows:
        fmt = r.get("creative_type")
        if not fmt:
            continue
        fmt_metrics[fmt].append({
            "roas": r.get("roas"),
            "ctr": r.get("ctr"),
            "cpa": r.get("cpa"),
            "spend": r.get("spend"),
        })

    results = {}
    for fmt, data in fmt_metrics.items():
        roas_vals = [d["roas"] for d in data if d["roas"] is not None]
        ctr_vals = [d["ctr"] for d in data if d["ctr"] is not None]
        cpa_vals = [d["cpa"] for d in data if d["cpa"] is not None]
        spend_vals = [d["spend"] for d in data if d["spend"] is not None]

        results[fmt] = {
            "sample_size": len(data),
            "avg_roas": _safe_avg(roas_vals),
            "avg_ctr": _safe_avg(ctr_vals),
            "avg_cpa": _safe_avg(cpa_vals),
            "total_spend": round(sum(spend_vals), 2),
        }

    # Sort by avg_roas descending
    results = dict(sorted(
        results.items(),
        key=lambda x: x[1]["avg_roas"] or 0,
        reverse=True,
    ))
    return results


# -- Winning patterns (the moat) ----------------------------------------------

def _build_winning_patterns(
    angles: dict,
    hooks: dict,
    formats: dict,
    category: str | None,
    brand_name: str | None,
) -> str:
    """
    Build a natural-language summary of winning patterns that gets injected
    into future concept_generation prompts.

    This is the compounding moat - each batch of performance data makes the
    next generation smarter.
    """
    scope = f"category '{category}'" if category else f"brand '{brand_name}'"
    lines = [
        f"PERFORMANCE FEEDBACK - winning patterns from real ad data across {scope}:",
        "",
    ]

    # Top angles
    top = [(a, d) for a, d in angles.items()
           if d.get("verdict") in ("top_performer", "solid")
           and d["sample_size"] >= _MIN_SAMPLES]
    top.sort(key=lambda x: x[1].get("avg_roas") or 0, reverse=True)

    if top:
        lines.append("PROVEN ANGLES (prioritise these):")
        for angle, data in top[:5]:
            nice = angle.replace("_", " ").title()
            roas = data["avg_roas"]
            ctr = data["avg_ctr"]
            lines.append(
                f"  - {nice}: avg ROAS {roas:.1f}x, avg CTR {ctr:.2f}%, "
                f"n={data['sample_size']}")

    # Underperformers
    under = [(a, d) for a, d in angles.items()
             if d.get("verdict") == "underperforming"
             and d["sample_size"] >= _MIN_SAMPLES]
    if under:
        lines.append("")
        lines.append("UNDERPERFORMING ANGLES (use sparingly):")
        for angle, data in under:
            nice = angle.replace("_", " ").title()
            roas = data.get("avg_roas") or 0
            lines.append(f"  - {nice}: avg ROAS {roas:.1f}x")

    # Top hook structures
    if hooks:
        lines.append("")
        lines.append("BEST HOOK STRUCTURES BY CTR:")
        for structure, data in list(hooks.items())[:5]:
            nice = structure.replace("_", " ").title()
            lines.append(
                f"  - {nice}: avg CTR {data['avg_ctr']:.2f}%, "
                f"n={data['sample_size']}")

    # Format performance
    if formats:
        lines.append("")
        lines.append("FORMAT PERFORMANCE:")
        for fmt, data in formats.items():
            roas = data.get("avg_roas") or 0
            ctr = data.get("avg_ctr") or 0
            lines.append(
                f"  - {fmt}: avg ROAS {roas:.1f}x, avg CTR {ctr:.2f}%, "
                f"n={data['sample_size']}")

    lines.append("")
    lines.append(
        "Use these patterns to weight concept generation. Allocate more "
        "concepts to proven angles and hook structures. Reduce allocation "
        "to underperformers unless testing new variations."
    )

    return "\n".join(lines)


# -- Next-batch weights --------------------------------------------------------

def _compute_next_batch_weights(angle_analysis: dict) -> dict:
    """
    ROAS-weighted concept allocation for the next batch.

    Returns dict mapping angle -> float weight (0--1, summing to 1.0).
    Higher weight = more concepts should use this angle.
    """
    # Use spend-weighted ROAS: angles that spent more AND had good ROAS
    # get higher weight than angles with high ROAS on tiny spend
    scores = {}
    for angle, data in angle_analysis.items():
        if data["sample_size"] < _MIN_SAMPLES:
            # Give untested angles a baseline weight so they still get explored
            scores[angle] = 1.0
            continue
        roas = data.get("avg_roas") or 0
        spend = data.get("total_spend") or 0
        # Score = ROAS * log(1 + spend) to reward proven + well-funded angles
        import math
        scores[angle] = roas * math.log1p(spend) if roas > 0 else 0.1

    total = sum(scores.values()) or 1
    weights = {angle: round(score / total, 3) for angle, score in scores.items()}

    # Sort descending
    return dict(sorted(weights.items(), key=lambda x: x[1], reverse=True))


# -- Helpers -------------------------------------------------------------------

def _safe_avg(values: list) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 3)


def _save_json(label: str, data: dict) -> None:
    out_dir = PROC_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    slug = safe_brand_slug(label)
    out_path = out_dir / f"{slug}_feedback_loop.json"
    out_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    logger.info("Feedback saved -> %s", out_path)


# -- CLI -----------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Analyse performance data and generate winning patterns.")
    parser.add_argument("--category", default=None,
                        help="Analyse all clients in this category")
    parser.add_argument("--brand", default=None,
                        help="Analyse a single brand")
    args = parser.parse_args()

    if not args.category and not args.brand:
        parser.error("Provide --category or --brand")

    result = run(category=args.category, brand_name=args.brand)
    if result.get("status") == "no_data":
        print("No performance data found. Import CSVs first with "
              "performance_parser.py")
    else:
        print(f"Feedback loop complete: {result['sample_size']} rows analysed")
        print(f"Patterns saved to data/processed/")
        if result.get("winning_patterns"):
            print("\n" + result["winning_patterns"])
