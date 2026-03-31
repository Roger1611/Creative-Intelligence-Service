"""
deliverables/sprint_generator.py — Generate the paid Creative Sprint deliverable.

The sprint is the full paid product: 50+ ad concepts organized by psychological
angle, complete competitor analysis, waste diagnosis, and a recommended creative
calendar. Output as PDF + JSON sidecar.

Structure:
  COVER       — Title, brand, date, concept count
  SECTION 1   — Executive Summary + Waste Diagnosis + ₹ Impact / ROI
  SECTION 2   — Competitor Intelligence Report
  SECTION 3   — Concepts by Psychological Angle (all 50+, expanded brief format)
  SECTION 4   — Recommended Creative Calendar (with production difficulty)

CLI: python -m deliverables.sprint_generator --brand "Mamaearth" --output "sprints/"
     python -m deliverables.sprint_generator --brand "Mamaearth" --batch abc123
"""

import argparse
import json
import logging
import math
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    HRFlowable,
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from config import FATIGUE_AD_MIN_DAYS, PROC_DIR, get_connection
from deliverables.utils import (
    format_inr,
    format_inr_short,
    load_json,
    severity_color,
    confidence_badge_text,
)
from scrapers.utils import safe_brand_slug

logger = logging.getLogger(__name__)

# ── Colours ───────────────────────────────────────────────────────────────────
_NAVY   = colors.HexColor("#0B1D3A")
_TEAL   = colors.HexColor("#0097A7")
_ORANGE = colors.HexColor("#F57C00")
_LIGHT  = colors.HexColor("#F4F6F9")
_GREY   = colors.HexColor("#6B7280")
_WHITE  = colors.white
_RED    = colors.HexColor("#E53935")
_GREEN  = colors.HexColor("#43A047")
_LIGHT_TEAL = colors.HexColor("#E0F7FA")

PAGE_W, PAGE_H = A4
_MARGIN = 18 * mm
_CONTENT_W = PAGE_W - 2 * _MARGIN

# Accent colour per trigger for visual variety
_TRIGGER_COLORS = {
    "status":              colors.HexColor("#7C4DFF"),
    "fear":                colors.HexColor("#E53935"),
    "social_proof":        colors.HexColor("#43A047"),
    "transformation":      colors.HexColor("#0097A7"),
    "agitation_solution":  colors.HexColor("#F57C00"),
    "curiosity":           colors.HexColor("#FFC107"),
    "urgency":             colors.HexColor("#D32F2F"),
    "authority":           colors.HexColor("#1565C0"),
    "belonging":           colors.HexColor("#AB47BC"),
    "aspiration":          colors.HexColor("#00897B"),
}

# Production difficulty → colour for badges
_DIFF_COLORS = {
    "low":    colors.HexColor("#43A047"),
    "medium": colors.HexColor("#F57C00"),
    "high":   colors.HexColor("#E53935"),
}


# ── Styles ────────────────────────────────────────────────────────────────────

def _s(name, **kw):
    defaults = {"fontName": "Helvetica", "fontSize": 10, "textColor": _NAVY,
                "leading": 14}
    defaults.update(kw)
    return ParagraphStyle(name, **defaults)

S_COVER_T = _s("cover_t", fontSize=28, fontName="Helvetica-Bold",
                alignment=TA_CENTER, spaceAfter=4)
S_COVER_B = _s("cover_b", fontSize=14, textColor=_TEAL,
                fontName="Helvetica-Bold", alignment=TA_CENTER, spaceAfter=4)
S_COVER_M = _s("cover_m", fontSize=10, textColor=_GREY,
                alignment=TA_CENTER, spaceAfter=2)
S_H1      = _s("h1", fontSize=20, fontName="Helvetica-Bold", spaceBefore=6,
                spaceAfter=6, textColor=_NAVY)
S_H2      = _s("h2", fontSize=15, fontName="Helvetica-Bold", spaceBefore=4,
                spaceAfter=4, textColor=_NAVY)
S_H3      = _s("h3", fontSize=12, fontName="Helvetica-Bold", spaceBefore=3,
                spaceAfter=2, textColor=_NAVY)
S_BODY    = _s("body", fontSize=10, leading=14, spaceAfter=4)
S_SMALL   = _s("small", fontSize=8, textColor=_GREY, leading=10)
S_LABEL   = _s("label", fontSize=8, textColor=_GREY,
                fontName="Helvetica-Bold", spaceAfter=1)
S_FOOTER  = _s("footer", fontSize=8, textColor=_GREY,
                alignment=TA_CENTER, spaceBefore=4)
S_SECTION_NOTE = _s("secnote", fontSize=9, textColor=_GREY, leading=12,
                     fontName="Helvetica-Oblique", spaceAfter=4)


# ── Public API ────────────────────────────────────────────────────────────────

def run(
    brand_name: str,
    batch_id: str | None = None,
    output_dir: str = "sprints",
) -> Path:
    """
    Generate the full Creative Sprint deliverable for *brand_name*.

    If *batch_id* is None, uses the latest concept batch.
    Returns path to the generated PDF (JSON sidecar written alongside).
    """
    data = _gather_data(brand_name, batch_id)

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    slug = safe_brand_slug(brand_name)
    bid  = data["batch_id"]
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path  = out_dir / f"{slug}_sprint_{bid}_{ts}.pdf"
    json_path = out_path.with_suffix(".json")

    # JSON sidecar with all raw data
    json_path.write_text(
        json.dumps(data["raw_export"], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("Sprint JSON saved → %s", json_path)

    _build_pdf(data, out_path)
    logger.info("Sprint PDF saved → %s", out_path)
    return out_path


# ── Data gathering ────────────────────────────────────────────────────────────

def _gather_data(brand_name: str, batch_id: str | None) -> dict:
    conn = get_connection()
    try:
        brand = conn.execute(
            "SELECT * FROM brands WHERE name = ? AND is_client = 1",
            (brand_name,),
        ).fetchone()
        if not brand:
            raise ValueError(f"Client brand '{brand_name}' not found.")
        brand = dict(brand)

        # Determine batch
        if batch_id:
            concepts_rows = conn.execute(
                """SELECT * FROM creative_concepts
                   WHERE client_brand_id = ? AND batch_id = ?
                   ORDER BY id""",
                (brand["id"], batch_id),
            ).fetchall()
        else:
            # Latest batch
            latest = conn.execute(
                """SELECT batch_id FROM creative_concepts
                   WHERE client_brand_id = ?
                   ORDER BY generated_at DESC LIMIT 1""",
                (brand["id"],),
            ).fetchone()
            if not latest:
                raise ValueError(f"No concept batches for '{brand_name}'.")
            batch_id = latest["batch_id"]
            concepts_rows = conn.execute(
                """SELECT * FROM creative_concepts
                   WHERE client_brand_id = ? AND batch_id = ?
                   ORDER BY id""",
                (brand["id"], batch_id),
            ).fetchall()

        concepts = [dict(r) for r in concepts_rows]

        # Waste report
        wr = conn.execute(
            """SELECT * FROM waste_reports WHERE client_brand_id = ?
               ORDER BY generated_at DESC LIMIT 1""",
            (brand["id"],),
        ).fetchone()
        waste_report = dict(wr) if wr else {}

        # Client ads
        client_ads = [dict(r) for r in conn.execute(
            """SELECT a.*, aa.psychological_trigger, aa.copy_tone,
                      aa.visual_layout, json_extract(aa.analysis_json, '$.effectiveness_score') AS effectiveness_score
               FROM ads a LEFT JOIN ad_analysis aa ON aa.ad_id = a.id
               WHERE a.brand_id = ?""",
            (brand["id"],),
        ).fetchall()]

        # Competitor analysis
        comp_analysis = [dict(r) for r in conn.execute(
            """SELECT aa.*, a.ad_library_id, a.duration_days, a.creative_type,
                      b.name AS competitor_name
               FROM ad_analysis aa
               JOIN ads a ON aa.ad_id = a.id
               JOIN brands b ON a.brand_id = b.id
               JOIN competitor_sets cs ON cs.competitor_brand_id = b.id
               WHERE cs.client_brand_id = ?
               ORDER BY b.name, aa.analyzed_at DESC""",
            (brand["id"],),
        ).fetchall()]

        # Competitors
        competitors = [dict(r) for r in conn.execute(
            """SELECT b.* FROM brands b
               JOIN competitor_sets cs ON cs.competitor_brand_id = b.id
               WHERE cs.client_brand_id = ?""",
            (brand["id"],),
        ).fetchall()]

    finally:
        conn.close()

    # Load pre-computed analysis from processed JSON files
    slug = brand_name.lower().replace(" ", "_")
    brand_intel = load_json(PROC_DIR / f"{slug}_brand_intel.json", "brand intel")
    competitor_deep_dive = load_json(
        PROC_DIR / f"{slug}_competitor_deep_dive.json", "competitor deep dive")
    impact_estimate = load_json(
        PROC_DIR / f"{slug}_impact_estimate.json", "impact estimate")

    # Build raw JSON export
    raw_export = {
        "brand": brand,
        "batch_id": batch_id,
        "generated_at": datetime.now().isoformat(),
        "concepts": concepts,
        "waste_report": waste_report,
        "competitor_analysis": comp_analysis,
    }

    return {
        "brand": brand,
        "batch_id": batch_id,
        "concepts": concepts,
        "waste_report": waste_report,
        "client_ads": client_ads,
        "comp_analysis": comp_analysis,
        "competitors": competitors,
        "brand_intel": brand_intel,
        "competitor_deep_dive": competitor_deep_dive,
        "impact_estimate": impact_estimate,
        "raw_export": raw_export,
    }


# ── PDF builder ───────────────────────────────────────────────────────────────

def _build_pdf(data: dict, out_path: Path) -> None:
    doc = SimpleDocTemplate(
        str(out_path), pagesize=A4,
        leftMargin=_MARGIN, rightMargin=_MARGIN,
        topMargin=_MARGIN, bottomMargin=_MARGIN,
    )
    story = []

    story += _section_cover(data)
    story.append(PageBreak())
    story += _section_executive_summary(data)
    story.append(PageBreak())
    story += _section_competitor_intel(data)
    story.append(PageBreak())
    story += _section_concepts(data)
    story.append(PageBreak())
    story += _section_calendar(data)

    doc.build(story)


# ── COVER PAGE ────────────────────────────────────────────────────────────────

def _section_cover(data: dict) -> list:
    brand    = data["brand"]
    concepts = data["concepts"]
    story    = []

    story.append(Spacer(1, 50 * mm))
    story.append(HRFlowable(width="40%", thickness=3, color=_TEAL))
    story.append(Spacer(1, 10 * mm))
    story.append(Paragraph("Creative Sprint", S_COVER_T))
    story.append(Paragraph(brand["name"], S_COVER_B))
    story.append(Spacer(1, 6 * mm))
    story.append(Paragraph(
        f"{len(concepts)} Strategic Ad Concepts &bull; "
        f"Batch {data['batch_id']}",
        S_COVER_M))
    story.append(Paragraph(
        f"Prepared by Roger Krishna &bull; Creative Intelligence Service",
        S_COVER_M))
    story.append(Paragraph(datetime.now().strftime("%d %B %Y"), S_COVER_M))
    story.append(Spacer(1, 10 * mm))
    story.append(HRFlowable(width="40%", thickness=3, color=_TEAL))

    # Summary stats
    story.append(Spacer(1, 20 * mm))
    angles = Counter(c.get("psychological_angle", "other") for c in concepts)
    formats = Counter()
    for c in concepts:
        ctas = c.get("cta_variations_json") or "[]"
        # count format from visual_direction hints or default
        formats["concept"] += 1

    stats_text = (
        f"<b>{len(concepts)}</b> concepts across "
        f"<b>{len(angles)}</b> psychological angles"
    )
    story.append(Paragraph(stats_text, _s("covstats", fontSize=12,
                                           alignment=TA_CENTER, spaceAfter=4)))

    return story


# ── SECTION 1: Executive Summary + Waste Diagnosis ───────────────────────────

def _section_executive_summary(data: dict) -> list:
    wr           = data["waste_report"]
    ads          = data["client_ads"]
    impact_data  = data.get("impact_estimate", {})
    story        = []

    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 3 * mm))
    story.append(Paragraph("1. Executive Summary &amp; Waste Diagnosis", S_H1))
    story.append(Spacer(1, 2 * mm))

    # Key metrics row — now includes ₹ waste if available
    diversity = wr.get("creative_diversity_score")
    avg_refresh = wr.get("avg_refresh_days")
    active_count = len([a for a in ads if a.get("is_active")])
    fatigued_count = len([a for a in ads if a.get("is_active") and
                          (a.get("duration_days") or 0) >= FATIGUE_AD_MIN_DAYS])

    total_waste = _get_total_monthly_waste(impact_data)

    metrics = [
        (f"{diversity:.0f}" if diversity else "\u2014", "Diversity Score"),
        (f"{avg_refresh:.0f}d" if avg_refresh else "\u2014", "Avg Refresh"),
        (str(active_count), "Active Ads"),
        (str(fatigued_count), "Fatigued"),
    ]

    # Replace last metric with ₹ waste if available
    if total_waste > 0:
        metrics[3] = (format_inr(total_waste), "Monthly Waste")

    story.append(_metric_row(metrics))
    story.append(Spacer(1, 6 * mm))

    # ₹ Impact summary box
    if total_waste > 0:
        impact_html = (
            f"<b>Estimated Creative Waste:</b> "
            f"{format_inr(total_waste)}/month<br/>"
        )
        per_gap = impact_data.get("per_gap_impact", [])
        # Show top 3 gaps by impact
        sorted_gaps = sorted(
            per_gap, key=lambda g: float(g.get("estimated_monthly_impact_inr", 0)),
            reverse=True)
        for gi in sorted_gaps[:3]:
            title = gi.get("gap_title", "")
            amt = float(gi.get("estimated_monthly_impact_inr", 0))
            if title and amt:
                impact_html += (
                    f"&bull; {title}: {format_inr(amt)}/month<br/>")

        story.append(_callout_box(impact_html))
        story.append(Spacer(1, 4 * mm))

    # ROI section
    sprint_roi = impact_data.get("sprint_roi", {})
    if sprint_roi:
        sprint_price = sprint_roi.get("sprint_price", 0)
        est_savings = sprint_roi.get("estimated_monthly_savings", total_waste)
        payback = sprint_roi.get("payback_days", 0)
        if not payback and sprint_price and est_savings:
            payback = round(sprint_price / (est_savings / 30))

        roi_parts = []
        if sprint_price:
            roi_parts.append(
                f"<b>Sprint investment:</b> {format_inr(sprint_price)}")
        if est_savings:
            roi_parts.append(
                f"<b>Estimated monthly savings:</b> {format_inr(est_savings)}")
        if payback:
            roi_parts.append(
                f"<b>Payback period:</b> {payback} days")

        if roi_parts:
            story.append(Paragraph("ROI Projection", S_H3))
            for part in roi_parts:
                story.append(Paragraph(part, S_BODY))
            story.append(Spacer(1, 4 * mm))

    # Fatigue flags
    flags_raw = wr.get("fatigue_flags_json") or "[]"
    flags = json.loads(flags_raw) if isinstance(flags_raw, str) else flags_raw
    if flags:
        story.append(Paragraph("Fatigued Ads Requiring Immediate Refresh", S_H3))
        rows = [["Ad ID", "Days Running", "Reason"]]
        for f in flags:
            ad_id = f.get("ad_library_id", "\u2014")
            days = str(f.get("duration_days", "\u2014"))
            reason = f.get("fatigue_reason", "") or f.get("refresh_suggestion", "\u2014")
            rows.append([_trunc(ad_id, 22), days, _trunc(reason, 50)])
        story.append(_styled_table(rows, [55 * mm, 25 * mm, _CONTENT_W - 80 * mm]))
        story.append(Spacer(1, 6 * mm))

    # Recommendations
    recs_raw = wr.get("recommendations_json") or "[]"
    recs = json.loads(recs_raw) if isinstance(recs_raw, str) else recs_raw
    if recs:
        story.append(Paragraph("Priority Actions", S_H3))
        for i, rec in enumerate(recs, 1):
            rank = rec.get("rank", i)
            action = rec.get("action", "\u2014")
            impact = rec.get("expected_impact", "")
            effort = rec.get("effort", "")
            story.append(Paragraph(
                f"<b>{rank}.</b> {action}",
                _s(f"rec{i}", fontSize=10, spaceAfter=1, leftIndent=4 * mm)))
            if impact or effort:
                meta = []
                if impact:
                    meta.append(f"Impact: {impact}")
                if effort:
                    meta.append(f"Effort: {effort}")
                story.append(Paragraph(
                    " &bull; ".join(meta),
                    _s(f"recm{i}", fontSize=8, textColor=_GREY,
                       spaceAfter=4, leftIndent=8 * mm)))

    return story


def _get_total_monthly_waste(impact_data: dict) -> float:
    """Extract total estimated monthly waste from impact estimate data."""
    if not impact_data:
        return 0.0
    total = impact_data.get("total_estimated_monthly_waste", 0)
    if total:
        return float(total)
    per_gap = impact_data.get("per_gap_impact", [])
    if per_gap:
        return sum(float(g.get("estimated_monthly_impact_inr", 0)) for g in per_gap)
    return 0.0


# ── SECTION 2: Competitor Intelligence ────────────────────────────────────────

def _section_competitor_intel(data: dict) -> list:
    comp_analysis = data["comp_analysis"]
    competitors   = data["competitors"]
    story         = []

    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 3 * mm))
    story.append(Paragraph("2. Competitor Intelligence Report", S_H1))
    story.append(Spacer(1, 2 * mm))

    if not comp_analysis:
        story.append(Paragraph(
            "<i>No competitor analysis data yet. Run the full pipeline.</i>",
            S_BODY))
        return story

    # Group by competitor
    by_comp = defaultdict(list)
    for a in comp_analysis:
        by_comp[a.get("competitor_name", "Unknown")].append(a)

    for comp_name, analyses in by_comp.items():
        story.append(Paragraph(comp_name, S_H2))

        # Summary stats
        triggers = Counter(a.get("psychological_trigger") for a in analyses
                           if a.get("psychological_trigger"))
        tones = Counter(a.get("copy_tone") for a in analyses
                        if a.get("copy_tone"))
        formats = Counter(a.get("creative_type") for a in analyses
                          if a.get("creative_type"))

        stats_data = [
            ["Total Ads Analysed", str(len(analyses))],
            ["Top Triggers", ", ".join(
                t.replace("_", " ").title() for t, _ in triggers.most_common(3))
             or "\u2014"],
            ["Top Tones", ", ".join(
                t.replace("_", " ").title() for t, _ in tones.most_common(2))
             or "\u2014"],
            ["Formats", ", ".join(
                f"{v} {k}" for k, v in formats.most_common()) or "\u2014"],
        ]
        t = Table(stats_data, colWidths=[45 * mm, _CONTENT_W - 45 * mm])
        t.setStyle(TableStyle([
            ("FONTNAME",    (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTSIZE",    (0, 0), (-1, -1), 9),
            ("TEXTCOLOR",   (0, 0), (0, -1), _GREY),
            ("TOPPADDING",    (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("ROWBACKGROUNDS", (0, 0), (-1, -1), [_WHITE, _LIGHT]),
        ]))
        story.append(t)
        story.append(Spacer(1, 2 * mm))

        # Top ads detail table (up to 5)
        if analyses:
            detail_rows = [["Ad ID", "Trigger", "Tone", "Days", "Layout"]]
            for a in analyses[:5]:
                detail_rows.append([
                    _trunc(a.get("ad_library_id", "\u2014"), 18),
                    (a.get("psychological_trigger") or "\u2014").replace("_", " "),
                    (a.get("copy_tone") or "\u2014"),
                    str(a.get("duration_days", "\u2014")),
                    _trunc(a.get("visual_layout") or "\u2014", 30),
                ])
            story.append(_styled_table(
                detail_rows,
                [40 * mm, 30 * mm, 25 * mm, 15 * mm, _CONTENT_W - 110 * mm],
            ))

        story.append(Spacer(1, 6 * mm))

    return story


# ── SECTION 3: All Concepts by Angle ─────────────────────────────────────────

def _section_concepts(data: dict) -> list:
    concepts = data["concepts"]
    story    = []

    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 3 * mm))
    story.append(Paragraph(
        f"3. Creative Concepts ({len(concepts)} Total)", S_H1))
    story.append(Spacer(1, 2 * mm))

    # Group by psychological angle
    by_angle = defaultdict(list)
    for c in concepts:
        angle = c.get("psychological_angle") or "other"
        by_angle[angle].append(c)

    # Sort by count descending
    sorted_angles = sorted(by_angle.items(), key=lambda x: -len(x[1]))

    concept_num = 0
    for angle, angle_concepts in sorted_angles:
        nice_angle = angle.replace("_", " ").title()
        accent = _TRIGGER_COLORS.get(angle, _TEAL)

        story.append(HRFlowable(width="30%", thickness=2, color=accent))
        story.append(Spacer(1, 2 * mm))
        story.append(Paragraph(
            f"{nice_angle} ({len(angle_concepts)} concepts)",
            _s(f"ang_{angle}", fontSize=14, fontName="Helvetica-Bold",
               textColor=accent, spaceBefore=4, spaceAfter=4)))

        for c in angle_concepts:
            concept_num += 1
            story += _concept_card(concept_num, c, accent)

    return story


def _concept_card(num: int, concept: dict, accent) -> list:
    """Render a single concept as an expanded creative brief card."""
    elements = []

    hook = concept.get("hook_text") or "\u2014"
    body = concept.get("body_script") or ""
    ctas_raw = concept.get("cta_variations_json") or "[]"
    if isinstance(ctas_raw, str):
        try:
            ctas = json.loads(ctas_raw)
        except (json.JSONDecodeError, TypeError):
            ctas = []
    else:
        ctas = ctas_raw

    # Header: number + hook
    elements.append(Paragraph(
        f"<font color='{accent.hexval()}'><b>#{num}</b></font>&nbsp;&nbsp;"
        f"&ldquo;{hook}&rdquo;",
        _s(f"ch{num}", fontSize=11, fontName="Helvetica-Bold", spaceAfter=2)))

    # Hindi hook if available
    hook_hindi = concept.get("hook_text_hindi", "")
    if hook_hindi:
        elements.append(Paragraph(
            f"<b>Hindi:</b> &ldquo;{hook_hindi}&rdquo;",
            _s(f"chh{num}", fontSize=9, textColor=_GREY, leading=12,
               spaceAfter=2, leftIndent=4 * mm)))

    # Badge row: format + production difficulty + estimated time
    badge_parts = []
    fmt = concept.get("format") or concept.get("creative_type") or ""
    if fmt:
        badge_parts.append(fmt.replace("_", " ").title())
    prod_diff = concept.get("production_difficulty", "")
    if prod_diff:
        diff_color = _DIFF_COLORS.get(prod_diff.lower(), _GREY)
        badge_parts.append(
            f"<font color='{diff_color.hexval()}'><b>{prod_diff.title()}"
            f" difficulty</b></font>")
    est_time = concept.get("estimated_production_time", "")
    if est_time:
        badge_parts.append(f"~{est_time}")
    if badge_parts:
        elements.append(Paragraph(
            " &bull; ".join(badge_parts),
            _s(f"cbadge{num}", fontSize=8, textColor=_GREY, leading=11,
               spaceAfter=2, leftIndent=4 * mm)))

    # Text overlay
    text_overlay = concept.get("text_overlay", "")
    if text_overlay:
        elements.append(Paragraph(
            f"<b>Text overlay:</b> &ldquo;{text_overlay}&rdquo;",
            _s(f"cto{num}", fontSize=9, leftIndent=4 * mm,
               spaceAfter=2, leading=12)))

    # Body script
    if body:
        # Strip data backing suffix if present
        display_body = body
        if "[DATA BACKING]" in display_body:
            display_body = display_body[:display_body.index("[DATA BACKING]")].strip()
        elements.append(Paragraph(display_body, _s(
            f"cb{num}", fontSize=9, textColor=_NAVY, leading=12,
            spaceAfter=2, leftIndent=4 * mm)))

    # Visual direction — render full object if available
    visual = concept.get("visual_direction") or ""
    visual_json_raw = concept.get("visual_direction_json", "")
    vis_obj = None
    if visual_json_raw:
        try:
            vis_obj = json.loads(visual_json_raw) if isinstance(
                visual_json_raw, str) else visual_json_raw
        except (json.JSONDecodeError, TypeError):
            pass
    if not vis_obj and isinstance(visual, dict):
        vis_obj = visual

    if vis_obj and isinstance(vis_obj, dict):
        vis_lines = []
        for vk, vl in [
            ("aspect_ratio", "Aspect ratio"),
            ("scene_description", "Scene"),
            ("talent_direction", "Talent"),
            ("product_placement", "Product"),
            ("lighting", "Lighting"),
            ("text_overlay_position", "Text position"),
            ("color_mood", "Color/mood"),
        ]:
            vv = vis_obj.get(vk, "")
            if vv:
                vis_lines.append(f"<b>{vl}:</b> {vv}")
        if vis_lines:
            elements.append(Paragraph(
                "<b>Visual Direction:</b><br/>" + "<br/>".join(vis_lines),
                _s(f"cv{num}", fontSize=8, textColor=_GREY, leading=11,
                   spaceAfter=2, leftIndent=4 * mm)))
    elif visual:
        vis_text = visual if isinstance(visual, str) else str(visual)
        elements.append(Paragraph(
            f"<b>Visual:</b> {_trunc(vis_text, 200)}",
            _s(f"cv{num}", fontSize=8, textColor=_GREY, leading=11,
               spaceAfter=2, leftIndent=4 * mm)))

    # Sound design
    sound = concept.get("sound_design", "")
    if sound:
        elements.append(Paragraph(
            f"<b>Sound design:</b> {sound}",
            _s(f"csd{num}", fontSize=8, textColor=_GREY, leading=11,
               spaceAfter=2, leftIndent=4 * mm)))

    # CTA placement
    cta_placement = concept.get("cta_placement", "")
    if cta_placement:
        elements.append(Paragraph(
            f"<b>CTA placement:</b> {cta_placement}",
            _s(f"ccta{num}", fontSize=8, textColor=_GREY, leading=11,
               spaceAfter=2, leftIndent=4 * mm)))

    # CTAs
    if ctas:
        cta_str = " &nbsp;|&nbsp; ".join(str(c) for c in ctas[:3])
        elements.append(Paragraph(
            f"<b>CTAs:</b> {cta_str}",
            _s(f"cc{num}", fontSize=8, textColor=_GREY, leading=11,
               spaceAfter=2, leftIndent=4 * mm)))

    # Carousel sequence (card-by-card breakdown)
    carousel_raw = concept.get("carousel_sequence", None)
    if carousel_raw:
        if isinstance(carousel_raw, str):
            try:
                carousel_seq = json.loads(carousel_raw)
            except (json.JSONDecodeError, TypeError):
                carousel_seq = None
        else:
            carousel_seq = carousel_raw

        if carousel_seq and isinstance(carousel_seq, list):
            elements.append(Paragraph(
                f"<b>Carousel Sequence ({len(carousel_seq)} cards):</b>",
                _s(f"ccar_h{num}", fontSize=9, fontName="Helvetica-Bold",
                   leftIndent=4 * mm, spaceAfter=2)))
            for ci, card in enumerate(carousel_seq, 1):
                if isinstance(card, dict):
                    card_text = card.get("text", "") or card.get("headline", "")
                    card_vis = card.get("visual", "") or card.get("image", "")
                    card_line = f"Card {ci}: "
                    if card_text:
                        card_line += f"&ldquo;{_trunc(card_text, 60)}&rdquo;"
                    if card_vis:
                        card_line += f" \u2014 {_trunc(card_vis, 60)}"
                    elements.append(Paragraph(
                        card_line,
                        _s(f"ccard{num}_{ci}", fontSize=8, textColor=_GREY,
                           leading=11, leftIndent=8 * mm, spaceAfter=1)))
                elif isinstance(card, str):
                    elements.append(Paragraph(
                        f"Card {ci}: {_trunc(card, 80)}",
                        _s(f"ccard{num}_{ci}", fontSize=8, textColor=_GREY,
                           leading=11, leftIndent=8 * mm, spaceAfter=1)))

    # A/B test variable
    ab_test = concept.get("ab_test_variable", "")
    if ab_test:
        elements.append(Paragraph(
            f"<b>A/B test variable:</b> {ab_test}",
            _s(f"cab{num}", fontSize=8, textColor=_GREY, leading=11,
               spaceAfter=2, leftIndent=4 * mm)))

    # Competitor inspiration
    comp_ref = concept.get("competitor_inspiration", "")
    if not comp_ref:
        comp_ref = concept.get("competitor_reference", "")
    if comp_ref:
        elements.append(Paragraph(
            f"<b>Inspired by:</b> {comp_ref}",
            _s(f"cinsp{num}", fontSize=8, textColor=_GREY, leading=11,
               spaceAfter=2, leftIndent=4 * mm)))

    # Data backing
    data_backing = concept.get("data_backing", "")
    if not data_backing and "[DATA BACKING]" in body:
        db_idx = body.index("[DATA BACKING]")
        data_backing = body[db_idx + len("[DATA BACKING]"):].strip()
    if data_backing:
        elements.append(Paragraph(
            f"<font color='{_TEAL.hexval()}'><b>Why this works:</b></font> "
            f"{data_backing}",
            _s(f"cwhy{num}", fontSize=8, leading=11,
               leftIndent=4 * mm, spaceAfter=2)))

    elements.append(Spacer(1, 4 * mm))
    return elements


# ── SECTION 4: Recommended Creative Calendar ─────────────────────────────────

def _section_calendar(data: dict) -> list:
    concepts = data["concepts"]
    brand    = data["brand"]
    story    = []

    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 3 * mm))
    story.append(Paragraph("4. Recommended Creative Calendar", S_H1))
    story.append(Paragraph(
        "Deploy concepts in this order to maximise angle diversity and "
        "prevent creative fatigue. Each week introduces fresh psychological "
        "triggers and formats. Production difficulty is shown so your team "
        "knows which concepts are quick wins vs full production days.",
        S_BODY))
    story.append(Spacer(1, 4 * mm))

    if not concepts:
        story.append(Paragraph("<i>No concepts to schedule.</i>", S_BODY))
        return story

    # Strategy: round-robin across angles, 7 concepts per week
    by_angle = defaultdict(list)
    for c in concepts:
        by_angle[c.get("psychological_angle") or "other"].append(c)

    # Interleave angles for maximum diversity
    scheduled = []
    angle_iters = {k: iter(v) for k, v in by_angle.items()}
    angles_cycle = list(angle_iters.keys())
    idx = 0
    while len(scheduled) < len(concepts):
        angle = angles_cycle[idx % len(angles_cycle)]
        try:
            scheduled.append(next(angle_iters[angle]))
        except StopIteration:
            angles_cycle.remove(angle)
            if not angles_cycle:
                break
            idx = idx % len(angles_cycle) if angles_cycle else 0
            continue
        idx += 1

    # Build weekly schedule
    concepts_per_week = 7
    total_weeks = math.ceil(len(scheduled) / concepts_per_week)
    start_date = datetime.now() + timedelta(days=(7 - datetime.now().weekday()) % 7)

    for week in range(total_weeks):
        week_start = start_date + timedelta(weeks=week)
        week_end   = week_start + timedelta(days=6)
        week_concepts = scheduled[
            week * concepts_per_week: (week + 1) * concepts_per_week]

        if not week_concepts:
            break

        story.append(Paragraph(
            f"Week {week + 1}: {week_start.strftime('%d %b')} \u2014 "
            f"{week_end.strftime('%d %b %Y')}",
            S_H3))

        rows = [["Day", "Hook", "Angle", "Difficulty"]]
        days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        for i, c in enumerate(week_concepts):
            day_label = days[i] if i < len(days) else f"D{i + 1}"
            diff = c.get("production_difficulty", "\u2014")
            if diff and diff != "\u2014":
                diff = diff.title()
            rows.append([
                day_label,
                _trunc(c.get("hook_text") or "\u2014", 40),
                (c.get("psychological_angle") or "\u2014").replace("_", " "),
                diff,
            ])

        story.append(_styled_table(
            rows, [14 * mm, _CONTENT_W - 84 * mm, 40 * mm, 30 * mm]))
        story.append(Spacer(1, 4 * mm))

    # Footer
    story.append(Spacer(1, 6 * mm))
    story.append(HRFlowable(width="100%", thickness=1, color=_LIGHT))
    story.append(Paragraph(
        f"Creative Intelligence Service &bull; {brand['name']} Sprint "
        f"&bull; Batch {data['batch_id']} &bull; Confidential",
        S_FOOTER))

    return story


# ── Shared helpers ────────────────────────────────────────────────────────────

def _metric_row(items: list[tuple[str, str]]) -> Table:
    col_w = _CONTENT_W / len(items)
    top = [Paragraph(v, _s("mv", fontSize=20, fontName="Helvetica-Bold",
                            alignment=TA_CENTER)) for v, _ in items]
    bot = [Paragraph(l, _s("ml", fontSize=8, textColor=_GREY,
                            alignment=TA_CENTER)) for _, l in items]
    t = Table([top, bot], colWidths=[col_w] * len(items))
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), _LIGHT),
        ("TOPPADDING",    (0, 0), (-1, 0), 10),
        ("BOTTOMPADDING", (0, 1), (-1, 1), 8),
        ("ALIGN",    (0, 0), (-1, -1), "CENTER"),
        ("VALIGN",   (0, 0), (-1, -1), "MIDDLE"),
        ("LINEAFTER", (0, 0), (-2, -1), 1, _WHITE),
        ("ROUNDEDCORNERS", [4, 4, 4, 4]),
    ]))
    return t


def _styled_table(rows: list, col_widths: list) -> Table:
    t = Table(rows, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND",  (0, 0), (-1, 0), _NAVY),
        ("TEXTCOLOR",   (0, 0), (-1, 0), _WHITE),
        ("FONTNAME",    (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, -1), 8),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [_WHITE, _LIGHT]),
        ("GRID",        (0, 0), (-1, -1), 0.4, colors.Color(0.85, 0.85, 0.85)),
        ("TOPPADDING",    (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING",   (0, 0), (-1, -1), 4),
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
    ]))
    return t


def _callout_box(html_text: str) -> Table:
    """Teal-bordered callout box."""
    p = Paragraph(html_text, _s("callout_sp", fontSize=10, textColor=_NAVY,
                                 leading=13, spaceAfter=4))
    t = Table([[p]], colWidths=[_CONTENT_W - 4 * mm])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), _LIGHT_TEAL),
        ("LINEBEFORECOL", (0, 0), (0, -1), 3, _TEAL),
        ("TOPPADDING",    (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("LEFTPADDING",   (0, 0), (-1, -1), 10),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 10),
    ]))
    return t


def _trunc(text: str, maxlen: int) -> str:
    if len(text) <= maxlen:
        return text
    return text[:maxlen - 1] + "\u2026"


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Generate a full Creative Sprint PDF + JSON deliverable.")
    parser.add_argument("--brand", required=True,
                        help="Brand name (must exist in DB as is_client=1)")
    parser.add_argument("--batch", default=None,
                        help="Batch ID (default: latest batch)")
    parser.add_argument("--output", default="sprints",
                        help="Output directory (default: sprints/)")
    args = parser.parse_args()

    path = run(args.brand, batch_id=args.batch, output_dir=args.output)
    print(f"Sprint deliverable generated: {path}")
