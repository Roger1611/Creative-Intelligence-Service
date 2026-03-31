"""
deliverables/audit_generator.py — Generate the 9-page Creative Intelligence Audit PDF.

A comprehensive, intelligence-grade audit that makes founders uncomfortable
about their creative spend. Every number comes from real data.

PAGE 1: Executive Diagnosis (verdict + 4 metric cards incl. ₹ waste)
PAGE 2: Competitive Landscape Overview (competitor rankings + conditional formatting)
PAGE 3: Competitor War Room — Winner Dissections (top 3 per competitor)
PAGE 4: Hook Swipe File (full hooks grouped by trigger, with hook_structure)
PAGE 5: Your Creative Gaps — With ₹ Impact (gaps sorted by estimated impact)
PAGE 6: Visual Pattern Intelligence (patterns + actionable checklist)
PAGE 7: Creative Strategy Blueprint (product-specific matrix + calendar)
PAGE 8: Sample Creative Briefs (5 expanded briefs with visual direction)
PAGE 9: Priority Action Plan + ROI (actions + payback calculation)

ReportLab note: coordinates are bottom-left origin (y=0 = bottom of page).

CLI: python -m deliverables.audit_generator --brand "Mamaearth" --output "audits/"
"""

import argparse
import json
import logging
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.platypus import (
    HRFlowable,
    Image,
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from config import (
    FATIGUE_AD_MIN_DAYS,
    PROC_DIR,
    PSYCHOLOGICAL_TRIGGERS,
    RAW_DIR,
    get_connection,
)
from deliverables.utils import (
    format_inr as _format_inr,
    format_inr_short as _format_inr_short,
    load_json as _load_json,
    severity_color as _severity_color,
    confidence_badge_text as _confidence_badge_text,
)
from scrapers.utils import safe_brand_slug

logger = logging.getLogger(__name__)

# ── Brand colours ─────────────────────────────────────────────────────────────
_NAVY   = colors.HexColor("#0B1D3A")
_TEAL   = colors.HexColor("#0097A7")
_ORANGE = colors.HexColor("#F57C00")
_LIGHT  = colors.HexColor("#F4F6F9")
_GREY   = colors.HexColor("#6B7280")
_WHITE  = colors.white
_RED    = colors.HexColor("#E53935")
_GREEN  = colors.HexColor("#43A047")
_LIGHT_TEAL = colors.HexColor("#E0F7FA")
_LIGHT_RED  = colors.HexColor("#FFEBEE")
_LIGHT_GREEN = colors.HexColor("#E8F5E9")
_LIGHT_ORANGE = colors.HexColor("#FFF3E0")

PAGE_W, PAGE_H = A4
_MARGIN = 18 * mm
_CONTENT_W = PAGE_W - 2 * _MARGIN


# ── Styles ────────────────────────────────────────────────────────────────────

def _s(name, **kw):
    defaults = {"fontName": "Helvetica", "fontSize": 10, "textColor": _NAVY,
                "leading": 14}
    defaults.update(kw)
    return ParagraphStyle(name, **defaults)

S_TITLE    = _s("title",   fontSize=24, fontName="Helvetica-Bold", spaceAfter=2)
S_SUBTITLE = _s("sub",     fontSize=11, textColor=_GREY, spaceAfter=10)
S_H2       = _s("h2",      fontSize=15, fontName="Helvetica-Bold", spaceBefore=6,
                 spaceAfter=4, textColor=_NAVY)
S_H3       = _s("h3",      fontSize=12, fontName="Helvetica-Bold", spaceBefore=4,
                 spaceAfter=2, textColor=_NAVY)
S_H4       = _s("h4",      fontSize=10, fontName="Helvetica-Bold", spaceBefore=3,
                 spaceAfter=2, textColor=_NAVY)
S_BODY     = _s("body",    fontSize=10, leading=14, spaceAfter=4)
S_SMALL    = _s("small",   fontSize=8,  textColor=_GREY, leading=10)
S_LABEL    = _s("label",   fontSize=8,  textColor=_GREY, fontName="Helvetica-Bold",
                 spaceAfter=1)
S_METRIC   = _s("metric",  fontSize=20, fontName="Helvetica-Bold", alignment=TA_CENTER)
S_METRIC_L = _s("mlabel",  fontSize=8,  textColor=_GREY, alignment=TA_CENTER,
                 spaceAfter=2)
S_VERDICT  = _s("verdict", fontSize=11, textColor=_WHITE, fontName="Helvetica-Bold",
                 leading=15, spaceAfter=4)
S_CALLOUT  = _s("callout", fontSize=10, textColor=_NAVY, leading=13, spaceAfter=4)
S_HOOK     = _s("hook",    fontSize=11, fontName="Helvetica-Bold", textColor=_NAVY,
                 spaceAfter=1)
S_HOOK_M   = _s("hookmeta", fontSize=9, textColor=_GREY, leading=12, spaceAfter=6)
S_CTA_BOX  = _s("ctabox",  fontSize=11, textColor=_TEAL, fontName="Helvetica-Bold",
                 alignment=TA_CENTER, spaceAfter=2)
S_FOOTER   = _s("footer",  fontSize=9,  textColor=_GREY, alignment=TA_CENTER,
                 spaceBefore=6)
S_DISCLAIMER = _s("disclaimer", fontSize=7, textColor=_GREY, leading=9, spaceAfter=2)
S_INSIGHT  = _s("insight",  fontSize=10, textColor=_NAVY, leading=13, spaceAfter=3,
                 leftIndent=4 * mm)
S_CHECK_G  = _s("check_green", fontSize=9, textColor=_GREEN, leading=12)
S_CHECK_R  = _s("check_red",   fontSize=9, textColor=_RED,   leading=12)
S_SECTION_NOTE = _s("secnote", fontSize=9, textColor=_GREY, leading=12,
                     fontName="Helvetica-Oblique", spaceAfter=4)


# ── Public API ────────────────────────────────────────────────────────────────

def run(brand_name: str, output_dir: str = "audits") -> Path:
    """Generate a PDF audit for *brand_name*. Returns path to the PDF."""
    data = _gather_data(brand_name)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    slug = safe_brand_slug(brand_name)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"{slug}_audit_{ts}.pdf"

    _build_pdf(data, out_path)
    logger.info("Audit PDF saved → %s", out_path)
    return out_path


# ── Data gathering ────────────────────────────────────────────────────────────

def _gather_data(brand_name: str) -> dict:
    """Pull everything needed for the audit from the database + processed JSONs."""
    conn = get_connection()
    try:
        brand = conn.execute(
            "SELECT * FROM brands WHERE name = ? AND is_client = 1",
            (brand_name,),
        ).fetchone()
        if not brand:
            raise ValueError(f"Client brand '{brand_name}' not found in database.")
        brand = dict(brand)

        # Client ads
        client_ads = [dict(r) for r in conn.execute(
            """SELECT a.*, aa.psychological_trigger, aa.copy_tone,
                      json_extract(aa.analysis_json, '$.effectiveness_score') AS effectiveness_score
               FROM ads a LEFT JOIN ad_analysis aa ON aa.ad_id = a.id
               WHERE a.brand_id = ?""",
            (brand["id"],),
        ).fetchall()]

        # Waste report
        wr = conn.execute(
            """SELECT * FROM waste_reports WHERE client_brand_id = ?
               ORDER BY generated_at DESC LIMIT 1""",
            (brand["id"],),
        ).fetchone()
        waste_report = dict(wr) if wr else {}

        # Competitors
        competitors = [dict(r) for r in conn.execute(
            """SELECT b.* FROM brands b
               JOIN competitor_sets cs ON cs.competitor_brand_id = b.id
               WHERE cs.client_brand_id = ?""",
            (brand["id"],),
        ).fetchall()]

        # Competitor ads + analysis (per competitor)
        comp_data = []
        for comp in competitors:
            comp_ads = [dict(r) for r in conn.execute(
                """SELECT a.*, aa.psychological_trigger, aa.copy_tone
                   FROM ads a LEFT JOIN ad_analysis aa ON aa.ad_id = a.id
                   WHERE a.brand_id = ?""",
                (comp["id"],),
            ).fetchall()]
            comp_data.append({"brand": comp, "ads": comp_ads})

        # Sample concepts (latest batch, up to 5)
        concepts = [dict(r) for r in conn.execute(
            """SELECT * FROM creative_concepts WHERE client_brand_id = ?
               ORDER BY generated_at DESC LIMIT 5""",
            (brand["id"],),
        ).fetchall()]

    finally:
        conn.close()

    # Load pre-computed analysis from processed JSON files
    slug = brand_name.lower().replace(" ", "_")

    fatigue_data = _load_json(PROC_DIR / f"{slug}_fatigue.json", "fatigue")
    intel_data = _load_json(PROC_DIR / f"{slug}_category_intelligence.json", "category intel")
    profit_data = _load_json(PROC_DIR / f"{slug}_profitable_ads_summary.json", "profitability")
    brand_intel = _load_json(PROC_DIR / f"{slug}_brand_intel.json", "brand intel")
    competitor_deep_dive = _load_json(PROC_DIR / f"{slug}_competitor_deep_dive.json", "competitor deep dive")
    impact_estimate = _load_json(PROC_DIR / f"{slug}_impact_estimate.json", "impact estimate")

    return {
        "brand": brand,
        "client_ads": client_ads,
        "waste_report": waste_report,
        "competitors": comp_data,
        "sample_concepts": concepts,
        "fatigue_analysis": fatigue_data,
        "category_intel": intel_data,
        "profitability_summary": profit_data,
        "brand_intel": brand_intel,
        "competitor_deep_dive": competitor_deep_dive,
        "impact_estimate": impact_estimate,
    }


# ── PDF builder ───────────────────────────────────────────────────────────────

def _build_pdf(data: dict, out_path: Path) -> None:
    logger.info(
        "Audit data sources: fatigue=%s, category_intel=%s, profitability=%s, "
        "brand_intel=%s, competitor_deep_dive=%s, impact_estimate=%s, "
        "concepts=%d, competitors=%d",
        "present" if data.get("fatigue_analysis") else "missing",
        "present" if data.get("category_intel") else "missing",
        "present" if data.get("profitability_summary") else "missing",
        "present" if data.get("brand_intel") else "missing",
        "present" if data.get("competitor_deep_dive") else "missing",
        "present" if data.get("impact_estimate") else "missing",
        len(data.get("sample_concepts", [])),
        len(data.get("competitors", [])),
    )
    doc = SimpleDocTemplate(
        str(out_path), pagesize=A4,
        leftMargin=_MARGIN, rightMargin=_MARGIN,
        topMargin=_MARGIN, bottomMargin=_MARGIN,
    )
    story = []
    story += _page_executive_diagnosis(data)
    story.append(PageBreak())
    story += _page_competitive_landscape(data)
    story.append(PageBreak())
    story += _page_competitor_war_room(data)
    story.append(PageBreak())
    story += _page_hook_swipe_file(data)
    story.append(PageBreak())
    story += _page_gap_analysis(data)
    story.append(PageBreak())
    story += _page_visual_patterns(data)
    story.append(PageBreak())
    story += _page_creative_strategy(data)
    story.append(PageBreak())
    story += _page_sample_briefs(data)
    story.append(PageBreak())
    story += _page_action_plan(data)
    doc.build(story)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — Executive Diagnosis
# ══════════════════════════════════════════════════════════════════════════════

def _page_executive_diagnosis(data: dict) -> list:
    brand        = data["brand"]
    ads          = data["client_ads"]
    fatigue_data = data.get("fatigue_analysis", {})
    impact_data  = data.get("impact_estimate", {})
    story        = []

    # Top accent bar
    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 8 * mm))

    # Title
    story.append(Paragraph(
        f"Creative Performance Diagnosis for {brand['name']}", S_TITLE))
    story.append(Paragraph(
        f"Prepared by Roger Krishna &bull; Creative Intelligence Service "
        f"&bull; {datetime.now().strftime('%d %B %Y')}", S_SUBTITLE))

    story.append(HRFlowable(width="100%", thickness=1, color=_LIGHT))
    story.append(Spacer(1, 6 * mm))

    # Verdict box — now includes ₹ waste figure
    total_waste = _get_total_monthly_waste(impact_data)
    verdict = _build_executive_verdict(fatigue_data, ads, total_waste)
    story.append(_verdict_box(verdict))
    story.append(Spacer(1, 6 * mm))

    # Four metric cards: Monthly Waste, Coverage Ratio, Fatigue Severity, Angle Diversity
    coverage = fatigue_data.get("creative_coverage", {})
    fi       = fatigue_data.get("fatigue_index", {})
    hd       = fatigue_data.get("hook_diversity", {})

    waste_str = _format_inr(total_waste) if total_waste else "\u2014"

    coverage_ratio = coverage.get("ratio")
    coverage_str = f"{coverage_ratio * 100:.0f}%" if coverage_ratio is not None else "\u2014"

    severity = fi.get("severity")
    severity_str = severity if severity else "\u2014"

    hd_score = hd.get("score")
    hd_str = f"{hd_score:.0f}/100" if hd_score is not None else "\u2014"

    metric_cards = _metric_row([
        (waste_str, "Monthly Creative Waste"),
        (coverage_str, "Coverage Ratio"),
        (severity_str, "Fatigue Severity"),
        (hd_str, "Angle Diversity"),
    ])
    story.append(metric_cards)
    story.append(Spacer(1, 4 * mm))

    # Format mix summary
    active_ads = [a for a in ads if a.get("is_active")]
    fmt_counts = Counter(a.get("creative_type", "unknown") for a in active_ads)
    fmt_str = " / ".join(f"{v} {k}" for k, v in fmt_counts.most_common())

    video_transcript_count = sum(
        1 for a in active_ads
        if a.get("creative_type") == "video" and a.get("transcript")
    )
    fmt_line = f"<b>Format mix:</b> {fmt_str or 'No ads found'}"
    if video_transcript_count:
        fmt_line += (f" &bull; {video_transcript_count} video "
                     f"ad{'s' if video_transcript_count != 1 else ''} "
                     f"with transcripts analyzed")
    story.append(Paragraph(fmt_line, _s("fmtline", fontSize=10, spaceAfter=6)))

    story.append(Spacer(1, 6 * mm))

    # Disclaimer
    story.append(Paragraph(
        "Metrics derived from Meta Ad Library data, competitor benchmarking, "
        "and creative diversity analysis. Not a guarantee of specific "
        "advertising outcomes.",
        S_DISCLAIMER))

    return story


def _get_total_monthly_waste(impact_data: dict) -> float:
    """Extract total estimated monthly waste from impact estimate data."""
    if not impact_data:
        return 0.0
    # Try direct field first
    total = impact_data.get("total_estimated_monthly_waste", 0)
    if total:
        return float(total)
    # Sum per_gap_impact entries
    per_gap = impact_data.get("per_gap_impact", [])
    if per_gap:
        return sum(float(g.get("estimated_monthly_impact_inr", 0)) for g in per_gap)
    return 0.0


def _build_executive_verdict(fatigue_data: dict, ads: list[dict],
                             total_waste: float = 0.0) -> str:
    """Build the verdict string dynamically from real data."""
    if not fatigue_data:
        return ("Run the full pipeline with competitor data to generate "
                "your creative performance diagnosis.")

    parts = []

    # Lead with money if available
    if total_waste > 0:
        parts.append(
            f"Your ad account is losing an estimated "
            f"{_format_inr(total_waste)}/month in creative waste")

    coverage = fatigue_data.get("creative_coverage", {})
    if coverage.get("ratio", 1) < 1:
        ratio_val = coverage.get("ratio", 1)
        deficit_pct = 100 - ratio_val * 100
        parts.append(
            f"with {coverage.get('client_count', '?')} active ads against "
            f"a benchmark of {coverage.get('benchmark', '?')}, you are "
            f"underfeeding Meta's algorithm by {deficit_pct:.0f}%")

    fi = fatigue_data.get("fatigue_index", {})
    if fi.get("severity") in ("HIGH", "CRITICAL"):
        avg_dur = fi.get("avg_duration", 0)
        parts.append(
            f"your average ad has been running {avg_dur:.0f} days "
            f"\u2014 {fi['severity'].lower()} fatigue detected")

    hd = fatigue_data.get("hook_diversity", {})
    if hd.get("score", 100) < 50:
        triggers_used = hd.get("triggers_used", [])
        parts.append(
            f"using only {len(triggers_used)}/10 psychological angles")

    if parts:
        return ("Your ad account shows critical creative fatigue \u2014 "
                + ", and ".join(parts) + ".")

    active_ads = [a for a in ads if a.get("is_active")]
    if not active_ads:
        return ("No active ads found. Your brand may not be running "
                "Meta ads currently.")

    return ("Your ad account looks healthy \u2014 keep iterating on "
            "fresh creatives.")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — Competitive Landscape Overview
# ══════════════════════════════════════════════════════════════════════════════

def _page_competitive_landscape(data: dict) -> list:
    deep_dive   = data.get("competitor_deep_dive", {})
    brand       = data["brand"]
    ads         = data["client_ads"]
    competitors = data.get("competitors", [])
    story       = []

    # Top accent bar
    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("Competitive Landscape Overview", S_H2))
    story.append(Spacer(1, 3 * mm))

    if not deep_dive and not competitors:
        story.append(Paragraph(
            "<i>Competitive landscape data not available. Run the full pipeline "
            "with competitor URLs to populate this section.</i>",
            S_SECTION_NOTE))
        return story

    # Landscape summary
    landscape_summary = deep_dive.get("competitive_landscape_summary", "")
    if landscape_summary:
        story.append(Paragraph(landscape_summary, S_BODY))
        story.append(Spacer(1, 4 * mm))

    # Build per-competitor table
    story.append(Paragraph("Competitor Comparison", S_H3))

    active_client_ads = [a for a in ads if a.get("is_active")]
    client_count = len(active_client_ads)

    # Collect competitor profiles
    comp_profiles = deep_dive.get("competitor_profiles", [])
    if not comp_profiles and competitors:
        # Fallback: build minimal rows from DB data
        comp_profiles = []
        for cd in competitors:
            comp_ads = cd.get("ads", [])
            active = [a for a in comp_ads if a.get("is_active")]
            profitable = [a for a in active if (a.get("duration_days") or 0) >= 21]
            win_rate = round(len(profitable) / len(active) * 100) if active else 0
            triggers = Counter(a.get("psychological_trigger") for a in active
                               if a.get("psychological_trigger"))
            dominant = triggers.most_common(1)[0][0] if triggers else "\u2014"
            comp_profiles.append({
                "name": cd["brand"]["name"],
                "active_ads": len(active),
                "win_rate": win_rate,
                "dominant_trigger": dominant.replace("_", " ").title(),
                "creative_velocity": "\u2014",
            })

    header = ["Brand", "Active Ads", "Win Rate", "Top Trigger", "Velocity"]
    rows = [header]

    # Client row first
    client_active = [a for a in ads if a.get("is_active")]
    client_profitable = [a for a in client_active if (a.get("duration_days") or 0) >= 21]
    client_wr = round(len(client_profitable) / len(client_active) * 100) if client_active else 0
    client_triggers = Counter(a.get("psychological_trigger") for a in client_active
                              if a.get("psychological_trigger"))
    client_dominant = (client_triggers.most_common(1)[0][0].replace("_", " ").title()
                       if client_triggers else "\u2014")
    rows.append([
        f"{brand['name']} (You)",
        str(len(client_active)),
        f"{client_wr}%",
        client_dominant,
        "\u2014",
    ])

    # Competitor rows
    all_active_counts = [len(client_active)]
    for cp in comp_profiles:
        active = cp.get("active_ads", 0)
        all_active_counts.append(active)
        wr = cp.get("win_rate", 0)
        trigger = cp.get("dominant_trigger", "\u2014")
        if isinstance(trigger, str) and "_" in trigger:
            trigger = trigger.replace("_", " ").title()
        velocity = cp.get("creative_velocity", "\u2014")
        rows.append([
            str(cp.get("name", "\u2014")),
            str(active),
            f"{wr}%" if isinstance(wr, (int, float)) else str(wr),
            str(trigger),
            str(velocity),
        ])

    # Determine highlight rows: client metrics below competitor average
    highlight_rows = []
    if len(comp_profiles) > 0:
        avg_active = sum(cp.get("active_ads", 0) for cp in comp_profiles) / len(comp_profiles)
        avg_wr = sum(cp.get("win_rate", 0) for cp in comp_profiles) / len(comp_profiles)
        # Row 1 is the client row
        if len(client_active) < avg_active or client_wr < avg_wr:
            highlight_rows.append(1)

    col_ws = [_CONTENT_W * 0.28, _CONTENT_W * 0.16, _CONTENT_W * 0.16,
              _CONTENT_W * 0.22, _CONTENT_W * 0.18]
    story.append(_data_table(rows, col_widths=col_ws, highlight_rows=highlight_rows,
                             highlight_color=_LIGHT_RED))
    story.append(Spacer(1, 4 * mm))

    # Client ranking
    sorted_counts = sorted(all_active_counts, reverse=True)
    rank = sorted_counts.index(len(client_active)) + 1
    total_brands = len(sorted_counts)
    story.append(Paragraph(
        f"<b>Your ranking:</b> #{rank} of {total_brands} in creative volume.",
        S_BODY))

    return story


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — Competitor War Room — Winner Dissections
# ══════════════════════════════════════════════════════════════════════════════

def _page_competitor_war_room(data: dict) -> list:
    deep_dive = data.get("competitor_deep_dive", {})
    story     = []

    # Top accent bar
    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("Competitor War Room \u2014 Winner Dissections", S_H2))
    story.append(Paragraph(
        "Top-performing competitor ads deconstructed", S_SUBTITLE))

    top_winners = deep_dive.get("top_winners", {})

    if not top_winners:
        story.append(Paragraph(
            "<i>Competitor winner data not available. Run the full pipeline "
            "with competitor URLs to populate this section.</i>",
            S_SECTION_NOTE))
        return story

    for comp_name, winners in top_winners.items():
        if not winners:
            continue
        story.append(Paragraph(comp_name, S_H3))

        for i, winner in enumerate(winners[:3], 1):
            hook_text = winner.get("hook_text", "") or winner.get("hook", "") or "\u2014"
            # Wrap to ~200 chars
            if len(hook_text) > 200:
                hook_text = hook_text[:197] + "..."

            duration = winner.get("duration_days", 0)
            trigger = winner.get("psychological_trigger", "") or winner.get("trigger", "")
            hook_structure = winner.get("hook_structure", "")
            why_works = winner.get("why_it_works", "")
            visual = winner.get("visual_layout", "") or winner.get("visual_direction", "")

            # Build card HTML
            parts = [f"<font size='8'><b>WINNER #{i}</b></font><br/>"]
            parts.append(f"<font size='11'><b>&ldquo;{hook_text}&rdquo;</b></font><br/>")

            badge_parts = []
            if duration:
                badge_parts.append(f"Running {duration} days")
            if trigger:
                badge_parts.append(trigger.replace("_", " ").title())
            if hook_structure:
                badge_parts.append(hook_structure.replace("_", " ").title())
            if badge_parts:
                parts.append(f"<font color='{_GREY.hexval()}'>"
                             f"{' &bull; '.join(badge_parts)}</font><br/>")

            if why_works:
                parts.append(f"<br/><b>Why it works:</b> {why_works}<br/>")
            if visual:
                vis_preview = visual[:150] + ("..." if len(visual) > 150 else "")
                parts.append(f"<font color='{_GREY.hexval()}'>"
                             f"<b>Visual:</b> {vis_preview}</font>")

            story.append(_callout_box("".join(parts)))
            story.append(Spacer(1, 2 * mm))

        story.append(Spacer(1, 3 * mm))

    return story


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — Hook Swipe File
# ══════════════════════════════════════════════════════════════════════════════

def _page_hook_swipe_file(data: dict) -> list:
    intel_data = data.get("category_intel", {})
    story      = []

    # Top accent bar
    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("Hook Swipe File \u2014 Competitor Hooks by Angle", S_H2))
    story.append(Paragraph(
        "Full hooks from profitable ads (21+ day run filter), sorted by duration",
        S_SUBTITLE))

    hook_database = intel_data.get("hook_database", {})

    if not hook_database:
        story.append(Paragraph(
            "<i>No hook data available. This section requires competitor ads "
            "with analyzed copy or video transcripts. Run the full pipeline "
            "with competitor URLs to populate.</i>", S_SECTION_NOTE))
        return story

    # Sort triggers by hook count descending
    sorted_triggers = sorted(
        hook_database.items(),
        key=lambda x: x[1].get("count", 0),
        reverse=True,
    )

    for trigger, tdata in sorted_triggers[:6]:
        pct_winners = tdata.get("pct_of_winners", 0)
        story.append(Paragraph(
            f"{trigger.replace('_', ' ').title()} "
            f"({pct_winners:.0f}% of winners)", S_H3))

        hooks = tdata.get("hooks", [])
        # Filter: exclude template vars and hooks < 5 chars
        hooks = [h for h in hooks
                 if h.get("text") and len(h["text"]) >= 5
                 and "$" not in h.get("text", "")
                 and "{{" not in h.get("text", "")]

        if hooks:
            # Sort by duration descending
            hooks = sorted(hooks, key=lambda h: h.get("duration_days", 0), reverse=True)

            hook_header = ["Hook Text", "Structure", "Source", "Days"]
            hook_rows = [hook_header]
            for hook in hooks[:5]:
                text = hook.get("text", "\u2014")
                # Full text with wrapping (up to 200 chars)
                if len(text) > 200:
                    text = text[:197] + "..."

                hs = hook.get("hook_structure", "\u2014")
                if isinstance(hs, str) and "_" in hs:
                    hs = hs.replace("_", " ").title()

                row = [
                    text,
                    str(hs),
                    str(hook.get("source_brand", "\u2014")),
                    str(hook.get("duration_days", "\u2014")),
                ]
                hook_rows.append(row)

                # Add spoken_hook row if available
                spoken = hook.get("spoken_hook")
                if spoken and len(spoken) >= 5:
                    hook_rows.append([
                        f"[Spoken] {spoken}",
                        "\u2014",
                        "",
                        "",
                    ])

            col_ws = [_CONTENT_W * 0.48, _CONTENT_W * 0.18,
                      _CONTENT_W * 0.18, _CONTENT_W * 0.16]
            story.append(_data_table(hook_rows, col_widths=col_ws))
        story.append(Spacer(1, 3 * mm))

    return story


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — Your Creative Gaps — With ₹ Impact
# ══════════════════════════════════════════════════════════════════════════════

def _build_gaps(data: dict) -> list[dict]:
    """Build a unified list of creative gaps from 3 sources."""
    intel_data   = data.get("category_intel", {})
    fatigue_data = data.get("fatigue_analysis", {})
    impact_data  = data.get("impact_estimate", {})
    brand        = data["brand"]
    ads          = data["client_ads"]
    gaps: list[dict] = []

    category = (brand.get("category") or "this category").title()

    # Build impact lookup: gap_key -> impact info
    impact_lookup = {}
    per_gap = impact_data.get("per_gap_impact", [])
    for gi in per_gap:
        key = gi.get("gap_type", "") + ":" + gi.get("gap_title", "")
        impact_lookup[key] = gi

    # ── Source 1: Trigger (angle) gaps ────────────────────────────────────
    trigger_analysis = intel_data.get("trigger_analysis", {})
    client_triggers  = set(
        fatigue_data.get("hook_diversity", {}).get("triggers_used", []))
    profitable_triggers = trigger_analysis.get("by_profitable_only", {})
    total_pt = sum(profitable_triggers.values()) or 1

    for trigger, count in profitable_triggers.items():
        if trigger not in client_triggers:
            pct = round(count / total_pt * 100, 1)
            if pct >= 5:
                title = f"Zero {trigger.replace('_', ' ').title()} Creatives"
                gap = {
                    "type": "ANGLE GAP",
                    "title": title,
                    "competitor_usage": f"{pct:.0f}% of profitable competitor ads",
                    "your_usage": "0%",
                    "impact": (f"Missing a proven conversion angle "
                               f"in {category}"),
                }
                # Try to find impact estimate
                _attach_impact(gap, impact_lookup)
                gaps.append(gap)

    # ── Source 2: Format gaps ─────────────────────────────────────────────
    format_analysis = intel_data.get("format_analysis", {})
    active_ads = [a for a in ads if a.get("is_active")]
    total_client = len(active_ads) or 1
    client_fmt_counts = Counter(
        a.get("creative_type", "unknown") for a in active_ads)

    for fmt, fdata in format_analysis.items():
        if fmt == "unknown":
            continue
        client_pct = round(client_fmt_counts.get(fmt, 0) / total_client * 100, 1)
        winner_pct = fdata.get("winner_pct", 0)
        if client_pct == 0 and winner_pct >= 10:
            title = f"No {fmt.title()} Ads"
            gap = {
                "type": "FORMAT GAP",
                "title": title,
                "competitor_usage": (
                    f"{fdata.get('total_pct', 0):.0f}% of competitor ads, "
                    f"{winner_pct:.0f}% of winners"),
                "your_usage": "0%",
                "impact": (f"{fmt.title()} format has "
                           f"{fdata.get('win_rate', 0):.0f}% win rate "
                           f"\u2014 high ROI potential"),
            }
            _attach_impact(gap, impact_lookup)
            gaps.append(gap)

    # ── Source 3: Hook structure gaps ─────────────────────────────────────
    hook_analysis = intel_data.get("hook_structure_analysis", {})
    client_hooks  = set(
        fatigue_data.get("hook_diversity", {}).get("hook_structures_used", []))
    profitable_hooks = hook_analysis.get("by_profitable_only", {})
    total_hc = sum(profitable_hooks.values()) or 1

    for hook, count in profitable_hooks.items():
        if hook not in client_hooks:
            pct = round(count / total_hc * 100, 1)
            if pct >= 10:
                hook_win_rate = hook_analysis.get(
                    "profitable_rate_by_hook", {}).get(hook, 0)
                title = (f"No '{hook.replace('_', ' ').title()}' Hooks")
                gap = {
                    "type": "HOOK STRUCTURE GAP",
                    "title": title,
                    "competitor_usage": (
                        f"{pct:.0f}% of profitable competitor hooks"),
                    "your_usage": "0%",
                    "impact": (f"This hook structure has "
                               f"{hook_win_rate:.0f}% win rate"),
                }
                _attach_impact(gap, impact_lookup)
                gaps.append(gap)

    # Sort by estimated impact (highest ₹ first), then by type priority
    type_order = {"ANGLE GAP": 0, "FORMAT GAP": 1, "HOOK STRUCTURE GAP": 2}
    gaps.sort(key=lambda g: (
        -g.get("estimated_monthly_impact", 0),
        type_order.get(g["type"], 9),
    ))
    return gaps


def _attach_impact(gap: dict, impact_lookup: dict) -> None:
    """Attach impact estimate data to a gap if available."""
    # Try exact match first, then fuzzy by title
    key = gap["type"] + ":" + gap["title"]
    if key in impact_lookup:
        gi = impact_lookup[key]
        gap["estimated_monthly_impact"] = float(gi.get("estimated_monthly_impact_inr", 0))
        gap["confidence"] = gi.get("confidence", "")
        return
    # Fuzzy: match by gap title keywords in any impact entry
    title_lower = gap["title"].lower()
    for ik, gi in impact_lookup.items():
        if gi.get("gap_title", "").lower() in title_lower or title_lower in ik.lower():
            gap["estimated_monthly_impact"] = float(gi.get("estimated_monthly_impact_inr", 0))
            gap["confidence"] = gi.get("confidence", "")
            return


def _page_gap_analysis(data: dict) -> list:
    story = []

    # Top accent bar
    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph(
        "Your Creative Gaps \u2014 With \u20b9 Impact", S_H2))
    story.append(Paragraph(
        "Where your competitors are monetizing and you are not",
        S_SUBTITLE))

    gaps = _build_gaps(data)

    if not gaps:
        story.append(Paragraph(
            "No significant creative gaps detected. Your creative strategy "
            "appears well-aligned with competitor patterns.",
            S_BODY))
        return story

    for gap in gaps[:6]:
        # Build structured callout content
        parts = [
            f"<font size='8'><b>{gap['type']}</b></font><br/>",
            f"<font size='12'><b>{gap['title']}</b></font><br/>",
            f"Competitor usage: {gap['competitor_usage']}<br/>",
            f"Your usage: {gap['your_usage']}<br/>",
        ]

        # ₹ impact line
        impact_amt = gap.get("estimated_monthly_impact", 0)
        if impact_amt > 0:
            parts.append(
                f"<font color='{_RED.hexval()}'><b>Estimated monthly cost of "
                f"this gap: {_format_inr(impact_amt)}</b></font><br/>")
            confidence = gap.get("confidence", "")
            if confidence:
                parts.append(
                    f"<font color='{_GREY.hexval()}'>"
                    f"Confidence: {confidence}</font><br/>")

        parts.append(
            f"<font color='{_GREY.hexval()}'><i>"
            f"{gap['impact']}</i></font>")
        story.append(_callout_box("".join(parts)))
        story.append(Spacer(1, 3 * mm))

    story.append(Spacer(1, 4 * mm))
    story.append(Paragraph(
        "Each gap represents a creative territory your competitors are "
        "actively monetizing. Closing these gaps is the highest-leverage "
        "action for your ad account's performance.",
        S_BODY))

    return story


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 6 — Visual Pattern Intelligence
# ══════════════════════════════════════════════════════════════════════════════

_PATTERN_LABELS = {
    "face_dominant_pct": "Face/person prominent",
    "text_overlay_pct": "Text overlay present",
    "minimal_aesthetic_pct": "Minimal/clean aesthetic",
    "before_after_pct": "Before/after format",
    "product_focused_pct": "Product-focused",
    "ugc_style_pct": "UGC/raw style",
}

_PATTERN_ACTIONS = {
    "face_dominant_pct": (
        "ACTION: Feature a real person (face visible) in every new creative. "
        "Prioritize close-ups and authentic expressions over stock imagery."),
    "text_overlay_pct": (
        "ACTION: Every new creative must include a text overlay with a problem "
        "statement or claim in \u22647 words."),
    "minimal_aesthetic_pct": (
        "ACTION: Use clean backgrounds with single focal points. "
        "Avoid cluttered layouts with multiple competing elements."),
    "before_after_pct": (
        "ACTION: Create before/after content showing visible transformation. "
        "Use split-screen or swipe formats."),
    "product_focused_pct": (
        "ACTION: Lead with clear product shots. Show the actual product "
        "prominently, not just lifestyle context."),
    "ugc_style_pct": (
        "ACTION: Shoot with phone-quality aesthetics. Raw, unpolished "
        "content outperforms studio-grade production in this category."),
}


def _page_visual_patterns(data: dict) -> list:
    intel_data = data.get("category_intel", {})
    story      = []

    # Top accent bar
    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("Visual Pattern Intelligence", S_H2))

    visual_stats = intel_data.get("visual_pattern_stats", {})
    total_analyzed = visual_stats.get("total_analyzed", 0)

    story.append(Paragraph(
        f"Visual patterns from {total_analyzed} profitable competitor ads"
        if total_analyzed > 0 else
        "Visual patterns from competitor ad analysis",
        S_SUBTITLE))

    if not visual_stats or total_analyzed == 0:
        story.append(Paragraph(
            "<i>Visual pattern analysis requires LLM-analyzed competitor "
            "ads. Run the full pipeline to populate.</i>", S_SECTION_NOTE))
        return story

    # Build pattern rows, sorted by pct descending, only pct > 0
    pattern_items = []
    for key, label in _PATTERN_LABELS.items():
        pct = visual_stats.get(key, 0)
        if pct > 0:
            pattern_items.append((key, label, pct))

    pattern_items.sort(key=lambda x: x[2], reverse=True)

    if pattern_items:
        # Table with pattern + pct + action
        vp_header = ["Visual Pattern", "% of Winners", "Action"]
        vp_rows = [vp_header]
        for key, label, pct in pattern_items:
            action = _PATTERN_ACTIONS.get(key, "")
            # Shorten action for table cell
            action_short = action.replace("ACTION: ", "") if action else ""
            vp_rows.append([label, f"{pct:.0f}%", action_short])

        story.append(_data_table(
            vp_rows,
            col_widths=[_CONTENT_W * 0.22, _CONTENT_W * 0.13, _CONTENT_W * 0.65],
        ))
        story.append(Spacer(1, 5 * mm))

        # Visual Checklist
        story.append(Paragraph("Visual Checklist for Creative Team", S_H3))
        for key, label, pct in pattern_items:
            if pct >= 20:
                story.append(Paragraph(
                    f"<font color='{_GREEN.hexval()}'>\u2713</font> "
                    f"{label} ({pct:.0f}% of winners) \u2014 include in every brief",
                    S_CHECK_G))
            else:
                story.append(Paragraph(
                    f"<font color='{_GREY.hexval()}'>\u25CB</font> "
                    f"{label} ({pct:.0f}% of winners) \u2014 test selectively",
                    _s(f"vcheck_{key}", fontSize=9, textColor=_GREY, leading=12)))
    else:
        story.append(Paragraph(
            "<i>No visual patterns detected above 0% threshold.</i>",
            S_SECTION_NOTE))

    return story


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 7 — Creative Strategy Blueprint
# ══════════════════════════════════════════════════════════════════════════════

_TOF_TRIGGERS  = {"transformation", "curiosity", "fear",
                  "agitation_solution", "aspiration"}
_MOF_TRIGGERS  = {"social_proof", "authority", "belonging"}
_BOF_TRIGGERS  = {"urgency", "status"}


def _page_creative_strategy(data: dict) -> list:
    intel_data   = data.get("category_intel", {})
    fatigue_data = data.get("fatigue_analysis", {})
    brand_intel  = data.get("brand_intel", {})
    brand        = data["brand"]
    story        = []

    # Top accent bar
    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph(
        "Recommended Creative Output \u2014 Next 14 Days", S_H2))
    story.append(Spacer(1, 3 * mm))

    # ── Section A: Creative Matrix ────────────────────────────────────────
    story.append(Paragraph("A. Creative Matrix", S_H3))

    gaps = _build_gaps(data)
    format_analysis = intel_data.get("format_analysis", {})

    # Find best-performing format (highest win_rate among non-unknown)
    best_format = "video"
    best_wr = 0
    for fmt, fdata in format_analysis.items():
        if fmt == "unknown":
            continue
        wr = fdata.get("win_rate", 0)
        if wr > best_wr:
            best_wr = wr
            best_format = fmt

    # Get brand products for specificity
    products = brand_intel.get("products_detected", [])

    matrix_rows: list[list[str]] = []

    # From angle gaps — use product names for specificity
    product_idx = 0
    for gap in gaps:
        if gap["type"] == "ANGLE GAP":
            trigger = (gap["title"]
                       .replace("Zero ", "")
                       .replace(" Creatives", "")
                       .strip())
            trigger_lower = trigger.lower().replace(" ", "_")
            if trigger_lower in _TOF_TRIGGERS:
                stage = "TOF"
            elif trigger_lower in _MOF_TRIGGERS:
                stage = "MOF"
            else:
                stage = "BOF"

            # Add product specificity if available
            label = trigger
            if products and product_idx < len(products):
                product_name = products[product_idx]
                if isinstance(product_name, dict):
                    product_name = product_name.get("name", "")
                if product_name:
                    label = f"{trigger} featuring {product_name}"
                product_idx += 1

            matrix_rows.append([
                label, best_format.title(), stage, "3\u20135"])

    # If no angle gaps, use top 3 competitor triggers instead
    if not matrix_rows:
        trigger_analysis = intel_data.get("trigger_analysis", {})
        by_profitable = trigger_analysis.get("by_profitable_only", {})
        product_idx = 0
        for trigger, _count in sorted(
            by_profitable.items(), key=lambda x: x[1], reverse=True
        )[:3]:
            label = trigger.replace("_", " ").title()
            if trigger in _TOF_TRIGGERS:
                stage = "TOF"
            elif trigger in _MOF_TRIGGERS:
                stage = "MOF"
            else:
                stage = "BOF"

            if products and product_idx < len(products):
                product_name = products[product_idx]
                if isinstance(product_name, dict):
                    product_name = product_name.get("name", "")
                if product_name:
                    label = f"{label} featuring {product_name}"
                product_idx += 1

            matrix_rows.append([
                label, best_format.title(), stage, "3\u20135"])

    if matrix_rows:
        header = ["Angle", "Format", "Funnel Stage", "Recommended Count"]
        story.append(_data_table(
            [header] + matrix_rows,
            col_widths=[_CONTENT_W * 0.38, _CONTENT_W * 0.18,
                        _CONTENT_W * 0.18, _CONTENT_W * 0.26],
        ))
    else:
        story.append(Paragraph(
            "<i>Run the full pipeline with competitor data to generate "
            "a creative matrix.</i>", S_SECTION_NOTE))

    story.append(Spacer(1, 5 * mm))

    # ── Section B: 14-Day Calendar ────────────────────────────────────────
    story.append(Paragraph("B. 14-Day Execution Calendar", S_H3))

    n_angles = len(matrix_rows) or 3
    n_creatives = n_angles * 4  # midpoint of 3-5 per angle
    gap_count = len([g for g in gaps if g["type"] == "ANGLE GAP"])

    # Build specific priority list from gaps
    gap_priorities = []
    for g in gaps[:3]:
        gap_priorities.append(g["title"].lower())
    priority_text = (", ".join(gap_priorities) if gap_priorities
                     else "top-performing competitor angles")

    calendar_items = [
        (
            "Week 1",
            f"Launch {n_creatives} new creatives across "
            f"{n_angles} angles. Priority: {priority_text}."
        ),
        (
            "Week 2",
            "Review performance. Kill bottom 50% by CTR. "
            "Scale top performers."
        ),
        (
            "Week 3",
            "Replace killed creatives with next batch. "
            "Introduce remaining gap angles."
        ),
    ]
    for week, desc in calendar_items:
        story.append(Paragraph(f"<b>{week}:</b> {desc}", S_BODY))

    story.append(Spacer(1, 5 * mm))

    # ── Section C: Metric Targets ─────────────────────────────────────────
    story.append(Paragraph("C. Metric Targets", S_H3))

    category = (brand.get("category") or "D2C").title()
    target_rows = [
        ["Metric", "Target", "Benchmark Source"],
        ["CTR", "> 1.5%", f"Indian D2C {category} benchmark"],
        ["Hook Retention (video)", "> 30%",
         "Meta Ads Manager 3-second retention"],
        ["Creative Refresh Cycle", "Every 7\u201314 days",
         "Andromeda algorithm decay window"],
    ]
    story.append(_data_table(
        target_rows,
        col_widths=[_CONTENT_W * 0.30, _CONTENT_W * 0.22, _CONTENT_W * 0.48],
    ))
    story.append(Spacer(1, 2 * mm))
    story.append(Paragraph(
        f"Category benchmarks for Indian D2C {category}.",
        S_SMALL))

    return story


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 8 — Sample Creative Briefs
# ══════════════════════════════════════════════════════════════════════════════

def _page_sample_briefs(data: dict) -> list:
    concepts = data.get("sample_concepts", [])
    brand    = data["brand"]
    story    = []

    # Top accent bar
    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("Sample Creative Briefs", S_H2))
    story.append(Paragraph(
        "Production-ready briefs derived from competitor intelligence",
        S_SUBTITLE))

    if not concepts:
        story.append(Paragraph(
            "<i>Creative briefs are generated during the full pipeline "
            "run. Run the sprint mode to see 50+ production-ready briefs.</i>",
            S_SECTION_NOTE))
        story.append(Spacer(1, 8 * mm))
        story.append(_cta_banner(brand["name"]))
        return story

    for i, concept in enumerate(concepts[:5], 1):
        story += _brief_card(i, concept)

    story.append(Spacer(1, 8 * mm))
    story.append(_cta_banner(brand["name"]))

    return story


def _brief_card(index: int, concept: dict) -> list:
    """Render one creative brief card with full details."""
    elements: list = []

    hook = concept.get("hook_text") or "\u2014"
    angle = concept.get("psychological_angle") or ""
    fmt = concept.get("format") or concept.get("creative_type") or ""

    # Hook text (prominent)
    elements.append(Paragraph(
        f"<font color='{_TEAL.hexval()}'>{index}.</font>&nbsp; "
        f"&ldquo;{hook}&rdquo;",
        S_HOOK))

    # Angle + Format badges
    badge_parts = []
    if angle:
        badge_parts.append(angle.replace("_", " ").title())
    if fmt:
        badge_parts.append(fmt.replace("_", " ").title())
    prod_diff = concept.get("production_difficulty", "")
    if prod_diff:
        badge_parts.append(f"Difficulty: {prod_diff}")
    if badge_parts:
        elements.append(Paragraph(
            " &bull; ".join(badge_parts), S_HOOK_M))

    # Text overlay
    text_overlay = concept.get("text_overlay", "")
    if text_overlay:
        elements.append(Paragraph(
            f"<b>Text overlay:</b> &ldquo;{text_overlay}&rdquo;",
            _s(f"bto_{index}", fontSize=9, leftIndent=4 * mm,
               spaceAfter=2, leading=12)))

    # Visual direction summary
    vis = concept.get("visual_direction") or ""
    if isinstance(vis, dict):
        vis_parts = []
        for vk in ("scene_description", "talent_direction", "product_placement"):
            vv = vis.get(vk, "")
            if vv:
                vis_parts.append(str(vv))
        vis = ". ".join(vis_parts)
    if vis:
        vis_preview = vis[:200] + ("..." if len(vis) > 200 else "")
        elements.append(Paragraph(
            f"<b>Visual:</b> {vis_preview}",
            _s(f"bvis_{index}", fontSize=9, textColor=_GREY,
               leftIndent=4 * mm, spaceAfter=2, leading=12)))

    # Competitor inspiration
    comp_ref = concept.get("competitor_inspiration", "")
    if not comp_ref:
        comp_ref = concept.get("competitor_reference", "")
    if comp_ref:
        elements.append(Paragraph(
            f"<b>Inspired by:</b> {comp_ref}",
            _s(f"binsp_{index}", fontSize=9, textColor=_GREY,
               leftIndent=4 * mm, spaceAfter=2, leading=12)))

    # "Why this works" with data backing
    data_backing = concept.get("data_backing", "")
    body = concept.get("body_script") or ""
    # Extract data backing from body_script if stored there
    if not data_backing and "[DATA BACKING]" in body:
        db_idx = body.index("[DATA BACKING]")
        data_backing = body[db_idx + len("[DATA BACKING]"):].strip()

    if data_backing:
        elements.append(Paragraph(
            f"<font color='{_TEAL.hexval()}'><b>Why this works:</b></font> "
            f"{data_backing}",
            _s(f"bwhy_{index}", fontSize=9, leading=12,
               leftIndent=4 * mm, spaceAfter=4)))

    elements.append(Spacer(1, 3 * mm))
    return elements


def _cta_banner(brand_name: str) -> Table:
    """CTA banner for the paid sprint — updated messaging."""
    lines = [
        Paragraph(
            "This audit includes 5 sample briefs. The full Creative Sprint "
            "includes 50+ production-ready briefs with complete visual direction, "
            "sound design, A/B test plans, and card-by-card carousel breakdowns.",
            _s("cta1", fontSize=11, textColor=_NAVY, alignment=TA_CENTER,
               fontName="Helvetica-Bold", spaceAfter=4)),
        Paragraph(
            "Want the full package? Reply to this message.",
            _s("cta3", fontSize=13, textColor=_TEAL, alignment=TA_CENTER,
               fontName="Helvetica-Bold")),
    ]
    inner = Table([[line] for line in lines],
                  colWidths=[_CONTENT_W - 12 * mm])
    inner.setStyle(TableStyle([
        ("TOPPADDING",    (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))

    t = Table([[inner]], colWidths=[_CONTENT_W])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), _LIGHT),
        ("TOPPADDING",    (0, 0), (-1, -1), 14),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 14),
        ("LEFTPADDING",   (0, 0), (-1, -1), 10),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 10),
        ("BOX",           (0, 0), (-1, -1), 2, _TEAL),
        ("ROUNDEDCORNERS", [6, 6, 6, 6]),
    ]))
    return t


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 9 — Priority Action Plan + ROI
# ══════════════════════════════════════════════════════════════════════════════

_PRIORITY_COLORS = {
    "high":   _RED,
    "medium": _ORANGE,
    "low":    _GREY,
}


def _page_action_plan(data: dict) -> list:
    waste_report = data.get("waste_report", {})
    impact_data  = data.get("impact_estimate", {})
    story        = []

    # Top accent bar
    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("Priority Action Plan + ROI", S_H2))
    story.append(Spacer(1, 3 * mm))

    # ── Section A: Top Actions ────────────────────────────────────────────
    story.append(Paragraph("A. Top Actions", S_H3))

    actions: list[dict] = []

    # From waste report recommendations
    recs_raw = waste_report.get("recommendations_json") or "[]"
    try:
        recs = json.loads(recs_raw) if isinstance(recs_raw, str) else recs_raw
    except (json.JSONDecodeError, TypeError):
        recs = []
    for rec in recs[:3]:
        actions.append({
            "priority": rec.get("priority", "medium"),
            "action":   rec.get("action", ""),
            "signal":   rec.get("signal", ""),
        })

    # From gap analysis (top 2)
    gaps = _build_gaps(data)
    for gap in gaps[:2]:
        actions.append({
            "priority": "high",
            "action": (
                f"Create {gap['title'].lower()} to close the "
                f"{gap['type'].lower()}: {gap.get('impact', '')}"),
            "signal": (
                f"{gap.get('competitor_usage', '')} vs your "
                f"{gap.get('your_usage', '')}"),
        })

    if actions:
        for i, action in enumerate(actions[:5], 1):
            priority = action.get("priority", "medium")
            color = _PRIORITY_COLORS.get(priority, _GREY)
            badge = (f"<font color='{color.hexval()}'>"
                     f"<b>[{priority.upper()}]</b></font>")

            story.append(Paragraph(
                f"{badge}&nbsp; {action['action']}",
                _s(f"act_{i}", fontSize=10, leading=14, spaceAfter=1)))

            if action.get("signal"):
                story.append(Paragraph(
                    f"Signal: {action['signal']}",
                    _s(f"actsig_{i}", fontSize=8, textColor=_GREY,
                       leading=10, leftIndent=8 * mm, spaceAfter=4)))
    else:
        story.append(Paragraph(
            "<i>Run the full pipeline to generate prioritized actions "
            "based on your ad account data.</i>", S_SECTION_NOTE))

    story.append(Spacer(1, 6 * mm))

    # ── Section B: ROI Projection ─────────────────────────────────────────
    if impact_data:
        story.append(Paragraph("B. ROI Projection", S_H3))

        total_waste = _get_total_monthly_waste(impact_data)
        sprint_roi = impact_data.get("sprint_roi", {})
        sprint_price = sprint_roi.get("sprint_price", 0)
        estimated_savings = sprint_roi.get("estimated_monthly_savings", total_waste)
        payback_days = sprint_roi.get("payback_days", 0)

        # Calculate payback if not provided
        if not payback_days and sprint_price and estimated_savings:
            payback_days = round(sprint_price / (estimated_savings / 30))

        roi_parts = []
        if sprint_price:
            roi_parts.append(
                f"<b>Investment:</b> {_format_inr(sprint_price)} for full Creative Sprint")
        if estimated_savings:
            roi_parts.append(
                f"<b>Estimated monthly savings:</b> {_format_inr(estimated_savings)}")
        if payback_days:
            roi_parts.append(
                f"<b>Payback period:</b> {payback_days} days")

        if roi_parts:
            roi_html = "<br/>".join(roi_parts)
            story.append(_callout_box(roi_html))
        elif total_waste > 0:
            story.append(Paragraph(
                f"<b>Your estimated creative waste:</b> "
                f"{_format_inr(total_waste)}/month. A structured creative "
                f"sprint can recover a significant portion of this.",
                S_BODY))

        story.append(Spacer(1, 6 * mm))

    # ── Section C: What This Audit Didn't Cover ──────────────────────────
    section_label = "C" if impact_data else "B"
    story.append(Paragraph(
        f"{section_label}. What This Audit Didn't Cover", S_H3))

    not_covered = [
        "50+ production-ready creative briefs with visual direction, "
        "sound design, and A/B test plans (included in full Creative Sprint)",
        "Weekly competitor tracking and creative refresh alerts",
        ("Performance feedback loop \u2014 which hooks actually convert "
         "for YOUR brand"),
        "Card-by-card carousel breakdowns and complete production specs",
    ]
    for item in not_covered:
        story.append(Paragraph(
            f"&bull;&nbsp; {item}",
            _s(f"nc_{id(item)}", fontSize=10, leading=14,
               leftIndent=4 * mm, spaceAfter=3)))

    # Footer
    story.append(Spacer(1, 8 * mm))
    story.append(HRFlowable(width="100%", thickness=1, color=_LIGHT))
    story.append(Paragraph(
        "Creative Intelligence Service &bull; Confidential &bull; "
        f"{datetime.now().strftime('%B %Y')}",
        S_FOOTER))

    return story


# ══════════════════════════════════════════════════════════════════════════════
# Shared helpers
# ══════════════════════════════════════════════════════════════════════════════

def _badge(text: str, bg_color=None, text_color=None) -> Paragraph:
    """Inline colored badge as a Paragraph."""
    tc = text_color or _WHITE
    bg = bg_color or _TEAL
    return Paragraph(
        f"<font color='{tc.hexval() if hasattr(tc, 'hexval') else tc}'>"
        f"<b>{text}</b></font>",
        _s(f"badge_{id(text)}", fontSize=8, backColor=bg,
           leftIndent=2 * mm, rightIndent=2 * mm, spaceAfter=2))


def _verdict_box(text: str) -> Table:
    """Coloured banner with the one-line verdict."""
    p = Paragraph(text, S_VERDICT)
    t = Table([[p]], colWidths=[_CONTENT_W])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), _NAVY),
        ("TOPPADDING",    (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("LEFTPADDING",   (0, 0), (-1, -1), 12),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 12),
        ("ROUNDEDCORNERS", [4, 4, 4, 4]),
    ]))
    return t


def _metric_row(items: list[tuple[str, str]]) -> Table:
    """Row of metric cards: big number on top, label underneath."""
    col_w = _CONTENT_W / len(items)
    top = []
    bot = []
    for value, label in items:
        top.append(Paragraph(value, S_METRIC))
        bot.append(Paragraph(label, S_METRIC_L))
    t = Table([top, bot], colWidths=[col_w] * len(items))
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), _LIGHT),
        ("TOPPADDING",    (0, 0), (-1, 0), 10),
        ("BOTTOMPADDING", (0, 1), (-1, 1), 8),
        ("ALIGN",    (0, 0), (-1, -1), "CENTER"),
        ("VALIGN",   (0, 0), (-1, -1), "MIDDLE"),
        ("LINEAFTER", (0, 0), (-2, -1), 1, _WHITE),
        ("ROUNDEDCORNERS", [4, 4, 4, 4]),
    ]))
    return t


def _data_table(
    rows: list[list],
    col_widths: list[float] | None = None,
    highlight_rows: list[int] | None = None,
    highlight_color=None,
) -> Table:
    """Standard data table with navy header, alternating row backgrounds."""
    if col_widths is None:
        n_cols = len(rows[0]) if rows else 1
        col_widths = [_CONTENT_W / n_cols] * n_cols

    # Wrap cell strings in Paragraphs for proper text wrapping
    wrapped_rows = []
    for i, row in enumerate(rows):
        wrapped = []
        for cell in row:
            if isinstance(cell, str):
                if i == 0:
                    wrapped.append(Paragraph(
                        cell, _s(f"th_{id(cell)}", fontSize=9,
                                 fontName="Helvetica-Bold",
                                 textColor=_WHITE)))
                else:
                    wrapped.append(Paragraph(
                        cell, _s(f"td_{id(cell)}", fontSize=9)))
            else:
                wrapped.append(cell)
        wrapped_rows.append(wrapped)

    t = Table(wrapped_rows, colWidths=col_widths)
    style_cmds = [
        ("BACKGROUND",  (0, 0), (-1, 0), _NAVY),
        ("TEXTCOLOR",   (0, 0), (-1, 0), _WHITE),
        ("FONTNAME",    (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, -1), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [_WHITE, _LIGHT]),
        ("GRID",        (0, 0), (-1, -1), 0.5, colors.Color(0.85, 0.85, 0.85)),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
    ]

    # Apply row highlighting (e.g., for red/teal flagging)
    if highlight_rows:
        hl_color = highlight_color or _LIGHT_RED
        for row_idx in highlight_rows:
            if row_idx < len(rows):
                style_cmds.append(
                    ("BACKGROUND", (0, row_idx), (-1, row_idx), hl_color))

    t.setStyle(TableStyle(style_cmds))
    return t


def _critical_callout(text: str) -> Table:
    """Red-bordered critical warning callout."""
    p = Paragraph(f"<b>{text}</b>", _s("crit_callout", fontSize=10,
                                        textColor=_RED, leading=13))
    t = Table([[p]], colWidths=[_CONTENT_W - 4 * mm])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), _LIGHT_RED),
        ("LINEBEFORECOL", (0, 0), (0, -1), 3, _RED),
        ("TOPPADDING",    (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("LEFTPADDING",   (0, 0), (-1, -1), 10),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 10),
    ]))
    return t


def _callout_box(html_text: str) -> Table:
    """Teal-bordered callout box."""
    p = Paragraph(html_text, S_CALLOUT)
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


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Generate the Creative Intelligence Audit PDF for a brand.")
    parser.add_argument("--brand", required=True, help="Brand name (must exist in DB)")
    parser.add_argument("--output", default="audits", help="Output directory (default: audits/)")
    args = parser.parse_args()

    path = run(args.brand, output_dir=args.output)
    print(f"Audit PDF generated: {path}")
