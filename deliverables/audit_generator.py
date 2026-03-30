"""
deliverables/audit_generator.py — Generate the 8-10 page Creative Intelligence Audit PDF.

A comprehensive, intelligence-grade audit that makes founders uncomfortable
about their creative spend. Every number comes from real data.

PAGE 1: Executive Diagnosis (verdict + 4 metric cards)
PAGE 2: Ad Account Health Metrics (coverage, fatigue, format gaps, hook diversity)
PAGE 3: Competitor Winning Model (trigger + format + patterns)
PAGE 4: Hook Intelligence (real hooks from profitable competitor ads)
PAGE 5: Visual Pattern Analysis (what winning ads look like)
PAGE 6: Gap Analysis (angle, format, hook structure gaps)
PAGE 7: Creative Strategy Blueprint (matrix, calendar, targets)
PAGE 8: Data-Backed Concepts (sample hooks with "why this works")
PAGE 9: Priority Action Plan + CTA

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

    fatigue_path = PROC_DIR / f"{slug}_fatigue.json"
    fatigue_data = {}
    if fatigue_path.exists():
        try:
            fatigue_data = json.loads(fatigue_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            logger.warning("Could not load fatigue data from %s", fatigue_path)

    intel_path = PROC_DIR / f"{slug}_category_intelligence.json"
    intel_data = {}
    if intel_path.exists():
        try:
            intel_data = json.loads(intel_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            logger.warning("Could not load category intel from %s", intel_path)

    profit_path = PROC_DIR / f"{slug}_profitable_ads_summary.json"
    profit_data = {}
    if profit_path.exists():
        try:
            profit_data = json.loads(profit_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            logger.warning("Could not load profitability data from %s", profit_path)

    return {
        "brand": brand,
        "client_ads": client_ads,
        "waste_report": waste_report,
        "competitors": comp_data,
        "sample_concepts": concepts,
        "fatigue_analysis": fatigue_data,
        "category_intel": intel_data,
        "profitability_summary": profit_data,
    }


# ── PDF builder ───────────────────────────────────────────────────────────────

def _build_pdf(data: dict, out_path: Path) -> None:
    logger.info(
        "Audit data sources: fatigue_analysis=%s, category_intel=%s, "
        "profitability=%s, concepts=%d, competitors=%d",
        "present" if data.get("fatigue_analysis") else "missing",
        "present" if data.get("category_intel") else "missing",
        "present" if data.get("profitability_summary") else "missing",
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
    story += _page_account_health(data)
    story.append(PageBreak())
    story += _page_competitor_winning_model(data)
    story.append(PageBreak())
    story += _page_hook_intelligence(data)
    story.append(PageBreak())
    story += _page_visual_patterns(data)
    story.append(PageBreak())
    story += _page_gap_analysis(data)
    story.append(PageBreak())
    story += _page_creative_strategy(data)
    story.append(PageBreak())
    story += _page_concepts(data)
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

    # Verdict box
    verdict = _build_executive_verdict(fatigue_data, ads)
    story.append(_verdict_box(verdict))
    story.append(Spacer(1, 6 * mm))

    # Four metric cards
    coverage = fatigue_data.get("creative_coverage", {})
    fi       = fatigue_data.get("fatigue_index", {})
    hd       = fatigue_data.get("hook_diversity", {})

    coverage_ratio = coverage.get("ratio")
    coverage_str = f"{coverage_ratio * 100:.0f}%" if coverage_ratio is not None else "\u2014"

    severity = fi.get("severity")
    severity_str = severity if severity else "\u2014"

    hd_score = hd.get("score")
    hd_str = f"{hd_score:.0f}/100" if hd_score is not None else "\u2014"

    active_ads = [a for a in ads if a.get("is_active")]
    fatigued = [a for a in active_ads
                if (a.get("duration_days") or 0) >= FATIGUE_AD_MIN_DAYS]
    fatigued_str = str(len(fatigued))

    metric_cards = _metric_row([
        (coverage_str, "Coverage Ratio"),
        (severity_str, "Fatigue Severity"),
        (hd_str, "Angle Diversity"),
        (fatigued_str, "Fatigued Ads"),
    ])
    story.append(metric_cards)
    story.append(Spacer(1, 4 * mm))

    # Format mix summary
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


def _build_executive_verdict(fatigue_data: dict, ads: list[dict]) -> str:
    """Build the verdict string dynamically from real data."""
    if not fatigue_data:
        return ("Run the full pipeline with competitor data to generate "
                "your creative performance diagnosis.")

    parts = []

    coverage = fatigue_data.get("creative_coverage", {})
    if coverage.get("ratio", 1) < 1:
        ratio_val = coverage.get("ratio", 1)
        deficit_pct = 100 - ratio_val * 100
        parts.append(
            f"With {coverage.get('client_count', '?')} active ads against "
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
# PAGE 2 — Ad Account Health Metrics
# ══════════════════════════════════════════════════════════════════════════════

def _page_account_health(data: dict) -> list:
    fatigue_data = data.get("fatigue_analysis", {})
    intel_data   = data.get("category_intel", {})
    ads          = data["client_ads"]
    story        = []

    # Top accent bar
    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 4 * mm))
    story.append(Paragraph("Ad Account Health Metrics", S_H2))
    story.append(Spacer(1, 3 * mm))

    # ── Section A: Creative Coverage Ratio ───────────────────────────────
    story.append(Paragraph("A. Creative Coverage Ratio", S_H3))

    coverage = fatigue_data.get("creative_coverage", {})
    if coverage:
        cov_rows = [
            ["Metric", "Value"],
            ["Your Active Ads", str(coverage.get("client_count", "\u2014"))],
            ["Category Benchmark", str(coverage.get("benchmark", "\u2014"))],
        ]
        story.append(_data_table(cov_rows, col_widths=[_CONTENT_W * 0.5, _CONTENT_W * 0.5]))
        story.append(Spacer(1, 2 * mm))

        interp = coverage.get("interpretation", "")
        if interp:
            story.append(Paragraph(interp, S_BODY))

        if coverage.get("ratio", 1) < 0.5:
            story.append(_critical_callout(
                "CRITICAL: Your creative volume is less than half "
                "the category minimum."))
    else:
        story.append(Paragraph(
            "<i>Creative coverage data not available. Run the fatigue "
            "scorer to populate.</i>", S_SECTION_NOTE))

    story.append(Spacer(1, 4 * mm))

    # ── Section B: Creative Fatigue Index ────────────────────────────────
    story.append(Paragraph("B. Creative Fatigue Index", S_H3))

    fi = fatigue_data.get("fatigue_index", {})
    if fi:
        fi_rows = [
            ["Metric", "Value"],
            ["Average Ad Duration", f"{fi.get('avg_duration', 0):.0f} days"],
            ["Optimal Refresh Window", f"{fi.get('benchmark', 0)} days"],
            ["Fatigue Index", f"{fi.get('index', 0):.1f}x"],
            ["Severity", fi.get("severity", "\u2014")],
        ]
        story.append(_data_table(fi_rows, col_widths=[_CONTENT_W * 0.5, _CONTENT_W * 0.5]))
        story.append(Spacer(1, 2 * mm))

        # List critical (fatigued) ads
        critical_ads = fatigue_data.get("critical_ads", [])
        if critical_ads:
            story.append(Paragraph(
                f"<b>Fatigued ads ({FATIGUE_AD_MIN_DAYS}+ days):</b>", S_BODY))
            fat_header = ["Ad ID", "Days Running", "Format"]
            fat_rows = [fat_header]
            for ad in critical_ads[:5]:
                fat_rows.append([
                    str(ad.get("ad_library_id", "\u2014")),
                    str(ad.get("duration_days", "\u2014")),
                    str(ad.get("creative_type", "\u2014")),
                ])
            remaining = len(critical_ads) - 5
            story.append(_data_table(
                fat_rows,
                col_widths=[_CONTENT_W * 0.4, _CONTENT_W * 0.3, _CONTENT_W * 0.3],
            ))
            if remaining > 0:
                story.append(Paragraph(
                    f"<i>...and {remaining} more</i>", S_SMALL))
    else:
        story.append(Paragraph(
            "<i>Fatigue index data not available. Run the fatigue "
            "scorer to populate.</i>", S_SECTION_NOTE))

    story.append(Spacer(1, 4 * mm))

    # ── Section C: Format Distribution Gap ───────────────────────────────
    story.append(Paragraph("C. Format Distribution Gap", S_H3))

    format_analysis = intel_data.get("format_analysis", {})
    active_ads = [a for a in ads if a.get("is_active")]

    if format_analysis and active_ads:
        # Compute client format percentages
        total_client = len(active_ads) or 1
        client_fmt = Counter(a.get("creative_type", "unknown") for a in active_ads)

        fmt_header = ["Format", "Your %", "Competitor Avg %"]
        fmt_rows = [fmt_header]

        all_formats = set(client_fmt.keys()) | set(format_analysis.keys())
        # Exclude 'unknown'
        all_formats.discard("unknown")

        highlight_rows = []
        row_idx = 1
        for fmt in sorted(all_formats):
            client_pct = round(client_fmt.get(fmt, 0) / total_client * 100, 1)
            comp_data = format_analysis.get(fmt, {})
            comp_pct = comp_data.get("total_pct", 0)
            fmt_rows.append([
                fmt.title(),
                f"{client_pct:.0f}%",
                f"{comp_pct:.0f}%",
            ])
            # Flag: client has 0% but competitors have >10%
            if client_pct == 0 and comp_pct > 10:
                highlight_rows.append(row_idx)
            row_idx += 1

        col_ws = [_CONTENT_W * 0.34, _CONTENT_W * 0.33, _CONTENT_W * 0.33]
        tbl = _data_table(fmt_rows, col_widths=col_ws, highlight_rows=highlight_rows)
        story.append(tbl)
    elif not format_analysis:
        story.append(Paragraph(
            "<i>Competitor format data not available. Run the full "
            "pipeline with competitor URLs to populate.</i>", S_SECTION_NOTE))
    else:
        story.append(Paragraph(
            "<i>No active ads found for format comparison.</i>",
            S_SECTION_NOTE))

    story.append(Spacer(1, 4 * mm))

    # ── Section D: Hook Diversity Score ──────────────────────────────────
    story.append(Paragraph("D. Hook Diversity Score", S_H3))

    hd = fatigue_data.get("hook_diversity", {})
    if hd:
        triggers_used = set(hd.get("triggers_used", []))
        triggers_missing = hd.get("triggers_missing", [])

        # Build 2-column checklist of all 10 triggers
        all_triggers = list(PSYCHOLOGICAL_TRIGGERS)
        mid = (len(all_triggers) + 1) // 2
        col1_triggers = all_triggers[:mid]
        col2_triggers = all_triggers[mid:]

        check_rows = []
        for i in range(max(len(col1_triggers), len(col2_triggers))):
            cells = []
            for col_trigs in (col1_triggers, col2_triggers):
                if i < len(col_trigs):
                    t = col_trigs[i]
                    label = t.replace("_", " ").title()
                    if t in triggers_used:
                        cells.append(Paragraph(
                            f"<font color='{_GREEN.hexval()}'>\u2713</font> {label}",
                            S_CHECK_G))
                    else:
                        cells.append(Paragraph(
                            f"<font color='{_RED.hexval()}'>\u2717</font> {label}",
                            S_CHECK_R))
                else:
                    cells.append("")
            check_rows.append(cells)

        check_tbl = Table(check_rows,
                          colWidths=[_CONTENT_W * 0.5, _CONTENT_W * 0.5])
        check_tbl.setStyle(TableStyle([
            ("TOPPADDING",    (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ("LEFTPADDING",   (0, 0), (-1, -1), 6),
            ("VALIGN",        (0, 0), (-1, -1), "TOP"),
        ]))
        story.append(check_tbl)
        story.append(Spacer(1, 2 * mm))

        interp = hd.get("interpretation", "")
        if interp:
            story.append(Paragraph(interp, S_BODY))
    else:
        story.append(Paragraph(
            "<i>Hook diversity data not available. Run the fatigue "
            "scorer to populate.</i>", S_SECTION_NOTE))

    return story


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — Competitor Winning Model
# ══════════════════════════════════════════════════════════════════════════════

def _page_competitor_winning_model(data: dict) -> list:
    intel_data = data.get("category_intel", {})
    brand      = data["brand"]
    story      = []

    # Top accent bar
    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 4 * mm))

    category = brand.get("category", "this category")
    total_profitable = intel_data.get("profitable_ads_in_universe", 0)

    story.append(Paragraph(
        f"What's Working in {category.title() if category else 'This Category'} "
        f"Right Now", S_H2))
    story.append(Paragraph(
        f"Based on {total_profitable} profitable ads (running 21+ days)"
        if total_profitable > 0 else
        "Based on competitor ad analysis",
        S_SUBTITLE))
    story.append(Spacer(1, 3 * mm))

    if not intel_data:
        story.append(Paragraph(
            "<i>Category intelligence not available. Run the full pipeline "
            "with competitor URLs to populate this section.</i>",
            S_SECTION_NOTE))
        return story

    # ── Section A: Trigger Distribution Among Winners ────────────────────
    story.append(Paragraph("A. Trigger Distribution Among Winners", S_H3))

    trigger_analysis = intel_data.get("trigger_analysis", {})
    by_profitable = trigger_analysis.get("by_profitable_only", {})
    p_rate = trigger_analysis.get("profitable_rate_by_trigger", {})

    if by_profitable:
        total_p_triggers = sum(by_profitable.values()) or 1
        trig_header = ["Trigger", "% of Winners", "Win Rate"]
        trig_rows = [trig_header]

        for trigger, count in sorted(
            by_profitable.items(), key=lambda x: x[1], reverse=True
        ):
            if count <= 0:
                continue
            pct_winners = round(count / total_p_triggers * 100, 1)
            win_rate = p_rate.get(trigger, 0)
            trig_rows.append([
                trigger.replace("_", " ").title(),
                f"{pct_winners:.0f}%",
                f"{win_rate:.0f}%",
            ])

        if len(trig_rows) > 1:
            story.append(_data_table(
                trig_rows,
                col_widths=[_CONTENT_W * 0.4, _CONTENT_W * 0.3, _CONTENT_W * 0.3],
            ))
        else:
            story.append(Paragraph(
                "<i>No trigger data in profitable ads.</i>", S_SECTION_NOTE))
    else:
        story.append(Paragraph(
            "<i>No profitable trigger data available.</i>", S_SECTION_NOTE))

    story.append(Spacer(1, 4 * mm))

    # ── Section B: Format Performance ────────────────────────────────────
    story.append(Paragraph("B. Format Performance", S_H3))

    format_analysis = intel_data.get("format_analysis", {})
    if format_analysis:
        fmt_header = ["Format", "Total %", "Winner %", "Win Rate"]
        fmt_rows = [fmt_header]
        highlight_rows = []
        row_idx = 1

        for fmt, fdata in sorted(
            format_analysis.items(),
            key=lambda x: x[1].get("win_rate", 0),
            reverse=True,
        ):
            if fmt == "unknown":
                continue
            total_pct = fdata.get("total_pct", 0)
            winner_pct = fdata.get("winner_pct", 0)
            win_rate = fdata.get("win_rate", 0)
            fmt_rows.append([
                fmt.title(),
                f"{total_pct:.0f}%",
                f"{winner_pct:.0f}%",
                f"{win_rate:.0f}%",
            ])
            # Highlight over-performing formats
            if winner_pct > total_pct * 1.3 and total_pct > 0:
                highlight_rows.append(row_idx)
            row_idx += 1

        if len(fmt_rows) > 1:
            col_ws = [_CONTENT_W * 0.28, _CONTENT_W * 0.24,
                      _CONTENT_W * 0.24, _CONTENT_W * 0.24]
            story.append(_data_table(
                fmt_rows, col_widths=col_ws,
                highlight_rows=highlight_rows, highlight_color=_LIGHT_TEAL,
            ))
        else:
            story.append(Paragraph(
                "<i>No format data available.</i>", S_SECTION_NOTE))
    else:
        story.append(Paragraph(
            "<i>No format analysis available.</i>", S_SECTION_NOTE))

    story.append(Spacer(1, 4 * mm))

    # ── Section C: Key Patterns ──────────────────────────────────────────
    story.append(Paragraph("C. Key Patterns", S_H3))

    patterns = intel_data.get("patterns", [])
    if patterns:
        for i, pattern in enumerate(patterns, 1):
            story.append(Paragraph(f"{i}. {pattern}", S_INSIGHT))
    else:
        story.append(Paragraph(
            "<i>Run the full pipeline with more competitor data "
            "to surface patterns.</i>", S_SECTION_NOTE))

    return story


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — Hook Intelligence
# ══════════════════════════════════════════════════════════════════════════════

def _page_hook_intelligence(data: dict) -> list:
    intel_data = data.get("category_intel", {})
    story      = []

    # Top accent bar
    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("Top Competitor Hooks by Psychological Angle", S_H2))
    story.append(Paragraph(
        "Extracted from profitable ads (21+ day run filter)", S_SUBTITLE))

    hook_database = intel_data.get("hook_database", {})

    if not hook_database:
        story.append(Paragraph(
            "<i>No hook data available. This section requires competitor ads "
            "with analyzed copy or video transcripts. Run the full pipeline "
            "with competitor URLs to populate.</i>", S_SECTION_NOTE))
        return story

    # Sort triggers by hook count descending, show top 5
    sorted_triggers = sorted(
        hook_database.items(),
        key=lambda x: x[1].get("count", 0),
        reverse=True,
    )

    for trigger, tdata in sorted_triggers[:5]:
        pct_winners = tdata.get("pct_of_winners", 0)
        story.append(Paragraph(
            f"{trigger.replace('_', ' ').title()} "
            f"({pct_winners:.0f}% of winners)", S_H3))

        hooks = tdata.get("hooks", [])
        if hooks:
            hook_header = ["Hook Text", "Source Brand", "Days Running"]
            hook_rows = [hook_header]
            for hook in hooks[:4]:
                text = hook.get("text", "\u2014")
                # Truncate long hooks for table
                if len(text) > 60:
                    text = text[:57] + "..."
                hook_rows.append([
                    text,
                    str(hook.get("source_brand", "\u2014")),
                    str(hook.get("duration_days", "\u2014")),
                ])

            col_ws = [_CONTENT_W * 0.55, _CONTENT_W * 0.25, _CONTENT_W * 0.20]
            story.append(_data_table(hook_rows, col_widths=col_ws))
        story.append(Spacer(1, 3 * mm))

    return story


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — Visual Pattern Analysis
# ══════════════════════════════════════════════════════════════════════════════

_PATTERN_LABELS = {
    "face_dominant_pct": "Face/person prominent",
    "text_overlay_pct": "Text overlay present",
    "minimal_aesthetic_pct": "Minimal/clean aesthetic",
    "before_after_pct": "Before/after format",
    "product_focused_pct": "Product-focused",
    "ugc_style_pct": "UGC/raw style",
}

_PATTERN_INTERPRETATIONS = {
    "face_dominant_pct": (
        "the algorithm rewards authenticity and human connection "
        "signals in this category"),
    "text_overlay_pct": (
        "strong opening text hooks are critical for scroll-stopping "
        "in this vertical"),
    "minimal_aesthetic_pct": (
        "clean, uncluttered visuals cut through the noise in "
        "crowded feeds"),
    "before_after_pct": (
        "transformation proof is a dominant persuasion pattern "
        "\u2014 viewers need to see the result"),
    "product_focused_pct": (
        "direct product presentation builds purchase intent better "
        "than lifestyle imagery here"),
    "ugc_style_pct": (
        "raw, user-generated aesthetics outperform polished studio "
        "content"),
}


def _page_visual_patterns(data: dict) -> list:
    intel_data = data.get("category_intel", {})
    story      = []

    # Top accent bar
    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("What Winning Ads Look Like", S_H2))

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
        vp_header = ["Visual Pattern", "% of Winners"]
        vp_rows = [vp_header]
        for _, label, pct in pattern_items:
            vp_rows.append([label, f"{pct:.0f}%"])

        story.append(_data_table(
            vp_rows,
            col_widths=[_CONTENT_W * 0.6, _CONTENT_W * 0.4],
        ))
        story.append(Spacer(1, 4 * mm))

        # Prose interpretation of top pattern
        top_key, top_label, top_pct = pattern_items[0]
        interp = _PATTERN_INTERPRETATIONS.get(top_key, "this pattern is dominant among winners")
        story.append(Paragraph(
            f"The dominance of <b>{top_label.lower()}</b> content "
            f"({top_pct:.0f}% of winners) suggests {interp}.",
            S_BODY))

        # Second pattern if different enough
        if len(pattern_items) >= 2:
            sec_key, sec_label, sec_pct = pattern_items[1]
            if sec_pct >= 20:
                sec_interp = _PATTERN_INTERPRETATIONS.get(
                    sec_key, "this pattern also appears frequently")
                story.append(Paragraph(
                    f"Additionally, <b>{sec_label.lower()}</b> at "
                    f"{sec_pct:.0f}% indicates {sec_interp}.",
                    S_BODY))
    else:
        story.append(Paragraph(
            "<i>No visual patterns detected above 0% threshold.</i>",
            S_SECTION_NOTE))

    return story


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 6 — Gap Analysis
# ══════════════════════════════════════════════════════════════════════════════

def _build_gaps(data: dict) -> list[dict]:
    """Build a unified list of creative gaps from 3 sources."""
    intel_data   = data.get("category_intel", {})
    fatigue_data = data.get("fatigue_analysis", {})
    brand        = data["brand"]
    ads          = data["client_ads"]
    gaps: list[dict] = []

    category = (brand.get("category") or "this category").title()

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
                gaps.append({
                    "type": "ANGLE GAP",
                    "title": f"Zero {trigger.replace('_', ' ').title()} Creatives",
                    "competitor_usage": f"{pct:.0f}% of profitable competitor ads",
                    "your_usage": "0%",
                    "impact": (f"Missing a proven conversion angle "
                               f"in {category}"),
                })

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
            gaps.append({
                "type": "FORMAT GAP",
                "title": f"No {fmt.title()} Ads",
                "competitor_usage": (
                    f"{fdata.get('total_pct', 0):.0f}% of competitor ads, "
                    f"{winner_pct:.0f}% of winners"),
                "your_usage": "0%",
                "impact": (f"{fmt.title()} format has "
                           f"{fdata.get('win_rate', 0):.0f}% win rate "
                           f"\u2014 high ROI potential"),
            })

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
                gaps.append({
                    "type": "HOOK STRUCTURE GAP",
                    "title": (f"No '{hook.replace('_', ' ').title()}' "
                              f"Hooks"),
                    "competitor_usage": (
                        f"{pct:.0f}% of profitable competitor hooks"),
                    "your_usage": "0%",
                    "impact": (f"This hook structure has "
                               f"{hook_win_rate:.0f}% win rate"),
                })

    # Sort by type priority
    type_order = {"ANGLE GAP": 0, "FORMAT GAP": 1, "HOOK STRUCTURE GAP": 2}
    gaps.sort(key=lambda g: type_order.get(g["type"], 9))
    return gaps


def _page_gap_analysis(data: dict) -> list:
    story = []

    # Top accent bar
    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("Critical Creative Gaps", S_H2))
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
        html = (
            f"<font size='8'><b>{gap['type']}</b></font><br/>"
            f"<font size='12'><b>{gap['title']}</b></font><br/>"
            f"Competitor usage: {gap['competitor_usage']}<br/>"
            f"Your usage: {gap['your_usage']}<br/>"
            f"<font color='{_GREY.hexval()}'><i>"
            f"{gap['impact']}</i></font>"
        )
        story.append(_callout_box(html))
        story.append(Spacer(1, 3 * mm))

    story.append(Spacer(1, 4 * mm))
    story.append(Paragraph(
        "Each gap represents a creative territory your competitors are "
        "actively monetizing. Closing these gaps is the highest-leverage "
        "action for your ad account's performance.",
        S_BODY))

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

    matrix_rows: list[list[str]] = []

    # From angle gaps
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
            matrix_rows.append([
                trigger, best_format.title(), stage, "3\u20135"])

    # If no angle gaps, use top 3 competitor triggers instead
    if not matrix_rows:
        trigger_analysis = intel_data.get("trigger_analysis", {})
        by_profitable = trigger_analysis.get("by_profitable_only", {})
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
            matrix_rows.append([
                label, best_format.title(), stage, "3\u20135"])

    if matrix_rows:
        header = ["Angle", "Format", "Funnel Stage", "Recommended Count"]
        story.append(_data_table(
            [header] + matrix_rows,
            col_widths=[_CONTENT_W * 0.30, _CONTENT_W * 0.22,
                        _CONTENT_W * 0.22, _CONTENT_W * 0.26],
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

    calendar_items = [
        (
            "Week 1",
            f"Launch {n_creatives} new creatives across "
            f"{n_angles} angles. Prioritize "
            f"{'gaps identified above' if gap_count else 'top-performing competitor angles'}."
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
# PAGE 8 — Data-Backed Concepts
# ══════════════════════════════════════════════════════════════════════════════

def _page_concepts(data: dict) -> list:
    intel_data = data.get("category_intel", {})
    concepts   = data.get("sample_concepts", [])
    brand      = data["brand"]
    story      = []

    # Top accent bar
    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("High-Conversion Creative Concepts", S_H2))
    story.append(Paragraph(
        "Each concept is linked to proven competitor patterns",
        S_SUBTITLE))

    if not concepts:
        story.append(Paragraph(
            "<i>Creative concepts are generated during the full pipeline "
            "run. Run the sprint mode to see 50+ data-backed concepts "
            "here.</i>", S_SECTION_NOTE))
        story.append(Spacer(1, 8 * mm))
        story.append(_cta_banner(brand["name"]))
        return story

    # Trigger stats for "why this works"
    trigger_stats = intel_data.get(
        "trigger_analysis", {}).get("by_profitable_only", {})
    total_winners = sum(trigger_stats.values()) or 1

    for i, concept in enumerate(concepts[:5], 1):
        story += _concept_card(i, concept, trigger_stats, total_winners)

    story.append(Spacer(1, 8 * mm))
    story.append(_cta_banner(brand["name"]))

    return story


def _concept_card(
    index: int,
    concept: dict,
    trigger_stats: dict,
    total_winners: int,
) -> list:
    """Render one concept card with hook, angle, preview, and 'why this works'."""
    elements: list = []

    hook  = concept.get("hook_text") or "\u2014"
    angle = concept.get("psychological_angle") or ""

    # Hook number + text
    elements.append(Paragraph(
        f"<font color='{_TEAL.hexval()}'>{index}.</font>&nbsp; "
        f"&ldquo;{hook}&rdquo;",
        S_HOOK))

    # Angle badge
    angle_label = angle.replace("_", " ").title() if angle else "\u2014"
    elements.append(Paragraph(
        f"Angle: {angle_label}", S_HOOK_M))

    # Body preview
    body = concept.get("body_script") or ""
    if body:
        preview = body[:150] + ("..." if len(body) > 150 else "")
        elements.append(Paragraph(preview, _s(
            f"cprev_{index}", fontSize=9, textColor=_GREY,
            leftIndent=4 * mm, spaceAfter=2, leading=12)))

    # Visual direction
    vis = concept.get("visual_direction") or ""
    if vis:
        vis_preview = vis[:100] + ("..." if len(vis) > 100 else "")
        elements.append(Paragraph(
            f"<b>Visual:</b> {vis_preview}", _s(
                f"cvis_{index}", fontSize=9, textColor=_GREY,
                leftIndent=4 * mm, spaceAfter=2, leading=12)))

    # "Why This Works" — data-backed justification
    if angle:
        angle_count = trigger_stats.get(angle, 0)
        angle_pct = round(angle_count / total_winners * 100, 1)
        if angle_pct > 0:
            why_works = (
                f"Based on {angle_pct:.0f}% competitor success rate for "
                f"{angle.replace('_', ' ')} hooks")
        else:
            why_works = (
                f"Fills an untapped angle gap \u2014 "
                f"{angle.replace('_', ' ')} is underused in this category")
    else:
        why_works = "Data-driven concept based on competitor intelligence"

    elements.append(Paragraph(
        f"<font color='{_TEAL.hexval()}'><b>Why this works:</b></font> "
        f"{why_works}",
        _s(f"cwhy_{index}", fontSize=9, leading=12,
           leftIndent=4 * mm, spaceAfter=4)))

    elements.append(Spacer(1, 2 * mm))
    return elements


def _cta_banner(brand_name: str) -> Table:
    """The money shot — CTA banner for the paid sprint."""
    lines = [
        Paragraph(
            "This is a sample of the 50+ strategic concepts included "
            "in a full Creative Sprint.",
            _s("cta1", fontSize=11, textColor=_NAVY, alignment=TA_CENTER,
               fontName="Helvetica-Bold", spaceAfter=4)),
        Paragraph(
            "Each concept includes hook copy, body script, visual direction, "
            "3 CTA variations, format specification, and competitor reference.",
            _s("cta2", fontSize=9, textColor=_GREY, alignment=TA_CENTER,
               spaceAfter=8)),
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
# PAGE 9 — Priority Action Plan
# ══════════════════════════════════════════════════════════════════════════════

_PRIORITY_COLORS = {
    "high":   _RED,
    "medium": _ORANGE,
    "low":    _GREY,
}


def _page_action_plan(data: dict) -> list:
    waste_report = data.get("waste_report", {})
    story        = []

    # Top accent bar
    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("Priority Action Plan", S_H2))
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

    # ── Section B: What This Audit Didn't Cover ───────────────────────────
    story.append(Paragraph("B. What This Audit Didn't Cover", S_H3))

    not_covered = [
        "50+ data-backed creative concepts (included in full Creative Sprint)",
        "Weekly competitor tracking and creative refresh alerts",
        ("Performance feedback loop \u2014 which hooks actually convert "
         "for YOUR brand"),
        "Ready-to-execute visual briefs with format specifications",
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
