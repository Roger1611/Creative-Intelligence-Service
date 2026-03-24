"""
deliverables/audit_generator.py — Generate the free Ad Fatigue Audit PDF.

The audit is the primary sales tool — sent to prospects to prove value before
asking for money. 2–3 pages, professional layout, dark navy + teal accent.

PAGE 1: Cover + Brand Snapshot
PAGE 2: Competitor Comparison (side-by-side table + callout boxes)
PAGE 3: Sample Hooks + CTA for paid sprint

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

from config import FATIGUE_AD_MIN_DAYS, RAW_DIR, get_connection
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
    """Pull everything needed for the audit from the database."""
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

    return {
        "brand": brand,
        "client_ads": client_ads,
        "waste_report": waste_report,
        "competitors": comp_data,
        "sample_concepts": concepts,
    }


# ── PDF builder ───────────────────────────────────────────────────────────────

def _build_pdf(data: dict, out_path: Path) -> None:
    doc = SimpleDocTemplate(
        str(out_path), pagesize=A4,
        leftMargin=_MARGIN, rightMargin=_MARGIN,
        topMargin=_MARGIN, bottomMargin=_MARGIN,
    )
    story = []
    story += _page_cover(data)
    story.append(PageBreak())
    story += _page_competitor(data)
    story.append(PageBreak())
    story += _page_hooks(data)
    doc.build(story)


# ── PAGE 1: Cover + Brand Snapshot ────────────────────────────────────────────

def _page_cover(data: dict) -> list:
    brand = data["brand"]
    ads   = data["client_ads"]
    wr    = data["waste_report"]
    story = []

    # Top accent bar
    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 8 * mm))

    # Title
    story.append(Paragraph(
        f"Ad Creative Audit for {brand['name']}", S_TITLE))
    story.append(Paragraph(
        f"Prepared by Roger Krishna &bull; Creative Intelligence Service "
        f"&bull; {datetime.now().strftime('%d %B %Y')}", S_SUBTITLE))

    story.append(HRFlowable(width="100%", thickness=1, color=_LIGHT))
    story.append(Spacer(1, 6 * mm))

    # Brand snapshot heading
    story.append(Paragraph("Brand Snapshot", S_H2))
    story.append(Spacer(1, 2 * mm))

    # Compute metrics
    active_ads = [a for a in ads if a.get("is_active")]
    total_active = len(active_ads)
    fatigued = [a for a in active_ads
                if (a.get("duration_days") or 0) >= FATIGUE_AD_MIN_DAYS]
    fatigued_count = len(fatigued)

    # Format distribution
    fmt_counts = Counter(a.get("creative_type", "unknown") for a in active_ads)
    fmt_str = " / ".join(f"{v} {k}" for k, v in fmt_counts.most_common())

    # Days since last new creative
    start_dates = [a["start_date"] for a in ads if a.get("start_date")]
    if start_dates:
        latest = max(start_dates)
        try:
            days_since = (datetime.now() - datetime.fromisoformat(latest)).days
        except (ValueError, TypeError):
            days_since = None
    else:
        days_since = None

    # Diversity score from waste report
    diversity = wr.get("creative_diversity_score", None)

    # Metric cards — built as a table for even spacing
    metric_cards = _metric_row([
        (str(total_active), "Active Ads"),
        (f"{days_since}d" if days_since is not None else "—", "Since Last Creative"),
        (f"{diversity:.0f}/100" if diversity is not None else "—", "Diversity Score"),
        (f"{fatigued_count}", "Fatigued Ads"),
    ])
    story.append(metric_cards)
    story.append(Spacer(1, 4 * mm))

    # Video ads with transcripts count
    video_transcript_count = sum(
        1 for a in active_ads
        if a.get("creative_type") == "video" and a.get("transcript")
    )

    # Format distribution line
    fmt_line = f"<b>Format mix:</b> {fmt_str or 'No ads found'}"
    if video_transcript_count:
        fmt_line += f" &bull; {video_transcript_count} video ad{'s' if video_transcript_count != 1 else ''} with transcripts analyzed"
    story.append(Paragraph(fmt_line, _s("fmtline", fontSize=10, spaceAfter=6)))

    story.append(Spacer(1, 4 * mm))

    # Verdict box
    verdict = _build_verdict(total_active, fatigued_count, days_since, diversity)
    story.append(_verdict_box(verdict))

    story.append(Spacer(1, 6 * mm))

    # Ad thumbnails (up to 4)
    thumbs = _ad_thumbnails(ads[:8], brand["name"])
    if thumbs:
        story.append(Paragraph("Active Ad Creatives", S_H3))
        story.append(Spacer(1, 2 * mm))
        story.append(thumbs)

    return story


def _build_verdict(total: int, fatigued: int, days_since, diversity) -> str:
    parts = []
    if fatigued > 0:
        parts.append(
            f"{fatigued} of {total} active ads have been running "
            f"{FATIGUE_AD_MIN_DAYS}+ days without refresh")
    if days_since is not None and days_since > 21:
        parts.append(f"no new creative launched in the past {days_since} days")
    if diversity is not None and diversity < 40:
        parts.append(
            f"creative diversity score is {diversity:.0f}/100 (below average)")

    if parts:
        return ("Your ad account shows critical creative fatigue — "
                + ", and ".join(parts) + ".")
    if total == 0:
        return "No active ads found. Your brand may not be running Meta ads currently."
    return "Your ad account looks healthy — keep iterating on fresh creatives."


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


def _ad_thumbnails(ads: list[dict], brand_name: str) -> Table | None:
    """Build a row of ad image thumbnails (up to 4)."""
    imgs = []
    slug = safe_brand_slug(brand_name)
    for ad in ads:
        img_path = ad.get("image_path") or ""
        if img_path and Path(img_path).is_file():
            try:
                imgs.append(Image(img_path, width=38 * mm, height=38 * mm,
                                  kind="proportional"))
            except Exception:
                continue
        if len(imgs) >= 4:
            break

    if not imgs:
        return None

    t = Table([imgs], colWidths=[42 * mm] * len(imgs))
    t.setStyle(TableStyle([
        ("ALIGN",  (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return t


# ── PAGE 2: Competitor Comparison ─────────────────────────────────────────────

def _page_competitor(data: dict) -> list:
    brand     = data["brand"]
    client_ads = data["client_ads"]
    comps     = data["competitors"]
    story     = []

    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 4 * mm))
    story.append(Paragraph("Competitor Comparison", S_H2))
    story.append(Spacer(1, 3 * mm))

    # Build side-by-side table
    header = ["Metric", brand["name"]]
    for c in comps[:2]:
        header.append(c["brand"]["name"])

    rows = [header]

    # Row: active ad count
    row = ["Active Ads", str(len([a for a in client_ads if a.get("is_active")]))]
    for c in comps[:2]:
        row.append(str(len([a for a in c["ads"] if a.get("is_active")])))
    rows.append(row)

    # Row: avg refresh (avg duration_days of active ads)
    def _avg_dur(ads_list):
        durs = [a["duration_days"] for a in ads_list
                if a.get("is_active") and a.get("duration_days")]
        return f"{sum(durs) / len(durs):.0f}d" if durs else "—"

    row = ["Avg Ad Lifespan", _avg_dur(client_ads)]
    for c in comps[:2]:
        row.append(_avg_dur(c["ads"]))
    rows.append(row)

    # Row: format mix
    def _fmt_summary(ads_list):
        counts = Counter(a.get("creative_type", "?") for a in ads_list if a.get("is_active"))
        return ", ".join(f"{v} {k}" for k, v in counts.most_common(3)) or "—"

    row = ["Format Mix", _fmt_summary(client_ads)]
    for c in comps[:2]:
        row.append(_fmt_summary(c["ads"]))
    rows.append(row)

    # Row: dominant triggers
    def _top_triggers(ads_list):
        trigs = [a["psychological_trigger"] for a in ads_list
                 if a.get("psychological_trigger")]
        if not trigs:
            return "—"
        c = Counter(trigs)
        return ", ".join(t.replace("_", " ").title() for t, _ in c.most_common(2))

    row = ["Top Triggers", _top_triggers(client_ads)]
    for c in comps[:2]:
        row.append(_top_triggers(c["ads"]))
    rows.append(row)

    # Build the table
    n_cols = len(header)
    first_w = 34 * mm
    other_w = (_CONTENT_W - first_w) / (n_cols - 1)
    col_ws = [first_w] + [other_w] * (n_cols - 1)

    style_cmds = [
        ("BACKGROUND",  (0, 0), (-1, 0), _NAVY),
        ("TEXTCOLOR",   (0, 0), (-1, 0), _WHITE),
        ("FONTNAME",    (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, -1), 9),
        ("FONTNAME",    (0, 1), (0, -1),  "Helvetica-Bold"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [_WHITE, _LIGHT]),
        ("GRID",        (0, 0), (-1, -1), 0.5, colors.Color(0.85, 0.85, 0.85)),
        ("TOPPADDING",    (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
    ]

    # Highlight client column header with teal
    style_cmds.append(("BACKGROUND", (1, 0), (1, 0), _TEAL))

    t = Table(rows, colWidths=col_ws)
    t.setStyle(TableStyle(style_cmds))
    story.append(t)
    story.append(Spacer(1, 6 * mm))

    # Callout boxes for biggest gaps
    callouts = _build_gap_callouts(data)
    for callout in callouts[:3]:
        story.append(_callout_box(callout))
        story.append(Spacer(1, 3 * mm))

    # Competitor ad thumbnails
    for c in comps[:2]:
        thumbs = _ad_thumbnails(c["ads"][:4], c["brand"]["name"])
        if thumbs:
            story.append(Spacer(1, 2 * mm))
            story.append(Paragraph(
                f"{c['brand']['name']} — Sample Ads", S_H3))
            story.append(thumbs)

    return story


def _build_gap_callouts(data: dict) -> list[str]:
    """Generate key insight callout strings from the data."""
    client_ads = data["client_ads"]
    comps      = data["competitors"]
    callouts   = []

    # Refresh frequency gap
    def _avg_days(ads):
        ds = [a["duration_days"] for a in ads
              if a.get("is_active") and a.get("duration_days")]
        return sum(ds) / len(ds) if ds else 0

    client_avg = _avg_days(client_ads)
    comp_avgs = []
    for c in comps:
        avg = _avg_days(c["ads"])
        if avg > 0:
            comp_avgs.append((c["brand"]["name"], avg))

    if comp_avgs and client_avg > 0:
        best = min(comp_avgs, key=lambda x: x[1])
        if client_avg > best[1] * 1.5:
            callouts.append(
                f"<b>Refresh Gap:</b> You're refreshing every "
                f"{client_avg:.0f} days. {best[0]} refreshes every "
                f"{best[1]:.0f} days — {client_avg / best[1]:.1f}x faster "
                f"creative iteration."
            )

    # Creative volume gap
    client_active = len([a for a in client_ads if a.get("is_active")])
    for c in comps:
        comp_active = len([a for a in c["ads"] if a.get("is_active")])
        if comp_active > client_active * 2:
            callouts.append(
                f"<b>Volume Gap:</b> {c['brand']['name']} has "
                f"{comp_active} active ads vs your {client_active} — "
                f"{comp_active / client_active:.0f}x more creative variations."
            )
            break

    # Trigger diversity gap
    client_trigs = set(a.get("psychological_trigger") for a in client_ads
                       if a.get("psychological_trigger"))
    for c in comps:
        comp_trigs = set(a.get("psychological_trigger") for a in c["ads"]
                         if a.get("psychological_trigger"))
        missing = comp_trigs - client_trigs
        if len(missing) >= 2:
            missing_str = ", ".join(
                t.replace("_", " ").title() for t in list(missing)[:3])
            callouts.append(
                f"<b>Angle Gap:</b> {c['brand']['name']} uses "
                f"triggers you're ignoring: {missing_str}."
            )
            break

    if not callouts:
        callouts.append(
            "<b>Data note:</b> Run the full pipeline with competitors "
            "to unlock detailed gap analysis."
        )

    return callouts


def _callout_box(html_text: str) -> Table:
    """Teal-bordered callout box."""
    p = Paragraph(html_text, S_CALLOUT)
    t = Table([[p]], colWidths=[_CONTENT_W - 4 * mm])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), colors.HexColor("#E0F7FA")),
        ("LINEBEFORECOL", (0, 0), (0, -1), 3, _TEAL),
        ("TOPPADDING",    (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("LEFTPADDING",   (0, 0), (-1, -1), 10),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 10),
    ]))
    return t


# ── PAGE 3: Sample Hooks + CTA ───────────────────────────────────────────────

def _page_hooks(data: dict) -> list:
    brand    = data["brand"]
    concepts = data["sample_concepts"]
    story    = []

    story.append(HRFlowable(width="100%", thickness=4, color=_TEAL))
    story.append(Spacer(1, 4 * mm))
    story.append(Paragraph("Sample Creative Concepts", S_H2))
    story.append(Paragraph(
        "Here are 5 scroll-stopping hooks tailored to your brand, "
        "based on competitor intelligence and proven psychological triggers.",
        S_BODY))
    story.append(Spacer(1, 3 * mm))

    if not concepts:
        story.append(Paragraph(
            "<i>No concepts generated yet. Run the full pipeline to see "
            "AI-generated hooks here.</i>", S_BODY))
    else:
        for i, concept in enumerate(concepts[:5], 1):
            story += _hook_card(i, concept)

    story.append(Spacer(1, 8 * mm))

    # CTA box
    story.append(_cta_banner(brand["name"]))

    # Footer
    story.append(Spacer(1, 6 * mm))
    story.append(HRFlowable(width="100%", thickness=1, color=_LIGHT))
    story.append(Paragraph(
        "Creative Intelligence Service &bull; Confidential &bull; "
        f"{datetime.now().strftime('%B %Y')}",
        S_FOOTER))

    return story


def _hook_card(index: int, concept: dict) -> list:
    """Render one hook concept as a numbered card."""
    elements = []

    hook = concept.get("hook_text", "—")
    angle = (concept.get("psychological_angle") or "—").replace("_", " ").title()

    # Parse CTA variations
    ctas_raw = concept.get("cta_variations_json") or "[]"
    if isinstance(ctas_raw, str):
        try:
            ctas = json.loads(ctas_raw)
        except (json.JSONDecodeError, TypeError):
            ctas = []
    else:
        ctas = ctas_raw

    # Hook number + text
    elements.append(Paragraph(
        f"<font color='{_TEAL.hexval()}'>{index}.</font>&nbsp; "
        f"&ldquo;{hook}&rdquo;",
        S_HOOK))

    # Meta line: angle + format
    meta_parts = [f"Angle: {angle}"]
    elements.append(Paragraph(" &bull; ".join(meta_parts), S_HOOK_M))

    # Brief body preview if available
    body = concept.get("body_script") or ""
    if body:
        preview = body[:120] + ("..." if len(body) > 120 else "")
        elements.append(Paragraph(preview, _s(
            f"preview{index}", fontSize=9, textColor=_GREY, leftIndent=4 * mm,
            spaceAfter=4, leading=12)))

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
            "3 CTA variations, competitor reference, and format recommendation.",
            _s("cta2", fontSize=9, textColor=_GREY, alignment=TA_CENTER,
               spaceAfter=8)),
        Paragraph(
            "Want the full package? Reply to this message.",
            _s("cta3", fontSize=13, textColor=_TEAL, alignment=TA_CENTER,
               fontName="Helvetica-Bold")),
    ]
    inner = Table([[line] for line in lines], colWidths=[_CONTENT_W - 12 * mm])
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


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Generate a free Ad Fatigue Audit PDF for a brand.")
    parser.add_argument("--brand", required=True, help="Brand name (must exist in DB)")
    parser.add_argument("--output", default="audits", help="Output directory (default: audits/)")
    args = parser.parse_args()

    path = run(args.brand, output_dir=args.output)
    print(f"Audit PDF generated: {path}")
