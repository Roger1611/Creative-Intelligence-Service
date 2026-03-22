"""
deliverables/audit_generator.py — Generate the free Creative Waste Audit PDF.

The audit is the primary lead-generation deliverable. It shows the prospect
exactly how much budget they're wasting on fatigued creatives and gives them
3 competitor-grounded recommendations — enough to prove value, not enough to
DIY the full solution.

ReportLab note: coordinates are bottom-left origin (y=0 = bottom of page).
"""

import json
import logging
from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    HRFlowable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from config import get_connection

logger = logging.getLogger(__name__)

# ── Brand colours ──────────────────────────────────────────────────────────────
_ACCENT  = colors.HexColor("#FF4D4D")
_DARK    = colors.HexColor("#1A1A2E")
_LIGHT   = colors.HexColor("#F5F5F5")
_SUCCESS = colors.HexColor("#27AE60")


def run(brand_name: str, output_dir: str = "audits") -> Path:
    """
    Generate a PDF audit for *brand_name* and save it to *output_dir*.
    Returns the path to the generated PDF.
    """
    brand = _fetch_brand(brand_name)
    if not brand:
        raise ValueError(f"Brand '{brand_name}' not found in database.")

    waste_report = _fetch_latest_waste_report(brand["id"])
    if not waste_report:
        raise ValueError(f"No waste report for '{brand_name}'. Run pipeline first.")

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = brand_name.lower().replace(" ", "_")
    out_path  = out_dir / f"{safe_name}_audit_{timestamp}.pdf"

    _build_pdf(brand, waste_report, out_path)
    logger.info("Audit PDF saved → %s", out_path)
    return out_path


# ── PDF builder ────────────────────────────────────────────────────────────────

def _build_pdf(brand: dict, report: dict, out_path: Path) -> None:
    doc = SimpleDocTemplate(
        str(out_path),
        pagesize=A4,
        leftMargin=20 * mm,
        rightMargin=20 * mm,
        topMargin=20 * mm,
        bottomMargin=20 * mm,
    )

    styles = getSampleStyleSheet()
    story  = []

    # ── Header ────────────────────────────────────────────────────────────────
    story.append(_para(
        f"Creative Waste Audit — {brand['name']}",
        ParagraphStyle("h1", fontSize=22, textColor=_DARK, spaceAfter=4, fontName="Helvetica-Bold"),
    ))
    story.append(_para(
        f"Category: {brand.get('category', '—')} &nbsp;|&nbsp; "
        f"Generated: {datetime.now().strftime('%d %b %Y')}",
        ParagraphStyle("sub", fontSize=10, textColor=colors.grey, spaceAfter=8),
    ))
    story.append(HRFlowable(width="100%", thickness=2, color=_ACCENT))
    story.append(Spacer(1, 6 * mm))

    # ── Score card ────────────────────────────────────────────────────────────
    diversity = report.get("creative_diversity_score", 0)
    waste_pct = report.get("waste_estimate_pct", 0)

    score_data = [
        ["Metric", "Value", "Benchmark"],
        ["Creative Diversity Score", f"{diversity:.0f} / 100", "≥ 60"],
        ["Estimated Wasted Spend",   f"{waste_pct:.0f}%",      "< 15%"],
        ["Avg Ad Lifespan",          f"{report.get('avg_refresh_days', 0):.0f} days", "14–21 days"],
    ]
    story.append(_table(score_data, col_widths=[80 * mm, 40 * mm, 50 * mm]))
    story.append(Spacer(1, 8 * mm))

    # ── Fatigue flags ─────────────────────────────────────────────────────────
    flags = json.loads(report["fatigue_flags_json"]) if report.get("fatigue_flags_json") else []
    if flags:
        story.append(_para("Fatigued Ads Detected", _heading_style()))
        flag_data = [["Ad ID", "Days Running", "Status"]] + [
            [f["ad_library_id"][:16] + "…", str(f["duration_days"]), "⚠ Refresh Now"]
            for f in flags[:10]
        ]
        story.append(_table(flag_data, col_widths=[90 * mm, 40 * mm, 40 * mm]))
        story.append(Spacer(1, 8 * mm))

    # ── Recommendations ───────────────────────────────────────────────────────
    recs = json.loads(report["recommendations_json"]) if report.get("recommendations_json") else []
    if recs:
        story.append(_para("Recommendations", _heading_style()))
        for i, rec in enumerate(recs[:5], 1):
            priority = rec.get("priority", "medium").upper()
            colour   = _ACCENT if priority == "HIGH" else colors.orange if priority == "MEDIUM" else _SUCCESS
            story.append(_para(
                f"<b>[{priority}]</b> {rec.get('action', '')}",
                ParagraphStyle(f"rec{i}", fontSize=10, textColor=_DARK,
                               spaceAfter=2, leftIndent=4 * mm),
            ))
            if rec.get("expected_impact"):
                story.append(_para(
                    f"<i>Expected impact: {rec['expected_impact']}</i>",
                    ParagraphStyle(f"imp{i}", fontSize=9, textColor=colors.grey,
                                   spaceAfter=5, leftIndent=8 * mm),
                ))

    # ── Summary ───────────────────────────────────────────────────────────────
    if report.get("summary"):
        story.append(Spacer(1, 6 * mm))
        story.append(HRFlowable(width="100%", thickness=1, color=_LIGHT))
        story.append(Spacer(1, 4 * mm))
        story.append(_para("Summary", _heading_style()))
        story.append(_para(report["summary"], ParagraphStyle(
            "summary", fontSize=10, textColor=_DARK, leading=14, spaceAfter=4,
        )))

    doc.build(story)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _fetch_brand(name: str) -> dict | None:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM brands WHERE name = ?", (name,)).fetchone()
    return dict(row) if row else None


def _fetch_latest_waste_report(brand_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM waste_reports WHERE client_brand_id = ? ORDER BY generated_at DESC LIMIT 1",
            (brand_id,),
        ).fetchone()
    return dict(row) if row else None


def _para(text: str, style: ParagraphStyle) -> Paragraph:
    return Paragraph(text, style)


def _heading_style() -> ParagraphStyle:
    return ParagraphStyle(
        "heading", fontSize=13, textColor=_DARK, fontName="Helvetica-Bold",
        spaceBefore=4, spaceAfter=3,
    )


def _table(data: list, col_widths: list) -> Table:
    t = Table(data, colWidths=col_widths)
    t.setStyle(TableStyle([
        ("BACKGROUND",  (0, 0), (-1, 0),  _DARK),
        ("TEXTCOLOR",   (0, 0), (-1, 0),  colors.white),
        ("FONTNAME",    (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, -1), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, _LIGHT]),
        ("GRID",        (0, 0), (-1, -1), 0.5, colors.lightgrey),
        ("TOPPADDING",  (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return t
