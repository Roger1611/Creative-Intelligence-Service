"""
deliverables/sprint_generator.py — Package generated creative concepts into
a structured sprint deliverable (JSON + PDF summary).

A sprint deliverable is the paid product: N ad concepts with full creative
direction, organised by psychological angle and format.
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
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from config import get_connection

logger = logging.getLogger(__name__)

_DARK   = colors.HexColor("#1A1A2E")
_ACCENT = colors.HexColor("#FF4D4D")
_LIGHT  = colors.HexColor("#F5F5F5")


def run(brand_name: str, batch_id: str, output_dir: str = "sprints") -> Path:
    """
    Generate a sprint deliverable PDF for *brand_name* / *batch_id*.
    Returns path to generated PDF.
    """
    brand = _fetch_brand(brand_name)
    if not brand:
        raise ValueError(f"Brand '{brand_name}' not found in database.")

    concepts = _fetch_concepts(brand["id"], batch_id)
    if not concepts:
        raise ValueError(f"No concepts found for batch '{batch_id}'.")

    out_dir  = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = brand_name.lower().replace(" ", "_")
    out_path  = out_dir / f"{safe_name}_sprint_{batch_id}_{timestamp}.pdf"

    # Also dump raw JSON alongside the PDF
    json_path = out_path.with_suffix(".json")
    json_path.write_text(
        json.dumps(concepts, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    logger.info("Sprint JSON saved → %s", json_path)

    _build_pdf(brand, concepts, batch_id, out_path)
    logger.info("Sprint PDF saved → %s", out_path)
    return out_path


# ── PDF builder ────────────────────────────────────────────────────────────────

def _build_pdf(brand: dict, concepts: list[dict], batch_id: str, out_path: Path) -> None:
    doc = SimpleDocTemplate(
        str(out_path),
        pagesize=A4,
        leftMargin=20 * mm,
        rightMargin=20 * mm,
        topMargin=20 * mm,
        bottomMargin=20 * mm,
    )

    story = []

    # Cover page
    story.append(Spacer(1, 30 * mm))
    story.append(Paragraph(
        f"Creative Sprint Concepts",
        ParagraphStyle("cover_title", fontSize=28, textColor=_DARK, fontName="Helvetica-Bold",
                       alignment=1, spaceAfter=6),
    ))
    story.append(Paragraph(
        brand["name"],
        ParagraphStyle("cover_brand", fontSize=18, textColor=_ACCENT, fontName="Helvetica-Bold",
                       alignment=1, spaceAfter=4),
    ))
    story.append(Paragraph(
        f"Batch: {batch_id} &nbsp;|&nbsp; {len(concepts)} concepts &nbsp;|&nbsp; "
        f"{datetime.now().strftime('%d %b %Y')}",
        ParagraphStyle("cover_meta", fontSize=10, textColor=colors.grey, alignment=1),
    ))
    story.append(PageBreak())

    # One concept per section
    for i, concept in enumerate(concepts, 1):
        story += _concept_section(i, concept)
        if i < len(concepts):
            story.append(HRFlowable(width="100%", thickness=1, color=_LIGHT))
            story.append(Spacer(1, 6 * mm))

    doc.build(story)


def _concept_section(index: int, concept: dict) -> list:
    body: list = []

    body.append(Paragraph(
        f"Concept {index}: {concept.get('hook_text', '')}",
        ParagraphStyle(f"ch{index}", fontSize=14, textColor=_DARK, fontName="Helvetica-Bold",
                       spaceBefore=4, spaceAfter=3),
    ))

    meta_data = [[
        concept.get("creative_type", "—").title(),
        concept.get("psychological_angle", "—").replace("_", " ").title(),
        concept.get("target_emotion", "—").title() if concept.get("target_emotion") else "—",
    ]]
    meta_table = Table(
        [["Format", "Psychological Angle", "Target Emotion"]] + meta_data,
        colWidths=[55 * mm, 70 * mm, 45 * mm],
    )
    meta_table.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0), _DARK),
        ("TEXTCOLOR",     (0, 0), (-1, 0), colors.white),
        ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, -1), 9),
        ("GRID",          (0, 0), (-1, -1), 0.5, colors.lightgrey),
        ("TOPPADDING",    (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("BACKGROUND",    (0, 1), (-1, -1), _LIGHT),
    ]))
    body.append(meta_table)
    body.append(Spacer(1, 4 * mm))

    label_style = ParagraphStyle("lbl", fontSize=9, textColor=colors.grey,
                                 fontName="Helvetica-Bold", spaceAfter=1)
    text_style  = ParagraphStyle("txt", fontSize=10, textColor=_DARK,
                                 leading=14, spaceAfter=5, leftIndent=3 * mm)

    if concept.get("body_script"):
        body.append(Paragraph("BODY / SCRIPT", label_style))
        body.append(Paragraph(concept["body_script"], text_style))

    if concept.get("visual_direction"):
        body.append(Paragraph("VISUAL DIRECTION", label_style))
        body.append(Paragraph(concept["visual_direction"], text_style))

    ctas = json.loads(concept["cta_variations_json"]) if concept.get("cta_variations_json") else []
    if ctas:
        body.append(Paragraph("CTA OPTIONS", label_style))
        body.append(Paragraph(" &nbsp;/&nbsp; ".join(ctas), text_style))

    if concept.get("why_it_will_work"):
        body.append(Paragraph("WHY IT WILL WORK", label_style))
        body.append(Paragraph(f"<i>{concept['why_it_will_work']}</i>", text_style))

    return body


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _fetch_brand(name: str) -> dict | None:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM brands WHERE name = ?", (name,)).fetchone()
    return dict(row) if row else None


def _fetch_concepts(brand_id: int, batch_id: str) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM creative_concepts WHERE client_brand_id = ? AND batch_id = ?",
            (brand_id, batch_id),
        ).fetchall()
    return [dict(r) for r in rows]
