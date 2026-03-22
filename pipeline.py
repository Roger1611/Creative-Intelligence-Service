"""
pipeline.py — Main orchestrator for the D2C Creative Intelligence pipeline.

Modes:
  audit       — free Creative Waste Audit PDF (lead-gen)
  sprint      — paid deliverable: N creative concepts + PDF
  batch-audit — run audit for multiple brands from a CSV
  refresh     — re-scrape and update existing brand data
"""

import argparse
import csv
import logging
import sys
import uuid
from pathlib import Path

import config  # triggers load_dotenv and exposes constants

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="pipeline",
        description="D2C Creative Intelligence Pipeline",
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    # audit
    p_audit = sub.add_parser("audit", help="Generate a free Creative Waste Audit PDF")
    p_audit.add_argument("--brand",       required=True)
    p_audit.add_argument("--competitors", default="",
                         help="Comma-separated competitor brand names")
    p_audit.add_argument("--category",    choices=config.VALID_CATEGORIES)
    p_audit.add_argument("--output",      default="audits")

    # sprint
    p_sprint = sub.add_parser("sprint", help="Generate N creative concepts")
    p_sprint.add_argument("--brand",        required=True)
    p_sprint.add_argument("--competitors",  default="")
    p_sprint.add_argument("--category",     choices=config.VALID_CATEGORIES)
    p_sprint.add_argument("--num-concepts", type=int, default=10)
    p_sprint.add_argument("--output",       default="sprints")

    # batch-audit
    p_batch = sub.add_parser("batch-audit", help="Run audit for brands listed in a CSV")
    p_batch.add_argument("--brands-file", required=True,
                         help="CSV with columns: brand, competitors, category")
    p_batch.add_argument("--category",    choices=config.VALID_CATEGORIES)
    p_batch.add_argument("--output",      default="audits")

    # refresh
    p_refresh = sub.add_parser("refresh", help="Re-scrape and update an existing brand")
    p_refresh.add_argument("--brand", required=True)

    args = parser.parse_args()
    config.init_db()

    if args.mode == "audit":
        _run_audit(
            brand_name=args.brand,
            competitors=_split(args.competitors),
            category=args.category,
            output=args.output,
        )

    elif args.mode == "sprint":
        _run_sprint(
            brand_name=args.brand,
            competitors=_split(args.competitors),
            category=args.category,
            num_concepts=args.num_concepts,
            output=args.output,
        )

    elif args.mode == "batch-audit":
        _run_batch_audit(
            brands_file=args.brands_file,
            default_category=args.category,
            output=args.output,
        )

    elif args.mode == "refresh":
        _run_refresh(brand_name=args.brand)


# ── Mode implementations ───────────────────────────────────────────────────────

def _run_audit(brand_name: str, competitors: list[str], category, output: str) -> None:
    from scrapers import meta_ad_library
    from analysis import structurer, profitability_filter, fatigue_scorer
    from llm import chains
    from deliverables import audit_generator

    logger.info("=== AUDIT: %s ===", brand_name)

    # 1. Scrape
    raw = meta_ad_library.run(brand_name, competitors)

    # 2. Ingest into DB
    brand_id = structurer.ingest(brand_name, raw["brand"], is_client=True, category=category)
    for comp_name, comp_ads in raw["competitors"].items():
        comp_id = structurer.ingest(comp_name, comp_ads, category=category)
        _ensure_competitor_set(brand_id, comp_id)

    # 3. Analysis pipeline
    structurer.run(brand_name, competitors)
    profitability_filter.run(brand_name, competitors)
    fatigue_data = fatigue_scorer.run(brand_name, competitors)

    # 4. LLM waste diagnosis
    brand_context = {"name": brand_name, "category": category}
    waste_report  = chains.run_waste_diagnosis(fatigue_data, brand_context)
    _save_waste_report(brand_id, fatigue_data, waste_report)

    # 5. Generate PDF
    pdf_path = audit_generator.run(brand_name, output_dir=output)
    print(f"\nAudit PDF ready: {pdf_path}")


def _run_sprint(
    brand_name: str,
    competitors: list[str],
    category,
    num_concepts: int,
    output: str,
) -> None:
    from scrapers import meta_ad_library
    from analysis import structurer, profitability_filter
    from llm import chains
    from deliverables import sprint_generator

    logger.info("=== SPRINT: %s (%d concepts) ===", brand_name, num_concepts)

    # 1. Scrape
    raw = meta_ad_library.run(brand_name, competitors)

    # 2. Ingest + analysis
    brand_id = structurer.ingest(brand_name, raw["brand"], is_client=True, category=category)
    all_comp_ads: list[dict] = []
    for comp_name, comp_ads in raw["competitors"].items():
        comp_id = structurer.ingest(comp_name, comp_ads, category=category)
        _ensure_competitor_set(brand_id, comp_id)
        all_comp_ads.extend(comp_ads)
    structurer.run(brand_name, competitors)
    profitability_filter.run(brand_name, competitors)

    # 3. Deconstruct competitor ads
    brand_context      = {"name": brand_name, "category": category}
    competitor_analyses = chains.run_competitor_deconstruction(all_comp_ads, brand_context)

    # 4. Generate concepts
    concepts = chains.run_concept_generation(
        brand_context, competitor_analyses, num_concepts=num_concepts
    )
    batch_id = _save_concepts(brand_id, concepts)

    # 5. Generate PDF
    pdf_path = sprint_generator.run(brand_name, batch_id, output_dir=output)
    print(f"\nSprint PDF ready: {pdf_path}")


def _run_batch_audit(brands_file: str, default_category, output: str) -> None:
    path = Path(brands_file)
    if not path.exists():
        logger.error("Brands file not found: %s", brands_file)
        sys.exit(1)

    with path.open(encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            brand       = row.get("brand", "").strip()
            competitors = _split(row.get("competitors", ""))
            category    = row.get("category", "").strip() or default_category
            if not brand:
                continue
            try:
                _run_audit(brand, competitors, category, output)
            except Exception as exc:
                logger.error("Audit failed for '%s': %s", brand, exc)


def _run_refresh(brand_name: str) -> None:
    from scrapers import meta_ad_library
    from analysis import structurer

    logger.info("=== REFRESH: %s ===", brand_name)

    with config.get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM brands WHERE name = ?", (brand_name,)
        ).fetchone()

    if not row:
        logger.error("Brand '%s' not in database. Run audit first.", brand_name)
        sys.exit(1)

    competitors = _fetch_competitor_names(row["id"])
    raw = meta_ad_library.run(brand_name, competitors)
    structurer.ingest(brand_name, raw["brand"])
    logger.info("Refresh complete for '%s'", brand_name)


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _ensure_competitor_set(client_id: int, competitor_id: int) -> None:
    with config.get_connection() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO competitor_sets (client_brand_id, competitor_brand_id)
               VALUES (?, ?)""",
            (client_id, competitor_id),
        )


def _save_waste_report(brand_id: int, fatigue_data: dict, llm_report: dict) -> None:
    import json
    diversity = llm_report.get(
        "creative_diversity_score",
        fatigue_data.get("creative_diversity_score", 0),
    )
    with config.get_connection() as conn:
        conn.execute(
            """INSERT INTO waste_reports (
                   client_brand_id, creative_diversity_score, format_mix_json,
                   avg_refresh_days, fatigue_flags_json, recommendations_json
               ) VALUES (?, ?, ?, ?, ?, ?)""",
            (
                brand_id,
                diversity,
                json.dumps(llm_report.get("format_mix_json", fatigue_data.get("format_mix", {}))),
                llm_report.get("avg_refresh_days", fatigue_data.get("avg_refresh_days", 0)),
                json.dumps(llm_report.get("fatigue_flags_json", fatigue_data.get("fatigue_flags", []))),
                json.dumps(llm_report.get("recommendations_json", [])),
            ),
        )


def _save_concepts(brand_id: int, concepts: list[dict]) -> str:
    import json
    batch_id = str(uuid.uuid4())[:8]
    with config.get_connection() as conn:
        for c in concepts:
            conn.execute(
                """INSERT INTO creative_concepts (
                       client_brand_id, batch_id, hook_text, body_script,
                       visual_direction, cta_variations_json, psychological_angle
                   ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    brand_id, batch_id,
                    c.get("hook_text"),
                    c.get("body_script"),
                    c.get("visual_direction"),
                    json.dumps(c.get("cta_variations_json", [])),
                    c.get("psychological_angle"),
                ),
            )
    logger.info("Saved %d concepts — batch_id=%s", len(concepts), batch_id)
    return batch_id


def _fetch_competitor_names(client_brand_id: int) -> list[str]:
    with config.get_connection() as conn:
        rows = conn.execute(
            """SELECT b.name FROM brands b
               JOIN competitor_sets cs ON cs.competitor_brand_id = b.id
               WHERE cs.client_brand_id = ?""",
            (client_brand_id,),
        ).fetchall()
    return [r["name"] for r in rows]


def _split(s: str) -> list[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


if __name__ == "__main__":
    main()
