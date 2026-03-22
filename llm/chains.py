"""
llm/chains.py — Orchestrate multi-step LLM prompt chains.

Each chain loads prompts from llm/prompts/*.txt, calls llm/client.py,
saves results to the database AND to data/processed/ as JSON files.
"""

import json
import logging
import uuid
from pathlib import Path
from string import Template

from config import PROC_DIR, get_connection
from llm.client import analyze_ad, batch_analyze, generate_text
from scrapers.utils import safe_brand_slug

logger = logging.getLogger(__name__)

_PROMPTS_DIR = Path(__file__).parent / "prompts"


# ── Public chain functions ────────────────────────────────────────────────────


def chain_competitor_analysis(brand_name: str, model: str = "claude") -> list[dict]:
    """
    Run competitor_deconstruction on all competitor ads for *brand_name*.
    Stores each result in ad_analysis and writes combined JSON to data/processed/.
    Returns list of analysis dicts.
    """
    conn = get_connection()
    try:
        # Find the client brand
        brand = conn.execute(
            "SELECT id, category FROM brands WHERE name = ? AND is_client = 1",
            (brand_name,),
        ).fetchone()
        if not brand:
            raise ValueError(f"Client brand '{brand_name}' not found in database")

        # Get all competitor ads
        competitor_ads = conn.execute(
            """
            SELECT a.id AS ad_id, a.ad_library_id, a.ad_copy, a.image_path,
                   a.creative_type, a.duration_days, b.name AS competitor_name
            FROM ads a
            JOIN brands b ON a.brand_id = b.id
            JOIN competitor_sets cs ON cs.competitor_brand_id = b.id
            WHERE cs.client_brand_id = ?
              AND a.is_active = 1
            """,
            (brand["id"],),
        ).fetchall()

        if not competitor_ads:
            logger.warning("No competitor ads found for '%s'", brand_name)
            return []

        logger.info(
            "Running competitor analysis: %d ads for '%s'",
            len(competitor_ads), brand_name,
        )

        system_prompt = _load_prompt("competitor_deconstruction.txt").safe_substitute(
            brand_name=brand_name,
            category=brand["category"] or "",
            ad_library_id="",  # will be set per-ad in the prompt
        )

        results = []
        for row in competitor_ads:
            ad = dict(row)
            # Customise system prompt with this ad's ID
            ad_system = system_prompt.replace(
                '"ad_library_id": ""',
                f'"ad_library_id": "{ad["ad_library_id"]}"',
            )

            try:
                analysis = analyze_ad(
                    image_path=ad.get("image_path") or "",
                    ad_copy=ad.get("ad_copy") or "",
                    system_prompt=ad_system,
                    model=model,
                )
                analysis["ad_id"] = ad["ad_id"]
                analysis["ad_library_id"] = ad["ad_library_id"]
                analysis["competitor_name"] = ad["competitor_name"]

                # Write to ad_analysis table
                _save_ad_analysis(conn, ad["ad_id"], analysis)
                results.append(analysis)

            except Exception as exc:
                logger.error(
                    "Failed to analyse ad %s: %s", ad["ad_library_id"], exc
                )
                results.append({
                    "ad_library_id": ad["ad_library_id"],
                    "error": str(exc),
                })

        conn.commit()
    finally:
        conn.close()

    # Save to processed JSON
    _save_json(brand_name, "competitor_analysis", results)
    logger.info("Competitor analysis complete: %d results", len(results))
    return results


def chain_waste_diagnosis(
    client_brand_name: str, model: str = "claude"
) -> dict:
    """
    Run waste diagnosis using client ad data + competitor benchmarks.
    Stores result in waste_reports and writes JSON to data/processed/.
    Returns the waste diagnosis dict.
    """
    conn = get_connection()
    try:
        brand = conn.execute(
            "SELECT id, name, category FROM brands WHERE name = ? AND is_client = 1",
            (client_brand_name,),
        ).fetchone()
        if not brand:
            raise ValueError(
                f"Client brand '{client_brand_name}' not found in database"
            )

        # Gather client ad data with fatigue info
        client_ads = conn.execute(
            """
            SELECT a.ad_library_id, a.creative_type, a.duration_days,
                   a.ad_copy, a.is_active,
                   aa.psychological_trigger, aa.effectiveness_score
            FROM ads a
            LEFT JOIN ad_analysis aa ON aa.ad_id = a.id
            WHERE a.brand_id = ?
            """,
            (brand["id"],),
        ).fetchall()

        # Gather competitor benchmark data (aggregated from ad_analysis)
        competitor_benchmarks = conn.execute(
            """
            SELECT b.name AS competitor_name,
                   aa.psychological_trigger,
                   aa.copy_tone,
                   a.creative_type,
                   COUNT(*) AS ad_count,
                   AVG(aa.effectiveness_score) AS avg_effectiveness
            FROM ads a
            JOIN brands b ON a.brand_id = b.id
            JOIN competitor_sets cs ON cs.competitor_brand_id = b.id
            JOIN ad_analysis aa ON aa.ad_id = a.id
            WHERE cs.client_brand_id = ?
            GROUP BY b.name, aa.psychological_trigger, aa.copy_tone, a.creative_type
            """,
            (brand["id"],),
        ).fetchall()

        # Load the latest waste_report diversity score if available
        existing_report = conn.execute(
            """
            SELECT creative_diversity_score FROM waste_reports
            WHERE client_brand_id = ?
            ORDER BY generated_at DESC LIMIT 1
            """,
            (brand["id"],),
        ).fetchone()

        client_data = {
            "brand_name": client_brand_name,
            "ads": [dict(r) for r in client_ads],
            "existing_diversity_score": (
                existing_report["creative_diversity_score"]
                if existing_report
                else None
            ),
        }
        benchmarks = [dict(r) for r in competitor_benchmarks]

        prompt_template = _load_prompt("waste_diagnosis.txt")
        prompt = prompt_template.safe_substitute(
            brand_name=client_brand_name,
            category=brand["category"] or "",
            client_data=json.dumps(client_data, ensure_ascii=False, indent=2),
            competitor_benchmarks=json.dumps(benchmarks, ensure_ascii=False, indent=2),
        )

        logger.info("Running waste diagnosis for '%s'", client_brand_name)
        result = generate_text(prompt=prompt, model=model)

        # Store in waste_reports
        _save_waste_report(conn, brand["id"], result)
        conn.commit()
    finally:
        conn.close()

    _save_json(client_brand_name, "waste_diagnosis", result)
    logger.info("Waste diagnosis complete for '%s'", client_brand_name)
    return result


def chain_concept_generation(
    client_brand_name: str,
    num_concepts: int = 50,
    model: str = "claude",
) -> list[dict]:
    """
    Generate ad concepts using brand context + competitor intel + waste diagnosis.
    Stores each concept in creative_concepts and writes JSON to data/processed/.
    Returns list of concept dicts.
    """
    conn = get_connection()
    try:
        brand = conn.execute(
            "SELECT id, name, category, website_url FROM brands "
            "WHERE name = ? AND is_client = 1",
            (client_brand_name,),
        ).fetchone()
        if not brand:
            raise ValueError(
                f"Client brand '{client_brand_name}' not found in database"
            )

        # Load competitor analysis from DB
        competitor_intel = conn.execute(
            """
            SELECT aa.analysis_json, aa.psychological_trigger, aa.visual_layout,
                   aa.copy_tone, a.ad_library_id, a.duration_days,
                   b.name AS competitor_name
            FROM ad_analysis aa
            JOIN ads a ON aa.ad_id = a.id
            JOIN brands b ON a.brand_id = b.id
            JOIN competitor_sets cs ON cs.competitor_brand_id = b.id
            WHERE cs.client_brand_id = ?
            ORDER BY aa.analyzed_at DESC
            """,
            (brand["id"],),
        ).fetchall()

        # Load latest waste diagnosis from DB
        waste_report = conn.execute(
            """
            SELECT recommendations_json, fatigue_flags_json, format_mix_json
            FROM waste_reports
            WHERE client_brand_id = ?
            ORDER BY generated_at DESC LIMIT 1
            """,
            (brand["id"],),
        ).fetchone()

        # Load brand's existing ads for differentiation context
        brand_ads = conn.execute(
            """
            SELECT a.ad_copy, a.creative_type, aa.psychological_trigger
            FROM ads a
            LEFT JOIN ad_analysis aa ON aa.ad_id = a.id
            WHERE a.brand_id = ? AND a.is_active = 1
            """,
            (brand["id"],),
        ).fetchall()

        brand_context = {
            "name": client_brand_name,
            "category": brand["category"],
            "website": brand["website_url"],
            "existing_ads": [dict(r) for r in brand_ads],
        }
        intel = [dict(r) for r in competitor_intel]
        waste = dict(waste_report) if waste_report else {}

        prompt_template = _load_prompt("concept_generation.txt")
        prompt = prompt_template.safe_substitute(
            brand_name=client_brand_name,
            category=brand["category"] or "",
            num_concepts=num_concepts,
            brand_context=json.dumps(brand_context, ensure_ascii=False, indent=2),
            competitor_intel=json.dumps(intel, ensure_ascii=False, indent=2),
            waste_diagnosis=json.dumps(waste, ensure_ascii=False, indent=2),
        )

        logger.info(
            "Generating %d concepts for '%s'", num_concepts, client_brand_name
        )
        result = generate_text(prompt=prompt, model=model)

        if not isinstance(result, list):
            result = [result]

        # Store in creative_concepts
        batch_id = str(uuid.uuid4())[:8]
        _save_concepts(conn, brand["id"], batch_id, result)
        conn.commit()
    finally:
        conn.close()

    _save_json(client_brand_name, "concepts", result)
    logger.info(
        "Concept generation complete: %d concepts, batch=%s",
        len(result), batch_id,
    )
    return result


def chain_full(client_brand_name: str, num_concepts: int = 50, model: str = "claude") -> dict:
    """
    Run all three chains in sequence:
    1. Competitor analysis
    2. Waste diagnosis
    3. Concept generation

    Returns dict with keys: competitor_analysis, waste_diagnosis, concepts.
    """
    logger.info("Starting full chain for '%s'", client_brand_name)

    competitor_analysis = chain_competitor_analysis(client_brand_name, model=model)
    waste_diagnosis = chain_waste_diagnosis(client_brand_name, model=model)
    concepts = chain_concept_generation(
        client_brand_name, num_concepts=num_concepts, model=model
    )

    full_output = {
        "brand": client_brand_name,
        "competitor_analysis": competitor_analysis,
        "waste_diagnosis": waste_diagnosis,
        "concepts": concepts,
    }

    _save_json(client_brand_name, "full_chain", full_output)
    logger.info("Full chain complete for '%s'", client_brand_name)
    return full_output


# ── Database helpers ──────────────────────────────────────────────────────────


def _save_ad_analysis(conn, ad_id: int, analysis: dict) -> None:
    """Insert or replace an ad_analysis row."""
    conn.execute(
        """
        INSERT INTO ad_analysis (
            ad_id, psychological_trigger, visual_layout, copy_tone,
            reading_level, color_palette_json, is_profitable, analysis_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ad_id,
            analysis.get("psychological_trigger"),
            analysis.get("visual_layout"),
            analysis.get("copy_tone"),
            analysis.get("reading_level"),
            json.dumps(analysis.get("color_palette", []), ensure_ascii=False),
            1 if analysis.get("effectiveness_score", 0) >= 7 else 0,
            json.dumps(analysis, ensure_ascii=False),
        ),
    )


def _save_waste_report(conn, brand_id: int, report: dict) -> None:
    """Insert a waste_reports row."""
    conn.execute(
        """
        INSERT INTO waste_reports (
            client_brand_id, creative_diversity_score, format_mix_json,
            avg_refresh_days, fatigue_flags_json, recommendations_json
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            brand_id,
            report.get("diversity_score_interpretation", {}).get("score"),
            json.dumps(report.get("format_gaps", []), ensure_ascii=False),
            None,  # avg_refresh_days computed from fatigue_diagnosis if needed
            json.dumps(report.get("fatigue_diagnosis", []), ensure_ascii=False),
            json.dumps(report.get("priority_actions", []), ensure_ascii=False),
        ),
    )


def _save_concepts(conn, brand_id: int, batch_id: str, concepts: list[dict]) -> None:
    """Insert creative_concepts rows for a batch."""
    for concept in concepts:
        conn.execute(
            """
            INSERT INTO creative_concepts (
                client_brand_id, batch_id, hook_text, body_script,
                visual_direction, cta_variations_json, psychological_angle
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                brand_id,
                batch_id,
                concept.get("hook"),
                concept.get("body_script"),
                concept.get("visual_direction"),
                json.dumps(
                    concept.get("cta_variations", []), ensure_ascii=False
                ),
                concept.get("psychological_angle"),
            ),
        )


# ── File helpers ──────────────────────────────────────────────────────────────


def _load_prompt(filename: str) -> Template:
    path = _PROMPTS_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Prompt file not found: {path}")
    return Template(path.read_text(encoding="utf-8"))


def _save_json(brand_name: str, stage: str, data) -> None:
    """Write chain output to data/processed/{slug}_{stage}.json."""
    slug = safe_brand_slug(brand_name)
    out_dir = PROC_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{slug}_{stage}.json"
    out_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info("Saved %s → %s", stage, out_path)
