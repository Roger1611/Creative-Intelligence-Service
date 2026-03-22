"""
llm/chains.py — Orchestrate multi-step LLM prompt chains.

Each chain loads its prompt template from llm/prompts/*.txt,
fills variables, calls llm/client.py, and returns structured JSON.
"""

import logging
from pathlib import Path
from string import Template

from llm.client import complete

logger = logging.getLogger(__name__)

_PROMPTS_DIR = Path(__file__).parent / "prompts"


def run_competitor_deconstruction(ads: list[dict], brand_context: dict) -> list[dict]:
    """
    Analyse a list of competitor ads and return structured creative breakdowns.

    Each item in *ads* should include: ad_copy, creative_type, image_path (optional).

    Returns list of analysis dicts matching the ad_analysis schema.
    """
    prompt_template = _load_prompt("competitor_deconstruction.txt")
    prompt = prompt_template.substitute(
        brand_name=brand_context.get("name", ""),
        category=brand_context.get("category", ""),
        ads_json=_serialise(ads),
    )

    system = (
        "You are an expert direct-response advertising analyst specialising in "
        "Indian D2C brands. Always respond with valid JSON only — no markdown, "
        "no commentary outside the JSON structure."
    )

    images = [ad["image_path"] for ad in ads if ad.get("image_path")]

    logger.info("Running competitor_deconstruction chain for %d ads", len(ads))
    result = complete(prompt, system=system, images=images or None)

    if not isinstance(result, list):
        result = [result]
    return result


def run_waste_diagnosis(fatigue_data: dict, brand_context: dict) -> dict:
    """
    Diagnose ad waste from fatigue scorer output and return recommendations.

    Returns a dict matching the waste_reports schema.
    """
    prompt_template = _load_prompt("waste_diagnosis.txt")
    prompt = prompt_template.substitute(
        brand_name=brand_context.get("name", ""),
        category=brand_context.get("category", ""),
        fatigue_json=_serialise(fatigue_data),
    )

    system = (
        "You are a performance marketing auditor. Diagnose creative waste and "
        "return actionable recommendations as valid JSON only."
    )

    logger.info("Running waste_diagnosis chain for brand '%s'", brand_context.get("name"))
    return complete(prompt, system=system)


def run_concept_generation(
    client_brand: dict,
    competitor_analyses: list[dict],
    num_concepts: int = 10,
) -> list[dict]:
    """
    Generate original ad creative concepts informed by competitor intelligence.

    Returns list of concept dicts matching the creative_concepts schema.
    """
    prompt_template = _load_prompt("concept_generation.txt")
    prompt = prompt_template.substitute(
        brand_name=client_brand.get("name", ""),
        category=client_brand.get("category", ""),
        brand_usp=client_brand.get("usp", ""),
        competitor_intel=_serialise(competitor_analyses),
        num_concepts=num_concepts,
    )

    system = (
        "You are a senior creative strategist for Indian D2C performance marketing. "
        "Generate scroll-stopping ad concepts grounded in psychological triggers. "
        "Return valid JSON only."
    )

    logger.info(
        "Running concept_generation chain — %d concepts for '%s'",
        num_concepts, client_brand.get("name"),
    )
    result = complete(prompt, system=system)

    if not isinstance(result, list):
        result = [result]
    return result[:num_concepts]


# ── Helpers ────────────────────────────────────────────────────────────────────

def _load_prompt(filename: str) -> Template:
    path = _PROMPTS_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Prompt file not found: {path}")
    return Template(path.read_text(encoding="utf-8"))


def _serialise(obj) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False, indent=2)
