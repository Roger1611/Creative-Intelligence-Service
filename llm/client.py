"""
llm/client.py — Thin wrapper around Anthropic (primary) and OpenAI (fallback).

Every call logs model, token usage, and estimated cost to stdout.
All responses are parsed as JSON — callers always get a dict or list back.
"""

import json
import logging
from typing import Any

import anthropic
import openai

from config import (
    ANTHROPIC_API_KEY,
    ANTHROPIC_MODEL,
    LLM_MAX_TOKENS,
    LLM_TEMPERATURE,
    OPENAI_API_KEY,
    OPENAI_MODEL,
)

logger = logging.getLogger(__name__)

# Approximate cost per 1M tokens (USD) — update as pricing changes
_ANTHROPIC_COST = {"input": 15.00, "output": 75.00}   # claude-opus-4-6
_OPENAI_COST    = {"input":  5.00, "output": 15.00}    # gpt-4o


def complete(
    prompt: str,
    system: str = "",
    images: list[str] | None = None,   # list of local file paths or URLs
    use_fallback: bool = True,
) -> Any:
    """
    Send a completion request. Returns parsed JSON (dict or list).

    Tries Anthropic first; falls back to OpenAI if *use_fallback* is True
    and the primary call fails.
    """
    try:
        return _anthropic_complete(prompt, system, images)
    except Exception as exc:
        logger.warning("Anthropic call failed: %s", exc)
        if not use_fallback:
            raise
        logger.info("Falling back to OpenAI")
        return _openai_complete(prompt, system)


# ── Anthropic ──────────────────────────────────────────────────────────────────

def _anthropic_complete(prompt: str, system: str, images: list[str] | None) -> Any:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    content: list[dict] = []

    if images:
        for path_or_url in images:
            content.append(_anthropic_image_block(path_or_url))

    content.append({"type": "text", "text": prompt})

    kwargs: dict = {
        "model":      ANTHROPIC_MODEL,
        "max_tokens": LLM_MAX_TOKENS,
        "messages":   [{"role": "user", "content": content}],
    }
    if system:
        kwargs["system"] = system

    response = client.messages.create(**kwargs)

    in_tok  = response.usage.input_tokens
    out_tok = response.usage.output_tokens
    cost    = _estimate_cost(in_tok, out_tok, _ANTHROPIC_COST)

    logger.info(
        "[Anthropic] model=%s in=%d out=%d tokens est_cost=$%.4f",
        ANTHROPIC_MODEL, in_tok, out_tok, cost,
    )

    raw = response.content[0].text
    return _parse_json(raw)


def _anthropic_image_block(path_or_url: str) -> dict:
    import base64
    from pathlib import Path

    if path_or_url.startswith("http"):
        return {
            "type": "image",
            "source": {"type": "url", "url": path_or_url},
        }
    data = base64.standard_b64encode(Path(path_or_url).read_bytes()).decode()
    return {
        "type": "image",
        "source": {
            "type":       "base64",
            "media_type": "image/jpeg",
            "data":       data,
        },
    }


# ── OpenAI ─────────────────────────────────────────────────────────────────────

def _openai_complete(prompt: str, system: str) -> Any:
    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=messages,
        max_tokens=LLM_MAX_TOKENS,
        temperature=LLM_TEMPERATURE,
    )

    in_tok  = response.usage.prompt_tokens
    out_tok = response.usage.completion_tokens
    cost    = _estimate_cost(in_tok, out_tok, _OPENAI_COST)

    logger.info(
        "[OpenAI] model=%s in=%d out=%d tokens est_cost=$%.4f",
        OPENAI_MODEL, in_tok, out_tok, cost,
    )

    raw = response.choices[0].message.content
    return _parse_json(raw)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _parse_json(text: str) -> Any:
    """Extract and parse the first JSON object or array from *text*."""
    text = text.strip()
    # Strip markdown code fences if present
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        logger.error("JSON parse failed: %s\nRaw: %.200s", exc, text)
        raise


def _estimate_cost(in_tokens: int, out_tokens: int, rates: dict) -> float:
    return (in_tokens / 1_000_000 * rates["input"]) + (out_tokens / 1_000_000 * rates["output"])
