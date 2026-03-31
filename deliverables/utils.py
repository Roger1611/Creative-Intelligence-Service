"""
deliverables/utils.py — Shared helpers for PDF deliverable generators.
"""

import json
import logging
from pathlib import Path

from reportlab.lib import colors

logger = logging.getLogger(__name__)


def format_inr(amount: float, prefix: str = "\u20b9") -> str:
    """Format a number in Indian numbering system.

    The Indian system groups the last 3 digits together, then every 2 digits
    after that (thousands, then lakhs, then crores).

    Examples:
        format_inr(1234)       -> "₹1,234"
        format_inr(123456)     -> "₹1,23,456"
        format_inr(12345678)   -> "₹1,23,45,678"
        format_inr(0)          -> "₹0"
        format_inr(500, "$")   -> "$500"
    """
    if amount < 0:
        return f"-{format_inr(-amount, prefix)}"
    amount = round(amount)
    s = str(amount)
    if len(s) <= 3:
        return f"{prefix}{s}"
    # Last 3 digits, then groups of 2
    last3 = s[-3:]
    rest = s[:-3]
    parts = []
    while rest:
        parts.append(rest[-2:])
        rest = rest[:-2]
    parts.reverse()
    return f"{prefix}{','.join(parts)},{last3}"


def format_inr_short(amount: float) -> str:
    """Short format for large amounts using Indian units (K/L/Cr).

    Examples:
        format_inr_short(500)        -> "₹500"
        format_inr_short(5000)       -> "₹5K"
        format_inr_short(50000)      -> "₹50K"
        format_inr_short(150000)     -> "₹1.5L"
        format_inr_short(100000)     -> "₹1L"
        format_inr_short(10000000)   -> "₹1Cr"
        format_inr_short(25000000)   -> "₹2.5Cr"
        format_inr_short(0)          -> "₹0"
    """
    prefix = "\u20b9"
    if amount < 0:
        return f"-{format_inr_short(-amount)}"
    amount = round(amount)
    if amount == 0:
        return f"{prefix}0"
    for threshold, suffix in [
        (1_00_00_000, "Cr"),
        (1_00_000, "L"),
        (1_000, "K"),
    ]:
        if amount >= threshold:
            val = amount / threshold
            if val == int(val):
                return f"{prefix}{int(val)}{suffix}"
            return f"{prefix}{val:.1f}{suffix}"
    return f"{prefix}{amount}"


# ── Severity / confidence colour helpers ─────────────────────────────────────

_SEVERITY_COLORS = {
    "LOW":      colors.HexColor("#43A047"),
    "MODERATE": colors.HexColor("#F57C00"),
    "HIGH":     colors.HexColor("#E53935"),
    "CRITICAL": colors.HexColor("#B71C1C"),
}


def severity_color(severity: str) -> colors.HexColor:
    """Map severity strings to colours for consistent PDF styling.

    Returns a grey default for unknown severity values.
    """
    return _SEVERITY_COLORS.get(
        severity.upper() if severity else "",
        colors.HexColor("#6B7280"),
    )


_CONFIDENCE_LABELS = {
    "low":    "Low confidence \u2014 directional estimate",
    "medium": "Medium confidence \u2014 based on partial data",
    "high":   "High confidence \u2014 strong data signal",
}


def confidence_badge_text(confidence: str) -> str:
    """Map confidence level to display text for PDF badges."""
    return _CONFIDENCE_LABELS.get(
        confidence.lower() if confidence else "",
        confidence or "",
    )


# ── JSON loading ─────────────────────────────────────────────────────────────

def load_json(path: Path, label: str) -> dict:
    """Load a JSON file, returning {} on missing/corrupt."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        logger.warning("Could not load %s data from %s", label, path)
        return {}
