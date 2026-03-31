"""Tests for deliverables/utils.py — shared formatting helpers."""

import json
from pathlib import Path

import pytest

from deliverables.utils import (
    format_inr,
    format_inr_short,
    severity_color,
    confidence_badge_text,
    load_json,
)


# ══════════════════════════════════════════════════════════════════════════════
# format_inr — Indian comma formatting
# ══════════════════════════════════════════════════════════════════════════════


class TestFormatInr:

    # ── Basic values ──────────────────────────────────────────────────────

    def test_zero(self):
        assert format_inr(0) == "\u20b90"

    def test_single_digit(self):
        assert format_inr(5) == "\u20b95"

    def test_hundreds(self):
        assert format_inr(999) == "\u20b9999"

    def test_thousands(self):
        assert format_inr(1234) == "\u20b91,234"

    def test_exact_thousand(self):
        assert format_inr(1000) == "\u20b91,000"

    # ── Indian comma placement (NOT Western) ──────────────────────────────

    def test_lakhs_indian_format(self):
        """₹1,23,456 NOT ₹123,456 — this is the key Indian formatting rule."""
        assert format_inr(123456) == "\u20b91,23,456"

    def test_exact_lakh(self):
        assert format_inr(100000) == "\u20b91,00,000"

    def test_ten_lakhs(self):
        assert format_inr(1234567) == "\u20b912,34,567"

    def test_crore(self):
        """₹1,23,45,678 NOT ₹12,345,678."""
        assert format_inr(12345678) == "\u20b91,23,45,678"

    def test_ten_crores(self):
        assert format_inr(123456789) == "\u20b912,34,56,789"

    def test_hundred_crores(self):
        assert format_inr(1234567890) == "\u20b91,23,45,67,890"

    # ── Edge cases ────────────────────────────────────────────────────────

    def test_negative(self):
        assert format_inr(-50000) == "-\u20b950,000"

    def test_float_rounds_up(self):
        assert format_inr(1234.7) == "\u20b91,235"

    def test_float_rounds_down(self):
        assert format_inr(1234.3) == "\u20b91,234"

    def test_very_large(self):
        # 100 crores
        result = format_inr(1_00_00_00_000)
        assert result == "\u20b91,00,00,00,000"

    # ── Custom prefix ─────────────────────────────────────────────────────

    def test_custom_prefix(self):
        assert format_inr(5000, prefix="Rs.") == "Rs.5,000"

    def test_empty_prefix(self):
        assert format_inr(1234, prefix="") == "1,234"


# ══════════════════════════════════════════════════════════════════════════════
# format_inr_short — compact display
# ══════════════════════════════════════════════════════════════════════════════


class TestFormatInrShort:

    def test_zero(self):
        assert format_inr_short(0) == "\u20b90"

    def test_below_thousand(self):
        assert format_inr_short(500) == "\u20b9500"

    def test_exact_thousand(self):
        assert format_inr_short(1000) == "\u20b91K"

    def test_thousands(self):
        assert format_inr_short(5000) == "\u20b95K"

    def test_tens_of_thousands(self):
        assert format_inr_short(50000) == "\u20b950K"

    def test_fractional_thousands(self):
        assert format_inr_short(1500) == "\u20b91.5K"

    # ── Lakhs ─────────────────────────────────────────────────────────────

    def test_exact_lakh(self):
        assert format_inr_short(100000) == "\u20b91L"

    def test_one_and_half_lakhs(self):
        assert format_inr_short(150000) == "\u20b91.5L"

    def test_multiple_lakhs(self):
        assert format_inr_short(500000) == "\u20b95L"

    def test_fractional_lakhs(self):
        assert format_inr_short(250000) == "\u20b92.5L"

    def test_99_lakhs(self):
        assert format_inr_short(9900000) == "\u20b999L"

    # ── Crores ────────────────────────────────────────────────────────────

    def test_exact_crore(self):
        assert format_inr_short(10000000) == "\u20b91Cr"

    def test_two_and_half_crores(self):
        assert format_inr_short(25000000) == "\u20b92.5Cr"

    def test_ten_crores(self):
        assert format_inr_short(100000000) == "\u20b910Cr"

    # ── Edge cases ────────────────────────────────────────────────────────

    def test_negative(self):
        assert format_inr_short(-150000) == "-\u20b91.5L"

    def test_rounds_to_nearest(self):
        # 1,49,999 rounds to 1,50,000 -> 1.5L
        result = format_inr_short(149999)
        assert "L" in result


# ══════════════════════════════════════════════════════════════════════════════
# severity_color
# ══════════════════════════════════════════════════════════════════════════════


class TestSeverityColor:

    def test_low_returns_green(self):
        c = severity_color("LOW")
        assert c.hexval() == "0x43a047"

    def test_moderate_returns_orange(self):
        c = severity_color("MODERATE")
        assert c.hexval() == "0xf57c00"

    def test_high_returns_red(self):
        c = severity_color("HIGH")
        assert c.hexval() == "0xe53935"

    def test_critical_returns_dark_red(self):
        c = severity_color("CRITICAL")
        assert c.hexval() == "0xb71c1c"

    def test_case_insensitive(self):
        assert severity_color("low").hexval() == severity_color("LOW").hexval()
        assert severity_color("High").hexval() == severity_color("HIGH").hexval()

    def test_unknown_returns_grey(self):
        c = severity_color("UNKNOWN")
        assert c.hexval() == "0x6b7280"

    def test_empty_string_returns_grey(self):
        c = severity_color("")
        assert c.hexval() == "0x6b7280"

    def test_none_returns_grey(self):
        c = severity_color(None)
        assert c.hexval() == "0x6b7280"


# ══════════════════════════════════════════════════════════════════════════════
# confidence_badge_text
# ══════════════════════════════════════════════════════════════════════════════


class TestConfidenceBadgeText:

    def test_low(self):
        assert "directional" in confidence_badge_text("low")

    def test_medium(self):
        assert "partial" in confidence_badge_text("medium")

    def test_high(self):
        assert "strong" in confidence_badge_text("high")

    def test_case_insensitive(self):
        assert confidence_badge_text("HIGH") == confidence_badge_text("high")

    def test_unknown_passes_through(self):
        assert confidence_badge_text("experimental") == "experimental"

    def test_empty_returns_empty(self):
        assert confidence_badge_text("") == ""

    def test_none_returns_empty(self):
        assert confidence_badge_text(None) == ""


# ══════════════════════════════════════════════════════════════════════════════
# load_json
# ══════════════════════════════════════════════════════════════════════════════


class TestLoadJson:

    def test_loads_valid_json(self, tmp_path):
        p = tmp_path / "test.json"
        p.write_text('{"key": "value"}', encoding="utf-8")
        assert load_json(p, "test") == {"key": "value"}

    def test_missing_file_returns_empty(self, tmp_path):
        p = tmp_path / "missing.json"
        assert load_json(p, "test") == {}

    def test_corrupt_json_returns_empty(self, tmp_path):
        p = tmp_path / "bad.json"
        p.write_text("not valid json {{{", encoding="utf-8")
        assert load_json(p, "test") == {}

    def test_empty_file_returns_empty(self, tmp_path):
        p = tmp_path / "empty.json"
        p.write_text("", encoding="utf-8")
        assert load_json(p, "test") == {}
