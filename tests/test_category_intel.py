"""Tests for analysis/category_intel.py — hook structure analysis + hook database validation."""

import pytest
from analysis.category_intel import _hook_structure_analysis, _build_hook_database, _is_valid_hook


class TestHookStructureAnalysis:
    """Test the hook_structure_analysis function."""

    def test_basic_prevalence(self):
        """Should count hook_structure prevalence across all analyses."""
        analyses = [
            {"hook_structure": "question", "ad_id": 1},
            {"hook_structure": "question", "ad_id": 2},
            {"hook_structure": "bold_claim", "ad_id": 3},
            {"hook_structure": "transformation", "ad_id": 4},
        ]
        profitable_ads = [{"id": 1}, {"id": 3}]

        result = _hook_structure_analysis(analyses, profitable_ads)

        assert result["by_prevalence"]["question"] == 2
        assert result["by_prevalence"]["bold_claim"] == 1
        assert result["by_prevalence"]["transformation"] == 1

    def test_profitable_only(self):
        """Should count hook_structures in profitable ads only."""
        analyses = [
            {"hook_structure": "question", "ad_id": 1},
            {"hook_structure": "question", "ad_id": 2},
            {"hook_structure": "bold_claim", "ad_id": 3},
        ]
        profitable_ads = [{"id": 1}]  # only ad_id=1 is profitable

        result = _hook_structure_analysis(analyses, profitable_ads)

        assert result["by_profitable_only"]["question"] == 1
        assert "bold_claim" not in result["by_profitable_only"]

    def test_profitable_rate(self):
        """Should compute win rate per hook_structure."""
        analyses = [
            {"hook_structure": "question", "ad_id": 1},
            {"hook_structure": "question", "ad_id": 2},
            {"hook_structure": "bold_claim", "ad_id": 3},
            {"hook_structure": "bold_claim", "ad_id": 4},
        ]
        profitable_ads = [{"id": 1}, {"id": 3}]

        result = _hook_structure_analysis(analyses, profitable_ads)

        assert result["profitable_rate_by_hook"]["question"] == 50.0
        assert result["profitable_rate_by_hook"]["bold_claim"] == 50.0

    def test_underused_hooks(self):
        """Should flag hook structures with 0 profitable ads."""
        analyses = [
            {"hook_structure": "question", "ad_id": 1},
            {"hook_structure": "bold_claim", "ad_id": 2},
        ]
        profitable_ads = [{"id": 1}]  # only question is profitable

        result = _hook_structure_analysis(analyses, profitable_ads)

        # bold_claim has no profitable ads, plus all the hooks not in analyses
        assert "bold_claim" in result["underused_hooks"]
        assert "question" not in result["underused_hooks"]
        # All unused hooks from the taxonomy should be in underused
        assert "pattern_interrupt" in result["underused_hooks"]
        assert "curiosity_gap" in result["underused_hooks"]

    def test_none_hook_structure_excluded(self):
        """Analyses with None hook_structure should be excluded."""
        analyses = [
            {"hook_structure": None, "ad_id": 1},
            {"hook_structure": "question", "ad_id": 2},
            {"hook_structure": "", "ad_id": 3},  # empty string = falsy
        ]
        profitable_ads = [{"id": 2}]

        result = _hook_structure_analysis(analyses, profitable_ads)

        assert result["by_prevalence"] == {"question": 1}

    def test_empty_analyses(self):
        """Empty analyses should return empty results."""
        result = _hook_structure_analysis([], [])

        assert result["by_prevalence"] == {}
        assert result["by_profitable_only"] == {}
        assert result["profitable_rate_by_hook"] == {}
        assert len(result["underused_hooks"]) == 10  # all hooks are underused

    def test_all_hooks_profitable(self):
        """If every ad is profitable, no hooks should be underused (for present hooks)."""
        analyses = [
            {"hook_structure": "question", "ad_id": 1},
            {"hook_structure": "bold_claim", "ad_id": 2},
        ]
        profitable_ads = [{"id": 1}, {"id": 2}]

        result = _hook_structure_analysis(analyses, profitable_ads)

        assert "question" not in result["underused_hooks"]
        assert "bold_claim" not in result["underused_hooks"]
        # But hooks never seen are still underused
        assert "urgency_lead" in result["underused_hooks"]


class TestIsValidHook:
    """Test the _is_valid_hook validation function."""

    def test_template_variable_rejected(self):
        """Hooks containing {{product.brand}} should be excluded."""
        assert _is_valid_hook("{{product.brand}} loves you") is False
        assert _is_valid_hook("Buy {{product.name}} now") is False
        assert _is_valid_hook("{{product.brand}}") is False

    def test_jinja_block_rejected(self):
        """Hooks containing {%...%} should be excluded."""
        assert _is_valid_hook("{% if sale %}Big Sale{% endif %}") is False

    def test_short_hooks_rejected(self):
        """Hooks shorter than 5 chars after stripping should be excluded."""
        assert _is_valid_hook("Hi") is False
        assert _is_valid_hook("    ") is False
        assert _is_valid_hook("ab") is False

    def test_url_only_rejected(self):
        """URL-only hooks should be excluded."""
        assert _is_valid_hook("https://example.com/sale") is False
        assert _is_valid_hook("http://shop.brand.com") is False

    def test_valid_hooks_accepted(self):
        """Normal hooks should pass validation."""
        assert _is_valid_hook("Stop scrolling — your skin deserves better") is True
        assert _is_valid_hook("5 reasons to switch to natural skincare") is True
        assert _is_valid_hook("Did you know your moisturizer is lying?") is True


class TestBuildHookDatabase:
    """Test _build_hook_database with template variable filtering."""

    def _make_data(self, ad_copies):
        """Helper to create analyses/ads/profitable/brands from ad copy list."""
        analyses = []
        ads = []
        for i, copy in enumerate(ad_copies, start=1):
            analyses.append({
                "ad_id": i,
                "psychological_trigger": "status",
            })
            ads.append({
                "id": i,
                "ad_copy": copy,
                "brand_id": 100,
                "ad_library_id": f"lib_{i}",
                "duration_days": 30,
                "transcript": None,
            })
        profitable_ads = list(ads)  # all profitable
        brand_rows = [{"id": 100, "name": "TestBrand"}]
        return analyses, ads, profitable_ads, brand_rows

    def test_template_vars_excluded_from_hooks(self):
        """Hooks with {{product.brand}} must not appear in the database."""
        analyses, ads, profitable, brands = self._make_data([
            "{{product.brand}} is amazing",
            "Your skin deserves the best care possible",
        ])
        result = _build_hook_database(analyses, ads, profitable, brands)
        all_hook_texts = [
            h["text"]
            for trigger_data in result.values()
            for h in trigger_data["hooks"]
        ]
        assert "{{product.brand}} is amazing" not in all_hook_texts
        assert any("Your skin" in t for t in all_hook_texts)

    def test_short_hooks_excluded(self):
        """Hooks shorter than 5 chars must be excluded."""
        analyses, ads, profitable, brands = self._make_data([
            "Hey",
            "Transform your skincare routine today",
        ])
        result = _build_hook_database(analyses, ads, profitable, brands)
        all_hook_texts = [
            h["text"]
            for trigger_data in result.values()
            for h in trigger_data["hooks"]
        ]
        assert "Hey" not in all_hook_texts

    def test_url_hooks_excluded(self):
        """URL-only hooks must be excluded."""
        analyses, ads, profitable, brands = self._make_data([
            "https://example.com/shop",
            "Stop wasting money on creams that don't work",
        ])
        result = _build_hook_database(analyses, ads, profitable, brands)
        all_hook_texts = [
            h["text"]
            for trigger_data in result.values()
            for h in trigger_data["hooks"]
        ]
        assert "https://example.com/shop" not in all_hook_texts

    def test_hook_structure_field_present(self):
        """Each hook dict should have a hook_structure field."""
        analyses, ads, profitable, brands = self._make_data([
            "Did you know your moisturizer is lying to you?",
        ])
        result = _build_hook_database(analyses, ads, profitable, brands)
        for trigger_data in result.values():
            for hook in trigger_data["hooks"]:
                assert "hook_structure" in hook
