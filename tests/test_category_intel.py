"""Tests for analysis/category_intel.py — hook structure analysis."""

import pytest
from analysis.category_intel import _hook_structure_analysis


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
