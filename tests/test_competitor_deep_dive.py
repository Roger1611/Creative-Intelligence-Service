"""Tests for analysis/competitor_deep_dive.py."""

import pytest
from analysis.competitor_deep_dive import (
    build_why_it_works,
    compute_creative_velocity,
    _build_landscape_summary,
    _compute_format_mix,
    _dominant_value,
    _empty_profile,
)


class TestBuildWhyItWorks:
    """Test data-backed why_it_works explanation generation."""

    def test_full_explanation_with_all_data(self):
        """With trigger rate, hook rate, and duration avg — should produce 2 sentences."""
        category_intel = {
            "trigger_analysis": {
                "profitable_rate_by_trigger": {"curiosity": 36.0, "status": 25.0},
            },
            "hook_structure_analysis": {
                "profitable_rate_by_hook": {"direct_address": 50.0, "question": 40.0},
            },
            "duration_analysis": {
                "all_ads": {"avg": 14.0},
            },
        }
        result = build_why_it_works(
            trigger="curiosity",
            hook_structure="direct_address",
            duration_days=35,
            category_intel=category_intel,
        )
        assert "curiosity" in result
        assert "36%" in result
        assert "direct address" in result
        assert "50%" in result
        assert "35 days" in result
        assert "2.5x" in result
        assert "14 days" in result

    def test_trigger_only_no_hook(self):
        """When hook_structure is None, should still mention trigger."""
        category_intel = {
            "trigger_analysis": {
                "profitable_rate_by_trigger": {"fear": 42.0},
            },
            "hook_structure_analysis": {"profitable_rate_by_hook": {}},
            "duration_analysis": {"all_ads": {"avg": 10.0}},
        }
        result = build_why_it_works(
            trigger="fear",
            hook_structure=None,
            duration_days=25,
            category_intel=category_intel,
        )
        assert "fear" in result
        assert "42%" in result
        assert "25 days" in result

    def test_no_category_intel(self):
        """Without category_intel, should fall back to duration-only explanation."""
        result = build_why_it_works(
            trigger="status",
            hook_structure="question",
            duration_days=30,
            category_intel=None,
        )
        assert "status" in result
        assert "30 days" in result
        assert "sustained performance" in result

    def test_no_trigger_no_hook_no_intel(self):
        """With no data at all, should return insufficient data message."""
        result = build_why_it_works(
            trigger=None,
            hook_structure=None,
            duration_days=0,
            category_intel=None,
        )
        assert "Insufficient data" in result

    def test_zero_duration_with_trigger(self):
        """Zero duration should skip the duration sentence."""
        category_intel = {
            "trigger_analysis": {
                "profitable_rate_by_trigger": {"urgency": 20.0},
            },
            "hook_structure_analysis": {"profitable_rate_by_hook": {}},
            "duration_analysis": {"all_ads": {"avg": 10.0}},
        }
        result = build_why_it_works(
            trigger="urgency",
            hook_structure=None,
            duration_days=0,
            category_intel=category_intel,
        )
        assert "urgency" in result
        # Should not mention "0 days" or multiplier
        assert "0 days" not in result
        assert "0.0x" not in result

    def test_trigger_not_in_rates(self):
        """Trigger present but not in win rate data — should still mention it."""
        category_intel = {
            "trigger_analysis": {
                "profitable_rate_by_trigger": {"curiosity": 30.0},
            },
            "hook_structure_analysis": {"profitable_rate_by_hook": {}},
            "duration_analysis": {"all_ads": {"avg": 10.0}},
        }
        result = build_why_it_works(
            trigger="belonging",
            hook_structure=None,
            duration_days=25,
            category_intel=category_intel,
        )
        assert "belonging" in result
        # Should not have a win rate since it's not in the data
        assert "win rate in category" not in result


class TestComputeCreativeVelocity:
    """Test creative velocity (new ads per week) calculation."""

    def test_steady_monthly_rate(self):
        """4 ads per month over 3 months = ~0.9/week."""
        ads = [
            {"start_date": "2026-01-05"},
            {"start_date": "2026-01-10"},
            {"start_date": "2026-01-15"},
            {"start_date": "2026-01-20"},
            {"start_date": "2026-02-05"},
            {"start_date": "2026-02-10"},
            {"start_date": "2026-02-15"},
            {"start_date": "2026-02-20"},
            {"start_date": "2026-03-05"},
            {"start_date": "2026-03-10"},
            {"start_date": "2026-03-15"},
            {"start_date": "2026-03-20"},
        ]
        velocity = compute_creative_velocity(ads)
        # 12 ads / 3 months = 4/month = ~0.9/week
        assert 0.8 <= velocity <= 1.0

    def test_single_month_burst(self):
        """All ads in one month — velocity = count / 4.33."""
        ads = [
            {"start_date": "2026-03-01"},
            {"start_date": "2026-03-05"},
            {"start_date": "2026-03-10"},
            {"start_date": "2026-03-15"},
            {"start_date": "2026-03-20"},
        ]
        velocity = compute_creative_velocity(ads)
        # 5 ads / 1 month / 4.33 = ~1.15
        assert 1.0 <= velocity <= 1.2

    def test_no_start_dates(self):
        """Ads without start_date should return 0."""
        ads = [
            {"start_date": None},
            {"start_date": None},
        ]
        assert compute_creative_velocity(ads) == 0.0

    def test_empty_ads(self):
        """Empty list returns 0."""
        assert compute_creative_velocity([]) == 0.0

    def test_mixed_dates_and_none(self):
        """Ads with some None dates — only count the ones with dates."""
        ads = [
            {"start_date": "2026-01-10"},
            {"start_date": None},
            {"start_date": "2026-01-20"},
        ]
        velocity = compute_creative_velocity(ads)
        # 2 ads in 1 month
        assert velocity > 0


class TestZeroProfitableCompetitor:
    """Test graceful degradation with 0 profitable ads."""

    def test_empty_profile_structure(self):
        """Empty profile should have all required keys with safe defaults."""
        profile = _empty_profile()
        assert profile["active_ads"] == 0
        assert profile["profitable_ads"] == 0
        assert profile["win_rate"] == 0.0
        assert profile["format_mix"] == {}
        assert profile["dominant_trigger"] is None
        assert profile["dominant_hook_structure"] is None
        assert profile["creative_velocity_per_week"] == 0.0
        assert profile["top_winners"] == []


class TestComputeFormatMix:
    """Test format mix computation."""

    def test_mixed_formats(self):
        """Should count and percentage each format."""
        ads = [
            {"creative_type": "video"},
            {"creative_type": "video"},
            {"creative_type": "static"},
            {"creative_type": "carousel"},
        ]
        result = _compute_format_mix(ads)
        assert result["video"]["count"] == 2
        assert result["video"]["pct"] == 50.0
        assert result["static"]["count"] == 1
        assert result["static"]["pct"] == 25.0

    def test_empty_ads(self):
        """Empty ads should return empty dict."""
        assert _compute_format_mix([]) == {}

    def test_none_creative_type(self):
        """None creative_type should count as 'unknown'."""
        ads = [{"creative_type": None}]
        result = _compute_format_mix(ads)
        assert "unknown" in result


class TestDominantValue:
    """Test dominant value extraction."""

    def test_finds_most_common(self):
        """Should return the most frequent non-null value."""
        analyses = [
            {"psychological_trigger": "curiosity"},
            {"psychological_trigger": "curiosity"},
            {"psychological_trigger": "status"},
        ]
        assert _dominant_value(analyses, "psychological_trigger") == "curiosity"

    def test_all_none(self):
        """All None values should return None."""
        analyses = [
            {"psychological_trigger": None},
            {"psychological_trigger": None},
        ]
        assert _dominant_value(analyses, "psychological_trigger") is None

    def test_empty_list(self):
        """Empty list should return None."""
        assert _dominant_value([], "psychological_trigger") is None


class TestBuildLandscapeSummary:
    """Test competitive landscape aggregation."""

    def test_basic_landscape(self):
        """Should aggregate metrics across competitors."""
        per_competitor = {
            "Brand A": {
                "active_ads": 20,
                "profitable_ads": 5,
                "win_rate": 25.0,
                "format_mix": {"video": {"count": 10, "pct": 50.0}, "static": {"count": 10, "pct": 50.0}},
                "dominant_trigger": "curiosity",
                "dominant_hook_structure": "question",
                "creative_velocity_per_week": 2.0,
                "top_winners": [],
            },
            "Brand B": {
                "active_ads": 10,
                "profitable_ads": 3,
                "win_rate": 30.0,
                "format_mix": {"video": {"count": 8, "pct": 80.0}, "static": {"count": 2, "pct": 20.0}},
                "dominant_trigger": "curiosity",
                "dominant_hook_structure": "bold_claim",
                "creative_velocity_per_week": 1.5,
                "top_winners": [],
            },
        }

        result = _build_landscape_summary(per_competitor)

        assert result["total_competitor_ads"] == 30
        assert result["total_profitable_across_competitors"] == 8
        assert result["avg_competitor_ad_count"] == 15.0
        assert result["most_aggressive_competitor"]["name"] == "Brand A"
        assert result["most_aggressive_competitor"]["active_ads"] == 20
        assert result["dominant_format_across_competitors"] == "video"
        assert result["dominant_trigger_across_competitors"] == "curiosity"
        assert result["creative_velocity_leader"]["name"] == "Brand A"
        assert result["creative_velocity_leader"]["estimated_new_per_week"] == 2.0

    def test_empty_competitors(self):
        """Empty competitor dict should return safe defaults."""
        result = _build_landscape_summary({})
        assert result["total_competitor_ads"] == 0
        assert result["most_aggressive_competitor"] is None
