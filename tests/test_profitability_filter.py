"""Tests for analysis/profitability_filter.py — 21-day filter logic."""

import pytest
from analysis.profitability_filter import (
    _cross_competitor_patterns,
    _is_profitable,
    _rank_winners,
)


class TestIsProfitable:
    """Test the 21-day profitability proxy rule."""

    def test_above_threshold(self, make_ad):
        """Ads running 21+ days should be profitable."""
        assert _is_profitable(make_ad(duration_days=21)) is True
        assert _is_profitable(make_ad(duration_days=50)) is True

    def test_below_threshold(self, make_ad):
        """Ads running < 21 days should not be profitable."""
        assert _is_profitable(make_ad(duration_days=20)) is False
        assert _is_profitable(make_ad(duration_days=1)) is False

    def test_exact_threshold(self, make_ad):
        """Exactly 21 days should be profitable (inclusive)."""
        assert _is_profitable(make_ad(duration_days=21)) is True

    def test_none_duration(self, make_ad):
        """None duration should not be profitable."""
        assert _is_profitable(make_ad(duration_days=None)) is False

    def test_zero_duration(self, make_ad):
        """Zero duration should not be profitable."""
        assert _is_profitable(make_ad(duration_days=0)) is False


class TestRankWinners:
    """Test winner ranking by duration."""

    def test_sorted_descending(self, make_ad):
        """Winners should be sorted by duration_days descending."""
        winners = [
            make_ad(ad_library_id="W1", duration_days=25),
            make_ad(ad_library_id="W2", duration_days=60),
            make_ad(ad_library_id="W3", duration_days=30),
        ]
        ranked = _rank_winners(winners)
        durations = [w["duration_days"] for w in ranked]
        assert durations == [60, 30, 25]

    def test_includes_required_fields(self, make_ad):
        """Ranked output should include expected fields."""
        ranked = _rank_winners([make_ad(duration_days=30)])
        assert len(ranked) == 1
        entry = ranked[0]
        assert "ad_library_id" in entry
        assert "duration_days" in entry
        assert "creative_type" in entry
        assert "cta_type" in entry
        assert "start_date" in entry

    def test_truncates_ad_copy(self, make_ad):
        """Ad copy should be truncated to 200 chars."""
        long_copy = "x" * 500
        ranked = _rank_winners([make_ad(ad_copy=long_copy, duration_days=25)])
        assert len(ranked[0]["ad_copy"]) == 200

    def test_empty_list(self):
        assert _rank_winners([]) == []

    def test_none_duration_sorts_to_end(self, make_ad):
        """Ads with None duration should sort after those with values."""
        winners = [
            make_ad(ad_library_id="N1", duration_days=None),
            make_ad(ad_library_id="N2", duration_days=30),
        ]
        ranked = _rank_winners(winners)
        assert ranked[0]["ad_library_id"] == "N2"


class TestCrossCompetitorPatterns:
    """Test cross-competitor aggregation."""

    def test_aggregates_winners(self):
        per_brand = {
            "Client": {
                "brand_id": 1,
                "total_ads": 10,
                "profitable_ads": 3,
                "profitable_pct": 30.0,
                "ranked_winners": [
                    {"ad_library_id": "C1", "duration_days": 25,
                     "creative_type": "static", "cta_type": "Shop Now"},
                ],
            },
            "Comp1": {
                "brand_id": 2,
                "total_ads": 15,
                "profitable_ads": 5,
                "profitable_pct": 33.3,
                "ranked_winners": [
                    {"ad_library_id": "X1", "duration_days": 40,
                     "creative_type": "video", "cta_type": "Learn More"},
                    {"ad_library_id": "X2", "duration_days": 30,
                     "creative_type": "static", "cta_type": "Shop Now"},
                ],
            },
            "Comp2": {
                "brand_id": 3,
                "total_ads": 8,
                "profitable_ads": 2,
                "profitable_pct": 25.0,
                "ranked_winners": [
                    {"ad_library_id": "Y1", "duration_days": 35,
                     "creative_type": "carousel", "cta_type": "Shop Now"},
                ],
            },
        }
        result = _cross_competitor_patterns(per_brand, ["Comp1", "Comp2"])

        assert result["total_winners_across_competitors"] == 3
        assert result["avg_winner_duration_days"] == 35.0
        assert result["max_winner_duration_days"] == 40
        assert result["most_durable_ad"]["ad_library_id"] == "X1"
        assert "Shop Now" in result["winner_cta_distribution"]

    def test_excludes_client(self):
        """Client brand winners should not appear in cross-competitor patterns."""
        per_brand = {
            "Client": {
                "ranked_winners": [
                    {"ad_library_id": "C1", "duration_days": 100,
                     "creative_type": "static", "cta_type": "Buy"},
                ],
            },
            "Comp": {
                "ranked_winners": [
                    {"ad_library_id": "X1", "duration_days": 25,
                     "creative_type": "video", "cta_type": "Learn"},
                ],
            },
        }
        result = _cross_competitor_patterns(per_brand, ["Comp"])
        assert result["total_winners_across_competitors"] == 1
        assert result["most_durable_ad"]["ad_library_id"] == "X1"

    def test_no_competitors(self):
        """No competitor data should return empty dict."""
        result = _cross_competitor_patterns({}, [])
        assert result == {}

    def test_format_win_pct(self):
        """Format win percentages should sum to ~100%."""
        per_brand = {
            "Comp": {
                "ranked_winners": [
                    {"ad_library_id": f"A{i}", "duration_days": 25,
                     "creative_type": fmt, "cta_type": "Shop Now"}
                    for i, fmt in enumerate(["static", "static", "video", "carousel"])
                ],
            },
        }
        result = _cross_competitor_patterns(per_brand, ["Comp"])
        total_pct = sum(result["winner_format_pct"].values())
        assert 99.0 <= total_pct <= 101.0
        assert result["winner_format_pct"]["static"] == 50.0
