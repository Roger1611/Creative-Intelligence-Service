"""Tests for analysis/fatigue_scorer.py — fatigue scoring edge cases."""

import pytest
from analysis.fatigue_scorer import (
    _ads_in_range,
    _concentration_penalty,
    _count_deficit_penalty,
    _critical_penalty,
    _interpret_score,
    _recency_penalty,
    _warning_penalty,
)


class TestAdsInRange:
    """Test the duration range filter."""

    def test_critical_range(self, make_ad):
        """Ads with duration >= 30 days should be in critical range."""
        ads = [
            make_ad(duration_days=35),
            make_ad(duration_days=30),
            make_ad(duration_days=29),
            make_ad(duration_days=15),
        ]
        result = _ads_in_range(ads, min_days=30, max_days=None)
        assert len(result) == 2

    def test_warning_range(self, make_ad):
        """Ads 14-29 days should be in warning range."""
        ads = [
            make_ad(duration_days=14),
            make_ad(duration_days=20),
            make_ad(duration_days=29),
            make_ad(duration_days=30),
            make_ad(duration_days=10),
        ]
        result = _ads_in_range(ads, min_days=14, max_days=30)
        assert len(result) == 3

    def test_none_duration_excluded(self, make_ad):
        """Ads with None duration should be excluded."""
        ads = [
            make_ad(duration_days=None),
            make_ad(duration_days=35),
        ]
        result = _ads_in_range(ads, min_days=30, max_days=None)
        assert len(result) == 1

    def test_empty_list(self):
        assert _ads_in_range([], 30, None) == []

    def test_exact_boundary(self, make_ad):
        """Boundary value: min_days is inclusive, max_days is exclusive."""
        ads = [make_ad(duration_days=14)]
        assert len(_ads_in_range(ads, min_days=14, max_days=30)) == 1
        assert len(_ads_in_range(ads, min_days=15, max_days=30)) == 0


class TestCriticalPenalty:
    """Test critical ads penalty (10pts/ad, cap 40)."""

    def test_no_critical_ads(self):
        assert _critical_penalty([]) == 0

    def test_one_critical_ad(self, make_ad):
        assert _critical_penalty([make_ad()]) == 10

    def test_three_critical_ads(self, make_ad):
        assert _critical_penalty([make_ad()] * 3) == 30

    def test_cap_at_40(self, make_ad):
        """More than 4 critical ads should still cap at 40."""
        assert _critical_penalty([make_ad()] * 10) == 40


class TestWarningPenalty:
    """Test warning ads penalty (3pts/ad, cap 15)."""

    def test_no_warning_ads(self):
        assert _warning_penalty([]) == 0

    def test_three_warning_ads(self, make_ad):
        assert _warning_penalty([make_ad()] * 3) == 9

    def test_cap_at_15(self, make_ad):
        assert _warning_penalty([make_ad()] * 10) == 15


class TestConcentrationPenalty:
    """Test format concentration penalty (0-25 pts)."""

    def test_no_concentration(self):
        """Evenly distributed formats should get no penalty."""
        fmt = {
            "static":   {"count": 5, "pct": 25.0},
            "video":    {"count": 5, "pct": 25.0},
            "carousel": {"count": 5, "pct": 25.0},
            "reel":     {"count": 5, "pct": 25.0},
        }
        assert _concentration_penalty(fmt, 20) == 0.0

    def test_full_concentration(self):
        """100% single format should give max penalty (25)."""
        fmt = {
            "static":   {"count": 10, "pct": 100.0},
            "video":    {"count": 0,  "pct": 0.0},
            "carousel": {"count": 0,  "pct": 0.0},
            "reel":     {"count": 0,  "pct": 0.0},
        }
        assert _concentration_penalty(fmt, 10) == 25.0

    def test_just_below_trigger(self):
        """60% dominance should trigger no penalty (threshold is >60%)."""
        fmt = {
            "static":   {"count": 6, "pct": 60.0},
            "video":    {"count": 4, "pct": 40.0},
            "carousel": {"count": 0, "pct": 0.0},
            "reel":     {"count": 0, "pct": 0.0},
        }
        assert _concentration_penalty(fmt, 10) == 0.0

    def test_just_above_trigger(self):
        """61% should trigger a small penalty."""
        fmt = {
            "static":   {"count": 61, "pct": 61.0},
            "video":    {"count": 39, "pct": 39.0},
            "carousel": {"count": 0,  "pct": 0.0},
            "reel":     {"count": 0,  "pct": 0.0},
        }
        penalty = _concentration_penalty(fmt, 100)
        assert 0 < penalty < 5  # small penalty

    def test_zero_ads(self):
        """Zero total ads should yield no penalty."""
        assert _concentration_penalty({}, 0) == 0.0


class TestCountDeficitPenalty:
    """Test the count deficit penalty (0-10 pts)."""

    def test_no_deficit(self):
        """Client at or above competitor avg should get 0 penalty."""
        assert _count_deficit_penalty(20, 15.0) == 0.0

    def test_half_or_less(self):
        """Client at 50% or below should get full 10 pts."""
        assert _count_deficit_penalty(5, 20.0) == 10.0

    def test_between_50_and_100(self):
        """Client between 50-100% should get scaled penalty."""
        penalty = _count_deficit_penalty(15, 20.0)
        assert 0 < penalty < 10

    def test_zero_competitor_avg(self):
        """Zero competitor avg should yield no penalty."""
        assert _count_deficit_penalty(10, 0.0) == 0.0

    def test_zero_client_count(self):
        """Zero client ads should yield no penalty (edge case)."""
        assert _count_deficit_penalty(0, 10.0) == 0.0


class TestRecencyPenalty:
    """Test the recency penalty (0-10 pts)."""

    def test_recent_launch(self):
        """Launched within 21 days should get 0 penalty."""
        assert _recency_penalty(10) == 0.0
        assert _recency_penalty(21) == 0.0

    def test_stale_launch(self):
        """51+ days (30 over threshold) should get full 10 pts."""
        assert _recency_penalty(51) == 10.0

    def test_moderate_staleness(self):
        """Between threshold and 30 days over should get partial penalty."""
        penalty = _recency_penalty(36)  # 15 days over threshold of 21
        assert 0 < penalty < 10

    def test_none_days(self):
        """None (unknown) should get half penalty as conservative estimate."""
        assert _recency_penalty(None) == 5.0


class TestInterpretScore:
    """Test score interpretation labels."""

    def test_healthy(self):
        assert _interpret_score(0) == "healthy"
        assert _interpret_score(19) == "healthy"

    def test_mild(self):
        assert _interpret_score(20) == "mild_fatigue"
        assert _interpret_score(39) == "mild_fatigue"

    def test_moderate(self):
        assert _interpret_score(40) == "moderate_fatigue"
        assert _interpret_score(59) == "moderate_fatigue"

    def test_high(self):
        assert _interpret_score(60) == "high_fatigue"
        assert _interpret_score(79) == "high_fatigue"

    def test_critical(self):
        assert _interpret_score(80) == "critical_fatigue"
        assert _interpret_score(100) == "critical_fatigue"
