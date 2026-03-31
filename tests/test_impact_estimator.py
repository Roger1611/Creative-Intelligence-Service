"""Tests for analysis/impact_estimator.py — ₹ impact estimation."""

import pytest
from analysis.impact_estimator import (
    estimate_daily_spend,
    calculate_fatigue_waste,
    calculate_refresh_waste,
    _price_gaps,
)
from config import ESTIMATED_DAILY_SPEND_PER_AD, SPRINT_PRICE_INR


class TestEstimateDailySpend:
    """Test spend estimation from ad count."""

    def test_known_ad_count(self):
        """10 active ads × ₹750 = ₹7500/day."""
        result = estimate_daily_spend(10)
        assert result["amount"] == 10 * ESTIMATED_DAILY_SPEND_PER_AD
        assert result["is_estimated"] is True

    def test_explicit_spend_overrides(self):
        """Explicit daily_spend_inr should be used as-is."""
        result = estimate_daily_spend(10, daily_spend_inr=5000.0)
        assert result["amount"] == 5000.0
        assert result["is_estimated"] is False

    def test_zero_ads(self):
        """Zero ads should produce zero estimated spend."""
        result = estimate_daily_spend(0)
        assert result["amount"] == 0.0
        assert result["is_estimated"] is True

    def test_single_ad(self):
        """1 ad should produce the per-ad benchmark."""
        result = estimate_daily_spend(1)
        assert result["amount"] == ESTIMATED_DAILY_SPEND_PER_AD


class TestFatigueWaste:
    """Test creative fatigue waste calculation."""

    def test_basic_fatigue_waste(self):
        """3 fatigued ads × ₹750/day × 0.35 × 30 days."""
        fatigue_data = {
            "critical_ads": [
                {"ad_library_id": "A1", "duration_days": 35},
                {"ad_library_id": "A2", "duration_days": 40},
                {"ad_library_id": "A3", "duration_days": 50},
            ],
        }
        daily_per_ad = 750.0
        waste = calculate_fatigue_waste(fatigue_data, daily_per_ad)
        expected = 3 * 750 * 0.35 * 30  # 23625
        assert waste == expected

    def test_zero_fatigued_ads(self):
        """Zero fatigued ads → zero waste."""
        fatigue_data = {"critical_ads": []}
        assert calculate_fatigue_waste(fatigue_data, 750.0) == 0.0

    def test_no_fatigue_data(self):
        """None fatigue data → zero waste."""
        assert calculate_fatigue_waste(None, 750.0) == 0.0

    def test_missing_critical_ads_key(self):
        """Fatigue data without critical_ads key → zero waste."""
        assert calculate_fatigue_waste({}, 750.0) == 0.0

    def test_single_fatigued_ad(self):
        """Single fatigued ad calculation."""
        fatigue_data = {
            "critical_ads": [{"ad_library_id": "A1", "duration_days": 45}],
        }
        waste = calculate_fatigue_waste(fatigue_data, 1000.0)
        expected = 1 * 1000 * 0.35 * 30  # 10500
        assert waste == expected


class TestRefreshWaste:
    """Test refresh cycle waste calculation."""

    def test_high_avg_duration(self):
        """Avg duration 20 days, 10 active ads → positive waste."""
        fatigue_data = {
            "fatigue_index": {"avg_duration": 20.0, "severity": "MODERATE"},
        }
        waste = calculate_refresh_waste(fatigue_data, 750.0, 10)
        # (20 - 10) × 750 × 10 × 0.20 × 30 / 20 = 22500
        expected = (20 - 10) * 750 * 10 * 0.20 * 30 / 20
        assert waste == expected

    def test_low_avg_duration(self):
        """Avg duration <= 14 days → zero waste."""
        fatigue_data = {
            "fatigue_index": {"avg_duration": 12.0},
        }
        assert calculate_refresh_waste(fatigue_data, 750.0, 10) == 0.0

    def test_exactly_14_days(self):
        """Avg duration exactly 14 → zero waste (threshold is >14)."""
        fatigue_data = {
            "fatigue_index": {"avg_duration": 14.0},
        }
        assert calculate_refresh_waste(fatigue_data, 750.0, 5) == 0.0

    def test_no_fatigue_data(self):
        """None fatigue data → zero waste."""
        assert calculate_refresh_waste(None, 750.0, 10) == 0.0

    def test_zero_avg_duration(self):
        """Zero avg duration → zero waste."""
        fatigue_data = {
            "fatigue_index": {"avg_duration": 0},
        }
        assert calculate_refresh_waste(fatigue_data, 750.0, 10) == 0.0


class TestROICalculation:
    """Test ROI / payback calculation from gap pricing."""

    def test_roi_with_gaps(self):
        """Gaps with known win rates should produce positive impact."""
        gaps = [
            {
                "type": "ANGLE GAP",
                "title": "Zero Curiosity Creatives",
                "competitor_usage_pct": 30.0,
                "win_rate": 40.0,
            },
        ]
        intel_data = {
            "duration_analysis": {
                "profitable_ads": {"avg": 28.0},
            },
        }
        fatigue_data = {
            "fatigue_index": {"avg_duration": 12.0},
        }

        angle_total, format_total, per_gap = _price_gaps(
            gaps, intel_data, fatigue_data, 750.0,
        )

        assert angle_total > 0
        assert format_total == 0
        assert len(per_gap) == 1
        assert per_gap[0]["gap_type"] == "ANGLE GAP"
        assert per_gap[0]["estimated_monthly_impact_inr"] > 0
        assert per_gap[0]["confidence"] == "high"  # win_rate >= 40

    def test_empty_gaps(self):
        """No gaps → zero impact."""
        angle, fmt, per_gap = _price_gaps([], None, None, 750.0)
        assert angle == 0.0
        assert fmt == 0.0
        assert per_gap == []

    def test_confidence_levels(self):
        """Verify confidence mapping: >=40 high, >=20 medium, else low."""
        gaps = [
            {"type": "ANGLE GAP", "title": "A", "competitor_usage_pct": 20, "win_rate": 50},
            {"type": "ANGLE GAP", "title": "B", "competitor_usage_pct": 15, "win_rate": 25},
            {"type": "FORMAT GAP", "title": "C", "competitor_usage_pct": 10, "win_rate": 10},
        ]
        intel_data = {
            "duration_analysis": {"profitable_ads": {"avg": 25}},
        }
        fatigue_data = {
            "fatigue_index": {"avg_duration": 10},
        }

        _, _, per_gap = _price_gaps(gaps, intel_data, fatigue_data, 750.0)

        assert per_gap[0]["confidence"] == "high"
        assert per_gap[1]["confidence"] == "medium"
        assert per_gap[2]["confidence"] == "low"

    def test_format_gap_goes_to_format_total(self):
        """FORMAT GAP and HOOK STRUCTURE GAP should accumulate in format_total."""
        gaps = [
            {"type": "FORMAT GAP", "title": "No Video", "competitor_usage_pct": 40, "win_rate": 30},
            {"type": "HOOK STRUCTURE GAP", "title": "No Question", "competitor_usage_pct": 25, "win_rate": 35},
        ]
        intel_data = {
            "duration_analysis": {"profitable_ads": {"avg": 25}},
        }
        fatigue_data = {
            "fatigue_index": {"avg_duration": 10},
        }

        angle_total, format_total, _ = _price_gaps(gaps, intel_data, fatigue_data, 750.0)
        assert angle_total == 0
        assert format_total > 0

    def test_payback_calculation(self):
        """Payback = sprint_cost / (monthly_savings / 30)."""
        monthly_savings = 30000.0
        daily_savings = monthly_savings / 30
        payback = round(SPRINT_PRICE_INR / daily_savings, 1)
        assert payback == round(8000 / 1000, 1)  # 8.0 days
