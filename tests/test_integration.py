"""Integration tests — audit PDF with all combinations of data sources.

Also tests edge-case hardening across analysis modules: brand_intel,
competitor_deep_dive, impact_estimator, and audit_generator.
"""

import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from deliverables.audit_generator import _build_pdf, run


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _minimal_data():
    return {
        "brand": {"id": 1, "name": "TestBrand", "is_client": 1,
                  "category": "skincare"},
        "client_ads": [],
        "waste_report": {},
        "competitors": [],
        "sample_concepts": [],
        "fatigue_analysis": {},
        "category_intel": {},
        "profitability_summary": {},
        "brand_intel": {},
        "competitor_deep_dive": {},
        "impact_estimate": {},
    }


def _full_data():
    """All data sources populated — the happy path."""
    data = _minimal_data()
    data["client_ads"] = [
        {"id": 1, "is_active": 1, "creative_type": "static",
         "duration_days": 25, "psychological_trigger": "fear",
         "copy_tone": "urgent", "effectiveness_score": 7,
         "ad_copy": "Test ad copy"},
        {"id": 2, "is_active": 1, "creative_type": "video",
         "duration_days": 10, "psychological_trigger": "transformation",
         "copy_tone": "warm", "effectiveness_score": 8,
         "transcript": "Great product", "ad_copy": "Watch this"},
    ]
    data["competitors"] = [
        {
            "brand": {"id": 2, "name": "CompA", "is_client": 0,
                      "category": "skincare"},
            "ads": [
                {"id": 10, "is_active": 1, "creative_type": "video",
                 "duration_days": 45, "psychological_trigger": "transformation"},
                {"id": 11, "is_active": 1, "creative_type": "static",
                 "duration_days": 30, "psychological_trigger": "fear"},
            ],
        },
    ]
    data["fatigue_analysis"] = {
        "creative_coverage": {
            "ratio": 0.5, "client_count": 2, "benchmark": 15,
            "deficit": 13,
            "interpretation": "Underfeeding by 87%",
        },
        "fatigue_index": {
            "index": 1.8, "avg_duration": 18.0, "benchmark": 10,
            "severity": "HIGH",
            "interpretation": "Average ad is 8 days past optimal",
        },
        "hook_diversity": {
            "score": 30.0,
            "triggers_used": ["fear", "transformation"],
            "triggers_missing": ["status", "social_proof", "curiosity",
                                 "urgency", "authority", "belonging",
                                 "aspiration", "agitation_solution"],
            "hook_structures_used": ["question"],
            "hook_structures_missing": ["bold_claim"],
            "interpretation": "2/10 angles",
        },
    }
    data["category_intel"] = {
        "profitable_ads_in_universe": 30,
        "trigger_analysis": {
            "by_profitable_only": {
                "transformation": 10, "fear": 6, "social_proof": 5,
                "curiosity": 4,
            },
            "profitable_rate_by_trigger": {
                "transformation": 60, "fear": 50,
                "social_proof": 45, "curiosity": 40,
            },
        },
        "format_analysis": {
            "video": {"total_pct": 50, "winner_pct": 60, "win_rate": 65},
            "carousel": {"total_pct": 20, "winner_pct": 25, "win_rate": 55},
        },
        "hook_structure_analysis": {
            "by_profitable_only": {"question": 12, "bold_claim": 8},
            "profitable_rate_by_hook": {"question": 55, "bold_claim": 60},
        },
        "hook_database": {
            "transformation": {
                "count": 10, "pct_of_winners": 40.0,
                "hooks": [
                    {"text": "Watch my skin transform in just 7 days",
                     "source_brand": "CompA", "duration_days": 45,
                     "ad_library_id": "C001", "hook_structure": "bold_claim",
                     "spoken_hook": "Watch how this works"},
                ],
            },
        },
        "visual_pattern_stats": {
            "total_analyzed": 25,
            "face_dominant_pct": 65.0, "text_overlay_pct": 80.0,
            "before_after_pct": 35.0, "product_focused_pct": 45.0,
            "ugc_style_pct": 20.0, "minimal_aesthetic_pct": 10.0,
        },
    }
    data["brand_intel"] = {
        "products_detected": [
            {"name": "Gotukola Face Wash", "frequency": 5},
            {"name": "Vitamin C Serum", "frequency": 3},
        ],
        "price_points": [{"amount": "499"}, {"amount": "799"}],
        "key_ingredients": [{"name": "gotukola", "frequency": 8}],
        "language_profile": {"primary": "english", "has_hindi": False},
    }
    data["competitor_deep_dive"] = {
        "competitive_landscape_summary": "CompA leads the market.",
        "competitor_profiles": [
            {"name": "CompA", "active_ads": 15, "win_rate": 60,
             "dominant_trigger": "transformation",
             "creative_velocity": "5/wk"},
        ],
        "top_winners": {
            "CompA": [
                {"hook_text": "Watch my skin transform in 7 days",
                 "duration_days": 45,
                 "psychological_trigger": "transformation",
                 "hook_structure": "bold_claim",
                 "why_it_works": "Strong timeframe + visual proof",
                 "visual_layout": "Split screen before/after"},
            ],
        },
    }
    data["impact_estimate"] = {
        "total_estimated_monthly_waste": 85000,
        "waste_breakdown": {
            "creative_fatigue_waste_monthly": 30000,
            "angle_gap_opportunity_cost_monthly": 35000,
            "format_gap_opportunity_cost_monthly": 10000,
            "refresh_cycle_waste_monthly": 10000,
            "total_estimated_monthly_waste": 85000,
        },
        "per_gap_impact": [
            {"gap_type": "ANGLE GAP",
             "gap_title": "Zero Social Proof Creatives",
             "estimated_monthly_impact_inr": 35000,
             "confidence": "high"},
            {"gap_type": "ANGLE GAP",
             "gap_title": "Zero Curiosity Creatives",
             "estimated_monthly_impact_inr": 25000,
             "confidence": "medium"},
        ],
        "roi_of_sprint": {
            "sprint_cost": 25000,
            "estimated_monthly_savings": 85000,
            "payback_period_days": 9,
        },
    }
    data["sample_concepts"] = [
        {
            "hook_text": "Your face wash is making your skin worse",
            "psychological_angle": "fear",
            "format": "video",
            "body_script": "Most face washes strip natural oils.",
            "text_overlay": "Is your cleanser the problem?",
            "visual_direction": {
                "scene_description": "Close-up of irritated skin",
                "talent_direction": "Woman touching face",
                "product_placement": "Gotukola Face Wash revealed",
            },
            "competitor_inspiration": "CompA's fear hook (38 days)",
            "production_difficulty": "low",
            "data_backing": "Fear hooks: 50% win rate",
        },
    ]
    data["waste_report"] = {
        "recommendations_json": json.dumps([
            {"priority": "high", "action": "Pause 1 fatigued ad",
             "signal": "AD running 25 days"},
        ]),
    }
    return data


# ══════════════════════════════════════════════════════════════════════════════
# TestAuditWithAllDataSources
# ══════════════════════════════════════════════════════════════════════════════


class TestAuditWithAllDataSources:
    """Test that the audit PDF generates correctly with all possible
    combinations of data."""

    def test_all_data_present(self, tmp_path):
        """Full data -> all pages render."""
        out = tmp_path / "test_full.pdf"
        _build_pdf(_full_data(), out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_no_competitor_deep_dive(self, tmp_path):
        """Missing competitor_deep_dive.json -> pages degrade gracefully."""
        data = _full_data()
        data["competitor_deep_dive"] = {}
        out = tmp_path / "test_no_deep_dive.pdf"
        _build_pdf(data, out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_no_impact_estimate(self, tmp_path):
        """Missing impact_estimate.json -> INR figures show placeholder."""
        data = _full_data()
        data["impact_estimate"] = {}
        out = tmp_path / "test_no_impact.pdf"
        _build_pdf(data, out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_no_brand_intel(self, tmp_path):
        """Missing brand_intel.json -> concepts still generate without
        product specificity."""
        data = _full_data()
        data["brand_intel"] = {}
        out = tmp_path / "test_no_brand_intel.pdf"
        _build_pdf(data, out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_no_data_at_all(self, tmp_path, in_memory_db):
        """Only brand exists in DB, no ads, no JSON files -> PDF still
        generates."""
        in_memory_db.execute(
            "INSERT INTO brands (name, is_client, category) "
            "VALUES (?, 1, 'skincare')",
            ("EmptyBrand",),
        )
        in_memory_db.commit()

        with patch("deliverables.audit_generator.get_connection",
                   return_value=in_memory_db):
            out = run("EmptyBrand", output_dir=str(tmp_path))

        assert out.exists()
        assert out.stat().st_size > 0

    def test_no_fatigue_no_intel_no_impact(self, tmp_path):
        """Core analysis outputs all missing — only raw DB data."""
        data = _minimal_data()
        data["client_ads"] = [
            {"id": 1, "is_active": 1, "creative_type": "static",
             "duration_days": 10},
        ]
        data["fatigue_analysis"] = {}
        data["category_intel"] = {}
        data["impact_estimate"] = {}
        data["brand_intel"] = {}
        data["competitor_deep_dive"] = {}
        out = tmp_path / "test_nothing.pdf"
        _build_pdf(data, out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_none_values_in_data_sources(self, tmp_path):
        """None instead of {} for data sources — no crash."""
        data = _minimal_data()
        data["fatigue_analysis"] = None
        data["category_intel"] = None
        data["impact_estimate"] = None
        data["brand_intel"] = None
        data["competitor_deep_dive"] = None
        data["profitability_summary"] = None
        out = tmp_path / "test_nones.pdf"
        _build_pdf(data, out)
        assert out.exists()
        assert out.stat().st_size > 0


# ══════════════════════════════════════════════════════════════════════════════
# Brand Intel edge cases
# ══════════════════════════════════════════════════════════════════════════════


class TestBrandIntelEdgeCases:

    def test_brand_not_in_db(self, in_memory_db):
        """Should raise ValueError with clear message."""
        from analysis.brand_intel import run as bi_run
        with patch("analysis.brand_intel.get_connection",
                   return_value=in_memory_db):
            with pytest.raises(ValueError, match="not found in DB"):
                bi_run("NonexistentBrand")

    def test_brand_with_zero_ads(self, in_memory_db):
        """Brand exists but has 0 ads — returns empty lists, no crash."""
        from analysis.brand_intel import run as bi_run
        in_memory_db.execute(
            "INSERT INTO brands (name, is_client, category) "
            "VALUES (?, 1, 'skincare')",
            ("EmptyBrand",),
        )
        in_memory_db.commit()

        with patch("analysis.brand_intel.get_connection",
                   return_value=in_memory_db):
            result = bi_run("EmptyBrand")

        assert result["total_ads"] == 0
        assert result["ads_with_copy"] == 0
        assert result["products_detected"] == []
        assert result["price_points"] == []
        assert result["key_ingredients"] == []

    def test_none_ad_copy_handled(self):
        """Ads with None ad_copy should be filtered out."""
        from analysis.brand_intel import (
            extract_products,
            extract_prices,
            extract_ingredients,
        )
        # These functions receive pre-filtered ad_copies list
        # Verify they handle empty list
        assert extract_products([]) == []
        assert extract_prices([]) == []
        assert extract_ingredients([]) == []

    def test_empty_string_ad_copy_handled(self):
        """Empty-string ad copy filtered before extraction."""
        from analysis.brand_intel import extract_products
        # Empty strings and whitespace-only should be filtered at run() level
        assert extract_products([""]) == []
        assert extract_products(["   "]) == []

    def test_hindi_text_with_english_product_names(self):
        """Product name extraction works for English names in Hindi text."""
        from analysis.brand_intel import extract_products
        copies = [
            "\u0906\u092a\u0915\u0940 \u0924\u094d\u0935\u091a\u093e "
            "Vitamin C Serum \u0938\u0947 \u091a\u092e\u0915\u0947\u0917\u0940",
            "\u0928\u092f\u093e Vitamin C Serum \u0905\u092d\u0940 "
            "\u0916\u0930\u0940\u0926\u0947\u0902",
        ]
        products = extract_products(copies)
        names = [p["name"] for p in products]
        assert "Vitamin C Serum" in names

    def test_language_profile_pure_hindi(self):
        """Pure Devanagari text detected as hindi primary."""
        from analysis.brand_intel import detect_language_profile
        copies = [
            "\u0906\u092a\u0915\u0940 \u0924\u094d\u0935\u091a\u093e "
            "\u092c\u0926\u0932 \u091c\u093e\u090f\u0917\u0940",
        ]
        profile = detect_language_profile(copies)
        assert profile["has_hindi"] is True
        assert profile["primary"] == "hindi"


# ══════════════════════════════════════════════════════════════════════════════
# Competitor Deep Dive edge cases
# ══════════════════════════════════════════════════════════════════════════════


class TestCompetitorDeepDiveEdgeCases:

    def test_competitor_with_zero_ads(self, in_memory_db):
        """Competitors with 0 ads get empty profile, don't crash."""
        from analysis.competitor_deep_dive import run as cdd_run
        in_memory_db.execute(
            "INSERT INTO brands (name, is_client, category) "
            "VALUES (?, 1, 'skincare')",
            ("Client",),
        )
        in_memory_db.execute(
            "INSERT INTO brands (name, is_client, category) "
            "VALUES (?, 0, 'skincare')",
            ("EmptyComp",),
        )
        in_memory_db.commit()

        with patch("analysis.competitor_deep_dive.get_connection",
                   return_value=in_memory_db):
            result = cdd_run("Client", ["EmptyComp"])

        profile = result["per_competitor"]["EmptyComp"]
        assert profile["active_ads"] == 0
        assert profile["top_winners"] == []
        assert profile["win_rate"] == 0.0

    def test_competitor_not_in_db(self, in_memory_db):
        """Unknown competitor returns empty profile, not a crash."""
        from analysis.competitor_deep_dive import run as cdd_run
        in_memory_db.execute(
            "INSERT INTO brands (name, is_client, category) "
            "VALUES (?, 1, 'skincare')",
            ("Client",),
        )
        in_memory_db.commit()

        with patch("analysis.competitor_deep_dive.get_connection",
                   return_value=in_memory_db):
            result = cdd_run("Client", ["GhostBrand"])

        profile = result["per_competitor"]["GhostBrand"]
        assert profile["active_ads"] == 0

    def test_no_profitable_ads(self, in_memory_db):
        """Competitor with ads but none profitable -> empty top_winners."""
        from analysis.competitor_deep_dive import run as cdd_run
        in_memory_db.execute(
            "INSERT INTO brands (name, is_client) VALUES (?, 1)", ("Client",))
        in_memory_db.execute(
            "INSERT INTO brands (name, is_client) VALUES (?, 0)", ("Comp",))
        # duration_days is a generated column — set start_date/last_seen_date
        # to produce short durations (< 21 days = not profitable)
        in_memory_db.execute(
            "INSERT INTO ads (brand_id, ad_library_id, creative_type, "
            "is_active, start_date, last_seen_date, scraped_at) "
            "VALUES (2, 'C001', 'static', 1, '2026-03-25', '2026-03-30', "
            "datetime('now'))")
        in_memory_db.execute(
            "INSERT INTO ads (brand_id, ad_library_id, creative_type, "
            "is_active, start_date, last_seen_date, scraped_at) "
            "VALUES (2, 'C002', 'video', 1, '2026-03-20', '2026-03-30', "
            "datetime('now'))")
        in_memory_db.commit()

        with patch("analysis.competitor_deep_dive.get_connection",
                   return_value=in_memory_db):
            result = cdd_run("Client", ["Comp"])

        profile = result["per_competitor"]["Comp"]
        assert profile["top_winners"] == []
        assert profile["profitable_ads"] == 0

    def test_null_trigger_and_hook_structure(self, in_memory_db):
        """Ads with NULL trigger/hook_structure use 'unknown'."""
        from analysis.competitor_deep_dive import run as cdd_run
        in_memory_db.execute(
            "INSERT INTO brands (name, is_client) VALUES (?, 1)", ("Client",))
        in_memory_db.execute(
            "INSERT INTO brands (name, is_client) VALUES (?, 0)", ("Comp",))
        # duration_days is generated — use start/last_seen for 30 days
        in_memory_db.execute(
            "INSERT INTO ads (brand_id, ad_library_id, creative_type, "
            "is_active, start_date, last_seen_date, ad_copy, scraped_at) "
            "VALUES (2, 'C001', 'static', 1, '2026-03-01', '2026-03-31', "
            "'Some copy', datetime('now'))")
        # analysis row with NULL trigger and hook_structure
        in_memory_db.execute(
            "INSERT INTO ad_analysis (ad_id, analysis_json, analyzed_at) "
            "VALUES (1, '{}', datetime('now'))")
        in_memory_db.commit()

        with patch("analysis.competitor_deep_dive.get_connection",
                   return_value=in_memory_db):
            result = cdd_run("Client", ["Comp"])

        winners = result["per_competitor"]["Comp"]["top_winners"]
        assert len(winners) == 1
        assert winners[0]["psychological_trigger"] == "unknown"
        assert winners[0]["hook_structure"] == "unknown"

    def test_missing_category_intel_falls_back(self):
        """why_it_works falls back to duration-only when no category_intel."""
        from analysis.competitor_deep_dive import build_why_it_works
        result = build_why_it_works(
            trigger="transformation",
            hook_structure="bold_claim",
            duration_days=45,
            category_intel=None,
        )
        assert "45 days" in result
        assert "category" not in result.lower()


# ══════════════════════════════════════════════════════════════════════════════
# Impact Estimator edge cases
# ══════════════════════════════════════════════════════════════════════════════


class TestImpactEstimatorEdgeCases:

    def test_zero_active_ads(self, in_memory_db):
        """Brand with 0 active ads -> spend=0, waste=0."""
        from analysis.impact_estimator import run as ie_run
        in_memory_db.execute(
            "INSERT INTO brands (name, is_client, category) "
            "VALUES (?, 1, 'skincare')",
            ("EmptyBrand",),
        )
        in_memory_db.commit()

        with patch("analysis.impact_estimator.get_connection",
                   return_value=in_memory_db):
            result = ie_run("EmptyBrand", [])

        assert result["daily_spend"]["amount"] == 0.0
        assert result["waste_breakdown"]["total_estimated_monthly_waste"] == 0.0

    def test_negative_daily_spend_raises(self, in_memory_db):
        """Negative daily_spend_inr should raise ValueError."""
        from analysis.impact_estimator import run as ie_run
        in_memory_db.execute(
            "INSERT INTO brands (name, is_client) VALUES (?, 1)",
            ("TestBrand",),
        )
        in_memory_db.commit()

        with patch("analysis.impact_estimator.get_connection",
                   return_value=in_memory_db):
            with pytest.raises(ValueError, match="must be positive"):
                ie_run("TestBrand", [], daily_spend_inr=-1000)

    def test_zero_daily_spend_raises(self, in_memory_db):
        """Zero daily_spend_inr should raise ValueError."""
        from analysis.impact_estimator import run as ie_run
        in_memory_db.execute(
            "INSERT INTO brands (name, is_client) VALUES (?, 1)",
            ("TestBrand",),
        )
        in_memory_db.commit()

        with patch("analysis.impact_estimator.get_connection",
                   return_value=in_memory_db):
            with pytest.raises(ValueError, match="must be positive"):
                ie_run("TestBrand", [], daily_spend_inr=0)

    def test_missing_fatigue_data(self, in_memory_db):
        """Missing fatigue JSON -> fatigue waste = 0, no crash."""
        from analysis.impact_estimator import (
            calculate_fatigue_waste,
            calculate_refresh_waste,
        )
        assert calculate_fatigue_waste(None, 500.0) == 0.0
        assert calculate_refresh_waste(None, 500.0, 10) == 0.0

    def test_no_division_by_zero_in_refresh_waste(self):
        """avg_duration=0 should not cause ZeroDivisionError."""
        from analysis.impact_estimator import calculate_refresh_waste
        fatigue = {
            "fatigue_index": {"avg_duration": 0.0},
        }
        result = calculate_refresh_waste(fatigue, 500.0, 10)
        assert result == 0.0

    def test_brand_not_in_db_raises(self, in_memory_db):
        """Unknown brand raises ValueError."""
        from analysis.impact_estimator import run as ie_run
        with patch("analysis.impact_estimator.get_connection",
                   return_value=in_memory_db):
            with pytest.raises(ValueError, match="not found"):
                ie_run("GhostBrand", [])
