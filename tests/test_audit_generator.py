"""Tests for deliverables/audit_generator.py — PDF generation with graceful degradation."""

import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from deliverables.audit_generator import (
    run,
    _build_pdf,
    _gather_data,
    _build_gaps,
    _format_inr,
    _get_total_monthly_waste,
)


# ══════════════════════════════════════════════════════════════════════════════
# _format_inr
# ══════════════════════════════════════════════════════════════════════════════


class TestFormatInr:

    def test_small_number(self):
        assert _format_inr(500) == "\u20b9500"

    def test_thousands(self):
        assert _format_inr(1234) == "\u20b91,234"

    def test_lakhs(self):
        assert _format_inr(123456) == "\u20b91,23,456"

    def test_ten_lakhs(self):
        assert _format_inr(1234567) == "\u20b912,34,567"

    def test_crore(self):
        assert _format_inr(12345678) == "\u20b91,23,45,678"

    def test_zero(self):
        assert _format_inr(0) == "\u20b90"

    def test_negative(self):
        assert _format_inr(-50000) == "-\u20b950,000"

    def test_float_rounds(self):
        result = _format_inr(1234.7)
        assert result == "\u20b91,235"

    def test_exact_thousand(self):
        assert _format_inr(1000) == "\u20b91,000"

    def test_exact_lakh(self):
        assert _format_inr(100000) == "\u20b91,00,000"


# ══════════════════════════════════════════════════════════════════════════════
# _get_total_monthly_waste
# ══════════════════════════════════════════════════════════════════════════════


class TestGetTotalMonthlyWaste:

    def test_empty_data(self):
        assert _get_total_monthly_waste({}) == 0.0

    def test_direct_field(self):
        data = {"total_estimated_monthly_waste": 50000}
        assert _get_total_monthly_waste(data) == 50000.0

    def test_sum_per_gap(self):
        data = {
            "per_gap_impact": [
                {"estimated_monthly_impact_inr": 10000},
                {"estimated_monthly_impact_inr": 20000},
                {"estimated_monthly_impact_inr": 5000},
            ]
        }
        assert _get_total_monthly_waste(data) == 35000.0

    def test_direct_field_takes_priority(self):
        data = {
            "total_estimated_monthly_waste": 99000,
            "per_gap_impact": [
                {"estimated_monthly_impact_inr": 10000},
            ]
        }
        assert _get_total_monthly_waste(data) == 99000.0


# ══════════════════════════════════════════════════════════════════════════════
# End-to-end: minimal data -> PDF without crash
# ══════════════════════════════════════════════════════════════════════════════


class TestAuditMinimalData:

    def test_audit_generates_with_minimal_data(self, tmp_path, in_memory_db):
        """Audit PDF should generate even with minimal data -- no crashes."""
        in_memory_db.execute(
            "INSERT INTO brands (name, is_client, category) VALUES (?, 1, 'skincare')",
            ("TestBrand",),
        )
        in_memory_db.execute(
            "INSERT INTO ads (brand_id, ad_library_id, creative_type, is_active, scraped_at) "
            "VALUES (1, 'TEST_001', 'static', 1, datetime('now'))",
        )
        in_memory_db.execute(
            "INSERT INTO ads (brand_id, ad_library_id, creative_type, is_active, scraped_at) "
            "VALUES (1, 'TEST_002', 'video', 1, datetime('now'))",
        )
        in_memory_db.commit()

        with patch("deliverables.audit_generator.get_connection", return_value=in_memory_db):
            out = run("TestBrand", output_dir=str(tmp_path))

        assert out.exists()
        assert out.suffix == ".pdf"
        assert out.stat().st_size > 0

    def test_audit_generates_with_no_active_ads(self, tmp_path, in_memory_db):
        """Audit should work even when brand has zero ads."""
        in_memory_db.execute(
            "INSERT INTO brands (name, is_client, category) VALUES (?, 1, 'skincare')",
            ("EmptyBrand",),
        )
        in_memory_db.commit()

        with patch("deliverables.audit_generator.get_connection", return_value=in_memory_db):
            out = run("EmptyBrand", output_dir=str(tmp_path))

        assert out.exists()
        assert out.stat().st_size > 0


# ══════════════════════════════════════════════════════════════════════════════
# _build_pdf with synthetic data dict
# ══════════════════════════════════════════════════════════════════════════════


def _minimal_data():
    return {
        "brand": {"id": 1, "name": "TestBrand", "is_client": 1, "category": "skincare"},
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


class TestBuildPdfDirect:

    def test_empty_data_produces_pdf(self, tmp_path):
        """All pages should render with empty data dicts -- no KeyError/TypeError."""
        out = tmp_path / "test_empty.pdf"
        _build_pdf(_minimal_data(), out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_full_fatigue_data_renders(self, tmp_path):
        """Pages render correctly with complete fatigue analysis data."""
        data = _minimal_data()
        data["fatigue_analysis"] = {
            "fatigue_score": 65.0,
            "score_interpretation": "HIGH",
            "creative_coverage": {
                "ratio": 0.33,
                "client_count": 5,
                "benchmark": 15,
                "deficit": 10,
                "interpretation": "Underfeeding Meta's algorithm by 67%",
            },
            "fatigue_index": {
                "index": 2.1,
                "avg_duration": 21.0,
                "benchmark": 10,
                "severity": "CRITICAL",
                "interpretation": "Average ad is 21 days past optimal refresh window",
            },
            "hook_diversity": {
                "score": 20.0,
                "triggers_used": ["fear", "transformation"],
                "triggers_missing": [
                    "status", "social_proof", "agitation_solution",
                    "curiosity", "urgency", "authority", "belonging", "aspiration",
                ],
                "hook_structures_used": ["question"],
                "hook_structures_missing": [
                    "number_lead", "pattern_interrupt", "direct_address",
                    "curiosity_gap", "transformation", "social_proof_lead",
                    "urgency_lead", "authority_lead", "bold_claim",
                ],
                "interpretation": "Using 2/10 psychological angles, 1/10 hook structures",
            },
            "critical_ads": [
                {"ad_library_id": "AD_001", "duration_days": 45, "creative_type": "static"},
            ],
            "warning_ads": [],
        }
        out = tmp_path / "test_fatigue.pdf"
        _build_pdf(data, out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_full_intel_data_renders(self, tmp_path):
        """Pages render correctly with complete category intel data."""
        data = _minimal_data()
        data["category_intel"] = {
            "profitable_ads_in_universe": 25,
            "trigger_analysis": {
                "by_profitable_only": {
                    "transformation": 8,
                    "fear": 5,
                    "social_proof": 4,
                },
                "profitable_rate_by_trigger": {
                    "transformation": 60,
                    "fear": 45,
                    "social_proof": 50,
                },
            },
            "format_analysis": {
                "video": {"total_pct": 40, "winner_pct": 55, "win_rate": 65},
                "static": {"total_pct": 35, "winner_pct": 25, "win_rate": 40},
                "carousel": {"total_pct": 25, "winner_pct": 20, "win_rate": 45},
            },
            "hook_structure_analysis": {
                "by_profitable_only": {
                    "question": 10,
                    "bold_claim": 7,
                },
                "profitable_rate_by_hook": {
                    "question": 55,
                    "bold_claim": 60,
                },
            },
            "hook_database": {
                "transformation": {
                    "count": 8,
                    "pct_of_winners": 32.0,
                    "hooks": [
                        {"text": "Watch my skin transform in 7 days",
                         "source_brand": "CompA", "duration_days": 45,
                         "ad_library_id": "COMP_001",
                         "spoken_hook": "Watch how this works",
                         "hook_structure": "bold_claim"},
                    ],
                },
            },
            "visual_pattern_stats": {
                "total_analyzed": 20,
                "face_dominant_pct": 60.0,
                "text_overlay_pct": 75.0,
                "minimal_aesthetic_pct": 15.0,
                "before_after_pct": 30.0,
                "product_focused_pct": 40.0,
                "ugc_style_pct": 25.0,
            },
            "patterns": [
                "Transformation hooks dominate profitable ads",
                "Video format over-indexes in winners",
            ],
        }
        out = tmp_path / "test_intel.pdf"
        _build_pdf(data, out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_concepts_page_renders(self, tmp_path):
        """Page 8 renders with sample concepts."""
        data = _minimal_data()
        data["sample_concepts"] = [
            {
                "hook_text": "Your skincare routine is making it worse",
                "psychological_angle": "fear",
                "body_script": "Most people don't realize their cleanser strips natural oils.",
                "visual_direction": "Close-up of woman touching irritated skin",
                "text_overlay": "Is your cleanser the problem?",
                "production_difficulty": "low",
                "competitor_inspiration": "Plum's fear-based hook (running 35 days)",
                "data_backing": "Fear hooks have 45% win rate in skincare",
            },
            {
                "hook_text": "10,000 women switched to this serum",
                "psychological_angle": "social_proof",
                "body_script": "Join the movement.",
                "visual_direction": {"scene_description": "UGC montage of real customers",
                                     "talent_direction": "Diverse women, natural lighting",
                                     "product_placement": "Serum bottle in hand"},
            },
        ]
        out = tmp_path / "test_concepts.pdf"
        _build_pdf(data, out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_action_plan_with_waste_report(self, tmp_path):
        """Page 9 renders with waste report recommendations."""
        data = _minimal_data()
        data["waste_report"] = {
            "recommendations_json": json.dumps([
                {"priority": "high", "action": "Pause fatigued ads", "signal": "3 ads over 30 days"},
                {"priority": "medium", "action": "Add video format", "signal": "0% video usage"},
            ]),
        }
        out = tmp_path / "test_actions.pdf"
        _build_pdf(data, out)
        assert out.exists()
        assert out.stat().st_size > 0


# ══════════════════════════════════════════════════════════════════════════════
# All new data sources present
# ══════════════════════════════════════════════════════════════════════════════


class TestBuildPdfAllDataSources:

    def _full_data(self):
        data = _minimal_data()
        data["client_ads"] = [
            {"id": 1, "is_active": 1, "creative_type": "static",
             "duration_days": 25, "psychological_trigger": "fear",
             "copy_tone": "urgent", "effectiveness_score": 7},
            {"id": 2, "is_active": 1, "creative_type": "video",
             "duration_days": 10, "psychological_trigger": "transformation",
             "copy_tone": "warm", "effectiveness_score": 8, "transcript": "Great product"},
        ]
        data["competitors"] = [
            {
                "brand": {"id": 2, "name": "CompA", "is_client": 0, "category": "skincare"},
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
                "deficit": 13, "interpretation": "Underfeeding by 87%",
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
                        {"text": "Watch my skin transform in just 7 days with this serum",
                         "source_brand": "CompA", "duration_days": 45,
                         "ad_library_id": "C001", "hook_structure": "bold_claim",
                         "spoken_hook": "Watch how this works"},
                        {"text": "Before and after using this cream for 2 weeks",
                         "source_brand": "CompA", "duration_days": 30,
                         "ad_library_id": "C002", "hook_structure": "transformation"},
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
            "products_detected": ["Gotukola Face Wash", "Vitamin C Serum", "Retinol Night Cream"],
            "price_points": ["499", "799", "999"],
            "key_ingredients": ["gotukola", "vitamin c", "retinol"],
            "language_profile": {"primary": "english", "secondary": "hindi"},
        }
        data["competitor_deep_dive"] = {
            "competitive_landscape_summary": (
                "The skincare category is dominated by transformation-led "
                "video content. CompA leads in creative velocity with 2x "
                "the ad volume of the average competitor."),
            "competitor_profiles": [
                {
                    "name": "CompA", "active_ads": 15, "win_rate": 60,
                    "dominant_trigger": "transformation",
                    "creative_velocity": "High (5 new/week)",
                },
            ],
            "top_winners": {
                "CompA": [
                    {
                        "hook_text": "Watch my skin transform in just 7 days",
                        "duration_days": 45,
                        "psychological_trigger": "transformation",
                        "hook_structure": "bold_claim",
                        "why_it_works": "Combines specific timeframe with visual proof",
                        "visual_layout": "Close-up face, split screen before/after",
                    },
                    {
                        "hook_text": "Dermatologists don't want you to know this",
                        "duration_days": 38,
                        "psychological_trigger": "curiosity",
                        "hook_structure": "curiosity_gap",
                        "why_it_works": "Authority rebellion + curiosity gap",
                        "visual_layout": "Doctor in lab coat, dramatic zoom",
                    },
                    {
                        "hook_text": "I stopped using expensive serums and here's what happened",
                        "duration_days": 32,
                        "psychological_trigger": "agitation_solution",
                        "hook_structure": "direct_address",
                        "why_it_works": "Anti-premium positioning resonates with value seekers",
                        "visual_layout": "UGC selfie style, bathroom setting",
                    },
                ],
            },
        }
        data["impact_estimate"] = {
            "total_estimated_monthly_waste": 85000,
            "per_gap_impact": [
                {
                    "gap_type": "ANGLE GAP",
                    "gap_title": "Zero Social Proof Creatives",
                    "estimated_monthly_impact_inr": 35000,
                    "confidence": "high",
                },
                {
                    "gap_type": "ANGLE GAP",
                    "gap_title": "Zero Curiosity Creatives",
                    "estimated_monthly_impact_inr": 25000,
                    "confidence": "medium",
                },
                {
                    "gap_type": "HOOK STRUCTURE GAP",
                    "gap_title": "No 'Bold Claim' Hooks",
                    "estimated_monthly_impact_inr": 15000,
                    "confidence": "medium",
                },
            ],
            "sprint_roi": {
                "sprint_price": 25000,
                "estimated_monthly_savings": 85000,
                "payback_days": 9,
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
                    "talent_direction": "Woman touching face, concerned expression",
                    "product_placement": "Gotukola Face Wash revealed at end",
                },
                "competitor_inspiration": "CompA's fear hook (running 38 days)",
                "production_difficulty": "low",
                "data_backing": "Fear hooks: 50% win rate, 6 profitable ads in category",
            },
        ]
        data["waste_report"] = {
            "recommendations_json": json.dumps([
                {"priority": "high", "action": "Pause 1 fatigued ad",
                 "signal": "AD running 25 days"},
            ]),
        }
        return data

    def test_full_data_produces_pdf(self, tmp_path):
        """PDF generates without crash when ALL data sources are present."""
        out = tmp_path / "test_full.pdf"
        _build_pdf(self._full_data(), out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_full_data_pdf_larger_than_minimal(self, tmp_path):
        """Full-data PDF should be meaningfully larger than empty PDF."""
        full_out = tmp_path / "full.pdf"
        empty_out = tmp_path / "empty.pdf"
        _build_pdf(self._full_data(), full_out)
        _build_pdf(_minimal_data(), empty_out)
        assert full_out.stat().st_size > empty_out.stat().st_size


# ══════════════════════════════════════════════════════════════════════════════
# Graceful degradation: missing data sources
# ══════════════════════════════════════════════════════════════════════════════


class TestGracefulDegradation:

    def test_missing_competitor_deep_dive(self, tmp_path):
        """PDF generates without crash when competitor_deep_dive is missing."""
        data = _minimal_data()
        data["competitor_deep_dive"] = {}
        data["client_ads"] = [
            {"id": 1, "is_active": 1, "creative_type": "static", "duration_days": 10},
        ]
        out = tmp_path / "test_no_deep_dive.pdf"
        _build_pdf(data, out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_missing_impact_estimate(self, tmp_path):
        """PDF generates without crash when impact_estimate is missing."""
        data = _minimal_data()
        data["impact_estimate"] = {}
        out = tmp_path / "test_no_impact.pdf"
        _build_pdf(data, out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_missing_brand_intel(self, tmp_path):
        """PDF generates without crash when brand_intel is missing."""
        data = _minimal_data()
        data["brand_intel"] = {}
        out = tmp_path / "test_no_brand_intel.pdf"
        _build_pdf(data, out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_missing_all_new_sources(self, tmp_path):
        """PDF generates when all three new sources are missing."""
        data = _minimal_data()
        data["brand_intel"] = {}
        data["competitor_deep_dive"] = {}
        data["impact_estimate"] = {}
        out = tmp_path / "test_no_new_sources.pdf"
        _build_pdf(data, out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_only_impact_estimate_present(self, tmp_path):
        """PDF generates with only impact estimate (no deep dive or brand intel)."""
        data = _minimal_data()
        data["impact_estimate"] = {
            "total_estimated_monthly_waste": 50000,
            "sprint_roi": {
                "sprint_price": 15000,
                "estimated_monthly_savings": 50000,
                "payback_days": 9,
            },
        }
        out = tmp_path / "test_impact_only.pdf"
        _build_pdf(data, out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_only_competitor_deep_dive_present(self, tmp_path):
        """PDF generates with only competitor deep dive data."""
        data = _minimal_data()
        data["competitor_deep_dive"] = {
            "competitive_landscape_summary": "CompA leads the market.",
            "competitor_profiles": [
                {"name": "CompA", "active_ads": 10, "win_rate": 50,
                 "dominant_trigger": "transformation", "creative_velocity": "Medium"},
            ],
            "top_winners": {
                "CompA": [
                    {"hook_text": "Test hook", "duration_days": 30,
                     "why_it_works": "It resonates", "psychological_trigger": "fear"},
                ],
            },
        }
        out = tmp_path / "test_deep_dive_only.pdf"
        _build_pdf(data, out)
        assert out.exists()
        assert out.stat().st_size > 0


# ══════════════════════════════════════════════════════════════════════════════
# _build_gaps
# ══════════════════════════════════════════════════════════════════════════════


class TestBuildGaps:

    def test_no_data_returns_empty(self):
        data = {
            "brand": {"id": 1, "name": "Test", "category": "skincare"},
            "client_ads": [],
            "fatigue_analysis": {},
            "category_intel": {},
            "impact_estimate": {},
        }
        gaps = _build_gaps(data)
        assert gaps == []

    def test_angle_gaps_detected(self):
        data = {
            "brand": {"id": 1, "name": "Test", "category": "skincare"},
            "client_ads": [{"is_active": 1, "creative_type": "static"}],
            "fatigue_analysis": {
                "hook_diversity": {
                    "triggers_used": ["fear"],
                    "hook_structures_used": ["question"],
                },
            },
            "category_intel": {
                "trigger_analysis": {
                    "by_profitable_only": {
                        "fear": 5,
                        "transformation": 10,
                    },
                },
                "format_analysis": {},
                "hook_structure_analysis": {},
            },
            "impact_estimate": {},
        }
        gaps = _build_gaps(data)
        angle_gaps = [g for g in gaps if g["type"] == "ANGLE GAP"]
        assert len(angle_gaps) >= 1
        assert any("Transformation" in g["title"] for g in angle_gaps)

    def test_format_gaps_detected(self):
        data = {
            "brand": {"id": 1, "name": "Test", "category": "skincare"},
            "client_ads": [
                {"is_active": 1, "creative_type": "static"},
                {"is_active": 1, "creative_type": "static"},
            ],
            "fatigue_analysis": {"hook_diversity": {}},
            "category_intel": {
                "trigger_analysis": {"by_profitable_only": {}},
                "format_analysis": {
                    "video": {"total_pct": 40, "winner_pct": 55, "win_rate": 65},
                },
                "hook_structure_analysis": {},
            },
            "impact_estimate": {},
        }
        gaps = _build_gaps(data)
        format_gaps = [g for g in gaps if g["type"] == "FORMAT GAP"]
        assert len(format_gaps) >= 1
        assert any("Video" in g["title"] for g in format_gaps)

    def test_gaps_sorted_by_impact_then_type(self):
        """Gaps with higher ₹ impact should come first."""
        data = {
            "brand": {"id": 1, "name": "Test", "category": "skincare"},
            "client_ads": [{"is_active": 1, "creative_type": "static"}],
            "fatigue_analysis": {
                "hook_diversity": {
                    "triggers_used": [],
                    "hook_structures_used": [],
                },
            },
            "category_intel": {
                "trigger_analysis": {
                    "by_profitable_only": {"transformation": 10},
                },
                "format_analysis": {
                    "video": {"total_pct": 40, "winner_pct": 55, "win_rate": 65},
                },
                "hook_structure_analysis": {
                    "by_profitable_only": {"question": 15},
                    "profitable_rate_by_hook": {"question": 60},
                },
            },
            "impact_estimate": {
                "per_gap_impact": [
                    {"gap_type": "FORMAT GAP", "gap_title": "No Video Ads",
                     "estimated_monthly_impact_inr": 50000, "confidence": "high"},
                    {"gap_type": "ANGLE GAP", "gap_title": "Zero Transformation Creatives",
                     "estimated_monthly_impact_inr": 10000, "confidence": "medium"},
                ],
            },
        }
        gaps = _build_gaps(data)
        # First gap should be the one with higher impact (FORMAT GAP: 50000)
        assert len(gaps) >= 2
        assert gaps[0].get("estimated_monthly_impact", 0) >= gaps[1].get("estimated_monthly_impact", 0)

    def test_gaps_include_impact_data(self):
        """Gaps should include estimated_monthly_impact when available."""
        data = {
            "brand": {"id": 1, "name": "Test", "category": "skincare"},
            "client_ads": [{"is_active": 1, "creative_type": "static"}],
            "fatigue_analysis": {
                "hook_diversity": {
                    "triggers_used": [],
                    "hook_structures_used": [],
                },
            },
            "category_intel": {
                "trigger_analysis": {
                    "by_profitable_only": {"transformation": 10},
                },
                "format_analysis": {},
                "hook_structure_analysis": {},
            },
            "impact_estimate": {
                "per_gap_impact": [
                    {"gap_type": "ANGLE GAP",
                     "gap_title": "Zero Transformation Creatives",
                     "estimated_monthly_impact_inr": 30000,
                     "confidence": "high"},
                ],
            },
        }
        gaps = _build_gaps(data)
        angle_gaps = [g for g in gaps if g["type"] == "ANGLE GAP"]
        assert len(angle_gaps) >= 1
        matched = [g for g in angle_gaps if "Transformation" in g["title"]]
        assert len(matched) == 1
        assert matched[0].get("estimated_monthly_impact") == 30000
        assert matched[0].get("confidence") == "high"
