"""Tests for deliverables/audit_generator.py — PDF generation with graceful degradation."""

import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from deliverables.audit_generator import run, _build_pdf, _gather_data, _build_gaps


# ══════════════════════════════════════════════════════════════════════════════
# End-to-end: minimal data → PDF without crash
# ══════════════════════════════════════════════════════════════════════════════


class TestAuditMinimalData:

    def test_audit_generates_with_minimal_data(self, tmp_path, in_memory_db):
        """Audit PDF should generate even with minimal data — no crashes."""
        # Insert minimal brand + 2 ads, no competitors, no analysis
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
            # No processed JSON files exist — should degrade gracefully
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


class TestBuildPdfDirect:

    def _minimal_data(self):
        return {
            "brand": {"id": 1, "name": "TestBrand", "is_client": 1, "category": "skincare"},
            "client_ads": [],
            "waste_report": {},
            "competitors": [],
            "sample_concepts": [],
            "fatigue_analysis": {},
            "category_intel": {},
            "profitability_summary": {},
        }

    def test_empty_data_produces_pdf(self, tmp_path):
        """All pages should render with empty data dicts — no KeyError/TypeError."""
        out = tmp_path / "test_empty.pdf"
        _build_pdf(self._minimal_data(), out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_full_fatigue_data_renders(self, tmp_path):
        """Pages render correctly with complete fatigue analysis data."""
        data = self._minimal_data()
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
        data = self._minimal_data()
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
                         "ad_library_id": "COMP_001", "spoken_hook": "Watch how this works"},
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
        data = self._minimal_data()
        data["sample_concepts"] = [
            {
                "hook_text": "Your skincare routine is making it worse",
                "psychological_angle": "fear",
                "body_script": "Most people don't realize their cleanser strips natural oils.",
                "visual_direction": "Close-up of woman touching irritated skin",
            },
            {
                "hook_text": "10,000 women switched to this serum",
                "psychological_angle": "social_proof",
                "body_script": "Join the movement.",
                "visual_direction": "UGC montage of real customers",
            },
        ]
        out = tmp_path / "test_concepts.pdf"
        _build_pdf(data, out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_action_plan_with_waste_report(self, tmp_path):
        """Page 9 renders with waste report recommendations."""
        data = self._minimal_data()
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
# _build_gaps
# ══════════════════════════════════════════════════════════════════════════════


class TestBuildGaps:

    def test_no_data_returns_empty(self):
        data = {
            "brand": {"id": 1, "name": "Test", "category": "skincare"},
            "client_ads": [],
            "fatigue_analysis": {},
            "category_intel": {},
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
        }
        gaps = _build_gaps(data)
        format_gaps = [g for g in gaps if g["type"] == "FORMAT GAP"]
        assert len(format_gaps) >= 1
        assert any("Video" in g["title"] for g in format_gaps)

    def test_gaps_sorted_by_type_priority(self):
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
        }
        gaps = _build_gaps(data)
        types = [g["type"] for g in gaps]
        # ANGLE GAP should come before FORMAT GAP which comes before HOOK STRUCTURE GAP
        type_order = {"ANGLE GAP": 0, "FORMAT GAP": 1, "HOOK STRUCTURE GAP": 2}
        orders = [type_order[t] for t in types]
        assert orders == sorted(orders)
