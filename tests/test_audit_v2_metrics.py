"""Tests for Audit V2 metric functions in fatigue_scorer and category_intel."""

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from analysis.fatigue_scorer import (
    _creative_coverage_ratio,
    _creative_fatigue_index,
    _hook_diversity_score,
)
from analysis.category_intel import (
    _build_hook_database,
    _visual_pattern_stats,
)


# ══════════════════════════════════════════════════════════════════════════════
# _creative_coverage_ratio
# ══════════════════════════════════════════════════════════════════════════════


class TestCreativeCoverageRatio:

    def test_deficit_produces_underfeeding_message(self):
        result = _creative_coverage_ratio(5, 20.0)
        assert result["ratio"] < 1.0
        assert result["deficit"] == 15
        assert result["benchmark"] == 20
        assert "Underfeeding" in result["interpretation"]

    def test_healthy_ratio(self):
        result = _creative_coverage_ratio(25, 20.0)
        assert result["ratio"] >= 1.0
        assert result["deficit"] == 0
        assert "meets category benchmarks" in result["interpretation"]

    def test_exactly_at_benchmark(self):
        result = _creative_coverage_ratio(15, 10.0)
        assert result["ratio"] == 1.0
        assert result["deficit"] == 0
        assert "meets category benchmarks" in result["interpretation"]

    def test_zero_client_count(self):
        result = _creative_coverage_ratio(0, 20.0)
        assert result["ratio"] == 0.0
        assert result["deficit"] == 20
        assert "Underfeeding" in result["interpretation"]

    def test_zero_competitor_avg_uses_benchmark(self):
        """When competitor avg is 0, should fall back to CREATIVE_COVERAGE_BENCHMARK."""
        result = _creative_coverage_ratio(10, 0.0)
        assert result["benchmark"] == 15  # CREATIVE_COVERAGE_BENCHMARK
        assert result["ratio"] == round(10 / 15, 3)

    def test_competitor_avg_below_benchmark_uses_benchmark(self):
        """Benchmark should be max(competitor_avg, CREATIVE_COVERAGE_BENCHMARK)."""
        result = _creative_coverage_ratio(10, 5.0)
        assert result["benchmark"] == 15  # CREATIVE_COVERAGE_BENCHMARK > 5


# ══════════════════════════════════════════════════════════════════════════════
# _creative_fatigue_index
# ══════════════════════════════════════════════════════════════════════════════


class TestCreativeFatigueIndex:

    def test_low_severity(self):
        result = _creative_fatigue_index(5.0, refresh_benchmark=10)
        assert result["severity"] == "LOW"
        assert result["index"] == 0.5
        assert "within optimal" in result["interpretation"]

    def test_moderate_severity(self):
        result = _creative_fatigue_index(12.0, refresh_benchmark=10)
        assert result["severity"] == "MODERATE"
        assert result["index"] == 1.2

    def test_high_severity(self):
        result = _creative_fatigue_index(18.0, refresh_benchmark=10)
        assert result["severity"] == "HIGH"
        assert result["index"] == 1.8

    def test_critical_severity(self):
        result = _creative_fatigue_index(25.0, refresh_benchmark=10)
        assert result["severity"] == "CRITICAL"
        assert result["index"] == 2.5
        assert "past optimal" in result["interpretation"]

    def test_zero_duration(self):
        result = _creative_fatigue_index(0.0, refresh_benchmark=10)
        assert result["severity"] == "LOW"
        assert result["index"] == 0.0

    def test_boundary_at_1_0(self):
        result = _creative_fatigue_index(10.0, refresh_benchmark=10)
        assert result["severity"] == "MODERATE"
        assert result["index"] == 1.0

    def test_boundary_at_1_5(self):
        result = _creative_fatigue_index(15.0, refresh_benchmark=10)
        assert result["severity"] == "MODERATE"  # 1.5 is within 1.0-1.5 range (inclusive)
        assert result["index"] == 1.5

    def test_boundary_at_2_0(self):
        result = _creative_fatigue_index(20.0, refresh_benchmark=10)
        assert result["severity"] == "HIGH"
        assert result["index"] == 2.0

    def test_uses_config_default(self):
        """Should use REFRESH_BENCHMARK_DAYS from config when no benchmark given."""
        result = _creative_fatigue_index(10.0)
        assert result["benchmark"] == 10  # REFRESH_BENCHMARK_DAYS


# ══════════════════════════════════════════════════════════════════════════════
# _hook_diversity_score
# ══════════════════════════════════════════════════════════════════════════════


class TestHookDiversityScore:

    def _setup_db(self):
        """Create in-memory DB with schema and sample data."""
        schema_path = Path(__file__).parent.parent / "db" / "schema.sql"
        schema = schema_path.read_text(encoding="utf-8")
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(schema)

        # Insert brand
        conn.execute(
            "INSERT INTO brands (id, name, is_client) VALUES (1, 'TestBrand', 1)"
        )
        # Insert active ads
        conn.execute(
            "INSERT INTO ads (id, brand_id, ad_library_id, is_active, start_date, last_seen_date) "
            "VALUES (1, 1, 'AD1', 1, '2026-01-01', '2026-03-01')"
        )
        conn.execute(
            "INSERT INTO ads (id, brand_id, ad_library_id, is_active, start_date, last_seen_date) "
            "VALUES (2, 1, 'AD2', 1, '2026-01-01', '2026-03-01')"
        )
        # Insert analyses with 2 triggers and 1 hook structure
        conn.execute(
            "INSERT INTO ad_analysis (ad_id, psychological_trigger, hook_structure) "
            "VALUES (1, 'fear', 'question')"
        )
        conn.execute(
            "INSERT INTO ad_analysis (ad_id, psychological_trigger, hook_structure) "
            "VALUES (2, 'status', 'bold_claim')"
        )
        conn.commit()
        return conn

    def test_score_with_sample_data(self):
        conn = self._setup_db()
        with patch("analysis.fatigue_scorer.get_connection", return_value=conn):
            result = _hook_diversity_score(1)

        assert result["score"] > 0
        assert "fear" in result["triggers_used"]
        assert "status" in result["triggers_used"]
        assert len(result["triggers_used"]) == 2
        # 8 triggers missing (10 total - 2 used)
        assert len(result["triggers_missing"]) == 8
        assert "question" in result["hook_structures_used"]
        assert "bold_claim" in result["hook_structures_used"]
        assert "2/10 psychological angles" in result["interpretation"]

    def test_empty_db(self):
        """No analysis rows should yield score 0."""
        schema_path = Path(__file__).parent.parent / "db" / "schema.sql"
        schema = schema_path.read_text(encoding="utf-8")
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(schema)
        conn.execute(
            "INSERT INTO brands (id, name, is_client) VALUES (1, 'Empty', 1)"
        )
        conn.commit()

        with patch("analysis.fatigue_scorer.get_connection", return_value=conn):
            result = _hook_diversity_score(1)

        assert result["score"] == 0
        assert len(result["triggers_used"]) == 0
        assert len(result["triggers_missing"]) == 10


# ══════════════════════════════════════════════════════════════════════════════
# _build_hook_database
# ══════════════════════════════════════════════════════════════════════════════


class TestBuildHookDatabase:

    def _make_data(self):
        brand_rows = [
            {"id": 1, "name": "BrandA", "is_client": 0},
            {"id": 2, "name": "BrandB", "is_client": 0},
        ]
        all_ads = [
            {"id": 1, "brand_id": 1, "ad_library_id": "AD1",
             "ad_copy": "Transform your skin\nSecond line", "duration_days": 45,
             "transcript": "Watch how this works. It's amazing.", "is_active": 1},
            {"id": 2, "brand_id": 1, "ad_library_id": "AD2",
             "ad_copy": "Get glowing skin now\nBody text", "duration_days": 30,
             "transcript": None, "is_active": 1},
            {"id": 3, "brand_id": 2, "ad_library_id": "AD3",
             "ad_copy": "Fear of aging?\nStop it", "duration_days": 50,
             "transcript": "Are you worried about wrinkles? Try this.", "is_active": 1},
            {"id": 4, "brand_id": 2, "ad_library_id": "AD4",
             "ad_copy": "", "duration_days": 25,
             "transcript": None, "is_active": 1},  # empty ad_copy, should be skipped
            {"id": 5, "brand_id": 1, "ad_library_id": "AD5",
             "ad_copy": "Social proof hook\nDetails", "duration_days": 10,
             "transcript": None, "is_active": 1},  # not profitable
        ]
        profitable_ads = [
            {"id": 1}, {"id": 2}, {"id": 3}, {"id": 4},
        ]
        all_analyses = [
            {"ad_id": 1, "psychological_trigger": "transformation"},
            {"ad_id": 2, "psychological_trigger": "transformation"},
            {"ad_id": 3, "psychological_trigger": "fear"},
            {"ad_id": 4, "psychological_trigger": "fear"},  # will be skipped (empty ad_copy)
            {"ad_id": 5, "psychological_trigger": "social_proof"},  # not profitable
        ]
        return all_analyses, all_ads, profitable_ads, brand_rows

    def test_hooks_clustered_by_trigger(self):
        all_analyses, all_ads, profitable_ads, brand_rows = self._make_data()
        result = _build_hook_database(all_analyses, all_ads, profitable_ads, brand_rows)

        assert "transformation" in result
        assert "fear" in result
        # social_proof ad (id=5) is not in profitable_ads
        assert "social_proof" not in result

    def test_hooks_sorted_by_duration_desc(self):
        all_analyses, all_ads, profitable_ads, brand_rows = self._make_data()
        result = _build_hook_database(all_analyses, all_ads, profitable_ads, brand_rows)

        hooks = result["transformation"]["hooks"]
        assert hooks[0]["duration_days"] >= hooks[1]["duration_days"]

    def test_hooks_capped_at_8(self):
        """Even with many hooks per trigger, only 8 should be returned."""
        brand_rows = [{"id": 1, "name": "Brand", "is_client": 0}]
        all_ads = [
            {"id": i, "brand_id": 1, "ad_library_id": f"AD{i}",
             "ad_copy": f"Hook text {i}\nBody", "duration_days": i * 5,
             "transcript": None, "is_active": 1}
            for i in range(1, 12)
        ]
        profitable_ads = [{"id": i} for i in range(1, 12)]
        all_analyses = [
            {"ad_id": i, "psychological_trigger": "transformation"}
            for i in range(1, 12)
        ]

        result = _build_hook_database(all_analyses, all_ads, profitable_ads, brand_rows)
        assert len(result["transformation"]["hooks"]) == 8
        assert result["transformation"]["count"] == 11

    def test_spoken_hook_extracted(self):
        all_analyses, all_ads, profitable_ads, brand_rows = self._make_data()
        result = _build_hook_database(all_analyses, all_ads, profitable_ads, brand_rows)

        # Ad 1 has transcript "Watch how this works. It's amazing."
        trans_hooks = result["transformation"]["hooks"]
        ad1_hook = next(h for h in trans_hooks if h["ad_library_id"] == "AD1")
        assert ad1_hook["spoken_hook"] == "Watch how this works."

    def test_empty_ad_copy_skipped(self):
        all_analyses, all_ads, profitable_ads, brand_rows = self._make_data()
        result = _build_hook_database(all_analyses, all_ads, profitable_ads, brand_rows)

        # Ad 4 has empty ad_copy, should be skipped even though it has "fear" trigger
        fear_hooks = result["fear"]["hooks"]
        ad_ids = [h["ad_library_id"] for h in fear_hooks]
        assert "AD4" not in ad_ids

    def test_pct_of_winners(self):
        all_analyses, all_ads, profitable_ads, brand_rows = self._make_data()
        result = _build_hook_database(all_analyses, all_ads, profitable_ads, brand_rows)

        # 4 profitable ads total; transformation has 2 hooks
        assert result["transformation"]["pct_of_winners"] == round(2 / 4 * 100, 1)


# ══════════════════════════════════════════════════════════════════════════════
# _visual_pattern_stats
# ══════════════════════════════════════════════════════════════════════════════


class TestVisualPatternStats:

    def test_keyword_detection(self):
        profitable_ads = [{"id": 1}, {"id": 2}, {"id": 3}]
        all_analyses = [
            {"ad_id": 1, "visual_layout": "Close-up of woman with product bottle"},
            {"ad_id": 2, "visual_layout": "Minimal white background with text overlay"},
            {"ad_id": 3, "visual_layout": "Before and after transformation split view"},
        ]
        result = _visual_pattern_stats(all_analyses, profitable_ads)

        assert result["total_analyzed"] == 3
        assert result["face_dominant_pct"] > 0       # "close-up", "woman"
        assert result["product_focused_pct"] > 0     # "product", "bottle"
        assert result["minimal_aesthetic_pct"] > 0   # "minimal", "white background"
        assert result["text_overlay_pct"] > 0        # "text overlay"
        assert result["before_after_pct"] > 0        # "before", "after", "transformation"

    def test_no_visual_layout_data(self):
        profitable_ads = [{"id": 1}, {"id": 2}]
        all_analyses = [
            {"ad_id": 1, "visual_layout": None},
            {"ad_id": 2, "visual_layout": None},
        ]
        result = _visual_pattern_stats(all_analyses, profitable_ads)

        assert result["total_analyzed"] == 0
        assert result["face_dominant_pct"] == 0
        assert result["ugc_style_pct"] == 0

    def test_non_profitable_excluded(self):
        profitable_ads = [{"id": 1}]  # only ad 1 is profitable
        all_analyses = [
            {"ad_id": 1, "visual_layout": "Face close-up portrait"},
            {"ad_id": 2, "visual_layout": "UGC phone selfie raw"},  # not profitable
        ]
        result = _visual_pattern_stats(all_analyses, profitable_ads)

        assert result["total_analyzed"] == 1
        assert result["face_dominant_pct"] == 100.0
        assert result["ugc_style_pct"] == 0  # ad 2 not included

    def test_ugc_detection(self):
        profitable_ads = [{"id": 1}]
        all_analyses = [
            {"ad_id": 1, "visual_layout": "UGC style handheld phone testimonial video"},
        ]
        result = _visual_pattern_stats(all_analyses, profitable_ads)
        assert result["ugc_style_pct"] == 100.0

    def test_percentages_correct(self):
        profitable_ads = [{"id": i} for i in range(1, 5)]
        all_analyses = [
            {"ad_id": 1, "visual_layout": "Face portrait"},
            {"ad_id": 2, "visual_layout": "Face close-up"},
            {"ad_id": 3, "visual_layout": "Product bottle on shelf"},
            {"ad_id": 4, "visual_layout": "Abstract gradient design"},
        ]
        result = _visual_pattern_stats(all_analyses, profitable_ads)

        assert result["total_analyzed"] == 4
        assert result["face_dominant_pct"] == 50.0  # 2/4
        assert result["product_focused_pct"] == 25.0  # 1/4
