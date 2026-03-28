"""Tests for llm/chains.py — cost-optimisation helpers (no API calls)."""

import json
import pytest
from unittest.mock import patch
from llm.chains import (
    _validate_entity_diversity,
    _is_analysis_cached,
    _cluster_similar_ads,
    _build_slim_client_data,
)


class TestValidateEntityDiversity:
    """Test Entity ID deduplication logic."""

    def test_empty_list(self):
        assert _validate_entity_diversity([]) == []

    def test_no_duplicates(self):
        """All unique entity_id_tags should pass through unchanged."""
        concepts = [
            {"entity_id_tag": "question_face_closeup_skin", "thumb_stop_score": 7},
            {"entity_id_tag": "transformation_before_after_face", "thumb_stop_score": 8},
            {"entity_id_tag": "social_proof_ugc_testimonial_review", "thumb_stop_score": 6},
        ]
        result = _validate_entity_diversity(concepts)
        assert len(result) == 3

    def test_exact_tag_dedup(self):
        """Two concepts with the same entity_id_tag should be deduplicated."""
        concepts = [
            {"entity_id_tag": "transformation_before_after_face", "thumb_stop_score": 5},
            {"entity_id_tag": "transformation_before_after_face", "thumb_stop_score": 8},
            {"entity_id_tag": "question_skin_problem_closeup", "thumb_stop_score": 7},
        ]
        result = _validate_entity_diversity(concepts)
        assert len(result) == 2
        # Should keep the one with higher thumb_stop_score
        tags = [c["entity_id_tag"] for c in result]
        assert "transformation_before_after_face" in tags
        scores = {c["entity_id_tag"]: c["thumb_stop_score"] for c in result}
        assert scores["transformation_before_after_face"] == 8

    def test_visual_keyword_clustering(self):
        """Same hook_structure + 2+ shared visual keywords = clustered."""
        concepts = [
            {
                "hook_structure": "transformation",
                "entity_id_tag": "transformation_skincare_glow_a",
                "visual_direction": "before and after face skin closeup with product",
                "thumb_stop_score": 9,
            },
            {
                "hook_structure": "transformation",
                "entity_id_tag": "transformation_skincare_glow_b",
                "visual_direction": "dramatic face skin transformation reveal",
                "thumb_stop_score": 6,
            },
            {
                "hook_structure": "question",
                "entity_id_tag": "question_ingredient_deep",
                "visual_direction": "face skin closeup with serum bottle",
                "thumb_stop_score": 7,
            },
        ]
        result = _validate_entity_diversity(concepts)
        # First two share hook_structure="transformation" + visual keywords {face, skin}
        assert len(result) == 2
        # The transformation cluster should keep score=9
        trans_concepts = [c for c in result if c["hook_structure"] == "transformation"]
        assert len(trans_concepts) == 1
        assert trans_concepts[0]["thumb_stop_score"] == 9

    def test_different_hook_structure_not_clustered(self):
        """Different hook_structures should not cluster even with shared visual keywords."""
        concepts = [
            {
                "hook_structure": "transformation",
                "entity_id_tag": "transformation_face_skin_a",
                "visual_direction": "face skin product closeup before after",
                "thumb_stop_score": 7,
            },
            {
                "hook_structure": "question",
                "entity_id_tag": "question_face_skin_b",
                "visual_direction": "face skin product closeup with text",
                "thumb_stop_score": 6,
            },
        ]
        result = _validate_entity_diversity(concepts)
        assert len(result) == 2

    def test_one_shared_keyword_not_enough(self):
        """Only 1 shared visual keyword should not trigger clustering."""
        concepts = [
            {
                "hook_structure": "bold_claim",
                "entity_id_tag": "bold_claim_a",
                "visual_direction": "face with text overlay on white background",
                "thumb_stop_score": 7,
            },
            {
                "hook_structure": "bold_claim",
                "entity_id_tag": "bold_claim_b",
                "visual_direction": "face with product bottle on dark background",
                "thumb_stop_score": 8,
            },
        ]
        # Only "face" is shared — not enough for clustering (need 2+)
        result = _validate_entity_diversity(concepts)
        assert len(result) == 2

    def test_keeps_higher_thumb_stop_score(self):
        """In a cluster, keep the concept with the higher thumb_stop_score."""
        concepts = [
            {"entity_id_tag": "same_tag", "thumb_stop_score": 3},
            {"entity_id_tag": "same_tag", "thumb_stop_score": 10},
            {"entity_id_tag": "same_tag", "thumb_stop_score": 7},
        ]
        result = _validate_entity_diversity(concepts)
        assert len(result) == 1
        assert result[0]["thumb_stop_score"] == 10

    def test_none_fields_handled_gracefully(self):
        """Concepts with missing fields should not crash the validator."""
        concepts = [
            {"entity_id_tag": None, "hook_structure": None, "thumb_stop_score": 5},
            {"entity_id_tag": "", "hook_structure": "", "thumb_stop_score": 6},
            {"thumb_stop_score": 7},
        ]
        result = _validate_entity_diversity(concepts)
        assert len(result) == 3

    def test_single_concept(self):
        """Single concept should pass through unchanged."""
        concepts = [{"entity_id_tag": "only_one", "thumb_stop_score": 8}]
        result = _validate_entity_diversity(concepts)
        assert len(result) == 1

    def test_case_insensitive_tag_matching(self):
        """Entity ID tag comparison should be case-insensitive."""
        concepts = [
            {"entity_id_tag": "Transformation_Before_After", "thumb_stop_score": 5},
            {"entity_id_tag": "transformation_before_after", "thumb_stop_score": 9},
        ]
        result = _validate_entity_diversity(concepts)
        assert len(result) == 1
        assert result[0]["thumb_stop_score"] == 9


class TestIsAnalysisCached:
    """Test the analysis cache check (Change 1)."""

    def test_no_existing_analysis(self):
        """Ad with no existing analysis should not be cached."""
        ad = {"existing_analysis": None, "last_analyzed_at": None}
        assert _is_analysis_cached(ad) is False

    def test_recent_analysis_is_cached(self):
        """Ad analyzed recently should be cached."""
        from datetime import datetime, timedelta
        recent = (datetime.utcnow() - timedelta(days=3)).isoformat()
        ad = {
            "existing_analysis": json.dumps({"effectiveness_score": 7}),
            "last_analyzed_at": recent,
            "duration_days": 25,
        }
        assert _is_analysis_cached(ad) is True

    def test_stale_analysis_not_cached(self):
        """Ad analyzed more than 14 days ago should not be cached."""
        from datetime import datetime, timedelta
        old = (datetime.utcnow() - timedelta(days=20)).isoformat()
        ad = {
            "existing_analysis": json.dumps({"effectiveness_score": 7}),
            "last_analyzed_at": old,
            "duration_days": 25,
        }
        assert _is_analysis_cached(ad) is False

    def test_force_reanalyze_overrides_cache(self):
        """FORCE_REANALYZE=True should bypass cache."""
        from datetime import datetime, timedelta
        recent = (datetime.utcnow() - timedelta(days=1)).isoformat()
        ad = {
            "existing_analysis": json.dumps({"effectiveness_score": 7}),
            "last_analyzed_at": recent,
            "duration_days": 25,
        }
        with patch("llm.chains.FORCE_REANALYZE", True):
            assert _is_analysis_cached(ad) is False

    def test_corrupted_json_not_cached(self):
        """Corrupted analysis_json should not be cached."""
        from datetime import datetime, timedelta
        recent = (datetime.utcnow() - timedelta(days=1)).isoformat()
        ad = {
            "existing_analysis": "NOT VALID JSON {{{",
            "last_analyzed_at": recent,
        }
        assert _is_analysis_cached(ad) is False

    def test_invalid_date_not_cached(self):
        """Invalid analyzed_at date should not be cached."""
        ad = {
            "existing_analysis": json.dumps({"effectiveness_score": 7}),
            "last_analyzed_at": "not-a-date",
        }
        assert _is_analysis_cached(ad) is False


class TestClusterSimilarAds:
    """Test near-duplicate ad clustering (Change 5)."""

    def test_empty_list(self):
        reps, cluster_map = _cluster_similar_ads([])
        assert reps == []
        assert cluster_map == {}

    def test_no_clusters(self):
        """Ads with different types should not cluster."""
        ads = [
            {"ad_id": 1, "creative_type": "static", "brand_id": 1,
             "ad_copy": "Buy our serum now", "video_url": None, "duration_days": 10},
            {"ad_id": 2, "creative_type": "video", "brand_id": 1,
             "ad_copy": "Buy our serum now", "video_url": None, "duration_days": 15},
        ]
        reps, cluster_map = _cluster_similar_ads(ads)
        assert len(reps) == 2
        assert cluster_map == {}

    def test_same_video_url_clusters(self):
        """Ads with the same video_url should cluster."""
        ads = [
            {"ad_id": 1, "creative_type": "video", "brand_id": 1,
             "ad_copy": "Different copy A", "video_url": "https://vid.com/1",
             "duration_days": 30},
            {"ad_id": 2, "creative_type": "video", "brand_id": 1,
             "ad_copy": "Different copy B", "video_url": "https://vid.com/1",
             "duration_days": 10},
        ]
        reps, cluster_map = _cluster_similar_ads(ads)
        assert len(reps) == 1
        assert reps[0]["ad_id"] == 1  # longest duration
        assert len(cluster_map[1]) == 1  # ad_id=2 is non-rep

    def test_similar_copy_clusters(self):
        """Ads with very similar copy should cluster."""
        ads = [
            {"ad_id": 1, "creative_type": "static", "brand_id": 1,
             "ad_copy": "Transform your skin in 7 days with our amazing serum formula",
             "video_url": None, "duration_days": 20},
            {"ad_id": 2, "creative_type": "static", "brand_id": 1,
             "ad_copy": "Transform your skin in 7 days with our amazing serum blend",
             "video_url": None, "duration_days": 25},
        ]
        reps, cluster_map = _cluster_similar_ads(ads)
        assert len(reps) == 1
        assert reps[0]["ad_id"] == 2  # longest duration

    def test_different_brand_id_no_cluster(self):
        """Ads from different brands should not cluster."""
        ads = [
            {"ad_id": 1, "creative_type": "static", "brand_id": 1,
             "ad_copy": "Same exact copy here", "video_url": None, "duration_days": 10},
            {"ad_id": 2, "creative_type": "static", "brand_id": 2,
             "ad_copy": "Same exact copy here", "video_url": None, "duration_days": 15},
        ]
        reps, cluster_map = _cluster_similar_ads(ads)
        assert len(reps) == 2

    def test_keeps_longest_duration_as_rep(self):
        """Representative should be the ad with longest duration_days."""
        ads = [
            {"ad_id": 1, "creative_type": "static", "brand_id": 1,
             "ad_copy": "Same copy", "video_url": None, "duration_days": 5},
            {"ad_id": 2, "creative_type": "static", "brand_id": 1,
             "ad_copy": "Same copy", "video_url": None, "duration_days": 50},
            {"ad_id": 3, "creative_type": "static", "brand_id": 1,
             "ad_copy": "Same copy", "video_url": None, "duration_days": 20},
        ]
        reps, cluster_map = _cluster_similar_ads(ads)
        assert len(reps) == 1
        assert reps[0]["ad_id"] == 2
        assert len(cluster_map[2]) == 2


class TestBuildSlimClientData:
    """Test pre-aggregated waste diagnosis data (Change 3)."""

    def test_basic_aggregation(self):
        """Should produce all expected keys with correct counts."""
        ads = [
            {"ad_library_id": "A1", "creative_type": "static", "duration_days": 35,
             "ad_copy": "Long ad copy " * 20, "is_active": 1, "start_date": "2026-03-01",
             "psychological_trigger": "fear", "effectiveness_score": 6},
            {"ad_library_id": "A2", "creative_type": "video", "duration_days": 20,
             "ad_copy": "Warning ad", "is_active": 1, "start_date": "2026-03-10",
             "psychological_trigger": "urgency", "effectiveness_score": 8},
            {"ad_library_id": "A3", "creative_type": "static", "duration_days": 5,
             "ad_copy": "Recent ad", "is_active": 1, "start_date": "2026-03-25",
             "psychological_trigger": "fear", "effectiveness_score": 4},
        ]
        result = _build_slim_client_data("TestBrand", ads, 65.0)

        assert result["brand_name"] == "TestBrand"
        assert result["total_active_ads"] == 3
        assert result["total_ads"] == 3
        assert result["existing_diversity_score"] == 65.0
        assert result["format_distribution"]["static"] == 2
        assert result["format_distribution"]["video"] == 1
        assert result["trigger_distribution"]["fear"] == 2
        assert result["avg_duration_days"] == 20.0
        assert len(result["fatigued_ads"]) == 1  # 35 days
        assert len(result["warning_ads"]) == 1   # 20 days
        assert len(result["recent_ads"]) == 1    # 5 days

    def test_ad_copy_truncation(self):
        """Fatigued/warning ads should have copy truncated to 200 chars."""
        long_copy = "A" * 500
        ads = [
            {"ad_library_id": "A1", "creative_type": "static", "duration_days": 35,
             "ad_copy": long_copy, "is_active": 1, "start_date": "2026-01-01",
             "psychological_trigger": None, "effectiveness_score": None},
        ]
        result = _build_slim_client_data("TestBrand", ads, None)
        assert len(result["fatigued_ads"][0]["ad_copy"]) == 200

    def test_recent_ads_no_copy(self):
        """Recent ads should not include ad_copy (metadata only)."""
        ads = [
            {"ad_library_id": "A1", "creative_type": "static", "duration_days": 3,
             "ad_copy": "Some copy", "is_active": 1, "start_date": "2026-03-25",
             "psychological_trigger": None, "effectiveness_score": None},
        ]
        result = _build_slim_client_data("TestBrand", ads, None)
        assert "ad_copy" not in result["recent_ads"][0]

    def test_empty_ads(self):
        """Empty ad list should return zero counts."""
        result = _build_slim_client_data("TestBrand", [], None)
        assert result["total_ads"] == 0
        assert result["total_active_ads"] == 0
        assert result["avg_duration_days"] == 0
        assert result["fatigued_ads"] == []
        assert result["warning_ads"] == []
        assert result["recent_ads"] == []
