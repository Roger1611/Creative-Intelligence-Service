"""Tests for analysis/structurer.py — deduplication and diversity scoring."""

import pytest
from analysis.structurer import _deduplicate, _diversity_score, _format_distribution


class TestDeduplicate:
    """Test the three-pass deduplication logic."""

    def test_no_duplicates(self, sample_ads):
        """All unique ads should pass through unchanged."""
        result = _deduplicate(sample_ads)
        assert len(result) == len(sample_ads)

    def test_exact_id_dedup(self, make_ad):
        """Duplicate ad_library_ids should be collapsed, keeping longer run."""
        ads = [
            make_ad(ad_library_id="DUP_1", duration_days=10),
            make_ad(ad_library_id="DUP_1", duration_days=30),
            make_ad(ad_library_id="UNIQUE_1", duration_days=5),
        ]
        result = _deduplicate(ads)
        assert len(result) == 2
        dup = [a for a in result if a["ad_library_id"] == "DUP_1"][0]
        assert dup["duration_days"] == 30

    def test_copy_fingerprint_dedup(self, make_ad):
        """Same ad copy with different IDs should be collapsed."""
        same_copy = "Buy now and save 50% on all skincare products today!"
        ads = [
            make_ad(ad_library_id="A1", ad_copy=same_copy, duration_days=15),
            make_ad(ad_library_id="A2", ad_copy=same_copy, duration_days=25),
            make_ad(ad_library_id="A3", ad_copy="Totally different copy here.",
                    duration_days=10),
        ]
        result = _deduplicate(ads)
        assert len(result) == 2
        # The longer-running duplicate should survive
        copies = [a["ad_copy"] for a in result]
        assert same_copy in copies

    def test_thumbnail_dedup(self, make_ad):
        """Same thumbnail URL should be collapsed."""
        ads = [
            make_ad(ad_library_id="T1", ad_copy="Copy one",
                    thumbnail_url="https://example.com/same.jpg",
                    duration_days=5),
            make_ad(ad_library_id="T2", ad_copy="Copy two",
                    thumbnail_url="https://example.com/same.jpg",
                    duration_days=20),
            make_ad(ad_library_id="T3", ad_copy="Copy three",
                    thumbnail_url="https://example.com/different.jpg",
                    duration_days=10),
        ]
        result = _deduplicate(ads)
        assert len(result) == 2
        thumb_survivor = [a for a in result
                          if a["thumbnail_url"] == "https://example.com/same.jpg"][0]
        assert thumb_survivor["duration_days"] == 20

    def test_empty_list(self):
        """Empty input should return empty output."""
        assert _deduplicate([]) == []

    def test_no_copy_no_thumbnail(self, make_ad):
        """Ads without copy or thumbnail should not be deduped on those passes."""
        ads = [
            make_ad(ad_library_id="NC1", ad_copy=None, thumbnail_url=None),
            make_ad(ad_library_id="NC2", ad_copy=None, thumbnail_url=None),
        ]
        result = _deduplicate(ads)
        assert len(result) == 2

    def test_keeps_longer_duration_on_all_passes(self, make_ad):
        """Across all three passes, the ad with longer duration should win."""
        copy = "Exact same copy for dedup test"
        thumb = "https://example.com/shared_thumb.jpg"
        ads = [
            make_ad(ad_library_id="X1", ad_copy=copy,
                    thumbnail_url=thumb, duration_days=5),
            make_ad(ad_library_id="X2", ad_copy=copy,
                    thumbnail_url=thumb, duration_days=50),
        ]
        result = _deduplicate(ads)
        assert len(result) == 1
        assert result[0]["duration_days"] == 50


class TestFormatDistribution:
    """Test format counting and percentage calculation."""

    def test_mixed_formats(self, sample_ads):
        dist = _format_distribution(sample_ads)
        assert dist["static"]["count"] == 2
        assert dist["video"]["count"] == 1
        assert dist["carousel"]["count"] == 1
        assert dist["reel"]["count"] == 1
        # Percentages should sum to ~100
        total_pct = sum(v["pct"] for v in dist.values())
        assert 99.0 <= total_pct <= 101.0

    def test_single_format(self, make_ad):
        ads = [make_ad(creative_type="static") for _ in range(5)]
        dist = _format_distribution(ads)
        assert dist["static"]["count"] == 5
        assert dist["static"]["pct"] == 100.0

    def test_empty_ads(self):
        dist = _format_distribution([])
        # Should still have format keys with 0 counts
        assert dist["static"]["count"] == 0


class TestDiversityScore:
    """Test the 4-component diversity score (0-100)."""

    def test_diverse_portfolio(self, sample_ads):
        """5 ads across 4 formats should score well."""
        dist = _format_distribution(sample_ads)
        score = _diversity_score(sample_ads, dist)
        assert 0 <= score["total"] <= 100
        assert score["breakdown"]["format_variety"] > 0
        assert score["breakdown"]["copy_variation"] > 0
        assert score["breakdown"]["visual_variety"] > 0
        assert score["breakdown"]["creative_volume"] > 0

    def test_perfect_format_variety(self, make_ad):
        """All 4 formats present should give full format_variety points (25)."""
        ads = [
            make_ad(ad_library_id="F1", creative_type="static",
                    ad_copy="A", thumbnail_url="https://a.com/1"),
            make_ad(ad_library_id="F2", creative_type="video",
                    ad_copy="B", thumbnail_url="https://a.com/2"),
            make_ad(ad_library_id="F3", creative_type="carousel",
                    ad_copy="C", thumbnail_url="https://a.com/3"),
            make_ad(ad_library_id="F4", creative_type="reel",
                    ad_copy="D", thumbnail_url="https://a.com/4"),
        ]
        dist = _format_distribution(ads)
        score = _diversity_score(ads, dist)
        assert score["breakdown"]["format_variety"] == 25.0

    def test_single_format_variety(self, make_ad):
        """Only 1 format should give 0 format_variety points."""
        ads = [
            make_ad(ad_library_id=f"S{i}", creative_type="static",
                    ad_copy=f"Copy {i}", thumbnail_url=f"https://a.com/{i}")
            for i in range(5)
        ]
        dist = _format_distribution(ads)
        score = _diversity_score(ads, dist)
        assert score["breakdown"]["format_variety"] == 0.0

    def test_volume_benchmark(self, make_ad):
        """20+ ads should give full creative_volume points (25)."""
        ads = [
            make_ad(ad_library_id=f"V{i}", creative_type="static",
                    ad_copy=f"Unique copy number {i}")
            for i in range(25)
        ]
        dist = _format_distribution(ads)
        score = _diversity_score(ads, dist)
        assert score["breakdown"]["creative_volume"] == 25.0

    def test_empty_ads_zero_score(self):
        """Empty ads should return 0 total score."""
        score = _diversity_score([], {})
        assert score["total"] == 0.0

    def test_all_same_copy_low_variation(self, make_ad):
        """Identical copy across ads should yield low copy_variation."""
        same = "Exact same copy repeated"
        ads = [
            make_ad(ad_library_id=f"C{i}", ad_copy=same,
                    thumbnail_url=f"https://a.com/{i}")
            for i in range(10)
        ]
        dist = _format_distribution(ads)
        score = _diversity_score(ads, dist)
        # 1 unique copy / 10 = 0.1 * 25 = 2.5
        assert score["breakdown"]["copy_variation"] == 2.5
