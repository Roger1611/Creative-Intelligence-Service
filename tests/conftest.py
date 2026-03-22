"""Shared fixtures for the test suite."""

import sqlite3
import pytest
from pathlib import Path


# ── Sample ad data ────────────────────────────────────────────────────────────

SAMPLE_ADS = [
    {
        "id": 1,
        "ad_library_id": "AD_001",
        "brand_id": 1,
        "creative_type": "static",
        "ad_copy": "Transform your skin in 7 days with our new serum. Clinically tested.",
        "cta_type": "Shop Now",
        "image_path": None,
        "thumbnail_url": "https://example.com/thumb1.jpg",
        "start_date": "2025-12-01",
        "last_seen_date": "2026-02-15",
        "duration_days": 76,
        "is_active": 1,
        "scraped_at": "2026-03-01T00:00:00",
    },
    {
        "id": 2,
        "ad_library_id": "AD_002",
        "brand_id": 1,
        "creative_type": "video",
        "ad_copy": "Our customers are raving about the new hydrating gel. See the reviews.",
        "cta_type": "Shop Now",
        "image_path": None,
        "thumbnail_url": "https://example.com/thumb2.jpg",
        "start_date": "2026-01-10",
        "last_seen_date": "2026-03-15",
        "duration_days": 64,
        "is_active": 1,
        "scraped_at": "2026-03-01T00:00:00",
    },
    {
        "id": 3,
        "ad_library_id": "AD_003",
        "brand_id": 1,
        "creative_type": "carousel",
        "ad_copy": "5 reasons dermatologists love this moisturizer. Number 3 will surprise you.",
        "cta_type": "Learn More",
        "image_path": None,
        "thumbnail_url": "https://example.com/thumb3.jpg",
        "start_date": "2026-02-01",
        "last_seen_date": "2026-03-20",
        "duration_days": 47,
        "is_active": 1,
        "scraped_at": "2026-03-01T00:00:00",
    },
    {
        "id": 4,
        "ad_library_id": "AD_004",
        "brand_id": 1,
        "creative_type": "static",
        "ad_copy": "Limited time: buy 2 get 1 free on all face washes!",
        "cta_type": "Shop Now",
        "image_path": None,
        "thumbnail_url": "https://example.com/thumb4.jpg",
        "start_date": "2026-03-10",
        "last_seen_date": "2026-03-18",
        "duration_days": 8,
        "is_active": 1,
        "scraped_at": "2026-03-15T00:00:00",
    },
    {
        "id": 5,
        "ad_library_id": "AD_005",
        "brand_id": 1,
        "creative_type": "reel",
        "ad_copy": "Watch how our vitamin C serum works on real skin. Before and after results.",
        "cta_type": "Shop Now",
        "image_path": None,
        "thumbnail_url": "https://example.com/thumb5.jpg",
        "start_date": "2026-03-01",
        "last_seen_date": "2026-03-20",
        "duration_days": 19,
        "is_active": 1,
        "scraped_at": "2026-03-10T00:00:00",
    },
]


_make_ad_counter = 0


def _make_ad(**overrides):
    """Create a sample ad dict with overrides. Each call gets a unique ID and thumbnail."""
    global _make_ad_counter
    _make_ad_counter += 1
    n = _make_ad_counter
    base = {
        "id": 1000 + n,
        "ad_library_id": f"AD_GEN_{n}",
        "brand_id": 1,
        "creative_type": "static",
        "ad_copy": f"Test ad copy number {n}.",
        "cta_type": "Shop Now",
        "image_path": None,
        "thumbnail_url": f"https://example.com/gen_{n}.jpg",
        "start_date": "2026-01-01",
        "last_seen_date": "2026-03-01",
        "duration_days": 59,
        "is_active": 1,
        "scraped_at": "2026-03-01T00:00:00",
    }
    base.update(overrides)
    return base


@pytest.fixture
def sample_ads():
    """Return a copy of the sample ads list."""
    return [dict(ad) for ad in SAMPLE_ADS]


@pytest.fixture
def make_ad():
    """Factory fixture for creating ads with overrides."""
    return _make_ad


@pytest.fixture
def in_memory_db(tmp_path):
    """
    Create an in-memory SQLite database with the project schema.
    Returns the connection.
    """
    schema_path = Path(__file__).parent.parent / "db" / "schema.sql"
    schema = schema_path.read_text(encoding="utf-8")

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(schema)
    return conn
