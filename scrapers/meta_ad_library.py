"""
scrapers/meta_ad_library.py — DEPRECATED.

The Playwright-based Meta Ad Library scraper has been replaced by
scrapers/apify_scraper.py (Apify actor) and scrapers/video_downloader.py
(video download + transcription + frame extraction).

This file is kept for:
  - _load_manual_fallback() — used as fallback when scraping fails
  - _save_raw() — used by apify_scraper
  - _upsert_brand(), _upsert_ads(), _ensure_competitor_set() — used by CLI
  - _cli() — legacy CLI entrypoint (now raises NotImplementedError)

All Playwright-specific functions have been removed.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Optional

from config import RAW_DIR, get_connection, init_db
from scrapers.utils import load_selectors, safe_brand_slug

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Public API (deprecated)
# ══════════════════════════════════════════════════════════════════════════════

def run(
    brand_name:  str,
    competitors: list[str] | None = None,
    country:     str = "IN",
    category:    str | None = None,
    max_pages:   int = 5,
    max_ads:     int = 0,
) -> dict:
    """Deprecated. Use ``scrapers.apify_scraper.run()`` instead."""
    raise NotImplementedError(
        "meta_ad_library.py has been replaced by apify_scraper.py. "
        "Use scrapers.apify_scraper.run() instead."
    )


# ══════════════════════════════════════════════════════════════════════════════
# Helpers (still in use)
# ══════════════════════════════════════════════════════════════════════════════

def _parse_date_from_text(text: str) -> Optional[str]:
    """Parse a date from text like 'Started running on 15 March, 2024' -> '2024-03-15'."""
    MONTHS = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
        "jan": 1, "feb": 2, "mar": 3, "apr": 4,
        "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }

    text = text.replace(",", " ").lower()

    m = re.search(
        r"(\d{1,2})\s+([a-z]+)\s+(\d{4})"
        r"|"
        r"([a-z]+)\s+(\d{1,2})\s+(\d{4})",
        text,
    )
    if m:
        if m.group(1):
            day, month_str, year = int(m.group(1)), m.group(2), int(m.group(3))
        else:
            month_str, day, year = m.group(4), int(m.group(5)), int(m.group(6))
        month = MONTHS.get(month_str[:3])
        if month:
            try:
                return date(year, month, day).isoformat()
            except ValueError:
                pass

    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return m.group(0)

    return None


def _compute_duration(start_date_iso: Optional[str], today_str: str) -> Optional[int]:
    if not start_date_iso:
        return None
    try:
        start = date.fromisoformat(start_date_iso)
        today = date.fromisoformat(today_str)
        delta = (today - start).days
        return max(0, delta)
    except ValueError:
        return None


# ── DB persistence ─────────────────────────────────────────────────────────────

def _upsert_brand(name: str, is_client: bool = False, category: Optional[str] = None) -> int:
    with get_connection() as conn:
        row = conn.execute("SELECT id FROM brands WHERE name = ?", (name,)).fetchone()
        if row:
            conn.execute(
                "UPDATE brands SET is_client = MAX(is_client, ?), "
                "category = COALESCE(?, category) WHERE id = ?",
                (int(is_client), category, row["id"]),
            )
            return row["id"]
        cur = conn.execute(
            "INSERT INTO brands (name, is_client, category) VALUES (?, ?, ?)",
            (name, int(is_client), category),
        )
        return cur.lastrowid


def _upsert_ads(brand_id: int, ads: list[dict]) -> int:
    count = 0
    with get_connection() as conn:
        for ad in ads:
            conn.execute(
                """INSERT INTO ads (
                       brand_id, ad_library_id, creative_type, ad_copy,
                       cta_type, image_path, thumbnail_url,
                       start_date, last_seen_date, is_active, scraped_at,
                       caption, transcript, transcript_language,
                       frames_path, video_url
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(ad_library_id) DO UPDATE SET
                       last_seen_date     = excluded.last_seen_date,
                       image_path         = COALESCE(excluded.image_path, image_path),
                       is_active          = excluded.is_active,
                       scraped_at         = excluded.scraped_at,
                       caption            = COALESCE(excluded.caption, caption),
                       transcript         = COALESCE(excluded.transcript, transcript),
                       transcript_language = COALESCE(excluded.transcript_language, transcript_language),
                       frames_path        = COALESCE(excluded.frames_path, frames_path),
                       video_url          = COALESCE(excluded.video_url, video_url)""",
                (
                    brand_id,
                    ad["ad_library_id"],
                    ad.get("creative_type"),
                    ad.get("ad_copy"),
                    ad.get("cta_type"),
                    ad.get("image_path"),
                    ad.get("thumbnail_url"),
                    ad.get("start_date"),
                    ad.get("last_seen_date"),
                    int(ad.get("is_active", True)),
                    ad.get("scraped_at"),
                    ad.get("caption"),
                    ad.get("transcript"),
                    ad.get("transcript_language"),
                    ad.get("frames_path"),
                    ad.get("video_url"),
                ),
            )
            count += 1
    return count


def _ensure_competitor_set(client_id: int, competitor_id: int) -> None:
    with get_connection() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO competitor_sets (client_brand_id, competitor_brand_id) "
            "VALUES (?, ?)",
            (client_id, competitor_id),
        )


# ── Misc helpers ───────────────────────────────────────────────────────────────

def _load_selectors_safe() -> dict:
    try:
        return load_selectors("meta_ad_library")
    except FileNotFoundError:
        logger.error("scraper_config.json not found — scraping will likely fail.")
        return {}
    except KeyError:
        logger.error("No 'meta_ad_library' section in scraper_config.json.")
        return {}


def _load_manual_fallback(brand_name: str) -> list[dict]:
    path = RAW_DIR / f"{brand_name.lower().replace(' ', '_')}_manual.json"
    if path.exists():
        logger.info("Loading manual fallback for '%s' from %s", brand_name, path)
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            logger.error("Manual fallback JSON is malformed: %s", exc)
    else:
        logger.warning(
            "No manual fallback at %s. "
            "Create it with ad data to bypass scraping.", path
        )
    return []


def _save_raw(brand_name: str, data: dict) -> None:
    safe = safe_brand_slug(brand_name)
    out = RAW_DIR / safe / "meta_ad_library_raw.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Raw scrape saved -> %s", out)


# ══════════════════════════════════════════════════════════════════════════════
# CLI (legacy — now raises NotImplementedError)
# ══════════════════════════════════════════════════════════════════════════════

def _cli() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s -- %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        prog="python -m scrapers.meta_ad_library",
        description="DEPRECATED. Use scrapers.apify_scraper instead.",
    )
    parser.add_argument("--brand",       required=True, help="Primary brand to scrape")
    parser.add_argument("--competitors", default="",
                        help="Comma-separated competitor brand names")
    parser.add_argument("--country",     default="IN", help="ISO country code (default: IN)")
    parser.add_argument("--category",    choices=["skincare","supplements","fashion","food","wellness"],
                        help="Brand category (optional)")
    parser.add_argument("--max-pages",   type=int, default=5,
                        help="Max scroll-page loads per brand (default: 5)")
    parser.add_argument("--max-ads",     type=int, default=0,
                        help="Max ads to extract per brand (0 = unlimited)")
    args = parser.parse_args()

    competitors = [c.strip() for c in args.competitors.split(",") if c.strip()]

    init_db()

    results = run(
        brand_name=args.brand,
        competitors=competitors,
        country=args.country,
        category=args.category,
        max_pages=args.max_pages,
        max_ads=args.max_ads,
    )

    brand_id = _upsert_brand(args.brand, is_client=True, category=args.category)
    n_brand  = _upsert_ads(brand_id, results["brand"])
    logger.info("[OK] %s: %d ads stored (brand_id=%d)", args.brand, n_brand, brand_id)

    for comp_name, comp_ads in results["competitors"].items():
        comp_id = _upsert_brand(comp_name, is_client=False, category=args.category)
        n_comp  = _upsert_ads(comp_id, comp_ads)
        _ensure_competitor_set(brand_id, comp_id)
        logger.info("  -> Competitor %s: %d ads stored (brand_id=%d)", comp_name, n_comp, comp_id)

    total = n_brand + sum(
        len(v) for v in results["competitors"].values()
    )
    logger.info("Done. %d total ads scraped and persisted.", total)


if __name__ == "__main__":
    _cli()
