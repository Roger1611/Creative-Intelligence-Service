"""
analysis/structurer.py

Two distinct responsibilities:

  ingest(brand_name, raw_ads, ...)  — DB upsert called immediately after scraping.
                                       Returns brand_id.  Used by pipeline.py and
                                       the scraper CLI.

  run(brand_name, competitor_names) — Analysis pass: reads from DB, deduplicates,
                                       computes format distribution + creative
                                       diversity score, writes
                                       data/processed/{brand_name}.json.
                                       Returns the structured dict.
"""

from __future__ import annotations

import json
import logging
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Optional

from config import (
    PROC_DIR,
    VALID_CATEGORIES,
    VALID_CREATIVE_TYPES,
    get_connection,
)

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# ingest — DB persistence (called right after scraping)
# ══════════════════════════════════════════════════════════════════════════════

def ingest(
    brand_name:       str,
    raw_ads:          list[dict],
    is_client:        bool = False,
    category:         Optional[str] = None,
    website_url:      Optional[str] = None,
    instagram_handle: Optional[str] = None,
) -> int:
    """
    Upsert *raw_ads* for *brand_name* into the database.
    Returns the brand's id.
    """
    brand_id = _upsert_brand(
        brand_name,
        is_client=is_client,
        category=category,
        website_url=website_url,
        instagram_handle=instagram_handle,
    )
    n = _upsert_ads(brand_id, raw_ads)
    logger.info("Ingested %d ads for '%s' (brand_id=%d)", n, brand_name, brand_id)
    return brand_id


# ══════════════════════════════════════════════════════════════════════════════
# run — Analysis pass (called from pipeline after all brands are ingested)
# ══════════════════════════════════════════════════════════════════════════════

def run(brand_name: str, competitor_names: list[str]) -> dict:
    """
    Read ads from DB for *brand_name* and each competitor, deduplicate,
    compute format distribution and creative diversity score, and write
    data/processed/{brand_name}.json.

    Returns the structured dict.
    """
    all_names   = [brand_name] + list(competitor_names)
    logger.debug("Fetching brands from DB: %s", all_names)
    brand_rows  = _fetch_brands(all_names)
    name_to_id  = {b["name"]: b["id"] for b in brand_rows}
    logger.debug("Found brands in DB: %s", name_to_id)

    if brand_name not in name_to_id:
        raise ValueError(f"Brand '{brand_name}' not found in DB. Run ingest first.")

    output: dict = {
        "brand_name":      brand_name,
        "generated_at":    datetime.utcnow().isoformat(),
        "brands":          {},
    }

    for name in all_names:
        brand_id = name_to_id.get(name)
        if brand_id is None:
            logger.warning("Brand '%s' not in DB — skipping.", name)
            continue

        raw_ads   = _fetch_ads(brand_id)
        logger.info("'%s' (brand_id=%d): fetched %d ads from DB",
                     name, brand_id, len(raw_ads))
        clean_ads = _deduplicate(raw_ads)
        removed   = len(raw_ads) - len(clean_ads)
        if removed:
            logger.info("'%s': removed %d duplicate ads", name, removed)

        fmt_dist  = _format_distribution(clean_ads)
        diversity = _diversity_score(clean_ads, fmt_dist)
        copy_stats = _copy_stats(clean_ads)

        output["brands"][name] = {
            "brand_id":                name_to_id[name],
            "is_client":               bool(
                next(b["is_client"] for b in brand_rows if b["name"] == name)
            ),
            "total_ads":               len(clean_ads),
            "duplicates_removed":      removed,
            "format_distribution":     fmt_dist,
            "creative_diversity_score": diversity["total"],
            "diversity_breakdown":     diversity["breakdown"],
            "copy_stats":              copy_stats,
            "ads":                     clean_ads,
        }

    _write_processed(brand_name, output)
    logger.info(
        "Structured output written for '%s' (%d brands)",
        brand_name, len(output["brands"]),
    )
    return output


# ══════════════════════════════════════════════════════════════════════════════
# Deduplication
# ══════════════════════════════════════════════════════════════════════════════

def _deduplicate(ads: list[dict]) -> list[dict]:
    """
    Three-pass deduplication:
      1. By ad_library_id (exact — should be unique already, but scraper can
         return the same card twice from lazy-load glitches).
      2. By normalised copy fingerprint — same creative text, different ID.
         Keep the one with the longer run (more informative).
      3. By thumbnail URL — same visual, different IDs.
         Keep the longer-running variant.
    """
    # Pass 1 — exact ID dedup
    seen_ids: dict[str, dict] = {}
    for ad in ads:
        aid = ad.get("ad_library_id", "")
        if aid and aid not in seen_ids:
            seen_ids[aid] = ad
        elif aid:
            # Keep the one with greater duration_days
            existing_dur = seen_ids[aid].get("duration_days") or 0
            new_dur      = ad.get("duration_days") or 0
            if new_dur > existing_dur:
                seen_ids[aid] = ad
    deduped = list(seen_ids.values())

    # Pass 2 — copy fingerprint dedup
    copy_seen: dict[str, dict] = {}
    for ad in deduped:
        fp = _copy_fingerprint(ad.get("ad_copy"))
        if fp is None:          # no copy — can't deduplicate on copy
            copy_seen[ad["ad_library_id"]] = ad
            continue
        if fp not in copy_seen:
            copy_seen[fp] = ad
        else:
            existing_dur = copy_seen[fp].get("duration_days") or 0
            new_dur      = ad.get("duration_days") or 0
            if new_dur > existing_dur:
                copy_seen[fp] = ad
    deduped = list(copy_seen.values())

    # Pass 3 — thumbnail URL dedup
    thumb_seen: dict[str, dict] = {}
    for ad in deduped:
        url = (ad.get("thumbnail_url") or "").strip()
        if not url:
            thumb_seen[ad["ad_library_id"]] = ad
            continue
        if url not in thumb_seen:
            thumb_seen[url] = ad
        else:
            existing_dur = thumb_seen[url].get("duration_days") or 0
            new_dur      = ad.get("duration_days") or 0
            if new_dur > existing_dur:
                thumb_seen[url] = ad
    deduped = list(thumb_seen.values())

    # Pass 4 — video URL dedup (same video creative, different IDs)
    vid_seen: dict[str, dict] = {}
    for ad in deduped:
        vurl = (ad.get("video_url") or "").strip()
        if not vurl:
            vid_seen[ad["ad_library_id"]] = ad
            continue
        if vurl not in vid_seen:
            vid_seen[vurl] = ad
        else:
            existing_dur = vid_seen[vurl].get("duration_days") or 0
            new_dur      = ad.get("duration_days") or 0
            if new_dur > existing_dur:
                vid_seen[vurl] = ad
    return list(vid_seen.values())


def _copy_fingerprint(text: Optional[str]) -> Optional[str]:
    """Normalised 80-char prefix used as copy identity key."""
    if not text:
        return None
    normalised = re.sub(r"\W+", " ", text.lower()).strip()
    return normalised[:80] if normalised else None


# ══════════════════════════════════════════════════════════════════════════════
# Format distribution
# ══════════════════════════════════════════════════════════════════════════════

def _format_distribution(ads: list[dict]) -> dict:
    """
    Returns per-format count and percentage.
    Example:
      {"static": {"count": 15, "pct": 60.0}, "video": {"count": 7, "pct": 28.0}, ...}
    """
    total  = len(ads) or 1
    counts = Counter(
        ad.get("creative_type") or "unknown"
        for ad in ads
    )
    return {
        fmt: {
            "count": counts.get(fmt, 0),
            "pct":   round(counts.get(fmt, 0) / total * 100, 1),
        }
        for fmt in VALID_CREATIVE_TYPES + ["unknown"]
        if counts.get(fmt, 0) > 0 or fmt != "unknown"
    }


# ══════════════════════════════════════════════════════════════════════════════
# Creative diversity score
# ══════════════════════════════════════════════════════════════════════════════

def _diversity_score(ads: list[dict], fmt_dist: dict) -> dict:
    """
    Composite score 0–100 split equally across four 25-pt dimensions:

      format_variety   — how many of the 4 formats (static/carousel/video/reel)
                         are represented (scaled 0–25)
      copy_variation   — ratio of unique copy fingerprints to total ads (0–25)
      visual_variety   — ratio of unique thumbnail URLs to total ads (0–25)
      creative_volume  — raw ad count normalised to a benchmark of 20 ads (0–25)
    """
    if not ads:
        return {"total": 0.0, "breakdown": {}}

    total = len(ads)

    # 1. Format variety (0–25)
    active_formats = sum(
        1 for fmt in VALID_CREATIVE_TYPES
        if fmt_dist.get(fmt, {}).get("count", 0) > 0
    )
    # Bonus: video ads with transcripts count as richer creative investment
    has_video_with_transcript = any(
        ad.get("creative_type") == "video" and ad.get("transcript")
        for ad in ads
    )
    if has_video_with_transcript and active_formats < 4:
        active_formats += 1  # treat video-with-transcript as an extra format variant
    # Scale: 1→0, 2→8, 3→17, 4→25
    variety_pts = round((min(active_formats, 4) - 1) / 3 * 25, 1) if active_formats > 0 else 0.0

    # 2. Copy variation (0–25)
    copies = [_copy_fingerprint(ad.get("ad_copy")) for ad in ads]
    copies_with_text = [c for c in copies if c is not None]
    if copies_with_text:
        unique_copy_ratio = len(set(copies_with_text)) / len(copies_with_text)
    else:
        unique_copy_ratio = 0.5  # neutral if no copy data
    copy_pts = round(unique_copy_ratio * 25, 1)

    # 3. Visual variety (0–25)
    thumbs = [
        (ad.get("thumbnail_url") or "").strip()
        for ad in ads
        if (ad.get("thumbnail_url") or "").strip()
    ]
    if thumbs:
        unique_visual_ratio = len(set(thumbs)) / len(thumbs)
    else:
        unique_visual_ratio = 0.5  # neutral if no thumbnails
    visual_pts = round(unique_visual_ratio * 25, 1)

    # 4. Creative volume (0–25) — benchmark: 20 unique ads = full score
    _VOLUME_BENCHMARK = 20
    volume_pts = round(min(total / _VOLUME_BENCHMARK, 1.0) * 25, 1)

    total_score = round(variety_pts + copy_pts + visual_pts + volume_pts, 1)

    return {
        "total": total_score,
        "breakdown": {
            "format_variety":  variety_pts,
            "copy_variation":  copy_pts,
            "visual_variety":  visual_pts,
            "creative_volume": volume_pts,
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# Copy statistics
# ══════════════════════════════════════════════════════════════════════════════

def _copy_stats(ads: list[dict]) -> dict:
    """Surface copy-level patterns useful for LLM context."""
    copies = [ad.get("ad_copy") for ad in ads if ad.get("ad_copy")]
    if not copies:
        return {"ads_with_copy": 0, "avg_word_count": 0, "cta_distribution": {}}

    word_counts = [len(c.split()) for c in copies]
    ctas = Counter(
        (ad.get("cta_type") or "unknown").strip()
        for ad in ads
        if ad.get("cta_type")
    )

    return {
        "ads_with_copy":  len(copies),
        "avg_word_count": round(sum(word_counts) / len(word_counts), 1),
        "min_word_count": min(word_counts),
        "max_word_count": max(word_counts),
        "cta_distribution": dict(ctas.most_common()),
    }


# ══════════════════════════════════════════════════════════════════════════════
# DB helpers
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_brands(names: list[str]) -> list[dict]:
    if not names:
        return []
    placeholders = ",".join("?" * len(names))
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT id, name, is_client, category FROM brands WHERE name IN ({placeholders})",
            names,
        ).fetchall()
    return [dict(r) for r in rows]


def _fetch_ads(brand_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM ads WHERE brand_id = ?", (brand_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def _upsert_brand(
    name:             str,
    is_client:        bool = False,
    category:         Optional[str] = None,
    website_url:      Optional[str] = None,
    instagram_handle: Optional[str] = None,
) -> int:
    if category and category not in VALID_CATEGORIES:
        raise ValueError(f"Invalid category '{category}'. Must be one of {VALID_CATEGORIES}")

    with get_connection() as conn:
        existing = conn.execute(
            "SELECT id FROM brands WHERE name = ?", (name,)
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE brands
                   SET is_client        = MAX(is_client, ?),
                       category         = COALESCE(?, category),
                       website_url      = COALESCE(?, website_url),
                       instagram_handle = COALESCE(?, instagram_handle)
                   WHERE id = ?""",
                (int(is_client), category, website_url, instagram_handle, existing["id"]),
            )
            return existing["id"]

        cursor = conn.execute(
            "INSERT INTO brands (name, is_client, category, website_url, instagram_handle) "
            "VALUES (?, ?, ?, ?, ?)",
            (name, int(is_client), category, website_url, instagram_handle),
        )
        return cursor.lastrowid


def _upsert_ads(brand_id: int, raw_ads: list[dict]) -> int:
    count = 0
    with get_connection() as conn:
        for ad in raw_ads:
            creative_type = ad.get("creative_type")
            if creative_type not in VALID_CREATIVE_TYPES:
                creative_type = None

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
                    ad.get("ad_library_id", ""),
                    creative_type,
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


# ══════════════════════════════════════════════════════════════════════════════
# Output
# ══════════════════════════════════════════════════════════════════════════════

def _write_processed(brand_name: str, data: dict) -> Path:
    PROC_DIR.mkdir(parents=True, exist_ok=True)
    safe  = brand_name.lower().replace(" ", "_")
    path  = PROC_DIR / f"{safe}.json"
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Processed JSON -> %s", path)
    return path


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def _cli() -> None:
    import argparse

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(name)s -- %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        prog="python -m analysis.structurer",
        description="Read ads from DB, deduplicate, compute diversity score, "
                    "write data/processed/{brand}.json.",
    )
    parser.add_argument("--brand", required=True, help="Primary brand name")
    parser.add_argument("--competitors", default="",
                        help="Comma-separated competitor brand names")
    args = parser.parse_args()

    competitors = [c.strip() for c in args.competitors.split(",") if c.strip()]

    # If no competitors supplied, auto-detect from competitor_sets table
    if not competitors:
        logger.info("No --competitors given, checking competitor_sets table...")
        with get_connection() as conn:
            brand_row = conn.execute(
                "SELECT id FROM brands WHERE name = ?", (args.brand,)
            ).fetchone()
            if brand_row:
                comp_rows = conn.execute(
                    "SELECT b.name FROM competitor_sets cs "
                    "JOIN brands b ON b.id = cs.competitor_brand_id "
                    "WHERE cs.client_brand_id = ?",
                    (brand_row["id"],),
                ).fetchall()
                competitors = [r["name"] for r in comp_rows]
                if competitors:
                    logger.info("Auto-detected competitors: %s", competitors)
                else:
                    logger.info("No competitors found in DB -- running brand-only.")
            else:
                logger.error("Brand '%s' not found in DB. Run the scraper first.", args.brand)
                return

    logger.info("Running structurer for '%s' with competitors=%s", args.brand, competitors)
    result = run(args.brand, competitors)

    # Summary
    for name, data in result["brands"].items():
        print(f"  {name}: {data['total_ads']} ads, "
              f"diversity={data['creative_diversity_score']}/100, "
              f"dupes_removed={data['duplicates_removed']}")

    safe = args.brand.lower().replace(" ", "_")
    print(f"\nOutput: {PROC_DIR / safe}.json")


if __name__ == "__main__":
    _cli()
