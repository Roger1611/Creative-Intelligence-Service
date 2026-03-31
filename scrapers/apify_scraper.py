"""
scrapers/apify_scraper.py — Meta Ad Library scraping via Apify actor.

Replaces the Playwright-based scrapers/meta_ad_library.py for environments
where headless browser scraping is impractical.  Produces ad dicts identical
to the schema expected by analysis.structurer.ingest().
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

import httpx
from apify_client import ApifyClient

from config import (
    APIFY_API_KEY,
    APIFY_ACTOR_ID,
    APIFY_TIMEOUT_SECS,
    MAX_ADS_DEFAULT,
    RAW_DIR,
)
from scrapers.utils import random_delay, random_user_agent, safe_brand_slug
from scrapers.video_downloader import process_video

_MAX_THUMBNAIL_BYTES = 10 * 1024 * 1024  # 10 MB cap

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# URL helpers
# ══════════════════════════════════════════════════════════════════════════════

def _extract_page_id(url: str) -> str:
    """
    Extract ``view_all_page_id`` from a Meta Ad Library URL.

    Also validates that the URL is not a keyword search and warns if
    active_status or country differ from expected defaults.
    """
    parsed = urlparse(url)
    params = parse_qs(parsed.query)

    # ── page_id (required) ────────────────────────────────────────────────
    page_ids = params.get("view_all_page_id", [])
    if not page_ids or not page_ids[0].strip():
        raise ValueError(
            "Could not find view_all_page_id in URL. "
            "Go to facebook.com/ads/library, search your brand with "
            "country=IN and search_type=page, click View Ads, "
            "then copy the full URL from your browser bar."
        )
    page_id = page_ids[0].strip()

    # ── search_type (must not be keyword) ─────────────────────────────────
    search_types = params.get("search_type", [])
    for st in search_types:
        if "keyword" in st.lower():
            raise ValueError(
                f"URL contains search_type={st!r} — keyword search is not "
                "allowed. Use a page-level URL with view_all_page_id instead."
            )

    # ── active_status (warn if not "active") ──────────────────────────────
    active_vals = params.get("active_status", [])
    if active_vals and active_vals[0].lower() != "active":
        logger.warning(
            "URL has active_status=%s — overriding to 'active' in actor URL",
            active_vals[0],
        )

    # ── country (warn if not "IN") ────────────────────────────────────────
    country_vals = params.get("country", [])
    if country_vals and country_vals[0].upper() != "IN":
        logger.warning(
            "URL has country=%s — overriding to 'IN' in actor URL",
            country_vals[0],
        )

    return page_id


def _build_actor_url(page_id: str) -> str:
    """Build a clean, controlled Meta Ad Library URL for the Apify actor."""
    return (
        "https://www.facebook.com/ads/library/"
        "?active_status=active"
        "&ad_type=all"
        "&country=IN"
        f"&view_all_page_id={page_id}"
        "&media_type=all"
        "&search_type=page"
    )


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def run(
    brand_name: str,
    brand_url: str,
    competitors: list[dict],
    country: str = "IN",
    max_ads: int = 10,
    skip_video: bool = False,
) -> dict:
    """
    Scrape Meta Ad Library via Apify for *brand_name* and each competitor.

    Parameters
    ----------
    brand_name : str
        Client brand name.
    brand_url : str
        Full Meta Ad Library URL for the client brand (must contain
        ``view_all_page_id``).
    competitors : list[dict]
        Each entry: ``{"name": "...", "url": "https://..."}``
    country : str
        ISO country code (default ``"IN"``). Used only for logging — the
        actor URL always forces ``country=IN``.
    max_ads : int
        Maximum ads to retrieve per actor call (default 10, hard cap 50).
    skip_video : bool
        If True, skip video download/transcription and thumbnail download.

    Returns
    -------
    dict
        ``{"brand": [ad_dict, ...], "competitors": {name: [ad_dict, ...]}}``
    """
    # ── Startup validation ────────────────────────────────────────────────
    if max_ads > 50:
        raise ValueError(
            "max_ads=%d is too high for development. "
            "Use 50 or less to protect credits." % max_ads
        )

    if not APIFY_API_KEY:
        raise RuntimeError(
            "APIFY_API_KEY is not set. Add it to .env before running."
        )

    brand_page_id = _extract_page_id(brand_url)
    clean_url = _build_actor_url(brand_page_id)

    logger.info("Starting scrape: brand=%s page_id=%s max_ads=%d",
                brand_name, brand_page_id, max_ads)
    logger.info("Actor URL (clean): %s", clean_url)

    client = ApifyClient(APIFY_API_KEY)

    # ── Brand ─────────────────────────────────────────────────────────────
    brand_ads = _fetch_and_map(
        client, brand_name, brand_page_id, max_ads, skip_video,
    )

    result: dict = {
        "brand": brand_ads,
        "competitors": {},
    }

    # ── Competitors ───────────────────────────────────────────────────────
    for comp in competitors:
        comp_name = comp["name"]
        comp_url = comp["url"]
        random_delay()
        comp_page_id = _extract_page_id(comp_url)
        comp_clean_url = _build_actor_url(comp_page_id)
        logger.info(
            "Fetching ads for competitor '%s' (page_id=%s)",
            comp_name, comp_page_id,
        )
        logger.info("Actor URL (clean): %s", comp_clean_url)
        comp_ads = _fetch_and_map(
            client, comp_name, comp_page_id, max_ads, skip_video,
        )
        result["competitors"][comp_name] = comp_ads

    return result


# ══════════════════════════════════════════════════════════════════════════════
# Actor invocation + mapping
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_and_map(
    client: ApifyClient,
    name: str,
    page_id: str,
    max_ads: int,
    skip_video: bool = False,
) -> list[dict]:
    """Run Apify actor for one brand/competitor, save raw JSON, return mapped ads."""
    clean_url = _build_actor_url(page_id)

    run_input = {
        "startUrls": [{"url": clean_url}],
        "maxItems": max_ads,
        "maxResults": max_ads,
        "resultsLimit": max_ads,
        "limit": max_ads,
    }

    actor_run = client.actor(APIFY_ACTOR_ID).call(
        run_input=run_input,
        timeout_secs=APIFY_TIMEOUT_SECS,
    )

    raw_items = list(
        client.dataset(actor_run["defaultDatasetId"]).iterate_items()
    )
    raw_count = len(raw_items)
    items = raw_items[:max_ads]
    logger.info(
        "Apify returned %d items before cap, %d after cap (max=%d)",
        raw_count, len(items), max_ads,
    )

    # Save raw response for debugging
    _save_raw(name, items)

    # Estimated credit usage: $0.75 per 1000 ads
    est_credits = (len(items) / 1000) * 0.75
    logger.info(
        "Estimated Apify credit usage for '%s': $%.4f (%d items)",
        name, est_credits, len(items),
    )

    # Map to pipeline ad dict schema
    mapped: list[dict] = []
    missing_snapshot_count = 0
    for item in items:
        if not item.get("snapshot"):
            logger.warning(
                "Item %s missing snapshot — skipping field extraction. "
                "Raw keys: %s",
                item.get("id", "unknown"),
                list(item.keys()),
            )
            missing_snapshot_count += 1
        mapped.append(_map_item(item))

    # Field coverage summary
    ads_with_copy = sum(1 for a in mapped if a.get("ad_copy") is not None)
    ads_with_video_url = sum(1 for a in mapped if a.get("video_url") is not None)
    ads_with_start_date = sum(1 for a in mapped if a.get("start_date") is not None)
    logger.info(
        "Field coverage for '%s': %d/%d with ad_copy, %d/%d with video_url, "
        "%d/%d with start_date, %d missing snapshot",
        name,
        ads_with_copy, len(mapped),
        ads_with_video_url, len(mapped),
        ads_with_start_date, len(mapped),
        missing_snapshot_count,
    )

    # ── Media processing (video download/transcription + thumbnail) ───────
    if not skip_video:
        _process_media(mapped, name)
    else:
        logger.info("Skipping video/thumbnail processing for '%s' (--skip-video)", name)

    return mapped


# ══════════════════════════════════════════════════════════════════════════════
# Media processing — video transcription + thumbnail download
# ══════════════════════════════════════════════════════════════════════════════

def _process_media(ads: list[dict], brand_name: str) -> None:
    """Download videos (transcribe + extract frames) and thumbnails for static ads."""
    slug = safe_brand_slug(brand_name)
    video_count = 0
    transcribed_count = 0
    static_count = 0

    for ad in ads:
        video_url = (ad.get("video_url") or "").strip()
        ad_id = ad.get("ad_library_id") or "unknown"

        if video_url:
            video_count += 1
            result = process_video(
                video_url=video_url,
                ad_library_id=ad_id,
                brand_slug=slug,
            )
            ad["transcript"] = result["transcript"]
            ad["transcript_language"] = result["transcript_language"]
            ad["frames_path"] = result["frames_path"]
            ad["image_path"] = result["image_path"]
            if result["transcript"]:
                transcribed_count += 1
        else:
            # Static ad — attempt thumbnail download
            thumb_url = (ad.get("thumbnail_url") or "").strip()
            if thumb_url:
                static_count += 1
                saved = _download_thumbnail(thumb_url, ad_id, slug)
                if saved:
                    ad["image_path"] = saved

    logger.info(
        "Processed %d ads for '%s': %d videos (%d transcribed), %d static",
        len(ads), brand_name, video_count, transcribed_count, static_count,
    )


def _download_thumbnail(url: str, ad_library_id: str, brand_slug: str) -> Optional[str]:
    """Download a thumbnail image to data/raw/{slug}/{ad_id}/thumbnail.jpg."""
    dest = RAW_DIR / brand_slug / ad_library_id / "thumbnail.jpg"
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        r = httpx.get(
            url,
            timeout=30,
            follow_redirects=True,
            headers={"User-Agent": random_user_agent()},
        )
        if r.status_code != 200:
            logger.warning("Thumbnail download HTTP %d for ad %s", r.status_code, ad_library_id)
            return None
        if len(r.content) > _MAX_THUMBNAIL_BYTES:
            logger.warning("Thumbnail exceeds 10MB for ad %s, skipping", ad_library_id)
            return None
        dest.write_bytes(r.content)
        logger.debug("Thumbnail saved → %s", dest)
        return str(dest)
    except Exception as exc:
        logger.warning("Thumbnail download failed for ad %s: %s", ad_library_id, exc)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# Ad copy cleaning
# ══════════════════════════════════════════════════════════════════════════════

_TEMPLATE_VAR_RE = re.compile(r"\{\{.*?\}\}")
_JINJA_BLOCK_RE = re.compile(r"\{%.*?%\}")
_MULTI_NEWLINE_RE = re.compile(r"\n{3,}")


def _clean_ad_copy(text: str) -> Optional[str]:
    """Strip template variables and Jinja blocks from ad copy.

    Returns None if the cleaned text is empty or only whitespace.
    """
    cleaned = _TEMPLATE_VAR_RE.sub("", text)
    cleaned = _JINJA_BLOCK_RE.sub("", cleaned)
    cleaned = cleaned.strip()
    cleaned = _MULTI_NEWLINE_RE.sub("\n\n", cleaned)
    return cleaned if cleaned.strip() else None


# ══════════════════════════════════════════════════════════════════════════════
# Item → ad dict mapping
# ══════════════════════════════════════════════════════════════════════════════

def _map_item(item: dict) -> dict:
    """
    Map a single Apify response item to the ad dict schema expected by
    ``analysis.structurer.ingest()`` / ``_upsert_ads()``.
    """
    snapshot = item.get("snapshot", {}) or {}
    images = snapshot.get("images") or [{}]
    videos = snapshot.get("videos") or [{}]

    return {
        "ad_library_id": (
            item.get("adArchiveID")
            or item.get("adArchiveId")
            or item.get("id")
            or item.get("ad_archive_id")
        ),
        "ad_copy": _clean_ad_copy(
            (snapshot.get("body") or {}).get("text")
            or snapshot.get("caption")
            or snapshot.get("message")
            or ""
        ),
        "cta_type": (
            snapshot.get("ctaText")
            or snapshot.get("ctaType")
            or (snapshot.get("cta") or {}).get("text")
            or snapshot.get("cta_type")
        ),
        "thumbnail_url": (
            images[0].get("resizedImageUrl")
            or images[0].get("originalImageUrl")
            or images[0].get("resized_image_url")
            or images[0].get("original_image_url")
            or (videos[0].get("videoPreviewImageUrl") if videos else None)
        ) if images else None,
        "video_url": (
            videos[0].get("videoHdUrl")
            or videos[0].get("videoSdUrl")
            or videos[0].get("video_hd_url")
            or videos[0].get("video_sd_url")
        ) if videos else None,
        "start_date": _parse_start_date(item),
        "last_seen_date": date.today().isoformat(),
        "is_active": item.get("isActive", True),
        "creative_type": _infer_creative_type(item),
        "scraped_at": datetime.utcnow().isoformat(),
        "image_path": None,
        "caption": None,
        "transcript": None,
        "transcript_language": None,
        "frames_path": None,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _infer_creative_type(item: dict) -> str:
    """Infer creative format from Apify item structure."""
    snapshot = item.get("snapshot", {}) or {}

    videos = snapshot.get("videos")
    has_video = isinstance(videos, list) and len(videos) > 0

    cards = snapshot.get("cards")
    has_carousel = isinstance(cards, list) and len(cards) > 0

    if has_video:
        # Check for reel indicators
        platforms = item.get("publisherPlatform") or item.get("publisherPlatforms") or []
        if isinstance(platforms, str):
            platforms = [platforms]
        url_str = json.dumps(item.get("snapshot", {}))
        if "instagram" in [p.lower() for p in platforms if isinstance(p, str)]:
            if "reel" in url_str.lower():
                return "reel"
        return "video"

    if has_carousel:
        return "carousel"

    return "static"


def _parse_start_date(item: dict) -> Optional[str]:
    """
    Extract and normalise the ad start date to YYYY-MM-DD.
    Tries multiple field names; returns None on failure, never raises.
    """
    candidates = [
        item.get("startDate"),
        item.get("start_date"),
        item.get("adDeliveryStartTime"),
    ]

    for raw in candidates:
        if raw is None:
            continue
        parsed = _normalize_date(raw)
        if parsed:
            return parsed

    logger.debug(
        "No start_date found for item %s",
        item.get("id", "unknown"),
    )
    return None


def _normalize_date(value) -> Optional[str]:
    """Convert an int (unix ts) or date string to YYYY-MM-DD."""
    if isinstance(value, (int, float)):
        try:
            return datetime.utcfromtimestamp(value).strftime("%Y-%m-%d")
        except (OSError, ValueError, OverflowError):
            return None

    if isinstance(value, str):
        value = value.strip()
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
                     "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%fZ",
                     "%d/%m/%Y", "%m/%d/%Y", "%B %d, %Y", "%b %d, %Y"):
            try:
                return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    return None


def _save_raw(name: str, items: list[dict]) -> None:
    """Persist raw Apify response to data/raw/{slug}/apify_raw.json."""
    slug = safe_brand_slug(name)
    out_dir = RAW_DIR / slug
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "apify_raw.json"
    path.write_text(
        json.dumps(items, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    logger.info("Raw Apify response saved → %s", path)


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def _parse_competitors_arg(raw: str) -> list[dict]:
    """
    Parse CLI competitor string ``"Name:URL,Name:URL"`` into list of dicts.

    Split on the *first* colon only, since URLs contain colons.
    Each entry must have both a name and a URL.
    """
    result: list[dict] = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if ":" in entry:
            name, url = entry.split(":", 1)
            name = name.strip()
            url = url.strip()
            if name and url:
                result.append({"name": name, "url": url})
            elif name:
                raise ValueError(
                    f"Competitor '{name}' is missing a URL. "
                    "Format: Name:URL,Name:URL"
                )
        else:
            raise ValueError(
                f"Competitor entry '{entry}' is missing a URL. "
                "Format: Name:URL,Name:URL"
            )
    return result


def _cli() -> None:
    import argparse

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(name)s -- %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        prog="python -m scrapers.apify_scraper",
        description="Scrape Meta Ad Library via Apify actor.",
    )
    parser.add_argument("--brand", required=True, help="Client brand name")
    parser.add_argument("--brand-url", required=True,
                        help="Full Meta Ad Library URL for the brand "
                             "(must contain view_all_page_id)")
    parser.add_argument("--competitors", default="",
                        help='Competitors as "Name:URL,Name:URL"')
    parser.add_argument("--max-ads", type=int, default=10,
                        help="Max ads per actor call (default: 10, max: 50)")
    parser.add_argument("--skip-video", action="store_true",
                        help="Skip video download/transcription (for fast testing)")

    args = parser.parse_args()
    competitors = _parse_competitors_arg(args.competitors) if args.competitors else []

    logger.info(
        "Starting Apify scraper: brand='%s', brand_url=%s, competitors=%s, "
        "max_ads=%d, skip_video=%s",
        args.brand, args.brand_url, competitors,
        args.max_ads, args.skip_video,
    )

    result = run(
        brand_name=args.brand,
        brand_url=args.brand_url,
        competitors=competitors,
        max_ads=args.max_ads,
        skip_video=args.skip_video,
    )

    logger.info(
        "Done. Brand ads: %d. Competitors: %s",
        len(result["brand"]),
        {name: len(ads) for name, ads in result["competitors"].items()},
    )


if __name__ == "__main__":
    _cli()
