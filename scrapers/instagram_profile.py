"""
scrapers/instagram_profile.py — Public Instagram profile scraper.

Extracts: follower count, post count, bio, profile picture URL,
and recent post engagement rates (likes + comments on up to 6 posts).

Stores results in the `instagram_profiles` table.

IMPORTANT: Supplementary only. The pipeline must never block on this output.
All failures are swallowed and logged as warnings.

All CSS selectors live in scraper_config.json["instagram_profile"].

CLI usage:
  python -m scrapers.instagram_profile --handle "mamaearth" --brand "Mamaearth"
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
from datetime import datetime
from typing import Optional

from config import get_connection, init_db
from scrapers.utils import download_image, load_selectors, random_delay, random_user_agent

logger = logging.getLogger(__name__)

_NAV_TIMEOUT_MS  = 30_000
_MAX_RETRIES     = 2
_BACKOFF_BASE    = 3
_MAX_POSTS_CHECK = 6   # number of recent posts to check for engagement data


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def run(instagram_handle: str, brand_name: str) -> dict:
    """
    Scrape public Instagram profile for *instagram_handle*.

    Returns profile dict on success, empty dict on any failure.
    Persists result to `instagram_profiles` table if brand exists in DB.
    """
    if not instagram_handle:
        logger.debug("No Instagram handle provided for '%s' — skipping.", brand_name)
        return {}

    handle = instagram_handle.lstrip("@").strip()
    logger.info("Scraping Instagram: @%s (brand: %s)", handle, brand_name)

    try:
        selectors = _load_selectors_safe()
        profile   = _scrape_with_retry(handle, selectors)
    except Exception as exc:
        logger.warning(
            "Instagram scrape failed for @%s: %s — skipping (supplementary).",
            handle, exc,
        )
        return {}

    if profile:
        _persist(profile, brand_name)

    return profile


# ══════════════════════════════════════════════════════════════════════════════
# Scraping
# ══════════════════════════════════════════════════════════════════════════════

def _scrape_with_retry(handle: str, selectors: dict) -> dict:
    last_exc: Exception = RuntimeError("No attempts made")
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            return _playwright_scrape(handle, selectors)
        except Exception as exc:
            last_exc = exc
            wait = _BACKOFF_BASE ** attempt
            logger.warning(
                "Instagram attempt %d/%d for @%s failed: %s. Retrying in %ds…",
                attempt, _MAX_RETRIES, handle, exc, wait,
            )
            time.sleep(wait)
    raise last_exc


def _playwright_scrape(handle: str, selectors: dict) -> dict:
    from playwright.sync_api import sync_playwright

    url = f"https://www.instagram.com/{handle}/"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx     = browser.new_context(
            user_agent=random_user_agent(),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        page = ctx.new_page()

        page.goto(url, timeout=_NAV_TIMEOUT_MS, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle", timeout=_NAV_TIMEOUT_MS)
        random_delay()

        # First try extracting from embedded JSON (faster, more reliable)
        profile = _extract_from_page_json(page, handle, url)

        if not _profile_is_complete(profile):
            # Fall back to DOM selectors
            profile = _extract_from_dom(page, handle, url, selectors)

        profile["recent_posts"] = _extract_recent_posts(page, selectors)
        profile["engagement_rate"] = _compute_engagement_rate(
            profile["recent_posts"], profile.get("follower_count")
        )

        browser.close()

    logger.info(
        "Instagram scraped: @%s — %s followers, %s posts, eng_rate=%.2f%%",
        handle,
        profile.get("follower_count", "?"),
        profile.get("post_count", "?"),
        profile.get("engagement_rate") or 0.0,
    )
    return profile


# ── Extraction strategies ──────────────────────────────────────────────────────

def _extract_from_page_json(page, handle: str, url: str) -> dict:
    """
    Instagram embeds profile data in a JSON blob inside a <script> tag.
    Try to parse it before falling back to DOM selectors.
    """
    profile: dict = {
        "handle": handle, "profile_url": url,
        "scraped_at": datetime.utcnow().isoformat(),
    }
    try:
        html = page.content()
        # Look for the JSON blob containing "edge_followed_by"
        m = re.search(
            r'"edge_followed_by"\s*:\s*\{"count"\s*:\s*(\d+)\}',
            html,
        )
        if m:
            profile["follower_count"] = int(m.group(1))

        m = re.search(
            r'"edge_owner_to_timeline_media"\s*:\s*\{"count"\s*:\s*(\d+)',
            html,
        )
        if m:
            profile["post_count"] = int(m.group(1))

        m = re.search(r'"biography"\s*:\s*"([^"]*)"', html)
        if m:
            profile["bio"] = m.group(1).encode().decode("unicode_escape")

        m = re.search(r'"full_name"\s*:\s*"([^"]*)"', html)
        if m:
            profile["display_name"] = m.group(1)

        m = re.search(r'"profile_pic_url_hd"\s*:\s*"([^"]*)"', html)
        if not m:
            m = re.search(r'"profile_pic_url"\s*:\s*"([^"]*)"', html)
        if m:
            profile["profile_pic_url"] = m.group(1).replace("\\u0026", "&")

    except Exception as exc:
        logger.debug("JSON extraction failed for @%s: %s", handle, exc)

    return profile


def _extract_from_dom(page, handle: str, url: str, selectors: dict) -> dict:
    """DOM-based extraction using selectors from scraper_config.json."""

    def _text(sel: str, alt_sel: str = "") -> Optional[str]:
        for s in (sel, alt_sel):
            if not s:
                continue
            try:
                el = page.query_selector(s)
                if el:
                    t = el.inner_text().strip()
                    if t:
                        return t
            except Exception:
                continue
        return None

    def _attr(sel: str, attribute: str) -> Optional[str]:
        try:
            el = page.query_selector(sel)
            return el.get_attribute(attribute) if el else None
        except Exception:
            return None

    follower_raw = _text(
        selectors.get("follower_count", ""),
        selectors.get("follower_count_alt", ""),
    )
    post_raw = _text(selectors.get("post_count", ""))

    return {
        "handle":          handle,
        "profile_url":     url,
        "display_name":    _text(selectors.get("profile_name", ""), selectors.get("profile_name_alt", "")),
        "bio":             _text(selectors.get("bio_text", ""), selectors.get("bio_text_alt", "")),
        "follower_count":  _parse_count(follower_raw),
        "post_count":      _parse_count(post_raw),
        "profile_pic_url": _attr(selectors.get("profile_pic", "header img"), "src"),
        "scraped_at":      datetime.utcnow().isoformat(),
    }


# ── Recent post engagement ─────────────────────────────────────────────────────

def _extract_recent_posts(page, selectors: dict) -> list[dict]:
    """
    Click through up to _MAX_POSTS_CHECK grid posts and extract
    like + comment counts from each post page.
    """
    posts: list[dict] = []
    post_sel = selectors.get("post_grid_link", "article a[href*='/p/']")

    try:
        post_links_els = page.query_selector_all(post_sel)[:_MAX_POSTS_CHECK]
        post_urls      = []
        for el in post_links_els:
            href = el.get_attribute("href") or ""
            if href and href not in post_urls:
                post_urls.append(href)

        for href in post_urls:
            post_url = f"https://www.instagram.com{href}" if href.startswith("/") else href
            post_data = _scrape_post_engagement(page, post_url, selectors)
            if post_data:
                posts.append(post_data)

    except Exception as exc:
        logger.debug("Recent posts extraction failed: %s", exc)

    return posts


def _scrape_post_engagement(page, post_url: str, selectors: dict) -> Optional[dict]:
    """Navigate to a single post and extract like/comment counts."""
    try:
        page.goto(post_url, timeout=_NAV_TIMEOUT_MS, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle", timeout=10_000)
        random_delay()

        likes    = _parse_count(_get_engagement_text(page, selectors, "like"))
        comments = _parse_count(_get_engagement_text(page, selectors, "comment"))

        # Also try JSON in page source
        html = page.content()
        if likes is None:
            m = re.search(r'"edge_media_preview_like"\s*:\s*\{"count"\s*:\s*(\d+)', html)
            if m:
                likes = int(m.group(1))
        if comments is None:
            m = re.search(r'"edge_media_to_parent_comment"\s*:\s*\{"count"\s*:\s*(\d+)', html)
            if m:
                comments = int(m.group(1))

        page.go_back()
        page.wait_for_load_state("domcontentloaded", timeout=10_000)
        random_delay()

        return {
            "url":       post_url,
            "likes":     likes,
            "comments":  comments,
        }
    except Exception as exc:
        logger.debug("Post engagement failed for %s: %s", post_url, exc)
        return None


def _get_engagement_text(page, selectors: dict, kind: str) -> Optional[str]:
    """
    kind = 'like' or 'comment'
    Tries aria-label selector first, then alt.
    """
    sel_key = f"post_{kind}_count"
    alt_key = f"post_{kind}_count_alt"
    for key in (sel_key, alt_key):
        sel = selectors.get(key, "")
        if not sel:
            continue
        try:
            el = page.query_selector(sel)
            if el:
                text = el.get_attribute("aria-label") or el.inner_text()
                if text:
                    return text.strip()
        except Exception:
            continue
    return None


# ── Engagement rate ────────────────────────────────────────────────────────────

def _compute_engagement_rate(posts: list[dict], follower_count: Optional[int]) -> Optional[float]:
    """
    Engagement rate = avg(likes + comments per post) / follower_count * 100
    Returns None if insufficient data.
    """
    if not posts or not follower_count or follower_count == 0:
        return None

    total_interactions = sum(
        (p.get("likes") or 0) + (p.get("comments") or 0)
        for p in posts
    )
    avg_interactions = total_interactions / len(posts)
    return round(avg_interactions / follower_count * 100, 4)


# ── Count parser ───────────────────────────────────────────────────────────────

def _parse_count(text: Optional[str]) -> Optional[int]:
    """
    Convert count strings to int:
      '12.3K' → 12300,  '1.2M' → 1200000,  '456' → 456
    Also handles aria-label like '1,234 likes'.
    """
    if not text:
        return None

    # Extract the first number-like token
    text = text.strip()
    m = re.search(r"([\d,]+\.?\d*)\s*([KkMmBb]?)", text)
    if not m:
        return None

    num_str  = m.group(1).replace(",", "")
    suffix   = m.group(2).upper()

    try:
        value = float(num_str)
        if suffix == "K":
            return int(value * 1_000)
        if suffix == "M":
            return int(value * 1_000_000)
        if suffix == "B":
            return int(value * 1_000_000_000)
        return int(value)
    except ValueError:
        return None


def _profile_is_complete(profile: dict) -> bool:
    return bool(
        profile.get("follower_count") is not None
        and profile.get("post_count") is not None
    )


# ── DB persistence ─────────────────────────────────────────────────────────────

def _persist(profile: dict, brand_name: str) -> None:
    """Upsert profile into instagram_profiles. No-op if brand not in DB."""
    try:
        with get_connection() as conn:
            brand_row = conn.execute(
                "SELECT id FROM brands WHERE name = ?", (brand_name,)
            ).fetchone()
            brand_id = brand_row["id"] if brand_row else None

            conn.execute(
                """INSERT INTO instagram_profiles (
                       brand_id, handle, display_name, bio, follower_count,
                       post_count, engagement_rate, profile_pic_url,
                       recent_posts_json, scraped_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(handle) DO UPDATE SET
                       follower_count   = excluded.follower_count,
                       post_count       = excluded.post_count,
                       engagement_rate  = excluded.engagement_rate,
                       bio              = excluded.bio,
                       display_name     = excluded.display_name,
                       profile_pic_url  = excluded.profile_pic_url,
                       recent_posts_json = excluded.recent_posts_json,
                       scraped_at       = excluded.scraped_at""",
                (
                    brand_id,
                    profile.get("handle"),
                    profile.get("display_name"),
                    profile.get("bio"),
                    profile.get("follower_count"),
                    profile.get("post_count"),
                    profile.get("engagement_rate"),
                    profile.get("profile_pic_url"),
                    json.dumps(profile.get("recent_posts", []), ensure_ascii=False),
                    profile.get("scraped_at", datetime.utcnow().isoformat()),
                ),
            )
        logger.info("Instagram profile persisted for @%s", profile.get("handle"))
    except Exception as exc:
        logger.warning("Failed to persist Instagram profile for '%s': %s", brand_name, exc)


# ── Selector loader ────────────────────────────────────────────────────────────

def _load_selectors_safe() -> dict:
    try:
        return load_selectors("instagram_profile")
    except (FileNotFoundError, KeyError) as exc:
        logger.warning("Could not load Instagram selectors: %s. Using empty dict.", exc)
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def _cli() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser(
        prog="python -m scrapers.instagram_profile",
        description="Scrape a public Instagram profile.",
    )
    parser.add_argument("--handle", required=True, help="Instagram handle (with or without @)")
    parser.add_argument("--brand",  required=True, help="Brand name as stored in DB")
    args = parser.parse_args()

    init_db()
    profile = run(args.handle, args.brand)

    if profile:
        print(json.dumps(profile, indent=2, ensure_ascii=False))
    else:
        print("Scrape failed or returned no data.")


if __name__ == "__main__":
    _cli()
