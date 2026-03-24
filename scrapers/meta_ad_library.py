"""
scrapers/meta_ad_library.py — Meta Ad Library scraper (Playwright).

Extracts ad content by clicking "See ad details" on each card, opening the
modal, and extracting caption/thumbnail/video from inside the modal.

Meta's DOM changes frequently. ALL selectors live in scraper_config.json
under the "meta_ad_library" key — no selectors are hardcoded here.

Fallback: if scraping fails entirely, the pipeline reads
  data/raw/{brand_name}_manual.json  (user-supplied JSON in the same schema).

CLI usage:
  python -m scrapers.meta_ad_library --brand "Just Herbs" --max-ads 1
  python -m scrapers.meta_ad_library --brand "Mamaearth" \
      --competitors "Plum,WOW Skin Science" --country IN --max-pages 5
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs

from config import RAW_DIR, get_connection, init_db
from scrapers.utils import load_selectors, random_delay, random_user_agent, safe_brand_slug

logger = logging.getLogger(__name__)

# ── Retry constants ────────────────────────────────────────────────────────────
_MAX_RETRIES    = 3
_BACKOFF_BASE   = 2      # seconds; delay = _BACKOFF_BASE ** attempt
_NAV_TIMEOUT_MS = 45_000


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def run(
    brand_name:  str,
    competitors: list[str] | None = None,
    country:     str = "IN",
    category:    str | None = None,
    max_pages:   int = 5,
    max_ads:     int = 0,
) -> dict:
    """
    Scrape Meta Ad Library for *brand_name* and any *competitors*.

    Args:
        max_ads: If > 0, stop after extracting this many ads per brand.
                 0 means no limit (use max_pages for pagination control).

    Returns:
        {
            "brand":       [<ad_dict>, ...],
            "competitors": {<name>: [<ad_dict>, ...], ...},
        }
    """
    results: dict = {"brand": [], "competitors": {}}
    selectors = _load_selectors_safe()

    logger.info("=== Meta Ad Library scrape: %s [country=%s] ===", brand_name, country)
    results["brand"] = _scrape_brand(brand_name, country=country, max_pages=max_pages,
                                     max_ads=max_ads, selectors=selectors)

    for comp in (competitors or []):
        logger.info("Scraping competitor: %s", comp)
        results["competitors"][comp] = _scrape_brand(
            comp, country=country, max_pages=max_pages, max_ads=max_ads,
            selectors=selectors,
        )
        random_delay()

    _save_raw(brand_name, results)
    return results


def persist(brand_name: str, raw_ads: list[dict], is_client: bool = False,
            category: str | None = None) -> int:
    """
    Upsert *raw_ads* into the `ads` table. Creates the brand row if needed.
    Returns the brand_id.
    """
    from analysis.structurer import run as structure_run
    return structure_run(brand_name, raw_ads, is_client=is_client, category=category)


# ══════════════════════════════════════════════════════════════════════════════
# Scraping
# ══════════════════════════════════════════════════════════════════════════════

def _scrape_brand(
    brand_name: str,
    country:    str,
    max_pages:  int,
    max_ads:    int,
    selectors:  dict,
) -> list[dict]:
    """Try Playwright scrape; fall back to manual JSON on total failure."""
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            return _playwright_scrape(brand_name, country, max_pages, max_ads, selectors)
        except Exception as exc:
            wait = _BACKOFF_BASE ** attempt
            logger.warning(
                "Scrape attempt %d/%d failed for '%s': %s. Retrying in %ds…",
                attempt, _MAX_RETRIES, brand_name, exc, wait,
            )
            time.sleep(wait)

    logger.error("All %d attempts failed for '%s'. Loading manual fallback.", _MAX_RETRIES, brand_name)
    return _load_manual_fallback(brand_name)


def _playwright_scrape(
    brand_name: str,
    country:    str,
    max_pages:  int,
    max_ads:    int,
    selectors:  dict,
) -> list[dict]:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    ads: list[dict] = []
    today_str = date.today().isoformat()
    brand_slug = safe_brand_slug(brand_name)

    debug_dir = Path("debug_output")
    debug_dir.mkdir(exist_ok=True)

    base_url = (
        "https://www.facebook.com/ads/library/"
        f"?active_status=active&ad_type=all"
        f"&country={country}"
        f"&media_type=all"
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            channel="chrome",
            args=["--no-sandbox", "--start-maximized"],
            slow_mo=200,
        )
        ctx = browser.new_context(
            user_agent=random_user_agent(),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        page = ctx.new_page()

        # Navigate with retry on timeout
        _navigate(page, base_url)
        random_delay()

        # Dismiss cookie banner if present
        _dismiss_cookie_banner(page)

        # Use the autocomplete search to find the advertiser page
        _search_via_autocomplete(page, brand_name, selectors)

        # ── Wait for ad cards to appear in the DOM ──────────────────────
        random_delay()
        page.wait_for_load_state("networkidle", timeout=_NAV_TIMEOUT_MS)

        found = _wait_for_any_card(page, selectors, timeout=15_000)
        if not found:
            logger.warning("No ad cards found after 15s wait.")
            page.screenshot(path=str(debug_dir / "no_cards.png"), full_page=False)
            browser.close()
            return []

        # Scroll down fully to load all ad cards (3-4 scrolls, 2s between)
        for scroll_round in range(4):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            logger.debug("Full scroll %d/4", scroll_round + 1)
        page.evaluate("window.scrollTo(0, 0)")
        time.sleep(1)

        # ── Find all "See ad details" buttons on cards ──────────────────
        detail_links = _find_detail_buttons(page, selectors)
        logger.info("Found %d 'See ad details' buttons on page", len(detail_links))

        if not detail_links:
            logger.warning("No 'See ad details' links found. Dumping page for debug.")
            page.screenshot(path=str(debug_dir / "no_detail_links.png"), full_page=False)
            browser.close()
            return []

        # Limit how many ads to process
        target_count = max_ads if max_ads > 0 else len(detail_links)
        target_count = min(target_count, len(detail_links))

        # ── Modal extraction loop ───────────────────────────────────────
        # Track already-scraped Library IDs to skip duplicates.
        # Advance button_idx independently — it may skip ahead past dupes.
        seen_ids: set[str] = set()
        button_idx = 0
        max_button_idx = len(detail_links) + target_count  # safety cap

        while len(ads) < target_count and button_idx < max_button_idx:
            ad_num = len(ads) + 1
            logger.info("── Processing ad %d/%d (button_idx=%d) ──",
                        ad_num, target_count, button_idx)

            # Re-query all "See ad details" buttons fresh
            fresh_buttons = _find_detail_buttons(page, selectors)
            if button_idx >= len(fresh_buttons):
                logger.warning("Only %d buttons found, need index %d. Stopping.",
                               len(fresh_buttons), button_idx)
                break

            link_info = fresh_buttons[button_idx]

            try:
                ad = _extract_ad_via_modal(
                    page, ctx, link_info, brand_name, brand_slug,
                    today_str, selectors, debug_dir, ad_num,
                    seen_ids=seen_ids,
                )
                if ad:
                    seen_ids.add(ad["ad_library_id"])
                    ads.append(ad)
                    logger.info("Extracted ad %s (%d/%d done)",
                                ad["ad_library_id"], len(ads), target_count)
                else:
                    logger.warning("Ad %d (button_idx=%d): extraction returned nothing",
                                   ad_num, button_idx)
            except Exception:
                import traceback
                logger.error("Ad %d: modal extraction FAILED — full traceback:\n%s",
                             ad_num, traceback.format_exc())
                # Try to close any open modal before continuing
                _close_modal_safe(page, selectors)

            button_idx += 1
            random_delay()

        browser.close()

    logger.info("Scrape complete for '%s': %d ads", brand_name, len(ads))
    return ads


# ══════════════════════════════════════════════════════════════════════════════
# Modal extraction
# ══════════════════════════════════════════════════════════════════════════════

def _find_detail_buttons(page, selectors: dict) -> list[dict]:
    """Find all 'See ad details' buttons by exact visible text.

    Filters to buttons in the main content area (not nav/menu) by checking
    bounding rect position.

    Returns a list of dicts with:
      - index: positional index among matched buttons
      - rect: bounding rectangle for position verification
    """
    buttons_data = page.evaluate(
        """() => {
            const allBtns = Array.from(document.querySelectorAll('[role="button"]'))
                .filter(el => {
                    const text = el.innerText.trim();
                    return text === 'See ad details';
                });
            const results = [];
            for (let i = 0; i < allBtns.length; i++) {
                const rect = allBtns[i].getBoundingClientRect();
                results.push({
                    index: i,
                    rect: {x: rect.x, y: rect.y, width: rect.width, height: rect.height},
                    text: allBtns[i].innerText.trim(),
                });
            }
            return results;
        }"""
    )

    # Log each button's position for debugging
    for btn in buttons_data:
        r = btn.get("rect", {})
        logger.info("  Button %d: text='%s' pos=(%d, %d) size=%dx%d",
                     btn["index"], btn.get("text", ""), r.get("x", 0), r.get("y", 0),
                     r.get("width", 0), r.get("height", 0))

    return buttons_data


def _extract_ad_via_modal(
    page, ctx, link_info: dict, brand_name: str, brand_slug: str,
    today_str: str, selectors: dict, debug_dir: Path, ad_num: int,
    seen_ids: set[str] | None = None,
) -> Optional[dict]:
    """Extract ad data: read Library ID + start date from the card listing,
    then click 'See ad details' to get caption/video/thumbnail."""
    from playwright.sync_api import TimeoutError as PWTimeout

    btn_index = link_info["index"]
    btn_rect = link_info.get("rect", {})

    logger.info("Processing card at button index %d, pos=(%d,%d)",
                btn_index, btn_rect.get("x", 0), btn_rect.get("y", 0))

    # ── Step 1: Read Library ID from the card BEFORE clicking ─────────
    # The Nth "Library ID:" span on the listing corresponds to the Nth button.
    card_info = page.evaluate(
        """(btnIndex) => {
            // Get all Library ID spans on the page (visible on each card)
            const idSpans = Array.from(document.querySelectorAll('span'))
                .filter(el => el.innerText.trim().startsWith('Library ID:'));

            // Get all "Started running on" spans
            const dateSpans = Array.from(document.querySelectorAll('span'))
                .filter(el => el.innerText.trim().startsWith('Started running on'));

            const libId = btnIndex < idSpans.length
                ? idSpans[btnIndex].innerText.replace('Library ID:', '').trim()
                : null;

            const startDate = btnIndex < dateSpans.length
                ? dateSpans[btnIndex].innerText.trim()
                : null;

            return { libraryId: libId, startDate: startDate };
        }""",
        btn_index,
    )

    ad_library_id = card_info.get("libraryId") if card_info else None
    start_date_raw = card_info.get("startDate") if card_info else None

    logger.info("Card %d — Library ID: %s, Start date: %s",
                btn_index, ad_library_id, start_date_raw)

    if not ad_library_id:
        logger.warning("Could not read Library ID from card at index %d", btn_index)
        return None

    # ── Duplicate check — skip if already scraped in this session ─────
    if seen_ids and ad_library_id in seen_ids:
        logger.warning("DUPLICATE: Library ID %s already scraped — skipping", ad_library_id)
        return None

    start_date = _parse_date_from_text(start_date_raw) if start_date_raw else None

    # ── Step 2: Scroll to and click "See ad details" button ───────────
    click_info = page.evaluate(
        """(btnIndex) => {
            const allBtns = Array.from(document.querySelectorAll('[role="button"]'))
                .filter(el => el.innerText.trim() === 'See ad details');
            if (btnIndex >= allBtns.length) return null;

            const btn = allBtns[btnIndex];
            const rect = btn.getBoundingClientRect();
            btn.scrollIntoView({behavior: 'smooth', block: 'center'});
            return {
                text: btn.innerText.trim(),
                x: rect.x, y: rect.y,
                width: rect.width, height: rect.height,
            };
        }""",
        btn_index,
    )
    if not click_info:
        logger.warning("Could not find button at index %d for clicking", btn_index)
        return None

    time.sleep(0.5)

    # Click the button
    page.evaluate(
        """(btnIndex) => {
            const allBtns = Array.from(document.querySelectorAll('[role="button"]'))
                .filter(el => el.innerText.trim() === 'See ad details');
            if (btnIndex < allBtns.length) {
                allBtns[btnIndex].click();
            }
        }""",
        btn_index,
    )

    # ── Step 3: Wait for expanded details to render ───────────────────
    page.wait_for_timeout(2000)

    # Debug screenshot: ad details open
    page.screenshot(path=str(debug_dir / f"modal_open_{ad_num}.png"), full_page=False)
    logger.info("Screenshot: modal_open_%d.png", ad_num)

    # ── Step 4: Extract caption, video, thumbnail from expanded view ──

    # Caption — expand "See more" inside the Nth ._7jyr, then read full text
    _expand_caption(page, selectors, card_index=btn_index)
    time.sleep(0.5)

    caption = _extract_caption_from_modal(page, selectors, card_index=btn_index)
    logger.info("Caption: %s", (caption[:80] + "...") if caption and len(caption) > 80 else caption)

    # Video URL + Thumbnail — scoped to the Nth card's container
    video_url, thumbnail_url = _extract_modal_video_and_thumbnail(page, card_index=btn_index)
    logger.info("Video URL (card %d): %s", btn_index,
                (video_url[:80] + "...") if video_url and len(video_url) > 80 else video_url)
    logger.info("Thumbnail URL (card %d): %s", btn_index, bool(thumbnail_url))

    # Creative type inference
    creative_type = "video" if video_url else "static"

    # CTA
    cta_type = _extract_modal_cta(page)

    # Debug screenshot: before close
    page.screenshot(path=str(debug_dir / f"modal_before_close_{ad_num}.png"), full_page=False)
    logger.info("Screenshot: modal_before_close_%d.png", ad_num)

    # ── Step 5: Close the expanded details ────────────────────────────
    _close_modal_safe(page, selectors)
    time.sleep(1)

    # ── Step 5: Download thumbnail and video ────────────────────────────
    ad_dir = RAW_DIR / brand_slug / ad_library_id
    ad_dir.mkdir(parents=True, exist_ok=True)

    # Thumbnail download (via Playwright's context request for session auth)
    image_path = None
    if thumbnail_url:
        thumb_path = ad_dir / "thumbnail.jpg"
        image_path = _download_via_playwright(ctx, thumbnail_url, thumb_path)

    # Video download + whisper + frames
    transcript = None
    frames_path = None
    if video_url:
        video_path = ad_dir / "video.mp4"
        dl_path = _download_via_playwright(ctx, video_url, video_path)
        if dl_path:
            # Transcribe with faster-whisper
            transcript = _transcribe_video(Path(dl_path))

            # Extract frames with ffmpeg
            frames_dir = ad_dir / "frames"
            frames_dir.mkdir(parents=True, exist_ok=True)
            _extract_frames(Path(dl_path), frames_dir)
            frames_path = str(frames_dir)

            # Delete video to save disk space
            try:
                Path(dl_path).unlink()
                logger.info("Deleted video file: %s", dl_path)
            except Exception as exc:
                logger.warning("Could not delete video: %s", exc)

    # ── Build the ad dict ───────────────────────────────────────────────
    return {
        "ad_library_id":  ad_library_id,
        "brand_name":     brand_name,
        "ad_copy":        caption,  # legacy field — same as caption
        "caption":        caption,
        "cta_type":       cta_type,
        "thumbnail_url":  thumbnail_url or "",
        "image_path":     image_path,
        "start_date":     start_date,
        "last_seen_date": today_str,
        "duration_days":  _compute_duration(start_date, today_str),
        "is_active":      True,
        "creative_type":  creative_type,
        "video_url":      video_url,
        "transcript":     transcript,
        "frames_path":    frames_path,
        "scraped_at":     datetime.utcnow().isoformat(),
    }


def _debug_dump_modal(page, debug_dir: Path, ad_num: int) -> None:
    """Find the ad detail container by walking up from 'Library ID:' span and dump its HTML."""
    html_dump = page.evaluate(
        """() => {
            // Find the span containing "Library ID:"
            const spans = document.querySelectorAll('span');
            let anchor = null;
            for (const s of spans) {
                if (s.innerText.trim().startsWith('Library ID:')) {
                    anchor = s;
                    break;
                }
            }
            if (!anchor) return {selector: 'none (no Library ID span)', html: '', textLen: 0};

            // Walk up to find a substantial container (the ad detail panel)
            let container = anchor;
            for (let i = 0; i < 20; i++) {
                if (!container.parentElement) break;
                container = container.parentElement;
                const text = container.innerText || '';
                // Stop when we find a container that has caption-like content
                // (Library ID + Started running + substantial text)
                if (text.includes('Library ID:') &&
                    text.includes('Started running') &&
                    text.length > 500) {
                    break;
                }
            }

            return {
                selector: 'walked-up-from-Library-ID (' + container.tagName + ')',
                html: container.innerHTML,
                textLen: (container.innerText || '').length,
            };
        }"""
    )

    sel_used = html_dump.get("selector", "none")
    html_content = html_dump.get("html", "")
    text_len = html_dump.get("textLen", 0)

    logger.info("DEBUG ad detail container via '%s' — innerHTML %d chars, innerText %d chars",
                sel_used, len(html_content), text_len)

    dump_path = debug_dir / f"modal_html_dump_{ad_num}.html"
    dump_path.write_text(html_content, encoding="utf-8")
    logger.info("DEBUG ad detail HTML dumped → %s", dump_path)


def _extract_id_from_modal(page, selectors: dict) -> Optional[str]:
    """Extract ad library ID by finding the span with 'Library ID:' on the page."""
    result = page.evaluate(
        """() => {
            const span = Array.from(document.querySelectorAll('span'))
                .find(el => el.innerText.trim().startsWith('Library ID:'));
            if (span) {
                return span.innerText.replace('Library ID:', '').trim() || null;
            }
            return null;
        }"""
    )
    return result


def _extract_modal_start_date(page) -> Optional[str]:
    """Extract start date by finding span/div with 'Started running on' on the page."""
    result = page.evaluate(
        """() => {
            const els = [
                ...document.querySelectorAll('span'),
                ...document.querySelectorAll('div'),
            ];
            for (const el of els) {
                const t = el.innerText.trim();
                if (t.startsWith('Started running on')) return t;
            }
            return null;
        }"""
    )
    return result


def _expand_caption(page, selectors: dict, card_index: int = 0) -> None:
    """Click 'See more' inside the Nth caption div (._7jyr) to reveal full text."""
    try:
        expanded = page.evaluate(
            """(cardIdx) => {
                const captionDivs = document.querySelectorAll('._7jyr');
                if (cardIdx >= captionDivs.length) return false;
                const captionDiv = captionDivs[cardIdx];

                // Find "See more" inside it
                const seeMore = Array.from(captionDiv.querySelectorAll('*'))
                    .find(el => el.innerText.trim().toLowerCase() === 'see more'
                             || el.innerText.trim() === 'See more');
                if (seeMore) {
                    seeMore.click();
                    return true;
                }
                return false;
            }""",
            card_index,
        )
        if expanded:
            logger.info("Expanded truncated caption via 'See more' click (card %d)", card_index)
            time.sleep(1)
        else:
            logger.debug("No 'See more' found in caption %d — may already be full", card_index)
    except Exception as exc:
        logger.debug("Caption expansion attempt failed: %s", exc)


def _extract_caption_from_modal(page, selectors: dict, card_index: int = 0) -> Optional[str]:
    """Extract the ad caption from the Nth div._7jyr on the page."""
    caption = page.evaluate(
        """(cardIdx) => {
            const captionDivs = document.querySelectorAll('._7jyr');
            if (cardIdx < captionDivs.length) {
                return captionDivs[cardIdx].innerText.trim() || null;
            }
            return null;
        }""",
        card_index,
    )
    return caption


def _extract_modal_video_and_thumbnail(page, card_index: int = 0) -> tuple[Optional[str], Optional[str]]:
    """Extract video URL and thumbnail URL from the Nth ad card.

    Scopes extraction to the Nth card by finding the Nth video container
    or the Nth large image — not the first on the whole page.
    """
    result = page.evaluate(
        """(cardIdx) => {
            // Find the Nth video container (each card has its own)
            const videoContainers = document.querySelectorAll(
                '[data-testid="ad-content-body-video-container"]');

            if (cardIdx < videoContainers.length) {
                const container = videoContainers[cardIdx];
                const video = container.querySelector('video');
                if (video) {
                    let videoUrl = video.src || video.currentSrc || null;
                    const poster = video.poster || null;
                    if (!videoUrl) {
                        const source = video.querySelector('source');
                        if (source && source.src) return [source.src, poster];
                    }
                    return [videoUrl, poster];
                }
            }

            // Fallback: find the Nth <video> on the page
            const allVideos = document.querySelectorAll('video');
            if (cardIdx < allVideos.length) {
                const video = allVideos[cardIdx];
                let videoUrl = video.src || video.currentSrc || null;
                const poster = video.poster || null;
                if (!videoUrl) {
                    const source = video.querySelector('source');
                    if (source && source.src) return [source.src, poster];
                }
                return [videoUrl, poster];
            }

            // Static ad — find the Nth large <img> (skip logos/icons < 100px)
            const largeImgs = Array.from(document.querySelectorAll('img'))
                .filter(img => {
                    const w = img.naturalWidth || img.width || 0;
                    const h = img.naturalHeight || img.height || 0;
                    return w > 100 && h > 100 && img.src;
                });
            if (cardIdx < largeImgs.length) {
                return [null, largeImgs[cardIdx].src];
            }

            return [null, null];
        }""",
        card_index,
    )
    if result and len(result) == 2:
        return result[0], result[1]
    return None, None


def _extract_modal_cta(page) -> Optional[str]:
    """Extract CTA button text from the page."""
    cta = page.evaluate(
        """() => {
            const text = document.body.innerText || '';
            const m = text.match(
                /\\b(Shop [Nn]ow|Learn [Mm]ore|Sign [Uu]p|Buy [Nn]ow|Get [Oo]ffer|Book [Nn]ow|Download|Subscribe|Order [Nn]ow|Contact [Uu]s|Apply [Nn]ow|Watch [Mm]ore|Send [Mm]essage|Get [Qq]uote|Install [Nn]ow|Use [Aa]pp|See [Mm]enu)\\b/
            );
            return m ? m[1] : null;
        }"""
    )
    return cta


def _close_modal_safe(page, selectors: dict) -> None:
    """Close the ad detail view by clicking the Close/X button, then verify
    we're back on the listing page by waiting for 'See ad details' buttons."""
    closed = False

    # Strategy 1: Find a close button by aria-label="Close"
    try:
        close_btn = page.query_selector('[aria-label="Close"]')
        if close_btn and close_btn.is_visible():
            close_btn.click()
            logger.info("Closed ad details via aria-label='Close' button")
            closed = True
    except Exception as exc:
        logger.debug("aria-label Close attempt failed: %s", exc)

    # Strategy 2: Find a button with × or ✕ text
    if not closed:
        try:
            found = page.evaluate(
                """() => {
                    const btns = document.querySelectorAll('[role="button"], button');
                    for (const btn of btns) {
                        const t = btn.innerText.trim();
                        if (t === '×' || t === '✕' || t === 'X' || t === '✖') {
                            btn.click();
                            return true;
                        }
                    }
                    return false;
                }"""
            )
            if found:
                logger.info("Closed ad details via ×/✕ button text")
                closed = True
        except Exception as exc:
            logger.debug("×/✕ button attempt failed: %s", exc)

    # Strategy 3: Escape key as last resort
    if not closed:
        try:
            page.keyboard.press("Escape")
            logger.info("Pressed Escape to close ad details")
            time.sleep(0.5)
        except Exception:
            pass

    # Wait for listing page to stabilize — "See ad details" buttons should reappear
    _wait_for_listing_buttons(page, min_buttons=1, timeout=10_000)


def _wait_for_listing_buttons(page, min_buttons: int = 1, timeout: int = 10_000) -> bool:
    """Wait until at least *min_buttons* 'See ad details' buttons are visible,
    confirming we're back on the listing page."""
    import time as _time
    deadline = _time.time() + timeout / 1000
    while _time.time() < deadline:
        count = page.evaluate(
            """() => {
                return Array.from(document.querySelectorAll('[role="button"]'))
                    .filter(el => el.innerText.trim() === 'See ad details').length;
            }"""
        )
        if count >= min_buttons:
            logger.info("Listing page stable: %d 'See ad details' buttons visible", count)
            return True
        _time.sleep(0.5)
    logger.warning("Timed out waiting for listing buttons to reappear (wanted %d)", min_buttons)
    return False


# ══════════════════════════════════════════════════════════════════════════════
# Download (via Playwright session)
# ══════════════════════════════════════════════════════════════════════════════

def _download_via_playwright(ctx, url: str, dest: Path) -> Optional[str]:
    """Download a file using Playwright's API request context (preserves session cookies).

    Meta CDN URLs require browser session auth — regular requests.get() returns 403.
    """
    if not url:
        return None

    dest.parent.mkdir(parents=True, exist_ok=True)

    try:
        response = ctx.request.get(url, timeout=60_000)
        if response.ok:
            content = response.body()
            # Cap at 10MB
            if len(content) > 10 * 1024 * 1024:
                logger.warning("Download exceeds 10MB limit, skipping: %s", url[:80])
                return None
            dest.write_bytes(content)
            logger.info("Downloaded %d bytes → %s", len(content), dest)
            return str(dest)
        else:
            logger.warning("Download failed (HTTP %d): %s", response.status, url[:80])
            return None
    except Exception as exc:
        logger.warning("Download error: %s — %s", exc, url[:80])
        return None


# ══════════════════════════════════════════════════════════════════════════════
# Whisper transcription
# ══════════════════════════════════════════════════════════════════════════════

def _transcribe_video(video_path: Path) -> Optional[str]:
    """Transcribe speech from video using faster-whisper. Auto-detects language."""
    try:
        from faster_whisper import WhisperModel
    except ImportError:
        logger.warning("faster-whisper not installed — skipping transcription")
        return None

    if not video_path.exists():
        return None

    try:
        logger.info("Transcribing %s with faster-whisper...", video_path.name)
        model = WhisperModel("base", device="cpu", compute_type="int8")
        segments, info = model.transcribe(str(video_path), beam_size=5)

        logger.info("Detected language: %s (prob=%.2f)", info.language, info.language_probability)

        transcript_parts = []
        for segment in segments:
            transcript_parts.append(segment.text.strip())

        transcript = " ".join(transcript_parts)
        logger.info("Transcript (%d chars): %s", len(transcript), transcript[:200])
        return transcript if transcript else None

    except Exception as exc:
        logger.warning("Transcription failed: %s", exc)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# Frame extraction (ffmpeg)
# ══════════════════════════════════════════════════════════════════════════════

def _extract_frames(video_path: Path, frames_dir: Path) -> bool:
    """Extract 7 frames from the video: 0s, 0.5s, 1s, 1.5s, 2s, 3s, and midpoint."""
    if not video_path.exists():
        return False

    # Get video duration for midpoint calculation
    duration = _get_video_duration(video_path)
    midpoint = duration / 2 if duration else 5.0

    timestamps = [0, 0.5, 1.0, 1.5, 2.0, 3.0, midpoint]

    frames_dir.mkdir(parents=True, exist_ok=True)
    extracted = 0

    for ts in timestamps:
        # Skip timestamps beyond video duration
        if duration and ts > duration:
            continue

        frame_name = f"frame_{ts:.1f}s.jpg"
        frame_path = frames_dir / frame_name

        try:
            result = subprocess.run(
                [
                    "ffmpeg", "-y",
                    "-ss", f"{ts:.2f}",
                    "-i", str(video_path),
                    "-frames:v", "1",
                    "-q:v", "2",
                    str(frame_path),
                ],
                capture_output=True,
                timeout=30,
            )
            if frame_path.exists() and frame_path.stat().st_size > 0:
                extracted += 1
            else:
                logger.debug("Frame at %.1fs: ffmpeg produced no output", ts)
        except FileNotFoundError:
            logger.warning("ffmpeg not found in PATH — cannot extract frames. "
                           "Install ffmpeg and add to PATH.")
            return False
        except subprocess.TimeoutExpired:
            logger.warning("ffmpeg timeout extracting frame at %.1fs", ts)
        except Exception as exc:
            logger.warning("Frame extraction error at %.1fs: %s", ts, exc)

    logger.info("Extracted %d/%d frames → %s", extracted, len(timestamps), frames_dir)
    return extracted > 0


def _get_video_duration(video_path: Path) -> Optional[float]:
    """Get video duration in seconds using ffprobe."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(video_path),
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())
    except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
        pass
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Search & navigation (kept from original)
# ══════════════════════════════════════════════════════════════════════════════

def _search_via_autocomplete(page, brand_name: str, selectors: dict) -> None:
    """
    Select the ad category, type the brand name into the search box, and select
    the matching advertiser from the autocomplete dropdown.
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    debug_dir = Path("debug_output")
    debug_dir.mkdir(exist_ok=True)

    # Step 1: Select ad category
    _select_ad_category(page, selectors)
    page.screenshot(path=str(debug_dir / "step1_after_category.png"), full_page=False)
    logger.info("DEBUG screenshot saved: step1_after_category.png")

    # Step 2: Find the search input
    search_input = _find_search_input(page, selectors)
    if not search_input:
        page.screenshot(path=str(debug_dir / "step2_no_input_found.png"), full_page=False)
        raise RuntimeError("Could not find search input on Ad Library page")

    search_input.click()
    time.sleep(0.5)
    search_input.fill("")
    time.sleep(0.3)

    logger.info("Typing '%s' into search box to trigger autocomplete...", brand_name)
    for char in brand_name:
        search_input.type(char, delay=120)

    page.screenshot(path=str(debug_dir / "step2_after_typing.png"), full_page=False)
    logger.info("DEBUG screenshot saved: step2_after_typing.png")

    # Step 3: Wait for autocomplete dropdown
    listbox_sel = selectors.get("autocomplete_listbox", "[role='listbox']")
    try:
        page.wait_for_selector(listbox_sel, timeout=8_000, state="visible")
    except PWTimeout:
        logger.warning(
            "Autocomplete dropdown did not appear for '%s'. "
            "Pressing Enter to do keyword search as fallback.",
            brand_name,
        )
        search_input.press("Enter")
        return

    time.sleep(1.5)

    page.screenshot(path=str(debug_dir / "step3_dropdown_visible.png"), full_page=False)
    logger.info("DEBUG screenshot saved: step3_dropdown_visible.png")

    # Step 4: Find and click the best advertiser match
    advertiser_sel = selectors.get(
        "autocomplete_advertiser_entry", "[aria-describedby^='pageID:']"
    )
    advertisers = page.query_selector_all(advertiser_sel)
    logger.info("Found %d advertiser entries in autocomplete dropdown", len(advertisers))

    if advertisers:
        best_match = _pick_best_advertiser(advertisers, brand_name)
        if best_match:
            try:
                adv_text = best_match.inner_text().replace("\n", " ").strip()
                logger.info("Clicking advertiser: %s", adv_text[:120])
            except Exception:
                logger.info("Clicking matched advertiser entry")
            best_match.click()
            return

    # Fallback: click "Search this exact phrase"
    logger.warning(
        "No matching advertiser found for '%s' in autocomplete dropdown. "
        "Falling back to exact phrase keyword search.",
        brand_name,
    )
    exact_sel = selectors.get(
        "autocomplete_exact_phrase", "[aria-label*='Search for exact phrase']"
    )
    exact_btn = page.query_selector(exact_sel)
    if exact_btn:
        exact_btn.click()
    else:
        logger.warning("Could not find exact phrase button either. Pressing Enter.")
        search_input.press("Enter")


def _select_ad_category(page, selectors: dict) -> None:
    """Select "All ads" from the ad category dropdown."""
    from playwright.sync_api import TimeoutError as PWTimeout

    debug_dir = Path("debug_output")
    debug_dir.mkdir(exist_ok=True)

    # Check if search input is already active
    search_sel = selectors.get("search_input", "input[placeholder*='Search']")
    for check_sel in [
        f"{search_sel}:not([disabled])",
        "input[type='search']:not([disabled])",
    ]:
        try:
            inp = page.wait_for_selector(check_sel, timeout=3_000, state="visible")
            if inp and inp.is_enabled():
                logger.info("Search input already active — category pre-selected.")
                return
        except PWTimeout:
            continue

    logger.info("Search input not yet active — selecting ad category...")
    page.screenshot(path=str(debug_dir / "cat_step0_before_select.png"), full_page=False)

    # Find the category dropdown
    category_combo = None

    for sel in [
        "[aria-label*='category' i]",
        "[aria-label*='Ad category' i]",
        "label:has-text('Ad category')",
    ]:
        el = page.query_selector(sel)
        if el:
            category_combo = el
            logger.debug("Found category element via: %s", sel)
            break

    if not category_combo:
        comboboxes = page.query_selector_all("[role='combobox']")
        logger.debug("Found %d combobox elements on page", len(comboboxes))
        for combo in comboboxes:
            try:
                combo_text = combo.evaluate("""el => {
                    let text = el.innerText || el.textContent || '';
                    let parent = el.closest('div')?.parentElement;
                    if (parent) text += ' ' + (parent.innerText || '').slice(0, 300);
                    return text.toLowerCase();
                }""")
                if any(kw in combo_text for kw in
                       ["category", "ad category", "choose an ad"]):
                    category_combo = combo
                    break
            except Exception:
                continue

        if not category_combo and len(comboboxes) >= 2:
            category_combo = comboboxes[1]

    if not category_combo:
        for sel in [
            "div:has-text('Ad category') >> visible=true",
            "span:has-text('Ad category') >> visible=true",
            "div:has-text('Choose an ad category') >> visible=true",
        ]:
            try:
                el = page.query_selector(sel)
                if el and el.is_visible():
                    category_combo = el
                    break
            except Exception:
                continue

    if not category_combo:
        page.screenshot(path=str(debug_dir / "cat_FAIL_no_dropdown.png"), full_page=False)
        logger.warning("Could not find category dropdown. Proceeding anyway.")
        return

    logger.info("Clicking category dropdown...")
    category_combo.scroll_into_view_if_needed()
    category_combo.click()
    time.sleep(1.5)

    page.screenshot(path=str(debug_dir / "cat_step1_dropdown_open.png"), full_page=False)

    # Select "All ads"
    all_ads_option = None
    for sel in [
        selectors.get("category_all_ads", "li[role='option']:has-text('All ads')"),
        "[role='option']:has-text('All ads')",
        "[role='option'] >> text='All ads'",
        "li:has-text('All ads') >> visible=true",
        "div[role='option']:has-text('All ads')",
        "span:has-text('All ads') >> visible=true",
    ]:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                all_ads_option = el
                break
        except Exception:
            continue

    if not all_ads_option:
        options = page.query_selector_all("[role='option']")
        for opt in options:
            try:
                text = opt.inner_text().strip().lower()
                if "all ads" in text:
                    all_ads_option = opt
                    break
            except Exception:
                continue

    if all_ads_option:
        all_ads_option.click()
        logger.info("Selected 'All ads' from category dropdown.")
        time.sleep(2)
    else:
        page.screenshot(path=str(debug_dir / "cat_FAIL_no_all_ads.png"), full_page=False)
        logger.warning("Could not find 'All ads' option. Trying keyboard fallback.")
        category_combo.press("ArrowDown")
        time.sleep(0.3)
        category_combo.press("Enter")
        time.sleep(2)

    page.screenshot(path=str(debug_dir / "cat_step2_after_select.png"), full_page=False)


def _find_search_input(page, selectors: dict):
    """Locate the visible AND enabled search input on the Ad Library page."""
    from playwright.sync_api import TimeoutError as PWTimeout

    debug_dir = Path("debug_output")
    debug_dir.mkdir(exist_ok=True)

    candidates = [
        "input[type='search']:not([disabled])",
        selectors.get("search_input", "input[placeholder*='Search']"),
        "input[placeholder*='Search by keyword or advertiser']",
        "input[placeholder*='keyword']",
        "input[placeholder*='advertiser']",
    ]

    for sel in candidates:
        try:
            page.wait_for_selector(sel, timeout=8_000, state="visible")
            inp = page.query_selector(sel)
            if inp and inp.is_visible() and inp.is_enabled():
                logger.info("Found enabled search input via: %s", sel)
                return inp
            elif inp and inp.is_visible():
                logger.debug("Input found but disabled via %s, waiting...", sel)
                for _ in range(10):
                    time.sleep(0.5)
                    if inp.is_enabled():
                        logger.info("Search input became enabled via: %s", sel)
                        return inp
        except PWTimeout:
            continue

    # Scan all visible inputs
    logger.debug("Primary selectors exhausted — scanning all inputs...")
    all_inputs = page.query_selector_all("input")
    for inp in all_inputs:
        try:
            if not inp.is_visible():
                continue
            ph = (inp.get_attribute("placeholder") or "").lower()
            typ = (inp.get_attribute("type") or "").lower()
            disabled = inp.get_attribute("disabled")

            is_search = (
                typ == "search"
                or ("search" in ph and "country" not in ph)
                or "keyword" in ph
                or "advertiser" in ph
            )
            if is_search:
                if disabled is not None:
                    for _ in range(10):
                        time.sleep(0.5)
                        if inp.is_enabled():
                            return inp
                else:
                    return inp
        except Exception:
            continue

    page.screenshot(path=str(debug_dir / "search_FAIL_no_input.png"), full_page=False)
    logger.error("No enabled search input found")
    return None


def _pick_best_advertiser(advertisers: list, brand_name: str):
    """Pick the advertiser entry that best matches the brand name."""
    brand_lower = brand_name.lower().strip()

    scored: list[tuple[int, int, object]] = []
    for adv in advertisers:
        try:
            text = adv.inner_text().strip()
        except Exception:
            continue

        lines = [l.strip() for l in text.split("\n") if l.strip()]
        if not lines:
            continue

        adv_name = lines[0].lower().strip()
        follower_count = _parse_follower_count(text)

        if adv_name == brand_lower:
            score = 100
        elif adv_name.startswith(brand_lower):
            score = 80
        elif brand_lower in adv_name:
            score = 60
        else:
            score = 10

        scored.append((score, follower_count, adv))

    if not scored:
        return None

    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return scored[0][2]


def _parse_follower_count(text: str) -> int:
    """Parse follower count from advertiser entry text."""
    m = re.search(r"([\d,.]+)\s*[Kk]\s*follow", text)
    if m:
        try:
            return int(float(m.group(1).replace(",", "")) * 1_000)
        except ValueError:
            pass
    m = re.search(r"([\d,.]+)\s*[Mm]\s*follow", text)
    if m:
        try:
            return int(float(m.group(1).replace(",", "")) * 1_000_000)
        except ValueError:
            pass
    m = re.search(r"([\d,]+)\s*follow", text)
    if m:
        try:
            return int(m.group(1).replace(",", ""))
        except ValueError:
            pass
    return 0


# ── Navigation helpers ─────────────────────────────────────────────────────────

def _navigate(page, url: str) -> None:
    """Navigate with retry on Playwright timeout."""
    from playwright.sync_api import TimeoutError as PWTimeout
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            page.goto(url, timeout=_NAV_TIMEOUT_MS, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle", timeout=_NAV_TIMEOUT_MS)
            return
        except PWTimeout:
            if attempt == _MAX_RETRIES:
                raise
            wait = _BACKOFF_BASE ** attempt
            logger.warning("Navigation timeout (attempt %d). Retrying in %ds…", attempt, wait)
            time.sleep(wait)


def _dismiss_cookie_banner(page) -> None:
    try:
        btn = page.query_selector("button[data-testid='cookie-policy-manage-dialog-accept-button']")
        if not btn:
            btn = page.query_selector("button:has-text('Allow all cookies')")
        if not btn:
            btn = page.query_selector("button:has-text('Accept')")
        if btn:
            btn.click()
            page.wait_for_load_state("networkidle", timeout=5_000)
            logger.debug("Cookie banner dismissed.")
    except Exception:
        pass


def _scroll_to_load_more(page, scroll_pause: float = 1.5) -> None:
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    time.sleep(scroll_pause)
    random_delay()


def _incremental_scroll(page, rounds: int = 5, pause: float = 2.0) -> None:
    """Scroll down incrementally to trigger lazy-loaded ad cards."""
    for i in range(rounds):
        page.evaluate("window.scrollBy(0, window.innerHeight)")
        time.sleep(pause)
        logger.debug("Incremental scroll %d/%d", i + 1, rounds)
    page.evaluate("window.scrollTo(0, 0)")
    time.sleep(1)


def _wait_for_any_card(page, selectors: dict, timeout: int = 15_000):
    """Wait for at least one ad card to appear by polling for 'Library ID' text."""
    anchor = selectors.get("card_text_anchor", "Library ID")
    deadline = time.time() + (timeout / 1000)
    while time.time() < deadline:
        count = page.evaluate(
            """(anchor) => {
                const walker = document.createTreeWalker(
                    document.body, NodeFilter.SHOW_TEXT);
                let n = 0;
                while (walker.nextNode()) {
                    if (walker.currentNode.textContent.includes(anchor)) n++;
                }
                return n;
            }""",
            anchor,
        )
        if count > 0:
            logger.info("Detected %d '%s' text nodes — ad cards present.", count, anchor)
            return True
        time.sleep(1)
    return False


# ── Date parsing ──────────────────────────────────────────────────────────────

def _parse_date_from_text(text: str) -> Optional[str]:
    """Parse a date from text like 'Started running on 15 March, 2024' → '2024-03-15'."""
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
                       caption, transcript, frames_path, video_url
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(ad_library_id) DO UPDATE SET
                       last_seen_date = excluded.last_seen_date,
                       image_path     = COALESCE(excluded.image_path, image_path),
                       is_active      = excluded.is_active,
                       scraped_at     = excluded.scraped_at,
                       caption        = COALESCE(excluded.caption, caption),
                       transcript     = COALESCE(excluded.transcript, transcript),
                       frames_path    = COALESCE(excluded.frames_path, frames_path),
                       video_url      = COALESCE(excluded.video_url, video_url)""",
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
    logger.info("Raw scrape saved → %s", out)


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
        prog="python -m scrapers.meta_ad_library",
        description="Scrape Meta Ad Library and persist results to SQLite.",
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

    # Initialise DB (no-op if already exists)
    init_db()

    # Scrape
    results = run(
        brand_name=args.brand,
        competitors=competitors,
        country=args.country,
        category=args.category,
        max_pages=args.max_pages,
        max_ads=args.max_ads,
    )

    # Persist brand
    brand_id = _upsert_brand(args.brand, is_client=True, category=args.category)
    n_brand  = _upsert_ads(brand_id, results["brand"])
    print(f"[OK] {args.brand}: {n_brand} ads stored (brand_id={brand_id})")

    # Persist competitors and create competitor_sets
    for comp_name, comp_ads in results["competitors"].items():
        comp_id = _upsert_brand(comp_name, is_client=False, category=args.category)
        n_comp  = _upsert_ads(comp_id, comp_ads)
        _ensure_competitor_set(brand_id, comp_id)
        print(f"  -> Competitor {comp_name}: {n_comp} ads stored (brand_id={comp_id})")

    total = n_brand + sum(
        len(v) for v in results["competitors"].values()
    )
    print(f"\nDone. {total} total ads scraped and persisted.")


if __name__ == "__main__":
    _cli()