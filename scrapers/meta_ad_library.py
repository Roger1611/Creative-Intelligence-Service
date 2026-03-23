"""
scrapers/meta_ad_library.py — Meta Ad Library scraper (Playwright).

Meta's DOM changes frequently. ALL selectors live in scraper_config.json
under the "meta_ad_library" key — no selectors are hardcoded here.

Fallback: if scraping fails entirely, the pipeline reads
  data/raw/{brand_name}_manual.json  (user-supplied JSON in the same schema).

CLI usage:
  python -m scrapers.meta_ad_library --brand "Mamaearth" \\
      --competitors "Plum,WOW Skin Science,mCaffeine" \\
      --country IN --max-pages 5
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs

from config import RAW_DIR, get_connection, init_db
from scrapers.utils import download_image, load_selectors, random_delay, random_user_agent

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
) -> dict:
    """
    Scrape Meta Ad Library for *brand_name* and any *competitors*.

    Returns:
        {
            "brand":       [<ad_dict>, ...],
            "competitors": {<name>: [<ad_dict>, ...], ...},
        }

    Each ad dict matches the `ads` table schema plus a `brand_name` key.
    Downloaded thumbnails are saved to data/raw/{safe_brand_name}/.
    """
    results: dict = {"brand": [], "competitors": {}}
    selectors = _load_selectors_safe()

    logger.info("=== Meta Ad Library scrape: %s [country=%s] ===", brand_name, country)
    results["brand"] = _scrape_brand(brand_name, country=country, max_pages=max_pages,
                                     selectors=selectors)

    for comp in (competitors or []):
        logger.info("Scraping competitor: %s", comp)
        results["competitors"][comp] = _scrape_brand(
            comp, country=country, max_pages=max_pages, selectors=selectors
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
    selectors:  dict,
) -> list[dict]:
    """Try Playwright scrape; fall back to manual JSON on total failure."""
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            return _playwright_scrape(brand_name, country, max_pages, selectors)
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
    selectors:  dict,
) -> list[dict]:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    ads: list[dict] = []
    today_str = date.today().isoformat()

    # Navigate to the base Ad Library page (no query — we'll use the autocomplete)
    base_url = (
        "https://www.facebook.com/ads/library/"
        f"?active_status=active&ad_type=all"
        f"&country={country}"
        f"&media_type=all"
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=["--no-sandbox"])
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
        debug_dir = Path("debug_output")
        debug_dir.mkdir(exist_ok=True)

        random_delay()
        page.wait_for_load_state("networkidle", timeout=_NAV_TIMEOUT_MS)

        # Explicit wait: at least one "Library ID" text node must appear
        found = _wait_for_any_card(page, selectors, timeout=15_000)
        if not found:
            logger.warning("No ad cards found after 15s wait.")
            page.screenshot(path=str(debug_dir / "step4_no_cards.png"),
                            full_page=False)

        # ── Incremental scroll to trigger lazy loading ─────────────────
        _incremental_scroll(page, rounds=5, pause=2.0)

        page.screenshot(path=str(debug_dir / "step4_before_extraction.png"),
                        full_page=False)
        logger.info("DEBUG screenshot: step4_before_extraction.png")

        # ── Extract ads via text-anchor DOM walking ────────────────────
        seen_ids: set[str] = set()
        pages_loaded = 0

        while pages_loaded < max_pages:
            card_dicts = _get_cards(page, selectors)
            logger.info("Text-anchor extraction found %d cards (page %d)",
                        len(card_dicts), pages_loaded + 1)

            # Dump first card's real HTML for debugging (once)
            if pages_loaded == 0 and card_dicts:
                first = card_dicts[0]
                if first.get("html_debug"):
                    html_path = debug_dir / "debug_real_card.html"
                    html_path.write_text(first["html_debug"], encoding="utf-8")
                    logger.info("Real card HTML dumped -> %s (%d chars)",
                                html_path, len(first["html_debug"]))

            new_cards = [c for c in card_dicts
                         if _card_id_hint(c) not in seen_ids]

            if not new_cards:
                logger.info("No new cards found -- stopping extraction.")
                break

            for card_data in new_cards:
                cid = _card_id_hint(card_data)
                seen_ids.add(cid)
                try:
                    ad = _build_ad_from_card_data(
                        card_data, brand_name, today_str)
                    if ad:
                        ads.append(ad)
                        logger.debug("Extracted ad %s", ad["ad_library_id"])
                    else:
                        logger.debug("Card skipped (no ID): copy=%s",
                                     (card_data.get("ad_copy") or "")[:60])
                except Exception as exc:
                    logger.warning("Card build failed: %s", exc)

            pages_loaded += 1
            logger.info("Page %d/%d: %d ads collected so far",
                        pages_loaded, max_pages, len(ads))

            # Scroll to load more
            prev_count = len(card_dicts)
            _scroll_to_load_more(page)
            time.sleep(2)

            after_count = len(_get_cards(page, selectors))
            if after_count <= prev_count:
                logger.debug("No new cards after scroll -- reached end.")
                break

        browser.close()

    logger.info("Scrape complete for '%s': %d ads", brand_name, len(ads))
    return ads


def _search_via_autocomplete(page, brand_name: str, selectors: dict) -> None:
    """
    Select the ad category, type the brand name into the search box, and select
    the matching advertiser from the autocomplete dropdown.

    Flow:
    1. Select "All ads" from the ad category dropdown (required before search)
    2. Find the now-active search input and type brand name char-by-char
    3. Wait for the autocomplete listbox to appear
    4. Scan advertiser entries (aria-describedby="pageID:...") for a match
    5. Click the best matching advertiser
    6. If none found, fall back to "Search this exact phrase" with a warning
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    debug_dir = Path("debug_output")
    debug_dir.mkdir(exist_ok=True)

    # ── Step 1: Select ad category ────────────────────────────────────────────
    _select_ad_category(page, selectors)
    page.screenshot(path=str(debug_dir / "step1_after_category.png"), full_page=False)
    logger.info("DEBUG screenshot saved: step1_after_category.png")

    # ── Step 2: Find the search input (now active after category selection) ───
    search_input = _find_search_input(page, selectors)
    if not search_input:
        page.screenshot(path=str(debug_dir / "step2_no_input_found.png"), full_page=False)
        raise RuntimeError("Could not find search input on Ad Library page")

    # Click into the search box and type brand name slowly to trigger autocomplete
    search_input.click()
    time.sleep(0.5)
    search_input.fill("")
    time.sleep(0.3)

    logger.info("Typing '%s' into search box to trigger autocomplete...", brand_name)
    for char in brand_name:
        search_input.type(char, delay=120)

    page.screenshot(path=str(debug_dir / "step2_after_typing.png"), full_page=False)
    logger.info("DEBUG screenshot saved: step2_after_typing.png")

    # ── Step 3: Wait for autocomplete dropdown ────────────────────────────────
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

    time.sleep(1.5)  # Let the full advertiser list populate

    page.screenshot(path=str(debug_dir / "step3_dropdown_visible.png"), full_page=False)
    logger.info("DEBUG screenshot saved: step3_dropdown_visible.png")

    # ── Step 4: Find and click the best advertiser match ──────────────────────
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

    # ── Fallback: click "Search this exact phrase" ────────────────────────────
    logger.warning(
        "No matching advertiser found for '%s' in autocomplete dropdown. "
        "Falling back to exact phrase keyword search — results may include "
        "unrelated brands.",
        brand_name,
    )
    exact_sel = selectors.get(
        "autocomplete_exact_phrase", "[aria-label*='Search for exact phrase']"
    )
    exact_btn = page.query_selector(exact_sel)
    if exact_btn:
        exact_btn.click()
    else:
        # Last resort: just press Enter
        logger.warning("Could not find exact phrase button either. Pressing Enter.")
        search_input.press("Enter")


def _select_ad_category(page, selectors: dict) -> None:
    """
    Select "All ads" from the ad category dropdown.

    The Meta Ad Library page loads with the search input disabled until an ad
    category is chosen. We try multiple strategies to find and click the
    category dropdown, then select "All ads".
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    debug_dir = Path("debug_output")
    debug_dir.mkdir(exist_ok=True)

    # Check if the search input is already active (category pre-selected via URL)
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

    # ── Strategy 1: Find the category dropdown by label/text association ───────
    # Look for elements containing "Ad category" text and find a clickable
    # sibling or child that acts as the dropdown trigger.
    category_combo = None

    # Try: any element whose accessible name or label mentions "category"
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

    # Try: role="combobox" elements — identify by surrounding text
    if not category_combo:
        comboboxes = page.query_selector_all("[role='combobox']")
        logger.debug("Found %d combobox elements on page", len(comboboxes))
        for combo in comboboxes:
            try:
                # Check the text content and surrounding context
                combo_text = combo.evaluate("""el => {
                    let text = el.innerText || el.textContent || '';
                    let parent = el.closest('div')?.parentElement;
                    if (parent) text += ' ' + (parent.innerText || '').slice(0, 300);
                    return text.toLowerCase();
                }""")
                if any(kw in combo_text for kw in
                       ["category", "ad category", "choose an ad"]):
                    category_combo = combo
                    logger.debug("Found category combobox via parent text match.")
                    break
            except Exception:
                continue

        # Fallback: second combobox (first is usually country)
        if not category_combo and len(comboboxes) >= 2:
            category_combo = comboboxes[1]
            logger.debug("Using second combobox as category dropdown (positional).")

    # Try: look for a div/span with "Ad category" or "Choose an ad category"
    # text and click it directly (some Meta layouts render custom dropdowns)
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
                    logger.debug("Found category element via text: %s", sel)
                    break
            except Exception:
                continue

    if not category_combo:
        page.screenshot(path=str(debug_dir / "cat_FAIL_no_dropdown.png"), full_page=False)
        logger.warning(
            "Could not find category dropdown. Screenshot saved. "
            "Proceeding anyway — search input may not be active."
        )
        return

    # ── Click the category dropdown to open it ─────────────────────────────────
    logger.info("Clicking category dropdown...")
    category_combo.scroll_into_view_if_needed()
    category_combo.click()
    time.sleep(1.5)

    page.screenshot(path=str(debug_dir / "cat_step1_dropdown_open.png"), full_page=False)
    logger.info("DEBUG screenshot: cat_step1_dropdown_open.png")

    # ── Select "All ads" from the dropdown options ─────────────────────────────
    all_ads_option = None

    # Try multiple selectors for the "All ads" option
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
                logger.debug("Found 'All ads' option via: %s", sel)
                break
        except Exception:
            continue

    # Fallback: scan all role='option' elements for text containing "All ads"
    if not all_ads_option:
        options = page.query_selector_all("[role='option']")
        logger.debug("Scanning %d role='option' elements for 'All ads'", len(options))
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
        time.sleep(2)  # Wait for the search input to become active
    else:
        page.screenshot(path=str(debug_dir / "cat_FAIL_no_all_ads.png"), full_page=False)
        logger.warning(
            "Could not find 'All ads' option. Trying keyboard: ArrowDown + Enter."
        )
        category_combo.press("ArrowDown")
        time.sleep(0.3)
        category_combo.press("Enter")
        time.sleep(2)

    page.screenshot(path=str(debug_dir / "cat_step2_after_select.png"), full_page=False)
    logger.info("DEBUG screenshot: cat_step2_after_select.png")


def _find_search_input(page, selectors: dict):
    """Locate the visible AND enabled search input on the Ad Library page.

    After category selection the search box transitions from disabled/greyed-out
    to active. This function waits for that transition, checking both visibility
    and enabled state.
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    debug_dir = Path("debug_output")
    debug_dir.mkdir(exist_ok=True)

    # Selectors to try, in priority order
    candidates = [
        "input[type='search']:not([disabled])",
        selectors.get("search_input", "input[placeholder*='Search']"),
        "input[placeholder*='Search by keyword or advertiser']",
        "input[placeholder*='keyword']",
        "input[placeholder*='advertiser']",
    ]

    # First pass: wait for an enabled search input (up to 15s total)
    for sel in candidates:
        try:
            page.wait_for_selector(sel, timeout=8_000, state="visible")
            inp = page.query_selector(sel)
            if inp and inp.is_visible() and inp.is_enabled():
                logger.info("Found enabled search input via: %s", sel)
                return inp
            elif inp and inp.is_visible():
                # Input exists but disabled — wait a bit for it to activate
                logger.debug("Input found but disabled via %s, waiting...", sel)
                for _ in range(10):
                    time.sleep(0.5)
                    if inp.is_enabled():
                        logger.info("Search input became enabled via: %s", sel)
                        return inp
                logger.debug("Input stayed disabled via: %s", sel)
        except PWTimeout:
            continue

    # Second pass: scan all visible inputs for a search-like one that's enabled
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
                    logger.debug("Found search input but it's disabled: placeholder='%s'", ph)
                    # Wait a bit more — category selection might still be propagating
                    for _ in range(10):
                        time.sleep(0.5)
                        if inp.is_enabled():
                            logger.info("Search input became enabled: placeholder='%s'", ph)
                            return inp
                else:
                    logger.info("Found enabled search input via scan: placeholder='%s'", ph)
                    return inp
        except Exception:
            continue

    page.screenshot(path=str(debug_dir / "search_FAIL_no_input.png"), full_page=False)
    logger.error("DEBUG screenshot: search_FAIL_no_input.png — no enabled search input found")
    return None


def _pick_best_advertiser(advertisers: list, brand_name: str):
    """
    From a list of advertiser elements, pick the one that best matches the
    brand name. Preference order:
    1. Exact name match (case-insensitive) with highest follower count
    2. Name starts with the brand name
    3. First advertiser entry (closest match by Meta's ranking)

    Returns the best matching element, or None if the list is empty.
    """
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

        # Score: higher is better
        if adv_name == brand_lower:
            score = 100  # Exact match
        elif adv_name.startswith(brand_lower):
            score = 80
        elif brand_lower in adv_name:
            score = 60
        else:
            score = 10  # Meta ranked it, some relevance

        scored.append((score, follower_count, adv))

    if not scored:
        return None

    # Sort by score descending, then follower count descending
    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)

    best_score, best_followers, best_el = scored[0]
    logger.debug(
        "Best advertiser match: score=%d, followers=%d", best_score, best_followers
    )
    return best_el


def _parse_follower_count(text: str) -> int:
    """Parse follower count from advertiser entry text like '232.4K follow this'."""
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
        pass  # Cookie banner is optional; never block on it


def _scroll_to_load_more(page, scroll_pause: float = 1.5) -> None:
    """Scroll down and wait for lazy-loaded content."""
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    time.sleep(scroll_pause)
    random_delay()


def _incremental_scroll(page, rounds: int = 5, pause: float = 2.0) -> None:
    """Scroll down incrementally to trigger lazy-loaded ad cards.

    Instead of jumping straight to the bottom, scrolls by one viewport height
    at a time, pausing between each scroll to let content load.
    """
    for i in range(rounds):
        page.evaluate("window.scrollBy(0, window.innerHeight)")
        time.sleep(pause)
        logger.debug("Incremental scroll %d/%d", i + 1, rounds)
    # Scroll back to top so extraction starts from the first card
    page.evaluate("window.scrollTo(0, 0)")
    time.sleep(1)


def _wait_for_any_card(page, selectors: dict, timeout: int = 15_000):
    """Wait for at least one ad card to appear by polling for stable text anchors.

    Instead of relying on CSS classes (which Meta rotates), we look for the
    text "Library ID" which appears exactly once per ad card.
    """
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


def _find_card_containers(page, selectors: dict) -> list[dict]:
    """Find ad card containers using text-anchor DOM walking.

    Strategy:
    1. Find every text node containing the card_text_anchor (default "Library ID")
    2. For each, walk UP the DOM until we hit a container that ALSO contains
       the date anchor ("Started running on") AND at least one <img>
    3. That container is the real ad card — extract structured data from it
       right here in JS to avoid stale element handles.

    Returns a list of dicts with raw extracted fields (all text-based).
    """
    anchor_id   = selectors.get("card_text_anchor", "Library ID")
    anchor_date = selectors.get("card_date_anchor", "Started running on")

    cards_data = page.evaluate(
        """([anchorId, anchorDate]) => {
            // Collect all text nodes that contain the ID anchor
            const walker = document.createTreeWalker(
                document.body, NodeFilter.SHOW_TEXT);
            const anchors = [];
            while (walker.nextNode()) {
                if (walker.currentNode.textContent.includes(anchorId))
                    anchors.push(walker.currentNode);
            }

            const seen = new Set();  // dedup by container reference
            const results = [];

            for (const textNode of anchors) {
                // Walk up to find the card container
                let node = textNode.parentElement;
                let container = null;
                for (let i = 0; i < 15 && node; i++) {
                    const text = node.innerText || '';
                    const hasDate = text.includes(anchorDate);
                    const hasImg = node.querySelector('img') !== null;
                    if (hasDate && hasImg) {
                        container = node;
                        break;
                    }
                    node = node.parentElement;
                }
                if (!container || seen.has(container)) continue;
                seen.add(container);

                // -- Extract structured data from this container --
                const fullText = container.innerText || '';

                // Library ID: look for numeric ID after the anchor text
                let adId = null;
                const idMatch = fullText.match(/Library\\s*ID[:\\s]*([\\d]{10,})/i);
                if (idMatch) adId = idMatch[1];
                // Fallback: any 14-18 digit number
                if (!adId) {
                    const numMatch = fullText.match(/\\b(\\d{14,18})\\b/);
                    if (numMatch) adId = numMatch[1];
                }
                // Fallback: parse from href ?id=XXXXX
                if (!adId) {
                    const links = container.querySelectorAll('a[href*="id="]');
                    for (const link of links) {
                        const url = new URL(link.href, location.origin);
                        const paramId = url.searchParams.get('id');
                        if (paramId && /^\\d{10,}$/.test(paramId)) {
                            adId = paramId;
                            break;
                        }
                    }
                }

                // Start date
                let startDate = null;
                const dateMatch = fullText.match(
                    /Started running on\\s+(.+?)(?:\\n|$)/i);
                if (dateMatch) startDate = dateMatch[1].trim();

                // Ad copy: the longest text block that isn't metadata
                // Split by newlines, filter out short/metadata lines, pick longest
                const lines = fullText.split('\\n').map(l => l.trim()).filter(l => l);
                const metaKeywords = [
                    'library id', 'started running', 'see ad details',
                    'platforms:', 'active', 'inactive', 'follow',
                ];
                let adCopy = null;
                let maxLen = 0;
                for (const line of lines) {
                    const lower = line.toLowerCase();
                    const isMeta = metaKeywords.some(kw => lower.startsWith(kw));
                    if (!isMeta && line.length > maxLen) {
                        maxLen = line.length;
                        adCopy = line;
                    }
                }

                // CTA text: look for common CTA patterns
                let cta = null;
                const ctaMatch = fullText.match(
                    /\\b(Shop [Nn]ow|Learn [Mm]ore|Sign [Uu]p|Buy [Nn]ow|Get [Oo]ffer|Book [Nn]ow|Download|Subscribe|Order [Nn]ow|Contact [Uu]s|Apply [Nn]ow|Watch [Mm]ore|Send [Mm]essage|Get [Qq]uote|Install [Nn]ow|Use [Aa]pp|See [Mm]enu)\\b/
                );
                if (ctaMatch) cta = ctaMatch[1];

                // Thumbnail: first <img> with substantial src (skip tiny icons)
                let thumbnailUrl = '';
                const imgs = container.querySelectorAll('img');
                for (const img of imgs) {
                    const src = img.src || '';
                    if (src.includes('fbcdn') || src.includes('scontent')) {
                        thumbnailUrl = src;
                        break;
                    }
                }
                if (!thumbnailUrl && imgs.length > 0) {
                    thumbnailUrl = imgs[0].src || '';
                }

                // Creative type inference
                let creativeType = 'static';
                if (container.querySelector('video')) {
                    creativeType = 'video';
                } else if (imgs.length > 2) {
                    creativeType = 'carousel';
                }
                // Check for reel indicators in text
                if (fullText.toLowerCase().includes('reel'))
                    creativeType = 'reel';

                // outerHTML snippet for debug (first card only)
                const htmlSnippet = results.length === 0
                    ? container.outerHTML : '';

                results.push({
                    ad_library_id: adId,
                    start_date_raw: startDate,
                    ad_copy: adCopy,
                    cta_type: cta,
                    thumbnail_url: thumbnailUrl,
                    creative_type: creativeType,
                    html_debug: htmlSnippet,
                });
            }
            return results;
        }""",
        [anchor_id, anchor_date],
    )

    logger.info("Text-anchor card search found %d ad containers", len(cards_data))
    return cards_data


def _get_cards(page, selectors: dict) -> list[dict]:
    """Find ad cards via text-anchor DOM walking. Returns extracted data dicts."""
    return _find_card_containers(page, selectors)


def _card_id_hint(card_data: dict) -> str:
    """Return a stable string identifying this card (for dedup)."""
    return card_data.get("ad_library_id") or str(id(card_data))


# ── Ad extraction ──────────────────────────────────────────────────────────────

def _build_ad_from_card_data(
    card_data: dict, brand_name: str, today_str: str,
) -> Optional[dict]:
    """Convert a card data dict (from JS extraction) into a proper ad record."""
    ad_library_id = card_data.get("ad_library_id")
    if not ad_library_id:
        return None

    thumbnail_url = card_data.get("thumbnail_url", "")
    image_path    = _download_thumbnail(thumbnail_url, brand_name, ad_library_id)
    start_date    = _parse_date_from_text(card_data.get("start_date_raw") or "")
    duration_days = _compute_duration(start_date, today_str)

    return {
        "ad_library_id":  ad_library_id,
        "brand_name":     brand_name,
        "ad_copy":        card_data.get("ad_copy"),
        "cta_type":       card_data.get("cta_type"),
        "thumbnail_url":  thumbnail_url,
        "image_path":     image_path,
        "start_date":     start_date,
        "last_seen_date": today_str,
        "duration_days":  duration_days,
        "is_active":      True,
        "creative_type":  card_data.get("creative_type", "static"),
        "scraped_at":     datetime.utcnow().isoformat(),
    }


def _extract_ad(card, selectors: dict, brand_name: str, today_str: str) -> Optional[dict]:
    """Legacy DOM-element-based extraction (kept for manual fallback)."""
    ad_library_id = _extract_ad_library_id(card, selectors)
    if not ad_library_id:
        return None

    thumbnail_url = _extract_thumbnail_url(card, selectors)
    image_path    = _download_thumbnail(thumbnail_url, brand_name, ad_library_id)
    ad_copy       = _extract_text(card, selectors, "ad_copy", "ad_copy_alt")
    cta_type      = _extract_text(card, selectors, "cta_button", "cta_button_alt")
    start_date    = _extract_start_date(card, selectors)
    creative_type = _infer_creative_type(card, selectors)
    duration_days = _compute_duration(start_date, today_str)

    return {
        "ad_library_id":  ad_library_id,
        "brand_name":     brand_name,
        "ad_copy":        ad_copy,
        "cta_type":       cta_type,
        "thumbnail_url":  thumbnail_url,
        "image_path":     image_path,
        "start_date":     start_date,
        "last_seen_date": today_str,
        "duration_days":  duration_days,
        "is_active":      True,
        "creative_type":  creative_type,
        "scraped_at":     datetime.utcnow().isoformat(),
    }


def _extract_ad_library_id(card, selectors: dict) -> Optional[str]:
    """
    Try multiple strategies to extract the Ad Library ID:
    1. Parse ?id=XXXXX from any <a> href in the card
    2. Look for a data-ad-id or id attribute
    3. Parse from inner text containing 'Library ID:'
    """
    # Strategy 1: href parse
    try:
        link_sel = selectors.get("ad_id_link", "a[href*='id=']")
        links = card.query_selector_all(link_sel)
        for link in links:
            href = link.get_attribute("href") or ""
            qs = parse_qs(urlparse(href).query)
            if "id" in qs:
                return qs["id"][0]
    except Exception:
        pass

    # Strategy 2: data attribute on a div
    try:
        container = card.query_selector("[data-ad-id]")
        if container:
            val = container.get_attribute("data-ad-id")
            if val:
                return val
    except Exception:
        pass

    # Strategy 3: text containing "Library ID:" or "Ad ID:"
    try:
        text = card.inner_text()
        m = re.search(r"(?:Library ID|Ad ID)[:\s]+(\d{10,})", text)
        if m:
            return m.group(1)
    except Exception:
        pass

    # Strategy 4: extract any 10+ digit number that looks like an ad ID
    try:
        text = card.inner_text()
        m = re.search(r"\b(\d{14,18})\b", text)
        if m:
            return m.group(1)
    except Exception:
        pass

    return None


def _extract_thumbnail_url(card, selectors: dict) -> str:
    """Return the src of the first fbcdn image in the card, or ''."""
    try:
        img_sel = selectors.get("thumbnail_img", "img[src*='fbcdn']")
        img = card.query_selector(img_sel)
        if img:
            return img.get_attribute("src") or ""
        # Fallback: any img
        img = card.query_selector(selectors.get("thumbnail_img_alt", "img"))
        if img:
            return img.get_attribute("src") or ""
    except Exception:
        pass
    return ""


def _extract_text(card, selectors: dict, primary_key: str, alt_key: str = "") -> Optional[str]:
    for key in (primary_key, alt_key):
        if not key:
            continue
        sel = selectors.get(key, "")
        if not sel:
            continue
        try:
            el = card.query_selector(sel)
            if el:
                text = el.inner_text().strip()
                if text:
                    return text
        except Exception:
            continue
    return None


def _extract_start_date(card, selectors: dict) -> Optional[str]:
    """
    Extract and normalise start date from text like:
    'Started running on 15 March, 2024' → '2024-03-15'
    """
    raw = _extract_text(card, selectors, "start_date_text", "start_date_text_alt")
    if not raw:
        # Try scanning all text in the card
        try:
            raw = card.inner_text()
        except Exception:
            return None

    return _parse_date_from_text(raw)


def _parse_date_from_text(text: str) -> Optional[str]:
    """
    Parse a date out of arbitrary text. Handles formats like:
      - 'Started running on March 15, 2024'
      - 'Started running on 15 March, 2024'
      - 'Started running on 15 March 2024'
      - Standalone 'March 15, 2024' / '15 March 2024'
    Returns ISO date string 'YYYY-MM-DD' or None.
    """
    MONTHS = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
        "jan": 1, "feb": 2, "mar": 3, "apr": 4,
        "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }

    text = text.replace(",", " ").lower()

    # Pattern: DD MonthName YYYY  or  MonthName DD YYYY
    m = re.search(
        r"(\d{1,2})\s+([a-z]+)\s+(\d{4})"
        r"|"
        r"([a-z]+)\s+(\d{1,2})\s+(\d{4})",
        text,
    )
    if m:
        if m.group(1):  # DD MonthName YYYY
            day, month_str, year = int(m.group(1)), m.group(2), int(m.group(3))
        else:           # MonthName DD YYYY
            month_str, day, year = m.group(4), int(m.group(5)), int(m.group(6))
        month = MONTHS.get(month_str[:3])
        if month:
            try:
                return date(year, month, day).isoformat()
            except ValueError:
                pass

    # Pattern: YYYY-MM-DD already present
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return m.group(0)

    return None


def _infer_creative_type(card, selectors: dict) -> str:
    """Infer creative type from DOM indicators in priority order."""
    try:
        if card.query_selector(selectors.get("reel_indicator", "")):
            return "reel"
    except Exception:
        pass
    try:
        if card.query_selector(selectors.get("carousel_container", "")) or \
           card.query_selector(selectors.get("carousel_container_alt", "")):
            return "carousel"
    except Exception:
        pass
    try:
        if card.query_selector(selectors.get("video_element", "video")):
            return "video"
    except Exception:
        pass
    return "static"


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


# ── Image download ─────────────────────────────────────────────────────────────

def _download_thumbnail(url: str, brand_name: str, ad_id: str) -> Optional[str]:
    if not url:
        return None
    safe = brand_name.lower().replace(" ", "_")
    dest = RAW_DIR / safe / f"{ad_id}.jpg"
    if dest.exists():
        return str(dest)  # Already downloaded
    return str(dest) if download_image(url, dest) else None


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
                       start_date, last_seen_date, is_active, scraped_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(ad_library_id) DO UPDATE SET
                       last_seen_date = excluded.last_seen_date,
                       image_path     = COALESCE(excluded.image_path, image_path),
                       is_active      = excluded.is_active,
                       scraped_at     = excluded.scraped_at""",
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
    """Load selectors, returning empty dict (graceful fallback) on failure."""
    try:
        return load_selectors("meta_ad_library")
    except FileNotFoundError:
        logger.error(
            "scraper_config.json not found — create it from the template. "
            "Scraping will likely fail."
        )
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
    safe = brand_name.lower().replace(" ", "_")
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
