"""
debug_meta_scraper.py — Visual debug of Meta Ad Library scraping.

Opens headless=False so you can see what Playwright sees, then:
  1. Screenshots the page after load
  2. Checks for cookie/login walls
  3. Tests every CSS selector from scraper_config.json
  4. Dumps full page HTML to debug_page.html
"""

import json
import logging
import time
from pathlib import Path
from urllib.parse import quote_plus

from playwright.sync_api import sync_playwright

from scrapers.utils import load_selectors, random_user_agent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("debug")

BRAND = "Just Herbs"
COUNTRY = "IN"
DEBUG_DIR = Path("debug_output")
DEBUG_DIR.mkdir(exist_ok=True)


def main():
    selectors = load_selectors("meta_ad_library")
    search_url = (
        "https://www.facebook.com/ads/library/"
        f"?active_status=active&ad_type=all"
        f"&country={COUNTRY}"
        f"&q={quote_plus(BRAND)}"
        f"&search_type=keyword_unordered"
        f"&media_type=all"
    )

    logger.info("URL: %s", search_url)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=["--no-sandbox"])
        ctx = browser.new_context(
            user_agent=random_user_agent(),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        page = ctx.new_page()

        # ── Step 1: Navigate ──────────────────────────────────────────────
        logger.info("Navigating to Meta Ad Library...")
        page.goto(search_url, timeout=45_000, wait_until="domcontentloaded")

        # Wait for network to settle
        try:
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            logger.warning("networkidle timed out — page may still be loading")

        # Extra wait for JS rendering
        logger.info("Waiting 8s for JS rendering...")
        time.sleep(8)

        # ── Step 2: Screenshot before any interaction ─────────────────────
        shot1 = DEBUG_DIR / "01_after_load.png"
        page.screenshot(path=str(shot1), full_page=True)
        logger.info("Screenshot saved: %s", shot1)

        # ── Step 3: Check for blockers ────────────────────────────────────
        logger.info("\n=== BLOCKER CHECK ===")

        # Cookie consent
        cookie_selectors = [
            "button[data-testid='cookie-policy-manage-dialog-accept-button']",
            "button:has-text('Allow all cookies')",
            "button:has-text('Allow essential and optional cookies')",
            "button:has-text('Accept')",
            "button:has-text('Accept All')",
            "[aria-label='Allow all cookies']",
            "[aria-label='Accept all']",
        ]
        cookie_found = False
        for sel in cookie_selectors:
            try:
                el = page.query_selector(sel)
                if el:
                    logger.info("COOKIE BANNER FOUND via: %s", sel)
                    logger.info("  Button text: %s", el.inner_text()[:100])
                    cookie_found = True
                    # Click it
                    el.click()
                    logger.info("  -> Clicked cookie accept button")
                    time.sleep(3)
                    break
            except Exception as e:
                logger.debug("Cookie selector '%s' error: %s", sel, e)

        if not cookie_found:
            logger.info("No cookie banner detected.")

        # Login wall
        login_indicators = [
            "input[name='email']",
            "input[name='pass']",
            "button[name='login']",
            "#loginbutton",
            "form[action*='login']",
            "div[role='dialog'] a[href*='login']",
        ]
        login_wall = False
        for sel in login_indicators:
            try:
                el = page.query_selector(sel)
                if el:
                    logger.info("LOGIN WALL DETECTED via: %s", sel)
                    login_wall = True
            except Exception:
                pass

        if not login_wall:
            logger.info("No login wall detected.")

        # Screenshot after dismissing any blockers
        shot2 = DEBUG_DIR / "02_after_dismiss.png"
        page.screenshot(path=str(shot2), full_page=True)
        logger.info("Screenshot saved: %s", shot2)

        # ── Step 4: Check page title and URL ──────────────────────────────
        logger.info("\n=== PAGE STATE ===")
        logger.info("Title: %s", page.title())
        logger.info("URL:   %s", page.url)

        # ── Step 5: Test every selector from config ───────────────────────
        logger.info("\n=== SELECTOR TEST ===")
        for key, value in selectors.items():
            if key.startswith("_"):
                continue
            try:
                matches = page.query_selector_all(value)
                count = len(matches)
                status = "MATCH" if count > 0 else "MISS"
                logger.info("  [%s] %-30s → %d elements  (selector: %s)",
                            status, key, count, value)
                if count > 0 and count <= 3:
                    for i, m in enumerate(matches):
                        try:
                            txt = m.inner_text()[:120].replace("\n", " ")
                            logger.info("       #%d text: %s", i, txt)
                        except Exception:
                            pass
            except Exception as e:
                logger.info("  [ERR]  %-30s → %s  (selector: %s)", key, e, value)

        # ── Step 6: Try broad selectors to find ad-like elements ──────────
        logger.info("\n=== BROAD SEARCH ===")
        broad_selectors = {
            "any div with 'ad' in data-testid": "div[data-testid*='ad']",
            "any div with 'archive' in data-testid": "div[data-testid*='archive']",
            "any div with 'search' in data-testid": "div[data-testid*='search']",
            "any div with 'result' in data-testid": "div[data-testid*='result']",
            "any div with 'card' in data-testid": "div[data-testid*='card']",
            "any div role=article": "div[role='article']",
            "any div role=listitem": "div[role='listitem']",
            "any a[href*='ads/library/?id=']": "a[href*='ads/library/?id=']",
            "any a[href*='id='] inside main": "a[href*='id=']",
            "img[src*='fbcdn'] (ad images)": "img[src*='fbcdn']",
            "span with 'Started running'": "span:has-text('Started running')",
            "any text with 'Started running'": "*:has-text('Started running')",
            "'No results' indicator": "*:has-text('No results')",
            "text 'Library ID'": "*:has-text('Library ID')",
            "divs with class containing '_7'": "div[class*='_7jy']",
            "divs with class containing '_99'": "div[class*='_99']",
        }
        for desc, sel in broad_selectors.items():
            try:
                matches = page.query_selector_all(sel)
                if matches:
                    logger.info("  FOUND %3d: %s", len(matches), desc)
                    # Show first match text snippet
                    try:
                        txt = matches[0].inner_text()[:150].replace("\n", " ")
                        logger.info("            first: %s", txt)
                    except Exception:
                        pass
            except Exception as e:
                logger.debug("  Error with '%s': %s", desc, e)

        # ── Step 7: Scroll and check if more content loads ────────────────
        logger.info("\n=== SCROLL TEST ===")
        pre_scroll_height = page.evaluate("document.body.scrollHeight")
        logger.info("Pre-scroll body height: %d", pre_scroll_height)

        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(3)

        post_scroll_height = page.evaluate("document.body.scrollHeight")
        logger.info("Post-scroll body height: %d (delta: %d)",
                     post_scroll_height, post_scroll_height - pre_scroll_height)

        # Final screenshot
        shot3 = DEBUG_DIR / "03_after_scroll.png"
        page.screenshot(path=str(shot3), full_page=True)
        logger.info("Screenshot saved: %s", shot3)

        # ── Step 8: Dump full HTML ────────────────────────────────────────
        html = page.content()
        html_path = DEBUG_DIR / "debug_page.html"
        html_path.write_text(html, encoding="utf-8")
        logger.info("\nFull HTML dumped to %s (%d bytes)", html_path, len(html))

        # Quick HTML analysis
        logger.info("\n=== HTML ANALYSIS ===")
        logger.info("Contains 'ad-archive-preview-card': %s",
                     "ad-archive-preview-card" in html)
        logger.info("Contains '_7jyg': %s", "_7jyg" in html)
        logger.info("Contains 'Started running': %s", "Started running" in html)
        logger.info("Contains 'No results': %s", "No results" in html)
        logger.info("Contains 'Library ID': %s", "Library ID" in html)
        logger.info("Contains 'login': %s", "login" in html.lower())
        logger.info("Contains 'Log in': %s", "Log in" in html)
        logger.info("Contains 'fbcdn': %s", "fbcdn" in html)
        logger.info("Contains 'Just Herbs': %s", "Just Herbs" in html)

        # Count some key patterns
        import re
        id_links = re.findall(r'href="[^"]*ads/library/\?id=(\d+)', html)
        logger.info("Ad library ID links found in HTML: %d", len(id_links))
        if id_links[:5]:
            logger.info("  First IDs: %s", id_links[:5])

        logger.info("\n=== DONE ===")
        logger.info("Check debug_output/ for screenshots and HTML dump.")
        logger.info("Browser will stay open for 30s for manual inspection...")
        time.sleep(30)

        browser.close()


if __name__ == "__main__":
    main()
