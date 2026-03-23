"""Probe the Meta Ad Library autocomplete dropdown to discover its DOM structure."""

import logging
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

from scrapers.utils import random_user_agent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("debug")

DEBUG_DIR = Path("debug_output")
DEBUG_DIR.mkdir(exist_ok=True)

BRAND = "Just Herbs"
BASE_URL = "https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=IN"


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=["--no-sandbox"])
        ctx = browser.new_context(
            user_agent=random_user_agent(),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        page = ctx.new_page()

        logger.info("Navigating to Ad Library base page...")
        page.goto(BASE_URL, timeout=45_000, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass
        time.sleep(3)

        # Dismiss cookie banner
        for sel in [
            "button[data-testid='cookie-policy-manage-dialog-accept-button']",
            "button:has-text('Allow all cookies')",
            "button:has-text('Allow essential and optional cookies')",
            "button:has-text('Accept')",
        ]:
            try:
                btn = page.query_selector(sel)
                if btn:
                    btn.click()
                    logger.info("Dismissed cookie banner via: %s", sel)
                    time.sleep(2)
                    break
            except Exception:
                pass

        page.screenshot(path=str(DEBUG_DIR / "ac_01_base.png"), full_page=False)

        # Find all inputs and log them
        all_inputs = page.query_selector_all("input")
        for i, inp in enumerate(all_inputs):
            ph = inp.get_attribute("placeholder") or ""
            typ = inp.get_attribute("type") or ""
            vis = inp.is_visible()
            logger.info("  input[%d] type=%s placeholder='%s' visible=%s", i, typ, ph, vis)

        # The ad search box — look for visible input with 'Search' placeholder
        search_box = None
        for inp in all_inputs:
            ph = inp.get_attribute("placeholder") or ""
            if "search" in ph.lower() and "country" not in ph.lower() and inp.is_visible():
                search_box = inp
                break

        # Fallback: try the search input by keyword/advertiser placeholder
        if not search_box:
            search_box = page.query_selector("input[placeholder*='keyword']")
        if not search_box:
            search_box = page.query_selector("input[placeholder*='advertiser']")

        if not search_box:
            logger.error("No search input found!")
            page.screenshot(path=str(DEBUG_DIR / "ac_no_input.png"), full_page=False)
            time.sleep(15)
            browser.close()
            return

        logger.info("Using search input: placeholder='%s'", search_box.get_attribute("placeholder"))

        # Click and type the brand name slowly to trigger autocomplete
        search_box.click()
        time.sleep(1)

        # Clear any existing text
        search_box.fill("")
        time.sleep(0.5)

        # Type character by character to trigger autocomplete
        logger.info("Typing '%s' into search box...", BRAND)
        for char in BRAND:
            search_box.type(char, delay=150)

        logger.info("Waiting for autocomplete dropdown...")
        time.sleep(4)

        page.screenshot(path=str(DEBUG_DIR / "ac_02_typing.png"), full_page=False)

        # Now probe the dropdown structure
        logger.info("\n=== DROPDOWN PROBE ===")

        # Try common dropdown patterns
        dropdown_selectors = {
            "ul[role='listbox']": "ul[role='listbox']",
            "div[role='listbox']": "div[role='listbox']",
            "ul[role='menu']": "ul[role='menu']",
            "div[role='menu']": "div[role='menu']",
            "div[role='dialog']": "div[role='dialog']",
            "li[role='option']": "li[role='option']",
            "div[role='option']": "div[role='option']",
            "li[role='menuitem']": "li[role='menuitem']",
            "div[role='presentation']": "div[role='presentation']",
            "[data-testid*='search']": "[data-testid*='search']",
            "[data-testid*='suggest']": "[data-testid*='suggest']",
            "[data-testid*='autocomplete']": "[data-testid*='autocomplete']",
            "[data-testid*='dropdown']": "[data-testid*='dropdown']",
            "[data-testid*='result']": "[data-testid*='result']",
            "text 'Advertisers'": "*:has-text('Advertisers')",
            "text 'Pages'": "*:has-text('Pages')",
            "text 'Just Herbs'": "*:has-text('Just Herbs')",
            "text 'Search this exact'": "*:has-text('Search this exact')",
            "text 'exact phrase'": "*:has-text('exact phrase')",
            "text 'See results'": "*:has-text('See results')",
        }

        for desc, sel in dropdown_selectors.items():
            try:
                matches = page.query_selector_all(sel)
                if matches:
                    count = len(matches)
                    txt = ""
                    try:
                        txt = matches[0].inner_text()[:120].replace("\n", " | ")
                    except Exception:
                        pass
                    logger.info("  FOUND %3d: %-40s → %s", count, desc, txt)
            except Exception as e:
                logger.debug("  Error: %s: %s", desc, e)

        # Dump the page HTML around dropdown area
        # Look for anything that appeared after typing
        html = page.content()

        # Save the full HTML for inspection
        (DEBUG_DIR / "ac_dropdown.html").write_text(html, encoding="utf-8")
        logger.info("\nFull HTML saved to debug_output/ac_dropdown.html (%d bytes)", len(html))

        # Search for 'Advertisers' or 'Just Herbs' in HTML context
        import re
        for keyword in ["Advertisers", "Just Herbs", "exact phrase", "See results for"]:
            idx = html.find(keyword)
            if idx >= 0:
                chunk = html[max(0, idx-300):idx+300]
                clean = re.sub(r'<[^>]+>', ' ', chunk)
                clean = re.sub(r'\s+', ' ', clean).strip()
                logger.info("\n  Context around '%s': ...%s...", keyword, clean[:250])

        logger.info("\nBrowser open for 30s — inspect the dropdown manually...")
        time.sleep(30)
        browser.close()


if __name__ == "__main__":
    main()
