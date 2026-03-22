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
from urllib.parse import quote_plus, urlparse, parse_qs

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

    search_url = (
        "https://www.facebook.com/ads/library/"
        f"?active_status=active&ad_type=all"
        f"&country={country}"
        f"&q={quote_plus(brand_name)}"
        f"&search_type=keyword_unordered"
        f"&media_type=all"
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = browser.new_context(
            user_agent=random_user_agent(),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        page = ctx.new_page()

        # Navigate with retry on timeout
        _navigate(page, search_url)
        random_delay()

        # Dismiss cookie banner if present
        _dismiss_cookie_banner(page)

        # Paginate by scrolling; each scroll that loads new cards = 1 page
        seen_ids: set[str] = set()
        pages_loaded = 0

        while pages_loaded < max_pages:
            cards = _get_cards(page, selectors)
            new_cards = [c for c in cards if _card_id_hint(c) not in seen_ids]

            if not new_cards:
                break

            for card in new_cards:
                hint = _card_id_hint(card)
                seen_ids.add(hint)
                try:
                    ad = _extract_ad(card, selectors, brand_name, today_str)
                    if ad:
                        ads.append(ad)
                        logger.debug("Extracted ad %s", ad["ad_library_id"])
                except Exception as exc:
                    logger.warning("Card extraction failed: %s", exc)

            # Attempt to load more ads
            prev_count = len(seen_ids)
            _scroll_to_load_more(page)

            # If no new cards loaded after scroll, we've reached the end
            after_count = len(_get_cards(page, selectors))
            if after_count <= prev_count:
                logger.debug("No new cards after scroll — reached end of results.")
                break

            pages_loaded += 1
            logger.info("Page %d/%d: %d ads collected so far", pages_loaded, max_pages, len(ads))

        browser.close()

    logger.info("Scrape complete for '%s': %d ads", brand_name, len(ads))
    return ads


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


def _get_cards(page, selectors: dict) -> list:
    """Try primary selector then alt selector for ad cards."""
    cards = page.query_selector_all(selectors["ad_card"])
    if not cards:
        cards = page.query_selector_all(selectors.get("ad_card_alt", ""))
    return cards


def _card_id_hint(card) -> str:
    """Return a stable string identifying this card element (for dedup)."""
    try:
        return card.evaluate("el => el.outerHTML.slice(0, 120)")
    except Exception:
        return str(id(card))


# ── Ad extraction ──────────────────────────────────────────────────────────────

def _extract_ad(card, selectors: dict, brand_name: str, today_str: str) -> Optional[dict]:
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
    print(f"✓ {args.brand}: {n_brand} ads stored (brand_id={brand_id})")

    # Persist competitors and create competitor_sets
    for comp_name, comp_ads in results["competitors"].items():
        comp_id = _upsert_brand(comp_name, is_client=False, category=args.category)
        n_comp  = _upsert_ads(comp_id, comp_ads)
        _ensure_competitor_set(brand_id, comp_id)
        print(f"  ↳ Competitor {comp_name}: {n_comp} ads stored (brand_id={comp_id})")

    total = n_brand + sum(
        len(v) for v in results["competitors"].values()
    )
    print(f"\nDone. {total} total ads scraped and persisted.")


if __name__ == "__main__":
    _cli()
