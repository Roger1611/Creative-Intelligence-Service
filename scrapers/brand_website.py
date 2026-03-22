"""
scrapers/brand_website.py — Extract brand positioning signals from a brand's
own website: hero copy, USP claims, product names, testimonials.

Uses httpx + BeautifulSoup. Falls back to Playwright for JS-rendered pages.
"""

import logging
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from config import SCRAPER_TIMEOUT
from scrapers.utils import random_delay, random_user_agent

logger = logging.getLogger(__name__)


def run(website_url: str, brand_name: str) -> dict:
    """
    Scrape top-level brand signals from *website_url*.

    Returns dict with keys: hero_copy, usp_claims, product_names,
    testimonials, meta_description, source_url.
    """
    logger.info("Scraping brand website: %s", website_url)
    html = _fetch_html(website_url)
    if not html:
        return {}
    result = _parse(html, website_url)
    logger.info("Brand website scrape complete for %s", brand_name)
    return result


# ── Internal ───────────────────────────────────────────────────────────────────

def _fetch_html(url: str) -> Optional[str]:
    headers = {"User-Agent": random_user_agent()}
    try:
        with httpx.Client(timeout=SCRAPER_TIMEOUT, follow_redirects=True) as client:
            r = client.get(url, headers=headers)
            r.raise_for_status()
        random_delay()
        if len(r.text) < 2000:          # likely JS-rendered
            return _fetch_html_playwright(url)
        return r.text
    except Exception as exc:
        logger.error("HTTP fetch failed for %s: %s", url, exc)
        return _fetch_html_playwright(url)


def _fetch_html_playwright(url: str) -> Optional[str]:
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_context(user_agent=random_user_agent()).new_page()
            page.goto(url, timeout=30_000)
            page.wait_for_load_state("networkidle")
            html = page.content()
            browser.close()
        random_delay()
        return html
    except Exception as exc:
        logger.error("Playwright fallback failed for %s: %s", url, exc)
        return None


def _parse(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    return {
        "meta_description": _meta_description(soup),
        "hero_copy":        _hero_copy(soup),
        "usp_claims":       _usp_claims(soup),
        "product_names":    _product_names(soup),
        "testimonials":     _testimonials(soup),
        "source_url":       url,
    }


def _meta_description(soup: BeautifulSoup) -> Optional[str]:
    tag = soup.find("meta", attrs={"name": "description"})
    return tag.get("content", "").strip() or None if tag else None


def _hero_copy(soup: BeautifulSoup) -> Optional[str]:
    h1 = soup.find("h1")
    return h1.get_text(" ", strip=True) if h1 else None


def _usp_claims(soup: BeautifulSoup) -> list[str]:
    claims: list[str] = []
    for tag in soup.find_all(["li", "p", "span"], limit=200):
        text = tag.get_text(" ", strip=True)
        if 5 < len(text) < 120 and text not in claims:
            claims.append(text)
    return claims[:20]


def _product_names(soup: BeautifulSoup) -> list[str]:
    names: list[str] = []
    for tag in soup.find_all(["h2", "h3", "a"], limit=300):
        text = tag.get_text(" ", strip=True)
        if 3 < len(text) < 80 and text not in names:
            names.append(text)
    return names[:30]


def _testimonials(soup: BeautifulSoup) -> list[str]:
    results: list[str] = []
    for tag in soup.find_all(
        attrs={"class": lambda c: c and any(
            kw in " ".join(c).lower()
            for kw in ("review", "testimonial", "quote")
        )},
        limit=50,
    ):
        text = tag.get_text(" ", strip=True)
        if len(text) > 20:
            results.append(text)
    return results[:10]
