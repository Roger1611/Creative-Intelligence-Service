"""
scrapers/utils.py — Shared helpers: random delays, user-agent rotation,
selector config loading, image downloading, and brand name sanitisation.
"""

import json
import logging
import random
import re
import time
from pathlib import Path

import httpx

from config import (
    ROOT_DIR,
    SCRAPER_DELAY_MAX,
    SCRAPER_DELAY_MIN,
    SCRAPER_TIMEOUT,
    SCRAPER_USER_AGENTS,
)

logger = logging.getLogger(__name__)

_SELECTOR_CONFIG_PATH = ROOT_DIR / "scraper_config.json"


def safe_brand_slug(name: str) -> str:
    """
    Convert a brand/competitor name into a filesystem-safe slug.

    Lowercase, spaces to hyphens, strip all non-alphanumeric/hyphen characters,
    collapse multiple hyphens, and trim leading/trailing hyphens.

    Examples:
        "Mamaearth"           → "mamaearth"
        "WOW Skin Science"    → "wow-skin-science"
        "Plum (Good Vibes!)"  → "plum-good-vibes"
    """
    slug = name.lower().strip()
    slug = slug.replace(" ", "-")
    slug = re.sub(r"[^a-z0-9\-]", "", slug)
    slug = re.sub(r"-{2,}", "-", slug)
    slug = slug.strip("-")
    return slug or "unnamed"


def random_delay() -> None:
    """Sleep for a random duration between SCRAPER_DELAY_MIN and SCRAPER_DELAY_MAX."""
    delay = random.uniform(SCRAPER_DELAY_MIN, SCRAPER_DELAY_MAX)
    logger.debug("Sleeping %.2fs", delay)
    time.sleep(delay)


def random_user_agent() -> str:
    return random.choice(SCRAPER_USER_AGENTS)


def load_selectors(scraper_name: str) -> dict:
    """
    Load CSS selectors for *scraper_name* from scraper_config.json.
    Raises FileNotFoundError if the config file is missing,
    KeyError if the scraper section doesn't exist.
    """
    if not _SELECTOR_CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"scraper_config.json not found at {_SELECTOR_CONFIG_PATH}. "
            "Create it before running scrapers."
        )
    config = json.loads(_SELECTOR_CONFIG_PATH.read_text(encoding="utf-8"))
    if scraper_name not in config:
        raise KeyError(
            f"No selector block for '{scraper_name}' in scraper_config.json"
        )
    return config[scraper_name]


def download_image(url: str, dest_path: Path) -> bool:
    """
    Download image from *url* to *dest_path*.
    Returns True on success, False on any failure.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with httpx.Client(timeout=SCRAPER_TIMEOUT) as client:
            r = client.get(url, headers={"User-Agent": random_user_agent()})
            r.raise_for_status()
        dest_path.write_bytes(r.content)
        logger.debug("Downloaded image → %s", dest_path)
        return True
    except Exception as exc:
        logger.error("Failed to download %s: %s", url, exc)
        return False
