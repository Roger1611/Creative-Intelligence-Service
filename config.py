"""
config.py — Central configuration. Loads .env, exposes typed settings,
and initialises the SQLite database (tables + indexes) on first run.
"""

import logging
import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── Root paths ─────────────────────────────────────────────────────────────────

ROOT_DIR    = Path(__file__).parent
DATA_DIR    = ROOT_DIR / os.getenv("DATA_DIR", "data")
RAW_DIR     = DATA_DIR / "raw"
PROC_DIR    = DATA_DIR / "processed"
PERF_DIR    = DATA_DIR / "performance"
DB_PATH     = ROOT_DIR / os.getenv("DB_PATH", "d2c_intel.db")
SCHEMA_PATH = ROOT_DIR / "db" / "schema.sql"

# ── API keys ───────────────────────────────────────────────────────────────────

OPENROUTER_API_KEY: str = os.getenv("OPENROUTER_API_KEY", "")

if not OPENROUTER_API_KEY:
    logger.warning("OPENROUTER_API_KEY is not set — LLM calls will fail.")

# ── LLM settings ──────────────────────────────────────────────────────────────

# Task → OpenRouter model mapping.  Override individual entries via env vars
# (e.g. MODEL_COMPETITOR_DECONSTRUCTION=anthropic/claude-sonnet-4).
# NOTE: OpenRouter model IDs do NOT use date suffixes — use the bare ID.
MODEL_MAP: dict[str, str] = {
    "competitor_deconstruction": os.getenv(
        "MODEL_COMPETITOR_DECONSTRUCTION",
        "anthropic/claude-sonnet-4",
    ),
    "waste_diagnosis": os.getenv(
        "MODEL_WASTE_DIAGNOSIS",
        "google/gemini-2.5-flash",
    ),
    "concept_generation": os.getenv(
        "MODEL_CONCEPT_GENERATION",
        "anthropic/claude-sonnet-4",
    ),
    "fallback": os.getenv(
        "MODEL_FALLBACK",
        "google/gemini-2.5-flash",
    ),
}

LLM_TEMPERATURE: float = float(os.getenv("LLM_TEMPERATURE", "0.7"))
LLM_MAX_TOKENS:  int   = int(os.getenv("LLM_MAX_TOKENS",   "4096"))

# ── Scraper settings ───────────────────────────────────────────────────────────

SCRAPER_DELAY_MIN: float = float(os.getenv("SCRAPER_DELAY_MIN", "2"))
SCRAPER_DELAY_MAX: float = float(os.getenv("SCRAPER_DELAY_MAX", "5"))
SCRAPER_TIMEOUT:   int   = int(os.getenv("SCRAPER_TIMEOUT",    "30"))

SCRAPER_USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# ── Domain constants ───────────────────────────────────────────────────────────

# Ads running >= this many days are treated as profitable winners.
PROFITABLE_AD_MIN_DAYS: int = 21

# Ads running >= this many days without refresh signal creative fatigue.
FATIGUE_AD_MIN_DAYS: int = 30

PSYCHOLOGICAL_TRIGGERS: list[str] = [
    "status", "fear", "social_proof", "transformation",
    "agitation_solution", "curiosity", "urgency",
    "authority", "belonging", "aspiration",
]

VALID_CATEGORIES: list[str] = [
    "skincare", "supplements", "fashion", "food", "wellness",
]

VALID_CREATIVE_TYPES: list[str] = ["static", "carousel", "video", "reel"]


# ── Database helpers ───────────────────────────────────────────────────────────

def get_connection() -> sqlite3.Connection:
    """Return a connection with FK enforcement and row_factory set."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    """Create all tables and indexes defined in schema.sql if they don't exist."""
    for directory in (RAW_DIR, PROC_DIR, PERF_DIR):
        directory.mkdir(parents=True, exist_ok=True)

    schema = SCHEMA_PATH.read_text(encoding="utf-8")
    with get_connection() as conn:
        conn.executescript(schema)

    logger.info("Database initialised at %s", DB_PATH)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    init_db()
    print(f"DB ready at {DB_PATH}")
