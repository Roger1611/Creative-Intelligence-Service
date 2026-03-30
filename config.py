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

APIFY_API_KEY: str = os.getenv("APIFY_API_KEY", "")
APIFY_ACTOR_ID: str = os.getenv("APIFY_ACTOR_ID", "apify/meta-ads-library-scraper")
APIFY_TIMEOUT_SECS: int = int(os.getenv("APIFY_TIMEOUT_SECS", "300"))

if not APIFY_API_KEY:
    logger.warning("APIFY_API_KEY is not set — Apify scraping will fail.")

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
MAX_ADS_DEFAULT:   int   = int(os.getenv("MAX_ADS_DEFAULT",    "200"))

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

# Audit V2 metrics
CREATIVE_COVERAGE_BENCHMARK: int = 15  # Meta Andromeda demands 15-20+ variations
REFRESH_BENCHMARK_DAYS: int = 10       # Optimal refresh window in days

# Set FORCE_REANALYZE=1 to skip the analysis cache and re-analyze all ads.
FORCE_REANALYZE: bool = os.getenv("FORCE_REANALYZE", "0") == "1"


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

    # Migrate: add new columns to ads table if missing (for existing DBs)
    _migrate_ads_table()

    logger.info("Database initialised at %s", DB_PATH)


def _migrate_ads_table() -> None:
    """Add caption/transcript/frames_path/video_url columns if they don't exist."""
    new_columns = {
        "caption": "TEXT",
        "transcript": "TEXT",
        "transcript_language": "TEXT",
        "frames_path": "TEXT",
        "video_url": "TEXT",
    }
    with get_connection() as conn:
        existing = {
            row[1] for row in conn.execute("PRAGMA table_info(ads)").fetchall()
        }
        for col, typ in new_columns.items():
            if col not in existing:
                conn.execute(f"ALTER TABLE ads ADD COLUMN {col} {typ}")
                logger.info("Migrated ads table: added column '%s'", col)

    _migrate_ad_analysis_table()
    _migrate_creative_concepts_table()


def _migrate_ad_analysis_table() -> None:
    """Add hook_structure/semantic_cluster/thumb_stop_score/trust_stack_json if missing."""
    new_columns = {
        "hook_structure": "TEXT",
        "semantic_cluster": "TEXT",
        "thumb_stop_score": "INTEGER",
        "trust_stack_json": "TEXT",
    }
    with get_connection() as conn:
        existing = {
            row[1] for row in conn.execute("PRAGMA table_info(ad_analysis)").fetchall()
        }
        for col, typ in new_columns.items():
            if col not in existing:
                conn.execute(f"ALTER TABLE ad_analysis ADD COLUMN {col} {typ}")
                logger.info("Migrated ad_analysis table: added column '%s'", col)


def _migrate_creative_concepts_table() -> None:
    """Add hook_structure/entity_id_tag/trust_stack_json/format_spec/thumb_stop_score if missing."""
    new_columns = {
        "hook_structure": "TEXT",
        "entity_id_tag": "TEXT",
        "trust_stack_json": "TEXT",
        "format_spec": "TEXT",
        "thumb_stop_score": "INTEGER",
    }
    with get_connection() as conn:
        existing = {
            row[1] for row in conn.execute("PRAGMA table_info(creative_concepts)").fetchall()
        }
        for col, typ in new_columns.items():
            if col not in existing:
                conn.execute(f"ALTER TABLE creative_concepts ADD COLUMN {col} {typ}")
                logger.info("Migrated creative_concepts table: added column '%s'", col)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    init_db()
    print(f"DB ready at {DB_PATH}")
