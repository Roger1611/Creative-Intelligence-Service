# D2C Creative Intelligence Pipeline

Solo-operator AI pipeline that scrapes Meta Ad Library, analyzes competitor ads via multimodal LLMs, and generates strategic ad creative concepts for Indian D2C brands (₹1–50Cr revenue band). One pipeline, three outputs: Creative Concepts, Competitor Intel, Ad Waste Audits.

## Environment

- Python 3.11.9 virtual environment in `cisenv/` (gitignored)
- Activate: `source cisenv/Scripts/activate` (Git Bash) or `cisenv\Scripts\activate` (cmd)
- When adding dependencies: `pip install <package>` then `pip freeze > requirements.txt`

## Tech Stack

Python 3.11+, SQLite, Playwright (scraping), httpx, Anthropic Claude API (multimodal analysis + generation), OpenAI GPT-4o (fallback), ReportLab (PDFs), python-dotenv. All keys in `.env` (never committed).

## Project Structure

```
scrapers/         → Meta Ad Library + Instagram + brand site scrapers (Playwright)
analysis/         → Data structuring, profitability filtering, fatigue scoring, category intel
llm/              → API client, prompt chains, prompt templates (in llm/prompts/)
deliverables/     → PDF audit generator, sprint deliverable generator
feedback/         → Performance data parsing + feedback loop for concept improvement
db/               → SQLite schema
data/             → raw/ (scraped), processed/ (structured JSON), performance/ (client CSVs)
pipeline.py       → Main orchestrator (audit, sprint, batch-audit, refresh modes)
config.py         → Central config, loads .env, initializes DB on first run
```

## Build Status

| Module | Status | Notes |
|--------|--------|-------|
| CLAUDE.md | ✅ Done | |
| db/schema.sql | ✅ Done | 8 tables incl. instagram_profiles; VIRTUAL duration_days column |
| config.py | ✅ Done | init_db(), get_connection(), all constants |
| scraper_config.json | ✅ Done | All CSS selectors for Meta + Instagram; update here when DOM changes |
| scrapers/meta_ad_library.py | ✅ Done | Playwright, 4-strategy ID extraction, retry+backoff, pagination, CLI |
| scrapers/instagram_profile.py | ✅ Done | JSON-first extraction, DOM fallback, engagement rate, CLI |
| scrapers/brand_website.py | ✅ Done | httpx + BS4, Playwright fallback for JS-rendered pages |
| scrapers/utils.py | ✅ Done | random_delay, load_selectors, download_image |
| analysis/structurer.py | ✅ Done | ingest() for DB write; run() for dedup + diversity score + processed JSON |
| analysis/profitability_filter.py | ✅ Done | Flags ad_analysis.is_profitable; ranked winner list; cross-competitor patterns |
| analysis/fatigue_scorer.py | ✅ Done | 5-component fatigue score (0–100); competitor benchmarking; waste_reports table |
| analysis/category_intel.py | ✅ Done | Trigger win rates; format over-performance; underused angles; patterns + opportunities |
| llm/client.py | 🔨 Needs rewrite | Anthropic primary / OpenAI fallback; token + cost logging; JSON parse |
| llm/prompts/*.txt | 🔨 Needs rewrite | competitor_deconstruction, waste_diagnosis, concept_generation |
| llm/chains.py | 🔨 Needs rewrite | run_competitor_deconstruction, run_waste_diagnosis, run_concept_generation |
| deliverables/audit_generator.py | 🔨 Needs rewrite | ReportLab PDF: score card, fatigue flags, recommendations, summary |
| deliverables/sprint_generator.py | 🔨 Needs rewrite | ReportLab PDF + JSON dump; one section per concept |
| feedback/performance_parser.py | 🔨 Needs rewrite | Parses Meta Ads Manager CSV; resolves ad_library_id FK |
| feedback/loop.py | 🔨 Needs rewrite | ROAS-weighted angle scoring; next-batch concept weights |
| pipeline.py | 🔨 Needs rewrite | audit, sprint, batch-audit, refresh modes |
| tests/ | 🔨 Needs rewrite | |

## Commands

```bash
# Setup
pip install -r requirements.txt
playwright install chromium

# Pipeline modes
python pipeline.py audit --brand "Mamaearth" --competitors "Plum,WOW Skin Science" --category skincare
python pipeline.py sprint --brand "Mamaearth" --competitors "Plum,WOW Skin Science" --num-concepts 50
python pipeline.py batch-audit --brands-file brands_to_audit.csv --category skincare
python pipeline.py refresh --brand "Mamaearth"

# Individual modules
python -m scrapers.meta_ad_library --brand "Mamaearth" --competitors "Plum,WOW"
python -m deliverables.audit_generator --brand "Mamaearth" --output audits/
python -m feedback.performance_parser --file export.csv --brand "Mamaearth"

# Tests
pytest tests/ -v
```

## Key Domain Concepts

- **Profitability proxy**: Ads running 21+ days are flagged as probable winners (no brand funds a losing ad for 3 weeks)
- **Fatigue signal**: Ads running 30+ days without refresh = critical creative fatigue
- **Creative diversity score**: 0–100 metric, four 25-pt components: format variety + copy variation (unique copies/total) + visual variety (unique thumbnails/total) + creative volume (count vs benchmark of 20)
- **Psychological triggers**: status, fear, social_proof, transformation, agitation_solution, curiosity, urgency, authority, belonging, aspiration

## Coding Rules

- All scraper CSS selectors go in `scraper_config.json` — never hardcode selectors in Python
- Every module exposes a `run(brand_name, competitor_names)` callable from `pipeline.py`
- `structurer` is the exception: `ingest(brand_name, raw_ads, ...)` writes to DB right after scraping; `run()` is the analysis pass that reads from DB and writes processed JSON
- LLM prompts live in `llm/prompts/*.txt` as plain text files, not inline strings
- All LLM outputs must be structured JSON — never free-form text
- Log every API call with token count and estimated cost to stdout
- Scraper actions must have randomized delays (2–5s) between requests
- Downloaded ad images go to `data/raw/{brand_name}/` — never store images in the DB
- Use `logging` module everywhere, not `print()`
- Functions should be pure where possible — scraper state flows through SQLite, not globals

## Gotchas

- Meta Ad Library is JS-rendered — must use Playwright, not requests/BeautifulSoup
- Meta's DOM changes frequently — all selectors in `scraper_config.json` so updates don't touch code
- If scraping fails, fall back to manual JSON input mode (user pastes ad data into `data/raw/{brand}_manual.json`)
- Instagram public profile scraping is fragile — treat as supplementary, never block pipeline on it
- ReportLab coordinates are bottom-left origin — y=0 is the bottom of the page
- SQLite doesn't enforce FK constraints by default — run `PRAGMA foreign_keys = ON` on every connection

## When Compacting

Always preserve: the full list of pipeline stages, the Build Status table above, the database schema design, any scraper selector changes made during the session, and the current state of which modules are built vs pending.

