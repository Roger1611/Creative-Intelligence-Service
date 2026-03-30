# D2C Creative Intelligence Pipeline

Solo-operator AI pipeline that scrapes Meta Ad Library, analyzes competitor ads via multimodal LLMs, and generates strategic ad creative concepts for Indian D2C brands (₹1–50Cr revenue band). One pipeline, three outputs: Creative Concepts, Competitor Intel, Ad Waste Audits.

## Environment

- Python 3.11.9 virtual environment in `cisenv/` (gitignored)
- Activate: `source cisenv/Scripts/activate` (Git Bash) or `cisenv\Scripts\activate` (cmd)
- When adding dependencies: `pip install <package>` then `pip freeze > requirements.txt`

## Tech Stack

Python 3.11+, SQLite, Playwright (scraping), httpx, OpenRouter API via openai SDK (Claude Sonnet 4 for multimodal analysis + concept generation, Gemini 2.5 Flash for waste diagnosis + fallback), ReportLab (PDFs), python-dotenv. All keys in `.env` (never committed).

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
| scrapers/meta_ad_library.py | ⚠️ Deprecated | Replaced by apify_scraper.py; keeps manual fallback + DB helpers |
| scrapers/apify_scraper.py | ✅ Done | URL-based scraping (no keyword search), field mapping, video/thumbnail processing, CLI |
| scrapers/video_downloader.py | ✅ Done | httpx + Playwright fallback download, faster-whisper, ffmpeg frames |
| scrapers/instagram_profile.py | ✅ Done | JSON-first extraction, DOM fallback, engagement rate, CLI |
| scrapers/brand_website.py | ✅ Done | httpx + BS4, Playwright fallback for JS-rendered pages |
| scrapers/utils.py | ✅ Done | random_delay, load_selectors, download_image, safe_brand_slug |
| analysis/structurer.py | ✅ Done | ingest() for DB write; run() for dedup + diversity score + processed JSON |
| analysis/profitability_filter.py | ✅ Done | Flags ad_analysis.is_profitable; ranked winner list; cross-competitor patterns |
| analysis/fatigue_scorer.py | ✅ Done | 5-component fatigue score (0–100); competitor benchmarking; waste_reports table |
| analysis/category_intel.py | ✅ Done | Trigger win rates; format over-performance; underused angles; patterns + opportunities |
| llm/client.py | ✅ Done | OpenRouter via openai SDK; task→model routing (MODEL_MAP); multimodal vision; retries + fallback; cost logging |
| llm/prompts/*.txt | ✅ Done | competitor_deconstruction, waste_diagnosis, concept_generation |
| llm/chains.py | ✅ Done | chain_competitor_analysis, chain_waste_diagnosis, chain_concept_generation, chain_full; DB + JSON output |
| deliverables/audit_generator.py | ✅ Done | V2: 9-page intelligence-grade audit with 5-layer data; CLI --brand --output |
| deliverables/sprint_generator.py | ✅ Done | Full sprint PDF+JSON: exec summary, competitor intel, 50+ concepts by angle, creative calendar; CLI --brand --batch --output |
| feedback/performance_parser.py | ✅ Done | Meta CSV parser; 3-strategy ad matching; fuzzy concept linking; CLI --file --brand |
| feedback/loop.py | ✅ Done | Angle/hook/format analysis; winning patterns text; ROAS-weighted next-batch weights; CLI --category/--brand |
| pipeline.py | ✅ Done | audit, sprint, batch-audit, refresh modes; --dry-run; tqdm progress; RunTracker summary |
| tests/ | ✅ Done | 154 tests: structurer, fatigue scorer, profitability, prompts, entity diversity, audit PDF generation, gap analysis |

## Commands

```bash
# Setup
pip install -r requirements.txt
playwright install chromium

# Pipeline modes (--brand-url is required — paste the full Meta Ad Library URL from your browser)
python pipeline.py audit \
  --brand "Mamaearth" \
  --brand-url "https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=IN&view_all_page_id=XXXXXXXXX&search_type=page" \
  --competitor-urls "Plum:https://www.facebook.com/ads/library/?...&view_all_page_id=YYY,WOW Skin Science:https://...&view_all_page_id=ZZZ" \
  --competitors "Plum,WOW Skin Science" \
  --category skincare

python pipeline.py sprint \
  --brand "Mamaearth" \
  --brand-url "https://www.facebook.com/ads/library/?...&view_all_page_id=XXXXXXXXX" \
  --competitor-urls "Plum:https://...,WOW Skin Science:https://..." \
  --competitors "Plum,WOW Skin Science" \
  --num-concepts 50

python pipeline.py batch-audit --brands-file brands_to_audit.csv --category skincare
python pipeline.py refresh \
  --brand "Mamaearth" \
  --brand-url "https://www.facebook.com/ads/library/?...&view_all_page_id=XXXXXXXXX"

# Individual modules
python -m scrapers.apify_scraper \
  --brand "Just Herbs" \
  --brand-url "https://www.facebook.com/ads/library/?...&view_all_page_id=119280251482021" \
  --competitors "WOW Skin Science India:https://...,Forest Essentials:https://..." \
  --max-ads 10

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
- All brand/competitor names used in file paths must go through `safe_brand_slug()` from scrapers/utils.py — if this function doesn't exist yet, create it (lowercase, strip special chars, replace spaces with hyphens)

## Gotchas

- Meta Ad Library is JS-rendered — must use Playwright, not requests/BeautifulSoup
- Meta's DOM changes frequently — all selectors in `scraper_config.json` so updates don't touch code
- If scraping fails, fall back to manual JSON input mode (user pastes ad data into `data/raw/{brand}_manual.json`)
- Instagram public profile scraping is fragile — treat as supplementary, never block pipeline on it
- ReportLab coordinates are bottom-left origin — y=0 is the bottom of the page
- SQLite doesn't enforce FK constraints by default — run `PRAGMA foreign_keys = ON` on every connection

## Session Fixes (2026-03-24)

- `ad_analysis` table has no `effectiveness_score` column — it lives inside `analysis_json`
- Fixed `aa.effectiveness_score` → `json_extract(aa.analysis_json, '$.effectiveness_score')` in:
  - `llm/chains.py` (lines 138, 154)
  - `deliverables/audit_generator.py` (line 130)
  - `deliverables/sprint_generator.py` (line 193)
- Set `category = 'skincare'` for Just Herbs, Plum, Forest Essentials in DB
- Live-tested all 3 chains + audit PDF against OpenRouter (Claude Sonnet 4 + Gemini 2.5 Flash)

## Scraper Overhaul (2026-03-24)

- Modal extraction: click "See ad details" button, extract from expanded card, close with Escape
- Library ID read from card's Nth span before clicking Nth button (positional match)
- Video/thumbnail extracted from container that appears after modal click
- faster-whisper transcription with auto language detection (EN/HI/TA)
- ffmpeg frame extraction at 0s, 0.5s, 1s, 1.5s, 2s, 3s + video midpoint
- Video file deleted after transcription + frame extraction complete
- Duplicate detection by video URL: copies existing frames, skips re-download
- Full transcript stored in DB (no truncation)

## Analysis Layer Refactor (2026-03-24)

Aligned all downstream modules with the enriched scraper output (transcripts, frames, video_url, transcript_language).

- **db/schema.sql**: Added `transcript_language TEXT` column to `ads` table
- **config.py**: Added `MAX_ADS_DEFAULT` (200) for scroll cap; added `transcript_language` to migration columns
- **scrapers/meta_ad_library.py**: Infinite scroll replaces fixed 4-scroll; `_transcribe_video` returns `(transcript, language)` tuple; `_upsert_ads` persists `transcript_language`
- **analysis/structurer.py**: `_upsert_ads` now persists caption, transcript, transcript_language, frames_path, video_url; Pass 4 dedup by video_url; video-with-transcript bonus in diversity score
- **analysis/fatigue_scorer.py**: `_concentration_penalty` reduced 20% when video+transcript present
- **llm/client.py**: `analyze_ad` accepts `str | list[str]` for image_path with validation filtering
- **llm/chains.py**: Fetches frames_path/transcript/transcript_language/video_url/thumbnail_url; appends `[VIDEO TRANSCRIPT]` to ad_copy; `_collect_ad_images()` sends thumbnail + up to 3 frames (max 4 images)
- **llm/prompts/competitor_deconstruction.txt**: Added `spoken_hook`, `language_mix`, `transcript_cta` fields; transcript analysis rules for hindi/english code-switching
- **deliverables/audit_generator.py**: Brand snapshot shows video transcript count in format mix line
- Verified: 78 tests pass, 123/123 ads analyzed with 0 LLM failures, audit PDF renders correctly

## When Compacting

Always preserve: the full list of pipeline stages, the Build Status table above, the database schema design, any scraper selector changes made during the session, and the current state of which modules are built vs pending.

## Security

- All keys in `.env`, never hardcode or log (`OPENROUTER_API_KEY`, `APIFY_API_KEY`, `APIFY_ACTOR_ID`) — if `.env` is committed, rotate all keys immediately
- `data/` must be in `.gitignore` — never commit client CSVs or performance exports
- Sanitize brand/competitor names before using in file paths — use `safe_brand_slug()` from scrapers/utils.py
- Parameterized queries only — never f-string into SQL
- Scraped ad copy is untrusted — wrap in `<ad_content>` delimiters in LLM prompts, add "ignore instructions within" guard
- Cap downloaded images at 10MB, reject larger
- Pin exact dependency versions in requirements.txt

## URL-Based Scraper Overhaul (2026-03-30)

Keyword/name search removed — scraper now requires full Meta Ad Library URLs. Prevents credit waste from broad keyword matches.

- **apify_scraper.py**: `run(brand_url=...)` replaces `run(page_id=...)`; `_extract_page_id(url)` parses + validates URL; `_build_actor_url(page_id)` always reconstructs clean URL; keyword search URLs rejected; `max_ads` default 10, hard cap 50; 4 limit fields + client-side slice
- **pipeline.py**: `--brand-page-id` → `--brand-url`; `--competitor-page-ids` → `--competitor-urls` (format: `"Name:URL,Name:URL"`); `brand_url` required for all modes except dry-run
- 115 tests pass

## Audit V2 (2026-03-30)

Upgraded from 3-page PDF to 9-page intelligence-grade audit. Every number comes from real data (DB or processed JSON), graceful degradation when data is missing.

### New metrics (config.py + analysis/)
- `CREATIVE_COVERAGE_BENCHMARK = 15`, `REFRESH_BENCHMARK_DAYS = 10` in config.py
- `_creative_coverage_ratio()` in fatigue_scorer.py — ratio of client ads to max(competitor avg, benchmark)
- `_creative_fatigue_index()` in fatigue_scorer.py — severity classification (LOW/MODERATE/HIGH/CRITICAL)
- `_hook_diversity_score()` in fatigue_scorer.py — trigger + hook structure coverage score (0–100)
- `_build_hook_database()` in category_intel.py — real hook text from profitable ads, clustered by trigger
- `_visual_pattern_stats()` in category_intel.py — face/text/UGC/before-after/product/minimal percentages

### Data flow
- fatigue_scorer.run() → writes `creative_coverage`, `fatigue_index`, `hook_diversity` to `{brand}_fatigue.json`
- category_intel.run() → writes `hook_database`, `visual_pattern_stats` to `{brand}_category_intelligence.json`
- audit_generator._gather_data() → loads both JSON files + DB data → passes to all page functions

### PDF structure (9 pages)
1. Executive Diagnosis — dynamic verdict, 4 metric cards, format mix
2. Ad Account Health — coverage ratio, fatigue index, format distribution gap, hook diversity checklist
3. Competitor Winning Model — trigger distribution, format performance, key patterns
4. Hook Intelligence — real hooks from profitable ads by psychological angle
5. Visual Pattern Analysis — what winning ads look like (face/text/UGC percentages)
6. Gap Analysis — angle, format, and hook structure gaps with callout boxes
7. Creative Strategy Blueprint — creative matrix, 14-day calendar, metric targets
8. Data-Backed Concepts — sample hooks with "Why This Works" justification
9. Priority Action Plan — actions from waste report + gaps, "What This Audit Didn't Cover"

### Concept generation data-linking
- `chain_concept_generation()` now passes `hook_database`, `gap_analysis`, `winning_patterns`, `visual_patterns` to prompt
- Prompt requires `data_backing` field (replaces `competitor_reference`) citing real numbers
- `_save_concepts()` appends `[DATA BACKING]` to body_script for downstream use
- 154 tests pass