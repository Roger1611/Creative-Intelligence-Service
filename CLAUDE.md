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
analysis/         → Data structuring, profitability filtering, fatigue scoring, category intel,
                    brand intel, competitor deep dive, impact estimation, shared utils
llm/              → API client, prompt chains, prompt templates (in llm/prompts/)
deliverables/     → PDF audit generator, sprint deliverable generator, shared PDF utils
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
| analysis/brand_intel.py | ✅ Done | Product names, prices, ingredients, USPs, language profile, brand voice from ad copy + website; CLI --brand |
| analysis/competitor_deep_dive.py | ✅ Done | Per-competitor profiles, top-5 winner dissections, why_it_works explanations, creative velocity, competitive landscape summary; CLI --brand --competitors |
| analysis/impact_estimator.py | ✅ Done | ₹ impact per gap: fatigue waste, angle/format gap opportunity cost, refresh cycle waste, sprint ROI; CLI --brand --competitors --daily-spend |
| analysis/category_intel.py | ✅ Done | Trigger win rates; format over-performance; underused angles; patterns + opportunities |
| analysis/utils.py | ✅ Done | classify_hook_structure, shared analysis helpers |
| deliverables/utils.py | ✅ Done | format_inr, format_inr_short, severity_color, confidence_badge_text, load_json |
| llm/client.py | ✅ Done | OpenRouter via openai SDK; task→model routing (MODEL_MAP); multimodal vision; retries + fallback; cost logging |
| llm/prompts/*.txt | ✅ Done | competitor_deconstruction, waste_diagnosis, concept_generation (V2: production-ready creative brief format) |
| llm/chains.py | ✅ Done | chain_competitor_analysis, chain_waste_diagnosis, chain_concept_generation, chain_full; DB + JSON output |
| deliverables/audit_generator.py | ✅ Done | V3: 9-page intelligence-grade audit with ₹ impact; CLI --brand --output |
| deliverables/sprint_generator.py | ✅ Done | Full sprint PDF+JSON: exec summary, competitor intel, 50+ concepts by angle, creative calendar; CLI --brand --batch --output |
| feedback/performance_parser.py | ✅ Done | Meta CSV parser; 3-strategy ad matching; fuzzy concept linking; CLI --file --brand |
| feedback/loop.py | ✅ Done | Angle/hook/format analysis; winning patterns text; ROAS-weighted next-batch weights; CLI --category/--brand |
| pipeline.py | ✅ Done | audit (14 steps), sprint (16 steps), batch-audit, refresh (11 steps) modes; brand_intel + competitor_deep_dive + impact_estimator integrated; --dry-run; tqdm progress; RunTracker summary |
| tests/ | ✅ Done | 308 tests: structurer, fatigue scorer, profitability, prompts, entity diversity, audit PDF generation, gap analysis, brand intel, competitor deep dive, impact estimator, deliverables utils |

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

### PDF structure (9 pages) — V3
1. Executive Diagnosis — ₹ waste figure, 4 metric cards, format mix
2. Competitive Landscape — per-competitor breakdown, ranking
3. Competitor War Room — top winner ad dissections with full hooks
4. Hook Swipe File — real hooks by angle, full text, hook structure
5. Creative Gaps with ₹ Impact — gaps sorted by estimated cost
6. Visual Pattern Intelligence — patterns + actionable checklist
7. Creative Strategy Blueprint — product-specific matrix, calendar
8. Sample Creative Briefs — expanded production-ready format
9. Priority Action Plan + ROI — actions, ₹ savings, payback period

### Concept generation data-linking
- `chain_concept_generation()` now passes `hook_database`, `gap_analysis`, `winning_patterns`, `visual_patterns` to prompt
- Prompt requires `data_backing` field (replaces `competitor_reference`) citing real numbers
- `_save_concepts()` appends `[DATA BACKING]` to body_script for downstream use

## Production-Ready Creative Briefs (2026-03-30)

Rewrote concept_generation prompt and chain to produce designer-executable creative briefs instead of generic concepts.

### New data sources loaded in chain_concept_generation()
- `{slug}_brand_intel.json` → products_detected, price_points, key_ingredients, language_profile
- `{slug}_competitor_deep_dive.json` → top 3 winners per competitor with full hook text + why_it_works
- `{slug}_impact_estimate.json` → per_gap_impact sorted by estimated_monthly_impact_inr desc

### New prompt template variables
- `$brand_products`, `$brand_prices`, `$brand_ingredients`, `$brand_language_profile`
- `$competitor_winners`, `$gap_impact_ranking`

### Expanded creative brief schema (concept_generation.txt)
- `hook_text` replaces `hook` — must mention specific product by name
- `hook_text_hindi` — Hindi translation if brand uses Hindi
- `text_overlay` — max 7 words, problem/claim only
- `visual_direction` — now an OBJECT with: aspect_ratio, scene_description, talent_direction, product_placement, lighting, text_overlay_position, color_mood
- `sound_design`, `cta_placement`, `carousel_sequence` (array for carousel, null otherwise)
- `ab_test_variable`, `competitor_inspiration`, `production_difficulty`, `estimated_production_time`
- `data_backing` must cite real numbers from competitor data

### DB changes
- `creative_concepts` table: added `visual_direction_json TEXT`, `brief_json TEXT` columns
- `_save_concepts()` stores full brief as JSON in `brief_json`, serialized visual_direction object in `visual_direction_json`
- `_validate_entity_diversity()` updated to extract keywords from visual_direction object sub-fields

### Pipeline integration (pipeline.py)
- `brand_intel.run()` called after category_intel, before competitor LLM analysis
- `competitor_deep_dive.run()` called after brand_intel, before competitor LLM analysis
- `impact_estimator.run()` called after waste diagnosis LLM, before concept generation
- Audit: 14 steps (was 11). Sprint: 16 steps (was 13). Refresh: 11 steps (was 8).
- All three modes import from `analysis.brand_intel`, `analysis.competitor_deep_dive`, `analysis.impact_estimator`

## Audit V3 (2026-03-30)

Rewrote audit PDF from V2 layout to intelligence-grade 9-page report with ₹ impact figures, competitor war room, and production-ready briefs. Every page now sources data from brand_intel, competitor_deep_dive, and impact_estimator outputs.

### Key changes from V2
- **Page 1 (Executive Diagnosis)**: Now shows ₹ total monthly waste figure from impact_estimator, not just fatigue score
- **Page 2 (Competitive Landscape)**: Replaced "Ad Account Health" with per-competitor breakdown table — ad count, profitable %, format mix, dominant trigger, creative velocity
- **Page 3 (Competitor War Room)**: New page — top winner ad dissections per competitor with full hook text, why_it_works explanation, duration, trigger
- **Page 4 (Hook Swipe File)**: Expanded from summary to full hook text grouped by psychological angle with hook_structure classification
- **Page 5 (Creative Gaps with ₹ Impact)**: Gaps now sorted by estimated_monthly_impact_inr from impact_estimator; each gap shows ₹ cost
- **Page 6 (Visual Pattern Intelligence)**: Added actionable checklist derived from visual_pattern_stats
- **Page 7 (Creative Strategy Blueprint)**: Product-specific matrix using brand_intel products; calendar references actual gap priorities
- **Page 8 (Sample Creative Briefs)**: Production-ready format with visual_direction object, hook_text_hindi, text_overlay, sound_design
- **Page 9 (Priority Action Plan + ROI)**: Added ₹ savings projections and payback period from impact_estimator sprint_roi

### New shared utilities
- **deliverables/utils.py**: Extracted `format_inr()`, `format_inr_short()`, `severity_color()`, `confidence_badge_text()`, `load_json()` from audit_generator for reuse across deliverables
- **analysis/utils.py**: Extracted `classify_hook_structure()` for reuse across category_intel and audit_generator

### Data sources per page
- Pages 1, 5, 9: `{slug}_impact_estimate.json` (impact_estimator output)
- Pages 2, 3: `{slug}_competitor_deep_dive.json` (competitor_deep_dive output)
- Pages 4, 6: `{slug}_category_intelligence.json` (category_intel output)
- Page 7: `{slug}_brand_intel.json` (brand_intel output)
- Page 8: `creative_concepts` table (chain_concept_generation output)
- All pages: `{slug}_fatigue.json`, DB queries for ad counts/durations