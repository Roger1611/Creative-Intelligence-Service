"""
pipeline.py — Main orchestrator for the D2C Creative Intelligence pipeline.

Modes
-----
  audit       — Free Ad Fatigue Audit PDF (prospecting / lead-gen)
  sprint      — Paid deliverable: full concept generation + sprint PDF
  batch-audit — Run audit for every brand in a CSV
  refresh     — Re-scrape existing brand, diff vs previous data, generate new concepts

Usage
-----
  python pipeline.py audit --brand "Mamaearth" --competitors "Plum,WOW Skin Science,mCaffeine" --category skincare
  python pipeline.py sprint --brand "Mamaearth" --competitors "Plum,WOW" --num-concepts 50
  python pipeline.py batch-audit --brands-file brands_to_audit.csv --category skincare
  python pipeline.py refresh --brand "Mamaearth"

Add --dry-run to any mode to preview steps without scraping or calling LLMs.
"""

import argparse
import csv
import json
import logging
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

from tqdm import tqdm

import config  # triggers load_dotenv, exposes constants

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")


# ── Run tracker ───────────────────────────────────────────────────────────────

class RunTracker:
    """Accumulates per-brand success/failure info for the final summary."""

    def __init__(self):
        self.successes: list[str] = []
        self.failures: list[tuple[str, str]] = []  # (brand, reason)
        self._start = time.monotonic()

    def ok(self, brand: str) -> None:
        self.successes.append(brand)

    def fail(self, brand: str, reason: str) -> None:
        self.failures.append((brand, reason))

    def summary(self) -> str:
        elapsed = time.monotonic() - self._start
        total = len(self.successes) + len(self.failures)
        lines = [
            "",
            "=" * 60,
            f"  Pipeline complete in {elapsed:.1f}s",
            f"  Succeeded: {len(self.successes)}/{total}",
        ]
        if self.failures:
            lines.append(f"  Failed:    {len(self.failures)}/{total}")
            for brand, reason in self.failures:
                lines.append(f"    • {brand}: {reason}")
        lines.append("=" * 60)
        return "\n".join(lines)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="pipeline",
        description="D2C Creative Intelligence Pipeline",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview steps without scraping or calling LLMs",
    )

    sub = parser.add_subparsers(dest="mode", required=True)

    # audit
    p_audit = sub.add_parser("audit",
                             help="Generate a free Creative Waste Audit PDF")
    p_audit.add_argument("--brand",       required=True)
    p_audit.add_argument("--competitors", default="",
                         help="Comma-separated competitor brand names")
    p_audit.add_argument("--category",    choices=config.VALID_CATEGORIES)
    p_audit.add_argument("--output",      default="audits")
    _add_url_args(p_audit)

    # sprint
    p_sprint = sub.add_parser("sprint",
                              help="Generate full creative sprint (paid)")
    p_sprint.add_argument("--brand",        required=True)
    p_sprint.add_argument("--competitors",  default="")
    p_sprint.add_argument("--category",     choices=config.VALID_CATEGORIES)
    p_sprint.add_argument("--num-concepts", type=int, default=50)
    p_sprint.add_argument("--output",       default="sprints")
    _add_url_args(p_sprint)

    # batch-audit
    p_batch = sub.add_parser("batch-audit",
                             help="Run audit for brands listed in a CSV")
    p_batch.add_argument("--brands-file", required=True,
                         help="CSV with columns: brand_name, competitors, category "
                              "(optional: brand_url, competitor_urls)")
    p_batch.add_argument("--category",    choices=config.VALID_CATEGORIES,
                         help="Default category if CSV row is blank")
    p_batch.add_argument("--output",      default="audits")
    _add_url_args(p_batch)

    # refresh
    p_refresh = sub.add_parser("refresh",
                               help="Re-scrape and update an existing brand")
    p_refresh.add_argument("--brand", required=True)
    _add_url_args(p_refresh)

    args = parser.parse_args()

    # Initialise DB + data dirs
    config.init_db()

    tracker = RunTracker()

    competitor_urls = _parse_competitor_urls(
        getattr(args, "competitor_urls", None)
    )
    brand_url = getattr(args, "brand_url", None)

    if args.mode == "audit":
        _run_audit(
            brand_name=args.brand,
            competitors=_split(args.competitors),
            category=args.category,
            output=args.output,
            dry_run=args.dry_run,
            tracker=tracker,
            brand_url=brand_url,
            competitor_urls=competitor_urls,
        )

    elif args.mode == "sprint":
        _run_sprint(
            brand_name=args.brand,
            competitors=_split(args.competitors),
            category=args.category,
            num_concepts=args.num_concepts,
            output=args.output,
            dry_run=args.dry_run,
            tracker=tracker,
            brand_url=brand_url,
            competitor_urls=competitor_urls,
        )

    elif args.mode == "batch-audit":
        _run_batch_audit(
            brands_file=args.brands_file,
            default_category=args.category,
            output=args.output,
            dry_run=args.dry_run,
            tracker=tracker,
            brand_url=brand_url,
            competitor_urls=competitor_urls,
        )

    elif args.mode == "refresh":
        _run_refresh(
            brand_name=args.brand,
            dry_run=args.dry_run,
            tracker=tracker,
            brand_url=brand_url,
            competitor_urls=competitor_urls,
        )

    print(tracker.summary())


# ── Pipeline steps (with progress tracking) ──────────────────────────────────

AUDIT_STEPS = [
    "Scrape via Apify (brand + competitors)",
    "Scrape Instagram profiles",
    "Scrape brand websites",
    "Ingest & structure data",
    "Profitability filter",
    "Fatigue scoring",
    "Category intelligence",
    "Brand product intelligence",
    "Competitor deep dive",
    "Competitor analysis (LLM)",
    "Waste diagnosis (LLM)",
    "Spend impact estimation",
    "Generate sample briefs (LLM)",
    "Build audit PDF",
]

SPRINT_EXTRA_STEPS = [
    "Generate full concepts (LLM)",
    "Build sprint deliverable",
]


# ── MODE: audit ───────────────────────────────────────────────────────────────

def _run_audit(
    brand_name: str,
    competitors: list[str],
    category: str | None,
    output: str,
    dry_run: bool,
    tracker: RunTracker,
    brand_url: str | None = None,
    competitor_urls: list[dict] | None = None,
) -> Path | None:
    """Run the full audit pipeline. Returns the PDF path on success."""
    logger.info("=" * 50)
    logger.info("AUDIT: %s  |  competitors: %s  |  category: %s",
                brand_name, ", ".join(competitors) or "(none)", category or "auto")

    if dry_run:
        _dry_run_preview("audit", brand_name, competitors, AUDIT_STEPS)
        tracker.ok(brand_name)
        return None

    if not brand_url:
        raise ValueError(
            "brand_url is required. Pass --brand-url with the full "
            "Meta Ad Library URL for the client brand."
        )

    competitor_urls = competitor_urls or []

    progress = tqdm(AUDIT_STEPS, desc=brand_name, unit="step", leave=True)
    try:
        # Lazy imports so startup stays fast
        from scrapers import apify_scraper, instagram_profile, brand_website
        from analysis import structurer, profitability_filter, fatigue_scorer
        from analysis import category_intel, brand_intel, competitor_deep_dive
        from analysis import impact_estimator
        from llm import chains
        from deliverables import audit_generator

        # 1. Scrape via Apify API
        progress.set_postfix_str("Scrape via Apify API")
        raw = apify_scraper.run(
            brand_name=brand_name,
            brand_url=brand_url,
            competitors=competitor_urls,
        )
        progress.update(1)

        # 2. Scrape Instagram (non-blocking — failures don't crash pipeline)
        progress.set_postfix_str("Instagram")
        _scrape_instagram_safe(instagram_profile, brand_name, competitors)
        progress.update(1)

        # 3. Scrape brand websites
        progress.set_postfix_str("websites")
        _scrape_websites_safe(brand_website, brand_name, competitors)
        progress.update(1)

        # 4. Ingest & structure
        progress.set_postfix_str("structuring")
        brand_id = structurer.ingest(
            brand_name, raw.get("brand", []),
            is_client=True, category=category,
        )
        for comp_name, comp_ads in raw.get("competitors", {}).items():
            comp_id = structurer.ingest(comp_name, comp_ads, category=category)
            _ensure_competitor_set(brand_id, comp_id)
        structurer.run(brand_name, competitors)
        progress.update(1)

        # 5. Profitability filter
        progress.set_postfix_str("profitability")
        profitability_filter.run(brand_name, competitors)
        progress.update(1)

        # 6. Fatigue scoring
        progress.set_postfix_str("fatigue")
        fatigue_scorer.run(brand_name, competitors)
        progress.update(1)

        # 7. Category intelligence
        progress.set_postfix_str("category intel")
        category_intel.run(brand_name, competitors)
        progress.update(1)

        # 8. Brand product intelligence
        progress.set_postfix_str("brand intel")
        brand_intel.run(brand_name)
        progress.update(1)

        # 9. Competitor deep dive
        progress.set_postfix_str("competitor deep dive")
        competitor_deep_dive.run(brand_name, competitors)
        progress.update(1)

        # 10. Competitor analysis (LLM)
        progress.set_postfix_str("LLM: competitor analysis")
        chains.chain_competitor_analysis(brand_name)
        progress.update(1)

        # 11. Waste diagnosis (LLM)
        progress.set_postfix_str("LLM: waste diagnosis")
        chains.chain_waste_diagnosis(brand_name)
        progress.update(1)

        # 12. Spend impact estimation
        progress.set_postfix_str("impact estimation")
        impact_estimator.run(brand_name, competitors)
        progress.update(1)

        # 13. Generate 5 sample briefs (LLM)
        progress.set_postfix_str("LLM: sample briefs")
        chains.chain_concept_generation(brand_name, num_concepts=5)
        progress.update(1)

        # 14. Build audit PDF
        progress.set_postfix_str("building PDF")
        pdf_path = audit_generator.run(brand_name, output_dir=output)
        progress.update(1)

        progress.set_postfix_str("done")
        progress.close()
        logger.info("Audit PDF ready: %s", pdf_path)
        tracker.ok(brand_name)
        return pdf_path

    except Exception as exc:
        progress.close()
        reason = _short_reason(exc)
        logger.error("Audit FAILED for '%s': %s", brand_name, reason)
        logger.debug(traceback.format_exc())
        tracker.fail(brand_name, reason)
        return None


# ── MODE: sprint ──────────────────────────────────────────────────────────────

def _run_sprint(
    brand_name: str,
    competitors: list[str],
    category: str | None,
    num_concepts: int,
    output: str,
    dry_run: bool,
    tracker: RunTracker,
    brand_url: str | None = None,
    competitor_urls: list[dict] | None = None,
) -> Path | None:
    """Run audit pipeline + full concept generation + sprint deliverable."""
    logger.info("=" * 50)
    logger.info("SPRINT: %s  |  %d concepts  |  competitors: %s",
                brand_name, num_concepts, ", ".join(competitors) or "(none)")

    all_steps = AUDIT_STEPS + SPRINT_EXTRA_STEPS

    if dry_run:
        _dry_run_preview("sprint", brand_name, competitors, all_steps)
        tracker.ok(brand_name)
        return None

    if not brand_url:
        raise ValueError(
            "brand_url is required. Pass --brand-url with the full "
            "Meta Ad Library URL for the client brand."
        )

    competitor_urls = competitor_urls or []

    progress = tqdm(all_steps, desc=brand_name, unit="step", leave=True)
    try:
        from scrapers import apify_scraper, instagram_profile, brand_website
        from analysis import structurer, profitability_filter, fatigue_scorer
        from analysis import category_intel, brand_intel, competitor_deep_dive
        from analysis import impact_estimator
        from llm import chains
        from deliverables import audit_generator, sprint_generator

        # Steps 1–14: same as audit
        raw = apify_scraper.run(
            brand_name=brand_name,
            brand_url=brand_url,
            competitors=competitor_urls,
        )
        progress.update(1)

        _scrape_instagram_safe(instagram_profile, brand_name, competitors)
        progress.update(1)

        _scrape_websites_safe(brand_website, brand_name, competitors)
        progress.update(1)

        brand_id = structurer.ingest(
            brand_name, raw.get("brand", []),
            is_client=True, category=category,
        )
        for comp_name, comp_ads in raw.get("competitors", {}).items():
            comp_id = structurer.ingest(comp_name, comp_ads, category=category)
            _ensure_competitor_set(brand_id, comp_id)
        structurer.run(brand_name, competitors)
        progress.update(1)

        profitability_filter.run(brand_name, competitors)
        progress.update(1)

        fatigue_scorer.run(brand_name, competitors)
        progress.update(1)

        category_intel.run(brand_name, competitors)
        progress.update(1)

        brand_intel.run(brand_name)
        progress.update(1)

        competitor_deep_dive.run(brand_name, competitors)
        progress.update(1)

        chains.chain_competitor_analysis(brand_name)
        progress.update(1)

        chains.chain_waste_diagnosis(brand_name)
        progress.update(1)

        impact_estimator.run(brand_name, competitors)
        progress.update(1)

        # 13. Sample briefs (still generated for audit PDF side)
        chains.chain_concept_generation(brand_name, num_concepts=5)
        progress.update(1)

        # 14. Build audit PDF (included even in sprint for reference)
        audit_generator.run(brand_name, output_dir=output)
        progress.update(1)

        # 15. Full concept generation (LLM)
        progress.set_postfix_str(f"LLM: {num_concepts} concepts")
        chains.chain_concept_generation(brand_name, num_concepts=num_concepts)
        progress.update(1)

        # 16. Build sprint deliverable
        progress.set_postfix_str("building sprint PDF")
        pdf_path = sprint_generator.run(brand_name, output_dir=output)
        progress.update(1)

        progress.set_postfix_str("done")
        progress.close()
        logger.info("Sprint deliverable ready: %s", pdf_path)
        tracker.ok(brand_name)
        return pdf_path

    except Exception as exc:
        progress.close()
        reason = _short_reason(exc)
        logger.error("Sprint FAILED for '%s': %s", brand_name, reason)
        logger.debug(traceback.format_exc())
        tracker.fail(brand_name, reason)
        return None


# ── MODE: batch-audit ─────────────────────────────────────────────────────────

def _run_batch_audit(
    brands_file: str,
    default_category: str | None,
    output: str,
    dry_run: bool,
    tracker: RunTracker,
    brand_url: str | None = None,
    competitor_urls: list[dict] | None = None,
) -> None:
    """Run audit mode for every brand in a CSV file."""
    path = Path(brands_file)
    if not path.exists():
        logger.error("Brands file not found: %s", brands_file)
        sys.exit(1)

    # Parse CSV
    with path.open(encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        logger.error("CSV is empty: %s", brands_file)
        sys.exit(1)

    # Batch output directory
    date_stamp = datetime.now().strftime("%Y%m%d")
    batch_dir = Path(output) / f"batch_{date_stamp}"
    batch_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Batch audit: %d brands → %s", len(rows), batch_dir)

    cli_competitor_urls = competitor_urls or []

    for i, row in enumerate(rows, 1):
        brand = (row.get("brand_name") or row.get("brand", "")).strip()
        competitors = _split(row.get("competitors", ""))
        category = (row.get("category") or "").strip() or default_category

        # Per-row URLs from CSV (override CLI args if present)
        row_brand_url = (row.get("brand_url") or "").strip() or brand_url
        row_comp_urls = list(cli_competitor_urls)
        csv_comp_urls = (row.get("competitor_urls") or "").strip()
        if csv_comp_urls:
            row_comp_urls = _parse_competitor_urls(csv_comp_urls)

        if not brand:
            logger.warning("Row %d: empty brand name, skipping", i)
            continue

        logger.info("[%d/%d] Processing '%s'", i, len(rows), brand)

        _run_audit(
            brand_name=brand,
            competitors=competitors,
            category=category,
            output=str(batch_dir),
            dry_run=dry_run,
            tracker=tracker,
            brand_url=row_brand_url,
            competitor_urls=row_comp_urls,
        )


# ── MODE: refresh ─────────────────────────────────────────────────────────────

def _run_refresh(
    brand_name: str,
    dry_run: bool,
    tracker: RunTracker,
    brand_url: str | None = None,
    competitor_urls: list[dict] | None = None,
) -> None:
    """Re-scrape, diff against previous data, generate new concepts."""
    logger.info("=" * 50)
    logger.info("REFRESH: %s", brand_name)

    if not brand_url:
        raise ValueError(
            "brand_url is required. Pass --brand-url with the full "
            "Meta Ad Library URL for the client brand."
        )

    # Look up brand + competitors from DB
    conn = config.get_connection()
    try:
        brand_row = conn.execute(
            "SELECT * FROM brands WHERE name = ? AND is_client = 1",
            (brand_name,),
        ).fetchone()
        if not brand_row:
            logger.error("Brand '%s' not in database. Run audit first.",
                         brand_name)
            tracker.fail(brand_name, "not in database — run audit first")
            return
        brand_row = dict(brand_row)

        competitors = _fetch_competitor_names(brand_row["id"])
        category = brand_row.get("category")

        # Snapshot previous state for diffing
        prev_ad_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM ads WHERE brand_id = ?",
            (brand_row["id"],),
        ).fetchone()["cnt"]
        prev_comp_ads = {}
        for comp_name in competitors:
            comp_row = conn.execute(
                "SELECT id FROM brands WHERE name = ?", (comp_name,),
            ).fetchone()
            if comp_row:
                cnt = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM ads WHERE brand_id = ?",
                    (comp_row["id"],),
                ).fetchone()["cnt"]
                prev_comp_ads[comp_name] = cnt
    finally:
        conn.close()

    competitor_urls = competitor_urls or []

    steps = [
        "Scrape via Apify API",
        "Re-scrape Instagram",
        "Re-ingest & structure",
        "Re-run analysis pipeline",
        "Brand product intelligence",
        "Competitor deep dive",
        "Competitor analysis (LLM)",
        "Waste diagnosis (LLM)",
        "Spend impact estimation",
        "Generate new briefs (LLM)",
        "Diff report",
    ]

    if dry_run:
        _dry_run_preview("refresh", brand_name, competitors, steps)
        tracker.ok(brand_name)
        return

    progress = tqdm(steps, desc=f"refresh:{brand_name}", unit="step", leave=True)
    try:
        from scrapers import apify_scraper, instagram_profile
        from analysis import structurer, profitability_filter, fatigue_scorer
        from analysis import category_intel, brand_intel, competitor_deep_dive
        from analysis import impact_estimator
        from llm import chains

        # 1. Re-scrape via Apify
        progress.set_postfix_str("Scrape via Apify API")
        raw = apify_scraper.run(
            brand_name=brand_name,
            brand_url=brand_url,
            competitors=competitor_urls,
        )
        progress.update(1)

        # 2. Instagram
        progress.set_postfix_str("Instagram")
        _scrape_instagram_safe(instagram_profile, brand_name, competitors)
        progress.update(1)

        # 3. Re-ingest
        progress.set_postfix_str("ingesting")
        brand_id = structurer.ingest(
            brand_name, raw.get("brand", []),
            is_client=True, category=category,
        )
        for comp_name, comp_ads in raw.get("competitors", {}).items():
            comp_id = structurer.ingest(comp_name, comp_ads, category=category)
            _ensure_competitor_set(brand_id, comp_id)
        structurer.run(brand_name, competitors)
        progress.update(1)

        # 4. Analysis
        progress.set_postfix_str("analysis")
        profitability_filter.run(brand_name, competitors)
        fatigue_scorer.run(brand_name, competitors)
        category_intel.run(brand_name, competitors)
        progress.update(1)

        # 5. Brand product intelligence
        progress.set_postfix_str("brand intel")
        brand_intel.run(brand_name)
        progress.update(1)

        # 6. Competitor deep dive
        progress.set_postfix_str("competitor deep dive")
        competitor_deep_dive.run(brand_name, competitors)
        progress.update(1)

        # 7. Competitor analysis (LLM)
        progress.set_postfix_str("LLM: competitors")
        chains.chain_competitor_analysis(brand_name)
        progress.update(1)

        # 8. Waste diagnosis (LLM)
        progress.set_postfix_str("LLM: waste")
        chains.chain_waste_diagnosis(brand_name)
        progress.update(1)

        # 9. Spend impact estimation
        progress.set_postfix_str("impact estimation")
        impact_estimator.run(brand_name, competitors)
        progress.update(1)

        # 10. New briefs (LLM)
        progress.set_postfix_str("LLM: briefs")
        chains.chain_concept_generation(brand_name, num_concepts=10)
        progress.update(1)

        # 11. Diff report
        progress.set_postfix_str("diffing")
        _log_refresh_diff(brand_name, brand_id, prev_ad_count, prev_comp_ads,
                          competitors)
        progress.update(1)

        progress.set_postfix_str("done")
        progress.close()
        tracker.ok(brand_name)

    except Exception as exc:
        progress.close()
        reason = _short_reason(exc)
        logger.error("Refresh FAILED for '%s': %s", brand_name, reason)
        logger.debug(traceback.format_exc())
        tracker.fail(brand_name, reason)


def _log_refresh_diff(
    brand_name: str,
    brand_id: int,
    prev_ad_count: int,
    prev_comp_ads: dict[str, int],
    competitors: list[str],
) -> None:
    """Compare current state to pre-refresh snapshot and log the diff."""
    conn = config.get_connection()
    try:
        new_ad_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM ads WHERE brand_id = ?",
            (brand_id,),
        ).fetchone()["cnt"]

        logger.info("Refresh diff for '%s':", brand_name)
        delta = new_ad_count - prev_ad_count
        logger.info("  %s ads: %d → %d (%+d)",
                     brand_name, prev_ad_count, new_ad_count, delta)

        for comp_name in competitors:
            comp_row = conn.execute(
                "SELECT id FROM brands WHERE name = ?", (comp_name,),
            ).fetchone()
            if comp_row:
                new_cnt = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM ads WHERE brand_id = ?",
                    (comp_row["id"],),
                ).fetchone()["cnt"]
                prev_cnt = prev_comp_ads.get(comp_name, 0)
                d = new_cnt - prev_cnt
                logger.info("  %s ads: %d → %d (%+d)",
                            comp_name, prev_cnt, new_cnt, d)
    finally:
        conn.close()


# ── Dry run ───────────────────────────────────────────────────────────────────

def _dry_run_preview(
    mode: str,
    brand_name: str,
    competitors: list[str],
    steps: list[str],
) -> None:
    """Show what would happen without executing anything."""
    print(f"\n{'=' * 50}")
    print(f"  DRY RUN — mode: {mode}")
    print(f"  Brand: {brand_name}")
    print(f"  Competitors: {', '.join(competitors) or '(none)'}")
    print(f"  Steps:")
    for i, step in enumerate(steps, 1):
        tag = "LLM" if "LLM" in step else "local"
        print(f"    {i:2d}. [{tag:>5s}] {step}")
    print(f"{'=' * 50}\n")


# ── Helper: safe Instagram scraping ──────────────────────────────────────────

def _scrape_instagram_safe(ig_module, brand_name: str, competitors: list[str]) -> None:
    """Scrape Instagram for brand + competitors. Never crashes the pipeline."""
    conn = config.get_connection()
    try:
        all_names = [brand_name] + competitors
        for name in all_names:
            row = conn.execute(
                "SELECT instagram_handle FROM brands WHERE name = ?",
                (name,),
            ).fetchone()
            handle = row["instagram_handle"] if row else None
            if not handle:
                logger.debug("No Instagram handle for '%s', skipping", name)
                continue
            try:
                ig_module.run(handle, name)
            except Exception as exc:
                logger.warning("Instagram scrape failed for '%s': %s",
                               name, exc)
    finally:
        conn.close()


# ── Helper: safe website scraping ─────────────────────────────────────────────

def _scrape_websites_safe(web_module, brand_name: str, competitors: list[str]) -> None:
    """Scrape brand websites. Never crashes the pipeline."""
    conn = config.get_connection()
    try:
        all_names = [brand_name] + competitors
        for name in all_names:
            row = conn.execute(
                "SELECT website_url FROM brands WHERE name = ?",
                (name,),
            ).fetchone()
            url = row["website_url"] if row else None
            if not url:
                logger.debug("No website URL for '%s', skipping", name)
                continue
            try:
                web_module.run(url, name)
            except Exception as exc:
                logger.warning("Website scrape failed for '%s': %s",
                               name, exc)
    finally:
        conn.close()


# ── DB helpers ────────────────────────────────────────────────────────────────

def _ensure_competitor_set(client_id: int, competitor_id: int) -> None:
    with config.get_connection() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO competitor_sets
               (client_brand_id, competitor_brand_id)
               VALUES (?, ?)""",
            (client_id, competitor_id),
        )


def _fetch_competitor_names(client_brand_id: int) -> list[str]:
    with config.get_connection() as conn:
        rows = conn.execute(
            """SELECT b.name FROM brands b
               JOIN competitor_sets cs ON cs.competitor_brand_id = b.id
               WHERE cs.client_brand_id = ?""",
            (client_brand_id,),
        ).fetchall()
    return [r["name"] for r in rows]


# ── Utilities ─────────────────────────────────────────────────────────────────

def _add_url_args(subparser: argparse.ArgumentParser) -> None:
    """Add --brand-url and --competitor-urls to a subparser."""
    subparser.add_argument(
        "--brand-url", default=None,
        help="Full Meta Ad Library URL for the client brand "
             "(must contain view_all_page_id)",
    )
    subparser.add_argument(
        "--competitor-urls", default=None,
        help='Competitor URLs as "Name:URL,Name:URL" '
             '(split on first colon — URLs contain colons)',
    )


def _parse_competitor_urls(raw: str | None) -> list[dict]:
    """Parse ``"Plum:https://...,Forest Essentials:https://..."`` into list of dicts."""
    if not raw:
        return []
    result: list[dict] = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry or ":" not in entry:
            continue
        name, url = entry.split(":", 1)
        name = name.strip()
        url = url.strip()
        if name and url:
            result.append({"name": name, "url": url})
    return result


def _split(s: str) -> list[str]:
    """Split comma-separated string, stripping whitespace."""
    return [x.strip() for x in s.split(",") if x.strip()]


def _short_reason(exc: Exception) -> str:
    """Extract a short, loggable reason from an exception."""
    name = type(exc).__name__
    msg = str(exc)
    # Truncate long messages
    if len(msg) > 120:
        msg = msg[:117] + "..."
    return f"{name}: {msg}"


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    main()
