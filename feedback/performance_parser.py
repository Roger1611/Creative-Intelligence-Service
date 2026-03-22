"""
feedback/performance_parser.py - Parse Meta Ads Manager CSV exports and persist
into the performance_data table.

Handles the standard Meta export format, resolves ad_library_id FKs, and
fuzzy-matches ad copy against creative_concepts to link performance data back
to generated concepts (enabling the feedback loop in loop.py).

CLI: python -m feedback.performance_parser --file ads_export.csv --brand "Mamaearth"
"""

import argparse
import csv
import logging
import re
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

from config import get_connection

logger = logging.getLogger(__name__)

# Meta Ads Manager CSV column names -> internal keys (case-insensitive match)
_COL_MAP = {
    "ad name":              "ad_name",
    "ad id":                "ad_id_raw",
    "campaign name":        "campaign_name",
    "ad set name":          "ad_set_name",
    "delivery":             "delivery",
    "ctr (all)":            "ctr",
    "ctr (link click-through rate)": "ctr",
    "cost per result":      "cpa",
    "cost per action type":  "cpa",
    "purchase roas":        "roas",
    "website purchase roas": "roas",
    "impressions":          "impressions",
    "reach":                "reach",
    "clicks (all)":         "clicks",
    "link clicks":          "clicks",
    "amount spent (inr)":   "spend",
    "amount spent":         "spend",
    "reporting starts":     "date_range_start",
    "reporting start":      "date_range_start",
    "reporting ends":       "date_range_end",
    "reporting end":        "date_range_end",
    "results":              "results",
    "result type":          "result_type",
    # Body / primary text - used for concept matching
    "body":                 "body_text",
    "primary text":         "body_text",
    "title":                "title_text",
}

# Minimum similarity score (0-1) for fuzzy concept matching
_MATCH_THRESHOLD = 0.55


# -- Public API ----------------------------------------------------------------

def run(filepath: str, brand_name: str) -> dict:
    """
    Parse *filepath* (Meta Ads Manager CSV) and insert rows into performance_data.

    Returns summary dict with counts and any matching info.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Performance CSV not found: {filepath}")

    conn = get_connection()
    try:
        brand = conn.execute(
            "SELECT id FROM brands WHERE name = ? AND is_client = 1",
            (brand_name,),
        ).fetchone()
        if not brand:
            raise ValueError(
                f"Client brand '{brand_name}' not found. Run the pipeline first.")
        brand_id = brand["id"]

        # Parse
        records = _parse_csv(path)
        if not records:
            logger.warning("No rows parsed from %s", path)
            return {"parsed": 0, "inserted": 0, "matched_ads": 0,
                    "matched_concepts": 0}

        # Load existing ads + concepts for matching
        ads_lookup = _build_ads_lookup(conn, brand_id)
        concepts_lookup = _build_concepts_lookup(conn, brand_id)

        # Insert
        inserted = 0
        matched_ads = 0
        matched_concepts = 0

        for rec in records:
            ad_id = _resolve_ad_id(conn, rec, ads_lookup)
            concept_id = _resolve_concept_id(rec, concepts_lookup)

            if ad_id:
                matched_ads += 1
            if concept_id:
                matched_concepts += 1

            conn.execute(
                """INSERT INTO performance_data (
                       creative_concept_id, ad_id,
                       ctr, cpa, roas, impressions, spend,
                       date_range_start, date_range_end
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    concept_id,
                    ad_id,
                    _parse_float(rec.get("ctr")),
                    _parse_float(rec.get("cpa")),
                    _parse_float(rec.get("roas")),
                    _parse_int(rec.get("impressions")),
                    _parse_float(rec.get("spend")),
                    _parse_date(rec.get("date_range_start")),
                    _parse_date(rec.get("date_range_end")),
                ),
            )
            inserted += 1

        conn.commit()
    finally:
        conn.close()

    summary = {
        "parsed": len(records),
        "inserted": inserted,
        "matched_ads": matched_ads,
        "matched_concepts": matched_concepts,
    }
    logger.info(
        "Imported %d rows for '%s' (%d matched to ads, %d matched to concepts)",
        inserted, brand_name, matched_ads, matched_concepts,
    )
    return summary


# -- CSV parsing ---------------------------------------------------------------

def _parse_csv(path: Path) -> list[dict]:
    """Parse Meta Ads Manager CSV into normalised dicts."""
    records: list[dict] = []

    with path.open(encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return []

        # Build case-insensitive column mapping
        # reader.fieldnames are the actual CSV headers
        col_map: dict[str, str] = {}
        for csv_col in reader.fieldnames:
            normalised = csv_col.lower().strip()
            if normalised in _COL_MAP:
                col_map[csv_col] = _COL_MAP[normalised]

        if not col_map:
            logger.warning("No recognised columns in CSV: %s",
                           reader.fieldnames)
            return []

        logger.info("Mapped %d/%d CSV columns", len(col_map),
                     len(reader.fieldnames))

        for raw_row in reader:
            rec = {}
            for csv_col, internal_key in col_map.items():
                val = raw_row.get(csv_col, "").strip()
                if val:
                    # Don't overwrite if already set (first match wins)
                    rec.setdefault(internal_key, val)
            if rec:
                records.append(rec)

    logger.info("Parsed %d rows from %s", len(records), path.name)
    return records


# -- Ad ID resolution ----------------------------------------------------------

def _build_ads_lookup(conn, brand_id: int) -> dict:
    """Build lookup dicts for fast ad matching."""
    rows = conn.execute(
        "SELECT id, ad_library_id, ad_copy FROM ads WHERE brand_id = ?",
        (brand_id,),
    ).fetchall()
    return {
        "by_library_id": {r["ad_library_id"]: r["id"] for r in rows},
        "by_copy": {_normalise_text(r["ad_copy"]): r["id"]
                    for r in rows if r["ad_copy"]},
    }


def _resolve_ad_id(conn, rec: dict, lookup: dict) -> int | None:
    """Try to resolve a performance row to an ads.id."""
    # Strategy 1: exact ad_library_id match
    raw_id = rec.get("ad_id_raw", "")
    if raw_id and raw_id in lookup["by_library_id"]:
        return lookup["by_library_id"][raw_id]

    # Strategy 2: ad_library_id as substring of ad_name
    ad_name = rec.get("ad_name", "")
    if ad_name:
        for lib_id, ad_id in lookup["by_library_id"].items():
            if lib_id in ad_name:
                return ad_id

    # Strategy 3: fuzzy match body text against ad_copy
    body = rec.get("body_text") or rec.get("title_text") or ""
    if body:
        norm_body = _normalise_text(body)
        for norm_copy, ad_id in lookup["by_copy"].items():
            if _similarity(norm_body, norm_copy) >= _MATCH_THRESHOLD:
                return ad_id

    return None


# -- Concept matching ----------------------------------------------------------

def _build_concepts_lookup(conn, brand_id: int) -> list[dict]:
    """Load concepts with their hook/body for fuzzy matching."""
    rows = conn.execute(
        """SELECT id, hook_text, body_script FROM creative_concepts
           WHERE client_brand_id = ?""",
        (brand_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _resolve_concept_id(rec: dict, concepts: list[dict]) -> int | None:
    """Fuzzy-match ad copy against generated concept hooks/bodies."""
    ad_text = rec.get("body_text") or rec.get("title_text") or rec.get("ad_name") or ""
    if not ad_text:
        return None

    norm_ad = _normalise_text(ad_text)
    best_id = None
    best_score = 0.0

    for concept in concepts:
        # Check hook match
        hook = concept.get("hook_text") or ""
        if hook:
            score = _similarity(norm_ad, _normalise_text(hook))
            # Hook appearing inside ad text is a strong signal
            if _normalise_text(hook) in norm_ad:
                score = max(score, 0.85)
            if score > best_score:
                best_score = score
                best_id = concept["id"]

        # Check body match
        body = concept.get("body_script") or ""
        if body:
            score = _similarity(norm_ad, _normalise_text(body))
            if score > best_score:
                best_score = score
                best_id = concept["id"]

    if best_score >= _MATCH_THRESHOLD:
        return best_id
    return None


# -- Text helpers --------------------------------------------------------------

def _normalise_text(text: str) -> str:
    """Lowercase, collapse whitespace, strip punctuation for matching."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _similarity(a: str, b: str) -> float:
    """Quick ratio similarity between two strings."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).quick_ratio()


def _parse_float(val: str | None) -> float | None:
    if not val:
        return None
    try:
        cleaned = val.replace(",", "").replace("%", "").replace("INR", "").strip()
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def _parse_int(val: str | None) -> int | None:
    f = _parse_float(val)
    return int(f) if f is not None else None


def _parse_date(val: str | None) -> str | None:
    """Try to normalise date strings to ISO format."""
    if not val:
        return None
    val = val.strip()
    # Already ISO
    if re.match(r"\d{4}-\d{2}-\d{2}", val):
        return val[:10]
    # Common Meta formats: "Mar 15, 2025" or "15/03/2025"
    for fmt in ("%b %d, %Y", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return val  # return as-is if no format matches


# -- CLI -----------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Parse a Meta Ads Manager CSV and import performance data.")
    parser.add_argument("--file", required=True,
                        help="Path to the Meta Ads Manager CSV export")
    parser.add_argument("--brand", required=True,
                        help="Brand name (must exist in DB as is_client=1)")
    args = parser.parse_args()

    result = run(args.file, args.brand)
    print(f"Import complete: {result['inserted']} rows inserted, "
          f"{result['matched_ads']} matched to ads, "
          f"{result['matched_concepts']} matched to concepts")
