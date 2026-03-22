"""
feedback/performance_parser.py — Parse client-uploaded performance CSVs
(Meta Ads Manager exports) and persist into performance_data table.

Month 2+ feature. The pipeline runs fine without this module.
"""

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from config import get_connection

logger = logging.getLogger(__name__)

# Expected column names from Meta Ads Manager export (case-insensitive)
_COL_MAP = {
    "ad name":              "ad_name",
    "ad id":                "ad_id_raw",
    "ctr (all)":            "ctr",
    "cost per result":      "cpa",
    "purchase roas":        "roas",
    "impressions":          "impressions",
    "amount spent (inr)":   "spend",
    "reporting starts":     "date_range_start",
    "reporting ends":       "date_range_end",
}


def run(filepath: str, brand_name: str) -> int:
    """
    Parse *filepath* (Meta Ads Manager CSV) and insert rows into performance_data.
    Returns the number of rows inserted.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Performance CSV not found: {filepath}")

    rows = _parse_csv(path)
    inserted = _persist(rows, brand_name)
    logger.info("Imported %d performance rows for '%s'", inserted, brand_name)
    return inserted


# ── Internal ───────────────────────────────────────────────────────────────────

def _parse_csv(path: Path) -> list[dict]:
    records: list[dict] = []
    with path.open(encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return []

        # Build normalised column mapping
        col_map = {
            col.lower().strip(): _COL_MAP[col.lower().strip()]
            for col in reader.fieldnames
            if col.lower().strip() in _COL_MAP
        }

        for raw_row in reader:
            row = {mapped: raw_row[orig].strip()
                   for orig, mapped in col_map.items()
                   if orig in {k.lower(): k for k in raw_row}}
            if row:
                records.append(row)

    logger.info("Parsed %d rows from %s", len(records), path)
    return records


def _persist(rows: list[dict], brand_name: str) -> int:
    count = 0
    with get_connection() as conn:
        for row in rows:
            # Try to resolve ad_id FK by matching ad_library_id
            ad_id = _resolve_ad_id(conn, row.get("ad_id_raw", ""))

            conn.execute(
                """INSERT INTO performance_data (
                       ad_id, ctr, cpa, roas, impressions, spend,
                       date_range_start, date_range_end, imported_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    ad_id,
                    _float(row.get("ctr")),
                    _float(row.get("cpa")),
                    _float(row.get("roas")),
                    _int(row.get("impressions")),
                    _float(row.get("spend")),
                    row.get("date_range_start"),
                    row.get("date_range_end"),
                    datetime.utcnow().isoformat(),
                ),
            )
            count += 1
    return count


def _resolve_ad_id(conn, raw_id: str) -> Optional[int]:
    if not raw_id:
        return None
    row = conn.execute(
        "SELECT id FROM ads WHERE ad_library_id = ?", (raw_id,)
    ).fetchone()
    return row["id"] if row else None


def _float(val: Optional[str]) -> Optional[float]:
    if not val:
        return None
    try:
        return float(val.replace(",", "").replace("%", "").strip())
    except ValueError:
        return None


def _int(val: Optional[str]) -> Optional[int]:
    f = _float(val)
    return int(f) if f is not None else None
