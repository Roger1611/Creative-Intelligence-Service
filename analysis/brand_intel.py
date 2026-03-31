"""
analysis/brand_intel.py — Extract brand-specific product intelligence from
ad copy and website scraper output.

Provides structured product data (names, prices, ingredients, USPs, language
profile) that feeds into concept generation for brand-specific creative briefs.

  run(brand_name) → dict
      • Loads the brand + its ads from DB
      • Extracts product names, price points, ingredients, USP claims,
        CTA patterns, language profile, and brand voice keywords from ad copy
      • Loads website scraper data if available
      • Writes data/processed/{brand_slug}_brand_intel.json
      • Returns the intel dict

CLI: python -m analysis.brand_intel --brand "Just Herbs"
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from collections import Counter
from datetime import datetime
from pathlib import Path

from config import PROC_DIR, get_connection
from scrapers.utils import safe_brand_slug

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Ingredient lexicon — common D2C skincare / wellness ingredient terms
# ══════════════════════════════════════════════════════════════════════════════

INGREDIENT_TERMS: list[str] = [
    "turmeric", "haldi", "neem", "vitamin c", "vitamin e", "vitamin b",
    "hyaluronic acid", "retinol", "retinoid", "kumkumadi", "gotukola",
    "gotu kola", "aloe vera", "tea tree", "charcoal", "salicylic acid",
    "glycolic acid", "lactic acid", "niacinamide", "ceramide", "peptide",
    "collagen", "squalane", "argan oil", "rosehip", "shea butter",
    "coconut oil", "jojoba", "bakuchiol", "centella", "ashwagandha",
    "saffron", "kesar", "mulethi", "licorice", "sandalwood", "chandan",
    "tulsi", "amla", "bhringraj", "brahmi", "zinc", "spf", "sunscreen",
    "kojic acid", "azelaic acid", "benzoyl peroxide", "caffeine",
    "snail mucin", "rice water", "honey", "oat", "aha", "bha",
]

# Pre-compile a pattern for each ingredient (word boundary, case insensitive)
_INGREDIENT_PATTERNS: list[tuple[str, re.Pattern]] = [
    (term, re.compile(r"\b" + re.escape(term) + r"\b", re.IGNORECASE))
    for term in INGREDIENT_TERMS
]

# ══════════════════════════════════════════════════════════════════════════════
# USP claim keywords
# ══════════════════════════════════════════════════════════════════════════════

USP_KEYWORDS: list[str] = [
    "clinically", "dermatologist", "certified", "organic", "natural",
    "ayurvedic", "toxin-free", "paraben-free", "cruelty-free", "vegan",
    "100%", "pure", "sulphate-free", "sulfate-free", "fragrance-free",
    "fda", "gmp", "iso", "made in india", "handmade", "cold-pressed",
    "no chemicals", "chemical-free", "lab-tested", "preservative-free",
    "gluten-free", "non-toxic", "plant-based", "herbal",
]

# ══════════════════════════════════════════════════════════════════════════════
# Price extraction pattern
# ══════════════════════════════════════════════════════════════════════════════

# Matches: ₹599, ₹ 1,299, Rs.599, Rs 1299, Rs. 1,299, INR 499, MRP 999
_PRICE_RE = re.compile(
    r"(?P<currency>₹|Rs\.?|INR|MRP)\s*(?P<amount>\d[\d,]*\d|\d+)(?:\.\d{1,2})?",
    re.IGNORECASE,
)

# ══════════════════════════════════════════════════════════════════════════════
# Devanagari detection (Hindi / code-switching)
# ══════════════════════════════════════════════════════════════════════════════

_DEVANAGARI_RE = re.compile(r"[\u0900-\u097F]")

# ══════════════════════════════════════════════════════════════════════════════
# English stop words (for brand voice keyword extraction)
# ══════════════════════════════════════════════════════════════════════════════

_STOP_WORDS: set[str] = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "shall",
    "should", "may", "might", "must", "can", "could", "to", "of", "in",
    "for", "on", "with", "at", "by", "from", "as", "into", "through",
    "during", "before", "after", "above", "below", "between", "out", "off",
    "over", "under", "again", "further", "then", "once", "and", "but", "or",
    "nor", "not", "so", "no", "if", "that", "this", "it", "its", "i", "me",
    "my", "we", "our", "you", "your", "he", "she", "they", "them", "their",
    "who", "which", "what", "when", "where", "how", "all", "each", "every",
    "both", "few", "more", "most", "other", "some", "such", "only", "own",
    "same", "than", "too", "very", "just", "about", "up", "get", "now",
    "also", "here", "there", "these", "those", "am", "any",
}


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def run(brand_name: str) -> dict:
    """
    Build brand-specific product intelligence from DB ads + website data.

    Returns the intel dict and writes brand_intel.json.
    """
    brand_row = _fetch_brand(brand_name)
    if not brand_row:
        raise ValueError(
            f"Brand '{brand_name}' not found in DB. Run ingest first."
        )

    brand_id = brand_row["id"]
    ads = _fetch_ads(brand_id)
    ad_copies = [a["ad_copy"] for a in ads if a.get("ad_copy")]

    logger.info(
        "Building brand intel for '%s': %d ads, %d with ad_copy",
        brand_name, len(ads), len(ad_copies),
    )

    products = extract_products(ad_copies)
    prices = extract_prices(ad_copies)
    ingredients = extract_ingredients(ad_copies)
    usps = extract_usps(ad_copies)
    cta_patterns = _extract_cta_patterns(ads)
    lang_profile = detect_language_profile(ad_copies)
    voice_keywords = extract_brand_voice_keywords(ad_copies)
    website_data = _load_website_data(brand_name)

    intel = {
        "brand_name": brand_name,
        "generated_at": datetime.utcnow().isoformat(),
        "total_ads": len(ads),
        "ads_with_copy": len(ad_copies),
        "products_detected": products,
        "price_points": prices,
        "key_ingredients": ingredients,
        "usp_claims": usps,
        "cta_patterns": cta_patterns,
        "language_profile": lang_profile,
        "brand_voice_keywords": voice_keywords,
        "website_data": website_data,
    }

    _write_processed(brand_name, intel)
    logger.info(
        "Brand intel built for '%s': %d products, %d prices, %d ingredients",
        brand_name, len(products), len(prices), len(ingredients),
    )
    return intel


# ══════════════════════════════════════════════════════════════════════════════
# Product name extraction
# ══════════════════════════════════════════════════════════════════════════════

# Matches capitalized multi-word phrases (2-5 words, each starting uppercase)
_CAPITALIZED_PHRASE_RE = re.compile(
    r"\b([A-Z][a-zA-Z]*(?:\s+[A-Z][a-zA-Z]*){1,4})\b"
)

# Patterns like "our X", "the X", "new X", "introducing X"
# Captures the capitalized words following the intro word
_INTRO_PATTERN_RE = re.compile(
    r"\b(?:[Oo]ur|[Tt]he|[Nn]ew|[Ii]ntroducing|[Tt]ry|[Mm]eet)\s+([A-Z][a-zA-Z]*(?:\s+[A-Z][a-zA-Z]*){0,3})",
)

# Leading words to strip from detected phrases
_LEADING_STOPWORDS: set[str] = {
    "Our", "The", "New", "Try", "Get", "Buy", "Its", "This", "That",
    "With", "For", "And", "From",
}

# Common false positives to filter out
_PRODUCT_STOPWORDS: set[str] = {
    "Shop Now", "Buy Now", "Learn More", "Sign Up", "Free Shipping",
    "Limited Time", "Best Seller", "New Arrival", "Add To Cart",
    "Free Delivery", "Order Now", "Click Here", "Subscribe Now",
    "Flat Off", "Get Yours", "Check Out", "View More", "Read More",
}


def _strip_leading_stopwords(phrase: str) -> str:
    """Strip common leading words like 'Our', 'The' from a detected phrase."""
    words = phrase.split()
    while words and words[0] in _LEADING_STOPWORDS:
        words.pop(0)
    return " ".join(words)


def extract_products(ad_copies: list[str]) -> list[dict]:
    """Extract likely product names from ad copy via capitalized phrases."""
    phrase_counts: Counter = Counter()
    phrase_contexts: dict[str, list[str]] = {}

    for copy in ad_copies:
        seen_in_ad: set[str] = set()

        # Capitalized multi-word phrases
        for match in _CAPITALIZED_PHRASE_RE.finditer(copy):
            phrase = _strip_leading_stopwords(match.group(1).strip())
            if phrase in _PRODUCT_STOPWORDS or len(phrase) < 4:
                continue
            if phrase not in seen_in_ad:
                phrase_counts[phrase] += 1
                seen_in_ad.add(phrase)
                phrase_contexts.setdefault(phrase, [])
                # Store a snippet around the match for context
                start = max(0, match.start() - 20)
                end = min(len(copy), match.end() + 40)
                snippet = copy[start:end].replace("\n", " ").strip()
                if len(phrase_contexts[phrase]) < 3:
                    phrase_contexts[phrase].append(snippet)

        # Intro patterns
        for match in _INTRO_PATTERN_RE.finditer(copy):
            phrase = match.group(1).strip()
            if phrase in _PRODUCT_STOPWORDS or len(phrase) < 4:
                continue
            if phrase not in seen_in_ad:
                phrase_counts[phrase] += 1
                seen_in_ad.add(phrase)
                phrase_contexts.setdefault(phrase, [])
                start = max(0, match.start() - 10)
                end = min(len(copy), match.end() + 40)
                snippet = copy[start:end].replace("\n", " ").strip()
                if len(phrase_contexts[phrase]) < 3:
                    phrase_contexts[phrase].append(snippet)

    # Only keep phrases appearing in 2+ different ads
    results = [
        {
            "name": phrase,
            "frequency": count,
            "context_snippets": phrase_contexts.get(phrase, []),
        }
        for phrase, count in phrase_counts.most_common()
        if count >= 2
    ]

    return results


# ══════════════════════════════════════════════════════════════════════════════
# Price extraction
# ══════════════════════════════════════════════════════════════════════════════

def extract_prices(ad_copies: list[str]) -> list[dict]:
    """Extract price mentions (₹, Rs, INR, MRP) from ad copy."""
    seen: set[str] = set()
    results: list[dict] = []

    for copy in ad_copies:
        for match in _PRICE_RE.finditer(copy):
            currency = match.group("currency")
            amount = match.group("amount")
            key = f"{currency}{amount}"
            if key in seen:
                continue
            seen.add(key)
            start = max(0, match.start() - 30)
            end = min(len(copy), match.end() + 30)
            context = copy[start:end].replace("\n", " ").strip()
            results.append({
                "amount": amount,
                "currency": currency,
                "context": context,
            })

    return results


# ══════════════════════════════════════════════════════════════════════════════
# Ingredient detection
# ══════════════════════════════════════════════════════════════════════════════

def extract_ingredients(ad_copies: list[str]) -> list[dict]:
    """Count ingredient keyword mentions across all ad copy."""
    counts: Counter = Counter()
    combined = "\n".join(ad_copies)

    for term, pattern in _INGREDIENT_PATTERNS:
        n = len(pattern.findall(combined))
        if n > 0:
            counts[term] = n

    return [
        {"name": name, "frequency": freq}
        for name, freq in counts.most_common()
    ]


# ══════════════════════════════════════════════════════════════════════════════
# USP claim extraction
# ══════════════════════════════════════════════════════════════════════════════

def extract_usps(ad_copies: list[str]) -> list[str]:
    """Extract unique USP claims from ad copy."""
    found: list[str] = []
    combined_lower = "\n".join(ad_copies).lower()

    for keyword in USP_KEYWORDS:
        if keyword.lower() in combined_lower:
            found.append(keyword)

    return found


# ══════════════════════════════════════════════════════════════════════════════
# CTA pattern extraction
# ══════════════════════════════════════════════════════════════════════════════

def _extract_cta_patterns(ads: list[dict]) -> dict[str, int]:
    """Count CTA types from the ads table."""
    cta_counts: Counter = Counter()
    for ad in ads:
        cta = ad.get("cta_type")
        if cta:
            cta_counts[cta] += 1
    return dict(cta_counts.most_common())


# ══════════════════════════════════════════════════════════════════════════════
# Language profile detection
# ══════════════════════════════════════════════════════════════════════════════

def detect_language_profile(ad_copies: list[str]) -> dict:
    """Detect whether ads use Hindi, English, or code-switching."""
    hindi_count = 0
    english_count = 0

    for copy in ad_copies:
        has_devanagari = bool(_DEVANAGARI_RE.search(copy))
        # Check for ASCII letter content (English)
        has_latin = bool(re.search(r"[a-zA-Z]{3,}", copy))

        if has_devanagari and has_latin:
            hindi_count += 1
            english_count += 1
        elif has_devanagari:
            hindi_count += 1
        else:
            english_count += 1

    has_hindi = hindi_count > 0
    has_code_switch = hindi_count > 0 and english_count > 0

    if english_count >= hindi_count:
        primary = "english"
    else:
        primary = "hindi"

    return {
        "primary": primary,
        "has_hindi": has_hindi,
        "has_code_switch": has_code_switch,
        "hindi_ad_count": hindi_count,
        "english_ad_count": english_count,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Brand voice keywords
# ══════════════════════════════════════════════════════════════════════════════

def extract_brand_voice_keywords(
    ad_copies: list[str],
    top_n: int = 20,
) -> list[str]:
    """Extract the most frequent non-stopword terms across all ad copy."""
    word_counts: Counter = Counter()

    for copy in ad_copies:
        words = re.findall(r"[a-zA-Z]{3,}", copy.lower())
        for w in words:
            if w not in _STOP_WORDS:
                word_counts[w] += 1

    return [word for word, _ in word_counts.most_common(top_n)]


# ══════════════════════════════════════════════════════════════════════════════
# Website data loader
# ══════════════════════════════════════════════════════════════════════════════

def _load_website_data(brand_name: str) -> dict | None:
    """Load website scraper output from data/processed if available."""
    slug = safe_brand_slug(brand_name)
    path = PROC_DIR / f"{slug}_website.json"
    if not path.exists():
        logger.debug("No website data found at %s", path)
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        logger.info("Loaded website data from %s", path)
        return data
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to load website data from %s: %s", path, exc)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# DB helpers
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_brand(name: str) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id, name, category, website_url, is_client FROM brands WHERE name = ?",
            (name,),
        ).fetchone()
    return dict(row) if row else None


def _fetch_ads(brand_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM ads WHERE brand_id = ?", (brand_id,)
        ).fetchall()
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# Output
# ══════════════════════════════════════════════════════════════════════════════

def _write_processed(brand_name: str, data: dict) -> Path:
    PROC_DIR.mkdir(parents=True, exist_ok=True)
    slug = safe_brand_slug(brand_name)
    path = PROC_DIR / f"{slug}_brand_intel.json"
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info("Brand intel -> %s", path)
    return path


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def _cli() -> None:
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s -- %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        prog="python -m analysis.brand_intel",
        description="Extract brand-specific product intelligence from DB ads + website data.",
    )
    parser.add_argument("--brand", required=True, help="Brand name (must exist in DB)")
    args = parser.parse_args()

    result = run(args.brand)

    print(f"\n  Brand: {result['brand_name']}")
    print(f"  Ads analysed: {result['total_ads']} ({result['ads_with_copy']} with copy)")
    print(f"  Products detected: {len(result['products_detected'])}")
    for p in result["products_detected"][:5]:
        print(f"    - {p['name']} (seen in {p['frequency']} ads)")
    print(f"  Price points: {len(result['price_points'])}")
    for p in result["price_points"][:5]:
        print(f"    - {p['currency']}{p['amount']}")
    print(f"  Key ingredients: {len(result['key_ingredients'])}")
    for i in result["key_ingredients"][:5]:
        print(f"    - {i['name']} ({i['frequency']}x)")
    print(f"  USP claims: {result['usp_claims']}")
    print(f"  Language: {result['language_profile']}")
    print(f"  Voice keywords: {result['brand_voice_keywords'][:10]}")
    print(f"  Website data: {'loaded' if result['website_data'] else 'not available'}")

    slug = safe_brand_slug(args.brand)
    print(f"\nOutput: {PROC_DIR / slug}_brand_intel.json")


if __name__ == "__main__":
    _cli()
