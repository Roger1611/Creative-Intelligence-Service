"""Tests for analysis/brand_intel.py — product intelligence extraction."""

import pytest
from analysis.brand_intel import (
    extract_products,
    extract_prices,
    extract_ingredients,
    extract_usps,
    detect_language_profile,
    extract_brand_voice_keywords,
)


class TestExtractProducts:
    """Test product name extraction from ad copy."""

    def test_capitalized_phrases_appearing_twice(self):
        """Capitalized multi-word phrases in 2+ ads should be detected."""
        copies = [
            "Try the new Kumkumadi Night Serum for glowing skin",
            "Our Kumkumadi Night Serum is a bestseller",
            "Get 20% off on all products",
        ]
        result = extract_products(copies)
        names = [p["name"] for p in result]
        assert "Kumkumadi Night Serum" in names

    def test_single_occurrence_excluded(self):
        """Phrases appearing in only 1 ad should not be returned."""
        copies = [
            "Introducing Rose Gold Face Mist",
            "Shop our collection of skincare essentials",
        ]
        result = extract_products(copies)
        names = [p["name"] for p in result]
        assert "Rose Gold Face Mist" not in names

    def test_intro_patterns(self):
        """'our/new/introducing' patterns should detect product names."""
        copies = [
            "Introducing Vitamin C Serum for brighter skin",
            "Try our new Vitamin C Serum today",
        ]
        result = extract_products(copies)
        names = [p["name"] for p in result]
        # Should find via intro pattern + capitalized phrase
        assert any("Vitamin C Serum" in n for n in names) or \
               any("Vitamin" in n for n in names)

    def test_context_snippets_present(self):
        """Each detected product should have context snippets."""
        copies = [
            "Buy Turmeric Face Wash now at 50% off",
            "Our Turmeric Face Wash is paraben-free",
        ]
        result = extract_products(copies)
        for p in result:
            if "Turmeric Face Wash" in p["name"]:
                assert len(p["context_snippets"]) > 0

    def test_empty_copies(self):
        """Empty list should return empty results."""
        assert extract_products([]) == []

    def test_stopword_phrases_excluded(self):
        """Common CTA phrases should not be detected as products."""
        copies = [
            "Shop Now and get flat 30% off",
            "Shop Now before the sale ends",
        ]
        result = extract_products(copies)
        names = [p["name"] for p in result]
        assert "Shop Now" not in names


class TestExtractPrices:
    """Test price point extraction."""

    def test_rupee_symbol(self):
        """₹ followed by number should be detected."""
        copies = ["Get it for just ₹599 today"]
        result = extract_prices(copies)
        assert len(result) == 1
        assert result[0]["amount"] == "599"
        assert result[0]["currency"] == "₹"

    def test_rs_format(self):
        """Rs. with number should be detected."""
        copies = ["Price: Rs. 1,299 only"]
        result = extract_prices(copies)
        assert len(result) == 1
        assert result[0]["amount"] == "1,299"

    def test_inr_format(self):
        """INR with number should be detected."""
        copies = ["Starting at INR 499"]
        result = extract_prices(copies)
        assert len(result) == 1
        assert result[0]["amount"] == "499"
        assert result[0]["currency"].upper() == "INR"

    def test_multiple_prices(self):
        """Multiple prices in one copy should all be extracted."""
        copies = ["Was ₹999, now ₹599! Save ₹400"]
        result = extract_prices(copies)
        amounts = {p["amount"] for p in result}
        assert "999" in amounts
        assert "599" in amounts
        assert "400" in amounts

    def test_context_included(self):
        """Each price should have a surrounding context snippet."""
        copies = ["Special launch price ₹299 for limited time"]
        result = extract_prices(copies)
        assert result[0]["context"]
        assert "299" in result[0]["context"]

    def test_empty_copies(self):
        """Empty list should return empty results."""
        assert extract_prices([]) == []

    def test_no_prices(self):
        """Copy without prices should return empty."""
        copies = ["Buy our amazing skincare products today"]
        assert extract_prices(copies) == []

    def test_mrp_format(self):
        """MRP with number should be detected."""
        copies = ["MRP 1,499"]
        result = extract_prices(copies)
        assert len(result) == 1
        assert result[0]["amount"] == "1,499"


class TestExtractIngredients:
    """Test ingredient keyword detection."""

    def test_common_ingredients(self):
        """Should detect standard D2C skincare ingredients."""
        copies = [
            "Powered by Vitamin C and Hyaluronic Acid for deep hydration",
            "Contains neem and turmeric extracts",
        ]
        result = extract_ingredients(copies)
        names = [i["name"] for i in result]
        assert "vitamin c" in names
        assert "hyaluronic acid" in names
        assert "neem" in names
        assert "turmeric" in names

    def test_frequency_counting(self):
        """Ingredients mentioned multiple times should have higher frequency."""
        copies = [
            "Vitamin C serum with Vitamin C power",
            "More Vitamin C for your skin",
        ]
        result = extract_ingredients(copies)
        vc = next(i for i in result if i["name"] == "vitamin c")
        assert vc["frequency"] == 3

    def test_no_ingredients(self):
        """Copy without ingredient terms should return empty."""
        copies = ["Buy now and save 50% off"]
        assert extract_ingredients(copies) == []

    def test_empty_copies(self):
        """Empty list should return empty results."""
        assert extract_ingredients([]) == []

    def test_ayurvedic_ingredients(self):
        """Should detect Indian/Ayurvedic ingredient terms."""
        copies = ["Made with ashwagandha, tulsi, and amla for holistic wellness"]
        result = extract_ingredients(copies)
        names = [i["name"] for i in result]
        assert "ashwagandha" in names
        assert "tulsi" in names
        assert "amla" in names


class TestExtractUsps:
    """Test USP claim extraction."""

    def test_common_claims(self):
        """Should detect standard D2C USP keywords."""
        copies = ["100% organic, paraben-free, cruelty-free skincare"]
        result = extract_usps(copies)
        assert "organic" in result
        assert "paraben-free" in result
        assert "cruelty-free" in result
        assert "100%" in result

    def test_no_claims(self):
        """Copy without USP keywords returns empty."""
        copies = ["Buy this product now"]
        assert extract_usps(copies) == []


class TestDetectLanguageProfile:
    """Test language profile detection."""

    def test_english_only(self):
        """Pure English copy should be detected."""
        copies = ["Get glowing skin with our new serum"]
        result = detect_language_profile(copies)
        assert result["primary"] == "english"
        assert result["has_hindi"] is False
        assert result["has_code_switch"] is False

    def test_hindi_detection(self):
        """Copy with Devanagari characters should flag Hindi."""
        copies = ["अब पाएं चमकती त्वचा"]
        result = detect_language_profile(copies)
        assert result["has_hindi"] is True

    def test_code_switching(self):
        """Copy mixing English and Hindi should flag code-switching."""
        copies = ["Get your निखरी त्वचा with our serum"]
        result = detect_language_profile(copies)
        assert result["has_hindi"] is True
        assert result["has_code_switch"] is True

    def test_empty_copies(self):
        """Empty list should return default English profile."""
        result = detect_language_profile([])
        assert result["primary"] == "english"
        assert result["has_hindi"] is False


class TestExtractBrandVoiceKeywords:
    """Test brand voice keyword extraction."""

    def test_frequent_words_returned(self):
        """Most frequent non-stopword terms should be returned."""
        copies = [
            "glowing skin naturally",
            "natural glow for your skin",
            "skin care with natural ingredients",
        ]
        result = extract_brand_voice_keywords(copies)
        assert "skin" in result
        assert "natural" in result or "naturally" in result

    def test_stopwords_excluded(self):
        """Common English stop words should not appear."""
        copies = ["the best product for your skin and your body"]
        result = extract_brand_voice_keywords(copies)
        assert "the" not in result
        assert "for" not in result
        assert "and" not in result

    def test_empty_copies(self):
        """Empty list returns empty."""
        assert extract_brand_voice_keywords([]) == []
