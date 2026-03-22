"""Tests for llm/chains.py — prompt formatting (no actual API calls)."""

import json
import pytest
from pathlib import Path
from string import Template


PROMPTS_DIR = Path(__file__).parent.parent / "llm" / "prompts"


class TestPromptFilesExist:
    """Verify all expected prompt template files exist."""

    @pytest.mark.parametrize("filename", [
        "competitor_deconstruction.txt",
        "waste_diagnosis.txt",
        "concept_generation.txt",
    ])
    def test_prompt_file_exists(self, filename):
        path = PROMPTS_DIR / filename
        assert path.exists(), f"Missing prompt file: {path}"

    @pytest.mark.parametrize("filename", [
        "competitor_deconstruction.txt",
        "waste_diagnosis.txt",
        "concept_generation.txt",
    ])
    def test_prompt_file_not_empty(self, filename):
        path = PROMPTS_DIR / filename
        content = path.read_text(encoding="utf-8")
        assert len(content.strip()) > 100, f"Prompt file is suspiciously short: {path}"


class TestCompetitorDeconstructionPrompt:
    """Test the competitor_deconstruction prompt template."""

    @pytest.fixture
    def template(self):
        path = PROMPTS_DIR / "competitor_deconstruction.txt"
        return Template(path.read_text(encoding="utf-8"))

    def test_substitution(self, template):
        """Template variables should substitute without error."""
        result = template.safe_substitute(
            brand_name="TestBrand",
            category="skincare",
            ad_library_id="AD_12345",
        )
        assert "TestBrand" in result
        assert "skincare" in result
        assert "AD_12345" in result

    def test_required_output_fields(self, template):
        """Prompt should mention expected JSON output fields."""
        text = template.template
        expected_fields = [
            "psychological_trigger",
            "visual_layout",
            "copy_tone",
            "effectiveness_score",
        ]
        for field in expected_fields:
            assert field in text, f"Missing field '{field}' in prompt"

    def test_json_guard(self, template):
        """Prompt should instruct JSON-only output."""
        text = template.template.lower()
        assert "json" in text


class TestWasteDiagnosisPrompt:
    """Test the waste_diagnosis prompt template."""

    @pytest.fixture
    def template(self):
        path = PROMPTS_DIR / "waste_diagnosis.txt"
        return Template(path.read_text(encoding="utf-8"))

    def test_substitution(self, template):
        """All template variables should substitute cleanly."""
        result = template.safe_substitute(
            brand_name="TestBrand",
            category="skincare",
            client_data=json.dumps({"ads": []}),
            competitor_benchmarks=json.dumps([]),
        )
        assert "TestBrand" in result
        assert "skincare" in result

    def test_required_output_fields(self, template):
        """Prompt should mention expected output fields."""
        text = template.template
        expected = ["format_gaps", "priority_actions"]
        for field in expected:
            assert field in text, f"Missing field '{field}' in waste diagnosis prompt"


class TestConceptGenerationPrompt:
    """Test the concept_generation prompt template."""

    @pytest.fixture
    def template(self):
        path = PROMPTS_DIR / "concept_generation.txt"
        return Template(path.read_text(encoding="utf-8"))

    def test_substitution(self, template):
        result = template.safe_substitute(
            brand_name="TestBrand",
            category="skincare",
            num_concepts="50",
            brand_context=json.dumps({"name": "TestBrand"}),
            competitor_intel=json.dumps([]),
            waste_diagnosis=json.dumps({}),
        )
        assert "TestBrand" in result
        assert "50" in result

    def test_required_concept_fields(self, template):
        """Prompt should specify expected concept output fields."""
        text = template.template
        expected = ["hook", "body_script", "visual_direction", "psychological_angle"]
        for field in expected:
            assert field in text, f"Missing field '{field}' in concept generation prompt"

    def test_diversity_enforcement(self, template):
        """Prompt should mention diversity constraints."""
        text = template.template.lower()
        # Should reference trigger/angle diversity
        assert "trigger" in text or "angle" in text or "diversity" in text

    def test_num_concepts_variable(self, template):
        """Should use $num_concepts variable."""
        assert "$num_concepts" in template.template


class TestLoadPromptHelper:
    """Test prompt loading logic (reimplemented to avoid importing llm.client)."""

    def _load_prompt(self, filename):
        """Reimplement _load_prompt to avoid importing llm.chains (needs anthropic)."""
        path = PROMPTS_DIR / filename
        if not path.exists():
            raise FileNotFoundError(f"Prompt file not found: {path}")
        return Template(path.read_text(encoding="utf-8"))

    def test_load_valid_prompt(self):
        tpl = self._load_prompt("competitor_deconstruction.txt")
        assert isinstance(tpl, Template)
        assert len(tpl.template) > 0

    def test_load_missing_prompt(self):
        with pytest.raises(FileNotFoundError):
            self._load_prompt("nonexistent_prompt.txt")
