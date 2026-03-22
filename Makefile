.PHONY: setup test audit sprint batch lint clean

PYTHON = python
PYTEST = pytest
BRAND ?= Mamaearth
COMPETITORS ?= Plum,WOW Skin Science
CATEGORY ?= skincare
NUM_CONCEPTS ?= 50
FILE ?= brands_to_audit.csv
OUTPUT ?= audits

setup:
	pip install -r requirements.txt
	playwright install chromium
	$(PYTHON) config.py

test:
	$(PYTEST) tests/ -v

audit:
	$(PYTHON) pipeline.py audit --brand "$(BRAND)" --competitors "$(COMPETITORS)" --category $(CATEGORY) --output $(OUTPUT)

sprint:
	$(PYTHON) pipeline.py sprint --brand "$(BRAND)" --competitors "$(COMPETITORS)" --category $(CATEGORY) --num-concepts $(NUM_CONCEPTS) --output sprints

batch:
	$(PYTHON) pipeline.py batch-audit --brands-file "$(FILE)" --category $(CATEGORY) --output $(OUTPUT)

refresh:
	$(PYTHON) pipeline.py refresh --brand "$(BRAND)"

lint:
	ruff check .

lint-fix:
	ruff check --fix .

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
