.PHONY: setup test audit sprint batch lint clean

PYTHON = python
PYTEST = pytest
BRAND ?= Mamaearth
COMPETITORS ?= Plum,WOW Skin Science
CATEGORY ?= skincare
NUM_CONCEPTS ?= 50
FILE ?= brands_to_audit.csv
OUTPUT ?= audits
BRAND_PAGE_ID ?=
COMPETITOR_PAGE_IDS ?=

setup:
	pip install -r requirements.txt
	playwright install chromium
	$(PYTHON) config.py

test:
	$(PYTEST) tests/ -v

audit:
	$(PYTHON) pipeline.py audit \
		--brand "$(BRAND)" \
		--competitors "$(COMPETITORS)" \
		--category $(CATEGORY) \
		--output $(OUTPUT) \
		$(if $(BRAND_PAGE_ID),--brand-page-id $(BRAND_PAGE_ID),) \
		$(if $(COMPETITOR_PAGE_IDS),--competitor-page-ids "$(COMPETITOR_PAGE_IDS)",)

sprint:
	$(PYTHON) pipeline.py sprint \
		--brand "$(BRAND)" \
		--competitors "$(COMPETITORS)" \
		--category $(CATEGORY) \
		--num-concepts $(NUM_CONCEPTS) \
		--output sprints \
		$(if $(BRAND_PAGE_ID),--brand-page-id $(BRAND_PAGE_ID),) \
		$(if $(COMPETITOR_PAGE_IDS),--competitor-page-ids "$(COMPETITOR_PAGE_IDS)",)

batch:
	$(PYTHON) pipeline.py batch-audit \
		--brands-file "$(FILE)" \
		--category $(CATEGORY) \
		--output $(OUTPUT) \
		$(if $(BRAND_PAGE_ID),--brand-page-id $(BRAND_PAGE_ID),) \
		$(if $(COMPETITOR_PAGE_IDS),--competitor-page-ids "$(COMPETITOR_PAGE_IDS)",)

refresh:
	$(PYTHON) pipeline.py refresh \
		--brand "$(BRAND)" \
		$(if $(BRAND_PAGE_ID),--brand-page-id $(BRAND_PAGE_ID),) \
		$(if $(COMPETITOR_PAGE_IDS),--competitor-page-ids "$(COMPETITOR_PAGE_IDS)",)

lint:
	ruff check .

lint-fix:
	ruff check --fix .

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
