# D2C Creative Intelligence Service

AI pipeline that scrapes Meta Ad Library, analyses competitor ads via multimodal LLMs, and generates strategic ad creative concepts for Indian D2C brands (₹1–50Cr revenue band).

## Setup

```bash
source cisenv/Scripts/activate          # Git Bash
pip install -r requirements.txt
playwright install chromium
cp .env.example .env                    # then fill in API keys
python config.py                        # initialise DB
```

## Pipeline modes

```bash
python pipeline.py audit  --brand "Mamaearth" --competitors "Plum,WOW Skin Science" --category skincare
python pipeline.py sprint --brand "Mamaearth" --competitors "Plum,WOW Skin Science" --num-concepts 50
python pipeline.py batch-audit --brands-file brands_to_audit.csv --category skincare
python pipeline.py refresh --brand "Mamaearth"
```

## Run individual modules

```bash
python -m scrapers.meta_ad_library --brand "Mamaearth" --competitors "Plum,WOW"
python -m deliverables.audit_generator --brand "Mamaearth" --output audits/
python -m feedback.performance_parser --file export.csv --brand "Mamaearth"
```

## Tests

```bash
pytest tests/ -v
```
