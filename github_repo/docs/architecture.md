# Architecture Decisions

## ELT Pattern Choice

Extract → Load → Transform was chosen over ETL because:
- Raw data is preserved in staging for re-processing if transform logic changes
- Each layer can be tested independently
- Aligns with modern data engineering practices (Ponniah, 2010)

## Why Python over Excel/VBA

| Factor | Excel/VBA (Current) | Python (Automated) |
|--------|--------------------|--------------------|
| Version control | Not possible | Git — full history |
| Unit testing | Not possible | pytest — 20 tests |
| Audit trail | Manual | Automatic logging |
| Reproducibility | Analyst-dependent | Deterministic |
| Scalability | Limited by file size | Unlimited |

## Module Design

Each function in `ptl_pipeline.py` has a single responsibility:
- `load_data()` — ingestion only
- `validate_schema()` — contract enforcement only
- `filter_ordered_exams()` — status filtering only
- etc.

This ensures each function can be independently tested and replaced
without affecting other pipeline stages.
