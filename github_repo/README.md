# NHS Radiology PTL Automated Pipeline

**ADF_3002-5 | Accelerating Data Solutions with DevOps Principles**  
NHS Trust — Radiology Department | Patient Tracking List (PTL) Workflow Redesign

---

## The Problem This Solves

The Radiology Department's monthly Patient Tracking List (PTL) reporting previously required **4–5 hours of manual work** each month:

| Manual Step | Time | Problem |
|------------|------|---------|
| 8 separate CSV exports from RadNet | 45 min | System crash if run as single export |
| Manual duplicate removal in Excel | 30 min | Error-prone; occasionally missed rows |
| MRN format correction (leading zeros) | 15 min | Excel stripped leading zeros from patient IDs |
| Appointment status filtering | 20 min | Forgot steps caused inflated breach counts |
| DATEDIF breach calculation | 30 min | Stale formulas caused incorrect NHS England submissions |
| DM01 exclusion cross-referencing | 45 min | Inconsistent decisions between analysts |
| Manual email distribution with PII | 30 min | Governance risk — full patient data sent over email |
| **Total** | **~4.5 hrs** | |

**This pipeline replaces all of the above in under 20 minutes, with a full audit trail.**

---

## How It Works

```
RadNet CSV Export
      │
      ▼
load_data()          ← Forces MRN to string, preserves leading zeros
      │
      ▼
validate_schema()    ← Catches schema drift before silent failures
      │
      ▼
filter_ordered_exams() ← Removes Completed/Cancelled/etc automatically
      │
      ▼
deduplicate()        ← Removes duplicates from multi-batch exports, keeps latest
      │
      ▼
calculate_waiting_times() ← Dynamic today-based breach flags (28d, 42d)
      │
      ▼
apply_exclusions()   ← DM01 exclusions from version-controlled CSV
      │
      ▼
classify_dm01()      ← Statutory NHS England return logic
      │
      ▼
generate_outputs()   ← ptl_full.csv (PII), dm01_return.csv (no PII), summary.csv
```

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/your-org/ptl-pipeline.git
cd ptl-pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the pipeline
python src/ptl_pipeline.py \
  --input data/sample/sample_radnet_export.csv \
  --output data/outputs/ \
  --exclusions data/sample/sample_exclusions.csv

# 4. Run tests
pytest tests/ -v --cov=src --cov-report=term-missing
```

---

## Repository Structure

```
ptl-pipeline/
├── src/
│   └── ptl_pipeline.py          # Main pipeline — all core logic
├── tests/
│   └── test_ptl_pipeline.py     # 20 unit tests (TDD approach)
├── data/
│   ├── sample/
│   │   ├── sample_radnet_export.csv   # Sample input (anonymised)
│   │   └── sample_exclusions.csv      # Sample DM01 exclusions
│   └── outputs/                       # Pipeline writes here (git-ignored)
├── docs/
│   └── architecture.md                # Architecture decisions
├── logs/                              # Pipeline run logs (git-ignored)
├── .github/
│   └── workflows/
│       └── ci_cd.yml                  # GitHub Actions CI/CD pipeline
├── requirements.txt
└── README.md
```

---

## Test Coverage

Tests were written **before** implementation (Test-Driven Development).  
Each test maps to a specific failure in the manual process:

```bash
pytest tests/ -v
```

```
tests/test_ptl_pipeline.py::TestSchemaValidation::test_valid_schema_passes          PASSED
tests/test_ptl_pipeline.py::TestSchemaValidation::test_missing_column_raises         PASSED
tests/test_ptl_pipeline.py::TestMRNFormatting::test_mrn_leading_zeros_preserved      PASSED
tests/test_ptl_pipeline.py::TestStatusFilter::test_only_ordered_exams_remain         PASSED
tests/test_ptl_pipeline.py::TestDeduplication::test_duplicate_accession_removed      PASSED
tests/test_ptl_pipeline.py::TestDeduplication::test_most_recent_record_kept          PASSED
tests/test_ptl_pipeline.py::TestBreachCalculation::test_6w_breach_correctly_flagged  PASSED
tests/test_ptl_pipeline.py::TestBreachCalculation::test_exact_28_day_boundary        PASSED
tests/test_ptl_pipeline.py::TestExclusionLogic::test_valid_exclusion_applied         PASSED
tests/test_ptl_pipeline.py::TestExclusionLogic::test_invalid_exclusion_reason_ignored PASSED
tests/test_ptl_pipeline.py::TestDM01Classification::test_6w_breach_not_excluded...   PASSED
tests/test_ptl_pipeline.py::TestOutputGeneration::test_dm01_output_excludes_pii      PASSED
... (20 tests total)
```

---

## CI/CD Pipeline (GitHub Actions)

Every `git push` to `main` triggers:

1. **Unit tests** — all 20 tests must pass
2. **Coverage check** — must remain ≥85%
3. **Schema validation** — sample data validated against contract
4. **Staging smoke test** — pipeline runs end-to-end on sample data
5. **Production deploy** — requires manual approval (NHS governance gate)

---

## Outputs

| File | Contains PII | Purpose |
|------|-------------|---------|
| `ptl_full_YYYY-MM-DD.csv` | Yes | Full clinical PTL — restricted access |
| `dm01_return_YYYY-MM-DD.csv` | No | NHS England statutory submission |
| `ptl_summary_YYYY-MM-DD.csv` | No | Management dashboard / email distribution |

---

## Governance & Compliance

- **UK GDPR**: PII separated from aggregate outputs; full access logging via Python `logging`
- **NHS Cyber Security Strategy 2023–2030**: No patient-identifiable data in email distribution
- **Audit Trail**: Every pipeline run logged to `logs/pipeline_run.log` with timestamps
- **Version Control**: All logic changes tracked in Git — full rollback capability
- **DM01 Exclusion Transparency**: Approved reasons stored in version-controlled CSV, not analyst memory

---

## Background & Context

This pipeline was developed as part of the **Multiverse Advanced Data Fellowship (ADF_3002-5)**  
module: *Accelerating Data Solutions with DevOps Principles*.

The project applies the **CALMS DevOps framework** (Culture, Automation, Lean, Measurement, Sharing)  
to transform a manual administrative task into a managed, tested, auditable data product.

**Time saving**: ~4 hours/month → ~20 minutes/month (>90% reduction)  
**Annual saving**: ~48 hours of analyst time redirected to clinical value-adding work  
**Governance improvement**: Zero uncontrolled PII email distributions
