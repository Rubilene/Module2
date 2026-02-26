"""
ptl_pipeline.py
---------------
NHS Radiology Department — Patient Tracking List (PTL) Automated Pipeline
Replaces the manual 4-5 hour Excel/Access macro workflow with a fully
auditable, tested Python pipeline that runs in under 20 minutes.

Problem Solved:
  - Eliminates 8 manual CSV exports by processing the full dataset in memory
  - Automatically removes duplicate Accession Numbers (previously done by hand)
  - Calculates 4-week (28-day) and 6-week (42-day) breach flags programmatically
  - Applies DM01 exclusion logic consistently, removing analyst subjectivity
  - Logs every run with timestamps for full audit trail (replaces zero audit coverage)
  - Outputs redacted and full versions for governance compliance

Usage:
    python src/ptl_pipeline.py --input data/radnet_export.csv --output data/outputs/
    python src/ptl_pipeline.py --input data/radnet_export.csv --output data/outputs/ --dm01

Author: NHS Radiology BI Team
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/pipeline_run.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
BREACH_4W_DAYS = 28   # 4-week threshold (NHS RTT standard)
BREACH_6W_DAYS = 42   # 6-week threshold (DM01 escalation threshold)
ORDERED_STATUS = "Ordered"
REQUIRED_COLUMNS = {
    "Accession", "MRN", "Patient_Name", "Exam_Status",
    "Request_Date", "Modality", "Exam_Description",
}

# Statuses to EXCLUDE from PTL (not waiting — already actioned)
EXCLUDE_STATUSES = {"Completed", "Cancelled", "Discontinued", "Arrived", "In Progress"}

# DM01 exclusion reasons (agreed with clinical leads — documented here for auditability)
DM01_EXCLUSION_REASONS = {
    "Patient Choice",
    "Clinical Reason",
    "Hospital Initiated Postponement",
    "Clinically Appropriate Pause",
}


# ── Core Pipeline ─────────────────────────────────────────────────────────────

def load_data(filepath: str) -> pd.DataFrame:
    """
    Load RadNet CSV export.

    WHY: RadNet exports MRN as a numeric field, stripping leading zeros (e.g. 0012345
    becomes 12345). This caused patient matching failures in the manual process.
    We force MRN to string and zero-pad to 7 digits on load, fixing the root cause.
    """
    log.info(f"Loading data from: {filepath}")
    df = pd.read_csv(
        filepath,
        dtype={"MRN": str, "Accession": str},   # prevent leading zero loss
        parse_dates=["Request_Date"],
        dayfirst=True,                            # NHS date format: DD/MM/YYYY
    )
    # Zero-pad MRN to 7 digits (NHS standard)
    df["MRN"] = df["MRN"].str.zfill(7)
    log.info(f"Loaded {len(df):,} rows from {filepath}")
    return df


def validate_schema(df: pd.DataFrame) -> None:
    """
    Assert all required columns exist with correct types.

    WHY: RadNet occasionally changes column names between releases. This check
    catches schema drift immediately rather than silently producing wrong output,
    which happened twice with the manual process causing incorrect breach reports.
    """
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Schema validation FAILED. Missing columns: {missing}")

    if not pd.api.types.is_datetime64_any_dtype(df["Request_Date"]):
        raise TypeError("Request_Date must be parsed as datetime. Check date format in export.")

    log.info("Schema validation PASSED.")


def filter_ordered_exams(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove non-waiting statuses from the dataset.

    WHY: The manual process required an analyst to manually delete rows with
    statuses like 'Completed', 'Cancelled', etc. — a step that was occasionally
    skipped under time pressure, inflating breach counts. This function enforces
    the filter consistently on every run.
    """
    before = len(df)
    df = df[df["Exam_Status"] == ORDERED_STATUS].copy()
    removed = before - len(df)
    log.info(f"Status filter: removed {removed:,} non-ordered rows. Remaining: {len(df):,}")
    return df


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate Accession Numbers, keeping the most recent Request_Date.

    WHY: The 8-batch manual export process created duplicates when a patient
    appeared in overlapping date windows. Manual deduplication was error-prone
    and took ~30 minutes. This step runs in milliseconds and is deterministic.
    """
    before = len(df)
    df = df.sort_values("Request_Date", ascending=False)
    df = df.drop_duplicates(subset=["Accession"], keep="first")
    removed = before - len(df)
    log.info(f"Deduplication: removed {removed:,} duplicate Accession Numbers.")
    return df


def calculate_waiting_times(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Wait_Days, Breach_4w, and Breach_6w columns.

    WHY: Excel DATEDIF formulas were used manually and required the analyst
    to remember to refresh them each month. On two occasions, stale formulas
    produced incorrect breach counts submitted to NHS England. This function
    calculates from today's date dynamically on every run.
    """
    today = pd.Timestamp(date.today())
    df["Wait_Days"] = (today - df["Request_Date"]).dt.days
    df["Breach_4w"] = df["Wait_Days"] >= BREACH_4W_DAYS
    df["Breach_6w"] = df["Wait_Days"] >= BREACH_6W_DAYS
    log.info(
        f"Breach summary — 4w: {df['Breach_4w'].sum():,} | "
        f"6w: {df['Breach_6w'].sum():,} | "
        f"Total waiting: {len(df):,}"
    )
    return df


def apply_exclusions(df: pd.DataFrame, exclusions_path: str = None) -> pd.DataFrame:
    """
    Apply DM01 exclusion logic by joining against the approved exclusions table.

    WHY: In the manual process, exclusion decisions were applied inconsistently —
    some analysts excluded 'Patient Choice' deferrals, others did not. This caused
    discrepancies between the Trust's internal PTL and the submitted DM01 return.
    This function enforces the agreed exclusion list, stored in a version-controlled
    CSV, so every run applies identical logic.

    If no exclusions file is provided, the function logs a warning and continues
    without exclusions (fail-safe rather than fail-hard, appropriate for PTL context).
    """
    if exclusions_path is None or not Path(exclusions_path).exists():
        log.warning("No exclusions file found. Proceeding without DM01 exclusions.")
        df["DM01_Excluded"] = False
        return df

    exclusions = pd.read_csv(exclusions_path, dtype={"Accession": str})
    # Only honour exclusions with approved reasons
    valid_exclusions = exclusions[
        exclusions["Exclusion_Reason"].isin(DM01_EXCLUSION_REASONS)
    ]["Accession"].unique()

    df["DM01_Excluded"] = df["Accession"].isin(valid_exclusions)
    excluded_count = df["DM01_Excluded"].sum()
    log.info(f"DM01 exclusions applied: {excluded_count:,} patients excluded.")
    return df


def classify_dm01(df: pd.DataFrame) -> pd.DataFrame:
    """
    Classify patients for DM01 return (6-week breaches, not excluded).

    WHY: DM01 is the statutory NHS England return for diagnostic waiting times.
    Previously this was a manual filter in Excel requiring the analyst to cross-
    reference two spreadsheets. Errors in this return have regulatory consequences.
    """
    df["DM01_Reportable"] = df["Breach_6w"] & ~df["DM01_Excluded"]
    dm01_count = df["DM01_Reportable"].sum()
    log.info(f"DM01 reportable patients: {dm01_count:,}")
    return df


def generate_outputs(df: pd.DataFrame, output_dir: str) -> dict:
    """
    Write PTL output, DM01 return, and redacted summary to output_dir.

    WHY: Previously, three separate spreadsheets were manually created and
    emailed to different recipients, each containing full PII. Now:
      - ptl_full.csv  — full dataset for internal clinical use (access controlled)
      - dm01_return.csv — 6w+ breaches for NHS England submission (no PII)
      - ptl_summary.csv — aggregate counts for management dashboard (no PII)
    Separating PII from statutory outputs reduces governance risk significantly.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    run_date = date.today().isoformat()

    # Full PTL (contains PII — restricted access)
    ptl_path = out / f"ptl_full_{run_date}.csv"
    df.to_csv(ptl_path, index=False)
    log.info(f"Full PTL written: {ptl_path}")

    # DM01 return — NO PII (Accession + clinical fields only)
    dm01_cols = ["Accession", "Modality", "Exam_Description",
                 "Wait_Days", "Breach_6w", "DM01_Excluded", "DM01_Reportable"]
    dm01_path = out / f"dm01_return_{run_date}.csv"
    df[dm01_cols].to_csv(dm01_path, index=False)
    log.info(f"DM01 return written: {dm01_path}")

    # Summary (aggregate counts — safe for email distribution)
    summary = {
        "Run_Date": run_date,
        "Total_Waiting": len(df),
        "Breach_4w_Count": int(df["Breach_4w"].sum()),
        "Breach_6w_Count": int(df["Breach_6w"].sum()),
        "DM01_Excluded": int(df["DM01_Excluded"].sum()),
        "DM01_Reportable": int(df["DM01_Reportable"].sum()),
    }
    summary_df = pd.DataFrame([summary])
    summary_path = out / f"ptl_summary_{run_date}.csv"
    summary_df.to_csv(summary_path, index=False)
    log.info(f"Summary written: {summary_path}")

    return {"ptl": ptl_path, "dm01": dm01_path, "summary": summary_path}


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_pipeline(input_path: str, output_dir: str, exclusions_path: str = None) -> dict:
    """
    End-to-end PTL pipeline orchestrator.

    Each step is independently testable and logged. Any step failure raises
    an exception with a descriptive message, halting the pipeline cleanly
    rather than silently producing incorrect output.
    """
    log.info("=" * 60)
    log.info("PTL PIPELINE STARTED")
    log.info("=" * 60)

    df = load_data(input_path)
    validate_schema(df)
    df = filter_ordered_exams(df)
    df = deduplicate(df)
    df = calculate_waiting_times(df)
    df = apply_exclusions(df, exclusions_path)
    df = classify_dm01(df)
    outputs = generate_outputs(df, output_dir)

    log.info("=" * 60)
    log.info("PTL PIPELINE COMPLETED SUCCESSFULLY")
    log.info("=" * 60)
    return outputs


# ── CLI Entry Point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NHS Radiology PTL Automated Pipeline")
    parser.add_argument("--input", required=True, help="Path to RadNet CSV export")
    parser.add_argument("--output", required=True, help="Output directory for results")
    parser.add_argument("--exclusions", default=None, help="Path to DM01 exclusions CSV")
    args = parser.parse_args()

    Path("logs").mkdir(exist_ok=True)
    run_pipeline(args.input, args.output, args.exclusions)
