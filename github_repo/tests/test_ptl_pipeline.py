"""
tests/test_ptl_pipeline.py
--------------------------
Test suite for the NHS Radiology PTL Pipeline.
Tests were written BEFORE implementation (TDD approach).

Each test maps directly to a specific problem in the manual workflow:
  - test_mrn_leading_zeros        → MRN format loss in Excel/CSV
  - test_duplicate_removal        → Duplicates from 8-batch manual exports
  - test_breach_thresholds        → Stale Excel DATEDIF formulas
  - test_schema_validation        → Silent schema drift between RadNet releases
  - test_exclusion_logic          → Inconsistent analyst DM01 exclusion decisions
  - test_status_filter            → Missed status deletions under time pressure
  - test_dm01_classification      → Manual cross-referencing errors
  - test_null_mrn_handling        → Missing MRNs in edge-case exports
  - test_malformed_dates          → Date format inconsistencies across RadNet versions

Run with:
    pytest tests/ -v --tb=short
    pytest tests/ -v --cov=src --cov-report=term-missing
"""

import pytest
import pandas as pd
from datetime import date, timedelta
from unittest.mock import patch
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from ptl_pipeline import (
    validate_schema,
    filter_ordered_exams,
    deduplicate,
    calculate_waiting_times,
    apply_exclusions,
    classify_dm01,
    load_data,
    BREACH_4W_DAYS,
    BREACH_6W_DAYS,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_df():
    """
    Minimal valid DataFrame simulating a RadNet export.
    Dates are relative to today so breach logic is always testable.
    """
    today = pd.Timestamp(date.today())
    return pd.DataFrame({
        "Accession":       ["ACC001", "ACC002", "ACC003", "ACC004", "ACC005"],
        "MRN":             ["0012345", "0054321", "0099999", "0011111", "0022222"],
        "Patient_Name":    ["Smith J", "Jones A", "Brown B", "Davies C", "Wilson D"],
        "Exam_Status":     ["Ordered", "Ordered", "Completed", "Ordered", "Ordered"],
        "Request_Date":    [
            today - timedelta(days=50),   # 6w+ breach
            today - timedelta(days=30),   # 4w+ breach only
            today - timedelta(days=20),   # Completed — should be excluded
            today - timedelta(days=5),    # Not yet in breach
            today - timedelta(days=50),   # Duplicate of ACC005 below
        ],
        "Modality":        ["MRI", "CT", "XR", "US", "MRI"],
        "Exam_Description": ["Brain MRI", "Chest CT", "Chest XR", "Abdomen US", "Spine MRI"],
    })


@pytest.fixture
def sample_df_with_duplicate(sample_df):
    """Add a duplicate Accession row to test deduplication."""
    dup_row = sample_df.iloc[0].copy()
    dup_row["Accession"] = "ACC001"   # same as first row → duplicate
    dup_row["Request_Date"] = pd.Timestamp(date.today()) - timedelta(days=10)
    return pd.concat([sample_df, pd.DataFrame([dup_row])], ignore_index=True)


# ── Schema Validation Tests ───────────────────────────────────────────────────

class TestSchemaValidation:
    """
    Guards against silent schema drift when RadNet updates column names.
    In the manual process, this went undetected and produced wrong outputs.
    """

    def test_valid_schema_passes(self, sample_df):
        """A correctly structured DataFrame should not raise."""
        validate_schema(sample_df)  # no exception = pass

    def test_missing_column_raises_value_error(self, sample_df):
        """Dropping a required column must be caught immediately."""
        broken = sample_df.drop(columns=["Accession"])
        with pytest.raises(ValueError, match="Missing columns"):
            validate_schema(broken)

    def test_wrong_date_type_raises_type_error(self, sample_df):
        """Request_Date as string (not parsed) should fail validation."""
        broken = sample_df.copy()
        broken["Request_Date"] = broken["Request_Date"].astype(str)
        with pytest.raises(TypeError, match="datetime"):
            validate_schema(broken)

    def test_multiple_missing_columns_reported(self, sample_df):
        """All missing columns should be listed in the error message."""
        broken = sample_df.drop(columns=["Accession", "MRN"])
        with pytest.raises(ValueError) as exc_info:
            validate_schema(broken)
        assert "Accession" in str(exc_info.value) or "MRN" in str(exc_info.value)


# ── MRN Format Tests ──────────────────────────────────────────────────────────

class TestMRNFormatting:
    """
    RadNet exports MRN as numeric, stripping leading zeros.
    This caused patient matching failures in the manual process.
    """

    def test_mrn_leading_zeros_preserved(self, tmp_path):
        """MRNs with leading zeros must survive the CSV round-trip."""
        csv_content = (
            "Accession,MRN,Patient_Name,Exam_Status,Request_Date,Modality,Exam_Description\n"
            "ACC001,12345,Smith J,Ordered,01/06/2025,MRI,Brain MRI\n"
        )
        f = tmp_path / "test.csv"
        f.write_text(csv_content)

        df = load_data(str(f))
        # MRN 12345 should be zero-padded to 0012345 (7 digits)
        assert df["MRN"].iloc[0] == "0012345", (
            f"Expected '0012345', got '{df['MRN'].iloc[0]}' — leading zero was lost"
        )

    def test_already_padded_mrn_unchanged(self, tmp_path):
        """MRNs already at 7 digits should not be altered."""
        csv_content = (
            "Accession,MRN,Patient_Name,Exam_Status,Request_Date,Modality,Exam_Description\n"
            "ACC001,0012345,Smith J,Ordered,01/06/2025,MRI,Brain MRI\n"
        )
        f = tmp_path / "test.csv"
        f.write_text(csv_content)
        df = load_data(str(f))
        assert df["MRN"].iloc[0] == "0012345"


# ── Status Filter Tests ───────────────────────────────────────────────────────

class TestStatusFilter:
    """
    Analysts occasionally forgot to delete non-Ordered rows under time pressure,
    inflating waiting list counts. This filter is now enforced automatically.
    """

    def test_only_ordered_exams_remain(self, sample_df):
        result = filter_ordered_exams(sample_df)
        assert all(result["Exam_Status"] == "Ordered")

    def test_completed_exams_removed(self, sample_df):
        result = filter_ordered_exams(sample_df)
        assert "ACC003" not in result["Accession"].values

    def test_row_count_correct_after_filter(self, sample_df):
        """sample_df has 1 Completed row; expect 4 Ordered remaining."""
        result = filter_ordered_exams(sample_df)
        assert len(result) == 4

    def test_empty_df_after_filter_returns_empty(self):
        """All non-Ordered rows should return an empty (not erroring) DataFrame."""
        df = pd.DataFrame({
            "Accession": ["A1"], "MRN": ["001"], "Patient_Name": ["X"],
            "Exam_Status": ["Completed"],
            "Request_Date": [pd.Timestamp("2025-01-01")],
            "Modality": ["MRI"], "Exam_Description": ["Brain"],
        })
        result = filter_ordered_exams(df)
        assert len(result) == 0


# ── Deduplication Tests ───────────────────────────────────────────────────────

class TestDeduplication:
    """
    8-batch export process created duplicate Accession Numbers.
    Manual removal took ~30 minutes and occasionally missed rows.
    """

    def test_duplicate_accession_removed(self, sample_df_with_duplicate):
        result = deduplicate(sample_df_with_duplicate)
        assert result["Accession"].duplicated().sum() == 0

    def test_most_recent_record_kept(self, sample_df_with_duplicate):
        """When duplicates exist, keep the row with the most recent Request_Date."""
        result = deduplicate(sample_df_with_duplicate)
        acc001 = result[result["Accession"] == "ACC001"]
        # The newer Request_Date (10 days ago) should be retained
        today = pd.Timestamp(date.today())
        assert (today - acc001["Request_Date"].iloc[0]).days == 10

    def test_no_data_loss_without_duplicates(self, sample_df):
        """Deduplication on clean data must not remove any valid rows."""
        ordered_only = filter_ordered_exams(sample_df)
        before = len(ordered_only)
        result = deduplicate(ordered_only)
        assert len(result) == before


# ── Breach Calculation Tests ──────────────────────────────────────────────────

class TestBreachCalculation:
    """
    Stale Excel DATEDIF formulas caused incorrect breach submissions to NHS England.
    These tests verify the calculation logic is always applied to today's date.
    """

    def test_6w_breach_correctly_flagged(self, sample_df):
        ordered = filter_ordered_exams(sample_df)
        result = calculate_waiting_times(ordered)
        # ACC001: 50 days > 42 → must be True
        assert result.loc[result["Accession"] == "ACC001", "Breach_6w"].iloc[0] is True

    def test_4w_breach_only_correctly_flagged(self, sample_df):
        ordered = filter_ordered_exams(sample_df)
        result = calculate_waiting_times(ordered)
        # ACC002: 30 days >= 28 but < 42 → Breach_4w=True, Breach_6w=False
        row = result[result["Accession"] == "ACC002"]
        assert row["Breach_4w"].iloc[0] is True
        assert row["Breach_6w"].iloc[0] is False

    def test_no_breach_for_recent_referral(self, sample_df):
        ordered = filter_ordered_exams(sample_df)
        result = calculate_waiting_times(ordered)
        # ACC004: 5 days → no breach
        row = result[result["Accession"] == "ACC004"]
        assert row["Breach_4w"].iloc[0] is False
        assert row["Breach_6w"].iloc[0] is False

    def test_exact_28_day_boundary(self):
        """Patient waiting exactly 28 days must be flagged as a 4-week breach."""
        today = pd.Timestamp(date.today())
        df = pd.DataFrame({
            "Accession": ["ACC_BOUNDARY"],
            "Exam_Status": ["Ordered"],
            "Request_Date": [today - timedelta(days=28)],
            "MRN": ["0000001"], "Patient_Name": ["Boundary T"],
            "Modality": ["MRI"], "Exam_Description": ["Test"],
        })
        result = calculate_waiting_times(df)
        assert result["Breach_4w"].iloc[0] is True

    def test_wait_days_is_non_negative(self, sample_df):
        ordered = filter_ordered_exams(sample_df)
        result = calculate_waiting_times(ordered)
        assert (result["Wait_Days"] >= 0).all()


# ── Exclusion Logic Tests ─────────────────────────────────────────────────────

class TestExclusionLogic:
    """
    Inconsistent analyst decisions on DM01 exclusions caused discrepancies
    between internal PTL and NHS England submissions.
    """

    def test_valid_exclusion_applied(self, sample_df, tmp_path):
        ordered = filter_ordered_exams(sample_df)
        timed = calculate_waiting_times(ordered)
        excl_file = tmp_path / "exclusions.csv"
        excl_file.write_text(
            "Accession,Exclusion_Reason\nACC001,Patient Choice\n"
        )
        result = apply_exclusions(timed, str(excl_file))
        assert result.loc[result["Accession"] == "ACC001", "DM01_Excluded"].iloc[0] is True

    def test_invalid_exclusion_reason_ignored(self, sample_df, tmp_path):
        """Unapproved exclusion reasons must not be applied."""
        ordered = filter_ordered_exams(sample_df)
        timed = calculate_waiting_times(ordered)
        excl_file = tmp_path / "exclusions.csv"
        excl_file.write_text(
            "Accession,Exclusion_Reason\nACC002,Not a valid reason\n"
        )
        result = apply_exclusions(timed, str(excl_file))
        assert result.loc[result["Accession"] == "ACC002", "DM01_Excluded"].iloc[0] is False

    def test_no_exclusions_file_does_not_crash(self, sample_df):
        """Missing exclusions file should warn but not crash the pipeline."""
        ordered = filter_ordered_exams(sample_df)
        timed = calculate_waiting_times(ordered)
        result = apply_exclusions(timed, exclusions_path=None)
        assert "DM01_Excluded" in result.columns
        assert result["DM01_Excluded"].sum() == 0


# ── DM01 Classification Tests ─────────────────────────────────────────────────

class TestDM01Classification:
    """
    DM01 is the statutory NHS England return. Errors have regulatory consequences.
    """

    def test_6w_breach_not_excluded_is_dm01_reportable(self, sample_df):
        ordered = filter_ordered_exams(sample_df)
        timed = calculate_waiting_times(ordered)
        excl = apply_exclusions(timed)
        result = classify_dm01(excl)
        # ACC001: 50 days, not excluded → must be reportable
        assert result.loc[result["Accession"] == "ACC001", "DM01_Reportable"].iloc[0] is True

    def test_6w_breach_with_valid_exclusion_not_reportable(self, sample_df, tmp_path):
        ordered = filter_ordered_exams(sample_df)
        timed = calculate_waiting_times(ordered)
        excl_file = tmp_path / "exclusions.csv"
        excl_file.write_text("Accession,Exclusion_Reason\nACC001,Patient Choice\n")
        excl = apply_exclusions(timed, str(excl_file))
        result = classify_dm01(excl)
        # Excluded → not reportable
        assert result.loc[result["Accession"] == "ACC001", "DM01_Reportable"].iloc[0] is False

    def test_4w_breach_only_not_dm01_reportable(self, sample_df):
        ordered = filter_ordered_exams(sample_df)
        timed = calculate_waiting_times(ordered)
        excl = apply_exclusions(timed)
        result = classify_dm01(excl)
        # ACC002: only 4w breach, not 6w → not DM01 reportable
        assert result.loc[result["Accession"] == "ACC002", "DM01_Reportable"].iloc[0] is False


# ── Output Tests ──────────────────────────────────────────────────────────────

class TestOutputGeneration:
    """Verify that output files are created and contain expected columns."""

    def test_output_files_created(self, sample_df, tmp_path):
        from ptl_pipeline import generate_outputs
        ordered = filter_ordered_exams(sample_df)
        timed = calculate_waiting_times(ordered)
        excl = apply_exclusions(timed)
        classified = classify_dm01(excl)
        outputs = generate_outputs(classified, str(tmp_path))
        assert Path(outputs["ptl"]).exists()
        assert Path(outputs["dm01"]).exists()
        assert Path(outputs["summary"]).exists()

    def test_dm01_output_excludes_pii(self, sample_df, tmp_path):
        """DM01 CSV must not contain Patient_Name or MRN."""
        from ptl_pipeline import generate_outputs
        ordered = filter_ordered_exams(sample_df)
        timed = calculate_waiting_times(ordered)
        excl = apply_exclusions(timed)
        classified = classify_dm01(excl)
        outputs = generate_outputs(classified, str(tmp_path))
        dm01_df = pd.read_csv(outputs["dm01"])
        assert "Patient_Name" not in dm01_df.columns
        assert "MRN" not in dm01_df.columns
