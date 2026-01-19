import pandas as pd
from src.data_loader import fetch_bluejays_batting_data
from src.data_loader import create_analysis_dataset
from src.validator import validate_analysis_data


def test_fetch_bluejays_batting_data_structure():
    """
    Validates that the data loader returns a non-empty DataFrame
    with expected columns.
    """
    df = fetch_bluejays_batting_data(2024)
    assert not df.empty, "The DataFrame should not be empty for 2024."
    expected_columns = ['Name', 'Age', 'WAR', 'Team']
    for col in expected_columns:
        assert col in df.columns, f"Missing critical column: {col}"
    unique_teams = df['Team'].unique()
    assert len(unique_teams) == 1, "Data contains multiple teams."
    assert unique_teams[0] == 'TOR', (
        f"Expected Team 'TOR', got {unique_teams[0]}"
    )


def test_data_types():
    """
    Validates data types for numerical operations.
    """
    df = fetch_bluejays_batting_data(2024)
    assert pd.api.types.is_numeric_dtype(df['WAR']), (
        "WAR column should be numeric."
    )


def test_create_analysis_dataset():
    """
    Validates that we can successfully merge stats and salary data.
    """
    df = create_analysis_dataset(2024)
    assert not df.empty, "Merged dataset is empty."
    required_cols = ['Name', 'WAR', 'Salary']
    for col in required_cols:
        assert col in df.columns, f"Missing {col} in merged dataset"
    assert (df['Salary'] > 0).all(), "Found players with 0 or negative salary"


def test_data_quality_schema():
    """
    SDET QA Test:
    Validates that the merged dataset adheres to strict business logic
    (Schema Validation) using Pandera.
    """
    df = create_analysis_dataset(2024)
    # This will raise an exception if data violates the contract
    validated_df = validate_analysis_data(df)
    assert validated_df is not None
    assert not validated_df.empty
