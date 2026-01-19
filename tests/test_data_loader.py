from src.data_loader import create_analysis_dataset


def test_create_analysis_dataset():
    """
    Validates that we can successfully merge stats and salary data.
    """
    df = create_analysis_dataset(2024)
    # 1. Check if merge happened
    # We might have fewer rows than the full roster due to name mismatches,
    # but it shouldn't be empty for a major team.
    assert not df.empty, (
        "Merged dataset is empty. Check name matching or API data."
        )
    # 2. Check for Critical ML Columns
    required_cols = ['Name', 'WAR', 'Salary']
    for col in required_cols:
        assert col in df.columns, f"Missing {col} in merged dataset"
    # 3. Data Integrity
    # Salary should be > 0
    assert (df['Salary'] > 0).all(), "Found players with 0 or negative salary"
