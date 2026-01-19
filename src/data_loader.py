import pandas as pd
from pybaseball import batting_stats

# --- Safe Import Strategy ---
# This prevents the "ImportError" from crashing the app at startup.
try:
    from pybaseball import team_salary
except ImportError:
    try:
        # Try alternate path found in some versions
        from pybaseball.salaries import team_salary  # type: ignore
    except ImportError:
        team_salary = None
        msg = (
            "Warning: 'team_salary' function not found in pybaseball. "
            "Mock data will be used."
        )
        print(msg)


def fetch_bluejays_batting_data(season: int = 2024) -> pd.DataFrame:
    """Fetches batting stats for TOR."""
    try:
        # qual=0 ensures we get all players
        data = batting_stats(season, qual=0)
        tor_data = data[data['Team'] == 'TOR'].copy()
        tor_data.reset_index(drop=True, inplace=True)
        return tor_data
    except Exception as e:
        raise RuntimeError(f"Failed to fetch batting data: {e}")


def _get_mock_salary_data() -> pd.DataFrame:
    """Helper to return mock data when API fails."""
    return pd.DataFrame({
        'Name': ['Vladimir Guerrero Jr.', 'Bo Bichette', 'George Springer'],
        'Salary': [19900000, 11000000, 24166666]
    })


def fetch_bluejays_salaries(season: int = 2024) -> pd.DataFrame:
    """
    Fetches salary data for TOR.
    """
    # 1. Check if the library function exists
    if team_salary is None:
        return _get_mock_salary_data()

    try:
        # 2. Attempt to fetch real data
        salaries = team_salary('TOR', season)
        # 3. Clean up columns
        salaries.columns = [c.lower() for c in salaries.columns]
        if 'name' in salaries.columns:
            salaries = salaries.rename(columns={'name': 'Name'})
        if 'salary' in salaries.columns:
            salaries = salaries.rename(columns={'salary': 'Salary'})
        return salaries
    except Exception as e:
        print(f"Warning: API fetch failed ({e}). Using mock data.")
        return _get_mock_salary_data()


def create_analysis_dataset(season: int = 2024) -> pd.DataFrame:
    """
    Orchestrator: Fetches both stats and salaries, then merges them.
    """
    stats_df = fetch_bluejays_batting_data(season)
    salary_df = fetch_bluejays_salaries(season)
    # Merge (Inner Join on Name)
    merged_df = pd.merge(stats_df, salary_df, on='Name', how='inner')
    return merged_df


if __name__ == "__main__":
    df = create_analysis_dataset(2024)
    print(f"Merged Dataset: {len(df)} players found.")
    print(df[['Name', 'Age', 'WAR', 'Salary']].head())
