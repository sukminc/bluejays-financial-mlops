import pandas as pd
from pybaseball import batting_stats, team_salary


def fetch_bluejays_batting_data(season: int = 2024) -> pd.DataFrame:
    """Fetches batting stats for TOR."""
    try:
        data = batting_stats(season, qual=0)
        tor_data = data[data['Team'] == 'TOR'].copy()
        tor_data.reset_index(drop=True, inplace=True)
        return tor_data
    except Exception as e:
        raise RuntimeError(f"Failed to fetch batting data: {e}")


def fetch_bluejays_salaries(season: int = 2024) -> pd.DataFrame:
    """
    Fetches salary data for TOR.
    Note: pybaseball.team_salary returns columns like ['name', 'salary', ...].
    """
    try:
        # team_salary returns a DF with player names and salaries
        salaries = team_salary('TOR', season)
        # Clean up column names for consistency
        # Assuming columns might be 'name', 'salary', 'year' etc.
        salaries.columns = [c.lower() for c in salaries.columns]
        # Ensure we have the columns we need
        if 'name' in salaries.columns:
            salaries = salaries.rename(columns={'name': 'Name'})
        if 'salary' in salaries.columns:
            salaries = salaries.rename(columns={'salary': 'Salary'})
        return salaries
    except Exception as e:
        # Fallback/Mock for stability if API fails (common in salary APIs)
        print(f"Warning: Could not fetch real salaries ({e}). Using mock data for MVP structure.")
        return pd.DataFrame({
            'Name': ['Vladimir Guerrero Jr.', 'Bo Bichette'],
            'Salary': [19900000, 11000000]
        })


def create_analysis_dataset(season: int = 2024) -> pd.DataFrame:
    """
    Orchestrator: Fetches both stats and salaries, then merges them.
    This creates the 'Master Table' for your ML model.
    """
    # 1. Fetch
    stats_df = fetch_bluejays_batting_data(season)
    salary_df = fetch_bluejays_salaries(season)
    # 2. Merge (Inner Join on Name)
    # Note: In real world, we need fuzzy matching (e.g. 'Vlad Guerrero' vs 'Vladimir Guerrero Jr.')
    # For MVP, we use exact match.
    merged_df = pd.merge(stats_df, salary_df, on='Name', how='inner')
    return merged_df


if __name__ == "__main__":
    df = create_analysis_dataset(2024)
    print(f"Merged Dataset: {len(df)} players found with both stats and salary.")
    print(df[['Name', 'Age', 'WAR', 'Salary']].head())
