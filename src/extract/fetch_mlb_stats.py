import requests
import pandas as pd
import os

# Configuration
TEAM_ID = 141  # Toronto Blue Jays
YEARS = [2023, 2024, 2025]
OUTPUT_DIR = "data/raw/stats"


def fetch_stats():
    print(f"⚾ MLB API: Fetching Deep Stats for {YEARS}...")

    for season in YEARS:
        print(f"\n📅 Processing Season: {season}")

        # 1. Get Roster
        try:
            base_url = "https://statsapi.mlb.com/api/v1"
            roster_url = f"{base_url}/teams/{TEAM_ID}/roster?season={season}"
            roster_data = requests.get(roster_url).json().get('roster', [])
        except Exception as e:
            print(f"❌ Failed to fetch roster: {e}")
            continue

        print(f"   found {len(roster_data)} players.")
        stats_list = []

        # 2. Loop players
        for player in roster_data:
            pid = player['person']['id']
            name = player['person']['fullName']
            pos = player['position']['abbreviation']

            # Fetch Hitting & Pitching
            # Split URL to satisfy E501 line length limit
            url = (
                f"{base_url}/people/{pid}"
                f"?hydrate=stats(group=[hitting,pitching],"
                f"type=[season],season={season})"
            )

            try:
                data = requests.get(url).json()
                if 'people' not in data or 'stats' not in data['people'][0]:
                    continue

                for group in data['people'][0]['stats']:
                    g_type = group['group']['displayName']
                    if not group.get('splits'):
                        continue

                    s = group['splits'][0]['stat']

                    # Common fields
                    row = {
                        'player_id': pid,
                        'player_name': name,
                        'season': season,
                        'position': pos,
                        'stat_type': g_type,
                        'games': s.get('gamesPlayed', 0)
                    }

                    if g_type == 'hitting':
                        # Advanced Hitting Metrics
                        row.update({
                            'plate_appearances': s.get('plateAppearances', 0),
                            'at_bats': s.get('atBats', 0),
                            'hits': s.get('hits', 0),
                            'doubles': s.get('doubles', 0),
                            'triples': s.get('triples', 0),
                            'homeruns': s.get('homeRuns', 0),
                            'rbi': s.get('rbi', 0),
                            'base_on_balls': s.get('baseOnBalls', 0),
                            'strikeouts': s.get('strikeOuts', 0),
                            'stolen_bases': s.get('stolenBases', 0),
                            'caught_stealing': s.get('caughtStealing', 0),
                            'avg': s.get('avg', '.000'),
                            'obp': s.get('obp', '.000'),
                            'slg': s.get('slg', '.000'),
                            'ops': s.get('ops', '.000')
                        })
                    elif g_type == 'pitching':
                        # Advanced Pitching Metrics
                        row.update({
                            'innings_pitched': s.get('inningsPitched', '0.0'),
                            'wins': s.get('wins', 0),
                            'losses': s.get('losses', 0),
                            'saves': s.get('saves', 0),
                            'blown_saves': s.get('blownSaves', 0),
                            'hits_allowed': s.get('hits', 0),
                            'runs_allowed': s.get('runs', 0),
                            'era': s.get('era', '0.00'),
                            'whip': s.get('whip', '0.00'),
                            'strikeouts': s.get('strikeOuts', 0),
                            'base_on_balls': s.get('baseOnBalls', 0)
                        })

                    stats_list.append(row)

            except Exception:
                continue

        # 3. Save
        if stats_list:
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            df = pd.DataFrame(stats_list)
            # Fill NaN for missing columns
            df = df.fillna(0)
            output_file = f"{OUTPUT_DIR}/mlb_stats_bluejays_{season}.csv"
            df.to_csv(output_file, index=False)
            print(f"   ✅ Saved {len(df)} rows.")


if __name__ == "__main__":
    fetch_stats()
