"""
Las Vegas Aces - Calculate Metrics from wehoop CSVs
====================================================
Loads existing team and player CSV files from wehoop.
Calculates Dean Oliver Four Factors and efficiency ratings.
Validates metrics against reasonable ranges.

Author: Krystal
Last Updated: 2026-01-06
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

# File paths - UPDATE THESE to your actual CSV locations
TEAM_CSV = "aces_team_box.csv"  # Your team box score CSV
PLAYER_CSV = "aces_player_box.csv"  # Your player box score CSV

# Output paths
OUTPUT_DIR = Path("data/processed")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("="*70)
print("LAS VEGAS ACES - METRIC CALCULATOR")
print("="*70)
print(f"Team CSV: {TEAM_CSV}")
print(f"Player CSV: {PLAYER_CSV}")
print("="*70)
print()


# ============================================================================
# DEAN OLIVER FORMULAS
# ============================================================================

def calculate_possessions(fga, fta, orb, tov):
    """
    Dean Oliver possession estimate.
    Poss = FGA + 0.44 * FTA - ORB + TOV
    """
    return np.maximum(fga + 0.44 * fta - orb + tov, 1)


def add_four_factors(df):
    """
    Calculate Four Factors and efficiency metrics.
    Assumes columns exist (or can be derived).
    """
    print("\n[METRICS] Calculating Four Factors...")
    
    # Ensure numeric
    numeric_cols = [
        'fgm', 'fga', 'fg3m', 'fg3a', 'ftm', 'fta',
        'orb', 'drb', 'tov', 'ast', 'pts'
    ]
    
    # Try multiple column name patterns
    col_mapping = {
        'field_goals_made': 'fgm',
        'fieldGoalsMade': 'fgm',
        'field_goals_attempted': 'fga',
        'fieldGoalsAttempted': 'fga',
        'three_point_field_goals_made': 'fg3m',
        'threePointFieldGoalsMade': 'fg3m',
        'three_point_field_goals_attempted': 'fg3a',
        'threePointFieldGoalsAttempted': 'fg3a',
        'free_throws_made': 'ftm',
        'freeThrowsMade': 'ftm',
        'free_throws_attempted': 'fta',
        'freeThrowsAttempted': 'fta',
        'offensive_rebounds': 'orb',
        'offensiveRebounds': 'orb',
        'defensive_rebounds': 'drb',
        'defensiveRebounds': 'drb',
        'turnovers': 'tov',
        'assists': 'ast',
        'points': 'pts',
        'team_score': 'pts'
    }
    
    # Rename if needed
    df = df.rename(columns=col_mapping)
    
    # Ensure all required columns exist
    for col in numeric_cols:
        if col not in df.columns:
            print(f"  ⚠ Missing column: {col}")
            df[col] = 0
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Possessions
    df['poss_est'] = calculate_possessions(df['fga'], df['fta'], df['orb'], df['tov'])
    
    # === FOUR FACTORS ===
    
    # 1. Effective Field Goal %
    df['efg_pct'] = np.where(
        df['fga'] > 0,
        (df['fgm'] + 0.5 * df['fg3m']) / df['fga'],
        0
    )
    
    # 2. Turnover %
    df['tov_pct'] = np.where(
        df['poss_est'] > 0,
        df['tov'] / df['poss_est'],
        0
    )
    
    # 3. Free Throw Rate
    df['ftr'] = np.where(
        df['fga'] > 0,
        df['fta'] / df['fga'],
        0
    )
    
    # === SHOOTING EFFICIENCY ===
    
    # True Shooting %
    df['ts_pct'] = np.where(
        (df['fga'] + 0.44 * df['fta']) > 0,
        df['pts'] / (2 * (df['fga'] + 0.44 * df['fta'])),
        0
    )
    
    # 2-Point %
    df['fg2m'] = df['fgm'] - df['fg3m']
    df['fg2a'] = df['fga'] - df['fg3a']
    df['fg2_pct'] = np.where(df['fg2a'] > 0, df['fg2m'] / df['fg2a'], 0)
    
    # 3-Point %
    df['fg3_pct'] = np.where(df['fg3a'] > 0, df['fg3m'] / df['fg3a'], 0)
    
    # Free Throw %
    df['ft_pct'] = np.where(df['fta'] > 0, df['ftm'] / df['fta'], 0)
    
    # 3-Point Attempt Rate
    df['fg3ar'] = np.where(df['fga'] > 0, df['fg3a'] / df['fga'], 0)
    
    # === EFFICIENCY RATINGS ===
    
    # Offensive Rating
    df['ortg'] = np.where(
        df['poss_est'] > 0,
        100 * df['pts'] / df['poss_est'],
        0
    )
    
    # === PLAYMAKING ===
    
    # Assist Rate
    df['ast_rate'] = np.where(df['fgm'] > 0, df['ast'] / df['fgm'], 0)
    
    # Assist-to-Turnover Ratio
    df['ast_tov'] = np.where(df['tov'] > 0, df['ast'] / df['tov'], 0)
    
    print(f"  ✓ Calculated {len(['efg_pct', 'tov_pct', 'ftr', 'ts_pct', 'ortg'])} core metrics")
    
    return df


def add_opponent_metrics(df):
    """
    Add opponent-adjusted metrics for TEAM data only.
    Requires game_id column.
    """
    print("\n[METRICS] Calculating opponent-adjusted metrics...")
    
    if 'game_id' not in df.columns:
        print("  ⚠ Skipping - no game_id column (not team-level data)")
        return df
    
    # Check if this looks like team data (should have ~80 games, not ~960 player-games)
    if len(df) > 500:
        print("  ⚠ Skipping - looks like player data, not team data")
        return df
    
    # Create opponent dataframe
    opp_df = df[['game_id', 'team_id', 'pts', 'poss_est', 'orb', 'drb']].copy()
    opp_df.columns = ['game_id', 'opp_team_id', 'opp_pts', 'opp_poss_est', 'opp_orb', 'opp_drb']
    
    # Self-join to get opponent stats
    df_with_opp = df.merge(opp_df, on='game_id', how='left')
    
    # Filter to actual opponents (not self)
    if 'team_id' in df.columns and 'opp_team_id' in df_with_opp.columns:
        df_with_opp = df_with_opp[df_with_opp['team_id'] != df_with_opp['opp_team_id']]
    
    # 4. Offensive Rebound %
    df_with_opp['oreb_pct'] = np.where(
        (df_with_opp['orb'] + df_with_opp['opp_drb']) > 0,
        df_with_opp['orb'] / (df_with_opp['orb'] + df_with_opp['opp_drb']),
        0
    )
    
    # Defensive Rebound %
    df_with_opp['dreb_pct'] = np.where(
        (df_with_opp['drb'] + df_with_opp['opp_orb']) > 0,
        df_with_opp['drb'] / (df_with_opp['drb'] + df_with_opp['opp_orb']),
        0
    )
    
    # Defensive Rating
    df_with_opp['drtg'] = np.where(
        df_with_opp['opp_poss_est'] > 0,
        100 * df_with_opp['opp_pts'] / df_with_opp['opp_poss_est'],
        0
    )
    
    # Net Rating
    df_with_opp['net_rtg'] = df_with_opp['ortg'] - df_with_opp['drtg']
    
    print(f"  ✓ Added opponent-adjusted metrics (OREB%, DREB%, DRtg, NetRtg)")
    return df_with_opp


# ============================================================================
# VALIDATION
# ============================================================================

def validate_metrics(df, data_type):
    """Run validation checks on calculated metrics."""
    if len(df) == 0:
        return
    
    print(f"\n[VALIDATION] {data_type}:")
    
    checks = {
        'efg_pct': (0, 1, "eFG% should be 0-1"),
        'ts_pct': (0, 1, "TS% should be 0-1"),
        'tov_pct': (0, 0.5, "TOV% should be 0-0.5"),
        'ortg': (50, 150, "ORtg should be 50-150"),
    }
    
    for metric, (min_val, max_val, msg) in checks.items():
        if metric in df.columns:
            valid = df[metric].between(min_val, max_val, inclusive='both')
            pct_valid = valid.sum() / len(df) * 100
            
            if pct_valid < 95:
                print(f"  ⚠ {metric}: {pct_valid:.1f}% in valid range ({msg})")
            else:
                print(f"  ✓ {metric}: {pct_valid:.1f}% valid")
            
            # Show sample values
            print(f"    Range: {df[metric].min():.3f} - {df[metric].max():.3f}, "
                  f"Mean: {df[metric].mean():.3f}")


def extract_player_ids(df):
    """Extract player IDs and names for use with pbpstats lineup builder."""
    print("\n[PLAYER IDS] Extracting for pbpstats...")
    
    # Try multiple column name patterns
    player_id_cols = ['player_id', 'athlete_id', 'playerId', 'athleteId']
    player_name_cols = ['player_name', 'athlete_display_name', 'playerName', 'athleteDisplayName']
    
    player_id_col = None
    player_name_col = None
    
    for col in player_id_cols:
        if col in df.columns:
            player_id_col = col
            break
    
    for col in player_name_cols:
        if col in df.columns:
            player_name_col = col
            break
    
    if not player_id_col or not player_name_col:
        print("  ⚠ Could not find player ID or name columns")
        return pd.DataFrame()
    
    # Get unique players with their total minutes
    if 'minutes' in df.columns:
        minutes_col = 'minutes'
    elif 'min' in df.columns:
        minutes_col = 'min'
    else:
        print("  ⚠ No minutes column found")
        return pd.DataFrame()
    
    # Convert minutes to numeric (might be "MM:SS" format)
    if df[minutes_col].dtype == 'object':
        # Try to convert from "MM:SS" to decimal
        try:
            df['minutes_decimal'] = df[minutes_col].apply(
                lambda x: int(x.split(':')[0]) + int(x.split(':')[1])/60 if isinstance(x, str) and ':' in x else float(x)
            )
        except:
            df['minutes_decimal'] = pd.to_numeric(df[minutes_col], errors='coerce')
    else:
        df['minutes_decimal'] = df[minutes_col]
    
    players = df.groupby([player_id_col, player_name_col]).agg(
        total_minutes=('minutes_decimal', 'sum'),
        games=('game_id', 'nunique') if 'game_id' in df.columns else (player_id_col, 'count')
    ).reset_index()
    
    players = players.rename(columns={
        player_id_col: 'player_id',
        player_name_col: 'player_name'
    })
    
    players = players.sort_values('total_minutes', ascending=False)
    
    print(f"  ✓ Found {len(players)} players")
    print(f"\n  Top 10 by minutes:")
    print(players.head(10).to_string(index=False))
    
    return players


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print(f"\nStarted: {pd.Timestamp.now()}\n")
    
    # Load team data
    print("="*70)
    print("LOADING TEAM DATA")
    print("="*70)
    
    try:
        team_df = pd.read_csv(TEAM_CSV)
        print(f"✓ Loaded team data: {len(team_df)} rows")
        print(f"  Columns: {list(team_df.columns[:10])}...")
    except FileNotFoundError:
        print(f"✗ Could not find: {TEAM_CSV}")
        print("  Please update TEAM_CSV path in script")
        team_df = pd.DataFrame()
    
    # Load player data
    print("\n" + "="*70)
    print("LOADING PLAYER DATA")
    print("="*70)
    
    try:
        player_df = pd.read_csv(PLAYER_CSV)
        print(f"✓ Loaded player data: {len(player_df)} rows")
        print(f"  Columns: {list(player_df.columns[:10])}...")
    except FileNotFoundError:
        print(f"✗ Could not find: {PLAYER_CSV}")
        print("  Please update PLAYER_CSV path in script")
        player_df = pd.DataFrame()
    
    # Process team data
    if len(team_df) > 0:
        print("\n" + "="*70)
        print("PROCESSING TEAM DATA")
        print("="*70)
        
        team_df = add_four_factors(team_df)
        team_df = add_opponent_metrics(team_df)
        validate_metrics(team_df, "Team Metrics")
        
        # Save
        output_file = OUTPUT_DIR / "aces_team_box_metrics.csv"
        team_df.to_csv(output_file, index=False)
        print(f"\n✓ Saved: {output_file}")
    
    # Process player data
    if len(player_df) > 0:
        print("\n" + "="*70)
        print("PROCESSING PLAYER DATA")
        print("="*70)
        
        player_df = add_four_factors(player_df)
        # Don't add opponent metrics for player data
        validate_metrics(player_df, "Player Metrics")
        
        # Extract player IDs
        players = extract_player_ids(player_df)
        if len(players) > 0:
            player_id_file = OUTPUT_DIR / "aces_player_ids.csv"
            players.to_csv(player_id_file, index=False)
            print(f"\n✓ Saved player IDs: {player_id_file}")
        
        # Save
        output_file = OUTPUT_DIR / "aces_player_box_metrics.csv"
        player_df.to_csv(output_file, index=False)
        print(f"✓ Saved: {output_file}")
    
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"Team rows processed: {len(team_df)}")
    print(f"Player rows processed: {len(player_df)}")
    print(f"\nOutput directory: {OUTPUT_DIR}")
    print("="*70)
    
    print(f"\nCompleted: {pd.Timestamp.now()}")
    
    print("\n" + "="*70)
    print("NEXT STEPS")
    print("="*70)
    print("""
1. Check output files in data/processed/:
   - aces_team_box_metrics.csv (with Four Factors + efficiency)
   - aces_player_box_metrics.csv (with Four Factors)
   - aces_player_ids.csv (for pbpstats lineup builder)

2. Use player IDs for pbpstats scripts:
   - Top 8-10 players by minutes
   - Build lineup combinations
   - Fetch lineup data from pbpstats

3. Validate metrics:
   - Spot check 3 games against ESPN box scores
   - Verify ORtg/DRtg calculations
   - Check if Four Factors look reasonable
    """)
    print("="*70)


if __name__ == "__main__":
    main()
