"""
Las Vegas Aces - Identify Top 8 Rotation (2025)
================================================
Filters raw wehoop parquet files to Las Vegas Aces only.
Identifies Top 8 players by total minutes for player profiles.

Author: Krystal
Last Updated: 2026-01-07
"""

import pandas as pd
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

# Input paths - UPDATE THESE to your actual parquet file locations
PLAYER_BOX_PARQUET = "kbsmd-sportsmusicdata/Basketball-Analytics-Course/ba final_lva2025/player_box_2025.parquet"  # Your raw wehoop player box scores
TEAM_BOX_PARQUET = "kbsmd-sportsmusicdata/Basketball-Analytics-Course/ba final_lva2025/team_box_2025.parquet"  # Your raw wehoop team box scores (optional)

# Output paths
OUTPUT_DIR = Path("data/processed")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Team filter - Las Vegas Aces
# wehoop uses different team identifiers - adjust as needed
LVA_TEAM_IDS = ["16"]  # wehoop team_id for Las Vegas Aces
LVA_TEAM_NAMES = ["Las Vegas Aces", "LV Aces", "LVA", "Las Vegas"]

print("="*70)
print("LAS VEGAS ACES - TOP 8 ROTATION IDENTIFIER")
print("="*70)
print(f"Input: {PLAYER_BOX_PARQUET}")
print(f"Team: Las Vegas Aces")
print(f"Season: 2025")
print("="*70)
print()


# ============================================================================
# LOAD AND FILTER DATA
# ============================================================================

print("[STEP 1] Loading raw player box scores...")

try:
    # Load parquet file
    df = pd.read_parquet(PLAYER_BOX_PARQUET)
    print(f"✓ Loaded {len(df):,} rows")
    print(f"  Columns: {list(df.columns[:15])}...")
    
    # Show unique teams to identify correct filter
    print(f"\n  Checking team identifiers in data...")
    if 'team_id' in df.columns:
        unique_teams = df['team_id'].unique()
        print(f"  Unique team_ids: {sorted(unique_teams)}")
    
    if 'team_display_name' in df.columns:
        unique_team_names = df['team_display_name'].unique()
        print(f"  Unique team names: {sorted(unique_team_names)[:10]}...")
    
except FileNotFoundError:
    print(f"✗ Could not find: {PLAYER_BOX_PARQUET}")
    print("  Please update PLAYER_BOX_PARQUET path in script")
    exit(1)


# ============================================================================
# FILTER TO LAS VEGAS ACES
# ============================================================================

print("\n[STEP 2] Filtering to Las Vegas Aces...")

# Try multiple filter approaches
df_lva = pd.DataFrame()

# Approach 1: Filter by team_id
if 'team_id' in df.columns:
    df_lva = df[df['team_id'].isin(LVA_TEAM_IDS)]
    print(f"  → By team_id: {len(df_lva):,} rows")

# Approach 2: Filter by team name (if team_id didn't work)
if len(df_lva) == 0 and 'team_display_name' in df.columns:
    df_lva = df[df['team_display_name'].isin(LVA_TEAM_NAMES)]
    print(f"  → By team_display_name: {len(df_lva):,} rows")

# Approach 3: Try partial match on team name
if len(df_lva) == 0 and 'team_display_name' in df.columns:
    df_lva = df[df['team_display_name'].str.contains('Vegas|Aces|LV', case=False, na=False)]
    print(f"  → By partial match: {len(df_lva):,} rows")

# Approach 4: Try abbreviation column
if len(df_lva) == 0 and 'team_abbreviation' in df.columns:
    df_lva = df[df['team_abbreviation'].isin(['LV', 'LVA'])]
    print(f"  → By team_abbreviation: {len(df_lva):,} rows")

if len(df_lva) == 0:
    print(f"\n✗ Could not filter to Las Vegas Aces!")
    print(f"  Please check team identifier columns and update LVA_TEAM_IDS or LVA_TEAM_NAMES")
    print(f"\n  Available team columns:")
    team_cols = [col for col in df.columns if 'team' in col.lower()]
    print(f"  {team_cols}")
    exit(1)

print(f"\n✓ Filtered to {len(df_lva):,} Las Vegas Aces player-game rows")

# Show sample
if 'athlete_display_name' in df_lva.columns:
    print(f"\n  Sample players:")
    sample_players = df_lva['athlete_display_name'].value_counts().head(5)
    print(sample_players)


# ============================================================================
# SAVE FILTERED DATA
# ============================================================================

print("\n[STEP 3] Saving filtered data...")

output_file = OUTPUT_DIR / "aces_player_box_2025.csv"
df_lva.to_csv(output_file, index=False)
print(f"✓ Saved: {output_file}")
print(f"  Rows: {len(df_lva):,}")
print(f"  Columns: {len(df_lva.columns)}")


# ============================================================================
# IDENTIFY TOP 8 ROTATION
# ============================================================================

print("\n[STEP 4] Identifying Top 8 rotation players...")

# Determine player identifier columns
player_id_col = None
player_name_col = None

for col in ['athlete_id', 'player_id', 'id']:
    if col in df_lva.columns:
        player_id_col = col
        break

for col in ['athlete_display_name', 'player_name', 'name']:
    if col in df_lva.columns:
        player_name_col = col
        break

if not player_id_col or not player_name_col:
    print(f"✗ Could not find player ID or name columns")
    print(f"  Available columns: {list(df_lva.columns)}")
    exit(1)

print(f"  Using: {player_id_col} and {player_name_col}")

# Determine minutes column
minutes_col = None
for col in ['minutes', 'min', 'mp']:
    if col in df_lva.columns:
        minutes_col = col
        break

if not minutes_col:
    print(f"✗ Could not find minutes column")
    exit(1)

# Convert minutes to numeric (might be "MM:SS" format)
if df_lva[minutes_col].dtype == 'object':
    print(f"  Converting minutes from MM:SS format...")
    try:
        df_lva['minutes_decimal'] = df_lva[minutes_col].apply(
            lambda x: int(str(x).split(':')[0]) + int(str(x).split(':')[1])/60 
            if isinstance(x, str) and ':' in str(x) 
            else float(x) if pd.notna(x) else 0
        )
    except:
        df_lva['minutes_decimal'] = pd.to_numeric(df_lva[minutes_col], errors='coerce').fillna(0)
else:
    df_lva['minutes_decimal'] = pd.to_numeric(df_lva[minutes_col], errors='coerce').fillna(0)

# Aggregate by player
player_summary = df_lva.groupby([player_id_col, player_name_col]).agg({
    'minutes_decimal': 'sum',
    'game_id': 'nunique'  # Games played
}).reset_index()

player_summary.columns = ['player_id', 'player_name', 'total_minutes', 'games']

# Calculate minutes per game
player_summary['mpg'] = player_summary['total_minutes'] / player_summary['games']

# Sort by total minutes
player_summary = player_summary.sort_values('total_minutes', ascending=False)

# Get Top 8
top_8 = player_summary.head(8).copy()

print(f"\n✓ Top 8 rotation by total minutes:")
print("="*70)
print(top_8.to_string(index=False))
print("="*70)

# Save Top 8 list
top_8_file = OUTPUT_DIR / "aces_top8_rotation_2025.csv"
top_8.to_csv(top_8_file, index=False)
print(f"\n✓ Saved Top 8 list: {top_8_file}")

# Also save full player summary
summary_file = OUTPUT_DIR / "aces_player_summary_2025.csv"
player_summary.to_csv(summary_file, index=False)
print(f"✓ Saved full player summary: {summary_file}")


# ============================================================================
# ADDITIONAL STATS FOR TOP 8
# ============================================================================

print("\n[STEP 5] Calculating detailed stats for Top 8...")

# Get detailed stats for top 8 players
top_8_ids = top_8['player_id'].tolist()
df_top8 = df_lva[df_lva[player_id_col].isin(top_8_ids)].copy()

# Determine available stat columns
stat_cols = []
for col in ['field_goals_made', 'fgm', 'field_goals_attempted', 'fga',
            'three_point_field_goals_made', 'fg3m', 'three_point_field_goals_attempted', 'fg3a',
            'free_throws_made', 'ftm', 'free_throws_attempted', 'fta',
            'points', 'pts', 'assists', 'ast', 'rebounds', 'reb',
            'offensive_rebounds', 'orb', 'defensive_rebounds', 'drb',
            'steals', 'stl', 'blocks', 'blk', 'turnovers', 'tov']:
    if col in df_top8.columns:
        stat_cols.append(col)

print(f"  Found {len(stat_cols)} stat columns")

# Aggregate detailed stats
agg_dict = {'minutes_decimal': 'sum', 'game_id': 'nunique'}
for col in stat_cols:
    if col in df_top8.columns:
        agg_dict[col] = 'sum'

detailed_stats = df_top8.groupby([player_id_col, player_name_col]).agg(agg_dict).reset_index()

# Calculate per-game averages
for col in stat_cols:
    if col in detailed_stats.columns and col not in ['game_id', 'minutes_decimal']:
        detailed_stats[f'{col}_pg'] = detailed_stats[col] / detailed_stats['game_id']

# Calculate shooting percentages
if 'field_goals_made' in detailed_stats.columns and 'field_goals_attempted' in detailed_stats.columns:
    detailed_stats['fg_pct'] = detailed_stats['field_goals_made'] / detailed_stats['field_goals_attempted']
elif 'fgm' in detailed_stats.columns and 'fga' in detailed_stats.columns:
    detailed_stats['fg_pct'] = detailed_stats['fgm'] / detailed_stats['fga']

if 'three_point_field_goals_made' in detailed_stats.columns and 'three_point_field_goals_attempted' in detailed_stats.columns:
    detailed_stats['fg3_pct'] = detailed_stats['three_point_field_goals_made'] / detailed_stats['three_point_field_goals_attempted']
elif 'fg3m' in detailed_stats.columns and 'fg3a' in detailed_stats.columns:
    detailed_stats['fg3_pct'] = detailed_stats['fg3m'] / detailed_stats['fg3a']

# Sort by minutes
detailed_stats = detailed_stats.sort_values('minutes_decimal', ascending=False)

# Save
detailed_file = OUTPUT_DIR / "aces_top8_detailed_stats_2025.csv"
detailed_stats.to_csv(detailed_file, index=False)
print(f"✓ Saved detailed stats: {detailed_file}")

# Show summary
print(f"\nTop 8 Summary:")
display_cols = [player_name_col, 'game_id', 'minutes_decimal']
if 'points' in detailed_stats.columns:
    display_cols.append('points')
if 'pts_pg' in detailed_stats.columns:
    display_cols.append('pts_pg')
if 'fg_pct' in detailed_stats.columns:
    display_cols.append('fg_pct')

print(detailed_stats[display_cols].head(8).to_string(index=False))


# ============================================================================
# NEXT STEPS
# ============================================================================

print("\n" + "="*70)
print("NEXT STEPS")
print("="*70)
print("""
1. Manual On/Off Data Collection (10 min):
   For each of the Top 8 players, visit:
   https://www.pbpstats.com/on-off/wnba/player?Season=2024-25&SeasonType=Regular+Season&TeamId=1611661319&PlayerId=XXXXXX
   
   Record:
   - Minutes On/Off
   - ORtg On/Off (+ differential)
   - DRtg On/Off (+ differential)
   - NetRtg On/Off (+ differential)
   
   Save as: aces_onoff_2025_manual.csv

2. Run calculate_metrics_from_csv.py:
   - Loads aces_player_box_2025.csv
   - Calculates Four Factors, efficiency metrics
   - Outputs: aces_player_box_metrics_2025.csv

3. Merge everything:
   - Combine box score metrics + on/off splits
   - Create final player profiles dataset

OUTPUT FILES CREATED:
- aces_player_box_2025.csv (filtered raw data)
- aces_top8_rotation_2025.csv (Top 8 list)
- aces_player_summary_2025.csv (all players summary)
- aces_top8_detailed_stats_2025.csv (Top 8 with detailed stats)

TOP 8 PLAYER IDS FOR PBPSTATS:
""")

for _, row in top_8.iterrows():
    print(f"  {row['player_name']}: {row['player_id']}")

print(f"\nCompleted: {pd.Timestamp.now()}")
print("="*70)