# Las Vegas Aces Master Dataset - Data Dictionary

## Overview
This master dataset combines **wehoop WNBA data** (box scores, team stats) with **pbpstats.com advanced metrics** (lineup data, shot location) to create a unified analytical foundation for both academic and portfolio projects.

**Data Overlap**: ~85% of raw data is shared between projects; divergence occurs primarily in analysis focus and presentation.

---

## Dataset Structure

```
data/
├── raw/                           # Original API responses (archived)
│   ├── wehoop_team_box_2024.parquet
│   ├── wehoop_team_box_2025.parquet
│   ├── wehoop_player_box_2024.parquet
│   ├── wehoop_player_box_2025.parquet
│   ├── pbpstats_lineups_2024.json
│   └── pbpstats_shots_2024.json
│
├── master/                        # Cleaned, unified datasets (80% overlap)
│   ├── aces_team_box.parquet      # Team-level box scores + Four Factors
│   ├── aces_player_box.parquet    # Player-level box scores + efficiency
│   ├── aces_schedule.parquet      # Game metadata
│   ├── aces_lineups.parquet       # 5-man lineup combinations
│   ├── aces_shots.parquet         # Shot location & outcomes
│   └── metadata.json              # Build timestamp, data sources
│
├── processed/
│   ├── academic/                  # Final project-specific transformations
│   │   ├── rotation_analysis.parquet
│   │   ├── opponent_profiles.parquet
│   │   └── game_scripts.parquet
│   │
│   └── portfolio/                 # Case study-specific transformations
│       ├── multi_year_trends.parquet
│       ├── championship_indicators.parquet
│       └── roster_composition.parquet
│
└── tableau/                       # Export-ready for visualization
    ├── academic_dashboard_source.hyper
    └── portfolio_dashboard_source.hyper
```

---

## Master Datasets

### 1. `aces_team_box.parquet`
**Source**: wehoop WNBA team box scores  
**Grain**: One row per team per game  
**Records**: ~80 games (2024 season: 40 games, 2025 season: ~40 games projected)

#### Columns

**Identifiers**
| Column | Type | Description |
|--------|------|-------------|
| `season` | int | Season year (2024, 2025) |
| `game_id` | string | Unique game identifier |
| `game_date` | date | Date of game |
| `team_id` | string | Las Vegas Aces team ID ("16") |
| `team_name` | string | "Las Vegas Aces" |
| `opponent_id` | string | Opponent team ID |
| `opponent_name` | string | Opponent team name |
| `home_away` | string | "home" or "away" |
| `win_loss` | string | "W" or "L" |

**Raw Box Score Stats** (wehoop direct)
| Column | Type | Description |
|--------|------|-------------|
| `pts` | int | Points scored |
| `fgm` | int | Field goals made |
| `fga` | int | Field goals attempted |
| `fg3m` | int | 3-pointers made |
| `fg3a` | int | 3-pointers attempted |
| `ftm` | int | Free throws made |
| `fta` | int | Free throws attempted |
| `orb` | int | Offensive rebounds |
| `drb` | int | Defensive rebounds |
| `ast` | int | Assists |
| `stl` | int | Steals |
| `blk` | int | Blocks |
| `tov` | int | Turnovers |

**Calculated Metrics** (Dean Oliver formulas)
| Column | Type | Formula | Description |
|--------|------|---------|-------------|
| `poss_est` | float | `FGA + 0.44*FTA - ORB + TOV` | Estimated possessions |
| `efg_pct` | float | `(FGM + 0.5*3PM) / FGA` | Effective field goal % |
| `ts_pct` | float | `PTS / (2*(FGA + 0.44*FTA))` | True shooting % |
| `tov_pct` | float | `TOV / Poss` | Turnover rate |
| `ftr` | float | `FTA / FGA` | Free throw rate |
| `oreb_pct` | float | `ORB / (ORB + Opp_DRB)` | Offensive rebound % |
| `dreb_pct` | float | `DRB / (DRB + Opp_ORB)` | Defensive rebound % |
| `ortg` | float | `100 * PTS / Poss` | Offensive rating (per 100 poss) |
| `drtg` | float | `100 * Opp_PTS / Opp_Poss` | Defensive rating (per 100 poss) |
| `net_rtg` | float | `ORtg - DRtg` | Net rating |
| `fg3ar` | float | `3PA / FGA` | 3-point attempt rate |
| `ast_rate` | float | `AST / FGM` | Assist rate |
| `ast_tov` | float | `AST / TOV` | Assist-to-turnover ratio |

**Opponent Stats** (mirror of above for opponent team)
| Column | Type | Description |
|--------|------|-------------|
| `opp_pts` | int | Opponent points |
| `opp_poss_est` | float | Opponent possessions |
| `opp_efg_pct` | float | Opponent eFG% |
| ... | ... | (All opponent versions of calculated metrics) |

---

### 2. `aces_player_box.parquet`
**Source**: wehoop WNBA player box scores  
**Grain**: One row per player per game  
**Records**: ~960 records (12 active roster × 80 games)

#### Columns

**Identifiers**
| Column | Type | Description |
|--------|------|-------------|
| `season` | int | Season year |
| `game_id` | string | Unique game identifier |
| `game_date` | date | Date of game |
| `player_id` | string | Unique player identifier |
| `player_name` | string | Player full name |
| `team_id` | string | Team ID |
| `position` | string | Listed position (G, F, C) |
| `starter` | boolean | Started the game? |
| `minutes` | float | Minutes played |

**Raw Box Score Stats**
| Column | Type | Description |
|--------|------|-------------|
| `pts` | int | Points |
| `fgm` | int | Field goals made |
| `fga` | int | Field goals attempted |
| `fg3m` | int | 3-pointers made |
| `fg3a` | int | 3-pointers attempted |
| `ftm` | int | Free throws made |
| `fta` | int | Free throws attempted |
| `orb` | int | Offensive rebounds |
| `drb` | int | Defensive rebounds |
| `ast` | int | Assists |
| `stl` | int | Steals |
| `blk` | int | Blocks |
| `tov` | int | Turnovers |
| `fouls` | int | Personal fouls |
| `plus_minus` | int | Plus/minus |

**Calculated Metrics** (same formulas as team-level)
| Column | Type | Description |
|--------|------|-------------|
| `ts_pct` | float | True shooting % |
| `efg_pct` | float | Effective field goal % |
| `usg_pct` | float | Usage rate (estimated) |
| `ast_rate` | float | Assist rate |
| `tov_pct` | float | Turnover rate |
| `oreb_pct` | float | Offensive rebound % |
| `dreb_pct` | float | Defensive rebound % |

**Role Indicators** (for Top 8 rotation identification)
| Column | Type | Description |
|--------|------|-------------|
| `role_primary_handler` | float | Primary ball-handler score |
| `role_connector` | float | Connector/wing score |
| `role_finisher` | float | Finisher/scorer score |
| `role_rim_protector` | float | Rim protector/big score |
| `primary_role` | string | Assigned primary role |

---

### 3. `aces_lineups.parquet`
**Source**: pbpstats.com lineup data  
**Grain**: One row per 5-man lineup per game  
**Records**: ~200-300 unique lineup combinations across season

#### Columns

| Column | Type | Description |
|--------|------|-------------|
| `season` | int | Season year |
| `game_id` | string | Game identifier |
| `team_id` | string | Team ID |
| `lineup_id` | string | Unique lineup combo ID |
| `player_1` | string | Player 1 ID |
| `player_2` | string | Player 2 ID |
| `player_3` | string | Player 3 ID |
| `player_4` | string | Player 4 ID |
| `player_5` | string | Player 5 ID |
| `minutes` | float | Minutes this lineup played together |
| `possessions` | int | Possessions with this lineup |
| `plus_minus` | int | Point differential |
| `ortg_lineup` | float | Offensive rating for lineup |
| `drtg_lineup` | float | Defensive rating for lineup |
| `net_rtg_lineup` | float | Net rating for lineup |
| `pts_for` | int | Points scored by lineup |
| `pts_against` | int | Points allowed by lineup |

**Lineup Identifiers** (derived)
| Column | Type | Description |
|--------|------|-------------|
| `is_starting_lineup` | boolean | Is this the starting 5? |
| `lineup_frequency` | int | Times this exact 5 played together |
| `lineup_continuity` | float | Measure of lineup stability |

---

### 4. `aces_shots.parquet`
**Source**: pbpstats.com shot location data  
**Grain**: One row per shot attempt  
**Records**: ~6,000 shots (75 FGA/game × 80 games)

#### Columns

| Column | Type | Description |
|--------|------|-------------|
| `season` | int | Season year |
| `game_id` | string | Game identifier |
| `team_id` | string | Team ID |
| `player_id` | string | Shooter ID |
| `player_name` | string | Shooter name |
| `shot_id` | string | Unique shot identifier |
| `x_coord` | float | X coordinate on court (-250 to 250) |
| `y_coord` | float | Y coordinate on court (0 to 940) |
| `shot_distance` | float | Distance from basket (feet) |
| `shot_zone` | string | Zone classification |
| `shot_made` | boolean | Made or missed |
| `shot_value` | int | 2 or 3 points |
| `quarter` | int | Quarter (1-4, 5=OT) |
| `time_remaining` | float | Seconds left in quarter |

**Shot Zones** (classification)
- `restricted_area` - Within 4 feet of basket
- `paint_non_ra` - Paint but outside restricted area
- `midrange` - 2-point shots outside paint
- `corner_3` - 3-pointers from corners
- `above_break_3` - 3-pointers above the break

**Calculated Shot Quality** (derived)
| Column | Type | Description |
|--------|------|-------------|
| `expected_value` | float | Points per attempt based on zone |
| `shot_quality` | string | "Elite", "Good", "Average", "Poor" |

---

### 5. `aces_schedule.parquet`
**Source**: wehoop WNBA schedule  
**Grain**: One row per game  
**Records**: ~80 games

#### Columns

| Column | Type | Description |
|--------|------|-------------|
| `season` | int | Season year |
| `game_id` | string | Game identifier |
| `game_date` | date | Date of game |
| `game_time` | time | Tip-off time |
| `home_team_id` | string | Home team ID |
| `home_team_name` | string | Home team name |
| `away_team_id` | string | Away team ID |
| `away_team_name` | string | Away team name |
| `venue_name` | string | Arena name |
| `attendance` | int | Attendance count |
| `broadcast` | string | TV network |

---

## Data Quality Notes

### Missing Data Handling
- **Player stats**: Players with DNP (Did Not Play) have 0 minutes, null for all other stats
- **Lineup data**: Minimum possession threshold (10 poss) for reliable lineup metrics
- **Shot location**: Some games may have incomplete shot tracking

### Data Updates
- **wehoop**: Updated weekly during season (pulled from ESPN)
- **pbpstats.com**: Updated daily during season (requires API subscription)
- **Master rebuild**: Run `aces_master_dataset_builder.py` to refresh

### Known Limitations
- Possessions are **estimated** using Dean Oliver formula (not play-by-play exact)
- Lineup data may not capture all substitution patterns
- Shot coordinates normalized to standard court dimensions

---

## Metric Definitions Reference

All metrics follow **Dean Oliver's Basketball on Paper** formulas and are consistent with industry-standard analytics practices used in NBA/WNBA front offices.

### Four Factors (Priority Order)
1. **Shooting (eFG%)**: Efficiency of shot-making
2. **Turnovers (TOV%)**: Ball security
3. **Rebounding (OREB%, DREB%)**: Second-chance opportunities
4. **Free Throws (FT Rate)**: Ability to get to the line

### Efficiency Ratings
- **Offensive Rating (ORtg)**: Points scored per 100 possessions
- **Defensive Rating (DRtg)**: Points allowed per 100 possessions  
- **Net Rating**: ORtg - DRtg (positive = good, negative = bad)

### Usage & Role Metrics
- **Usage %**: Player's share of team possessions while on court
- **Role Scores**: Composite metrics identifying player archetypes

---

## File Formats

**Parquet**: All master datasets use Apache Parquet format for:
- Efficient compression (~10x smaller than CSV)
- Fast columnar queries
- Native Tableau Hyper compatibility

**JSON**: Metadata files for build tracking

**Hyper**: Tableau-optimized extracts for dashboard performance

---

## Usage Examples

### Loading Master Dataset (Python)
```python
import pandas as pd

# Load team box scores
team_df = pd.read_parquet('data/master/aces_team_box.parquet')

# Filter to 2024 season only
df_2024 = team_df[team_df['season'] == 2024]

# Calculate season averages
season_avg = df_2024.groupby('season').agg({
    'ortg': 'mean',
    'drtg': 'mean',
    'efg_pct': 'mean',
    'tov_pct': 'mean'
})
```

### Loading for Tableau
```python
from pathlib import Path
import tableauhyperapi as hyper

# Export to Hyper format
df = pd.read_parquet('data/master/aces_team_box.parquet')
hyper_file = Path('data/tableau/aces_team_box.hyper')

# Create Hyper extract (see Tableau docs for full implementation)
```

---

## Changelog

**2026-01-06**: Initial master dataset structure defined  
**Pending**: pbpstats.com API integration, academic/portfolio subset creation
