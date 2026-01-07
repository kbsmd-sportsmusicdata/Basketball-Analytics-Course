# pbpstats.com API Endpoint Mapper
## Complete Reference for Las Vegas Aces Project

**Key Info**:
- **No API key required** - just use `requests.get()` or `httr::GET()`
- **Base URL**: `https://api.pbpstats.com`
- **Team ID**: Las Vegas Aces = `'1611661319'` (numeric ID) or `'LVA'` (text abbreviation)
- **Season Format**: `'2024-25'` (not `2024` or `2025`)
- **League**: `'wnba'` for all WNBA endpoints

---

## Priority Endpoints for Aces Project

### 🎯 MUST-HAVE (Top 8 Rotation + Lineup Analysis)

#### 1. Get Team Players for Season
**Purpose**: Get all player IDs for Aces roster to identify Top 8 rotation

**Endpoint**: `/get-team-players-for-season`

**Python Example**:
```python
import requests

url = "https://api.pbpstats.com/get-team-players-for-season"
params = {
    "Season": "2024-25",
    "SeasonType": "Regular Season",
    "TeamId": "1611661319"  # Las Vegas Aces
}
response = requests.get(url, params=params)
players = response.json()

# Returns:
# {
#   "players": {
#     "203244": "A'ja Wilson",
#     "202644": "Kelsey Plum",
#     "1628886": "Jackie Young",
#     ...
#   }
# }
```

**Use**: Create list of player IDs to pass to other endpoints

---

#### 2. Get On/Off Stats (Player Impact)
**Purpose**: Identify Top 8 by showing team performance with/without each player

**Endpoint**: `/get-on-off/wnba`

**Python Example**:
```python
url = "https://api.pbpstats.com/get-on-off/wnba"
params = {
    "Season": "2024-25",
    "SeasonType": "Regular Season",
    "TeamId": "1611661319",
    "PlayerId": "203244"  # A'ja Wilson
}
response = requests.get(url, params=params)
on_off = response.json()

# Returns:
# {
#   "results": [
#     {
#       "stat": "OffPoss",
#       "On": 2450,
#       "Off": 890,
#       ...
#     }
#   ]
# }
```

**Use**: Calculate On/Off splits for NetRtg, ORtg, DRtg to identify most impactful players

---

#### 3. Get Four Factor On/Off
**Purpose**: More granular on/off analysis using Four Factors

**Endpoint**: `/get-four-factor-on-off/wnba`

**Python Example**:
```python
url = "https://api.pbpstats.com/get-four-factor-on-off/wnba"
params = {
    "Season": "2024-25",
    "SeasonType": "Regular Season",
    "TeamId": "1611661319",
    "PlayerId": "203244"
}
response = requests.get(url, params=params)
four_factors = response.json()

# Returns:
# {
#   "offense_results": [
#     {"On": 0.52, "Off": 0.48, "stat": "eFG%"},
#     {"On": 0.12, "Off": 0.15, "stat": "TOV%"},
#     ...
#   ],
#   "defense_results": [...]
# }
```

**Use**: Understand HOW players impact team (shooting, turnovers, rebounding, FTs)

---

#### 4. Get Lineup Player Stats
**Purpose**: See stats for specific 5-man lineup combinations

**Endpoint**: `/get-lineup-player-stats/wnba`

**Python Example**:
```python
url = "https://api.pbpstats.com/get-lineup-player-stats/wnba"
params = {
    "Season": "2024-25",
    "SeasonType": "Regular Season",
    "TeamId": "1611661319",
    "LineupId": "203244-202644-1628886-203497-1629011"  # Wilson-Plum-Young-Gray-Stokes
}
response = requests.get(url, params=params)
lineup_stats = response.json()

# Returns player-level stats for this specific lineup
```

**Use**: Analyze how individual players perform in different lineup contexts

---

#### 5. Get Totals (Season Aggregates)
**Purpose**: Get season totals for team or individual players (base for Top 8 identification)

**Endpoint**: `/get-totals/wnba`

**Python Example**:
```python
url = "https://api.pbpstats.com/get-totals/wnba"
params = {
    "Season": "2024-25",
    "SeasonType": "Regular Season",
    "EntityId": "1611661319",  # Team or Player ID
    "EntityType": "Team"  # or "Player"
}
response = requests.get(url, params=params)
totals = response.json()

# Returns:
# {
#   "results": [
#     {"stat": "FGM", "value": 850},
#     {"stat": "FGA", "value": 1800},
#     {"stat": "eFG%", "value": 0.52},
#     ...
#   ]
# }
```

**Use**: Get aggregated stats to filter by minutes/usage for Top 8

---

### 🎲 NICE-TO-HAVE (Advanced Analysis)

#### 6. Get Shots (Shot Chart Data)
**Purpose**: Shot location and efficiency by zone

**Endpoint**: `/get-shots/wnba`

**Python Example**:
```python
url = "https://api.pbpstats.com/get-shots/wnba"
params = {
    "Season": "2024-25",
    "SeasonType": "Regular Season",
    "EntityId": "1611661319",
    "EntityType": "Team"
}
response = requests.get(url, params=params)
shots = response.json()

# Returns individual shot data with x, y coordinates
```

**Use**: Create shot charts, analyze shot selection by player/lineup

---

#### 7. Get WOWY Stats (With or Without You)
**Purpose**: 2-player combination analysis

**Endpoint**: `/get-wowy-stats/wnba`

**Python Example**:
```python
url = "https://api.pbpstats.com/get-wowy-stats/wnba"
params = {
    "Season": "2024-25",
    "SeasonType": "Regular Season",
    "TeamId": "1611661319",
    "PlayerId1": "203244",  # A'ja Wilson
    "PlayerId2": "202644"   # Kelsey Plum
}
response = requests.get(url, params=params)
wowy = response.json()

# Returns:
# - Both On
# - Player1 On, Player2 Off
# - Player1 Off, Player2 On
# - Both Off
```

**Use**: Find best player combinations for lineup building

---

#### 8. Get Lineup Opponent Summary
**Purpose**: How lineups perform against different opponent styles

**Endpoint**: `/get-lineup-opponent-summary/wnba`

**Python Example**:
```python
url = "https://api.pbpstats.com/get-lineup-opponent-summary/wnba"
params = {
    "Season": "2024-25",
    "SeasonType": "Regular Season",
    "TeamId": "1611661319",
    "LineupId": "203244-202644-1628886-203497-1629011"
}
response = requests.get(url, params=params)
opp_summary = response.json()
```

**Use**: Game-specific adjustments section of academic project

---

#### 9. Get Game Logs
**Purpose**: Game-by-game performance for player or team

**Endpoint**: `/get-game-logs/wnba`

**Python Example**:
```python
url = "https://api.pbpstats.com/get-game-logs/wnba"
params = {
    "Season": "2024-25",
    "SeasonType": "Regular Season",
    "EntityId": "203244",  # A'ja Wilson
    "EntityType": "Player"
}
response = requests.get(url, params=params)
game_logs = response.json()

# Returns game-by-game stats
```

**Use**: Track player consistency, identify trends over season

---

#### 10. Get Pace & Efficiency Summary
**Purpose**: Team tempo and style characteristics

**Endpoint**: `/get-pace-efficiency-summary/wnba`

**Python Example**:
```python
url = "https://api.pbpstats.com/get-pace-efficiency-summary/wnba"
params = {
    "Season": "2024-25",
    "SeasonType": "Regular Season",
    "TeamId": "1611661319"
}
response = requests.get(url, params=params)
pace_eff = response.json()
```

**Use**: Understand Aces' style (fast/slow, efficient/inefficient)

---

## Complete Endpoint List (Reference)

Based on `dblackrun/pbpstats-api-code-examples` GitHub repo:

| Endpoint | Use Case | Priority |
|----------|----------|----------|
| `/get-team-players-for-season` | Get roster player IDs | ⭐⭐⭐ |
| `/get-on-off/wnba` | Player impact analysis | ⭐⭐⭐ |
| `/get-four-factor-on-off/wnba` | Four Factors on/off | ⭐⭐⭐ |
| `/get-lineup-player-stats/wnba` | Lineup combinations | ⭐⭐⭐ |
| `/get-totals/wnba` | Season aggregates | ⭐⭐⭐ |
| `/get-shots/wnba` | Shot chart data | ⭐⭐ |
| `/get-wowy-stats/wnba` | 2-player combinations | ⭐⭐ |
| `/get-lineup-opponent-summary/wnba` | Matchup analysis | ⭐⭐ |
| `/get-game-logs/wnba` | Game-by-game tracking | ⭐⭐ |
| `/get-pace-efficiency-summary/wnba` | Team style/tempo | ⭐⭐ |
| `/get-games/wnba` | List of all games in season | ⭐ |
| `/get-game-stats/wnba` | Single game box score | ⭐ |
| `/get-playing-time-distribution/wnba` | Rotation patterns | ⭐ |
| `/get-assist-networks/wnba` | Passing connections | ⭐ |
| `/get-shot-query-summary/wnba` | Filtered shot aggregates | ⭐ |
| `/get-lineup-subunit-stats/wnba` | 2/3/4-man lineup stats | ⭐ |
| `/get-score-margin-breakdown/wnba` | Performance by score state | ⭐ |
| `/get-leverage/wnba` | Win probability added | ⭐ |
| `/get-possessions/wnba` | Play-by-play possessions | ⭐ |

---

## Recommended Data Pull Sequence

### Step 1: Get Player IDs (Foundation)
```python
import requests
import pandas as pd

# Get all Aces players for 2024-25
players_url = "https://api.pbpstats.com/get-team-players-for-season"
players_params = {
    "Season": "2024-25",
    "SeasonType": "Regular Season",
    "TeamId": "1611661319"
}
players_response = requests.get(players_url, params=players_params)
players_dict = players_response.json()["players"]

# Convert to DataFrame
players_df = pd.DataFrame([
    {"player_id": pid, "player_name": name}
    for pid, name in players_dict.items()
])

print(f"Found {len(players_df)} players on 2024-25 Aces roster")
```

---

### Step 2: Get Season Totals for All Players
```python
# Loop through all players to get their totals
all_player_totals = []

for player_id in players_df["player_id"]:
    totals_url = "https://api.pbpstats.com/get-totals/wnba"
    totals_params = {
        "Season": "2024-25",
        "SeasonType": "Regular Season",
        "EntityId": player_id,
        "EntityType": "Player"
    }
    totals_response = requests.get(totals_url, params=totals_params)
    totals_data = totals_response.json()
    
    # Convert list of {"stat": "X", "value": Y} to flat dict
    totals_dict = {"player_id": player_id}
    for item in totals_data.get("results", []):
        totals_dict[item["stat"]] = item["value"]
    
    all_player_totals.append(totals_dict)

totals_df = pd.DataFrame(all_player_totals)

# Merge with player names
totals_df = totals_df.merge(players_df, on="player_id")

# Filter to Top 8 by minutes played
top_8 = totals_df.nlargest(8, "Minutes")
print(top_8[["player_name", "Minutes", "OffPoss", "ORtg", "DRtg"]])
```

---

### Step 3: Get On/Off Stats for Top 8
```python
top_8_on_off = []

for player_id in top_8["player_id"]:
    on_off_url = "https://api.pbpstats.com/get-on-off/wnba"
    on_off_params = {
        "Season": "2024-25",
        "SeasonType": "Regular Season",
        "TeamId": "1611661319",
        "PlayerId": player_id
    }
    on_off_response = requests.get(on_off_url, params=on_off_params)
    on_off_data = on_off_response.json()
    
    # Extract NetRtg on/off
    for item in on_off_data.get("results", []):
        if item["stat"] == "NetRtg":
            top_8_on_off.append({
                "player_id": player_id,
                "net_rtg_on": item["On"],
                "net_rtg_off": item["Off"],
                "net_rtg_diff": item["On"] - item["Off"]
            })

on_off_df = pd.DataFrame(top_8_on_off)
on_off_df = on_off_df.merge(players_df, on="player_id")

print(on_off_df.sort_values("net_rtg_diff", ascending=False))
```

---

### Step 4: Get Lineup Stats (Top 5-Man Combos)
```python
# Get all lineup combinations for team
lineups_url = "https://api.pbpstats.com/get-lineup-player-stats/wnba"
# Note: You may need to iterate through known lineups
# or use /get-totals with EntityType="Lineup" to get all lineup IDs first

# Example for known starting lineup:
lineup_id = "-".join(sorted([str(pid) for pid in top_8["player_id"][:5]]))

lineups_params = {
    "Season": "2024-25",
    "SeasonType": "Regular Season",
    "TeamId": "1611661319",
    "LineupId": lineup_id
}
lineups_response = requests.get(lineups_url, params=lineups_params)
lineup_stats = lineups_response.json()

print(f"Lineup {lineup_id} stats:")
print(lineup_stats)
```

---

## Common Parameters Across Endpoints

| Parameter | Required? | Options | Description |
|-----------|-----------|---------|-------------|
| `Season` | Yes | `"2024-25"`, `"2023-24"` | Season in YYYY-YY format |
| `SeasonType` | Yes | `"Regular Season"`, `"Playoffs"` | Type of season |
| `TeamId` | Usually | `"1611661319"` (Aces) | Numeric team ID |
| `PlayerId` | Sometimes | `"203244"` (A'ja Wilson) | Numeric player ID |
| `EntityId` | Sometimes | Team or Player ID | When using generic entity endpoints |
| `EntityType` | Sometimes | `"Team"`, `"Player"`, `"Lineup"` | Type of entity |
| `LineupId` | Sometimes | `"203244-202644-1628886"` | Hyphen-separated player IDs (sorted) |

---

## Response Structure Patterns

Most pbpstats endpoints return JSON in this format:

```json
{
  "results": [
    {
      "stat": "StatName",
      "value": 123.45,
      "On": 110.5,
      "Off": 98.3,
      ...
    }
  ],
  "players": {
    "player_id": "Player Name"
  },
  "offense_results": [...],
  "defense_results": [...]
}
```

**Key patterns**:
- `results` array contains stat objects
- `players` dict maps IDs to names
- Separate `offense_results` and `defense_results` for Four Factors
- On/Off stats have `On` and `Off` keys

---

## R Implementation Template

```r
library(httr)
library(jsonlite)
library(dplyr)

# Get team players
get_aces_players <- function(season = "2024-25") {
  url <- "https://api.pbpstats.com/get-team-players-for-season"
  
  response <- GET(
    url,
    query = list(
      Season = season,
      SeasonType = "Regular Season",
      TeamId = "1611661319"
    )
  )
  
  data <- content(response, as = "parsed")
  
  # Convert to data frame
  players_df <- data.frame(
    player_id = names(data$players),
    player_name = unlist(data$players),
    stringsAsFactors = FALSE
  )
  
  return(players_df)
}

# Get player totals
get_player_totals <- function(player_id, season = "2024-25") {
  url <- "https://api.pbpstats.com/get-totals/wnba"
  
  response <- GET(
    url,
    query = list(
      Season = season,
      SeasonType = "Regular Season",
      EntityId = player_id,
      EntityType = "Player"
    )
  )
  
  data <- content(response, as = "parsed")
  
  # Convert results list to named vector
  totals <- sapply(data$results, function(x) x$value)
  names(totals) <- sapply(data$results, function(x) x$stat)
  
  return(as.list(totals))
}

# Usage
players <- get_aces_players()
print(players)

# Get totals for first player
totals <- get_player_totals(players$player_id[1])
print(totals)
```

---

## Next Steps for Your Project

1. **Run Step 1**: Get all 2024-25 Aces player IDs
2. **Run Step 2**: Get season totals, filter to Top 8 by minutes
3. **Run Step 3**: Get On/Off stats for Top 8 to quantify impact
4. **Save outputs**: Store in `data/master/aces_players_pbpstats.parquet`
5. **Integrate with wehoop**: Merge pbpstats player IDs with wehoop data
6. **Build lineup analysis**: Use lineup endpoints to analyze combinations
7. **Create opponent profiles**: Use opponent summary endpoints for matchup analysis

---

## Troubleshooting

**Issue**: Empty results returned  
**Solution**: Check Season format is `"2024-25"` not `"2024"` or `2025`

**Issue**: Player ID not recognized  
**Solution**: Use numeric ID (`"203244"`) not name or abbreviation

**Issue**: Lineup ID not working  
**Solution**: Ensure player IDs are sorted alphabetically as strings before joining with `-`

**Issue**: Rate limiting  
**Solution**: Add 0.5-1 second delay between requests (`time.sleep(0.5)` in Python or `Sys.sleep(0.5)` in R)

---

## Resources

- **GitHub Examples**: https://github.com/dblackrun/pbpstats-api-code-examples
- **API Docs**: https://api.pbpstats.com/docs (may have limited info)
- **Support**: Create issue on GitHub repo if endpoints don't work as expected
