# Poker Analyzer - Implementation Plan

## Overview

This document outlines the step-by-step implementation plan. Each phase builds on the previous one. We'll work through these phases one at a time.

---

## Phase 0: Prerequisites (Outside poker_analyzer)

### 0.1 JSON/HH Uploader Service

**Location:** `C:\ai_game_analyst\data_uploader\` (new folder, separate from poker_analyzer)

**Goal:** Windows service that syncs local files to Databricks volumes

**What it does:**
- Watches `C:\ai_game_analyst\data\captures\analysis_result\` for new JSONs
- Watches GG hand history folder for new `.txt` files
- Uploads to `/Volumes/poker/bronze/bronze/analysis_json/` and `/Volumes/poker/bronze/bronze/gg_hand_history/`
- Tracks what's already uploaded (avoid duplicates)
- Runs as background service or scheduled task

**Deliverable:** Working uploader that keeps volumes in sync with local files.

---

## Phase 1: Bronze Layer - Raw Data Ingestion

### 1.1 JSON Ingestion Pipeline

**Goal:** Read raw JSONs from volume into Delta table

**Tables:**
- `poker.bronze.raw_analysis_json` - raw JSON content with metadata

**Pipeline:**
- Auto Loader to detect new files
- Store filename, timestamp, raw JSON content
- No transformation, just landing zone

### 1.2 GG Hand History Ingestion Pipeline

**Goal:** Read raw hand history files from volume into Delta table

**Tables:**
- `poker.bronze.raw_hand_history` - raw HH text with metadata

**Pipeline:**
- Auto Loader for `.txt` files
- Store filename, timestamp, raw content
- Parse hand boundaries (each hand starts with specific pattern)

**Deliverable:** Two Bronze tables with all raw data, auto-updating when new files arrive.

---

## Phase 2: Silver Layer - Parsed & Structured Data

### 2.1 Parse Analysis JSONs

**Goal:** Extract structured data from JSONs

**Tables:**
- `poker.silver.decisions` - one row per hero decision point
- `poker.silver.player_observations` - one row per opponent seen

**Key fields in decisions:**
- timestamp, stage, street, hero_cards, hero_position, hero_stack
- pot, to_call, board, gpt_recommendation
- opponents array (name, position, stack, vpip, bet, active)

**Key fields in player_observations:**
- player_name, timestamp, position, stack, vpip, action_faced, has_cards

### 2.2 Parse GG Hand History

**Goal:** Extract structured data from HH files

**Tables:**
- `poker.silver.hands` - one row per hand
- `poker.silver.actions` - one row per action in hand

**Key fields in hands:**
- hand_id, timestamp, stage, final_board, pot_size, winner
- hero_cards, hero_position, hero_result_bb

**Key fields in actions:**
- hand_id, street, seat, action_type, amount, is_all_in

### 2.3 Correlation: JSONs ↔ Hand History

**Goal:** Link decisions from JSONs with outcomes from HH

**Method:**
1. Match by timestamp (±5 seconds)
2. Validate by hero_cards + board
3. Add `hand_id` to decisions table

**Result:** `poker.silver.decisions` enriched with:
- `hand_id` (FK to hands)
- `hero_actual_action` (what hero really did)
- `result_bb` (profit/loss from HH)
- `went_to_showdown`, `won_hand`

**Deliverable:** Complete Silver layer with linked data.

---

## Phase 3: Player Statistics (Gold Layer)

### 3.1 Preflop Statistics

**Table:** `poker.gold.player_stats_preflop`

| Stat | Name | Formula | What it tells |
|------|------|---------|---------------|
| VPIP | Voluntarily Put $ In Pot | % hands played (not forced blind) | How loose/tight |
| PFR | Preflop Raise | % hands raised preflop | Aggression preflop |
| 3Bet | 3-Bet % | % re-raised when facing raise | Aggression vs raisers |
| F3B | Fold to 3-Bet | % folded when 3-betted | Exploitable by 3-betting |
| 4Bet | 4-Bet % | % 4-bet when facing 3-bet | Ultra aggression |
| F4B | Fold to 4-Bet | % folded when 4-betted | How committed |
| Steal | Steal Attempt % | % raised from CO/BTN/SB when folded to | Late position aggression |
| FvSteal | Fold vs Steal | % folded BB to steal attempt | Blind defense |
| Limp | Limp % | % just called BB preflop | Passive/trappy |
| LimpRR | Limp-Reraise | % raised after limping | Trap frequency |
| SQZ | Squeeze % | % 3-bet vs raise + caller(s) | Multiway aggression |
| CC | Cold Call | % called raise without prior action | Passive calling |

### 3.2 Postflop Statistics

**Table:** `poker.gold.player_stats_postflop`

| Stat | Name | Formula | What it tells |
|------|------|---------|---------------|
| CB | C-Bet Flop | % bet flop as preflop raiser | Continuation aggression |
| CBT | C-Bet Turn | % bet turn after c-betting flop | Barrel frequency |
| CBR | C-Bet River | % bet river after c-betting turn | Triple barrel |
| FCB | Fold to C-Bet | % folded to c-bet | Exploitable by c-betting |
| XR | Check-Raise % | % check-raised | Trappiness |
| DnB | Donk Bet % | % bet into preflop raiser | Unusual aggression |
| AF | Aggression Factor | (bet + raise) / call | Overall aggression |
| AFq | Aggression Frequency | (bet + raise) / (bet + raise + call + fold) | How often aggressive |
| WTSD | Went to Showdown | % of hands went to SD | Calling station indicator |
| W$SD | Won $ at Showdown | % won when reached SD | Hand reading skill |
| WWSF | Won When Saw Flop | % won after seeing flop | Postflop skill |

### 3.3 All-In & Short Stack Stats

**Table:** `poker.gold.player_stats_allin`

| Stat | Name | Formula | What it tells |
|------|------|---------|---------------|
| AIPre | All-In Preflop % | % shoved/called AI preflop | Shove frequency |
| FAI | Fold to All-In | % folded facing all-in | Exploitable by shoving |
| CAI | Call All-In % | % called all-in | Station or knows ranges |
| ShoveStack | Shove < 15BB % | % shoved with short stack | Push/fold knowledge |
| ICMAdj | ICM Awareness | Deviation from chipEV in bubble | Tournament skill |

### 3.4 Positional Stats

**Table:** `poker.gold.player_stats_by_position`

All above stats broken down by position (UTG, MP, CO, BTN, SB, BB).

**Key insights:**
- Steal % only matters from CO/BTN/SB
- VPIP by position shows positional awareness
- 3Bet from blinds vs from position = different ranges

### 3.5 Street-Specific Stats

**Table:** `poker.gold.player_stats_by_street`

| Stat | Flop | Turn | River |
|------|------|------|-------|
| Bet % | How often bets | Barrel % | River value/bluff |
| Raise % | Check-raise | Float raise | River raise |
| Fold % | Give up | Fold to 2nd barrel | Fold to river bet |
| Call % | Float | Peel turn | Call river |

### 3.6 Tendencies & Exploitability Indexes

**Table:** `poker.gold.player_tendencies`

| Index | Formula | Meaning |
|-------|---------|---------|
| Overfold Index | F3B + FCB + FvSteal / 3 | How exploitable by aggression |
| Overcall Index | VPIP - PFR + WTSD | Calling station score |
| Aggression Index | PFR + 3Bet + AF / 3 | Overall aggression |
| Steal Profit | Steal% - FvSteal | Expected profit from steals |
| 3Bet Profit | 3Bet% * (1 - F3B) | Expected profit from 3bets |
| Trap Index | XR + LimpRR / 2 | How often traps |
| Bluffy Index | CB * (1 - W$SD) | Bluffs too much |

### 3.7 Sample Size & Confidence

**Important:** Stats mean nothing without sample size

| Stat Type | Min Hands for Reliability |
|-----------|--------------------------|
| VPIP, PFR | 30 |
| 3Bet, Steal | 50 |
| Postflop stats | 100 |
| Positional breakdown | 200+ |

**Table:** `poker.gold.player_stat_confidence`

```sql
CASE 
  WHEN hands < 30 THEN 'unreliable'
  WHEN hands < 100 THEN 'low_confidence'
  WHEN hands < 300 THEN 'medium_confidence'
  ELSE 'high_confidence'
END as confidence_level
```

### 3.8 Player Type Classification

**Rule-based classification using multiple stats:**

```sql
CASE 
  -- Nit: Plays few hands, rarely aggressive
  WHEN vpip < 15 AND pfr < 12 AND three_bet < 4 THEN 'nit'
  
  -- TAG: Tight-aggressive, solid player
  WHEN vpip BETWEEN 15 AND 22 AND pfr BETWEEN 14 AND 20 
       AND af > 2.5 THEN 'tag'
  
  -- LAG: Loose-aggressive, dangerous
  WHEN vpip BETWEEN 25 AND 35 AND pfr > 20 
       AND three_bet > 8 AND af > 3 THEN 'lag'
  
  -- Maniac: Ultra aggressive, too many hands
  WHEN vpip > 40 AND pfr > 30 AND af > 4 THEN 'maniac'
  
  -- Calling Station: Plays too many, rarely raises
  WHEN vpip > 35 AND (vpip - pfr) > 15 AND wtsd > 35 THEN 'calling_station'
  
  -- Weak-Tight: Folds too much postflop
  WHEN vpip < 20 AND fold_to_cbet > 60 AND fold_to_3bet > 70 THEN 'weak_tight'
  
  -- Rock: Ultra tight, only premiums
  WHEN vpip < 12 AND pfr < 10 THEN 'rock'
  
  -- Fish: Random/bad play
  WHEN vpip > 45 AND w_sd < 45 THEN 'fish'
  
  ELSE 'regular'
END as player_type
```

**Deliverable:** Complete player profiles with 30+ stats, confidence levels, and type classification.

---

## Phase 4: GPT Deviation Analysis

### 4.1 Basic Deviation Tracking (SQL)

**Goal:** Find where hero didn't follow GPT

**Table:** `poker.gold.gpt_deviations`

**Query:**
```sql
SELECT *
FROM silver.decisions
WHERE gpt_recommendation != hero_actual_action
```

**Metrics:**
- Deviation rate (overall, by street, by stage)
- EV of deviations (AVG result_bb when deviated)
- EV of following (AVG result_bb when followed)

### 4.2 Deviation Outcome Analysis

**Goal:** Determine if deviations were good or bad

**Categories:**
| GPT Said | Hero Did | Result | Verdict |
|----------|----------|--------|---------|
| Fold | Call | Won | Lucky or GPT wrong |
| Fold | Call | Lost | Should have folded |
| Raise | Call | Won | Maybe ok |
| Raise | Call | Lost | Should have raised |

**Output:** List of deviation patterns with EV impact

### 4.3 Pattern Detection (ML Candidate)

**Goal:** Find WHEN deviations work

**Hypothesis:** Maybe deviations are profitable in specific spots:
- Against specific player types
- At specific stack depths
- On specific board textures

**Approach:** Train model to predict if deviation will be +EV

**Input:** Game state features + GPT recommendation
**Output:** Probability that deviation is +EV

**Deliverable:** Understanding of when to trust/ignore GPT.

---

## Phase 5: Feature Store & ML Preparation

### 5.1 Create Feature Tables

**Tables in `poker.ml` schema:**
- `player_features` - normalized player stats
- `hand_features` - normalized game state
- `board_features` - board texture vectors

**Normalization:**
- StandardScaler for continuous features
- One-hot encoding for categorical (position, street)
- Store both raw and normalized

### 5.2 Training Data Preparation

**Goal:** Create labeled dataset for ML models

**Labels from HH:**
- `result_bb` (for EV prediction)
- `won_hand` (for win prediction)
- `opponent_hand_strength` (from showdowns)

**Deliverable:** ML-ready feature tables in Unity Catalog.

---

## Phase 6: ML Models

### 6.1 Model 1: EV Estimator

**Goal:** Predict expected value of action

**Baseline:** XGBoost regression
**Advanced:** Neural network

**MLflow:**
- Track experiments
- Compare models
- Register best to Model Registry

### 6.2 Model 2: Player Classifier

**Goal:** Cluster opponents into types

**Approach:** KMeans → labeled classifier

### 6.3 Model 3: Deviation Predictor

**Goal:** Predict when to deviate from GPT

**Input:** Game state + GPT recommendation
**Output:** P(deviation is +EV)

**Deliverable:** Trained models in MLflow Registry.

---

## Phase 7: Integration & Dashboard

### 7.1 GPT Prompt Enhancement

**Goal:** Inject player profiles into GPT prompts

**Location:** Modify `print_screen_reader/gpt_advisor.py`

**Logic:**
1. Before calling GPT, lookup opponent stats from Databricks
2. Add to prompt: "BTN is a calling station (VPIP 55%, folds to river 20%)"

### 7.2 Databricks SQL Dashboard

**Goal:** Visual analytics

**Charts:**
- Win rate over time
- Deviation analysis
- Player type distribution
- Leak detection

### 7.3 Alerts

**Goal:** Real-time notifications

**Examples:**
- "Tilt detected - consider stopping"
- "New player seen - no data"
- "Strong deviation pattern found"

---

## Summary: What We'll Build

| Phase | Deliverable | Complexity |
|-------|-------------|------------|
| 0 | Data Uploader service | Medium |
| 1 | Bronze tables (raw data) | Low |
| 2 | Silver tables (parsed, linked) | Medium |
| 3 | Gold tables (player stats) | Low |
| 4 | GPT deviation analysis | Medium |
| 5 | Feature Store | Medium |
| 6 | ML Models | High |
| 7 | Integration & Dashboard | Medium |

**Estimated timeline:** 
- Phase 0-1: 1-2 days
- Phase 2: 2-3 days
- Phase 3-4: 2-3 days
- Phase 5-6: 3-5 days
- Phase 7: 2-3 days

**Total:** ~2-3 weeks of focused work

---

## Next Action

**Start with Phase 0.1:** Create the data_uploader service to sync files to Databricks volumes.
