# Poker Analyzer - Design Document

## Goal: Build Competitive Edge

The purpose is NOT generic ML/analytics, but **actionable intelligence** that improves win rate:
1. Better GPT recommendations through opponent profiles
2. Find leaks in my play (where I lose EV)
3. Find leaks in GPT recommendations (where it's wrong)
4. Identify profitable spots I'm missing

---

## Data Sources

### 1. Screenshot JSONs (Primary)

Captured every time action is on hero. Contains:

| Data | Value |
|------|-------|
| **Hero** | Cards, position, stack (BB), current bet |
| **Opponents** | Name, position, stack, VPIP %, bet, active/folded, all-in |
| **Game State** | Street, board, pot, to_call, stage (rush/final) |
| **GPT Advice** | Recommended action, full system prompt, user prompt, response time |
| **Hand History** | Multi-street context (previous actions this hand) |
| **Timestamp** | Microsecond precision in filename |

**Example opponent data:**
```json
{
  "position": "BTN",
  "name": "Player123",
  "stack": 22.3,
  "vpip": 45,
  "bet": null,
  "has_cards": true,
  "all_in": false
}
```

### 2. GG Hand History (Secondary)

Complete hand records but **WITHOUT player names** - just seat numbers.

**Correlation Strategy:**
- Match by timestamp (±5 seconds)
- Validate by hero cards + board
- Map seat numbers to player names from JSONs

**Value:**
- Showdown data → see what opponents actually had
- Complete action sequences → what happened after hero acted
- Final results → who won, actual pot sizes

### 3. GG Tournament History (Results)

Tournament summary files with final results.

**Example content:**
```
Tournament #260520685, Mystery Battle Royale $25, Hold'em No Limit
Buy-in: $12.5+$2+$10.5
18 Players
Total Prize Pool: $414
Tournament started 2026/01/25 23:41:11
1st : Hero, $161
```

**Parsed data:**
| Field | Example |
|-------|--------|
| tournament_id | 260520685 |
| tournament_name | Mystery Battle Royale $25 |
| buyin_prize | $12.5 |
| buyin_rake | $2 |
| buyin_bounty | $10.5 |
| total_players | 18 |
| prize_pool | $414 |
| started_at | 2026-01-25 23:41:11 |
| hero_position | 1 |
| hero_prize | $161 |

**Value:**
- Track overall ROI per tournament type
- Correlate GPT compliance with tournament results
- Identify which stake levels are most profitable

---

## Use Cases

### BASE USE CASES (Priority 1-5)

#### 1. Player Profiles for GPT Enhancement

**Problem:** GPT only sees current VPIP. No history, no tendencies.

**Solution:** Build player profiles aggregated from all sessions:

| Stat | Source | How to Calculate |
|------|--------|------------------|
| VPIP | JSONs | Already captured per screenshot |
| Seen at showdown | Hand History | Did they go to showdown? |
| Bluff frequency | Hand History | Bet big, showed weak hand |
| Fold to aggression | JSONs | Had cards → no cards after bet faced |
| Position tendencies | JSONs | VPIP by position |
| Stack depth play | JSONs | How they play deep vs short |

**Output:** Add to GPT prompt:
```
Opponent BTN "Player123":
- VPIP: 45% (fish, plays too many hands)
- Folds to river bet: 70% (can be bluffed)
- Rarely bluffs (only 12% at showdown with air)
- Opens 3x from BTN (positional awareness)
```

#### 2. Decision Deviation Analysis (Hero vs GPT)

**Problem:** I don't always follow GPT. Was I right or wrong?

**Data Needed:**
- GPT recommendation from JSON
- My actual action (from next JSON or hand history)
- Result (hand history - did I win?)

**Analysis:**
```
Scenario: GPT said "fold", I called
├── Outcome: Won pot → +EV deviation (GPT was wrong OR I got lucky)
├── Outcome: Lost pot → -EV deviation (GPT was right)
└── Calculate: $ won/lost when I deviate from GPT
```

**Output:** Dashboard showing:
- Deviation rate by street (preflop/flop/turn/river)
- EV impact of deviations
- Specific hands where deviation was profitable/costly

#### 3. GPT Error Pattern Detection

**Problem:** GPT makes systematic errors in certain spots.

**Known Issues (from user):**
- Final table with 10BB → recommends 2BB raise when everyone shoves
- Doesn't adjust for bounty value
- ICM mistakes in bubble situations

**Detection Method:**
```sql
SELECT 
  stage,
  street,
  hero_stack_bb,
  gpt_action,
  COUNT(*) as occurrences,
  AVG(ev_result) as avg_ev
FROM decisions
WHERE hero_stack_bb < 12
  AND stage = 'final'
  AND gpt_action LIKE 'raise%'
  AND gpt_action NOT LIKE '%all%'
GROUP BY 1,2,3,4
HAVING avg_ev < 0  -- Negative EV patterns
```

**Output:** List of GPT failure patterns:
- "When stack < 12BB in final, raise < all-in loses on average"
- "Calling all-in with Ax offsuit in BB vs UTG is -EV"

#### 4. Showdown Leak Analysis

**Problem:** I call too wide / too tight in big pots.

**Data Needed:**
- All hands that went to showdown (from HH)
- Hero's decision to call/fold
- Opponent's actual hand
- Pot odds at decision point

**Analysis:**
```
All river calls where pot odds > 3:1:
├── Called and won: 65%
├── Called and lost: 35%
├── Pot odds needed to break even: 25%
├── My actual call rate at this odds: 70%
└── VERDICT: Calling correctly, maybe even too tight
```

**Output:**
- "You fold river too much getting 4:1. Call more with Ax."
- "You call all-in with Ax offsuit. This is -EV vs typical shove range."

#### 5. Spot Identification (Missed Value / Missed Bluffs)

**Problem:** I miss profitable spots.

**Detection:**
```
Find hands where:
- I checked, opponent checked behind
- Showdown: I had best hand
- Pot was multiway or opponent was passive
→ MISSED VALUE BET
```

```
Find hands where:
- I folded to small bet
- Pot odds were > 5:1
- Board was scary (many draws completed)
→ POTENTIALLY MISSED BLUFF CATCH
```

#### 6. GPT Compliance vs Tournament Results

**Problem:** Does following GPT advice actually improve my results?

**Hypothesis:** Tournaments where I follow GPT more consistently should have better outcomes.

**Data Needed:**
- `silver.tournaments`: hero_position, hero_prize, buyin_total
- `silver.screenshots`: gpt_recommendation per decision in tournament
- `silver.hands`: hero actual action (matched via timestamp)

**Metrics per tournament:**
```sql
SELECT 
  t.tournament_id,
  t.hero_position,
  t.hero_prize - t.buyin_total as profit,
  COUNT(s.file_name) as total_decisions,
  SUM(CASE WHEN s.gpt_recommendation = h.hero_actual_action THEN 1 ELSE 0 END) as gpt_followed,
  gpt_followed * 100.0 / total_decisions as compliance_pct
FROM silver.tournaments t
JOIN silver.screenshots s ON s.tournament_id = t.tournament_id
JOIN silver.hands h ON h.hand_id = s.matched_hand_id
GROUP BY 1,2,3
```

**Analysis:**
```
Tournaments by GPT compliance quartile:
├── Q1 (0-25% compliance):  avg finish 12th, avg profit -$8
├── Q2 (25-50% compliance): avg finish 9th,  avg profit -$3
├── Q3 (50-75% compliance): avg finish 6th,  avg profit +$5
└── Q4 (75-100% compliance): avg finish 4th, avg profit +$15
```

**Key Questions:**
1. Does higher GPT compliance correlate with better finishes?
2. In which spots does deviation from GPT hurt most? (preflop vs postflop)
3. Are there spots where my deviation is actually +EV? (I know better than GPT)
4. Does compliance matter more in rush stage vs final table?

**Output:**
- "Your ROI is +35% when following GPT >70% vs -15% when <50%"
- "Deviating from GPT on river decisions costs you 2.5BB/tournament"
- "Your preflop deviations are +EV (you're tighter than GPT in early position)"

---

### ENHANCED USE CASES (Priority 6-10)

#### 6. Session Quality Scoring

**Metrics per session:**
- Decisions made vs GPT alignment %
- EV of deviations
- Tilt indicators (deviation rate increases over time?)
- Optimal session length (when do mistakes increase?)

**Output:** "Stop playing" alert when quality drops.

#### 7. Opponent-Specific Adjustments

**Build adjustment rules from data:**
```
Player "FishGuy99":
- VPIP: 65% → "Wide range, value bet thinner"
- Folds to c-bet: 20% → "Don't bluff, they call everything"
- 3-bet range: Only KK+ → "Fold AK to their 3-bet"
```

Inject into GPT prompt for real-time adjustment.

#### 8. All-In Decision Audit

**Focus on biggest decisions:**
- All calls/folds to all-in
- All shoves
- Stack sizes, positions, stage

**Analyze:**
- Hand strength vs expected range
- ICM implications
- Bounty value considerations
- Result tracking

**Output:** "You call all-in too light with Axo. Expected range is TT+, AQs+"

#### 9. Position Leak Analysis

**By position:**
- Win rate BB/100
- VPIP/PFR equivalent
- c-bet frequency
- Fold to 3-bet

**Find:**
- "You lose most money from SB"
- "You don't defend BB enough"
- "You c-bet too much on wet boards from EP"

#### 10. Tournament Stage Strategy Validation

**Compare results by stage:**
- Rush early (50+ players)
- Rush late (near final table bubble)
- Final table bubble
- Final table deep
- Heads up

**Validate GPT prompts:**
- Is ICM weighting correct?
- Is bounty value properly considered?
- Are stack band rules optimal?

---

## Data Architecture

### Implemented Schema (as of 2026-01-27)

All objects are created by numbered migration notebooks in `src/notebooks/setup/`.

#### Catalog & Schemas
```
poker (catalog)
├── bronze   - Raw data layer (JSON/text files as-is)
├── silver   - Processed/matched data
├── gold     - Analytics views
└── utils    - Python UDFs for hand evaluation
```

#### Bronze Layer (Raw Data)
| Table | Source | Description |
|-------|--------|-------------|
| `bronze.analysis_result` | Screenshot JSONs | Raw JSON from screenshot analyzer |
| `bronze.hand_history` | GGPoker .txt | Raw hand history text files |
| `bronze.tournament_history` | GGPoker .txt | Raw tournament result files |
| `bronze._migrations` | System | Tracks executed migrations |

#### Silver Layer (Processed)
| Table | Source | Description |
|-------|--------|-------------|
| `silver.screenshots` | analysis_result | Main screenshot data: hero cards, GPT advice, pot, etc. |
| `silver.screenshot_players` | analysis_result | Opponents per screenshot (1:N) |
| `silver.screenshot_history` | analysis_result | Hand history per street per screenshot |
| `silver.tournaments` | tournament_history | Parsed tournament results with ROI |
| `silver.hands` | hand_history | Hand headers - one row per hand |
| `silver.hand_players` | hand_history | Players per hand with VPIP, PFR, result |
| `silver.hand_actions` | hand_history | Individual actions (fold, call, raise) |
| `silver.screenshot_hand_mapping` | join | Bridge table: screenshots → hands (n:1) |

**Screenshot-Hand Mapping:**
- Matches screenshots to hand history by: cards + position + timestamp (±5 min)
- Confidence levels: `high` (cards+pos+time), `medium` (cards+time), `low`
- Current match rate: ~97%

#### Gold Layer (Analytics)

**Tournament Analysis:**
| View | Purpose |
|------|---------|
| `gold.tournament_analysis` | Tournament results with stage breakdown, bounty vs position prize |
| `gold.tournament_summary_by_stage` | Stats by finish stage (champion, runner-up, bubble, etc.) |
| `gold.tournament_summary_by_buyin` | Stats by buyin level ($10 vs $25) |
| `gold.tournament_summary_daily` | Daily P&L tracking |

**GPT Decision Analysis:**
| View | Purpose |
|------|---------|
| `gold.screenshot_hand_analysis` | GPT advice vs actual outcome per decision |
| `gold.hand_line_analysis` | Full hand line - all GPT recs vs hero actions |
| `gold.fold_showdown_analysis` | "What if" - hero folded but showdown happened |

**Key columns in `screenshot_hand_analysis`:**
- `followed_gpt_advice`: BOOLEAN - did hero do what GPT said?
- `outcome_category`: ignored_fold_won, ignored_fold_lost, followed_play_won, etc.
- `profit_bb`: profit/loss in big blinds
- `missed_pot_bb`: pot hero missed when folding

**Key columns in `fold_showdown_analysis`:**
- `hero_would_have_won`: BOOLEAN - simulation of showdown
- `potential_profit_bb`: what hero would have won/lost
- `hero_hand_at_showdown`: hero's hand strength

#### Utils Schema (Python UDFs)
| Function | Signature | Returns |
|----------|-----------|---------|
| `utils.evaluate_hand` | `(hole_cards STRING, board STRING)` | `STRUCT<rank, hand_class, hand_name>` |
| `utils.compare_hands` | `(hero_cards STRING, opponent_cards ARRAY<STRING>, board STRING)` | `STRUCT<winner_index, hero_rank, hero_hand, winning_rank, winning_hand, hero_would_win>` |

---

### Tables (Design Document)

_Note: Below is the original design. See above for implemented schema._

```sql
-- Silver: Cleaned facts
CREATE TABLE silver.decisions (
  decision_id STRING,
  timestamp TIMESTAMP,
  session_id STRING,
  stage STRING,           -- rush/final
  street STRING,          -- preflop/flop/turn/river
  hero_cards STRING,
  hero_position STRING,
  hero_stack_bb DOUBLE,
  pot_bb DOUBLE,
  to_call_bb DOUBLE,
  board ARRAY<STRING>,
  opponents ARRAY<STRUCT<name, position, stack, vpip, bet, active>>,
  gpt_recommendation STRING,
  hero_action STRING,     -- Filled from HH correlation
  result_bb DOUBLE,       -- Filled from HH
  hand_id STRING          -- Link to HH
);

-- Silver: Player observations
CREATE TABLE silver.player_observations (
  player_name STRING,
  timestamp TIMESTAMP,
  position STRING,
  stack_bb DOUBLE,
  vpip INT,
  action_faced STRING,    -- What action they faced
  action_taken STRING,    -- What they did
  went_to_showdown BOOLEAN,
  hand_shown STRING       -- From HH if available
);

-- Gold: Player profiles
CREATE TABLE gold.player_profiles (
  player_name STRING,
  hands_observed INT,
  avg_vpip DOUBLE,
  fold_to_cbet_pct DOUBLE,
  fold_to_river_bet_pct DOUBLE,
  showdown_frequency DOUBLE,
  bluff_frequency DOUBLE,
  last_seen TIMESTAMP
);

-- Gold: GPT error patterns
CREATE TABLE gold.gpt_errors (
  pattern_id STRING,
  description STRING,
  conditions STRING,      -- JSON with stage, stack range, etc
  occurrences INT,
  avg_ev_loss DOUBLE,
  example_hands ARRAY<STRING>
);

-- Gold: Hero leaks
CREATE TABLE gold.hero_leaks (
  leak_id STRING,
  description STRING,
  position STRING,
  street STRING,
  frequency DOUBLE,
  ev_impact_bb DOUBLE,
  recommendation STRING
);
```

### Pipeline

```
JSONs → Bronze (raw) → Silver (parsed decisions) ←→ Hand History correlation
                              ↓
                        Gold (aggregates)
                              ↓
                        Reports / GPT Enhancement
```

---

## Implementation Priority

| # | Use Case | Effort | Impact |
|---|----------|--------|--------|
| 1 | Player Profiles → GPT | Medium | HIGH - direct win rate improvement |
| 2 | Decision Deviation | Low | HIGH - find where I'm wrong |
| 3 | GPT Error Patterns | Medium | HIGH - fix systematic mistakes |
| 4 | Showdown Analysis | Medium | MEDIUM - requires HH correlation |
| 5 | Missed Spots | High | MEDIUM - complex detection |
| 6 | Session Quality | Low | MEDIUM - prevent tilt |
| 7 | Opponent Adjustments | Medium | HIGH - personalized strategy |
| 8 | All-In Audit | Low | HIGH - biggest decisions |
| 9 | Position Leaks | Low | MEDIUM - standard analysis |
| 10 | Stage Validation | High | MEDIUM - validates prompts |

---

## Next Steps

1. **Build ingestion pipeline** - Upload JSONs to Bronze volume
2. **Parse to Silver** - Create decisions table
3. **Player profiles** - Aggregate observations → profiles
4. **Integrate HH** - Add GG hand history parser, correlate by timestamp
5. **Dashboard** - Databricks SQL for key metrics
6. **GPT Enhancement** - Inject player profiles into prompts

---

## ML/AI Use Cases

### Which Use Cases Use ML?

| # | Use Case | ML? | Model Type | Why ML? |
|---|----------|-----|------------|---------|
| 1 | Player Profiles → GPT | ❌ | - | Pure aggregation (SQL) |
| 2 | Decision Deviation | ✅ | Regression | Predict EV of action |
| 3 | GPT Error Patterns | ✅ | Classification | Classify error types |
| 4 | Showdown Analysis | ✅ | Classification | Predict opponent range |
| 5 | Missed Spots | ✅ | Anomaly Detection | Find unusual patterns |
| 6 | Session Quality | ✅ | Time Series | Predict tilt/fatigue |
| 7 | Opponent Adjustments | ✅ | Clustering | Group player types |
| 8 | All-In Audit | ✅ | Regression | EV calculation |
| 9 | Position Leaks | ❌ | - | Pure aggregation (SQL) |
| 10 | Stage Validation | ❌ | - | A/B testing (statistics) |

---

## ML Architecture in Databricks

### Feature Store Design

Unity Catalog Feature Engineering tables:

```
poker.ml.features (schema)
├── player_features      # Per-player aggregated stats
├── hand_features        # Per-hand game state
├── board_features       # Board texture analysis
└── action_features      # Action sequence encoding
```

#### 1. Player Features Table

```sql
CREATE TABLE poker.ml.player_features (
  -- Primary key
  player_name STRING,
  
  -- Raw stats (aggregated from observations)
  hands_observed INT,
  vpip_pct DOUBLE,
  pfr_pct DOUBLE,                    -- Preflop raise %
  aggression_factor DOUBLE,          -- (bet+raise) / call
  three_bet_pct DOUBLE,
  fold_to_three_bet_pct DOUBLE,
  cbet_flop_pct DOUBLE,
  fold_to_cbet_pct DOUBLE,
  wtsd_pct DOUBLE,                   -- Went to showdown %
  wwsf_pct DOUBLE,                   -- Won when saw flop %
  showdown_win_pct DOUBLE,
  avg_stack_bb DOUBLE,
  
  -- Normalized features (for ML models)
  vpip_normalized DOUBLE,            -- StandardScaler
  pfr_normalized DOUBLE,
  af_normalized DOUBLE,
  stack_normalized DOUBLE,
  
  -- Derived features
  tightness_score DOUBLE,            -- 1 - vpip_pct
  aggression_score DOUBLE,           -- (pfr + 3bet + cbet) / 3
  calling_station_score DOUBLE,      -- vpip - pfr
  
  -- Metadata
  last_updated TIMESTAMP,
  sample_size_bucket STRING          -- 'low' (<30), 'medium' (30-100), 'high' (>100)
);
```

#### 2. Hand Features Table

```sql
CREATE TABLE poker.ml.hand_features (
  -- Primary key
  decision_id STRING,
  
  -- Game state (raw)
  stage STRING,                      -- 'rush' / 'final'
  street STRING,                     -- 'preflop', 'flop', 'turn', 'river'
  position STRING,                   -- 'UTG', 'MP', 'CO', 'BTN', 'SB', 'BB'
  hero_stack_bb DOUBLE,
  pot_bb DOUBLE,
  to_call_bb DOUBLE,
  num_opponents INT,
  num_active INT,                    -- Still have cards
  
  -- Hand strength
  hero_cards STRING,                 -- 'AhKs'
  hand_rank INT,                     -- 1-169 (AA=1, 72o=169)
  hand_suited BOOLEAN,
  hand_connected BOOLEAN,            -- Connectors like 87s
  hand_pair BOOLEAN,
  
  -- Board analysis (flop+)
  board_high_card INT,               -- 14=A, 13=K, ...
  board_paired BOOLEAN,
  board_suited INT,                  -- Count of same suit
  board_connected INT,               -- Straight draw potential
  board_texture_score DOUBLE,        -- 0=dry, 1=wet
  
  -- Pot odds & equity
  pot_odds DOUBLE,                   -- to_call / (pot + to_call)
  spr DOUBLE,                        -- Stack-to-pot ratio
  effective_stack_bb DOUBLE,         -- Min(hero, villain)
  
  -- Normalized features (for NN)
  position_encoded ARRAY<DOUBLE>,    -- One-hot [6 dims]
  street_encoded ARRAY<DOUBLE>,      -- One-hot [4 dims]
  stack_normalized DOUBLE,           -- Log-scaled
  pot_normalized DOUBLE,
  
  -- Labels (from hand history correlation)
  hero_action STRING,                -- What hero actually did
  action_encoded INT,                -- 0=fold, 1=check, 2=call, 3=raise, 4=allin
  result_bb DOUBLE,                  -- Profit/loss
  went_to_showdown BOOLEAN,
  won_hand BOOLEAN,
  
  -- GPT comparison
  gpt_action STRING,
  gpt_action_encoded INT,
  followed_gpt BOOLEAN
);
```

#### 3. Board Features Table

```sql
CREATE TABLE poker.ml.board_features (
  board_hash STRING,                 -- MD5 of sorted cards
  cards ARRAY<STRING>,
  
  -- Texture
  high_card INT,
  second_card INT,
  low_card INT,
  is_monotone BOOLEAN,               -- 3 same suit
  is_two_tone BOOLEAN,               -- 2 same suit
  is_rainbow BOOLEAN,                -- All different suits
  is_paired BOOLEAN,
  is_trips BOOLEAN,
  
  -- Draw potential
  straight_draws INT,                -- Number of straight draw combos
  flush_draw BOOLEAN,
  
  -- Normalized texture vector
  texture_vector ARRAY<DOUBLE>       -- [8 dims] for NN input
);
```

---

## MLflow Experiment Structure

```
Workspace: /Users/{user}/poker_analyzer/
└── mlflow/
    ├── experiments/
    │   ├── 01_ev_prediction/           # Use Case 2, 8
    │   │   ├── baseline_xgboost
    │   │   ├── neural_net_v1
    │   │   └── neural_net_v2_attention
    │   │
    │   ├── 02_opponent_range/          # Use Case 4
    │   │   ├── random_forest
    │   │   └── gradient_boosting
    │   │
    │   ├── 03_player_clustering/       # Use Case 7
    │   │   ├── kmeans_k5
    │   │   ├── kmeans_k8
    │   │   └── dbscan
    │   │
    │   ├── 04_error_classification/    # Use Case 3
    │   │   └── multiclass_xgboost
    │   │
    │   └── 05_tilt_detection/          # Use Case 6
    │       └── lstm_sequence
    │
    └── registry/
        ├── ev_estimator (Production)
        ├── player_classifier (Staging)
        └── tilt_detector (Development)
```

---

## ML Models - Detailed Design

### Model 1: EV Estimator (Use Cases 2, 8)

**Goal:** Predict expected value of hero's action

**Input Features (from Feature Store):**
```python
features = [
    # From hand_features
    'position_encoded',        # [6 dims]
    'street_encoded',          # [4 dims]
    'stack_normalized',
    'pot_normalized',
    'pot_odds',
    'spr',
    'hand_rank',
    'num_opponents',
    
    # From board_features
    'texture_vector',          # [8 dims]
    
    # From player_features (opponent)
    'opp_vpip_normalized',
    'opp_af_normalized',
    'opp_fold_to_cbet_pct'
]
# Total: ~25 features
```

**Target:** `result_bb` (continuous, profit/loss in BB)

**Architecture Options:**

```python
# Option A: XGBoost (baseline)
from xgboost import XGBRegressor
model = XGBRegressor(
    n_estimators=100,
    max_depth=6,
    learning_rate=0.1
)

# Option B: Neural Network
import torch.nn as nn
class EVEstimator(nn.Module):
    def __init__(self, input_dim=25):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, 64),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(32, 16),
            nn.ReLU(),
            nn.Linear(16, 1)  # Output: predicted EV
        )
    
    def forward(self, x):
        return self.net(x)
```

**MLflow Tracking:**
```python
import mlflow
import mlflow.pytorch

with mlflow.start_run(experiment_id="01_ev_prediction"):
    # Log parameters
    mlflow.log_params({
        "model_type": "neural_net",
        "hidden_layers": [64, 32, 16],
        "dropout": 0.2,
        "learning_rate": 0.001,
        "epochs": 100
    })
    
    # Train model
    model = train_model(X_train, y_train)
    
    # Log metrics
    mlflow.log_metrics({
        "train_mae": train_mae,
        "val_mae": val_mae,
        "train_rmse": train_rmse,
        "val_rmse": val_rmse
    })
    
    # Log model
    mlflow.pytorch.log_model(model, "ev_estimator")
    
    # Register if good
    if val_mae < 2.0:  # Less than 2 BB error
        mlflow.register_model(
            f"runs:/{mlflow.active_run().info.run_id}/ev_estimator",
            "ev_estimator"
        )
```

---

### Model 2: Player Type Classifier (Use Case 7)

**Goal:** Cluster opponents into types for strategy adjustment

**Input Features:**
```python
features = [
    'vpip_normalized',
    'pfr_normalized',
    'af_normalized',
    'three_bet_pct',
    'fold_to_cbet_pct',
    'wtsd_pct'
]
```

**Approach:** Unsupervised clustering → classification

```python
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Step 1: Cluster to find types
scaler = StandardScaler()
X_scaled = scaler.fit_transform(player_stats)

kmeans = KMeans(n_clusters=5, random_state=42)
clusters = kmeans.fit_predict(X_scaled)

# Player types discovered:
# 0: Nit (low VPIP, low PFR, low AF)
# 1: TAG (low VPIP, high PFR, high AF)
# 2: LAG (high VPIP, high PFR, high AF)
# 3: Calling Station (high VPIP, low PFR, low AF)
# 4: Maniac (very high VPIP, very high PFR, very high AF)

# Step 2: Train classifier on labeled data
from xgboost import XGBClassifier
classifier = XGBClassifier()
classifier.fit(X_scaled, clusters)
```

**MLflow:**
```python
with mlflow.start_run(experiment_id="03_player_clustering"):
    mlflow.log_param("n_clusters", 5)
    mlflow.log_metric("silhouette_score", silhouette)
    mlflow.sklearn.log_model(kmeans, "player_clusterer")
    mlflow.sklearn.log_model(classifier, "player_classifier")
```

---

### Model 3: Opponent Range Predictor (Use Case 4)

**Goal:** Given opponent action, predict their hand range strength

**Input Features:**
```python
features = [
    # Opponent profile
    'opp_vpip_normalized',
    'opp_pfr_normalized',
    'opp_af_normalized',
    
    # Action taken
    'action_encoded',          # fold/check/call/raise/allin
    'bet_size_normalized',     # As % of pot
    
    # Context
    'street_encoded',
    'position_encoded',
    'pot_normalized'
]
```

**Target:** `range_strength` (0-1, derived from showdown data)

```python
# Classification into range buckets
# 0: Air/bluff (bottom 20% of range)
# 1: Weak (20-40%)
# 2: Medium (40-60%)
# 3: Strong (60-80%)
# 4: Monster (top 20%)
```

---

### Model 4: Tilt/Fatigue Detector (Use Case 6)

**Goal:** Predict when hero's play quality is declining

**Input:** Sequence of last N decisions

```python
features_per_decision = [
    'followed_gpt',            # Binary
    'ev_result',               # Outcome
    'decision_time_seconds',   # How long to decide
    'bet_size_vs_optimal',     # Deviation from GPT sizing
    'time_since_session_start' # Minutes
]

# LSTM input: (batch, sequence_length=20, features=5)
```

**Architecture:**
```python
class TiltDetector(nn.Module):
    def __init__(self, input_dim=5, hidden_dim=32):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_dim,
            hidden_size=hidden_dim,
            num_layers=2,
            batch_first=True,
            dropout=0.2
        )
        self.fc = nn.Linear(hidden_dim, 1)
        self.sigmoid = nn.Sigmoid()
    
    def forward(self, x):
        # x: (batch, seq_len, features)
        lstm_out, _ = self.lstm(x)
        last_output = lstm_out[:, -1, :]  # Take last timestep
        return self.sigmoid(self.fc(last_output))

# Output: probability of tilt (0-1)
# Alert if > 0.7
```

---

## Feature Engineering Pipeline

```python
# Databricks notebook: feature_engineering.py

from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import functions as F

fe = FeatureEngineeringClient()

# 1. Create player features from observations
player_features_df = spark.sql("""
    SELECT 
        player_name,
        COUNT(*) as hands_observed,
        AVG(vpip) as vpip_pct,
        -- ... other aggregations
    FROM silver.player_observations
    GROUP BY player_name
    HAVING COUNT(*) >= 10  -- Minimum sample size
""")

# 2. Normalize features
from sklearn.preprocessing import StandardScaler
import pandas as pd

pdf = player_features_df.toPandas()
scaler = StandardScaler()
pdf['vpip_normalized'] = scaler.fit_transform(pdf[['vpip_pct']])
# ... other normalizations

# 3. Write to Feature Store
fe.create_table(
    name="poker.ml.player_features",
    primary_keys=["player_name"],
    df=spark.createDataFrame(pdf),
    description="Player statistics for opponent modeling"
)
```

---

## Training Pipeline (MLflow)

```python
# Databricks notebook: train_ev_model.py

import mlflow
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# 1. Load training data with feature lookup
training_set = fe.create_training_set(
    df=spark.table("silver.decisions"),
    feature_lookups=[
        FeatureLookup(
            table_name="poker.ml.player_features",
            lookup_key="opponent_name",
            feature_names=["vpip_normalized", "af_normalized"]
        ),
        FeatureLookup(
            table_name="poker.ml.board_features", 
            lookup_key="board_hash",
            feature_names=["texture_vector"]
        )
    ],
    label="result_bb"
)

# 2. Convert to pandas for training
training_df = training_set.load_df().toPandas()

# 3. Train with MLflow tracking
mlflow.set_experiment("/Users/{user}/poker_analyzer/01_ev_prediction")

with mlflow.start_run(run_name="xgboost_baseline"):
    # ... training code
    
    # Log model with feature store metadata
    fe.log_model(
        model=trained_model,
        artifact_path="model",
        flavor=mlflow.sklearn,
        training_set=training_set,
        registered_model_name="ev_estimator"
    )
```

---

## Model Serving

```python
# After model is in registry, deploy for inference

# Option 1: Batch scoring
fe = FeatureEngineeringClient()
predictions = fe.score_batch(
    model_uri="models:/ev_estimator/Production",
    df=new_decisions_df
)

# Option 2: Real-time endpoint (for live GPT enhancement)
# Create Model Serving endpoint in Databricks UI
# Endpoint: /serving-endpoints/ev-estimator/invocations
```

---

## Implementation Roadmap for ML

| Phase | Task | MLflow Component |
|-------|------|------------------|
| 1 | Ingest data → Silver tables | - |
| 2 | Create Feature Store tables | Feature Engineering |
| 3 | Build normalization pipeline | Feature Engineering |
| 4 | Train EV Estimator (XGBoost baseline) | Experiment Tracking |
| 5 | Train EV Estimator (Neural Net) | Experiment Tracking |
| 6 | Compare models, promote best | Model Registry |
| 7 | Train Player Classifier | Experiment Tracking |
| 8 | Deploy EV model for batch scoring | Model Serving |
| 9 | Train Tilt Detector (LSTM) | Experiment Tracking |
| 10 | Build real-time serving endpoint | Model Serving |
