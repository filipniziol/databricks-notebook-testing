# Databricks notebook source
# MAGIC %md
# MAGIC # Update Hand Features
# MAGIC 
# MAGIC Creates Feature Table with hand-level features for outcome prediction.
# MAGIC 
# MAGIC **Schedule:** Daily (after silver layer refresh)
# MAGIC **Output:** `poker.ml.hand_features` (Feature Table)

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

# COMMAND ----------

FEATURE_TABLE_NAME = "poker.ml.hand_features"

fe = FeatureEngineeringClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering
# MAGIC 
# MAGIC Features:
# MAGIC - **Hand strength:** Pocket pair rank, suited/offsuit, high card
# MAGIC - **Position:** Early/Middle/Late/Blinds
# MAGIC - **Stack depth:** In big blinds
# MAGIC - **Opponents:** Count, avg cluster type at table
# MAGIC - **Pot odds:** Pot size relative to stack

# COMMAND ----------

df_features = spark.sql("""
WITH hand_strength AS (
    -- Parse hero cards into features
    -- Format: 5hAs = 5h As (4 chars: rank1+suit1+rank2+suit2)
    SELECT 
        h.hand_id,
        h.hero_cards,
        -- Extract card ranks (A=14, K=13, Q=12, J=11, T=10, 9-2)
        CASE 
            WHEN SUBSTRING(h.hero_cards, 1, 1) = 'A' THEN 14
            WHEN SUBSTRING(h.hero_cards, 1, 1) = 'K' THEN 13
            WHEN SUBSTRING(h.hero_cards, 1, 1) = 'Q' THEN 12
            WHEN SUBSTRING(h.hero_cards, 1, 1) = 'J' THEN 11
            WHEN SUBSTRING(h.hero_cards, 1, 1) = 'T' THEN 10
            ELSE CAST(SUBSTRING(h.hero_cards, 1, 1) AS INT)
        END AS card1_rank,
        CASE 
            WHEN SUBSTRING(h.hero_cards, 3, 1) = 'A' THEN 14
            WHEN SUBSTRING(h.hero_cards, 3, 1) = 'K' THEN 13
            WHEN SUBSTRING(h.hero_cards, 3, 1) = 'Q' THEN 12
            WHEN SUBSTRING(h.hero_cards, 3, 1) = 'J' THEN 11
            WHEN SUBSTRING(h.hero_cards, 3, 1) = 'T' THEN 10
            ELSE CAST(SUBSTRING(h.hero_cards, 3, 1) AS INT)
        END AS card2_rank,
        -- Suited? (compare suit1 at pos 2 with suit2 at pos 4)
        CASE WHEN SUBSTRING(h.hero_cards, 2, 1) = SUBSTRING(h.hero_cards, 4, 1) THEN 1 ELSE 0 END AS is_suited,
        -- Pocket pair? (compare rank1 at pos 1 with rank2 at pos 3)
        CASE WHEN SUBSTRING(h.hero_cards, 1, 1) = SUBSTRING(h.hero_cards, 3, 1) THEN 1 ELSE 0 END AS is_pocket_pair
    FROM poker.silver.hands h
    WHERE h.hero_cards IS NOT NULL 
      AND LENGTH(h.hero_cards) >= 4
),

hero_stats AS (
    SELECT 
        hp.hand_id,
        hp.chips_start,
        hp.net_profit,
        hp.went_to_showdown,
        hp.won_hand AS hero_won,  -- Use won_hand from hand_players (correct)
        h.big_blind,
        h.hand_timestamp,
        -- Stack depth in BB
        CAST(hp.chips_start AS DOUBLE) / NULLIF(h.big_blind, 0) AS stack_bb,
        -- Position encoding (0=early, 1=middle, 2=late, 3=blinds)
        CASE 
            WHEN hp.position IN ('UTG', 'UTG+1', 'UTG+2') THEN 0
            WHEN hp.position IN ('MP', 'MP+1', 'LJ', 'HJ') THEN 1
            WHEN hp.position IN ('CO', 'BTN') THEN 2
            WHEN hp.position IN ('SB', 'BB') THEN 3
            ELSE 1
        END AS position_encoded,
        -- Is late position (CO/BTN)?
        CASE WHEN hp.position IN ('CO', 'BTN') THEN 1 ELSE 0 END AS is_late_position
    FROM poker.silver.hand_players hp
    JOIN poker.silver.hands h ON hp.hand_id = h.hand_id
    WHERE hp.is_hero = true
),

opponent_info AS (
    -- Aggregate opponent info per hand
    SELECT 
        hp.hand_id,
        COUNT(*) - 1 AS opponent_count,
        -- Avg opponent cluster (if available)
        AVG(CASE WHEN pc.cluster_id IS NOT NULL AND hp.is_hero = false THEN pc.cluster_id END) AS avg_opponent_cluster
    FROM poker.silver.hand_players hp
    LEFT JOIN poker.gold.player_clusters pc ON hp.player_name = pc.player_name
    GROUP BY hp.hand_id
),

preflop_action AS (
    -- Was there a raise preflop?
    SELECT 
        hand_id,
        MAX(CASE WHEN street = 'preflop' AND action_type = 'raise' THEN 1 ELSE 0 END) AS had_preflop_raise,
        MAX(CASE WHEN street = 'preflop' AND action_type = 'raise' AND is_hero = true THEN 1 ELSE 0 END) AS hero_raised_preflop,
        SUM(CASE WHEN street = 'preflop' AND action_type = 'raise' THEN 1 ELSE 0 END) AS preflop_raise_count
    FROM poker.silver.hand_actions
    GROUP BY hand_id
),

hand_stage AS (
    -- Get stage (rush/final) from screenshot mapping
    -- Use FIRST stage if multiple screenshots match same hand
    SELECT 
        hand_id,
        FIRST(stage) AS stage
    FROM (
        SELECT 
            shm.hand_id,
            s.stage
        FROM poker.silver.screenshot_hand_mapping shm
        JOIN poker.silver.screenshots s ON shm.file_name = s.file_name
    )
    GROUP BY hand_id
)

SELECT 
    hs.hand_id,
    
    -- Hand strength features
    CAST(GREATEST(hs.card1_rank, hs.card2_rank) AS DOUBLE) AS high_card_rank,
    CAST(LEAST(hs.card1_rank, hs.card2_rank) AS DOUBLE) AS low_card_rank,
    CAST(hs.is_suited AS DOUBLE) AS is_suited,
    CAST(hs.is_pocket_pair AS DOUBLE) AS is_pocket_pair,
    -- Hand strength score (simplified Sklansky-like)
    CAST(
        GREATEST(hs.card1_rank, hs.card2_rank) + 
        LEAST(hs.card1_rank, hs.card2_rank) * 0.5 + 
        hs.is_suited * 2 + 
        hs.is_pocket_pair * 5
    AS DOUBLE) AS hand_strength_score,
    
    -- Stage feature (rush=0, final=1)
    CAST(CASE WHEN stg.stage = 'final' THEN 1 ELSE 0 END AS DOUBLE) AS is_final_stage,
    
    -- Position features
    CAST(hero.position_encoded AS DOUBLE) AS position_encoded,
    CAST(hero.is_late_position AS DOUBLE) AS is_late_position,
    
    -- Stack features
    CAST(COALESCE(hero.stack_bb, 20) AS DOUBLE) AS stack_bb,
    
    -- Opponent features
    CAST(COALESCE(opp.opponent_count, 5) AS DOUBLE) AS opponent_count,
    CAST(COALESCE(opp.avg_opponent_cluster, 1) AS DOUBLE) AS avg_opponent_cluster,
    
    -- Action features
    CAST(COALESCE(pf.had_preflop_raise, 0) AS DOUBLE) AS had_preflop_raise,
    CAST(COALESCE(pf.hero_raised_preflop, 0) AS DOUBLE) AS hero_raised_preflop,
    CAST(COALESCE(pf.preflop_raise_count, 0) AS DOUBLE) AS preflop_raise_count,
    
    -- Target: did hero win? (1=won, 0=lost/folded)
    CASE WHEN hero.hero_won = true THEN 1 ELSE 0 END AS won_hand,
    
    -- Target: profit in BB (for regression)
    -- net_profit is in chips, divide by big_blind to get BB
    CAST(COALESCE(hero.net_profit, 0) AS DOUBLE) / NULLIF(hero.big_blind, 0) AS profit_bb,
    
    -- Metadata
    hero.hand_timestamp AS feature_timestamp
    
FROM hand_strength hs
JOIN hero_stats hero ON hs.hand_id = hero.hand_id
LEFT JOIN opponent_info opp ON hs.hand_id = opp.hand_id
LEFT JOIN preflop_action pf ON hs.hand_id = pf.hand_id
LEFT JOIN hand_stage stg ON hs.hand_id = stg.hand_id
WHERE hero.stack_bb IS NOT NULL
  AND hero.stack_bb > 0
""")

print(f"Total hands with features: {df_features.count()}")

# COMMAND ----------

# Preview features
display(df_features.limit(20))

# COMMAND ----------

# Check if Feature Table exists
table_exists = spark.catalog.tableExists(FEATURE_TABLE_NAME)

if not table_exists:
    fe.create_table(
        name=FEATURE_TABLE_NAME,
        primary_keys=["hand_id"],
        df=df_features,
        description="Hand-level features for outcome prediction model"
    )
    print(f"Created new feature table: {FEATURE_TABLE_NAME}")
else:
    # Feature Store only supports 'merge' - delete and recreate for full refresh
    spark.sql(f"DROP TABLE IF EXISTS {FEATURE_TABLE_NAME}")
    fe.create_table(
        name=FEATURE_TABLE_NAME,
        primary_keys=["hand_id"],
        df=df_features,
        description="Hand-level features for outcome prediction model"
    )
    print(f"Recreated feature table: {FEATURE_TABLE_NAME}")

# COMMAND ----------

# Verify
print(f"\nFeature Table row count: {spark.table(FEATURE_TABLE_NAME).count()}")
display(spark.sql(f"DESCRIBE {FEATURE_TABLE_NAME}"))
