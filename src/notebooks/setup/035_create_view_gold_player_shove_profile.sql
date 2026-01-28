-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: player_shove_profile
-- MAGIC 
-- MAGIC Detailed analysis of player's all-in (shove) hands - what cards they showed.
-- MAGIC 
-- MAGIC **Categories:**
-- MAGIC - Stack depth: short (≤10BB), medium (10-20BB), deep (>20BB)
-- MAGIC - Position: early, middle, late, blinds
-- MAGIC - Hand type: premium, broadway, pocket pairs, suited connectors, Ax, trash
-- MAGIC 
-- MAGIC **Note:** Only hands that went to showdown are included (hole_cards visible).

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.player_shove_profile AS

WITH shove_hands AS (
    SELECT 
        phs.player_name,
        phs.hand_id,
        phs.stage,
        phs.position,
        phs.position_group,
        phs.stack_bb,
        phs.hole_cards,
        phs.won_hand,
        phs.net_profit,
        -- Stack depth category
        CASE 
            WHEN phs.stack_bb <= 10 THEN 'short'
            WHEN phs.stack_bb <= 20 THEN 'medium'
            ELSE 'deep'
        END AS stack_category,
        -- Parse hole cards (e.g., "7sAd" -> card1=7s, card2=Ad)
        SUBSTRING(phs.hole_cards, 1, 2) AS card1,
        SUBSTRING(phs.hole_cards, 3, 2) AS card2,
        -- Extract ranks
        SUBSTRING(phs.hole_cards, 1, 1) AS rank1,
        SUBSTRING(phs.hole_cards, 3, 1) AS rank2,
        -- Extract suits
        SUBSTRING(phs.hole_cards, 2, 1) AS suit1,
        SUBSTRING(phs.hole_cards, 4, 1) AS suit2
    FROM poker.gold.player_hand_stats phs
    WHERE phs.went_allin = true 
      AND phs.preflop_shove = true
      AND phs.hole_cards IS NOT NULL
      AND phs.hole_cards != ''
),

categorized AS (
    SELECT 
        sh.*,
        -- Is suited?
        CASE WHEN sh.suit1 = sh.suit2 THEN true ELSE false END AS is_suited,
        -- Is pocket pair?
        CASE WHEN sh.rank1 = sh.rank2 THEN true ELSE false END AS is_pocket_pair,
        -- Rank values for comparison (A=14, K=13, Q=12, J=11, T=10)
        CASE sh.rank1 
            WHEN 'A' THEN 14 WHEN 'K' THEN 13 WHEN 'Q' THEN 12 WHEN 'J' THEN 11 WHEN 'T' THEN 10
            ELSE CAST(sh.rank1 AS INT)
        END AS rank1_val,
        CASE sh.rank2
            WHEN 'A' THEN 14 WHEN 'K' THEN 13 WHEN 'Q' THEN 12 WHEN 'J' THEN 11 WHEN 'T' THEN 10
            ELSE CAST(sh.rank2 AS INT)
        END AS rank2_val
    FROM shove_hands sh
),

final AS (
    SELECT 
        c.*,
        -- Hand category
        CASE 
            -- Premium: AA, KK, QQ, JJ, AKs, AKo
            WHEN c.is_pocket_pair AND c.rank1_val >= 11 THEN 'premium_pair'
            WHEN c.rank1_val = 14 AND c.rank2_val = 13 THEN 'premium_broadway'
            -- Strong: TT-99, AQ, AJ, KQ
            WHEN c.is_pocket_pair AND c.rank1_val >= 9 THEN 'medium_pair'
            WHEN c.rank1_val = 14 AND c.rank2_val >= 11 THEN 'strong_ace'
            WHEN c.rank1_val = 13 AND c.rank2_val = 12 THEN 'strong_broadway'
            -- Medium pairs
            WHEN c.is_pocket_pair AND c.rank1_val >= 6 THEN 'small_pair'
            WHEN c.is_pocket_pair THEN 'micro_pair'
            -- Ax hands
            WHEN c.rank1_val = 14 OR c.rank2_val = 14 THEN 
                CASE WHEN c.is_suited THEN 'Axs' ELSE 'Axo' END
            -- Broadway (T+)
            WHEN c.rank1_val >= 10 AND c.rank2_val >= 10 THEN 
                CASE WHEN c.is_suited THEN 'broadway_suited' ELSE 'broadway_offsuit' END
            -- Suited connectors
            WHEN c.is_suited AND ABS(c.rank1_val - c.rank2_val) = 1 THEN 'suited_connector'
            -- Suited gapper
            WHEN c.is_suited AND ABS(c.rank1_val - c.rank2_val) <= 3 THEN 'suited_gapper'
            -- Kx suited
            WHEN c.is_suited AND (c.rank1_val = 13 OR c.rank2_val = 13) THEN 'Kxs'
            -- Trash
            ELSE 'trash'
        END AS hand_category,
        -- Simplified for aggregation: hand notation (e.g., "A7s", "QQ", "JTo")
        CASE 
            WHEN c.is_pocket_pair THEN CONCAT(c.rank1, c.rank1)
            WHEN c.rank1_val > c.rank2_val THEN 
                CONCAT(c.rank1, c.rank2, CASE WHEN c.is_suited THEN 's' ELSE 'o' END)
            ELSE 
                CONCAT(c.rank2, c.rank1, CASE WHEN c.is_suited THEN 's' ELSE 'o' END)
        END AS hand_notation
    FROM categorized c
)

SELECT 
    player_name,
    hand_id,
    stage,
    position,
    position_group,
    stack_bb,
    stack_category,
    hole_cards,
    hand_notation,
    hand_category,
    is_suited,
    is_pocket_pair,
    won_hand,
    net_profit
FROM final
ORDER BY player_name, stack_bb;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Aggregated Shove Statistics per Player
-- MAGIC 
-- MAGIC Summary view for quick player profiling.

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.player_shove_summary AS

SELECT 
    player_name,
    stage,
    stack_category,
    position_group,
    COUNT(*) AS total_shoves,
    ROUND(100.0 * SUM(CASE WHEN won_hand = true THEN 1 ELSE 0 END) / COUNT(*), 1) AS win_rate,
    -- Hand category distribution
    ROUND(100.0 * SUM(CASE WHEN hand_category IN ('premium_pair', 'premium_broadway') THEN 1 ELSE 0 END) / COUNT(*), 1) AS premium_pct,
    ROUND(100.0 * SUM(CASE WHEN hand_category IN ('medium_pair', 'strong_ace', 'strong_broadway') THEN 1 ELSE 0 END) / COUNT(*), 1) AS strong_pct,
    ROUND(100.0 * SUM(CASE WHEN hand_category IN ('small_pair', 'micro_pair') THEN 1 ELSE 0 END) / COUNT(*), 1) AS small_pair_pct,
    ROUND(100.0 * SUM(CASE WHEN hand_category IN ('Axs', 'Axo') THEN 1 ELSE 0 END) / COUNT(*), 1) AS ax_pct,
    ROUND(100.0 * SUM(CASE WHEN hand_category = 'trash' THEN 1 ELSE 0 END) / COUNT(*), 1) AS trash_pct,
    -- Actual hands shown (for detailed review)
    CONCAT_WS(', ', COLLECT_LIST(hand_notation)) AS hands_shown
FROM poker.gold.player_shove_profile
GROUP BY player_name, stage, stack_category, position_group
HAVING COUNT(*) >= 1
ORDER BY player_name, stage, stack_category, position_group;
