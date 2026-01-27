-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: rush_allin_call_analysis
-- MAGIC 
-- MAGIC When to_call > 50% of hero stack and GPT recommends call/raise in Rush stage
-- MAGIC Groups by normalized hand type (e.g., AKs, 76s, JJ)

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.rush_allin_call_analysis AS

WITH allin_situations AS (
    SELECT 
        s.file_name,
        s.screenshot_at,
        s.hero_cards,
        s.hero_stack,
        s.to_call,
        ROUND(s.to_call * 100.0 / NULLIF(s.hero_stack, 0), 1) AS pct_of_stack,
        s.pot,
        s.gpt_action,
        s.gpt_recommendation,
        m.hand_id,
        h.hero_result,
        h.big_blind,
        hp.net_profit,
        ha.action_type AS hero_action
    FROM poker.silver.screenshots s
    LEFT JOIN poker.silver.screenshot_hand_mapping m ON s.file_name = m.file_name
    LEFT JOIN poker.silver.hands h ON m.hand_id = h.hand_id
    LEFT JOIN poker.silver.hand_players hp ON h.hand_id = hp.hand_id AND hp.is_hero = true
    LEFT JOIN poker.silver.hand_actions ha ON h.hand_id = ha.hand_id 
        AND ha.is_hero = true 
        AND ha.street = s.street
        AND ha.action_type NOT IN ('post_ante', 'post_sb', 'post_bb', 'show')
    WHERE s.stage = 'rush'
      AND s.to_call > s.hero_stack * 0.5  -- All-in or near all-in situation
      AND s.gpt_action IN ('call', 'raise')  -- GPT says to play
),

-- Normalize hands (e.g., AhKs -> AKo, 7d7c -> 77)
normalized AS (
    SELECT 
        *,
        -- Extract ranks and suits
        SUBSTR(hero_cards, 1, 1) AS rank1,
        SUBSTR(hero_cards, 2, 1) AS suit1,
        SUBSTR(hero_cards, 3, 1) AS rank2,
        SUBSTR(hero_cards, 4, 1) AS suit2,
        -- Check if suited
        CASE WHEN SUBSTR(hero_cards, 2, 1) = SUBSTR(hero_cards, 4, 1) THEN true ELSE false END AS is_suited,
        -- Check if pair
        CASE WHEN SUBSTR(hero_cards, 1, 1) = SUBSTR(hero_cards, 3, 1) THEN true ELSE false END AS is_pair
    FROM allin_situations
),

-- Build hand notation with proper rank ordering
with_notation AS (
    SELECT 
        *,
        -- Assign numeric values to ranks for proper ordering
        CASE rank1 
            WHEN 'A' THEN 14 WHEN 'K' THEN 13 WHEN 'Q' THEN 12 WHEN 'J' THEN 11 WHEN 'T' THEN 10
            ELSE CAST(rank1 AS INT)
        END AS rank1_val,
        CASE rank2
            WHEN 'A' THEN 14 WHEN 'K' THEN 13 WHEN 'Q' THEN 12 WHEN 'J' THEN 11 WHEN 'T' THEN 10
            ELSE CAST(rank2 AS INT)
        END AS rank2_val
    FROM normalized
),

with_hand_type AS (
    SELECT 
        *,
        CASE 
            WHEN is_pair THEN CONCAT(rank1, rank1)
            WHEN is_suited THEN 
                CASE 
                    WHEN rank1_val >= rank2_val THEN CONCAT(rank1, rank2, 's')
                    ELSE CONCAT(rank2, rank1, 's')
                END
            ELSE 
                CASE 
                    WHEN rank1_val >= rank2_val THEN CONCAT(rank1, rank2, 'o')
                    ELSE CONCAT(rank2, rank1, 'o')
                END
        END AS hand_type
    FROM with_notation
)

SELECT
    hand_type,
    
    -- Sample size
    COUNT(*) AS total_situations,
    
    -- Did hero follow GPT?
    SUM(CASE WHEN hero_action IN ('call', 'raise') THEN 1 ELSE 0 END) AS hero_played,
    SUM(CASE WHEN hero_action = 'fold' THEN 1 ELSE 0 END) AS hero_folded,
    
    -- Results when hero played
    SUM(CASE WHEN hero_action IN ('call', 'raise') AND hero_result = 'won' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN hero_action IN ('call', 'raise') AND hero_result IN ('lost', 'folded') THEN 1 ELSE 0 END) AS losses,
    
    -- Win rate
    ROUND(SUM(CASE WHEN hero_action IN ('call', 'raise') AND hero_result = 'won' THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN hero_action IN ('call', 'raise') THEN 1 ELSE 0 END), 0), 1) AS win_rate_pct,
    
    -- Profit
    ROUND(SUM(CASE WHEN hero_action IN ('call', 'raise') THEN net_profit / big_blind ELSE 0 END), 1) AS total_net_bb,
    ROUND(AVG(CASE WHEN hero_action IN ('call', 'raise') THEN net_profit / big_blind END), 1) AS avg_net_bb_per_hand,
    
    -- Average stack commitment
    ROUND(AVG(pct_of_stack), 1) AS avg_pct_of_stack

FROM with_hand_type
GROUP BY hand_type
HAVING COUNT(*) >= 2  -- At least 2 occurrences
ORDER BY total_net_bb DESC;
