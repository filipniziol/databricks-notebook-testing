-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: rush_allin_call_details
-- MAGIC 
-- MAGIC Detailed list of Rush all-in call situations with opponent info

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.rush_allin_call_details AS

WITH hand_notation AS (
    SELECT 
        s.*,
        -- Normalize hand notation
        CASE SUBSTR(s.hero_cards, 1, 1) 
            WHEN 'A' THEN 14 WHEN 'K' THEN 13 WHEN 'Q' THEN 12 WHEN 'J' THEN 11 WHEN 'T' THEN 10
            ELSE CAST(SUBSTR(s.hero_cards, 1, 1) AS INT)
        END AS rank1_val,
        CASE SUBSTR(s.hero_cards, 3, 1)
            WHEN 'A' THEN 14 WHEN 'K' THEN 13 WHEN 'Q' THEN 12 WHEN 'J' THEN 11 WHEN 'T' THEN 10
            ELSE CAST(SUBSTR(s.hero_cards, 3, 1) AS INT)
        END AS rank2_val,
        SUBSTR(s.hero_cards, 1, 1) AS rank1,
        SUBSTR(s.hero_cards, 3, 1) AS rank2,
        CASE WHEN SUBSTR(s.hero_cards, 2, 1) = SUBSTR(s.hero_cards, 4, 1) THEN 's' ELSE 'o' END AS suited_flag,
        CASE WHEN SUBSTR(s.hero_cards, 1, 1) = SUBSTR(s.hero_cards, 3, 1) THEN true ELSE false END AS is_pair
    FROM poker.silver.screenshots s
    WHERE s.stage = 'rush'
      AND s.to_call > s.hero_stack * 0.5
      AND s.gpt_action IN ('call', 'raise')
),

with_hand_type AS (
    SELECT 
        *,
        CASE 
            WHEN is_pair THEN CONCAT(rank1, rank1)
            WHEN rank1_val >= rank2_val THEN CONCAT(rank1, rank2, suited_flag)
            ELSE CONCAT(rank2, rank1, suited_flag)
        END AS hand_type
    FROM hand_notation
),

-- Get first meaningful hero action per hand/street
first_hero_action AS (
    SELECT 
        hand_id,
        street,
        FIRST_VALUE(action_type) OVER (
            PARTITION BY hand_id, street 
            ORDER BY action_order
        ) AS hero_action
    FROM poker.silver.hand_actions
    WHERE is_hero = true
      AND action_type IN ('fold', 'call', 'raise', 'check', 'bet')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY hand_id, street ORDER BY action_order) = 1
)

SELECT 
    h.hand_type,
    h.hero_cards,
    h.hero_pos,
    h.hero_stack,
    h.to_call,
    ROUND(h.to_call * 100.0 / NULLIF(h.hero_stack, 0), 1) AS pct_of_stack,
    h.pot,
    h.gpt_action,
    h.gpt_recommendation,
    hd.hero_result,
    fha.hero_action,
    ROUND(hp.net_profit / hd.big_blind, 1) AS net_profit_bb,
    h.file_name,
    h.screenshot_at
FROM with_hand_type h
LEFT JOIN poker.silver.screenshot_hand_mapping m ON h.file_name = m.file_name
LEFT JOIN poker.silver.hands hd ON m.hand_id = hd.hand_id
LEFT JOIN poker.silver.hand_players hp ON hd.hand_id = hp.hand_id AND hp.is_hero = true
LEFT JOIN first_hero_action fha ON hd.hand_id = fha.hand_id AND fha.street = h.street
ORDER BY h.hand_type, h.screenshot_at;
