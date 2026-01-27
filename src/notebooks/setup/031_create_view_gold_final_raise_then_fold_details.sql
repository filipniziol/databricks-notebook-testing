-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: final_raise_then_fold_details
-- MAGIC 
-- MAGIC Detailed list of cases where hero raised small then folded to re-raise

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.final_raise_then_fold_details AS

WITH small_raise_situations AS (
    SELECT 
        s.file_name,
        s.hero_cards,
        s.hero_pos,
        s.hero_stack,
        s.gpt_recommendation,
        m.hand_id,
        h.hero_result,
        h.big_blind,
        hp.net_profit
    FROM poker.silver.screenshots s
    LEFT JOIN poker.silver.screenshot_hand_mapping m ON s.file_name = m.file_name
    LEFT JOIN poker.silver.hands h ON m.hand_id = h.hand_id
    LEFT JOIN poker.silver.hand_players hp ON h.hand_id = hp.hand_id AND hp.is_hero = true
    WHERE s.stage = 'final'
      AND s.street = 'preflop'
      AND s.gpt_action = 'raise'
      AND (s.gpt_recommendation LIKE '%2.%BB%' OR s.gpt_recommendation LIKE '%2 BB%' OR s.gpt_recommendation LIKE '%3%BB%')
),

with_actions AS (
    SELECT 
        sr.*,
        MAX(CASE WHEN ha.street = 'preflop' AND ha.action_type = 'raise' THEN 1 ELSE 0 END) AS hero_raised,
        MAX(CASE WHEN ha.action_type = 'fold' THEN 1 ELSE 0 END) AS hero_folded
    FROM small_raise_situations sr
    LEFT JOIN poker.silver.hand_actions ha ON sr.hand_id = ha.hand_id AND ha.is_hero = true
        AND ha.action_type NOT IN ('post_ante', 'post_sb', 'post_bb', 'show')
    GROUP BY sr.file_name, sr.hero_cards, sr.hero_pos, sr.hero_stack, sr.gpt_recommendation,
             sr.hand_id, sr.hero_result, sr.big_blind, sr.net_profit
)

SELECT 
    file_name,
    hero_cards,
    hero_pos,
    hero_stack,
    gpt_recommendation,
    hero_result,
    ROUND(net_profit / big_blind, 1) AS net_profit_bb,
    CASE WHEN hero_raised = 1 AND hero_folded = 1 THEN 'RAISED THEN FOLDED' ELSE 'played through' END AS sequence
FROM with_actions
WHERE hero_raised = 1 AND hero_folded = 1
ORDER BY net_profit DESC;
