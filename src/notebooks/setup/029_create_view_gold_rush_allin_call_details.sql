-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: rush_allin_call_details
-- MAGIC 
-- MAGIC Detailed list of Rush all-in call situations

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.rush_allin_call_details AS

SELECT 
    s.file_name,
    s.hero_cards,
    s.hero_stack,
    s.to_call,
    ROUND(s.to_call * 100.0 / NULLIF(s.hero_stack, 0), 1) AS pct_of_stack,
    s.pot,
    s.gpt_action,
    h.hero_result,
    ha.action_type AS hero_action,
    ROUND(hp.net_profit / h.big_blind, 1) AS net_profit_bb
FROM poker.silver.screenshots s
LEFT JOIN poker.silver.screenshot_hand_mapping m ON s.file_name = m.file_name
LEFT JOIN poker.silver.hands h ON m.hand_id = h.hand_id
LEFT JOIN poker.silver.hand_players hp ON h.hand_id = hp.hand_id AND hp.is_hero = true
LEFT JOIN poker.silver.hand_actions ha ON h.hand_id = ha.hand_id 
    AND ha.is_hero = true 
    AND ha.street = s.street
    AND ha.action_type NOT IN ('post_ante', 'post_sb', 'post_bb', 'show')
WHERE s.stage = 'rush'
  AND s.to_call > s.hero_stack * 0.5
  AND s.gpt_action IN ('call', 'raise')
ORDER BY s.hero_stack DESC;
