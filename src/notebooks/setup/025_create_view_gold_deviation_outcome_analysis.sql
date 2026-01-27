-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: deviation_outcome_analysis
-- MAGIC 
-- MAGIC Detailed analysis of what happens when hero deviates from GPT:
-- MAGIC - Ignored fold → called/raised: win rate?
-- MAGIC - Ignored call/raise → folded: missed value?
-- MAGIC - All-in vs call when deviating: which is better?

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.deviation_outcome_analysis AS

WITH player_counts AS (
    SELECT file_name, COUNT(*) + 1 AS players_in_hand
    FROM poker.silver.screenshot_players
    GROUP BY file_name
),

decisions AS (
    SELECT 
        s.file_name,
        s.stage,
        s.street,
        s.gpt_action,
        s.gpt_recommendation,
        s.hero_stack,
        s.pot,
        s.to_call,
        m.hand_id,
        h.tournament_id,
        h.hero_result,
        h.big_blind,
        h.total_pot,
        hp.amount_won,
        hp.chips_start AS hero_chips_start,
        COALESCE(pc.players_in_hand, 1) AS players_in_hand
    FROM poker.silver.screenshots s
    JOIN poker.silver.screenshot_hand_mapping m ON s.file_name = m.file_name
    JOIN poker.silver.hands h ON m.hand_id = h.hand_id
    JOIN poker.silver.hand_players hp ON h.hand_id = hp.hand_id AND hp.is_hero = true
    LEFT JOIN player_counts pc ON s.file_name = pc.file_name
    WHERE s.gpt_action IS NOT NULL
),

with_actions AS (
    SELECT 
        d.*,
        ha.action_type AS hero_action,
        ha.amount AS hero_amount,
        -- Is this an all-in?
        CASE WHEN ha.amount >= d.hero_chips_start * 0.95 THEN true ELSE false END AS hero_went_allin,
        -- Compliance
        CASE
            WHEN d.gpt_action = 'fold' AND ha.action_type = 'fold' THEN 'followed_fold'
            WHEN d.gpt_action = 'fold' AND ha.action_type IN ('call', 'raise', 'bet') THEN 'ignored_fold'
            WHEN d.gpt_action IN ('call', 'raise', 'bet') AND ha.action_type IN ('call', 'raise', 'bet') THEN 'followed_play'
            WHEN d.gpt_action IN ('call', 'raise', 'bet') AND ha.action_type = 'fold' THEN 'ignored_play'
            ELSE 'unknown'
        END AS decision_type
    FROM decisions d
    LEFT JOIN poker.silver.hand_actions ha ON d.hand_id = ha.hand_id 
        AND ha.is_hero = true 
        AND ha.street = d.street
        AND ha.action_type NOT IN ('post_ante', 'post_sb', 'post_bb', 'show', 'check')
)

SELECT
    decision_type,
    stage,
    CASE WHEN players_in_hand = 2 THEN 'heads_up' ELSE 'multiway' END AS table_type,
    hero_action,
    hero_went_allin,
    
    -- Counts
    COUNT(*) AS total,
    
    -- Results
    SUM(CASE WHEN hero_result = 'won' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN hero_result = 'lost' THEN 1 ELSE 0 END) AS losses,
    SUM(CASE WHEN hero_result = 'folded' THEN 1 ELSE 0 END) AS folded_later,
    
    -- Win rate
    ROUND(SUM(CASE WHEN hero_result = 'won' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_rate_pct,
    
    -- Profit
    ROUND(SUM(CASE WHEN hero_result = 'won' THEN amount_won / big_blind ELSE 0 END), 1) AS total_won_bb,
    ROUND(AVG(CASE WHEN hero_result = 'won' THEN amount_won / big_blind END), 1) AS avg_won_bb

FROM with_actions
WHERE decision_type != 'unknown'
GROUP BY decision_type, stage, 
    CASE WHEN players_in_hand = 2 THEN 'heads_up' ELSE 'multiway' END,
    hero_action, hero_went_allin
HAVING COUNT(*) >= 3  -- At least 3 samples
ORDER BY decision_type, stage, table_type, total DESC;
