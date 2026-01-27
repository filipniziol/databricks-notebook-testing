-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: headsup_analysis
-- MAGIC 
-- MAGIC Specific analysis of heads-up play:
-- MAGIC - GPT compliance in heads-up vs multiway
-- MAGIC - GPT recommendation patterns in heads-up
-- MAGIC - Win rate when following vs ignoring GPT in heads-up

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.headsup_analysis AS

WITH player_counts AS (
    SELECT file_name, COUNT(*) + 1 AS players_count
    FROM poker.silver.screenshot_players
    GROUP BY file_name
),

headsup_hands AS (
    SELECT 
        s.file_name,
        s.screenshot_at,
        s.stage,
        s.street,
        s.gpt_action,
        s.gpt_recommendation,
        s.hero_cards,
        s.hero_pos,
        s.hero_stack,
        s.pot,
        s.to_call,
        m.hand_id,
        h.tournament_id,
        h.hero_result,
        h.big_blind,
        h.total_pot,
        hp.amount_won,
        COALESCE(pc.players_count, 1) AS players_count
    FROM poker.silver.screenshots s
    JOIN poker.silver.screenshot_hand_mapping m ON s.file_name = m.file_name
    JOIN poker.silver.hands h ON m.hand_id = h.hand_id
    JOIN poker.silver.hand_players hp ON h.hand_id = hp.hand_id AND hp.is_hero = true
    LEFT JOIN player_counts pc ON s.file_name = pc.file_name
    WHERE s.gpt_action IS NOT NULL
),

with_actions AS (
    SELECT 
        hh.*,
        ha.action_type AS hero_action,
        CASE
            WHEN hh.gpt_action = 'fold' AND ha.action_type = 'fold' THEN 'followed_fold'
            WHEN hh.gpt_action = 'fold' AND ha.action_type IN ('call', 'raise', 'bet') THEN 'ignored_fold'
            WHEN hh.gpt_action IN ('call', 'raise', 'bet') AND ha.action_type IN ('call', 'raise', 'bet') THEN 'followed_play'
            WHEN hh.gpt_action IN ('call', 'raise', 'bet') AND ha.action_type = 'fold' THEN 'ignored_play'
            ELSE 'unknown'
        END AS decision_type,
        CASE WHEN hh.players_count = 2 THEN true ELSE false END AS is_headsup
    FROM headsup_hands hh
    LEFT JOIN poker.silver.hand_actions ha ON hh.hand_id = ha.hand_id 
        AND ha.is_hero = true 
        AND ha.street = hh.street
        AND ha.action_type NOT IN ('post_ante', 'post_sb', 'post_bb', 'show')
)

-- Compare heads-up vs multiway
SELECT
    is_headsup,
    stage,
    gpt_action AS gpt_recommended,
    
    -- Total decisions
    COUNT(*) AS total_decisions,
    
    -- GPT recommendation distribution
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY is_headsup, stage), 1) AS pct_of_recommendations,
    
    -- Compliance
    SUM(CASE WHEN decision_type IN ('followed_fold', 'followed_play') THEN 1 ELSE 0 END) AS followed_count,
    ROUND(SUM(CASE WHEN decision_type IN ('followed_fold', 'followed_play') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS compliance_pct,
    
    -- When followed GPT
    SUM(CASE WHEN decision_type IN ('followed_fold', 'followed_play') AND hero_result = 'won' THEN 1 ELSE 0 END) AS wins_when_followed,
    ROUND(SUM(CASE WHEN decision_type IN ('followed_fold', 'followed_play') AND hero_result = 'won' THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN decision_type IN ('followed_fold', 'followed_play') THEN 1 ELSE 0 END), 0), 1) AS win_rate_when_followed,
    
    -- When deviated
    SUM(CASE WHEN decision_type IN ('ignored_fold', 'ignored_play') AND hero_result = 'won' THEN 1 ELSE 0 END) AS wins_when_deviated,
    ROUND(SUM(CASE WHEN decision_type IN ('ignored_fold', 'ignored_play') AND hero_result = 'won' THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN decision_type IN ('ignored_fold', 'ignored_play') THEN 1 ELSE 0 END), 0), 1) AS win_rate_when_deviated

FROM with_actions
WHERE decision_type != 'unknown'
GROUP BY is_headsup, stage, gpt_action
ORDER BY is_headsup DESC, stage, total_decisions DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Heads-Up Specific: GPT Says Fold Too Much?

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.headsup_fold_analysis AS

WITH player_counts AS (
    SELECT file_name, COUNT(*) AS opponent_count
    FROM poker.silver.screenshot_players
    GROUP BY file_name
),

headsup_folds AS (
    SELECT 
        s.file_name,
        s.stage,
        s.street,
        s.gpt_action,
        s.hero_cards,
        s.hero_pos,
        s.pot,
        s.to_call,
        m.hand_id,
        h.hero_result,
        h.big_blind,
        hp.net_profit,
        ha.action_type AS hero_action
    FROM poker.silver.screenshots s
    JOIN poker.silver.screenshot_hand_mapping m ON s.file_name = m.file_name
    JOIN poker.silver.hands h ON m.hand_id = h.hand_id
    JOIN poker.silver.hand_players hp ON h.hand_id = hp.hand_id AND hp.is_hero = true
    LEFT JOIN poker.silver.hand_actions ha ON h.hand_id = ha.hand_id 
        AND ha.is_hero = true 
        AND ha.street = s.street
        AND ha.action_type NOT IN ('post_ante', 'post_sb', 'post_bb', 'show')
    JOIN player_counts pc ON s.file_name = pc.file_name AND pc.opponent_count = 1  -- Heads-up only
    WHERE s.gpt_action = 'fold'  -- GPT recommended fold
)

SELECT
    stage,
    street,
    
    -- How often GPT says fold in heads-up
    COUNT(*) AS gpt_fold_recommendations,
    
    -- Hero compliance with fold
    SUM(CASE WHEN hero_action = 'fold' THEN 1 ELSE 0 END) AS hero_folded,
    SUM(CASE WHEN hero_action IN ('call', 'raise', 'bet') THEN 1 ELSE 0 END) AS hero_played,
    
    -- Results when ignored fold
    SUM(CASE WHEN hero_action IN ('call', 'raise', 'bet') AND hero_result = 'won' THEN 1 ELSE 0 END) AS won_when_ignored_fold,
    SUM(CASE WHEN hero_action IN ('call', 'raise', 'bet') AND hero_result IN ('lost', 'folded') THEN 1 ELSE 0 END) AS lost_when_ignored_fold,
    
    -- Win rate when ignoring fold in heads-up
    ROUND(SUM(CASE WHEN hero_action IN ('call', 'raise', 'bet') AND hero_result = 'won' THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN hero_action IN ('call', 'raise', 'bet') THEN 1 ELSE 0 END), 0), 1) AS win_rate_when_ignored_fold_pct,
    
    -- Average BB profit when won
    ROUND(AVG(CASE WHEN hero_action IN ('call', 'raise', 'bet') AND hero_result = 'won' THEN net_profit / big_blind END), 1) AS avg_won_bb,
    
    -- Average BB lost when lost (should be negative)
    ROUND(AVG(CASE WHEN hero_action IN ('call', 'raise', 'bet') AND hero_result IN ('lost', 'folded') THEN net_profit / big_blind END), 1) AS avg_lost_bb,
    
    -- Total net BB when ignoring fold
    ROUND(SUM(CASE WHEN hero_action IN ('call', 'raise', 'bet') THEN net_profit / big_blind ELSE 0 END), 1) AS total_net_bb_ignoring_fold,
    
    -- Average net BB per hand when ignoring fold
    ROUND(AVG(CASE WHEN hero_action IN ('call', 'raise', 'bet') THEN net_profit / big_blind END), 1) AS avg_net_bb_per_hand

FROM headsup_folds
GROUP BY stage, street
ORDER BY stage, gpt_fold_recommendations DESC;
