-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: player_hand_stats
-- MAGIC 
-- MAGIC Per-hand statistics for each player - for debugging and verification.
-- MAGIC 
-- MAGIC **Data sources:**
-- MAGIC - `hand_players` - VPIP, PFR, 3bet, position, chips_start
-- MAGIC - `hand_actions` - all actions with amounts, is_allin
-- MAGIC - `hands` - big_blind for BB conversion
-- MAGIC - `screenshots` - stage (rush/final) via mapping
-- MAGIC - `player_identity` - per-hand mapping of real names to anonymous IDs
-- MAGIC 
-- MAGIC Use this view to verify stats for a specific hand:
-- MAGIC ```sql
-- MAGIC SELECT * FROM poker.gold.player_hand_stats WHERE hand_id = 'BR1234567890'
-- MAGIC ```

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.player_hand_stats AS

-- Action-level aggregation per player per hand
WITH action_agg AS (
    SELECT 
        ha.hand_id,
        ha.player_name AS anonymous_id,
        -- All-in flag
        MAX(CASE WHEN ha.is_allin = true THEN 1 ELSE 0 END) AS went_allin,
        -- Preflop raise size
        MAX(CASE WHEN ha.street = 'preflop' AND ha.action_type = 'raise' THEN ha.amount END) AS preflop_raise_amount,
        MAX(CASE WHEN ha.street = 'preflop' AND ha.action_type = 'raise' AND ha.is_allin = true THEN 1 ELSE 0 END) AS preflop_shove,
        -- Action counts by street
        SUM(CASE WHEN ha.street = 'preflop' AND ha.action_type IN ('bet', 'raise') THEN 1 ELSE 0 END) AS preflop_aggr,
        SUM(CASE WHEN ha.street != 'preflop' AND ha.action_type IN ('bet', 'raise') THEN 1 ELSE 0 END) AS postflop_aggr,
        SUM(CASE WHEN ha.action_type = 'call' THEN 1 ELSE 0 END) AS total_calls_from_actions,
        -- Average sizing
        AVG(CASE WHEN ha.action_type IN ('bet', 'raise') THEN ha.amount END) AS avg_action_amount
    FROM poker.silver.hand_actions ha
    GROUP BY ha.hand_id, ha.player_name
)

SELECT 
    -- Identifiers
    pi.player_name,
    hp.hand_id,
    h.hand_timestamp,
    h.tournament_id,
    
    -- Context
    s.stage,
    hp.position,
    CASE 
        WHEN hp.position IN ('UTG', 'UTG+1', 'EP') THEN 'early'
        WHEN hp.position IN ('MP', 'MP+1', 'HJ') THEN 'middle'
        WHEN hp.position IN ('CO', 'BTN') THEN 'late'
        WHEN hp.position IN ('SB', 'BB') THEN 'blinds'
        ELSE 'unknown'
    END AS position_group,
    
    -- Stack info
    hp.chips_start,
    h.big_blind,
    ROUND(hp.chips_start / NULLIF(h.big_blind, 0), 1) AS stack_bb,
    
    -- Hole cards (if shown)
    hp.hole_cards,
    
    -- Preflop flags from hand_players
    hp.vpip,
    hp.pfr,
    hp.three_bet,
    
    -- Action counts from hand_players
    hp.total_bets,
    hp.total_raises,
    hp.total_calls,
    
    -- All-in from actions
    CASE WHEN aa.went_allin = 1 THEN true ELSE false END AS went_allin,
    
    -- Preflop sizing (in BB)
    ROUND(aa.preflop_raise_amount / NULLIF(h.big_blind, 0), 1) AS preflop_raise_bb,
    CASE WHEN aa.preflop_shove = 1 THEN true ELSE false END AS preflop_shove,
    
    -- Average action size (in BB)
    ROUND(aa.avg_action_amount / NULLIF(h.big_blind, 0), 1) AS avg_action_size_bb,
    
    -- Result
    hp.went_to_showdown,
    hp.won_hand,
    hp.amount_won,
    hp.net_profit,
    hp.result_description

FROM poker.silver.player_identity pi
JOIN poker.silver.hand_players hp 
    ON pi.hand_id = hp.hand_id 
    AND pi.anonymous_id = hp.player_name
JOIN poker.silver.hands h 
    ON hp.hand_id = h.hand_id
LEFT JOIN action_agg aa 
    ON hp.hand_id = aa.hand_id AND hp.player_name = aa.anonymous_id
LEFT JOIN poker.silver.screenshots s 
    ON pi.file_name = s.file_name;

