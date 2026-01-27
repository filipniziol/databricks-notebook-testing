-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: hand_line_analysis
-- MAGIC Analyzes full hand line - all GPT recommendations vs hero actions per hand.
-- MAGIC 
-- MAGIC Shows deviations from GPT line (e.g. GPT said fold preflop, hero called, 
-- MAGIC then GPT said fold flop, hero called again = consistent deviation pattern).

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.hand_line_analysis AS

WITH hand_screenshots AS (
    -- All screenshots for each hand, ordered by street
    SELECT
        m.hand_id,
        s.file_name,
        s.screenshot_at,
        s.street,
        s.gpt_action,
        s.gpt_recommendation,
        s.pot,
        s.to_call,
        s.hero_stack,
        ROW_NUMBER() OVER (PARTITION BY m.hand_id ORDER BY s.screenshot_at) AS screenshot_order,
        -- Street order for sorting
        CASE s.street 
            WHEN 'preflop' THEN 1 
            WHEN 'flop' THEN 2 
            WHEN 'turn' THEN 3 
            WHEN 'river' THEN 4 
            ELSE 5 
        END AS street_order
    FROM poker.silver.screenshot_hand_mapping m
    JOIN poker.silver.screenshots s ON m.file_name = s.file_name
    WHERE m.hand_id IS NOT NULL
),

hero_actions_by_street AS (
    SELECT 
        hand_id,
        street,
        -- Aggregate hero actions on this street
        COLLECT_LIST(action_type) AS hero_actions,
        MAX(CASE WHEN action_type = 'fold' THEN 1 ELSE 0 END) AS folded,
        MAX(CASE WHEN action_type IN ('call', 'bet', 'raise') THEN 1 ELSE 0 END) AS put_money_in,
        SUM(CASE WHEN action_type IN ('call', 'bet', 'raise') THEN amount ELSE 0 END) AS total_invested
    FROM poker.silver.hand_actions
    WHERE is_hero = true
    AND action_type NOT IN ('post_ante', 'post_sb', 'post_bb', 'show')
    GROUP BY hand_id, street
),

hand_summary AS (
    SELECT
        h.hand_id,
        h.hero_cards,
        h.hero_position,
        h.board,
        h.total_pot,
        h.hero_result,
        hp.amount_won,
        hp.chips_start
    FROM poker.silver.hands h
    JOIN poker.silver.hand_players hp ON h.hand_id = hp.hand_id AND hp.is_hero = true
),

-- Build the line per hand
hand_line AS (
    SELECT
        hs.hand_id,
        hs.street,
        hs.street_order,
        hs.gpt_action,
        hs.gpt_recommendation,
        hs.pot,
        hs.to_call,
        ha.hero_actions,
        ha.folded,
        ha.put_money_in,
        -- Did hero follow GPT on this street?
        CASE
            WHEN hs.gpt_action = 'fold' AND ha.folded = 1 THEN 'followed'
            WHEN hs.gpt_action = 'fold' AND ha.put_money_in = 1 THEN 'deviated'
            WHEN hs.gpt_action IN ('call', 'raise', 'bet') AND ha.put_money_in = 1 THEN 'followed'
            WHEN hs.gpt_action IN ('call', 'raise', 'bet') AND ha.folded = 1 THEN 'deviated'
            WHEN hs.gpt_action = 'check' AND ha.folded = 0 AND ha.put_money_in = 0 THEN 'followed'
            ELSE 'unknown'
        END AS gpt_compliance
    FROM hand_screenshots hs
    LEFT JOIN hero_actions_by_street ha ON hs.hand_id = ha.hand_id AND hs.street = ha.street
)

SELECT
    hl.hand_id,
    sm.hero_cards,
    sm.hero_position,
    sm.board,
    sm.hero_result,
    sm.amount_won,
    sm.total_pot,
    
    -- Preflop line
    MAX(CASE WHEN hl.street = 'preflop' THEN hl.gpt_action END) AS preflop_gpt,
    MAX(CASE WHEN hl.street = 'preflop' THEN hl.gpt_compliance END) AS preflop_compliance,
    
    -- Flop line
    MAX(CASE WHEN hl.street = 'flop' THEN hl.gpt_action END) AS flop_gpt,
    MAX(CASE WHEN hl.street = 'flop' THEN hl.gpt_compliance END) AS flop_compliance,
    
    -- Turn line
    MAX(CASE WHEN hl.street = 'turn' THEN hl.gpt_action END) AS turn_gpt,
    MAX(CASE WHEN hl.street = 'turn' THEN hl.gpt_compliance END) AS turn_compliance,
    
    -- River line  
    MAX(CASE WHEN hl.street = 'river' THEN hl.gpt_action END) AS river_gpt,
    MAX(CASE WHEN hl.street = 'river' THEN hl.gpt_compliance END) AS river_compliance,
    
    -- Count deviations
    SUM(CASE WHEN hl.gpt_compliance = 'deviated' THEN 1 ELSE 0 END) AS deviation_count,
    
    -- Full line as string (e.g. "fold->call->fold" for GPT, "call->call->fold" for hero)
    CONCAT_WS('->', 
        COLLECT_LIST(hl.gpt_action) 
    ) AS gpt_line,
    
    CONCAT_WS('->',
        COLLECT_LIST(hl.gpt_compliance)
    ) AS compliance_line,
    
    -- Did hero deviate at any point?
    MAX(CASE WHEN hl.gpt_compliance = 'deviated' THEN 1 ELSE 0 END) AS had_deviation,
    
    -- First street where hero deviated
    MIN(CASE WHEN hl.gpt_compliance = 'deviated' THEN hl.street END) AS first_deviation_street

FROM hand_line hl
JOIN hand_summary sm ON hl.hand_id = sm.hand_id
GROUP BY 
    hl.hand_id,
    sm.hero_cards,
    sm.hero_position,
    sm.board,
    sm.hero_result,
    sm.amount_won,
    sm.total_pot;
