-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: final_small_raise_analysis
-- MAGIC 
-- MAGIC When GPT says raise 2-3BB on Final Table, track what happens next
-- MAGIC Focus: How often does hero open/fold when big stacks are BEHIND (yet to act)?

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.final_small_raise_analysis AS

WITH small_raise_situations AS (
    SELECT 
        s.file_name,
        s.screenshot_at,
        s.hero_cards,
        s.hero_pos,
        s.hero_stack,
        s.pot,
        s.to_call,
        s.gpt_action,
        s.gpt_recommendation,
        m.hand_id,
        h.hero_result,
        h.big_blind,
        h.total_pot,
        hp.net_profit,
        hp.went_to_showdown
    FROM poker.silver.screenshots s
    LEFT JOIN poker.silver.screenshot_hand_mapping m ON s.file_name = m.file_name
    LEFT JOIN poker.silver.hands h ON m.hand_id = h.hand_id
    LEFT JOIN poker.silver.hand_players hp ON h.hand_id = hp.hand_id AND hp.is_hero = true
    WHERE s.stage = 'final'
      AND s.street = 'preflop'
      AND s.gpt_action = 'raise'
      AND (s.gpt_recommendation LIKE '%2.%BB%' OR s.gpt_recommendation LIKE '%2 BB%' OR s.gpt_recommendation LIKE '%3%BB%')
),

-- Get max opponent stack BEHIND hero (based on position)
-- Position order: UTG=1, UTG+1=2, HJ=3, CO=4, BTN=5, SB=6, BB=7
-- Players "behind" = positions > hero_position (except BB which is always behind SB)
opponent_stacks_behind AS (
    SELECT 
        sr.file_name,
        sr.hero_pos,
        MAX(hp.chips_start / h.big_blind) AS max_stack_behind
    FROM small_raise_situations sr
    JOIN poker.silver.hand_players hp ON sr.hand_id = hp.hand_id AND hp.is_hero = false
    JOIN poker.silver.hands h ON sr.hand_id = h.hand_id
    WHERE 
        -- Determine if opponent is BEHIND hero based on positions
        CASE sr.hero_pos
            WHEN 'UTG' THEN hp.position IN ('UTG+1', 'HJ', 'MP', 'CO', 'BTN', 'SB', 'BB')
            WHEN 'UTG+1' THEN hp.position IN ('HJ', 'MP', 'CO', 'BTN', 'SB', 'BB')
            WHEN 'HJ' THEN hp.position IN ('CO', 'BTN', 'SB', 'BB')
            WHEN 'MP' THEN hp.position IN ('CO', 'BTN', 'SB', 'BB')
            WHEN 'CO' THEN hp.position IN ('BTN', 'SB', 'BB')
            WHEN 'BTN' THEN hp.position IN ('SB', 'BB')
            WHEN 'SB' THEN hp.position IN ('BB')
            ELSE false
        END
    GROUP BY sr.file_name, sr.hero_pos
),

-- Get hero's actual actions in this hand
with_hero_actions AS (
    SELECT 
        sr.*,
        osb.max_stack_behind,
        -- Did hero raise preflop?
        MAX(CASE WHEN ha.street = 'preflop' AND ha.action_type = 'raise' THEN 1 ELSE 0 END) AS hero_raised_preflop,
        -- Did hero fold later?
        MAX(CASE WHEN ha.action_type = 'fold' THEN 1 ELSE 0 END) AS hero_folded_later,
        -- Count hero actions
        COUNT(DISTINCT ha.action_order) AS hero_action_count
    FROM small_raise_situations sr
    LEFT JOIN opponent_stacks_behind osb ON sr.file_name = osb.file_name
    LEFT JOIN poker.silver.hand_actions ha ON sr.hand_id = ha.hand_id AND ha.is_hero = true
        AND ha.action_type NOT IN ('post_ante', 'post_sb', 'post_bb', 'show')
    GROUP BY sr.file_name, sr.screenshot_at, sr.hero_cards, sr.hero_pos, sr.hero_stack,
             sr.pot, sr.to_call, sr.gpt_action, sr.gpt_recommendation, sr.hand_id,
             sr.hero_result, sr.big_blind, sr.total_pot, sr.net_profit, sr.went_to_showdown,
             osb.max_stack_behind
),

-- Categorize outcome
categorized AS (
    SELECT 
        *,
        CASE 
            WHEN hero_raised_preflop = 1 AND hero_folded_later = 1 THEN 'raised_then_folded'
            WHEN hero_raised_preflop = 1 AND hero_result = 'won' THEN 'raised_and_won'
            WHEN hero_raised_preflop = 1 AND hero_result IN ('lost', 'folded') THEN 'raised_and_lost'
            WHEN hero_raised_preflop = 0 THEN 'did_not_raise'
            ELSE 'other'
        END AS outcome_type,
        -- Is there a big stack BEHIND that covers hero significantly?
        CASE 
            WHEN COALESCE(max_stack_behind, 0) > hero_stack * 1.5 THEN 'big_stack_behind'
            ELSE 'no_big_stack_behind'
        END AS stack_situation,
        -- Hero stack category
        CASE 
            WHEN hero_stack <= 12 THEN 'short_10-12bb'
            WHEN hero_stack <= 15 THEN 'short_13-15bb'
            WHEN hero_stack <= 20 THEN 'medium_16-20bb'
            ELSE 'deep_20+bb'
        END AS hero_stack_category
    FROM with_hero_actions
)

SELECT
    outcome_type,
    stack_situation,
    hero_stack_category,
    
    COUNT(*) AS total,
    
    -- Profit/Loss
    ROUND(SUM(net_profit / big_blind), 1) AS total_net_bb,
    ROUND(AVG(net_profit / big_blind), 1) AS avg_net_bb,
    
    -- Stack info
    ROUND(AVG(hero_stack), 1) AS avg_hero_stack,
    ROUND(AVG(max_stack_behind), 1) AS avg_max_stack_behind

FROM categorized
GROUP BY outcome_type, stack_situation, hero_stack_category
ORDER BY outcome_type, stack_situation, hero_stack_category;
