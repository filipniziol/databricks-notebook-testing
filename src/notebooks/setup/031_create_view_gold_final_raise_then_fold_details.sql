-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: final_raise_then_fold_details
-- MAGIC 
-- MAGIC Detailed list of cases where hero raised small then folded to re-raise
-- MAGIC Shows who shoved and their stack size

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

-- Get max opponent stack BEHIND hero (based on position)
opponent_stacks_behind AS (
    SELECT 
        sr.file_name,
        sr.hero_pos,
        MAX(hp.chips_start / h.big_blind) AS max_stack_behind
    FROM small_raise_situations sr
    JOIN poker.silver.hand_players hp ON sr.hand_id = hp.hand_id AND hp.is_hero = false
    JOIN poker.silver.hands h ON sr.hand_id = h.hand_id
    WHERE 
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

-- Get who shoved after hero's raise (all-in preflop)
shover_info AS (
    SELECT 
        sr.file_name,
        FIRST(hp.position) AS shover_pos,
        FIRST(ROUND(hp.chips_start / h.big_blind, 1)) AS shover_stack,
        FIRST(hp.vpip) AS shover_vpip
    FROM small_raise_situations sr
    JOIN poker.silver.hand_actions ha ON sr.hand_id = ha.hand_id 
        AND ha.is_hero = false 
        AND ha.street = 'preflop'
        AND ha.action_type = 'raise'
        AND ha.is_allin = true
    JOIN poker.silver.hand_players hp ON sr.hand_id = hp.hand_id AND ha.player_name = hp.player_name
    JOIN poker.silver.hands h ON sr.hand_id = h.hand_id
    GROUP BY sr.file_name
),

with_actions AS (
    SELECT 
        sr.*,
        osb.max_stack_behind,
        si.shover_pos,
        si.shover_stack,
        si.shover_vpip,
        MAX(CASE WHEN ha.street = 'preflop' AND ha.action_type = 'raise' THEN 1 ELSE 0 END) AS hero_raised,
        MAX(CASE WHEN ha.action_type = 'fold' THEN 1 ELSE 0 END) AS hero_folded
    FROM small_raise_situations sr
    LEFT JOIN opponent_stacks_behind osb ON sr.file_name = osb.file_name
    LEFT JOIN shover_info si ON sr.file_name = si.file_name
    LEFT JOIN poker.silver.hand_actions ha ON sr.hand_id = ha.hand_id AND ha.is_hero = true
        AND ha.action_type NOT IN ('post_ante', 'post_sb', 'post_bb', 'show')
    GROUP BY sr.file_name, sr.hero_cards, sr.hero_pos, sr.hero_stack, sr.gpt_recommendation,
             sr.hand_id, sr.hero_result, sr.big_blind, sr.net_profit,
             osb.max_stack_behind, si.shover_pos, si.shover_stack, si.shover_vpip
)

SELECT 
    file_name,
    hero_cards,
    hero_pos,
    ROUND(hero_stack, 1) AS hero_stack,
    ROUND(max_stack_behind, 1) AS max_stack_behind,
    shover_pos,
    shover_stack,
    shover_vpip,
    ROUND(net_profit / big_blind, 1) AS net_bb,
    -- Hero stack category
    CASE 
        WHEN hero_stack <= 12 THEN 'short_10-12bb'
        WHEN hero_stack <= 15 THEN 'short_13-15bb'
        WHEN hero_stack <= 20 THEN 'medium_16-20bb'
        ELSE 'deep_20+bb'
    END AS hero_stack_cat,
    -- Big stack behind?
    CASE 
        WHEN COALESCE(max_stack_behind, 0) > hero_stack * 1.5 THEN 'big_behind'
        ELSE 'no_big_behind'
    END AS stack_sit
FROM with_actions
WHERE hero_raised = 1 AND hero_folded = 1
ORDER BY hero_stack DESC, net_bb;
