-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: fold_showdown_analysis
-- MAGIC Analyzes hands where hero folded but there was a showdown.
-- MAGIC 
-- MAGIC This lets us see "what would have happened" - was the fold correct?
-- MAGIC We can see winner's cards, pot size, AND simulate who would have won.
-- MAGIC 
-- MAGIC Requires: poker.utils.compare_hands function (023_create_poker_evaluation_functions.sql)

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.fold_showdown_analysis AS

WITH hero_folds AS (
    -- Hands where hero folded
    SELECT
        h.hand_id,
        h.hand_timestamp,
        h.tournament_id,
        h.hero_cards,
        h.hero_position,
        h.board,
        h.total_pot,
        h.big_blind,
        -- Find the street where hero folded
        MIN(ha.street) AS fold_street
    FROM poker.silver.hands h
    JOIN poker.silver.hand_actions ha ON h.hand_id = ha.hand_id 
        AND ha.is_hero = true 
        AND ha.action_type = 'fold'
    WHERE h.hero_result = 'folded'
    GROUP BY h.hand_id, h.hand_timestamp, h.tournament_id, h.hero_cards, 
             h.hero_position, h.board, h.total_pot, h.big_blind
),

showdown_winners AS (
    -- Players who won - may or may not have shown cards
    -- hole_cards are shown only at actual showdown
    SELECT
        hp.hand_id,
        hp.player_name AS winner_name,
        hp.hole_cards AS winner_cards,  -- NULL if no showdown
        hp.result_description AS winner_hand,
        hp.amount_won,
        hp.went_to_showdown AS winner_showed_cards
    FROM poker.silver.hand_players hp
    WHERE hp.won_hand = true
),

screenshot_advice AS (
    -- GPT advice for these hands
    SELECT
        m.hand_id,
        s.street,
        s.gpt_action,
        s.gpt_recommendation,
        s.pot AS pot_at_decision,
        s.to_call,
        s.hero_stack
    FROM poker.silver.screenshot_hand_mapping m
    JOIN poker.silver.screenshots s ON m.file_name = s.file_name
),

-- Join and compute who would have won
base_data AS (
    SELECT
        hf.hand_id,
        hf.hand_timestamp,
        hf.tournament_id,
        hf.hero_cards,
        hf.hero_position,
        hf.fold_street,
        hf.board,
        hf.total_pot,
        hf.big_blind,
        sw.winner_name,
        sw.winner_cards,
        sw.winner_hand,
        sw.amount_won AS pot_won,
        sw.winner_showed_cards,
        sa.gpt_action AS gpt_advised,
        sa.gpt_recommendation,
        sa.pot_at_decision,
        sa.to_call,
        sa.hero_stack
    FROM hero_folds hf
    JOIN showdown_winners sw ON hf.hand_id = sw.hand_id
    LEFT JOIN screenshot_advice sa ON hf.hand_id = sa.hand_id AND hf.fold_street = sa.street
)

SELECT
    bd.*,
    ROUND(bd.pot_won / bd.big_blind, 1) AS pot_won_bb,
    
    -- GPT analysis
    CASE 
        WHEN bd.gpt_advised = 'fold' THEN 'GPT_agreed_fold'
        WHEN bd.gpt_advised IN ('call', 'raise', 'bet') THEN 'GPT_said_play'
        ELSE 'no_gpt_advice'
    END AS gpt_vs_hero,
    
    -- Pot odds at decision
    CASE 
        WHEN bd.to_call > 0 AND bd.pot_at_decision > 0 
        THEN ROUND(bd.to_call / (bd.pot_at_decision + bd.to_call) * 100, 1)
        ELSE NULL
    END AS pot_odds_pct,
    
    -- Stack commitment if called
    CASE
        WHEN bd.hero_stack > 0 AND bd.to_call > 0
        THEN ROUND(bd.to_call / bd.hero_stack * 100, 1)
        ELSE NULL
    END AS stack_commit_pct,
    
    -- WHO WOULD HAVE WON? (only if we have both cards and full board)
    -- compare_hands returns: winner_index (0=hero, 1=opponent), hero_would_win, hero_hand, winning_hand
    CASE
        WHEN bd.hero_cards IS NOT NULL 
         AND bd.winner_cards IS NOT NULL 
         AND bd.board IS NOT NULL
         AND SIZE(bd.board) = 5
        THEN poker.utils.compare_hands(bd.hero_cards, ARRAY(bd.winner_cards), CONCAT_WS('', bd.board))
        ELSE NULL
    END AS showdown_simulation,
    
    -- Simplified: would hero have won?
    CASE
        WHEN bd.hero_cards IS NOT NULL 
         AND bd.winner_cards IS NOT NULL 
         AND bd.board IS NOT NULL
         AND SIZE(bd.board) = 5
        THEN poker.utils.compare_hands(bd.hero_cards, ARRAY(bd.winner_cards), CONCAT_WS('', bd.board)).hero_would_win
        ELSE NULL
    END AS hero_would_have_won,
    
    -- Hero's hand at showdown
    CASE
        WHEN bd.hero_cards IS NOT NULL 
         AND bd.board IS NOT NULL
         AND SIZE(bd.board) = 5
        THEN poker.utils.compare_hands(bd.hero_cards, ARRAY(bd.winner_cards), CONCAT_WS('', bd.board)).hero_hand
        ELSE NULL
    END AS hero_hand_at_showdown,
    
    -- Potential profit/loss if hero had called
    CASE
        WHEN bd.hero_cards IS NOT NULL 
         AND bd.winner_cards IS NOT NULL 
         AND bd.board IS NOT NULL
         AND SIZE(bd.board) = 5
         AND poker.utils.compare_hands(bd.hero_cards, ARRAY(bd.winner_cards), CONCAT_WS('', bd.board)).hero_would_win = true
        THEN ROUND(bd.pot_won / bd.big_blind, 1)  -- Would have won this pot
        WHEN bd.hero_cards IS NOT NULL 
         AND bd.winner_cards IS NOT NULL 
         AND bd.board IS NOT NULL
         AND SIZE(bd.board) = 5
         AND poker.utils.compare_hands(bd.hero_cards, ARRAY(bd.winner_cards), CONCAT_WS('', bd.board)).hero_would_win = false
        THEN ROUND(-1 * COALESCE(bd.to_call, 0) / bd.big_blind, 1)  -- Would have lost to_call
        ELSE NULL
    END AS potential_profit_bb

FROM base_data bd;
