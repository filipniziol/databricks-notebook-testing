-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: screenshot_hand_analysis
-- MAGIC Combines screenshot GPT advice with actual hand outcome.
-- MAGIC 
-- MAGIC Key questions:
-- MAGIC 1. Did hero follow GPT advice?
-- MAGIC 2. What was the outcome when hero ignored GPT?
-- MAGIC 3. Was GPT advice profitable?

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.screenshot_hand_analysis AS

WITH screenshot_data AS (
    SELECT
        s.file_name,
        s.screenshot_at,
        s.stage,
        s.street AS screenshot_street,
        s.hero_cards,
        s.hero_pos,
        s.hero_stack,
        s.pot,
        s.to_call,
        s.gpt_recommendation,
        s.gpt_action,
        s.gpt_amount,
        m.hand_id,
        m.match_confidence,
        m.time_diff_seconds
    FROM poker.silver.screenshots s
    JOIN poker.silver.screenshot_hand_mapping m ON s.file_name = m.file_name
    WHERE m.hand_id IS NOT NULL  -- Only matched screenshots
),

hand_data AS (
    SELECT
        h.hand_id,
        h.hand_timestamp,
        h.tournament_id,
        h.level,
        h.big_blind,
        h.board,
        h.total_pot,
        h.hero_result,
        -- Calculate hero profit from hand_players
        hp.amount_won,
        hp.chips_start AS hero_chips_start,
        hp.vpip AS hero_vpip,
        hp.pfr AS hero_pfr,
        hp.went_to_showdown AS hero_went_to_showdown
    FROM poker.silver.hands h
    JOIN poker.silver.hand_players hp ON h.hand_id = hp.hand_id AND hp.is_hero = true
),

-- Check if hand had showdown and who won (for fold analysis)
showdown_info AS (
    SELECT
        hand_id,
        -- Was there a showdown?
        MAX(CASE WHEN went_to_showdown = true THEN 1 ELSE 0 END) AS had_showdown,
        -- Winner's hand description
        MAX(CASE WHEN won_hand = true THEN result_description END) AS winner_hand_description,
        -- Winner's hole cards (if shown)
        MAX(CASE WHEN won_hand = true THEN hole_cards END) AS winner_hole_cards,
        -- How much winner won
        MAX(CASE WHEN won_hand = true THEN amount_won END) AS showdown_pot_won
    FROM poker.silver.hand_players
    GROUP BY hand_id
),

-- Get hero's actual action on the screenshot street
hero_actions AS (
    SELECT 
        ha.hand_id,
        ha.street,
        -- First meaningful action (not post blinds)
        FIRST(ha.action_type) AS hero_first_action,
        -- Did hero put money in voluntarily on this street?
        MAX(CASE WHEN ha.action_type IN ('call', 'bet', 'raise') THEN 1 ELSE 0 END) AS hero_put_money_in,
        -- Did hero fold on this street?
        MAX(CASE WHEN ha.action_type = 'fold' THEN 1 ELSE 0 END) AS hero_folded_on_street
    FROM poker.silver.hand_actions ha
    WHERE ha.is_hero = true
    AND ha.action_type NOT IN ('post_ante', 'post_sb', 'post_bb', 'show')
    GROUP BY ha.hand_id, ha.street
)

SELECT
    -- Screenshot context
    sd.file_name,
    sd.screenshot_at,
    sd.stage,
    sd.screenshot_street,
    sd.hero_cards,
    sd.hero_pos,
    sd.hero_stack,
    sd.pot,
    sd.to_call,
    sd.match_confidence,
    
    -- GPT advice
    sd.gpt_recommendation,
    sd.gpt_action,
    sd.gpt_amount,
    
    -- Hand outcome
    sd.hand_id,
    hd.hand_timestamp,
    hd.tournament_id,
    hd.board,
    hd.total_pot AS final_pot,
    hd.hero_result,
    hd.amount_won AS hero_amount_won,
    hd.hero_vpip,
    hd.hero_pfr,
    
    -- Hero actual action on screenshot street
    ha.hero_first_action,
    ha.hero_put_money_in,
    ha.hero_folded_on_street,
    
    -- Did hero follow GPT?
    CASE
        WHEN sd.gpt_action = 'fold' AND ha.hero_folded_on_street = 1 THEN true
        WHEN sd.gpt_action = 'fold' AND ha.hero_put_money_in = 1 THEN false
        WHEN sd.gpt_action = 'check' AND ha.hero_first_action = 'check' THEN true
        WHEN sd.gpt_action IN ('call', 'raise', 'bet') AND ha.hero_put_money_in = 1 THEN true
        WHEN sd.gpt_action IN ('call', 'raise', 'bet') AND ha.hero_folded_on_street = 1 THEN false
        ELSE NULL  -- Can't determine
    END AS followed_gpt_advice,
    
    -- Outcome analysis (only meaningful when we have clear action)
    CASE
        -- GPT said fold, hero played anyway
        WHEN sd.gpt_action = 'fold' AND ha.hero_put_money_in = 1 AND hd.hero_result = 'won' 
            THEN 'ignored_fold_won'
        WHEN sd.gpt_action = 'fold' AND ha.hero_put_money_in = 1 AND hd.hero_result IN ('lost', 'folded')
            THEN 'ignored_fold_lost'
        
        -- GPT said play, hero played
        WHEN sd.gpt_action IN ('call', 'raise', 'bet') AND ha.hero_put_money_in = 1 AND hd.hero_result = 'won'
            THEN 'followed_play_won'
        WHEN sd.gpt_action IN ('call', 'raise', 'bet') AND ha.hero_put_money_in = 1 AND hd.hero_result IN ('lost', 'folded')
            THEN 'followed_play_lost'
        
        -- GPT said play, hero folded (missed value?)
        WHEN sd.gpt_action IN ('call', 'raise', 'bet') AND ha.hero_folded_on_street = 1
            THEN 'ignored_play_folded'
        
        -- GPT said fold, hero folded (good discipline)
        WHEN sd.gpt_action = 'fold' AND ha.hero_folded_on_street = 1
            THEN 'followed_fold'
        
        ELSE 'unknown'
    END AS outcome_category,
    
    -- Profit/Loss in BB (for hands where hero didn't fold early)
    CASE 
        WHEN hd.hero_result = 'won' THEN hd.amount_won / hd.big_blind
        WHEN hd.hero_result = 'lost' THEN -1 * (hd.hero_chips_start - COALESCE(hd.amount_won, 0)) / hd.big_blind
        ELSE 0
    END AS profit_bb,
    
    -- Showdown info (for fold analysis - "what would have happened")
    si.had_showdown,
    si.winner_hole_cards,
    si.winner_hand_description,
    si.showdown_pot_won,
    
    -- Could hero have won? (when hero folded but there was showdown)
    CASE
        WHEN hd.hero_result = 'folded' AND si.had_showdown = 1 
        THEN si.showdown_pot_won / hd.big_blind
        ELSE NULL
    END AS missed_pot_bb,
    
    -- Flag: Hero folded but GPT said play AND there was showdown
    CASE
        WHEN sd.gpt_action IN ('call', 'raise', 'bet') 
         AND ha.hero_folded_on_street = 1 
         AND si.had_showdown = 1
        THEN true
        ELSE false
    END AS folded_with_showdown_data

FROM screenshot_data sd
JOIN hand_data hd ON sd.hand_id = hd.hand_id
LEFT JOIN hero_actions ha ON sd.hand_id = ha.hand_id AND sd.screenshot_street = ha.street
LEFT JOIN showdown_info si ON sd.hand_id = si.hand_id;
