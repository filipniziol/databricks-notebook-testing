-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: final_small_raise_analysis
-- MAGIC 
-- MAGIC When GPT says raise 2-3BB on Final Table, track what happens next

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

-- Get hero's actual actions in this hand
with_hero_actions AS (
    SELECT 
        sr.*,
        -- Did hero raise preflop?
        MAX(CASE WHEN ha.street = 'preflop' AND ha.action_type = 'raise' THEN 1 ELSE 0 END) AS hero_raised_preflop,
        -- Did hero fold later?
        MAX(CASE WHEN ha.action_type = 'fold' THEN 1 ELSE 0 END) AS hero_folded_later,
        -- Count hero actions
        COUNT(DISTINCT ha.action_order) AS hero_action_count
    FROM small_raise_situations sr
    LEFT JOIN poker.silver.hand_actions ha ON sr.hand_id = ha.hand_id AND ha.is_hero = true
        AND ha.action_type NOT IN ('post_ante', 'post_sb', 'post_bb', 'show')
    GROUP BY sr.file_name, sr.screenshot_at, sr.hero_cards, sr.hero_pos, sr.hero_stack,
             sr.pot, sr.to_call, sr.gpt_action, sr.gpt_recommendation, sr.hand_id,
             sr.hero_result, sr.big_blind, sr.total_pot, sr.net_profit, sr.went_to_showdown
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
        CASE 
            WHEN hero_cards RLIKE '^(AA|KK|QQ|JJ|TT|AK|AQ)' THEN 'premium'
            WHEN hero_cards RLIKE '^(99|88|77|66|AJ|AT|KQ|KJ)' THEN 'strong'
            WHEN hero_cards RLIKE '^(55|44|33|22|A[2-9]|K[T-9]|Q[J-9])' THEN 'medium'
            ELSE 'speculative'
        END AS hand_strength
    FROM with_hero_actions
)

SELECT
    outcome_type,
    
    COUNT(*) AS total,
    
    -- By hand strength
    SUM(CASE WHEN hand_strength = 'premium' THEN 1 ELSE 0 END) AS premium_hands,
    SUM(CASE WHEN hand_strength = 'strong' THEN 1 ELSE 0 END) AS strong_hands,
    SUM(CASE WHEN hand_strength = 'medium' THEN 1 ELSE 0 END) AS medium_hands,
    SUM(CASE WHEN hand_strength = 'speculative' THEN 1 ELSE 0 END) AS speculative_hands,
    
    -- Profit/Loss
    ROUND(SUM(net_profit / big_blind), 1) AS total_net_bb,
    ROUND(AVG(net_profit / big_blind), 1) AS avg_net_bb,
    
    -- Stack info
    ROUND(AVG(hero_stack), 1) AS avg_hero_stack

FROM categorized
GROUP BY outcome_type
ORDER BY total DESC;
