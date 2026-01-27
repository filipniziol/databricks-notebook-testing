-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: rush_allin_call_analysis
-- MAGIC 
-- MAGIC When to_call > 50% of hero stack and GPT recommends call/raise in Rush stage

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.rush_allin_call_analysis AS

WITH allin_situations AS (
    SELECT 
        s.file_name,
        s.screenshot_at,
        s.hero_cards,
        s.hero_stack,
        s.to_call,
        ROUND(s.to_call * 100.0 / NULLIF(s.hero_stack, 0), 1) AS pct_of_stack,
        s.pot,
        s.gpt_action,
        s.gpt_recommendation,
        m.hand_id,
        h.hero_result,
        h.big_blind,
        hp.net_profit,
        ha.action_type AS hero_action
    FROM poker.silver.screenshots s
    LEFT JOIN poker.silver.screenshot_hand_mapping m ON s.file_name = m.file_name
    LEFT JOIN poker.silver.hands h ON m.hand_id = h.hand_id
    LEFT JOIN poker.silver.hand_players hp ON h.hand_id = hp.hand_id AND hp.is_hero = true
    LEFT JOIN poker.silver.hand_actions ha ON h.hand_id = ha.hand_id 
        AND ha.is_hero = true 
        AND ha.street = s.street
        AND ha.action_type NOT IN ('post_ante', 'post_sb', 'post_bb', 'show')
    WHERE s.stage = 'rush'
      AND s.to_call > s.hero_stack * 0.5  -- All-in or near all-in situation
      AND s.gpt_action IN ('call', 'raise')  -- GPT says to play
),

-- Categorize hands
categorized AS (
    SELECT 
        *,
        CASE 
            WHEN hero_cards RLIKE '^(A[A-Z]|K[K-Z]|Q[Q-Z]|J[J-Z]|T[T-Z]|[2-9][2-9])' 
                 AND SUBSTR(hero_cards, 1, 1) = SUBSTR(hero_cards, 3, 1) THEN 'pair'
            WHEN hero_cards LIKE 'A%' THEN 'ace_high'
            WHEN hero_cards LIKE 'K%' THEN 'king_high'
            WHEN SUBSTR(hero_cards, 2, 1) = SUBSTR(hero_cards, 4, 1) THEN 'suited'
            ELSE 'offsuit'
        END AS hand_category,
        CASE 
            WHEN hero_cards RLIKE '^(AA|KK|QQ|JJ|TT|AK|AQ)' THEN 'premium'
            WHEN hero_cards RLIKE '^(99|88|77|66|AJ|AT|KQ|KJ)' THEN 'strong'
            WHEN hero_cards RLIKE '^(55|44|33|22|A[2-9]|K[T-9]|Q[J-9])' THEN 'medium'
            ELSE 'speculative'
        END AS hand_strength
    FROM allin_situations
)

SELECT
    hand_strength,
    
    -- Sample size
    COUNT(*) AS total_situations,
    
    -- GPT recommendation breakdown
    SUM(CASE WHEN gpt_action = 'call' THEN 1 ELSE 0 END) AS gpt_said_call,
    SUM(CASE WHEN gpt_action = 'raise' THEN 1 ELSE 0 END) AS gpt_said_raise,
    
    -- Did hero follow GPT?
    SUM(CASE WHEN hero_action IN ('call', 'raise') THEN 1 ELSE 0 END) AS hero_played,
    SUM(CASE WHEN hero_action = 'fold' THEN 1 ELSE 0 END) AS hero_folded,
    
    -- Results when hero played
    SUM(CASE WHEN hero_action IN ('call', 'raise') AND hero_result = 'won' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN hero_action IN ('call', 'raise') AND hero_result IN ('lost', 'folded') THEN 1 ELSE 0 END) AS losses,
    
    -- Win rate
    ROUND(SUM(CASE WHEN hero_action IN ('call', 'raise') AND hero_result = 'won' THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN hero_action IN ('call', 'raise') THEN 1 ELSE 0 END), 0), 1) AS win_rate_pct,
    
    -- Profit
    ROUND(SUM(CASE WHEN hero_action IN ('call', 'raise') THEN net_profit / big_blind ELSE 0 END), 1) AS total_net_bb,
    ROUND(AVG(CASE WHEN hero_action IN ('call', 'raise') THEN net_profit / big_blind END), 1) AS avg_net_bb_per_hand,
    
    -- Average stack commitment
    ROUND(AVG(pct_of_stack), 1) AS avg_pct_of_stack_committed

FROM categorized
GROUP BY hand_strength
ORDER BY 
    CASE hand_strength 
        WHEN 'premium' THEN 1 
        WHEN 'strong' THEN 2 
        WHEN 'medium' THEN 3 
        ELSE 4 
    END;
