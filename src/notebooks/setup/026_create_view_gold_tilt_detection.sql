-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: tilt_detection
-- MAGIC 
-- MAGIC Analyzes if hero compliance drops during session:
-- MAGIC - After losing big pots
-- MAGIC - After winning big pots (overconfidence?)
-- MAGIC - Late in tournament vs early
-- MAGIC - Consecutive losses

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.tilt_detection AS

WITH session_hands AS (
    SELECT 
        h.tournament_id,
        h.hand_id,
        h.hand_timestamp,
        h.hero_result,
        h.big_blind,
        hp.amount_won,
        hp.chips_start,
        -- Order within tournament
        ROW_NUMBER() OVER (PARTITION BY h.tournament_id ORDER BY h.hand_timestamp) AS hand_num,
        -- Running profit
        SUM(CASE 
            WHEN h.hero_result = 'won' THEN hp.amount_won 
            WHEN h.hero_result IN ('lost', 'folded') THEN -hp.chips_start + COALESCE(hp.amount_won, 0)
            ELSE 0 
        END) OVER (PARTITION BY h.tournament_id ORDER BY h.hand_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_profit,
        -- Previous hand result
        LAG(h.hero_result) OVER (PARTITION BY h.tournament_id ORDER BY h.hand_timestamp) AS prev_result,
        LAG(hp.amount_won) OVER (PARTITION BY h.tournament_id ORDER BY h.hand_timestamp) AS prev_amount_won
    FROM poker.silver.hands h
    JOIN poker.silver.hand_players hp ON h.hand_id = hp.hand_id AND hp.is_hero = true
),

with_gpt_decisions AS (
    SELECT 
        sh.*,
        s.gpt_action,
        s.street,
        -- Compliance
        CASE
            WHEN s.gpt_action = 'fold' AND ha.action_type = 'fold' THEN true
            WHEN s.gpt_action = 'fold' AND ha.action_type IN ('call', 'raise', 'bet') THEN false
            WHEN s.gpt_action IN ('call', 'raise', 'bet') AND ha.action_type IN ('call', 'raise', 'bet') THEN true
            WHEN s.gpt_action IN ('call', 'raise', 'bet') AND ha.action_type = 'fold' THEN false
            ELSE NULL
        END AS followed_gpt,
        -- Was previous hand a big loss (>10bb)?
        CASE WHEN sh.prev_result IN ('lost', 'folded') AND sh.prev_amount_won < -sh.big_blind * 10 THEN true ELSE false END AS after_big_loss,
        -- Running status
        CASE 
            WHEN sh.running_profit > sh.big_blind * 20 THEN 'winning_big'
            WHEN sh.running_profit > 0 THEN 'winning'
            WHEN sh.running_profit > -sh.big_blind * 20 THEN 'losing'
            ELSE 'losing_big'
        END AS session_status
    FROM session_hands sh
    LEFT JOIN poker.silver.screenshot_hand_mapping m ON sh.hand_id = m.hand_id
    LEFT JOIN poker.silver.screenshots s ON m.file_name = s.file_name
    LEFT JOIN poker.silver.hand_actions ha ON sh.hand_id = ha.hand_id 
        AND ha.is_hero = true 
        AND ha.street = s.street
        AND ha.action_type NOT IN ('post_ante', 'post_sb', 'post_bb', 'show')
    WHERE s.gpt_action IS NOT NULL
)

-- Aggregate tilt indicators
SELECT
    session_status,
    after_big_loss,
    
    -- Counts
    COUNT(*) AS decisions,
    
    -- Compliance
    SUM(CASE WHEN followed_gpt = true THEN 1 ELSE 0 END) AS followed_count,
    ROUND(SUM(CASE WHEN followed_gpt = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS compliance_pct,
    
    -- Deviation types
    SUM(CASE WHEN followed_gpt = false AND gpt_action = 'fold' THEN 1 ELSE 0 END) AS ignored_fold_count,
    SUM(CASE WHEN followed_gpt = false AND gpt_action IN ('call', 'raise', 'bet') THEN 1 ELSE 0 END) AS ignored_play_count,
    
    -- Results when deviating
    SUM(CASE WHEN followed_gpt = false AND hero_result = 'won' THEN 1 ELSE 0 END) AS wins_when_deviated,
    SUM(CASE WHEN followed_gpt = false AND hero_result IN ('lost', 'folded') THEN 1 ELSE 0 END) AS losses_when_deviated

FROM with_gpt_decisions
WHERE followed_gpt IS NOT NULL
GROUP BY session_status, after_big_loss
ORDER BY session_status, after_big_loss;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consecutive Losses Analysis

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.tilt_consecutive_losses AS

WITH hands_with_streak AS (
    SELECT 
        h.tournament_id,
        h.hand_id,
        h.hand_timestamp,
        h.hero_result,
        h.big_blind,
        -- Count consecutive non-wins before this hand
        SUM(CASE WHEN h.hero_result != 'won' THEN 1 ELSE 0 END) OVER (
            PARTITION BY h.tournament_id 
            ORDER BY h.hand_timestamp 
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS recent_losses_5hands
    FROM poker.silver.hands h
),

with_decisions AS (
    SELECT 
        hws.*,
        s.gpt_action,
        CASE
            WHEN s.gpt_action = 'fold' AND ha.action_type = 'fold' THEN true
            WHEN s.gpt_action = 'fold' AND ha.action_type IN ('call', 'raise', 'bet') THEN false
            WHEN s.gpt_action IN ('call', 'raise', 'bet') AND ha.action_type IN ('call', 'raise', 'bet') THEN true
            WHEN s.gpt_action IN ('call', 'raise', 'bet') AND ha.action_type = 'fold' THEN false
            ELSE NULL
        END AS followed_gpt
    FROM hands_with_streak hws
    LEFT JOIN poker.silver.screenshot_hand_mapping m ON hws.hand_id = m.hand_id
    LEFT JOIN poker.silver.screenshots s ON m.file_name = s.file_name
    LEFT JOIN poker.silver.hand_actions ha ON hws.hand_id = ha.hand_id 
        AND ha.is_hero = true 
        AND ha.street = s.street
        AND ha.action_type NOT IN ('post_ante', 'post_sb', 'post_bb', 'show')
    WHERE s.gpt_action IS NOT NULL
)

SELECT
    CASE 
        WHEN recent_losses_5hands >= 4 THEN '4+ losses'
        WHEN recent_losses_5hands >= 2 THEN '2-3 losses'
        ELSE '0-1 losses'
    END AS recent_loss_streak,
    
    COUNT(*) AS decisions,
    ROUND(SUM(CASE WHEN followed_gpt = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS compliance_pct,
    SUM(CASE WHEN followed_gpt = false AND gpt_action = 'fold' THEN 1 ELSE 0 END) AS ignored_fold_count,
    SUM(CASE WHEN followed_gpt = false AND hero_result = 'won' THEN 1 ELSE 0 END) AS wins_when_deviated

FROM with_decisions
WHERE followed_gpt IS NOT NULL
GROUP BY 
    CASE 
        WHEN recent_losses_5hands >= 4 THEN '4+ losses'
        WHEN recent_losses_5hands >= 2 THEN '2-3 losses'
        ELSE '0-1 losses'
    END
ORDER BY 1;
