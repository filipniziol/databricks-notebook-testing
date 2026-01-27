-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: gpt_compliance_stats
-- MAGIC 
-- MAGIC Aggregated stats on GPT compliance by different dimensions:
-- MAGIC - Stage (rush vs final)
-- MAGIC - Players remaining (heads-up vs multi-way)
-- MAGIC - Street (preflop vs postflop)
-- MAGIC - Stack depth (short vs deep)
-- MAGIC - Session progress (early vs late in tournament)

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.gpt_compliance_stats AS

WITH player_counts AS (
    -- Pre-calculate player counts per screenshot
    SELECT file_name, COUNT(*) + 1 AS players_in_hand
    FROM poker.silver.screenshot_players
    GROUP BY file_name
),

base_decisions AS (
    SELECT 
        s.file_name,
        s.screenshot_at,
        s.stage,
        s.street,
        s.hero_pos,
        s.hero_stack,
        s.pot,
        s.to_call,
        s.gpt_action,
        m.hand_id,
        m.match_confidence,
        h.tournament_id,
        h.hero_result,
        h.big_blind,
        h.total_pot,
        hp.amount_won,
        COALESCE(pc.players_in_hand, 1) AS players_in_hand,
        -- Hero stack in BB
        ROUND(s.hero_stack / (h.big_blind / 100), 1) AS hero_stack_bb,
        -- Sequence in tournament
        ROW_NUMBER() OVER (PARTITION BY h.tournament_id ORDER BY s.screenshot_at) AS hand_seq_in_tournament,
        COUNT(*) OVER (PARTITION BY h.tournament_id) AS total_hands_in_tournament
    FROM poker.silver.screenshots s
    JOIN poker.silver.screenshot_hand_mapping m ON s.file_name = m.file_name
    JOIN poker.silver.hands h ON m.hand_id = h.hand_id
    JOIN poker.silver.hand_players hp ON h.hand_id = hp.hand_id AND hp.is_hero = true
    LEFT JOIN player_counts pc ON s.file_name = pc.file_name
    WHERE s.gpt_action IS NOT NULL
    AND m.hand_id IS NOT NULL
),

with_compliance AS (
    SELECT 
        bd.*,
        -- Get hero action from hand_actions
        ha.action_type AS hero_action,
        -- Compliance
        CASE
            WHEN bd.gpt_action = 'fold' AND ha.action_type = 'fold' THEN true
            WHEN bd.gpt_action = 'fold' AND ha.action_type IN ('call', 'raise', 'bet') THEN false
            WHEN bd.gpt_action IN ('call', 'raise', 'bet') AND ha.action_type IN ('call', 'raise', 'bet') THEN true
            WHEN bd.gpt_action IN ('call', 'raise', 'bet') AND ha.action_type = 'fold' THEN false
            WHEN bd.gpt_action = 'check' AND ha.action_type = 'check' THEN true
            ELSE NULL
        END AS followed_gpt,
        -- Profit in BB
        CASE 
            WHEN bd.hero_result = 'won' THEN bd.amount_won / bd.big_blind
            WHEN bd.hero_result IN ('lost', 'folded') THEN -1 * (bd.total_pot - bd.amount_won) / bd.big_blind * 0.1  -- Rough estimate
            ELSE 0
        END AS profit_bb,
        -- Categorize
        CASE WHEN bd.players_in_hand = 2 THEN 'heads_up' ELSE 'multiway' END AS table_type,
        CASE 
            WHEN bd.hero_stack_bb < 10 THEN 'short_<10bb'
            WHEN bd.hero_stack_bb < 20 THEN 'medium_10-20bb'
            ELSE 'deep_>20bb'
        END AS stack_category,
        CASE 
            WHEN bd.hand_seq_in_tournament <= bd.total_hands_in_tournament * 0.33 THEN 'early'
            WHEN bd.hand_seq_in_tournament <= bd.total_hands_in_tournament * 0.66 THEN 'middle'
            ELSE 'late'
        END AS tournament_phase
    FROM base_decisions bd
    LEFT JOIN poker.silver.hand_actions ha ON bd.hand_id = ha.hand_id 
        AND ha.is_hero = true 
        AND ha.street = bd.street
        AND ha.action_type NOT IN ('post_ante', 'post_sb', 'post_bb', 'show')
)

-- Final aggregation by multiple dimensions
SELECT
    -- Dimension
    stage,
    table_type,
    street,
    stack_category,
    tournament_phase,
    
    -- Counts
    COUNT(*) AS total_decisions,
    SUM(CASE WHEN followed_gpt = true THEN 1 ELSE 0 END) AS followed_count,
    SUM(CASE WHEN followed_gpt = false THEN 1 ELSE 0 END) AS deviated_count,
    
    -- Compliance rate
    ROUND(SUM(CASE WHEN followed_gpt = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS compliance_pct,
    
    -- When followed GPT
    ROUND(AVG(CASE WHEN followed_gpt = true THEN profit_bb END), 2) AS avg_profit_bb_followed,
    SUM(CASE WHEN followed_gpt = true AND hero_result = 'won' THEN 1 ELSE 0 END) AS wins_when_followed,
    
    -- When deviated from GPT
    ROUND(AVG(CASE WHEN followed_gpt = false THEN profit_bb END), 2) AS avg_profit_bb_deviated,
    SUM(CASE WHEN followed_gpt = false AND hero_result = 'won' THEN 1 ELSE 0 END) AS wins_when_deviated,
    
    -- Deviation breakdown by GPT recommendation
    SUM(CASE WHEN followed_gpt = false AND gpt_action = 'fold' THEN 1 ELSE 0 END) AS ignored_fold_count,
    SUM(CASE WHEN followed_gpt = false AND gpt_action IN ('call', 'raise', 'bet') THEN 1 ELSE 0 END) AS ignored_play_count

FROM with_compliance
WHERE followed_gpt IS NOT NULL
GROUP BY stage, table_type, street, stack_category, tournament_phase
ORDER BY stage, table_type, street;
