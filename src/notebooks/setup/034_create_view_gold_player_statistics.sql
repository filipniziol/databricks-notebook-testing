-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: player_statistics
-- MAGIC 
-- MAGIC Aggregated player statistics from player_hand_stats (033).
-- MAGIC 
-- MAGIC Depends on: poker.gold.player_hand_stats

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.player_statistics AS

WITH basic_stats AS (
    SELECT 
        player_name,
        COUNT(*) AS total_hands,
        
        -- VPIP / PFR / 3bet
        ROUND(100.0 * SUM(CASE WHEN vpip = true THEN 1 ELSE 0 END) / COUNT(*), 1) AS vpip_pct,
        ROUND(100.0 * SUM(CASE WHEN pfr = true THEN 1 ELSE 0 END) / COUNT(*), 1) AS pfr_pct,
        ROUND(100.0 * SUM(CASE WHEN three_bet = true THEN 1 ELSE 0 END) / COUNT(*), 1) AS three_bet_pct,
        
        -- Aggression Factor
        ROUND(
            SUM(total_bets + total_raises) * 1.0 / NULLIF(SUM(total_calls), 0), 
        2) AS aggression_factor,
        
        -- Showdown stats
        ROUND(100.0 * SUM(CASE WHEN went_to_showdown = true THEN 1 ELSE 0 END) / COUNT(*), 1) AS wtsd_pct,
        ROUND(100.0 * SUM(CASE WHEN won_hand = true AND went_to_showdown = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN went_to_showdown = true THEN 1 ELSE 0 END), 0), 1) AS wsd_pct,
        
        -- All-in frequency
        ROUND(100.0 * SUM(CASE WHEN went_allin = true THEN 1 ELSE 0 END) / COUNT(*), 1) AS allin_pct,
        
        -- Sizing stats (in BB)
        ROUND(AVG(preflop_raise_bb), 1) AS avg_preflop_raise_bb,
        ROUND(AVG(avg_action_size_bb), 1) AS avg_bet_size_bb,
        
        -- Profit
        SUM(net_profit) AS total_net_profit
        
    FROM poker.gold.player_hand_stats
    GROUP BY player_name
),

position_stats AS (
    SELECT 
        player_name,
        ROUND(100.0 * SUM(CASE WHEN position_group = 'early' AND pfr = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN position_group = 'early' THEN 1 ELSE 0 END), 0), 1) AS open_raise_early_pct,
        ROUND(100.0 * SUM(CASE WHEN position_group = 'middle' AND pfr = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN position_group = 'middle' THEN 1 ELSE 0 END), 0), 1) AS open_raise_middle_pct,
        ROUND(100.0 * SUM(CASE WHEN position_group = 'late' AND pfr = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN position_group = 'late' THEN 1 ELSE 0 END), 0), 1) AS open_raise_late_pct,
        ROUND(100.0 * SUM(CASE WHEN position = 'SB' AND pfr = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN position = 'SB' THEN 1 ELSE 0 END), 0), 1) AS sb_open_pct,
        ROUND(100.0 * SUM(CASE WHEN position = 'BB' AND vpip = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN position = 'BB' THEN 1 ELSE 0 END), 0), 1) AS bb_defend_pct
    FROM poker.gold.player_hand_stats
    GROUP BY player_name
),

final_table_stats AS (
    SELECT 
        player_name,
        SUM(CASE WHEN stage = 'final' THEN 1 ELSE 0 END) AS ft_hands,
        ROUND(100.0 * SUM(CASE WHEN stage = 'final' AND vpip = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN stage = 'final' THEN 1 ELSE 0 END), 0), 1) AS ft_vpip_pct,
        ROUND(100.0 * SUM(CASE WHEN stage = 'final' AND pfr = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN stage = 'final' THEN 1 ELSE 0 END), 0), 1) AS ft_pfr_pct,
        ROUND(100.0 * SUM(CASE WHEN stage = 'final' AND went_allin = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN stage = 'final' THEN 1 ELSE 0 END), 0), 1) AS ft_allin_pct,
        -- Shove by stack depth
        ROUND(100.0 * SUM(CASE WHEN stage = 'final' AND stack_bb <= 10 AND went_allin = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN stage = 'final' AND stack_bb <= 10 THEN 1 ELSE 0 END), 0), 1) AS ft_shove_short_pct,
        ROUND(100.0 * SUM(CASE WHEN stage = 'final' AND stack_bb > 10 AND stack_bb <= 20 AND went_allin = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN stage = 'final' AND stack_bb > 10 AND stack_bb <= 20 THEN 1 ELSE 0 END), 0), 1) AS ft_shove_medium_pct,
        ROUND(100.0 * SUM(CASE WHEN stage = 'final' AND stack_bb > 20 AND went_allin = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN stage = 'final' AND stack_bb > 20 THEN 1 ELSE 0 END), 0), 1) AS ft_shove_deep_pct,
        -- Shove by position
        ROUND(100.0 * SUM(CASE WHEN stage = 'final' AND position_group = 'late' AND went_allin = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN stage = 'final' AND position_group = 'late' THEN 1 ELSE 0 END), 0), 1) AS ft_shove_late_pct,
        ROUND(100.0 * SUM(CASE WHEN stage = 'final' AND position_group = 'blinds' AND went_allin = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN stage = 'final' AND position_group = 'blinds' THEN 1 ELSE 0 END), 0), 1) AS ft_shove_blinds_pct,
        -- Open shove %
        ROUND(100.0 * SUM(CASE WHEN stage = 'final' AND preflop_shove = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN stage = 'final' AND pfr = true THEN 1 ELSE 0 END), 0), 1) AS ft_open_shove_pct
    FROM poker.gold.player_hand_stats
    GROUP BY player_name
),

rush_stats AS (
    SELECT 
        player_name,
        SUM(CASE WHEN stage = 'rush' THEN 1 ELSE 0 END) AS rush_hands,
        ROUND(100.0 * SUM(CASE WHEN stage = 'rush' AND vpip = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN stage = 'rush' THEN 1 ELSE 0 END), 0), 1) AS rush_vpip_pct,
        ROUND(100.0 * SUM(CASE WHEN stage = 'rush' AND pfr = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN stage = 'rush' THEN 1 ELSE 0 END), 0), 1) AS rush_pfr_pct,
        ROUND(100.0 * SUM(CASE WHEN stage = 'rush' AND three_bet = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN stage = 'rush' THEN 1 ELSE 0 END), 0), 1) AS rush_3bet_pct,
        ROUND(100.0 * SUM(CASE WHEN stage = 'rush' AND went_allin = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN stage = 'rush' THEN 1 ELSE 0 END), 0), 1) AS rush_allin_pct,
        ROUND(100.0 * SUM(CASE WHEN stage = 'rush' AND went_to_showdown = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN stage = 'rush' THEN 1 ELSE 0 END), 0), 1) AS rush_wtsd_pct,
        ROUND(100.0 * SUM(CASE WHEN stage = 'rush' AND won_hand = true AND went_to_showdown = true THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN stage = 'rush' AND went_to_showdown = true THEN 1 ELSE 0 END), 0), 1) AS rush_wsd_pct,
        SUM(CASE WHEN stage = 'rush' THEN net_profit ELSE 0 END) AS rush_net_profit
    FROM poker.gold.player_hand_stats
    GROUP BY player_name
),

player_types AS (
    SELECT 
        player_name,
        CASE 
            WHEN vpip_pct >= 40 AND pfr_pct >= 25 THEN 'LAG'
            WHEN vpip_pct >= 40 AND pfr_pct < 25 THEN 'LP'
            WHEN vpip_pct < 25 AND pfr_pct >= 18 THEN 'TAG'
            WHEN vpip_pct < 25 AND pfr_pct < 18 THEN 'NIT'
            ELSE 'REG'
        END AS player_type,
        CASE 
            WHEN aggression_factor >= 3 THEN 'very_aggressive'
            WHEN aggression_factor >= 2 THEN 'aggressive'
            WHEN aggression_factor >= 1 THEN 'neutral'
            ELSE 'passive'
        END AS aggression_style
    FROM basic_stats
)

SELECT 
    b.player_name,
    b.total_hands,
    pt.player_type,
    pt.aggression_style,
    -- Basic
    b.vpip_pct,
    b.pfr_pct,
    b.three_bet_pct,
    b.aggression_factor,
    b.wtsd_pct,
    b.wsd_pct,
    b.allin_pct,
    -- Sizing
    b.avg_preflop_raise_bb,
    b.avg_bet_size_bb,
    b.total_net_profit,
    -- Position
    p.open_raise_early_pct,
    p.open_raise_middle_pct,
    p.open_raise_late_pct,
    p.sb_open_pct,
    p.bb_defend_pct,
    -- Rush stats
    r.rush_hands,
    r.rush_vpip_pct,
    r.rush_pfr_pct,
    r.rush_3bet_pct,
    r.rush_allin_pct,
    r.rush_wtsd_pct,
    r.rush_wsd_pct,
    r.rush_net_profit,
    -- Final Table stats
    f.ft_hands,
    f.ft_vpip_pct,
    f.ft_pfr_pct,
    f.ft_allin_pct,
    f.ft_shove_short_pct,
    f.ft_shove_medium_pct,
    f.ft_shove_deep_pct,
    f.ft_shove_late_pct,
    f.ft_shove_blinds_pct,
    f.ft_open_shove_pct
FROM basic_stats b
LEFT JOIN position_stats p ON b.player_name = p.player_name
LEFT JOIN rush_stats r ON b.player_name = r.player_name
LEFT JOIN final_table_stats f ON b.player_name = f.player_name
LEFT JOIN player_types pt ON b.player_name = pt.player_name
WHERE b.total_hands >= 10
ORDER BY b.total_hands DESC;
