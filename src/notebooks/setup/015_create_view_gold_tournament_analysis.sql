-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: tournament_analysis
-- MAGIC Aggregated tournament results with stage breakdown and bounty vs position prize analysis

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.tournament_analysis AS
WITH tournament_stages AS (
    SELECT 
        *,
        -- Tournament stage based on finish position
        CASE 
            WHEN hero_position = 1 THEN '1_champion'
            WHEN hero_position = 2 THEN '2_runner_up'
            WHEN hero_position = 3 THEN '3_third'
            WHEN hero_position = 4 THEN '4_fourth'
            WHEN hero_position BETWEEN 5 AND 6 THEN '5_mid_final'
            WHEN hero_position BETWEEN 7 AND 9 THEN '6_early_final'
            WHEN hero_position >= 10 THEN '7_rush_bust'
        END AS stage,
        
        -- Position prize estimate (only top 3 paid from prize pool)
        -- Multipliers: 1st=8x, 2nd=6x, 3rd=4x of buyin_prize (total 18x = prize pool)
        CASE 
            WHEN hero_position = 1 THEN buyin_prize * 8  -- ~44% of prize pool
            WHEN hero_position = 2 THEN buyin_prize * 6  -- ~33% of prize pool  
            WHEN hero_position = 3 THEN buyin_prize * 4  -- ~22% of prize pool
            ELSE 0
        END AS estimated_position_prize,
        
        -- Bounty earnings = total prize - position prize
        hero_prize - CASE 
            WHEN hero_position = 1 THEN buyin_prize * 8
            WHEN hero_position = 2 THEN buyin_prize * 6
            WHEN hero_position = 3 THEN buyin_prize * 4
            ELSE 0
        END AS estimated_bounty_prize,
        
        -- Profit/Loss
        hero_prize - buyin_total AS profit
        
    FROM poker.silver.tournaments
)
SELECT 
    tournament_id,
    tournament_name,
    buyin_total,
    hero_position,
    stage,
    hero_prize,
    estimated_position_prize,
    estimated_bounty_prize,
    profit,
    started_at,
    
    -- Running totals (for trend analysis)
    SUM(profit) OVER (ORDER BY started_at ROWS UNBOUNDED PRECEDING) AS cumulative_profit,
    SUM(buyin_total) OVER (ORDER BY started_at ROWS UNBOUNDED PRECEDING) AS cumulative_buyin,
    
    -- ROI
    ROUND(profit / buyin_total * 100, 1) AS roi_pct
    
FROM tournament_stages
ORDER BY started_at DESC;
