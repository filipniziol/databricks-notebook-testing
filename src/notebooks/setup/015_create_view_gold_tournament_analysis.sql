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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Summary view by stage

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.tournament_summary_by_stage AS
SELECT 
    stage,
    COUNT(*) AS tournaments,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_total,
    
    -- Financials
    SUM(buyin_total) AS total_buyin,
    SUM(hero_prize) AS total_prize,
    SUM(profit) AS total_profit,
    ROUND(AVG(profit), 2) AS avg_profit,
    
    -- Bounty vs Position breakdown
    SUM(estimated_position_prize) AS total_position_prize,
    SUM(estimated_bounty_prize) AS total_bounty_prize,
    
    -- ROI
    ROUND(SUM(profit) / SUM(buyin_total) * 100, 1) AS roi_pct
    
FROM poker.gold.tournament_analysis
GROUP BY stage
ORDER BY stage;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Summary by buyin level

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.tournament_summary_by_buyin AS
SELECT 
    buyin_total,
    COUNT(*) AS tournaments,
    
    -- Stage distribution
    SUM(CASE WHEN stage = '7_rush_bust' THEN 1 ELSE 0 END) AS rush_busts,
    SUM(CASE WHEN stage IN ('5_mid_final', '6_early_final') THEN 1 ELSE 0 END) AS final_no_cash,
    SUM(CASE WHEN stage IN ('1_champion', '2_runner_up', '3_third') THEN 1 ELSE 0 END) AS itm,
    
    -- ITM rate
    ROUND(SUM(CASE WHEN hero_prize > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS itm_rate,
    
    -- Financials
    SUM(buyin_total) AS total_buyin,
    SUM(hero_prize) AS total_prize,
    SUM(profit) AS total_profit,
    ROUND(SUM(profit) / SUM(buyin_total) * 100, 1) AS roi_pct,
    
    -- Bounty efficiency
    SUM(estimated_bounty_prize) AS total_bounty_earned,
    ROUND(AVG(CASE WHEN estimated_bounty_prize > 0 THEN estimated_bounty_prize END), 2) AS avg_bounty_when_won
    
FROM poker.gold.tournament_analysis
GROUP BY buyin_total
ORDER BY buyin_total;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Daily summary

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.tournament_summary_daily AS
SELECT 
    DATE(started_at) AS play_date,
    COUNT(*) AS tournaments,
    SUM(buyin_total) AS total_buyin,
    SUM(hero_prize) AS total_prize,
    SUM(profit) AS daily_profit,
    ROUND(SUM(profit) / SUM(buyin_total) * 100, 1) AS roi_pct,
    
    -- Best/worst result
    MAX(profit) AS best_result,
    MIN(profit) AS worst_result,
    
    -- Stage breakdown
    SUM(CASE WHEN stage = '7_rush_bust' THEN 1 ELSE 0 END) AS rush_busts,
    SUM(CASE WHEN hero_prize > 0 THEN 1 ELSE 0 END) AS itm_count
    
FROM poker.gold.tournament_analysis
GROUP BY DATE(started_at)
ORDER BY play_date DESC;
