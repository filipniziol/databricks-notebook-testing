-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: tournament_summary_daily
-- MAGIC Daily profit/loss tracking

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
