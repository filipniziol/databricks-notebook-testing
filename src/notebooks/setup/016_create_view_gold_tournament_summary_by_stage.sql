-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: tournament_summary_by_stage
-- MAGIC Aggregated stats by tournament finish stage

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
