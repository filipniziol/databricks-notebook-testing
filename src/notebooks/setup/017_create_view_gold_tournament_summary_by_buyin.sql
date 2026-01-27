-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold View: tournament_summary_by_buyin
-- MAGIC Aggregated stats by buyin level ($10 vs $25)

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
