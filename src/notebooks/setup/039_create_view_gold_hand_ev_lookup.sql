-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create View: Hand EV Lookup
-- MAGIC 
-- MAGIC Simple lookup table for expected value by hand, position, and stage.
-- MAGIC More reliable than ML model with limited data.

-- COMMAND ----------

CREATE OR REPLACE VIEW poker.gold.hand_ev_lookup AS
WITH hand_categories AS (
    SELECT 
        hand_id,
        -- Hand representation (e.g., "AA", "AKs", "72o")
        CASE 
            WHEN is_pocket_pair = 1 THEN 
                CONCAT(
                    CASE CAST(high_card_rank AS INT) 
                        WHEN 14 THEN 'A' WHEN 13 THEN 'K' WHEN 12 THEN 'Q' 
                        WHEN 11 THEN 'J' WHEN 10 THEN 'T' ELSE CAST(CAST(high_card_rank AS INT) AS STRING) 
                    END,
                    CASE CAST(high_card_rank AS INT) 
                        WHEN 14 THEN 'A' WHEN 13 THEN 'K' WHEN 12 THEN 'Q' 
                        WHEN 11 THEN 'J' WHEN 10 THEN 'T' ELSE CAST(CAST(high_card_rank AS INT) AS STRING) 
                    END
                )
            ELSE
                CONCAT(
                    CASE CAST(high_card_rank AS INT) 
                        WHEN 14 THEN 'A' WHEN 13 THEN 'K' WHEN 12 THEN 'Q' 
                        WHEN 11 THEN 'J' WHEN 10 THEN 'T' ELSE CAST(CAST(high_card_rank AS INT) AS STRING) 
                    END,
                    CASE CAST(low_card_rank AS INT) 
                        WHEN 14 THEN 'A' WHEN 13 THEN 'K' WHEN 12 THEN 'Q' 
                        WHEN 11 THEN 'J' WHEN 10 THEN 'T' ELSE CAST(CAST(low_card_rank AS INT) AS STRING) 
                    END,
                    CASE WHEN is_suited = 1 THEN 's' ELSE 'o' END
                )
        END AS hand_type,
        -- Position name
        CASE position_encoded
            WHEN 0 THEN 'Early'
            WHEN 1 THEN 'Middle'
            WHEN 2 THEN 'Late'
            WHEN 3 THEN 'Blinds'
        END AS position,
        -- Stage name
        CASE WHEN is_final_stage = 1 THEN 'Final' ELSE 'Rush' END AS stage,
        -- Opponent count bucket
        CASE 
            WHEN opponent_count <= 2 THEN '1-2'
            WHEN opponent_count <= 4 THEN '3-4'
            ELSE '5+'
        END AS opponent_bucket,
        profit_bb,
        won_hand
    FROM poker.ml.hand_features
)
SELECT 
    hand_type,
    position,
    stage,
    opponent_bucket,
    ROUND(AVG(profit_bb), 2) AS avg_profit_bb,
    ROUND(AVG(won_hand) * 100, 1) AS win_rate_pct,
    COUNT(*) AS sample_size,
    ROUND(MIN(profit_bb), 2) AS min_profit_bb,
    ROUND(MAX(profit_bb), 2) AS max_profit_bb,
    ROUND(STDDEV(profit_bb), 2) AS stddev_profit_bb
FROM hand_categories
GROUP BY hand_type, position, stage, opponent_bucket
-- No minimum sample - show all data, filter in queries if needed
ORDER BY hand_type, position, stage, opponent_bucket;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Example Queries
-- MAGIC 
-- MAGIC ```sql
-- MAGIC -- Best hands overall
-- MAGIC SELECT * FROM poker.gold.hand_ev_lookup 
-- MAGIC WHERE sample_size >= 5
-- MAGIC ORDER BY avg_profit_bb DESC LIMIT 20;
-- MAGIC 
-- MAGIC -- Specific hand lookup
-- MAGIC SELECT * FROM poker.gold.hand_ev_lookup 
-- MAGIC WHERE hand_type = 'AKo';
-- MAGIC 
-- MAGIC -- Best spots in Final stage
-- MAGIC SELECT * FROM poker.gold.hand_ev_lookup 
-- MAGIC WHERE stage = 'Final' AND sample_size >= 5
-- MAGIC ORDER BY avg_profit_bb DESC;
-- MAGIC ```
