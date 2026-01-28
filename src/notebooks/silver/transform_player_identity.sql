-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Transform: Player Identity
-- MAGIC Maps anonymized hand history players to real screenshot player names - per hand.
-- MAGIC 
-- MAGIC One row per (player_name, hand_id) - fact table, not aggregated lookup.

-- COMMAND ----------

MERGE INTO poker.silver.player_identity AS target
USING (
    -- Pick one screenshot per (player_name, hand_id) using ROW_NUMBER
    WITH ranked AS (
        SELECT 
            sp.name AS player_name,
            m.hand_id,
            hp.player_name AS anonymous_id,
            sp.seat,
            sp.file_name,
            ROW_NUMBER() OVER (PARTITION BY sp.name, m.hand_id ORDER BY sp.file_name) AS rn
        FROM poker.silver.screenshot_players sp
        JOIN poker.silver.screenshot_hand_mapping m ON sp.file_name = m.file_name
        JOIN poker.silver.hand_players hp ON m.hand_id = hp.hand_id AND sp.seat = hp.seat
        WHERE sp.name IS NOT NULL 
          AND sp.name != ''
          AND hp.player_name IS NOT NULL
          AND sp.name != 'Hero'
          AND hp.is_hero = false
    )
    SELECT player_name, hand_id, anonymous_id, seat, file_name
    FROM ranked
    WHERE rn = 1
) AS source
ON target.player_name = source.player_name AND target.hand_id = source.hand_id
WHEN NOT MATCHED THEN INSERT (
    player_name, hand_id, anonymous_id, seat, file_name, created_at
) VALUES (
    source.player_name, source.hand_id, source.anonymous_id, source.seat, source.file_name, current_timestamp()
);
