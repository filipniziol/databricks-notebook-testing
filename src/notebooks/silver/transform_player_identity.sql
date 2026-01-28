-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Transform: Player Identity
-- MAGIC Maps anonymized hand history players to real screenshot player names.

-- COMMAND ----------

MERGE INTO poker.silver.player_identity AS target
USING (
    WITH player_mapping AS (
        SELECT DISTINCT
            sp.name AS player_name,
            hp.player_name AS anonymous_id,
            COUNT(*) OVER (PARTITION BY sp.name, hp.player_name) AS match_count,
            MIN(h.hand_timestamp) OVER (PARTITION BY sp.name) AS first_seen,
            MAX(h.hand_timestamp) OVER (PARTITION BY sp.name) AS last_seen
        FROM poker.silver.screenshot_players sp
        JOIN poker.silver.screenshot_hand_mapping m ON sp.file_name = m.file_name
        JOIN poker.silver.hand_players hp ON m.hand_id = hp.hand_id AND sp.seat = hp.seat
        JOIN poker.silver.hands h ON hp.hand_id = h.hand_id
        WHERE sp.name IS NOT NULL 
          AND hp.player_name IS NOT NULL
          AND sp.name != 'Hero'
          AND hp.is_hero = false
    )
    SELECT 
        player_name,
        anonymous_id,
        SUM(match_count) AS hands_matched,
        MIN(first_seen) AS first_seen,
        MAX(last_seen) AS last_seen,
        CASE WHEN SUM(match_count) >= 3 THEN 'HIGH' ELSE 'LOW' END AS match_confidence
    FROM player_mapping
    GROUP BY player_name, anonymous_id
) AS source
ON target.player_name = source.player_name AND target.anonymous_id = source.anonymous_id
WHEN MATCHED THEN UPDATE SET
    hands_matched = source.hands_matched,
    last_seen = source.last_seen,
    match_confidence = source.match_confidence,
    updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    player_name, anonymous_id, hands_matched, first_seen, last_seen, match_confidence, created_at, updated_at
) VALUES (
    source.player_name, source.anonymous_id, source.hands_matched, source.first_seen, source.last_seen, 
    source.match_confidence, current_timestamp(), current_timestamp()
);
