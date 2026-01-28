-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Silver Table: player_identity
-- MAGIC Maps anonymized hand history players to real screenshot player names.
-- MAGIC 
-- MAGIC **Join logic:**
-- MAGIC - screenshot_hand_mapping gives us: file_name ↔ hand_id
-- MAGIC - hand_players has: hand_id + seat + anonymous_name
-- MAGIC - screenshot_players has: file_name + seat + real_name
-- MAGIC - Match by: hand_id → file_name → same seat = same player!
-- MAGIC 
-- MAGIC This allows us to aggregate stats across all hands for a real player.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS poker.silver.player_identity (
    -- Player identification
    player_name             STRING          NOT NULL    COMMENT 'Real player name from screenshots',
    anonymous_id            STRING                      COMMENT 'Anonymized hash from hand history (one of many)',
    
    -- Evidence tracking
    hands_matched           INT                         COMMENT 'Number of hands where this mapping was confirmed',
    first_seen              TIMESTAMP                   COMMENT 'First time this player was seen',
    last_seen               TIMESTAMP                   COMMENT 'Last time this player was seen',
    
    -- Confidence
    match_confidence        STRING                      COMMENT 'HIGH if multiple hands confirm, LOW if single hand',
    
    -- Metadata
    created_at              TIMESTAMP                   COMMENT 'When mapping was created',
    updated_at              TIMESTAMP                   COMMENT 'When mapping was last updated'
)
USING DELTA
COMMENT 'Silver layer: Maps real player names to anonymized IDs. One player can have multiple anonymous IDs across sessions.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Populate Player Identity
-- MAGIC 
-- MAGIC Join screenshot_players → screenshot_hand_mapping → hand_players by seat

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
