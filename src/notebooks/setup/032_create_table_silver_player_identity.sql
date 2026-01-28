-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Silver Table: player_identity
-- MAGIC Maps anonymized hand history players to real screenshot player names - **per hand**.
-- MAGIC 
-- MAGIC **Join logic:**
-- MAGIC - screenshot_hand_mapping gives us: file_name ↔ hand_id
-- MAGIC - hand_players has: hand_id + seat + anonymous_name
-- MAGIC - screenshot_players has: file_name + seat + real_name
-- MAGIC - Match by: hand_id → file_name → same seat = same player!
-- MAGIC 
-- MAGIC **Important:** This is a per-hand fact table, not an aggregated lookup.
-- MAGIC One row per (player_name, hand_id) combination.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS poker.silver.player_identity (
    -- Composite primary key
    player_name             STRING          NOT NULL    COMMENT 'Real player name from screenshots',
    hand_id                 STRING          NOT NULL    COMMENT 'Hand ID where this mapping was observed',
    
    -- Mapping data
    anonymous_id            STRING          NOT NULL    COMMENT 'Anonymized hash from hand history',
    seat                    INT                         COMMENT 'Seat number (for verification)',
    
    -- Source tracking
    file_name               STRING                      COMMENT 'Screenshot file where player name was found',
    
    -- Metadata
    created_at              TIMESTAMP                   COMMENT 'When mapping was created'
)
USING DELTA
COMMENT 'Silver layer: Per-hand mapping of real player names to anonymized IDs. Fact table - one row per player per hand.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
