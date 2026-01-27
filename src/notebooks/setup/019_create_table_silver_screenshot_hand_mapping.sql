-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Silver Table: screenshot_hand_mapping
-- MAGIC Bridge table mapping screenshots to hands (n:1 relationship)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS poker.silver.screenshot_hand_mapping (
    -- Keys
    file_name               STRING          NOT NULL    COMMENT 'FK to silver.screenshots (PK)',
    hand_id                 STRING                      COMMENT 'FK to silver.hands (matched hand)',
    
    -- Match quality
    match_confidence        STRING                      COMMENT 'high/medium/low based on criteria matched',
    match_method            STRING                      COMMENT 'Description: cards+pos+time, cards+time, manual',
    time_diff_seconds       INT                         COMMENT 'Difference in seconds between screenshot and hand',
    
    -- Debug info
    screenshot_hero_cards   STRING                      COMMENT 'Cards from screenshot (for debugging)',
    hand_hero_cards         STRING                      COMMENT 'Cards from hand history (for debugging)',
    screenshot_hero_pos     STRING                      COMMENT 'Position from screenshot (normalized)',
    hand_hero_pos           STRING                      COMMENT 'Position from hand (normalized)',
    
    -- Metadata
    matched_at              TIMESTAMP                   COMMENT 'When mapping was created'
)
USING DELTA
COMMENT 'Silver layer: Maps screenshots to hands. One hand can have multiple screenshots (preflop, flop, turn shots).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
