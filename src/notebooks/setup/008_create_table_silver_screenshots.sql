-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Silver Table: screenshots
-- MAGIC Main screenshots table with hero flattened and GPT advice summary

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS poker.silver.screenshots (
    -- Primary key
    file_name               STRING          NOT NULL    COMMENT 'Source filename (PK)',
    screenshot_at           TIMESTAMP       NOT NULL    COMMENT 'Timestamp parsed from filename (screenshot_YYYYMMDD_HHMMSS_microsec)',
    
    -- Game state
    action_type             STRING                      COMMENT 'Action type: play, wait, etc.',
    stage                   STRING                      COMMENT 'Tournament stage: rush, final, null',
    street                  STRING                      COMMENT 'Current street: preflop, flop, turn, river',
    board                   ARRAY<STRING>               COMMENT 'Community cards on board',
    pot                     DECIMAL(10,2)               COMMENT 'Total pot size in BB',
    to_call                 DECIMAL(10,2)               COMMENT 'Amount to call in BB',
    actions                 ARRAY<STRING>               COMMENT 'Available actions: fold, check, call, raise, bet',
    
    -- Hero (flattened - always 1:1)
    hero_cards              STRING                      COMMENT 'Hero hole cards (e.g. AhKs)',
    hero_pos                STRING                      COMMENT 'Hero position: UTG, MP, BTN, SB, BB',
    hero_stack              DECIMAL(10,2)               COMMENT 'Hero stack in BB',
    hero_bet                DECIMAL(10,2)               COMMENT 'Hero current bet in BB',
    hero_is_allin           BOOLEAN                     COMMENT 'Is hero all-in',
    hero_seat               INT                         COMMENT 'Hero seat number (1-5)',
    
    -- GPT advice (flattened - key fields only)
    gpt_recommendation      STRING                      COMMENT 'GPT recommendation: fold, call, raise, check, bet',
    gpt_action              STRING                      COMMENT 'GPT parsed action',
    gpt_amount              DECIMAL(10,2)               COMMENT 'GPT recommended amount (for raise/bet)',
    gpt_time                DECIMAL(6,3)                COMMENT 'GPT response time in seconds',
    
    -- Metadata
    ingested_at             TIMESTAMP                   COMMENT 'When record was loaded to silver'
)
USING DELTA
COMMENT 'Silver layer: Parsed screenshot analysis with hero and GPT advice flattened'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Create index on common query patterns
-- ALTER TABLE poker.silver.screenshots ADD CONSTRAINT pk_screenshots PRIMARY KEY (file_name);
