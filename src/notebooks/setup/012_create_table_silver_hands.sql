-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Silver Table: hands
-- MAGIC Hand headers - one row per hand. Match to screenshots via hand_timestamp ≈ screenshot_at

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS poker.silver.hands (
    -- Primary key
    hand_id                 STRING          NOT NULL    COMMENT 'Hand ID e.g. BR1006865849 (PK)',
    
    -- Tournament context (FK to tournaments)
    tournament_id           BIGINT                      COMMENT 'FK to silver.tournaments',
    tournament_name         STRING                      COMMENT 'Tournament name for quick access',
    
    -- Game parameters
    game_type               STRING                      COMMENT 'Hold''em No Limit',
    level                   INT                         COMMENT 'Blind level (e.g. 10)',
    small_blind             DECIMAL(10,2)               COMMENT 'SB amount in chips',
    big_blind               DECIMAL(10,2)               COMMENT 'BB amount in chips',
    ante                    DECIMAL(10,2)               COMMENT 'Ante amount in chips',
    
    -- Table info
    table_name              STRING                      COMMENT 'Table identifier',
    max_seats               INT                         COMMENT 'Max seats (9-max, 5-max)',
    button_seat             INT                         COMMENT 'Dealer button seat number',
    
    -- Timing (KEY FOR SCREENSHOT MATCHING)
    hand_timestamp          TIMESTAMP       NOT NULL    COMMENT 'Hand start time - use to match with screenshot_at',
    
    -- Board & result
    board                   ARRAY<STRING>               COMMENT 'Community cards [Jc, As, 5s, Td, 4h]',
    total_pot               DECIMAL(10,2)               COMMENT 'Final pot size',
    rake                    DECIMAL(10,2)               COMMENT 'Rake taken',
    
    -- Hero quick access (denormalized for easy queries)
    hero_seat               INT                         COMMENT 'Hero seat number',
    hero_position           STRING                      COMMENT 'Hero position: BTN, SB, BB, UTG, etc.',
    hero_cards              STRING                      COMMENT 'Hero hole cards e.g. QdQs',
    hero_chips_start        DECIMAL(10,2)               COMMENT 'Hero starting chips',
    hero_chips_end          DECIMAL(10,2)               COMMENT 'Hero ending chips',
    hero_result             STRING                      COMMENT 'won/lost/folded',
    hero_profit             DECIMAL(10,2)               COMMENT 'Profit/loss: chips_end - chips_start (negative if lost)',
    
    -- Metadata
    file_name               STRING                      COMMENT 'Source filename',
    ingested_at             TIMESTAMP                   COMMENT 'When record was loaded to silver'
)
USING DELTA
COMMENT 'Silver layer: Hand headers - one row per poker hand. Match to screenshots via hand_timestamp ≈ screenshot_at'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
