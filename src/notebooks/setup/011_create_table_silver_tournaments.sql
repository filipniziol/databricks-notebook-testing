-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Silver Table: tournaments
-- MAGIC Tournament results parsed from GGPoker tournament history files

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS poker.silver.tournaments (
    -- Primary key
    tournament_id           BIGINT          NOT NULL    COMMENT 'Tournament ID (PK)',
    
    -- Tournament info
    tournament_name         STRING                      COMMENT 'Tournament name: Mystery Battle Royale $25',
    game_type               STRING                      COMMENT 'Game type: Hold''em No Limit',
    
    -- Buy-in breakdown (parsed from: $12.5+$2+$10.5)
    buyin_prize             DECIMAL(10,2)               COMMENT 'Prize pool contribution',
    buyin_rake              DECIMAL(10,2)               COMMENT 'Rake/fee',
    buyin_bounty            DECIMAL(10,2)               COMMENT 'Bounty contribution',
    buyin_total             DECIMAL(10,2)               COMMENT 'Total buy-in (sum)',
    
    -- Tournament details
    total_players           INT                         COMMENT 'Number of players',
    prize_pool              DECIMAL(10,2)               COMMENT 'Total prize pool',
    started_at              TIMESTAMP                   COMMENT 'Tournament start time',
    
    -- Hero result
    hero_position           INT                         COMMENT 'Final position (1st, 2nd, etc.)',
    hero_prize              DECIMAL(10,2)               COMMENT 'Prize won by hero',
    
    -- Metadata
    file_name               STRING                      COMMENT 'Source filename',
    ingested_at             TIMESTAMP                   COMMENT 'When record was loaded to silver'
)
USING DELTA
COMMENT 'Silver layer: Parsed tournament results from GGPoker history'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
