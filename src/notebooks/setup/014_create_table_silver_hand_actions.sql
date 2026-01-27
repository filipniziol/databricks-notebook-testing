-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Silver Table: hand_actions
-- MAGIC Individual actions per hand - full action sequence for replay/analysis

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS poker.silver.hand_actions (
    -- Composite primary key
    hand_id                 STRING          NOT NULL    COMMENT 'FK to silver.hands',
    action_order            INT             NOT NULL    COMMENT 'Sequential action number (1, 2, 3...)',
    
    -- Action context
    street                  STRING                      COMMENT 'preflop, flop, turn, river, showdown',
    
    -- Who acted
    seat                    INT                         COMMENT 'Seat number of player',
    player_name             STRING                      COMMENT 'Player name (anonymized or Hero)',
    is_hero                 BOOLEAN                     COMMENT 'Is this hero action',
    
    -- What action
    action_type             STRING                      COMMENT 'fold, check, call, bet, raise, posts, shows, collected',
    amount                  DECIMAL(10,2)               COMMENT 'Amount for bet/raise/call/collected',
    is_allin                BOOLEAN                     COMMENT 'Was this an all-in action',
    
    -- For raise actions
    raise_to                DECIMAL(10,2)               COMMENT 'Total raise amount (raises X to Y)',
    
    -- Raw line for debugging
    raw_line                STRING                      COMMENT 'Original text line from history'
)
USING DELTA
COMMENT 'Silver layer: Individual actions per hand in sequence. Full action history for replay/analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
