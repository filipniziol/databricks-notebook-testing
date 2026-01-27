-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Silver Table: screenshot_history
-- MAGIC Hand history per street per screenshot - tracks action progression

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS poker.silver.screenshot_history (
    -- Composite primary key
    file_name               STRING          NOT NULL    COMMENT 'FK to screenshots',
    street                  STRING          NOT NULL    COMMENT 'Street: preflop, flop, turn, river',
    
    -- Street state
    board                   ARRAY<STRING>               COMMENT 'Board cards at this street',
    actions                 ARRAY<STRING>               COMMENT 'Available actions at this street',
    bets                    MAP<STRING, DECIMAL(10,2)>  COMMENT 'Bets per position {SB: 0.5, BB: 1.0, MP: 3.5}',
    pot                     DECIMAL(10,2)               COMMENT 'Pot size at this street',
    hero_prev_action        STRING                      COMMENT 'Hero previous action on this street'
)
USING DELTA
COMMENT 'Silver layer: Hand history per street (1:N from screenshots)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Composite PK: (file_name, street)
-- ALTER TABLE poker.silver.screenshot_history ADD CONSTRAINT pk_screenshot_history PRIMARY KEY (file_name, street);
-- ALTER TABLE poker.silver.screenshot_history ADD CONSTRAINT fk_screenshot_history FOREIGN KEY (file_name) REFERENCES poker.silver.screenshots(file_name);
