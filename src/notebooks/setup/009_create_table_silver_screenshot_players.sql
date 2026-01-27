-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Silver Table: screenshot_players
-- MAGIC Players (opponents) per screenshot - 1:N relationship

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS poker.silver.screenshot_players (
    -- Composite primary key
    file_name               STRING          NOT NULL    COMMENT 'FK to screenshots',
    seat                    INT             NOT NULL    COMMENT 'Seat number (1-5)',
    
    -- Player info
    pos                     STRING                      COMMENT 'Position: UTG, MP, BTN, SB, BB',
    name                    STRING                      COMMENT 'Player nickname',
    stack                   DECIMAL(10,2)               COMMENT 'Stack size in BB',
    vpip                    INT                         COMMENT 'VPIP percentage (0-100)',
    bet                     DECIMAL(10,2)               COMMENT 'Current bet in BB',
    active                  BOOLEAN                     COMMENT 'Is player still in hand',
    is_allin                BOOLEAN                     COMMENT 'Is player all-in'
)
USING DELTA
COMMENT 'Silver layer: Opponents per screenshot (1:N from screenshots)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Composite PK: (file_name, seat)
-- ALTER TABLE poker.silver.screenshot_players ADD CONSTRAINT pk_screenshot_players PRIMARY KEY (file_name, seat);
-- ALTER TABLE poker.silver.screenshot_players ADD CONSTRAINT fk_screenshot_players FOREIGN KEY (file_name) REFERENCES poker.silver.screenshots(file_name);
