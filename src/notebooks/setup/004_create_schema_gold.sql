-- Databricks notebook source
-- Create gold schema for analytics/aggregations
-- Requires: catalog poker exists

USE CATALOG poker;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Analytics layer - decision analysis, player stats, ML features';

-- COMMAND ----------

-- Create volume for gold output files (exports, reports)
CREATE VOLUME IF NOT EXISTS poker.gold.gold
COMMENT 'Gold volume for analytics output files';

-- COMMAND ----------

-- Verify schema and volume
SHOW VOLUMES IN poker.gold;
