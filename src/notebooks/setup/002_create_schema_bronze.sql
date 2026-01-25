-- Databricks notebook source
-- Create bronze schema for raw data
-- Requires: catalog poker exists

USE CATALOG poker;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw data layer - source JSON files from screenshot analyzer and GG hand histories';

-- COMMAND ----------

-- Create volume for bronze raw files
-- Folder structure: /analysis_json/, /gg_hand_history/
CREATE VOLUME IF NOT EXISTS poker.bronze.bronze
COMMENT 'Bronze volume for raw files (analysis_json, gg_hand_history folders)';

-- COMMAND ----------

-- Verify schema and volume
SHOW VOLUMES IN poker.bronze;
