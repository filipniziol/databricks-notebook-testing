-- Databricks notebook source
-- Create poker catalog for all poker analysis data

CREATE CATALOG IF NOT EXISTS poker
COMMENT 'Poker hand analysis - screenshots and GG hand histories';

-- COMMAND ----------

-- Verify catalog was created
SHOW CATALOGS LIKE 'poker';
