-- Databricks notebook source
CREATE WIDGET TEXT catalog_name DEFAULT 'dev'; 
CREATE WIDGET TEXT bronze_schema_name DEFAULT 'bronze'; 
CREATE WIDGET TEXT silver_schema_name DEFAULT 'silver'; 
CREATE WIDGET TEXT source_table_name DEFAULT 'source_table';
CREATE WIDGET TEXT target_table_name DEFAULT 'target_table';

-- COMMAND ----------

MERGE INTO IDENTIFIER(:catalog_name || '.' || :silver_schema_name ||  '.' || :target_table_name) AS target
USING IDENTIFIER(:catalog_name || '.' || :bronze_schema_name ||  '.' || :source_table_name) AS source
ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET
    target.name = source.name,
    target.created_at = source.created_at,
    target.updated_at = source.updated_at,
    target.status = source.status
WHEN NOT MATCHED THEN
  INSERT (id, name, created_at, updated_at, status)
  VALUES (source.id, source.name, source.created_at, source.updated_at, source.status);
