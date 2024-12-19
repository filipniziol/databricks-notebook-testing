-- Databricks notebook source
CREATE WIDGET TEXT catalog_name DEFAULT 'dev';
CREATE WIDGET TEXT schema_name DEFAULT 'silver';
CREATE WIDGET TEXT table_name DEFAULT 'target_table';

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name || '.' || :schema_name ||  '.' || :table_name)
(
    id INT,
    name STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    status STRING
);
