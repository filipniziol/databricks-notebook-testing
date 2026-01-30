-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create ML Table: cluster_definitions
-- MAGIC Cluster metadata and interpretations from K-Means model.
-- MAGIC 
-- MAGIC Updated when model is retrained. Contains centroid values and human labels.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS poker.ml.cluster_definitions (
    -- Cluster identification
    cluster_id              INT             NOT NULL    COMMENT 'Cluster ID (PK)',
    cluster_name            STRING          NOT NULL    COMMENT 'Human-readable name (e.g., LAG, Nit, Calling Station)',
    cluster_description     STRING                      COMMENT 'Detailed description for GPT prompt',
    strategy_vs             STRING                      COMMENT 'How to play against this type',
    
    -- Centroid values (typical player in this cluster)
    centroid_vpip           DOUBLE                      COMMENT 'Centroid VPIP %',
    centroid_pfr            DOUBLE                      COMMENT 'Centroid PFR %',
    centroid_three_bet      DOUBLE                      COMMENT 'Centroid 3-bet %',
    centroid_af             DOUBLE                      COMMENT 'Centroid Aggression Factor',
    centroid_wtsd           DOUBLE                      COMMENT 'Centroid WTSD %',
    centroid_wsd            DOUBLE                      COMMENT 'Centroid W$SD %',
    centroid_allin          DOUBLE                      COMMENT 'Centroid All-in %',
    
    -- Cluster statistics
    player_count            INT                         COMMENT 'Number of players in this cluster',
    pct_of_total            DOUBLE                      COMMENT 'Percentage of all players',
    
    -- Metadata
    model_version           STRING                      COMMENT 'MLflow model version',
    created_at              TIMESTAMP                   COMMENT 'When cluster was defined'
)
USING DELTA
COMMENT 'ML layer: Cluster definitions with centroids and human interpretations'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
