-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold Table: player_clusters
-- MAGIC Player cluster assignments from K-Means model.
-- MAGIC 
-- MAGIC This table is refreshed when the clustering model is retrained.
-- MAGIC Use this for GPT prompt enrichment.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS poker.gold.player_clusters (
    -- Player identification
    player_name             STRING          NOT NULL    COMMENT 'Player name (PK)',
    
    -- Cluster assignment
    cluster_id              INT             NOT NULL    COMMENT 'Cluster ID from K-Means model',
    cluster_name            STRING                      COMMENT 'Human-readable cluster name (e.g., LAG, Nit, Fish)',
    cluster_description     STRING                      COMMENT 'Strategy description for GPT prompt',
    
    -- Confidence / reliability
    distance_to_centroid    DOUBLE                      COMMENT 'Distance to cluster center (lower = more typical)',
    total_hands             INT                         COMMENT 'Number of hands for this player',
    confidence              STRING                      COMMENT 'low (<10 hands), medium (10-30), high (30+)',
    
    -- Key stats for quick reference
    vpip_pct                DOUBLE                      COMMENT 'VPIP percentage',
    pfr_pct                 DOUBLE                      COMMENT 'PFR percentage', 
    aggression_factor       DOUBLE                      COMMENT 'Aggression factor',
    
    -- Metadata
    model_version           STRING                      COMMENT 'MLflow model version used',
    clustered_at            TIMESTAMP                   COMMENT 'When clustering was performed'
)
USING DELTA
COMMENT 'Gold layer: Player cluster assignments for GPT prompt enrichment. Updated by ML pipeline.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
