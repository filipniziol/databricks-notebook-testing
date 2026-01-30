# Databricks notebook source
# MAGIC %md
# MAGIC # Score Player Clusters (Batch Inference)
# MAGIC 
# MAGIC Assigns cluster labels to all players using the trained model.
# MAGIC 
# MAGIC **Schedule:** Daily (after feature update)
# MAGIC **Input:** `poker.ml.player_features` + `poker.ml.player_clustering` model
# MAGIC **Output:** `poker.gold.player_clusters`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

import mlflow
import mlflow.sklearn
import json
from mlflow import MlflowClient

from databricks.feature_engineering import FeatureEngineeringClient

import pandas as pd
import numpy as np
from datetime import datetime

# Config
MODEL_NAME = "poker.ml.player_clustering"
MODEL_ALIAS = "champion"  # Use alias for production
FEATURE_TABLE_NAME = "poker.ml.player_features"
OUTPUT_TABLE = "poker.gold.player_clusters"

fe = FeatureEngineeringClient()
client = MlflowClient()

FEATURE_COLUMNS = [
    'vpip_pct', 'pfr_pct', 'three_bet_pct', 'aggression_factor',
    'wtsd_pct', 'wsd_pct', 'allin_pct'
]

print(f"Model: {MODEL_NAME}@{MODEL_ALIAS}")
print(f"Output: {OUTPUT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Model and Cluster Definitions

# COMMAND ----------

# Load model using alias (production-safe)
# Unity Catalog doesn't support "latest" - must use alias or version number
model_uri = f"models:/{MODEL_NAME}@{MODEL_ALIAS}"
run_id = None

try:
    # Try loading by alias first
    model_version_info = client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
    pipeline = mlflow.sklearn.load_model(model_uri)
    run_id = model_version_info.run_id
    print(f"Loaded model: {model_uri}")
except Exception as e:
    # Fallback: get latest version by searching model versions
    print(f"Alias '{MODEL_ALIAS}' not found, searching for latest version...")
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    if not versions:
        raise ValueError(f"No versions found for model {MODEL_NAME}")
    
    # Sort by version number (descending) and get the latest
    latest_version = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
    model_uri = f"models:/{MODEL_NAME}/{latest_version.version}"
    pipeline = mlflow.sklearn.load_model(model_uri)
    run_id = latest_version.run_id
    print(f"Loaded model: {model_uri} (version {latest_version.version})")

# Load cluster definitions from artifact (run_id already set above)
artifact_path = client.download_artifacts(run_id, "cluster_definitions.json")
with open(artifact_path, 'r') as f:
    cluster_definitions = json.load(f)

print(f"\nCluster definitions loaded:")
for cid, info in cluster_definitions.items():
    print(f"  {cid}: {info['cluster_name']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Features

# COMMAND ----------

# Load all players from Feature Store
df_all = fe.read_table(name=FEATURE_TABLE_NAME).toPandas()

print(f"Loaded {len(df_all)} players from Feature Store")

# Handle NaN with median (same as training)
for col in FEATURE_COLUMNS:
    median_val = df_all[col].median()
    df_all[col] = df_all[col].fillna(median_val)

X_all = df_all[FEATURE_COLUMNS].values

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Predict Clusters

# COMMAND ----------

# Predict clusters
cluster_ids = pipeline.predict(X_all)

# Get distances for confidence scoring
scaler = pipeline.named_steps['scaler']
kmeans = pipeline.named_steps['kmeans']
X_scaled = scaler.transform(X_all)
distances = kmeans.transform(X_scaled)
assigned_distances = [distances[i, c] for i, c in enumerate(cluster_ids)]

# Build result
df_result = df_all[['player_name', 'total_hands']].copy()
df_result['cluster_id'] = cluster_ids
df_result['distance_to_centroid'] = assigned_distances

# Map cluster names from definitions
df_result['cluster_name'] = df_result['cluster_id'].apply(lambda x: cluster_definitions[str(x)]['cluster_name'])
df_result['cluster_description'] = df_result['cluster_id'].apply(lambda x: cluster_definitions[str(x)]['description'])
df_result['strategy_vs'] = df_result['cluster_id'].apply(lambda x: cluster_definitions[str(x)]['strategy'])

# Confidence based on hands played
df_result['confidence'] = df_result['total_hands'].apply(
    lambda x: 'high' if x >= 30 else ('medium' if x >= 10 else 'low')
)

# Metadata
df_result['model_version'] = model_uri
df_result['scored_at'] = datetime.now()

print(f"\nCluster Distribution:")
print(df_result['cluster_name'].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Save Results

# COMMAND ----------

# Convert types
df_result['cluster_id'] = df_result['cluster_id'].astype(int)
df_result['total_hands'] = df_result['total_hands'].astype(int)
df_result['distance_to_centroid'] = df_result['distance_to_centroid'].astype(float)

# Save to Delta
spark_df = spark.createDataFrame(df_result)
spark_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(OUTPUT_TABLE)

print(f"Saved {len(df_result)} players to {OUTPUT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        cluster_name,
        COUNT(*) as players,
        ROUND(AVG(total_hands), 0) as avg_hands,
        SUM(CASE WHEN confidence = 'high' THEN 1 ELSE 0 END) as high_confidence
    FROM {OUTPUT_TABLE}
    GROUP BY cluster_name
    ORDER BY players DESC
"""))

# COMMAND ----------

# Sample players per cluster
display(spark.sql(f"""
    SELECT player_name, cluster_name, total_hands, confidence, strategy_vs
    FROM {OUTPUT_TABLE}
    WHERE confidence = 'high'
    ORDER BY cluster_name, total_hands DESC
    LIMIT 15
"""))
