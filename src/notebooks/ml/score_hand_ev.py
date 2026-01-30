# Databricks notebook source
# MAGIC %md
# MAGIC # Score Hand EV
# MAGIC 
# MAGIC Batch inference: predict expected profit (BB) for all hands.
# MAGIC 
# MAGIC **Schedule:** Daily (after features update)
# MAGIC **Input:** `poker.ml.hand_features` (Feature Table)
# MAGIC **Output:** `poker.gold.hand_ev_predictions` (Delta Table)

# COMMAND ----------

import mlflow
import json
from mlflow import MlflowClient
import pandas as pd

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

MODEL_NAME = "poker.ml.hand_ev_predictor"
FEATURE_TABLE_NAME = "poker.ml.hand_features"
OUTPUT_TABLE = "poker.gold.hand_ev_predictions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Model

# COMMAND ----------

client = MlflowClient()

# Try champion alias first, fallback to latest version
try:
    model_version = client.get_model_version_by_alias(MODEL_NAME, "champion")
    model_uri = f"models:/{MODEL_NAME}@champion"
    run_id = model_version.run_id
    print(f"Using champion alias (version {model_version.version})")
except Exception as e:
    print(f"Champion alias not found, searching for latest version...")
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    if not versions:
        raise Exception(f"No model versions found for {MODEL_NAME}")
    latest = max(versions, key=lambda v: int(v.version))
    model_uri = f"models:/{MODEL_NAME}/{latest.version}"
    run_id = latest.run_id
    print(f"Using version {latest.version}")

# Load model
model = mlflow.sklearn.load_model(model_uri)
print(f"Model loaded: {model_uri}")

# COMMAND ----------

# Load feature config to get column order
artifact_path = client.download_artifacts(run_id, "model_config.json", "/tmp")
with open(artifact_path) as f:
    config = json.load(f)

feature_cols = config['feature_columns']
print(f"Feature columns: {feature_cols}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score Hands

# COMMAND ----------

# Load features
df_features = spark.table(FEATURE_TABLE_NAME).toPandas()
print(f"Total hands to score: {len(df_features)}")

# Prepare features in correct order
X = df_features[feature_cols].fillna(0)

# Predict EV
predicted_ev = model.predict(X)

# Add predictions to dataframe
df_features['predicted_ev_bb'] = predicted_ev

# COMMAND ----------

# Preview predictions
print("Sample predictions:")
display(df_features[['hand_id', 'hand_strength_score', 'is_final_stage', 'is_late_position', 
                     'stack_bb', 'profit_bb', 'predicted_ev_bb']].head(20))

# COMMAND ----------

# Compare predicted vs actual
print(f"\nPrediction quality:")
print(f"Correlation: {df_features['profit_bb'].corr(df_features['predicted_ev_bb']):.3f}")

# Directional accuracy
directional_acc = ((df_features['predicted_ev_bb'] > 0) == (df_features['profit_bb'] > 0)).mean()
print(f"Directional accuracy: {directional_acc:.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze: Best and Worst Spots

# COMMAND ----------

# Top 10 best predicted EV spots
print("Top 10 Highest EV Spots:")
display(df_features.nlargest(10, 'predicted_ev_bb')[
    ['hand_id', 'high_card_rank', 'low_card_rank', 'is_suited', 'is_final_stage', 
     'is_late_position', 'stack_bb', 'predicted_ev_bb', 'profit_bb']
])

# COMMAND ----------

# Worst predicted EV spots (should fold)
print("Top 10 Lowest EV Spots:")
display(df_features.nsmallest(10, 'predicted_ev_bb')[
    ['hand_id', 'high_card_rank', 'low_card_rank', 'is_suited', 'is_final_stage', 
     'is_late_position', 'stack_bb', 'predicted_ev_bb', 'profit_bb']
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Predictions

# COMMAND ----------

# Select columns for output
output_cols = [
    'hand_id',
    'high_card_rank',
    'low_card_rank',
    'is_suited',
    'is_pocket_pair',
    'hand_strength_score',
    'is_final_stage',
    'position_encoded',
    'is_late_position',
    'stack_bb',
    'opponent_count',
    'predicted_ev_bb',
    'profit_bb',
    'won_hand',
    'feature_timestamp'
]

df_output = spark.createDataFrame(df_features[output_cols])

# Overwrite predictions table
df_output.write.mode("overwrite").saveAsTable(OUTPUT_TABLE)

print(f"Saved {df_output.count()} predictions to {OUTPUT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by Situation

# COMMAND ----------

# EV by stage and position
summary = df_features.groupby(['is_final_stage', 'is_late_position']).agg({
    'predicted_ev_bb': ['mean', 'std', 'count'],
    'profit_bb': 'mean'
}).round(2)

print("Average EV by Stage and Position:")
display(summary)
