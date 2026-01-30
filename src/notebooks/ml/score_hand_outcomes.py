# Databricks notebook source
# MAGIC %md
# MAGIC # Score Hand Outcomes
# MAGIC 
# MAGIC Batch inference: predict win probability for recent hands.
# MAGIC 
# MAGIC **Schedule:** Daily (after features update)
# MAGIC **Input:** `poker.ml.hand_features` (Feature Table)
# MAGIC **Output:** `poker.gold.hand_win_predictions` (Delta Table)

# COMMAND ----------

import mlflow
import json
from mlflow import MlflowClient
import pandas as pd

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

MODEL_NAME = "poker.ml.hand_outcome_predictor"
FEATURE_TABLE_NAME = "poker.ml.hand_features"
OUTPUT_TABLE = "poker.gold.hand_win_predictions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Model

# COMMAND ----------

client = MlflowClient()

# Try champion alias first, fallback to latest version
try:
    model_version = client.get_model_version_by_alias(MODEL_NAME, "champion")
    model_uri = f"models:/{MODEL_NAME}@champion"
    print(f"Using champion alias (version {model_version.version})")
except Exception as e:
    print(f"Champion alias not found, searching for latest version...")
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    if not versions:
        raise Exception(f"No model versions found for {MODEL_NAME}")
    latest = max(versions, key=lambda v: int(v.version))
    model_uri = f"models:/{MODEL_NAME}/{latest.version}"
    print(f"Using version {latest.version}")

# Load model
model = mlflow.sklearn.load_model(model_uri)
print(f"Model loaded: {model_uri}")

# COMMAND ----------

# Load feature config to get column order
run_id = client.get_model_version_by_alias(MODEL_NAME, "champion").run_id if 'model_version' in dir() else latest.run_id

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

# Predict
win_probabilities = model.predict_proba(X)[:, 1]

# Add predictions to dataframe
df_features['win_probability'] = win_probabilities
df_features['predicted_win'] = (win_probabilities >= 0.5).astype(int)

# COMMAND ----------

# Preview predictions
display(df_features[['hand_id', 'hand_strength_score', 'position_encoded', 'stack_bb', 
                     'won_hand', 'win_probability', 'predicted_win']].head(20))

# COMMAND ----------

# Check prediction accuracy
df_features['correct_prediction'] = (df_features['predicted_win'] == df_features['won_hand']).astype(int)
accuracy = df_features['correct_prediction'].mean()
print(f"Overall accuracy: {accuracy:.2%}")

# By position
print("\nAccuracy by position:")
display(df_features.groupby('position_encoded').agg({
    'correct_prediction': 'mean',
    'win_probability': 'mean',
    'won_hand': 'mean'
}).round(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Predictions

# COMMAND ----------

# Select columns for output
output_cols = [
    'hand_id',
    'hand_strength_score',
    'position_encoded',
    'is_late_position',
    'stack_bb',
    'opponent_count',
    'win_probability',
    'predicted_win',
    'won_hand',
    'feature_timestamp'
]

df_output = spark.createDataFrame(df_features[output_cols])

# Overwrite predictions table
df_output.write.mode("overwrite").saveAsTable(OUTPUT_TABLE)

print(f"Saved {df_output.count()} predictions to {OUTPUT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis: When Model Performs Best/Worst

# COMMAND ----------

# High confidence correct vs wrong
df_features['confidence'] = abs(df_features['win_probability'] - 0.5) * 2  # 0-1 scale

# High confidence (>0.8) accuracy
high_conf = df_features[df_features['confidence'] > 0.6]
if len(high_conf) > 0:
    print(f"High confidence predictions: {len(high_conf)}")
    print(f"High confidence accuracy: {high_conf['correct_prediction'].mean():.2%}")

# Low confidence accuracy  
low_conf = df_features[df_features['confidence'] <= 0.3]
if len(low_conf) > 0:
    print(f"\nLow confidence predictions: {len(low_conf)}")
    print(f"Low confidence accuracy: {low_conf['correct_prediction'].mean():.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Schema
# MAGIC 
# MAGIC | Column | Description |
# MAGIC |--------|-------------|
# MAGIC | hand_id | Unique hand identifier |
# MAGIC | hand_strength_score | Computed hand strength |
# MAGIC | position_encoded | 0=early, 1=mid, 2=late, 3=blinds |
# MAGIC | stack_bb | Stack depth in big blinds |
# MAGIC | win_probability | Model's predicted win probability |
# MAGIC | predicted_win | Binary prediction (prob >= 0.5) |
# MAGIC | won_hand | Actual outcome (ground truth) |
