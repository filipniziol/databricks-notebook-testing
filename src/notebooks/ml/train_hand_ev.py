# Databricks notebook source
# MAGIC %md
# MAGIC # Train Hand EV Model
# MAGIC 
# MAGIC Predicts expected profit in BB for a given hand situation.
# MAGIC 
# MAGIC **Schedule:** Manual/Weekly
# MAGIC **Input:** `poker.ml.hand_features` (Feature Table)
# MAGIC **Output:** `poker.ml.hand_ev_predictor` (Registered Model)
# MAGIC
# MAGIC **Input per row:** Hero's hole cards + position + stack + stage + opponents
# MAGIC **Output:** Expected profit in big blinds (can be negative)

# COMMAND ----------

import mlflow
import json
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np
import pandas as pd

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

FEATURE_TABLE_NAME = "poker.ml.hand_features"
MODEL_NAME = "poker.ml.hand_ev_predictor"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Features

# COMMAND ----------

# Load features
df_features = spark.table(FEATURE_TABLE_NAME).toPandas()

# Remove outliers in profit_bb (beyond 3 std)
profit_mean = df_features['profit_bb'].mean()
profit_std = df_features['profit_bb'].std()
df_features = df_features[
    (df_features['profit_bb'] > profit_mean - 3*profit_std) & 
    (df_features['profit_bb'] < profit_mean + 3*profit_std)
]

print(f"Total samples: {len(df_features)}")
print(f"Profit BB stats:")
print(f"  Mean: {df_features['profit_bb'].mean():.2f} BB")
print(f"  Std:  {df_features['profit_bb'].std():.2f} BB")
print(f"  Min:  {df_features['profit_bb'].min():.2f} BB")
print(f"  Max:  {df_features['profit_bb'].max():.2f} BB")

# COMMAND ----------

# Feature columns (exclude targets and metadata)
feature_cols = [
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
    'avg_opponent_cluster',
    'had_preflop_raise',
    'hero_raised_preflop',
    'preflop_raise_count'
]

X = df_features[feature_cols].fillna(0)
y = df_features['profit_bb'].fillna(0)

print(f"Features shape: {X.shape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Model

# COMMAND ----------

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

print(f"Train: {len(X_train)}, Test: {len(X_test)}")

# COMMAND ----------

# Create pipeline (scaler + regressor)
pipeline = Pipeline([
    ('scaler', StandardScaler()),
    ('regressor', RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        min_samples_split=20,
        random_state=42,
        n_jobs=-1
    ))
])

# COMMAND ----------

# Train with MLflow tracking
with mlflow.start_run(run_name="hand_ev_rf") as run:
    
    # Fit pipeline
    pipeline.fit(X_train, y_train)
    
    # Evaluate
    y_pred = pipeline.predict(X_test)
    
    # Metrics
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)
    
    print(f"MAE:  {mae:.2f} BB (average prediction error)")
    print(f"RMSE: {rmse:.2f} BB")
    print(f"R²:   {r2:.3f}")
    
    # Directional accuracy (did we predict win/loss correctly?)
    directional_acc = ((y_pred > 0) == (y_test > 0)).mean()
    print(f"Directional accuracy: {directional_acc:.2%}")
    
    # Log metrics
    mlflow.log_metric("mae", mae)
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2)
    mlflow.log_metric("directional_accuracy", directional_acc)
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 10)
    mlflow.log_param("n_features", len(feature_cols))
    
    # Feature importance
    feature_importance = dict(zip(
        feature_cols, 
        pipeline.named_steps['regressor'].feature_importances_
    ))
    print("\nFeature Importance:")
    for feat, imp in sorted(feature_importance.items(), key=lambda x: -x[1]):
        print(f"  {feat}: {imp:.3f}")
    
    # Save feature importance as artifact
    with open("/tmp/feature_importance.json", "w") as f:
        json.dump(feature_importance, f, indent=2)
    mlflow.log_artifact("/tmp/feature_importance.json")
    
    # Save feature columns for inference
    feature_config = {
        "feature_columns": feature_cols,
        "target_column": "profit_bb",
        "model_type": "RandomForestRegressor",
        "description": "Predicts expected profit in BB for a hand"
    }
    with open("/tmp/model_config.json", "w") as f:
        json.dump(feature_config, f, indent=2)
    mlflow.log_artifact("/tmp/model_config.json")
    
    # Register model with input_example for auto signature
    mlflow.sklearn.log_model(
        sk_model=pipeline,
        artifact_path="model",
        input_example=X_train[:5],
        registered_model_name=MODEL_NAME
    )
    
    run_id = run.info.run_id
    print(f"\nRun ID: {run_id}")
    print(f"Model registered: {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Champion Alias

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()

# Get latest version
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest_version = max(versions, key=lambda v: int(v.version))

print(f"Latest version: {latest_version.version}")

# Set champion alias
client.set_registered_model_alias(
    name=MODEL_NAME,
    alias="champion",
    version=latest_version.version
)

print(f"Set @champion alias to version {latest_version.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze: When is EV Highest?

# COMMAND ----------

# Add predictions to test set
df_test = X_test.copy()
df_test['actual_profit_bb'] = y_test.values
df_test['predicted_profit_bb'] = y_pred

# Best predicted spots
print("Top 10 highest EV situations (predicted):")
display(df_test.nlargest(10, 'predicted_profit_bb')[
    ['hand_strength_score', 'is_final_stage', 'is_late_position', 'stack_bb', 'predicted_profit_bb', 'actual_profit_bb']
])

# COMMAND ----------

# EV by position and stage
print("\nAverage predicted EV by position and stage:")
df_test['position_name'] = df_test['position_encoded'].map({0: 'Early', 1: 'Middle', 2: 'Late', 3: 'Blinds'})
df_test['stage_name'] = df_test['is_final_stage'].map({0: 'Rush', 1: 'Final'})

ev_by_situation = df_test.groupby(['stage_name', 'position_name']).agg({
    'predicted_profit_bb': 'mean',
    'actual_profit_bb': 'mean'
}).round(2)
display(ev_by_situation)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Summary
# MAGIC 
# MAGIC **Input (1 row = 1 hand dealt to hero):**
# MAGIC - Hole cards: rank1, rank2, suited, pocket pair
# MAGIC - Position: early/middle/late/blinds
# MAGIC - Stack depth in BB
# MAGIC - Stage: rush vs final
# MAGIC - Opponent count and types
# MAGIC - Preflop action so far
# MAGIC 
# MAGIC **Output:**
# MAGIC - `predicted_profit_bb` - expected profit/loss in big blinds
# MAGIC 
# MAGIC **Interpretation:**
# MAGIC - Positive = profitable spot
# MAGIC - Negative = expected to lose
# MAGIC - Higher value = better opportunity
