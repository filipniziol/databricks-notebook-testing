# Databricks notebook source
# MAGIC %md
# MAGIC # Train Hand Outcome Model
# MAGIC 
# MAGIC Trains binary classifier to predict if hero will win the hand.
# MAGIC 
# MAGIC **Schedule:** Manual/Weekly
# MAGIC **Input:** `poker.ml.hand_features` (Feature Table)
# MAGIC **Output:** `poker.ml.hand_outcome_predictor` (Registered Model)

# COMMAND ----------

import mlflow
import json
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
import pandas as pd

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

FEATURE_TABLE_NAME = "poker.ml.hand_features"
MODEL_NAME = "poker.ml.hand_outcome_predictor"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Features

# COMMAND ----------

# Load features
df_features = spark.table(FEATURE_TABLE_NAME).toPandas()

print(f"Total samples: {len(df_features)}")
print(f"Win rate: {df_features['won_hand'].mean():.2%}")

# COMMAND ----------

# Feature columns (exclude targets and metadata)
feature_cols = [
    'high_card_rank',
    'low_card_rank', 
    'is_suited',
    'is_pocket_pair',
    'hand_strength_score',
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
y = df_features['won_hand']

print(f"Features shape: {X.shape}")
print(f"Class distribution:\n{y.value_counts(normalize=True)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Model

# COMMAND ----------

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"Train: {len(X_train)}, Test: {len(X_test)}")

# COMMAND ----------

# Create pipeline (scaler + classifier)
pipeline = Pipeline([
    ('scaler', StandardScaler()),
    ('classifier', RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        min_samples_split=20,
        class_weight='balanced',  # Handle imbalanced classes
        random_state=42,
        n_jobs=-1
    ))
])

# COMMAND ----------

# Train with MLflow tracking
with mlflow.start_run(run_name="hand_outcome_rf") as run:
    
    # Fit pipeline
    pipeline.fit(X_train, y_train)
    
    # Evaluate
    y_pred = pipeline.predict(X_test)
    y_proba = pipeline.predict_proba(X_test)[:, 1]
    
    # Metrics
    accuracy = (y_pred == y_test).mean()
    auc_roc = roc_auc_score(y_test, y_proba)
    
    print(f"Accuracy: {accuracy:.3f}")
    print(f"AUC-ROC: {auc_roc:.3f}")
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))
    
    # Log metrics
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("auc_roc", auc_roc)
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 10)
    mlflow.log_param("n_features", len(feature_cols))
    
    # Feature importance
    feature_importance = dict(zip(
        feature_cols, 
        pipeline.named_steps['classifier'].feature_importances_
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
        "target_column": "won_hand",
        "model_type": "RandomForestClassifier",
        "description": "Predicts probability of hero winning the hand"
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
# MAGIC ## Model Summary
# MAGIC 
# MAGIC The Hand Outcome Predictor uses:
# MAGIC - **Hand strength:** Card ranks, suited, pocket pair
# MAGIC - **Position:** Early/Middle/Late/Blinds
# MAGIC - **Stack depth:** BB count
# MAGIC - **Opponent info:** Count, cluster distribution
# MAGIC - **Preflop action:** Raises, hero raised
# MAGIC 
# MAGIC Output: `win_probability` (0-1) - probability hero wins the hand
