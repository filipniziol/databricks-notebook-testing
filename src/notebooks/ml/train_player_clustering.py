# Databricks notebook source
# MAGIC %md
# MAGIC # Train Player Clustering Model
# MAGIC 
# MAGIC Trains K-Means clustering model on player statistics.
# MAGIC 
# MAGIC **Schedule:** Manual / Weekly
# MAGIC **Input:** `poker.ml.player_features` (Feature Table)
# MAGIC **Output:** `poker.ml.player_clustering` (Registered Model with cluster definitions as artifact)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

import mlflow
import mlflow.sklearn
import json

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt

# MLflow config
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

EXPERIMENT_NAME = "/Shared/experiments/player_clustering"
mlflow.set_experiment(EXPERIMENT_NAME)

MODEL_NAME = "poker.ml.player_clustering"
FEATURE_TABLE_NAME = "poker.ml.player_features"

fe = FeatureEngineeringClient()

FEATURE_COLUMNS = [
    'vpip_pct', 'pfr_pct', 'three_bet_pct', 'aggression_factor',
    'wtsd_pct', 'wsd_pct', 'allin_pct'
]

MIN_HANDS_FOR_TRAINING = 10
MIN_K = 3

print(f"Experiment: {EXPERIMENT_NAME}")
print(f"Model: {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Training Data

# COMMAND ----------

# Load from Feature Store
df_all = fe.read_table(name=FEATURE_TABLE_NAME).toPandas()

# Filter training set
df_train = df_all[df_all['total_hands'] >= MIN_HANDS_FOR_TRAINING].copy()

print(f"Total players: {len(df_all)}")
print(f"Training players (≥{MIN_HANDS_FOR_TRAINING} hands): {len(df_train)}")

# Handle NaN
for col in FEATURE_COLUMNS:
    median_val = df_train[col].median()
    df_train[col] = df_train[col].fillna(median_val)

X_train = df_train[FEATURE_COLUMNS].values

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Find Optimal K

# COMMAND ----------

# Temporary scaler for K search
temp_scaler = StandardScaler()
X_train_scaled = temp_scaler.fit_transform(X_train)

K_range = range(2, 9)
inertias = []
silhouettes = []

for k in K_range:
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    km.fit(X_train_scaled)
    inertias.append(km.inertia_)
    silhouettes.append(silhouette_score(X_train_scaled, km.labels_))
    print(f"K={k}: Silhouette={silhouettes[-1]:.3f}")

# Elbow method
inertia_changes = np.diff(inertias)
inertia_acceleration = np.diff(inertia_changes)
elbow_idx = np.argmax(np.abs(inertia_acceleration)) + 1
OPTIMAL_K = max(MIN_K, list(K_range)[elbow_idx])

print(f"\nOptimal K={OPTIMAL_K} (min={MIN_K})")

# COMMAND ----------

# Plot
fig, axes = plt.subplots(1, 2, figsize=(12, 4))
axes[0].plot(K_range, inertias, 'bo-')
axes[0].axvline(x=OPTIMAL_K, color='r', linestyle='--', label=f'K={OPTIMAL_K}')
axes[0].set_xlabel('K')
axes[0].set_ylabel('Inertia')
axes[0].set_title('Elbow Method')
axes[0].legend()

axes[1].plot(K_range, silhouettes, 'go-')
axes[1].axvline(x=OPTIMAL_K, color='r', linestyle='--', label=f'K={OPTIMAL_K}')
axes[1].set_xlabel('K')
axes[1].set_ylabel('Silhouette')
axes[1].set_title('Silhouette Score')
axes[1].legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Train Final Model

# COMMAND ----------

def classify_cluster(centroid, all_centroids, feature_cols):
    """Classify cluster based on relative position among all clusters."""
    vpip = centroid[feature_cols.index('vpip_pct')]
    pfr = centroid[feature_cols.index('pfr_pct')]
    af = centroid[feature_cols.index('aggression_factor')]
    allin = centroid[feature_cols.index('allin_pct')]
    
    # Percentile ranks within clusters
    vpip_rank = sum(1 for c in all_centroids if c[feature_cols.index('vpip_pct')] < vpip) / len(all_centroids)
    pfr_rank = sum(1 for c in all_centroids if c[feature_cols.index('pfr_pct')] < pfr) / len(all_centroids)
    af_rank = sum(1 for c in all_centroids if c[feature_cols.index('aggression_factor')] < af) / len(all_centroids)
    allin_rank = sum(1 for c in all_centroids if c[feature_cols.index('allin_pct')] < allin) / len(all_centroids)
    
    is_tight = vpip_rank < 0.3
    is_loose = vpip_rank > 0.7
    is_aggressive = af_rank > 0.6 or pfr_rank > 0.6
    is_passive = af_rank < 0.4 and pfr_rank < 0.4
    is_allin_heavy = allin_rank > 0.7
    
    if is_tight and is_aggressive:
        return "TAG", "Tight-Aggressive", "Respect their raises"
    elif is_tight and is_passive:
        return "Nit", "Ultra-tight passive", "Steal blinds"
    elif is_loose and is_aggressive:
        return "LAG", "Loose-Aggressive", "Call down lighter"
    elif is_loose and is_passive:
        return "Fish", "Loose-Passive", "Value bet relentlessly"
    elif is_allin_heavy:
        return "Maniac", "High all-in frequency", "Tighten up, call with premiums"
    elif is_aggressive:
        return "Reg", "Balanced aggressive", "Play solid"
    else:
        return "Rec", "Recreational player", "Standard play"

# COMMAND ----------

with mlflow.start_run(run_name=f"kmeans_k{OPTIMAL_K}") as run:
    
    # Create pipeline (scaler + kmeans)
    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('kmeans', KMeans(n_clusters=OPTIMAL_K, random_state=42, n_init=10))
    ])
    pipeline.fit(X_train)
    
    # Get components
    kmeans = pipeline.named_steps['kmeans']
    scaler = pipeline.named_steps['scaler']
    X_scaled = scaler.transform(X_train)
    
    # Metrics
    silhouette = silhouette_score(X_scaled, kmeans.labels_)
    
    # Build cluster definitions
    centroids_original = scaler.inverse_transform(kmeans.cluster_centers_)
    cluster_counts = pd.Series(kmeans.labels_).value_counts().sort_index()
    
    cluster_definitions = {}
    for i, centroid in enumerate(centroids_original):
        name, desc, strategy = classify_cluster(centroid, centroids_original.tolist(), FEATURE_COLUMNS)
        cluster_definitions[str(i)] = {
            "cluster_id": i,
            "cluster_name": name,
            "description": desc,
            "strategy": strategy,
            "centroid": {col: float(centroid[j]) for j, col in enumerate(FEATURE_COLUMNS)},
            "player_count": int(cluster_counts[i]),
            "pct_of_training": round(cluster_counts[i] / len(df_train) * 100, 1)
        }
    
    # Log parameters
    mlflow.log_param("n_clusters", OPTIMAL_K)
    mlflow.log_param("min_hands_for_training", MIN_HANDS_FOR_TRAINING)
    mlflow.log_param("features", FEATURE_COLUMNS)
    mlflow.log_param("n_training_players", len(df_train))
    
    # Log metrics
    mlflow.log_metric("silhouette_score", silhouette)
    mlflow.log_metric("inertia", kmeans.inertia_)
    
    # Log cluster definitions as artifact (JSON)
    with open("/tmp/cluster_definitions.json", "w") as f:
        json.dump(cluster_definitions, f, indent=2)
    mlflow.log_artifact("/tmp/cluster_definitions.json")
    
    # Log model
    input_example = pd.DataFrame(X_train[:5], columns=FEATURE_COLUMNS)
    mlflow.sklearn.log_model(
        sk_model=pipeline,
        artifact_path="model",
        input_example=input_example,
        registered_model_name=MODEL_NAME
    )
    
    run_id = run.info.run_id
    print(f"MLflow Run ID: {run_id}")
    print(f"Silhouette Score: {silhouette:.3f}")
    print(f"Model registered: {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Review Cluster Definitions

# COMMAND ----------

print("Cluster Definitions (saved as model artifact):\n")
for cid, info in cluster_definitions.items():
    print(f"Cluster {cid}: {info['cluster_name']} ({info['description']})")
    print(f"  Players: {info['player_count']} ({info['pct_of_training']}%)")
    print(f"  VPIP: {info['centroid']['vpip_pct']:.1f}%, PFR: {info['centroid']['pfr_pct']:.1f}%")
    print(f"  Strategy: {info['strategy']}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC Model artifacts include:
# MAGIC - **model/** - Pipeline (StandardScaler + KMeans)
# MAGIC - **cluster_definitions.json** - Mapping of cluster_id → name, description, strategy, centroid
# MAGIC 
# MAGIC To use:
# MAGIC ```python
# MAGIC model = mlflow.sklearn.load_model("models:/poker.ml.player_clustering@champion")
# MAGIC cluster_id = model.predict([[45, 30, 8, 2.5, 35, 50, 5]])[0]
# MAGIC 
# MAGIC # Load definitions from artifact
# MAGIC client = mlflow.MlflowClient()
# MAGIC artifact_path = client.download_artifacts(run_id, "cluster_definitions.json")
# MAGIC definitions = json.load(open(artifact_path))
# MAGIC print(definitions[str(cluster_id)]['cluster_name'])
# MAGIC ```
