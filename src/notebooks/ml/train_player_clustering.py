# Databricks notebook source
# MAGIC %md
# MAGIC # Player Clustering Model with Feature Store
# MAGIC 
# MAGIC Clusters players based on their playing style using K-Means.
# MAGIC 
# MAGIC **Features:**
# MAGIC - Uses Databricks Feature Store for feature management
# MAGIC - Model lineage tracked via Unity Catalog
# MAGIC - MLflow experiment tracking
# MAGIC 
# MAGIC **Input:** `poker.ml.player_features` (Feature Table)
# MAGIC **Output:** `poker.gold.player_clusters` + registered model

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt
from datetime import datetime

# Configure MLflow for Databricks + Unity Catalog
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

# Set experiment
EXPERIMENT_NAME = "/Shared/player_clustering"
mlflow.set_experiment(EXPERIMENT_NAME)
print(f"MLflow Experiment: {EXPERIMENT_NAME}")

# Model and Feature Store config
MODEL_NAME = "poker.ml.player_clustering"
FEATURE_TABLE_NAME = "poker.ml.player_features"

# Initialize Feature Engineering Client
fe = FeatureEngineeringClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create/Update Feature Table
# MAGIC 
# MAGIC Feature Table stores player statistics for ML training.

# COMMAND ----------

# Load player statistics as features
df_features = spark.sql("""
    SELECT 
        player_name,
        total_hands,
        vpip_pct,
        pfr_pct,
        three_bet_pct,
        aggression_factor,
        wtsd_pct,
        wsd_pct,
        allin_pct,
        avg_preflop_raise_bb,
        current_timestamp() as feature_timestamp
    FROM poker.gold.player_statistics
""")

print(f"Total players: {df_features.count()}")

# COMMAND ----------

# Create or update Feature Table
# Primary key is player_name - unique identifier

try:
    # Try to create new feature table
    fe.create_table(
        name=FEATURE_TABLE_NAME,
        primary_keys=["player_name"],
        timestamp_keys=["feature_timestamp"],
        df=df_features,
        description="Player statistics features for clustering model"
    )
    print(f"Created new feature table: {FEATURE_TABLE_NAME}")
except Exception as e:
    if "already exists" in str(e):
        # Update existing table
        fe.write_table(
            name=FEATURE_TABLE_NAME,
            df=df_features,
            mode="overwrite"
        )
        print(f"Updated existing feature table: {FEATURE_TABLE_NAME}")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Features from Feature Store

# COMMAND ----------

# Define which features to use for clustering
FEATURE_COLUMNS = [
    'vpip_pct',
    'pfr_pct', 
    'three_bet_pct',
    'aggression_factor',
    'wtsd_pct',
    'wsd_pct',
    'allin_pct'
]

# Read features from Feature Store
df_all = fe.read_table(name=FEATURE_TABLE_NAME).toPandas()

print(f"Loaded {len(df_all)} players from Feature Store")
print(f"Players with ≥10 hands: {len(df_all[df_all['total_hands'] >= 10])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prepare Training Data

# COMMAND ----------

# Training set: players with ≥10 hands (reliable stats)
MIN_HANDS_FOR_TRAINING = 10
df_train = df_all[df_all['total_hands'] >= MIN_HANDS_FOR_TRAINING].copy()

print(f"Training on {len(df_train)} players with ≥{MIN_HANDS_FOR_TRAINING} hands")

# Handle NaN values - fill with median
for col in FEATURE_COLUMNS:
    median_val = df_train[col].median()
    df_train[col] = df_train[col].fillna(median_val)
    df_all[col] = df_all[col].fillna(median_val)

# Prepare feature matrices
X_train = df_train[FEATURE_COLUMNS].values
X_all = df_all[FEATURE_COLUMNS].values

print(f"Feature matrix shape (train): {X_train.shape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Normalize Features

# COMMAND ----------

# StandardScaler - important for K-Means (distance-based)
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_all_scaled = scaler.transform(X_all)

print("Features normalized with StandardScaler")
print(f"Mean after scaling: {X_train_scaled.mean(axis=0).round(2)}")
print(f"Std after scaling: {X_train_scaled.std(axis=0).round(2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Find Optimal K (Number of Clusters)

# COMMAND ----------

# Test different K values
K_range = range(2, 9)
inertias = []
silhouettes = []

for k in K_range:
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    kmeans.fit(X_train_scaled)
    inertias.append(kmeans.inertia_)
    silhouettes.append(silhouette_score(X_train_scaled, kmeans.labels_))
    print(f"K={k}: Inertia={kmeans.inertia_:.1f}, Silhouette={silhouettes[-1]:.3f}")

# COMMAND ----------

# Plot Elbow and Silhouette
fig, axes = plt.subplots(1, 2, figsize=(12, 4))

axes[0].plot(K_range, inertias, 'bo-')
axes[0].set_xlabel('Number of clusters (K)')
axes[0].set_ylabel('Inertia')
axes[0].set_title('Elbow Method')

axes[1].plot(K_range, silhouettes, 'ro-')
axes[1].set_xlabel('Number of clusters (K)')
axes[1].set_ylabel('Silhouette Score')
axes[1].set_title('Silhouette Score (higher = better)')

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Train Final Model with Feature Store Integration

# COMMAND ----------

# Choose optimal K (adjust based on plots above)
OPTIMAL_K = 5

print(f"Training K-Means with K={OPTIMAL_K}")

# Create training set with player_name for Feature Store lookup
df_train_with_key = df_train[['player_name']].copy()
training_set = fe.create_training_set(
    df=spark.createDataFrame(df_train_with_key),
    feature_lookups=[
        FeatureLookup(
            table_name=FEATURE_TABLE_NAME,
            feature_names=FEATURE_COLUMNS,
            lookup_key="player_name"
        )
    ],
    label=None,  # Unsupervised learning - no label
    exclude_columns=["feature_timestamp"]
)

# Load training data from Feature Store
training_df = training_set.load_df().toPandas()

# Prepare features
X_train_fs = training_df[FEATURE_COLUMNS].values
for i, col in enumerate(FEATURE_COLUMNS):
    median_val = np.nanmedian(X_train_fs[:, i])
    X_train_fs[np.isnan(X_train_fs[:, i]), i] = median_val

X_train_fs_scaled = scaler.fit_transform(X_train_fs)

# Start MLflow run
with mlflow.start_run(run_name=f"kmeans_k{OPTIMAL_K}_with_features") as run:
    
    # Train model
    kmeans = KMeans(n_clusters=OPTIMAL_K, random_state=42, n_init=10)
    kmeans.fit(X_train_fs_scaled)
    
    # Metrics
    train_silhouette = silhouette_score(X_train_fs_scaled, kmeans.labels_)
    
    # Log parameters
    mlflow.log_param("n_clusters", OPTIMAL_K)
    mlflow.log_param("min_hands_for_training", MIN_HANDS_FOR_TRAINING)
    mlflow.log_param("features", FEATURE_COLUMNS)
    mlflow.log_param("feature_table", FEATURE_TABLE_NAME)
    mlflow.log_param("n_training_players", len(df_train))
    
    # Log metrics
    mlflow.log_metric("silhouette_score", train_silhouette)
    mlflow.log_metric("inertia", kmeans.inertia_)
    
    # Log model with Feature Store - this creates lineage!
    fe.log_model(
        model=kmeans,
        artifact_path="model",
        flavor=mlflow.sklearn,
        training_set=training_set,
        registered_model_name=MODEL_NAME
    )
    
    # Also save scaler as separate artifact
    mlflow.sklearn.log_model(scaler, "scaler")
    
    run_id = run.info.run_id
    print(f"MLflow Run ID: {run_id}")
    print(f"Silhouette Score: {train_silhouette:.3f}")
    print(f"Model registered with Feature Store lineage: {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Analyze Clusters (Centroids)

# COMMAND ----------

# Get centroids in original scale
centroids_scaled = kmeans.cluster_centers_
centroids_original = scaler.inverse_transform(centroids_scaled)

# Create DataFrame with centroid values
df_centroids = pd.DataFrame(centroids_original, columns=FEATURE_COLUMNS)
df_centroids['cluster_id'] = range(OPTIMAL_K)

# Count players per cluster (training set)
cluster_counts = pd.Series(kmeans.labels_).value_counts().sort_index()
df_centroids['player_count'] = cluster_counts.values
df_centroids['pct_of_total'] = (df_centroids['player_count'] / len(df_train) * 100).round(1)

print("Cluster Centroids:")
display(df_centroids)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Name Clusters Based on Data

# COMMAND ----------

def classify_cluster(row, all_centroids):
    """
    Classify cluster based on its RELATIVE position among all clusters.
    Uses percentile ranking within the cluster set.
    """
    vpip = row['vpip_pct']
    pfr = row['pfr_pct']
    af = row['aggression_factor'] if pd.notna(row['aggression_factor']) else 1.0
    allin = row['allin_pct']
    
    # Calculate percentile ranks within clusters
    vpip_rank = (all_centroids['vpip_pct'] < vpip).sum() / len(all_centroids)
    pfr_rank = (all_centroids['pfr_pct'] < pfr).sum() / len(all_centroids)
    af_rank = (all_centroids['aggression_factor'] < af).sum() / len(all_centroids)
    allin_rank = (all_centroids['allin_pct'] < allin).sum() / len(all_centroids)
    
    # Derive characteristics from data
    is_tight = vpip_rank < 0.3
    is_loose = vpip_rank > 0.7
    is_aggressive = af_rank > 0.6 or pfr_rank > 0.6
    is_passive = af_rank < 0.4 and pfr_rank < 0.4
    is_allin_heavy = allin_rank > 0.7
    
    # Name based on discovered characteristics
    if is_tight and is_aggressive:
        return ("TAG", f"Tight-Aggressive (VPIP={vpip:.0f}%, PFR={pfr:.0f}%, AF={af:.1f})", "Respect their raises.")
    elif is_tight and is_passive:
        return ("Nit", f"Ultra-tight passive (VPIP={vpip:.0f}%, PFR={pfr:.0f}%)", "Steal blinds.")
    elif is_loose and is_aggressive:
        return ("LAG", f"Loose-Aggressive (VPIP={vpip:.0f}%, PFR={pfr:.0f}%, AF={af:.1f})", "Call down lighter.")
    elif is_loose and is_passive:
        return ("Fish", f"Loose-Passive (VPIP={vpip:.0f}%, AF={af:.1f})", "Value bet relentlessly.")
    elif is_allin_heavy:
        return ("All-in Player", f"High all-in frequency (Allin={allin:.0f}%)", "Shove/fold adjust.")
    elif is_aggressive:
        return ("Aggressive Reg", f"Balanced aggressive (VPIP={vpip:.0f}%, AF={af:.1f})", "Play solid.")
    else:
        return ("Average Player", f"Standard stats (VPIP={vpip:.0f}%, PFR={pfr:.0f}%)", "Standard play.")

# Apply classification
cluster_info = []
for _, row in df_centroids.iterrows():
    name, desc, strategy = classify_cluster(row, df_centroids)
    cluster_info.append({
        'cluster_id': int(row['cluster_id']),
        'cluster_name': name,
        'cluster_description': desc,
        'strategy_vs': strategy,
        'centroid_vpip': row['vpip_pct'],
        'centroid_pfr': row['pfr_pct'],
        'centroid_three_bet': row['three_bet_pct'],
        'centroid_af': row['aggression_factor'],
        'centroid_wtsd': row['wtsd_pct'],
        'centroid_wsd': row['wsd_pct'],
        'centroid_allin': row['allin_pct'],
        'player_count': int(row['player_count']),
        'pct_of_total': row['pct_of_total']
    })

df_cluster_defs = pd.DataFrame(cluster_info)
print("Cluster Definitions:")
display(df_cluster_defs[['cluster_id', 'cluster_name', 'player_count', 'pct_of_total', 'centroid_vpip', 'centroid_pfr']])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Predict Clusters for ALL Players

# COMMAND ----------

# Predict for all players (including those with <10 hands)
all_clusters = kmeans.predict(X_all_scaled)
all_distances = kmeans.transform(X_all_scaled)

# Get distance to assigned centroid
assigned_distances = [all_distances[i, c] for i, c in enumerate(all_clusters)]

# Build result DataFrame
df_result = df_all[['player_name', 'total_hands', 'vpip_pct', 'pfr_pct', 'aggression_factor']].copy()
df_result['cluster_id'] = all_clusters
df_result['distance_to_centroid'] = assigned_distances

# Add cluster names
cluster_name_map = df_cluster_defs.set_index('cluster_id')['cluster_name'].to_dict()
cluster_desc_map = df_cluster_defs.set_index('cluster_id')['cluster_description'].to_dict()

df_result['cluster_name'] = df_result['cluster_id'].map(cluster_name_map)
df_result['cluster_description'] = df_result['cluster_id'].map(cluster_desc_map)

# Confidence based on hands
df_result['confidence'] = df_result['total_hands'].apply(
    lambda x: 'high' if x >= 30 else ('medium' if x >= 10 else 'low')
)

# Add metadata
model_version = f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}"
df_result['model_version'] = model_version
df_result['clustered_at'] = datetime.now()

print(f"Clustered {len(df_result)} players")
print(f"\nCluster distribution:")
print(df_result['cluster_name'].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Save Results

# COMMAND ----------

# Fix dtypes before converting to Spark
df_result['cluster_id'] = df_result['cluster_id'].astype(int)
df_result['distance_to_centroid'] = df_result['distance_to_centroid'].astype(float)
df_result['total_hands'] = df_result['total_hands'].astype(int)
df_result['vpip_pct'] = df_result['vpip_pct'].astype(float)
df_result['pfr_pct'] = df_result['pfr_pct'].astype(float)
df_result['aggression_factor'] = df_result['aggression_factor'].astype(float)

# Save player clusters
spark_df_clusters = spark.createDataFrame(df_result)
spark_df_clusters.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("poker.gold.player_clusters")
print(f"Saved {len(df_result)} players to poker.gold.player_clusters")

# Save cluster definitions
df_cluster_defs['model_version'] = model_version
df_cluster_defs['created_at'] = datetime.now()
for col in ['centroid_vpip', 'centroid_pfr', 'centroid_three_bet', 'centroid_af', 'centroid_wtsd', 'centroid_wsd', 'centroid_allin', 'pct_of_total']:
    df_cluster_defs[col] = df_cluster_defs[col].astype(float)
df_cluster_defs['cluster_id'] = df_cluster_defs['cluster_id'].astype(int)
df_cluster_defs['player_count'] = df_cluster_defs['player_count'].astype(int)

spark_df_defs = spark.createDataFrame(df_cluster_defs)
spark_df_defs.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("poker.ml.cluster_definitions")
print(f"Saved {len(df_cluster_defs)} cluster definitions to poker.ml.cluster_definitions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC **Created:**
# MAGIC - Feature Table: `poker.ml.player_features`
# MAGIC - Model: `poker.ml.player_clustering` (with Feature Store lineage)
# MAGIC - Results: `poker.gold.player_clusters`
# MAGIC - Definitions: `poker.ml.cluster_definitions`
# MAGIC 
# MAGIC **Check in UI:**
# MAGIC - **Catalog → poker → ml → Tables** → `player_features` (Feature Table)
# MAGIC - **Catalog → poker → ml → Models** → `player_clustering` (Lineage tab shows feature dependency)
# MAGIC - **Machine Learning → Experiments** → `/Shared/poker_analyzer/player_clustering`
