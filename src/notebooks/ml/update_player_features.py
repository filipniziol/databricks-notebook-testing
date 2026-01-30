# Databricks notebook source
# MAGIC %md
# MAGIC # Update Player Features
# MAGIC 
# MAGIC Updates the Feature Table with latest player statistics.
# MAGIC 
# MAGIC **Schedule:** Daily (after silver layer refresh)
# MAGIC **Output:** `poker.ml.player_features` (Feature Table)

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

# COMMAND ----------

FEATURE_TABLE_NAME = "poker.ml.player_features"

fe = FeatureEngineeringClient()

# COMMAND ----------

# Load player statistics and cast DECIMAL to DOUBLE for MLflow compatibility
df_features = spark.sql("""
    SELECT
        player_name,
        total_hands,
        CAST(vpip_pct AS DOUBLE) as vpip_pct,
        CAST(pfr_pct AS DOUBLE) as pfr_pct,
        CAST(three_bet_pct AS DOUBLE) as three_bet_pct,
        CAST(aggression_factor AS DOUBLE) as aggression_factor,
        CAST(wtsd_pct AS DOUBLE) as wtsd_pct,
        CAST(wsd_pct AS DOUBLE) as wsd_pct,
        CAST(allin_pct AS DOUBLE) as allin_pct,
        CAST(avg_preflop_raise_bb AS DOUBLE) as avg_preflop_raise_bb,
        current_timestamp() as feature_timestamp
    FROM poker.gold.player_statistics
""")

print(f"Total players: {df_features.count()}")

# COMMAND ----------

# Check if Feature Table exists
table_exists = spark.catalog.tableExists(FEATURE_TABLE_NAME)

if not table_exists:
    # Create new Feature Table
    fe.create_table(
        name=FEATURE_TABLE_NAME,
        primary_keys=["player_name"],
        df=df_features,
        description="Player statistics features for clustering model"
    )
    print(f"Created new feature table: {FEATURE_TABLE_NAME}")
else:
    # Update existing table (merge/upsert by primary key)
    fe.write_table(
        name=FEATURE_TABLE_NAME,
        df=df_features,
        mode="merge"
    )
    print(f"Updated feature table: {FEATURE_TABLE_NAME}")

# COMMAND ----------

# Verify
print(f"\nFeature Table row count: {spark.table(FEATURE_TABLE_NAME).count()}")
