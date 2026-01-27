# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Tournament History to Silver
# MAGIC Parse tournament summary files from bronze and load to silver.
# MAGIC 
# MAGIC **Source:** `poker.bronze.tournament_history`  
# MAGIC **Target:** `poker.silver.tournaments`

# COMMAND ----------

import sys
import os

# Add src folder to path for package imports
# Works both in Repos and Workspace Files
notebook_path = os.getcwd()
src_path = os.path.dirname(os.path.dirname(notebook_path))  # Go up from notebooks/silver to src
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, LongType
from package.gg_parser import GGTournamentSummaryParser

# COMMAND ----------

# Configuration
SOURCE_TABLE = "poker.bronze.tournament_history"
TARGET_TABLE = "poker.silver.tournaments"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define parsing UDF

# COMMAND ----------

def parse_tournament_summary(raw_content: str) -> dict:
    """Parse raw tournament text into structured data"""
    try:
        parser = GGTournamentSummaryParser(text=raw_content)
        result = parser.parse()
        if result:
            return {
                "tournament_id": int(result.tournament_id),
                "tournament_name": result.tournament_name,
                "game_type": result.game_type,
                "buyin_prize": float(result.buy_in_prize),
                "buyin_rake": float(result.buy_in_fee),
                "buyin_bounty": float(result.buy_in_bounty),
                "buyin_total": float(result.buy_in_total),
                "total_players": result.total_players,
                "prize_pool": float(result.prize_pool),
                "started_at": result.start_time.isoformat(),
                "hero_position": result.hero_finish_position,
                "hero_prize": float(result.hero_prize),
            }
    except Exception as e:
        print(f"Parse error: {e}")
    return None

# Define schema for UDF return type
tournament_schema = StructType([
    StructField("tournament_id", LongType(), True),
    StructField("tournament_name", StringType(), True),
    StructField("game_type", StringType(), True),
    StructField("buyin_prize", DoubleType(), True),
    StructField("buyin_rake", DoubleType(), True),
    StructField("buyin_bounty", DoubleType(), True),
    StructField("buyin_total", DoubleType(), True),
    StructField("total_players", IntegerType(), True),
    StructField("prize_pool", DoubleType(), True),
    StructField("started_at", StringType(), True),
    StructField("hero_position", IntegerType(), True),
    StructField("hero_prize", DoubleType(), True),
])

parse_tournament_udf = udf(parse_tournament_summary, tournament_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from bronze and transform

# COMMAND ----------

# Read all bronze data (full refresh approach - small table)
df_bronze = spark.read.table(SOURCE_TABLE)

# COMMAND ----------

# Parse and extract fields
df_parsed = df_bronze.withColumn("parsed", parse_tournament_udf(col("raw_content")))

# Filter out failed parses
df_valid = df_parsed.filter(col("parsed").isNotNull())

# COMMAND ----------

# Flatten struct to columns
df_silver = df_valid.select(
    col("parsed.tournament_id").alias("tournament_id"),
    col("parsed.tournament_name").alias("tournament_name"),
    col("parsed.game_type").alias("game_type"),
    col("parsed.buyin_prize").cast("decimal(10,2)").alias("buyin_prize"),
    col("parsed.buyin_rake").cast("decimal(10,2)").alias("buyin_rake"),
    col("parsed.buyin_bounty").cast("decimal(10,2)").alias("buyin_bounty"),
    col("parsed.buyin_total").cast("decimal(10,2)").alias("buyin_total"),
    col("parsed.total_players").alias("total_players"),
    col("parsed.prize_pool").cast("decimal(10,2)").alias("prize_pool"),
    col("parsed.started_at").cast("timestamp").alias("started_at"),
    col("parsed.hero_position").alias("hero_position"),
    col("parsed.hero_prize").cast("decimal(10,2)").alias("hero_prize"),
    col("file_name"),
    current_timestamp().alias("ingested_at"),
)

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to silver (full overwrite)

# COMMAND ----------

# Full overwrite - small table, always recalculate
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

print(f"✅ Refreshed {TARGET_TABLE}")
