# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Analysis Result to Silver
# MAGIC Parse screenshot analysis JSONs from bronze and load to silver tables:
# MAGIC - `poker.silver.screenshots` - main table with deduplicated spots
# MAGIC - `poker.silver.screenshot_players` - opponents per screenshot
# MAGIC - `poker.silver.screenshot_history` - hand history per street
# MAGIC 
# MAGIC **Filters:**
# MAGIC - Exclude records where `gpt_advice` is null (no actual game)
# MAGIC 
# MAGIC **Deduplication:**
# MAGIC - Group by (hero_cards, hero_pos, street, board, pot) to find same spot
# MAGIC - Aggregate multiple GPT recommendations into array

# COMMAND ----------

from pyspark.sql.functions import (
    col, current_timestamp, from_json, explode, collect_list, first,
    to_timestamp, regexp_extract, concat_ws, sort_array, row_number,
    struct, array_distinct, size, when, lit, date_format, floor
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, 
    BooleanType, ArrayType, MapType, TimestampType
)
from pyspark.sql.window import Window

# COMMAND ----------

# Configuration
SOURCE_TABLE = "poker.bronze.analysis_result"
TARGET_SCREENSHOTS = "poker.silver.screenshots"
TARGET_PLAYERS = "poker.silver.screenshot_players"
TARGET_HISTORY = "poker.silver.screenshot_history"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define JSON Schema

# COMMAND ----------

# Player schema
player_schema = ArrayType(StructType([
    StructField("pos", StringType(), True),
    StructField("name", StringType(), True),
    StructField("stack", DoubleType(), True),
    StructField("vpip", IntegerType(), True),
    StructField("bet", DoubleType(), True),
    StructField("active", BooleanType(), True),
    StructField("is_allin", BooleanType(), True),
    StructField("seat", IntegerType(), True),
]))

# Hand history schema
history_schema = ArrayType(StructType([
    StructField("street", StringType(), True),
    StructField("board", ArrayType(StringType()), True),
    StructField("actions", ArrayType(StringType()), True),
    StructField("bets", MapType(StringType(), DoubleType()), True),
    StructField("pot", DoubleType(), True),
    StructField("hero_prev_action", StringType(), True),
]))

# GPT advice schema
gpt_schema = StructType([
    StructField("recommendation", StringType(), True),
    StructField("raw", StructType([
        StructField("action", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("raw_response", StringType(), True),
        StructField("prompt_system", StringType(), True),
        StructField("prompt_user", StringType(), True),
    ]), True),
    StructField("time", DoubleType(), True),
])

# Full JSON schema
json_schema = StructType([
    StructField("action_type", StringType(), True),
    StructField("hero", StructType([
        StructField("cards", StringType(), True),
        StructField("pos", StringType(), True),
        StructField("stack", DoubleType(), True),
        StructField("bet", DoubleType(), True),
        StructField("is_allin", BooleanType(), True),
        StructField("seat", IntegerType(), True),
    ]), True),
    StructField("stage", StringType(), True),
    StructField("street", StringType(), True),
    StructField("board", ArrayType(StringType()), True),
    StructField("pot", DoubleType(), True),
    StructField("to_call", DoubleType(), True),
    StructField("players", player_schema, True),
    StructField("actions", ArrayType(StringType()), True),
    StructField("file", StringType(), True),
    StructField("hand_history", history_schema, True),
    StructField("gpt_advice", gpt_schema, True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and Parse Bronze Data

# COMMAND ----------

# Read bronze
df_bronze = spark.read.table(SOURCE_TABLE)

# Parse JSON
df_parsed = df_bronze.withColumn("data", from_json(col("raw_content"), json_schema))

# Filter: only records with gpt_advice (actual game situations)
df_valid = df_parsed.filter(col("data.gpt_advice").isNotNull())

# Extract timestamp from filename: screenshot_YYYYMMDD_HHMMSS_microsec
df_with_ts = df_valid.withColumn(
    "screenshot_at",
    to_timestamp(
        regexp_extract(col("file_name"), r"screenshot_(\d{8}_\d{6})", 1),
        "yyyyMMdd_HHmmss"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplicate Same Spots

# COMMAND ----------

# Create dedup key: same spot = same hero cards, position, street, board, pot
# PLUS 10-minute time window (same spot captured multiple times in quick succession)
# Screenshots from different sessions (hours/days apart) are different spots
df_with_key = df_with_ts.withColumn(
    "time_bucket",
    # Round to 10-minute windows: 21:15 and 21:22 = same bucket, 21:15 and 21:35 = different
    date_format(
        (floor(col("screenshot_at").cast("long") / 600) * 600).cast("timestamp"),
        "yyyyMMdd_HHmm"
    )
).withColumn(
    "spot_key",
    concat_ws("|",
        col("time_bucket"),
        col("data.hero.cards"),
        col("data.hero.pos"),
        col("data.street"),
        concat_ws(",", col("data.board")),
        col("data.pot").cast("string")
    )
)

# Window to order by timestamp within same spot
window_spot = Window.partitionBy("spot_key").orderBy("screenshot_at")

# Get first screenshot per spot and collect all recommendations
df_deduped = df_with_key.withColumn("row_num", row_number().over(window_spot))

# Aggregate recommendations per spot
df_recommendations = df_with_key.groupBy("spot_key").agg(
    collect_list(col("data.gpt_advice.raw.action")).alias("all_recommendations"),
    collect_list(col("data.gpt_advice.recommendation")).alias("all_recommendations_full"),
    collect_list(col("data.gpt_advice.time")).alias("all_gpt_times")
)

# Keep only first record per spot, join with aggregated recommendations
df_first = df_deduped.filter(col("row_num") == 1).drop("row_num")
df_final = df_first.join(df_recommendations, "spot_key", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Screenshots Table

# COMMAND ----------

df_screenshots = df_final.select(
    col("file_name"),
    col("screenshot_at"),
    col("data.action_type").alias("action_type"),
    col("data.stage").alias("stage"),
    col("data.street").alias("street"),
    col("data.board").alias("board"),
    col("data.pot").cast("decimal(10,2)").alias("pot"),
    col("data.to_call").cast("decimal(10,2)").alias("to_call"),
    col("data.actions").alias("actions"),
    
    # Hero flattened
    col("data.hero.cards").alias("hero_cards"),
    col("data.hero.pos").alias("hero_pos"),
    col("data.hero.stack").cast("decimal(10,2)").alias("hero_stack"),
    col("data.hero.bet").cast("decimal(10,2)").alias("hero_bet"),
    col("data.hero.is_allin").alias("hero_is_allin"),
    col("data.hero.seat").alias("hero_seat"),
    
    # GPT advice - first recommendation
    col("data.gpt_advice.recommendation").alias("gpt_recommendation"),
    col("data.gpt_advice.raw.action").alias("gpt_action"),
    col("data.gpt_advice.raw.amount").cast("decimal(10,2)").alias("gpt_amount"),
    col("data.gpt_advice.time").cast("decimal(6,3)").alias("gpt_time"),
    col("data.gpt_advice.raw.prompt_system").alias("gpt_prompt_system"),
    
    # Deduplication tracking
    col("spot_key"),
    col("all_recommendations"),
    col("all_recommendations_full"),
    size(col("all_recommendations")).alias("recommendation_count"),
    
    current_timestamp().alias("ingested_at"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Players Table

# COMMAND ----------

# Explode players array
df_players = df_final.select(
    col("file_name"),
    explode(col("data.players")).alias("player")
).select(
    col("file_name"),
    col("player.seat").alias("seat"),
    col("player.pos").alias("pos"),
    col("player.name").alias("name"),
    col("player.stack").cast("decimal(10,2)").alias("stack"),
    col("player.vpip").alias("vpip"),
    col("player.bet").cast("decimal(10,2)").alias("bet"),
    col("player.active").alias("active"),
    col("player.is_allin").alias("is_allin"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build History Table

# COMMAND ----------

# Explode hand_history array
df_history = df_final.select(
    col("file_name"),
    explode(col("data.hand_history")).alias("hist")
).select(
    col("file_name"),
    col("hist.street").alias("street"),
    col("hist.board").alias("board"),
    col("hist.actions").alias("actions"),
    col("hist.bets").alias("bets"),
    col("hist.pot").cast("decimal(10,2)").alias("pot"),
    col("hist.hero_prev_action").alias("hero_prev_action"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver

# COMMAND ----------

# Write screenshots (need to update table schema to include new columns)
df_screenshots.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_SCREENSHOTS)

print(f"✅ Wrote {df_screenshots.count()} records to {TARGET_SCREENSHOTS}")

# COMMAND ----------

# Write players
df_players.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_PLAYERS)

print(f"✅ Wrote {df_players.count()} records to {TARGET_PLAYERS}")

# COMMAND ----------

# Write history  
df_history.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_HISTORY)

print(f"✅ Wrote {df_history.count()} records to {TARGET_HISTORY}")
