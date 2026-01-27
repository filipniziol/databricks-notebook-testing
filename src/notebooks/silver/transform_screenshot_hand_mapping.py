# Databricks notebook source
# MAGIC %md
# MAGIC # Map Screenshots to Hands
# MAGIC Creates mapping between screenshots and hand history records.
# MAGIC 
# MAGIC **Matching criteria:**
# MAGIC 1. Hero cards must match (normalized: "As5h" = "5hAs")
# MAGIC 2. Hero position must match (normalized: BTN=P1, SB=P2, etc.)
# MAGIC 3. Time window: screenshot_at within ±5 minutes of hand_timestamp
# MAGIC 
# MAGIC **Confidence levels:**
# MAGIC - HIGH: cards + position + time all match
# MAGIC - MEDIUM: cards + time match (position mismatch or null)
# MAGIC - LOW: only cards match within time window

# COMMAND ----------

from pyspark.sql.functions import (
    col, current_timestamp, abs as spark_abs, when, lit,
    regexp_replace, concat, sort_array, split, array_join,
    unix_timestamp, row_number, lower
)
from pyspark.sql.window import Window

# COMMAND ----------

# Configuration
SOURCE_SCREENSHOTS = "poker.silver.screenshots"
SOURCE_HANDS = "poker.silver.hands"
TARGET_TABLE = "poker.silver.screenshot_hand_mapping"

TIME_WINDOW_SECONDS = 5 * 60  # 5 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Position Normalization
# MAGIC 
# MAGIC Based on actual data:
# MAGIC - Screenshots: BTN, BB, SB, MP, UTG, P1, CO, HJ, UTG+1, MP+1
# MAGIC - Hands: BTN, SB, BB, UTG, MP, CO, UTG+1, HJ, null
# MAGIC 
# MAGIC P1/P2/etc in screenshots are seat numbers from final stage (9-max), not positions.
# MAGIC We normalize standard position names but leave P1-P9 as-is (won't match).

# COMMAND ----------

def normalize_position_expr(pos_col):
    """
    Normalize position names for matching.
    Standard positions: BTN, SB, BB, UTG, UTG+1, MP, MP+1, HJ, CO
    
    P1-P9 from final stage screenshots won't match - that's OK,
    we'll still match on cards+time with medium confidence.
    """
    return (
        when(col(pos_col).isin("BTN", "BU", "D"), "BTN")
        .when(col(pos_col) == "SB", "SB")
        .when(col(pos_col) == "BB", "BB")
        .when(col(pos_col).isin("UTG", "EP"), "UTG")
        .when(col(pos_col) == "UTG+1", "UTG+1")
        .when(col(pos_col) == "MP", "MP")
        .when(col(pos_col) == "MP+1", "MP+1")
        .when(col(pos_col) == "HJ", "HJ")
        .when(col(pos_col) == "CO", "CO")
        .otherwise(col(pos_col))  # Keep P1-P9 and null as-is
    )

# COMMAND ----------

def normalize_cards_expr(cards_col):
    """
    Normalize hero cards to consistent format.
    Cards like 'As5h' and '5hAs' should match.
    Sort alphabetically: '5hAs' -> 'As5h' (A before 5 in card ranking doesn't matter, 
    we just need consistency, so use string sort)
    
    Actually for 2-card hands, we can split into individual cards and sort.
    'As5h' -> ['As', '5h'] -> sort -> ['5h', 'As'] -> '5hAs'
    
    Better: extract both cards, sort them, rejoin.
    Card format: Rank(1 char or 2 for 10) + Suit(1 char)
    For simplicity: first 2 chars = card1, last 2 chars = card2
    """
    # Split into 2-char cards (assuming format like "As5h", "KdQs", "Th9c")
    # Take first 2 and last 2, sort, concat
    return concat(
        when(
            col(cards_col).substr(1, 2) <= col(cards_col).substr(3, 2),
            col(cards_col)
        ).otherwise(
            concat(col(cards_col).substr(3, 2), col(cards_col).substr(1, 2))
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Source Tables

# COMMAND ----------

df_screenshots = spark.read.table(SOURCE_SCREENSHOTS).select(
    col("file_name"),
    col("screenshot_at"),
    col("hero_cards").alias("screenshot_hero_cards"),
    col("hero_pos").alias("screenshot_hero_pos"),
    col("street").alias("screenshot_street"),
    col("board").alias("screenshot_board")
)

df_hands = spark.read.table(SOURCE_HANDS).select(
    col("hand_id"),
    col("hand_timestamp"),
    col("hero_cards").alias("hand_hero_cards"),
    col("hero_position").alias("hand_hero_pos"),
    col("board").alias("hand_board")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normalize and Match

# COMMAND ----------

# Add normalized columns
df_screenshots = df_screenshots.withColumn(
    "screenshot_cards_norm", 
    normalize_cards_expr("screenshot_hero_cards")
).withColumn(
    "screenshot_pos_norm",
    normalize_position_expr("screenshot_hero_pos")
).withColumn(
    "screenshot_ts",
    unix_timestamp("screenshot_at")
)

df_hands = df_hands.withColumn(
    "hand_cards_norm",
    normalize_cards_expr("hand_hero_cards")
).withColumn(
    "hand_pos_norm",
    normalize_position_expr("hand_hero_pos")
).withColumn(
    "hand_ts",
    unix_timestamp("hand_timestamp")
)

# COMMAND ----------

# Join on normalized cards within time window
df_joined = df_screenshots.join(
    df_hands,
    (df_screenshots.screenshot_cards_norm == df_hands.hand_cards_norm) &
    (spark_abs(df_screenshots.screenshot_ts - df_hands.hand_ts) <= TIME_WINDOW_SECONDS),
    "left"
)

# Calculate time difference and confidence
df_matched = df_joined.withColumn(
    "time_diff_seconds",
    (col("screenshot_ts") - col("hand_ts")).cast("int")
).withColumn(
    "position_match",
    col("screenshot_pos_norm") == col("hand_pos_norm")
).withColumn(
    "match_confidence",
    when(
        col("hand_id").isNotNull() & col("position_match"),
        "high"
    ).when(
        col("hand_id").isNotNull() & ~col("position_match"),
        "medium"
    ).otherwise("unmatched")
).withColumn(
    "match_method",
    when(col("position_match"), "cards+position+time")
    .when(col("hand_id").isNotNull(), "cards+time")
    .otherwise(None)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle Multiple Matches
# MAGIC If a screenshot matches multiple hands, pick the closest in time.

# COMMAND ----------

# Window to pick best match per screenshot
window_best = Window.partitionBy("file_name").orderBy(
    # Prefer high confidence
    when(col("match_confidence") == "high", 0)
    .when(col("match_confidence") == "medium", 1)
    .otherwise(2),
    # Then closest in time
    spark_abs(col("time_diff_seconds"))
)

df_best_match = df_matched.withColumn(
    "rank", 
    row_number().over(window_best)
).filter(
    col("rank") == 1
).drop("rank")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Final Output

# COMMAND ----------

df_output = df_best_match.select(
    col("file_name"),
    col("hand_id"),
    col("match_confidence"),
    col("match_method"),
    col("time_diff_seconds"),
    col("screenshot_hero_cards"),
    col("hand_hero_cards"),
    col("screenshot_pos_norm").alias("screenshot_hero_pos"),
    col("hand_pos_norm").alias("hand_hero_pos"),
    current_timestamp().alias("matched_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver

# COMMAND ----------

df_output.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

# Stats
total = df_output.count()
matched = df_output.filter(col("hand_id").isNotNull()).count()
high_conf = df_output.filter(col("match_confidence") == "high").count()
medium_conf = df_output.filter(col("match_confidence") == "medium").count()

print(f"✅ Wrote {total} records to {TARGET_TABLE}")
print(f"   - Matched: {matched} ({100*matched//total if total else 0}%)")
print(f"   - High confidence: {high_conf}")
print(f"   - Medium confidence: {medium_conf}")
print(f"   - Unmatched: {total - matched}")
