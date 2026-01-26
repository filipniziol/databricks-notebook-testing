# Databricks notebook source
# MAGIC %md
# MAGIC # Load Analysis Result to Bronze
# MAGIC Load JSON files from screenshot analysis using Auto Loader.
# MAGIC 
# MAGIC **Source:** `/Volumes/poker/bronze/bronze/analysis_result/`  
# MAGIC **Target:** `poker.bronze.analysis_result`

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, regexp_extract

# COMMAND ----------

# Configuration
SOURCE_PATH = "/Volumes/poker/bronze/bronze/analysis_result/"
TARGET_TABLE = "poker.bronze.analysis_result"
CHECKPOINT_PATH = "/Volumes/poker/bronze/bronze/_checkpoints/analysis_result"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview source files (for debugging)

# COMMAND ----------

# Check what files exist in source
dbutils.fs.ls(SOURCE_PATH)

# COMMAND ----------

# Test read a sample file to verify format
df_sample = (
    spark.read
    .format("text")
    .option("wholeText", "true")
    .load(SOURCE_PATH)
    .limit(5)
)

df_sample.select(
    col("value").alias("raw_content"),
    col("_metadata.file_path").alias("file_path")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define streaming ingestion

# COMMAND ----------

def load_analysis_result():
    """
    Stream JSON files from analysis_result volume to bronze table.
    Uses Auto Loader with checkpoint for exactly-once processing.
    """
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("wholeText", "true")
        .load(SOURCE_PATH)
        .select(
            col("value").alias("raw_content"),
            col("_metadata.file_path").alias("file_path"),
            current_timestamp().alias("ingested_at")
        )
        .withColumn(
            "file_name",
            regexp_extract(col("file_path"), r"[^/]+$", 0)
        )
        .select("raw_content", "file_name", "file_path", "ingested_at")
    )
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the stream

# COMMAND ----------

# Start streaming write
df_stream = load_analysis_result()

query = (
    df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)  # Process all available files and stop
    .toTable(TARGET_TABLE)
)

# Wait for completion
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify results

# COMMAND ----------

# Check row count
count = spark.table(TARGET_TABLE).count()
print(f"Total rows in {TARGET_TABLE}: {count}")

# COMMAND ----------

# Preview data
spark.table(TARGET_TABLE).orderBy(col("ingested_at").desc()).limit(10).display()
