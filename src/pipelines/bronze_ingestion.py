"""
Bronze Ingestion Pipeline

Load raw files from volumes into bronze tables using Auto Loader.

Data Sources (Volumes):
- /Volumes/poker/bronze/bronze/analysis_result/ - JSON files from screenshot analysis
- /Volumes/poker/bronze/bronze/gg_hand_history/ - TXT files from GGPoker hand history
- /Volumes/poker/bronze/bronze/gg_tournament_history/ - TXT files from GGPoker tournament results
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, regexp_extract


# =============================================================================
# Analysis Result (JSON)
# Screenshots analyzed by GPT - raw JSON content
# =============================================================================

@dp.table(
    name="analysis_result",
    comment="Raw JSON analysis results from screenshot processing. Loaded with Auto Loader from /Volumes/poker/bronze/bronze/analysis_result/"
)
def analysis_result():
    """
    Ingest JSON files from screenshot analysis.
    Each file contains GPT analysis of a poker screenshot.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("wholeText", "true")  # Read entire file as single row
        .load("/Volumes/poker/bronze/bronze/analysis_result/")
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


# =============================================================================
# Hand History (TXT)
# GGPoker hand history files - raw text content
# =============================================================================

@dp.table(
    name="hand_history",
    comment="Raw hand history files from GGPoker. Loaded with Auto Loader from /Volumes/poker/bronze/bronze/gg_hand_history/"
)
def hand_history():
    """
    Ingest text files from GGPoker hand history exports.
    Each file contains multiple hands from tournament sessions.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("wholeText", "true")  # Read entire file as single row
        .load("/Volumes/poker/bronze/bronze/gg_hand_history/")
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


# =============================================================================
# Tournament History (TXT)
# GGPoker tournament results - raw text content
# =============================================================================

@dp.table(
    name="tournament_history",
    comment="Raw tournament history files from GGPoker. Loaded with Auto Loader from /Volumes/poker/bronze/bronze/gg_tournament_history/"
)
def tournament_history():
    """
    Ingest text files from GGPoker tournament history exports.
    Each file contains summary of a single tournament.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("wholeText", "true")  # Read entire file as single row
        .load("/Volumes/poker/bronze/bronze/gg_tournament_history/")
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
