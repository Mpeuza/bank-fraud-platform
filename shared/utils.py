$content = @"
# =============================================================
# utils.py — Reusable helper functions for bank fraud platform
# Every notebook imports from here to avoid repeating code
# =============================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType
from datetime import datetime


# =============================================================
# CSV READING
# Safely reads a CSV file into a Spark DataFrame
# =============================================================

def read_csv(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Read a CSV file into a Spark DataFrame.

    Args:
        spark     : Active SparkSession
        file_path : Path to the CSV file

    Returns:
        DataFrame with all columns as STRING (raw bronze load)
    """
    print(f"Reading CSV: {file_path}")
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .load(file_path)
    print(f"Rows loaded: {df.count()}")
    return df


# =============================================================
# ADD INGESTION METADATA
# Adds _ingested_at and _source_file columns to every bronze load
# =============================================================

def add_ingestion_metadata(df: DataFrame, source_file: str) -> DataFrame:
    """
    Adds standard metadata columns to a bronze DataFrame.

    Args:
        df          : Input DataFrame
        source_file : Name of the source file

    Returns:
        DataFrame with _ingested_at and _source_file columns added
    """
    return df \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .withColumn("_source_file", F.lit(source_file))


# =============================================================
# WRITE TO DELTA TABLE
# Saves a DataFrame to a Delta table (overwrite or append)
# =============================================================

def write_delta(df: DataFrame, table_path: str, mode: str = "overwrite"):
    """
    Write a DataFrame to a Delta table.

    Args:
        df         : DataFrame to write
        table_path : Full table name e.g. fraud_catalog.bronze_card.bronze_card_txn
        mode       : overwrite (default) or append
    """
    print(f"Writing to Delta table: {table_path} (mode={mode})")
    df.write \
        .format("delta") \
        .mode(mode) \
        .saveAsTable(table_path)
    print(f"Write complete: {table_path}")


# =============================================================
# TYPE CASTING HELPERS
# Used in silver layer to clean bronze STRING columns
# =============================================================

def cast_to_double(df: DataFrame, col_name: str) -> DataFrame:
    """Cast a STRING column to DOUBLE. Nulls on failure."""
    return df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

def cast_to_int(df: DataFrame, col_name: str) -> DataFrame:
    """Cast a STRING column to INT. Nulls on failure."""
    return df.withColumn(col_name, F.col(col_name).cast(IntegerType()))

def cast_to_timestamp(df: DataFrame, col_name: str, fmt: str = "yyyy-MM-dd HH:mm:ss") -> DataFrame:
    """Cast a STRING column to TIMESTAMP using a given format."""
    return df.withColumn(col_name, F.to_timestamp(F.col(col_name), fmt))

def cast_to_date(df: DataFrame, col_name: str, fmt: str = "yyyy-MM-dd") -> DataFrame:
    """Cast a STRING column to DATE using a given format."""
    return df.withColumn(col_name, F.to_date(F.col(col_name), fmt))


# =============================================================
# NULL CHECKS
# Used in silver layer to detect data quality issues
# =============================================================

def count_nulls(df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame showing null count per column.
    Useful for data quality checks in silver notebooks.
    """
    null_counts = [(c, df.filter(F.col(c).isNull()).count()) for c in df.columns]
    return null_counts

def drop_nulls_in(df: DataFrame, col_names: list) -> DataFrame:
    """Drop rows where any of the given columns are null."""
    return df.dropna(subset=col_names)


# =============================================================
# RISK TIER HELPER
# Converts a numeric fraud score to a risk tier label
# Used in gold layer notebooks
# =============================================================

def assign_risk_tier(df: DataFrame, score_col: str, tier_col: str) -> DataFrame:
    """
    Adds a risk tier column based on a fraud score column.
    Thresholds from config:
        >= 0.7  -> high
        >= 0.4  -> medium
        < 0.4   -> low

    Args:
        df        : Input DataFrame
        score_col : Name of the score column e.g. card_fraud_score
        tier_col  : Name of the new tier column e.g. card_risk_tier
    """
    from shared.config import HIGH_RISK_THRESHOLD, MEDIUM_RISK_THRESHOLD

    return df.withColumn(
        tier_col,
        F.when(F.col(score_col) >= HIGH_RISK_THRESHOLD,   "high")
         .when(F.col(score_col) >= MEDIUM_RISK_THRESHOLD, "medium")
         .otherwise("low")
    )


# =============================================================
# DEDUPLICATION
# Removes duplicate rows based on a unique key column
# =============================================================

def deduplicate(df: DataFrame, key_col: str) -> DataFrame:
    """
    Keep only the latest record per unique key.

    Args:
        df      : Input DataFrame
        key_col : Column to deduplicate on e.g. transaction_id
    """
    return df.dropDuplicates([key_col])


# =============================================================
# ROW COUNT ASSERTION
# Fails the pipeline if a DataFrame is empty
# =============================================================

def assert_not_empty(df: DataFrame, table_name: str):
    """
    Raises an error if the DataFrame has zero rows.
    Use this after reading or writing to catch empty loads early.
    """
    count = df.count()
    if count == 0:
        raise ValueError(f"EMPTY DATAFRAME: {table_name} has 0 rows. Pipeline stopped.")
    print(f"Row count check passed: {table_name} has {count} rows.")
"@

Set-Content -Path "shared\utils.py" -Value $content
Write-Host "utils.py created successfully!" -ForegroundColor Green