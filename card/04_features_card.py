# =============================================================
# 04_features_card.py — Card Domain Feature Engineering
# Reads from silver_card_txn and silver_card_acc
# Builds one row per customer with all fraud signals
# Writes to silver_card_features
# This table feeds the ML model and Power BI dashboard
# =============================================================

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from shared.config import (
    SILVER_CARD_TXN,
    SILVER_CARD_ACC,
    SILVER_CARD_FEATURES,
    TARGET_CATALOG,
    CARD_SILVER_SCHEMA
)
from shared.logger import log_info, log_warning, log_error, print_run_summary
from shared.utils import write_delta, assert_not_empty, assign_risk_tier

PIPELINE = "card_features"

# =============================================================
# START SPARK
# =============================================================
spark = SparkSession.builder \
    .appName("FraudPlatform - Card Features") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
log_info(spark, PIPELINE, "Feature engineering started")

# =============================================================
# STEP 1 — Read silver tables
# =============================================================
log_info(spark, PIPELINE, "Step 1: Reading silver tables")

try:
    df_txn = spark.read.format("delta").table(SILVER_CARD_TXN)
    df_acc  = spark.read.format("delta").table(SILVER_CARD_ACC)
    assert_not_empty(df_txn, SILVER_CARD_TXN)
    assert_not_empty(df_acc,  SILVER_CARD_ACC)
    log_info(spark, PIPELINE, f"Step 1: {df_txn.count()} txn rows, {df_acc.count()} acc rows")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 1 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 2 — Velocity features
# How many transactions per customer in different time windows
# =============================================================
log_info(spark, PIPELINE, "Step 2: Computing velocity features")

try:
    # Total transactions last 30 days (using all data as proxy)
    velocity_30d = df_txn.groupBy("customer_id").agg(
        F.count("transaction_id").alias("txn_count_30d"),
        F.avg("amount").alias("avg_txn_amount_30d"),
        F.max("amount").alias("max_txn_amount_30d"),
        F.sum(F.when(F.col("status") == "declined", 1).otherwise(0)).alias("declined_txn_count_7d")
    )

    # Transactions in last 1 hour proxy — high amount_zscore transactions
    velocity_1h = df_txn.groupBy("customer_id").agg(
        F.sum(F.when(F.col("hour_of_day").between(0, 1), 1).otherwise(0)).alias("txn_count_1h")
    )

    # Transactions late night (00:00 - 04:00)
    late_night = df_txn.groupBy("customer_id").agg(
        (F.sum(F.when(F.col("hour_of_day").between(0, 4), 1).otherwise(0)) /
         F.count("transaction_id")).alias("late_night_txn_ratio")
    )

    log_info(spark, PIPELINE, "Step 2: Velocity features computed")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 2 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 3 — Geographic and channel features
# =============================================================
log_info(spark, PIPELINE, "Step 3: Computing geo and channel features")

try:
    geo_features = df_txn.groupBy("customer_id").agg(
        F.countDistinct("merchant_country").alias("distinct_countries_30d"),
        F.countDistinct("merchant_name").alias("distinct_merchants_7d"),
        (F.sum(F.when(F.col("is_international") == True, 1).otherwise(0)) /
         F.count("transaction_id")).alias("international_txn_ratio"),
        (F.sum(F.when(F.col("channel") == "online", 1).otherwise(0)) /
         F.count("transaction_id")).alias("online_txn_ratio")
    )

    log_info(spark, PIPELINE, "Step 3: Geo and channel features computed")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 3 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 4 — Device and anomaly features
# =============================================================
log_info(spark, PIPELINE, "Step 4: Computing device and anomaly features")

try:
    # Count distinct devices per customer
    device_features = df_txn.groupBy("customer_id").agg(
        F.countDistinct("device_id").alias("distinct_devices")
    ).withColumn(
        "unknown_device_flag",
        F.when(F.col("distinct_devices") > 3, True).otherwise(False)
    ).drop("distinct_devices")

    # Merchant risk score — based on amount_zscore average per merchant
    merchant_risk = df_txn.groupBy("customer_id").agg(
        F.avg("amount_zscore").alias("merchant_risk_score")
    )

    log_info(spark, PIPELINE, "Step 4: Device and anomaly features computed")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 4 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 5 — Join all features together
# =============================================================
log_info(spark, PIPELINE, "Step 5: Joining all features")

try:
    # Start with account base
    df_base = df_acc.select("customer_id", "account_id", "account_status", "credit_limit")

    df_features = df_base \
        .join(velocity_30d,   on="customer_id", how="left") \
        .join(velocity_1h,    on="customer_id", how="left") \
        .join(late_night,     on="customer_id", how="left") \
        .join(geo_features,   on="customer_id", how="left") \
        .join(device_features,on="customer_id", how="left") \
        .join(merchant_risk,  on="customer_id", how="left")

    # Fill nulls with 0 for numeric feature columns
    numeric_cols = [
        "txn_count_30d", "avg_txn_amount_30d", "max_txn_amount_30d",
        "declined_txn_count_7d", "txn_count_1h", "late_night_txn_ratio",
        "distinct_countries_30d", "distinct_merchants_7d",
        "international_txn_ratio", "online_txn_ratio", "merchant_risk_score"
    ]
    df_features = df_features.fillna(0, subset=numeric_cols)
    df_features = df_features.fillna(False, subset=["unknown_device_flag"])

    log_info(spark, PIPELINE, f"Step 5: Features joined — {df_features.count()} customer rows")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 5 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 6 — Compute fraud score (rule-based for now)
# Simple weighted score — replace with ML model later
# =============================================================
log_info(spark, PIPELINE, "Step 6: Computing fraud score")

try:
    df_features = df_features.withColumn(
        "card_fraud_score",
        F.least(F.lit(1.0), F.greatest(F.lit(0.0),
            (
                F.when(F.col("distinct_countries_30d") > 2,  0.25).otherwise(0.0) +
                F.when(F.col("unknown_device_flag") == True,  0.20).otherwise(0.0) +
                F.when(F.col("declined_txn_count_7d") > 3,   0.20).otherwise(0.0) +
                F.when(F.col("international_txn_ratio") > 0.5, 0.15).otherwise(0.0) +
                F.when(F.col("late_night_txn_ratio") > 0.3,  0.10).otherwise(0.0) +
                F.when(F.col("merchant_risk_score") > 2.0,   0.10).otherwise(0.0)
            )
        ))
    )

    # Assign risk tier from config thresholds
    df_features = assign_risk_tier(df_features, "card_fraud_score", "card_risk_tier")

    # Add timestamp
    df_features = df_features.withColumn("feature_updated_at", F.current_timestamp())

    log_info(spark, PIPELINE, "Step 6: Fraud scores computed")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 6 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 7 — Write to silver_card_features
# =============================================================
log_info(spark, PIPELINE, "Step 7: Writing silver_card_features")

try:
    write_delta(df_features, SILVER_CARD_FEATURES, mode="overwrite")
    log_info(spark, PIPELINE, f"Step 7: Written to {SILVER_CARD_FEATURES}")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 7 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 8 — Preview results
# =============================================================
log_info(spark, PIPELINE, "Step 8: Feature preview")

print("\n── Risk tier breakdown ──")
df_features.groupBy("card_risk_tier").count().orderBy("count", ascending=False).show()

print("\n── Top 10 highest risk customers ──")
df_features.select(
    "customer_id", "card_fraud_score", "card_risk_tier",
    "distinct_countries_30d", "unknown_device_flag",
    "declined_txn_count_7d", "international_txn_ratio"
).orderBy("card_fraud_score", ascending=False).show(10)

print("\n── Score distribution ──")
df_features.select(
    F.min("card_fraud_score").alias("min_score"),
    F.max("card_fraud_score").alias("max_score"),
    F.avg("card_fraud_score").alias("avg_score"),
    F.count("customer_id").alias("total_customers")
).show()

log_info(spark, PIPELINE, "Step 8: Preview complete")

# =============================================================
# SUMMARY
# =============================================================
total     = df_features.count()
high_risk = df_features.filter(F.col("card_risk_tier") == "high").count()
print(f"\nsilver_card_features : {total} customers")
print(f"High risk customers  : {high_risk}")

print_run_summary(spark, PIPELINE)