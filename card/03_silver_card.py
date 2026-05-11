# =============================================================
# 03_silver_card.py — Card Domain Silver Transformation
# Reads from bronze_card_txn and bronze_card_acc
# Cleans data, casts types, engineers fraud signals
# Writes to silver_card_txn and silver_card_acc
# =============================================================

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from shared.config import (
    BRONZE_CARD_TXN,
    BRONZE_CARD_ACC,
    SILVER_CARD_TXN,
    SILVER_CARD_ACC,
    TARGET_CATALOG,
    CARD_SILVER_SCHEMA
)
from shared.logger import log_info, log_warning, log_error, print_run_summary
from shared.utils import (
    write_delta,
    assert_not_empty,
    cast_to_double,
    cast_to_timestamp,
    deduplicate
)

PIPELINE = "card_silver"

# =============================================================
# START SPARK
# =============================================================
spark = SparkSession.builder \
    .appName("FraudPlatform - Card Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
log_info(spark, PIPELINE, "Silver transformation started")

# =============================================================
# SETUP
# =============================================================
try:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_CATALOG}.{CARD_SILVER_SCHEMA}")
    log_info(spark, PIPELINE, f"Schema ready: {TARGET_CATALOG}.{CARD_SILVER_SCHEMA}")
except Exception as e:
    log_error(spark, PIPELINE, f"Schema creation failed: {str(e)}")
    raise

# =============================================================
# STEP 1 — Read bronze tables
# =============================================================
log_info(spark, PIPELINE, "Step 1: Reading bronze tables")

try:
    df_txn = spark.read.format("delta").table(BRONZE_CARD_TXN)
    df_acc  = spark.read.format("delta").table(BRONZE_CARD_ACC)
    assert_not_empty(df_txn, BRONZE_CARD_TXN)
    assert_not_empty(df_acc,  BRONZE_CARD_ACC)
    log_info(spark, PIPELINE, f"Step 1: Loaded {df_txn.count()} txn rows, {df_acc.count()} acc rows")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 1 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 2 — Clean and cast silver_card_txn
# =============================================================
log_info(spark, PIPELINE, "Step 2: Cleaning card transactions")

try:
    df_txn_silver = df_txn \
        .withColumn(
            "transaction_ts",
            F.to_timestamp(
                F.concat(F.col("transaction_date"), F.lit(" "), F.col("transaction_time")),
                "yyyy-MM-dd HH:mm:ss"
            )
        ) \
        .withColumn("amount",       F.col("amount").cast("double")) \
        .withColumn("is_fraud_label", F.col("is_fraud_label").cast("int")) \
        .withColumn("merchant_country", F.upper(F.trim(F.col("merchant_country")))) \
        .withColumn("channel",      F.lower(F.trim(F.col("channel")))) \
        .withColumn("status",       F.lower(F.trim(F.col("status")))) \
        .withColumn("currency",     F.upper(F.trim(F.col("currency"))))

    # ── Fraud signal columns ──────────────────────────────────

    # 1. is_international — merchant country differs from billing country
    df_txn_silver = df_txn_silver.join(
        df_acc.select("account_id", "billing_country"),
        on="account_id", how="left"
    ).withColumn(
        "is_international",
        F.when(F.col("merchant_country") != F.col("billing_country"), True).otherwise(False)
    )

    # 2. hour_of_day — what hour the transaction happened
    df_txn_silver = df_txn_silver.withColumn(
        "hour_of_day", F.hour(F.col("transaction_ts"))
    )

    # 3. is_weekend — Saturday=7, Sunday=1 in Spark dayofweek
    df_txn_silver = df_txn_silver.withColumn(
        "is_weekend",
        F.when(F.dayofweek(F.col("transaction_ts")).isin(1, 7), True).otherwise(False)
    )

    # 4. amount_zscore — how far this transaction is from the customer average
    w = Window.partitionBy("customer_id")
    df_txn_silver = df_txn_silver \
        .withColumn("avg_amount", F.avg("amount").over(w)) \
        .withColumn("std_amount", F.stddev("amount").over(w)) \
        .withColumn(
            "amount_zscore",
            F.when(
                F.col("std_amount") > 0,
                (F.col("amount") - F.col("avg_amount")) / F.col("std_amount")
            ).otherwise(0.0)
        ) \
        .drop("avg_amount", "std_amount")

    # Drop original raw date/time columns — replaced by transaction_ts
    df_txn_silver = df_txn_silver.drop("transaction_date", "transaction_time", "billing_country")

    # Deduplicate on transaction_id
    df_txn_silver = deduplicate(df_txn_silver, "transaction_id")

    log_info(spark, PIPELINE, f"Step 2: silver_card_txn ready — {df_txn_silver.count()} rows")

except Exception as e:
    log_error(spark, PIPELINE, f"Step 2 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 3 — Clean and cast silver_card_acc
# =============================================================
log_info(spark, PIPELINE, "Step 3: Cleaning card accounts")

try:
    df_acc_silver = df_acc \
        .withColumn("credit_limit",      F.col("credit_limit").cast("double")) \
        .withColumn("account_open_date", F.to_date(F.col("account_open_date"), "yyyy-MM-dd")) \
        .withColumn("card_type",         F.lower(F.trim(F.col("card_type")))) \
        .withColumn("card_tier",         F.lower(F.trim(F.col("card_tier")))) \
        .withColumn("account_status",    F.lower(F.trim(F.col("account_status")))) \
        .withColumn("billing_country",   F.upper(F.trim(F.col("billing_country"))))

    df_acc_silver = deduplicate(df_acc_silver, "account_id")
    log_info(spark, PIPELINE, f"Step 3: silver_card_acc ready — {df_acc_silver.count()} rows")

except Exception as e:
    log_error(spark, PIPELINE, f"Step 3 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 4 — Write to Silver Delta tables
# =============================================================
log_info(spark, PIPELINE, "Step 4: Writing to silver tables")

try:
    write_delta(df_txn_silver, SILVER_CARD_TXN, mode="overwrite")
    log_info(spark, PIPELINE, f"Step 4: Written to {SILVER_CARD_TXN}")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 4 FAILED writing silver_card_txn: {str(e)}")
    raise

try:
    write_delta(df_acc_silver, SILVER_CARD_ACC, mode="overwrite")
    log_info(spark, PIPELINE, f"Step 4: Written to {SILVER_CARD_ACC}")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 4 FAILED writing silver_card_acc: {str(e)}")
    raise

# =============================================================
# STEP 5 — Preview fraud signals
# =============================================================
log_info(spark, PIPELINE, "Step 5: Fraud signal preview")

print("\n── International transactions ──")
df_txn_silver.groupBy("is_international").count().show()

print("\n── Transactions by hour (top 5) ──")
df_txn_silver.groupBy("hour_of_day").count().orderBy("count", ascending=False).show(5)

print("\n── Weekend vs weekday ──")
df_txn_silver.groupBy("is_weekend").count().show()

print("\n── High amount_zscore (potential anomalies) ──")
df_txn_silver.filter(F.col("amount_zscore") > 2.0) \
    .select("transaction_id", "customer_id", "amount", "amount_zscore") \
    .orderBy("amount_zscore", ascending=False) \
    .show(10)

log_info(spark, PIPELINE, "Step 5: Preview complete")

# =============================================================
# SUMMARY
# =============================================================
txn_count = df_txn_silver.count()
acc_count = df_acc_silver.count()
print(f"\nsilver_card_txn : {txn_count} rows")
print(f"silver_card_acc : {acc_count} rows")

print_run_summary(spark, PIPELINE)