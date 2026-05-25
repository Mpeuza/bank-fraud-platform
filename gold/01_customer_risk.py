# =============================================================
# 01_customer_risk.py — Gold Layer: Unified Customer Risk
# Reads from silver_card_features and silver_loan_features
# Merges card and loan fraud scores per customer
# Writes to gold_customer_risk — the main Power BI table
# =============================================================

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from shared.config import (
    SILVER_CARD_FEATURES,
    SILVER_LOAN_FEATURES,
    GOLD_CARD_SCORES,
    GOLD_LOAN_SCORES,
    GOLD_CUSTOMER_RISK,
    CUSTOMERS_FILE,
    TARGET_CATALOG,
    GOLD_SCHEMA
)
from shared.logger import log_info, log_warning, log_error, print_run_summary
from shared.utils import read_csv, write_delta, assert_not_empty, assign_risk_tier

PIPELINE = "gold_customer_risk"

# =============================================================
# START SPARK
# =============================================================
spark = SparkSession.builder \
    .appName("FraudPlatform - Gold Customer Risk") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
log_info(spark, PIPELINE, "Gold customer risk started")

# =============================================================
# SETUP
# =============================================================
try:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")
    log_info(spark, PIPELINE, f"Schema ready: {TARGET_CATALOG}.{GOLD_SCHEMA}")
except Exception as e:
    log_error(spark, PIPELINE, f"Schema creation failed: {str(e)}")
    raise

# =============================================================
# STEP 1 — Read silver feature tables
# =============================================================
log_info(spark, PIPELINE, "Step 1: Reading silver feature tables")

try:
    df_card = spark.read.format("delta").table(SILVER_CARD_FEATURES)
    df_loan = spark.read.format("delta").table(SILVER_LOAN_FEATURES)
    assert_not_empty(df_card, SILVER_CARD_FEATURES)
    assert_not_empty(df_loan, SILVER_LOAN_FEATURES)
    log_info(spark, PIPELINE, f"Step 1: {df_card.count()} card rows, {df_loan.count()} loan rows")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 1 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 2 — Read customers reference table
# =============================================================
log_info(spark, PIPELINE, "Step 2: Reading customers reference")

try:
    df_customers = read_csv(spark, CUSTOMERS_FILE)
    assert_not_empty(df_customers, "customers.csv")
    log_info(spark, PIPELINE, f"Step 2: {df_customers.count()} customers loaded")
except Exception as e:
    log_error(spark, PIPELINE, f"Step 2 FAILED: {str(e)}")
    raise

# =============================================================
# STEP 3 — Build gold_card_scores
# =============================================================
log_info(spark, PIPELINE, "Step 3: Building gold_card_scores")

try:
    df_card_scores = df_card.select(
        "customer_id",
        "account_id",
        "card_fraud_score",
        "card_