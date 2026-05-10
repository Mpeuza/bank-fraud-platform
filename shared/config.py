$content = @"
# =============================================================
# config.py — Central configuration for bank fraud platform
# All notebooks import this file to get shared settings
# =============================================================

# ── Catalog & Schema names ────────────────────────────────────
TARGET_CATALOG = "fraud_catalog"

# Card domain schemas
CARD_BRONZE_SCHEMA  = "bronze_card"
CARD_SILVER_SCHEMA  = "silver_card"

# Loan domain schemas
LOAN_BRONZE_SCHEMA  = "bronze_loan"
LOAN_SILVER_SCHEMA  = "silver_loan"

# Gold layer (shared)
GOLD_SCHEMA         = "gold"

# Logging
LOG_SCHEMA          = "logs"
LOG_TABLE           = "logtable"

# ── Full table paths ──────────────────────────────────────────

# Card bronze
BRONZE_CARD_TXN     = f"{TARGET_CATALOG}.{CARD_BRONZE_SCHEMA}.bronze_card_txn"
BRONZE_CARD_ACC     = f"{TARGET_CATALOG}.{CARD_BRONZE_SCHEMA}.bronze_card_acc"

# Card silver
SILVER_CARD_TXN      = f"{TARGET_CATALOG}.{CARD_SILVER_SCHEMA}.silver_card_txn"
SILVER_CARD_ACC      = f"{TARGET_CATALOG}.{CARD_SILVER_SCHEMA}.silver_card_acc"
SILVER_CARD_FEATURES = f"{TARGET_CATALOG}.{CARD_SILVER_SCHEMA}.silver_card_features"

# Loan bronze
BRONZE_LOAN_APP     = f"{TARGET_CATALOG}.{LOAN_BRONZE_SCHEMA}.bronze_loan_app"
BRONZE_LOAN_REP     = f"{TARGET_CATALOG}.{LOAN_BRONZE_SCHEMA}.bronze_loan_rep"

# Loan silver
SILVER_LOAN_APP      = f"{TARGET_CATALOG}.{LOAN_SILVER_SCHEMA}.silver_loan_app"
SILVER_LOAN_REP      = f"{TARGET_CATALOG}.{LOAN_SILVER_SCHEMA}.silver_loan_rep"
SILVER_LOAN_FEATURES = f"{TARGET_CATALOG}.{LOAN_SILVER_SCHEMA}.silver_loan_features"

# Gold
GOLD_CARD_SCORES    = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_card_scores"
GOLD_LOAN_SCORES    = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_loan_scores"
GOLD_FRAUD_ALERTS   = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_fraud_alerts"
GOLD_CUSTOMER_RISK  = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_customer_risk"

# Log table
LOGTABLE            = f"{TARGET_CATALOG}.{LOG_SCHEMA}.{LOG_TABLE}"

# ── Source data paths (your CSV files) ───────────────────────
DATA_PATH           = "data/sample/"
CARD_TXN_FILE       = f"{DATA_PATH}card_transactions.csv"
CARD_ACC_FILE       = f"{DATA_PATH}card_accounts.csv"
LOAN_APP_FILE       = f"{DATA_PATH}loan_applications.csv"
LOAN_REP_FILE       = f"{DATA_PATH}loan_repayments.csv"
CUSTOMERS_FILE      = f"{DATA_PATH}customers.csv"

# ── Pipeline settings ─────────────────────────────────────────
MAX_RECORDS_PER_FILE = 100000
RETRIES              = 3
MAX_CONCURRENCY      = 5

# ── Risk score thresholds ─────────────────────────────────────
HIGH_RISK_THRESHOLD   = 0.7
MEDIUM_RISK_THRESHOLD = 0.4

# ── Risk tier labels ──────────────────────────────────────────
RISK_TIER_HIGH   = "high"
RISK_TIER_MEDIUM = "medium"
RISK_TIER_LOW    = "low"
"@

Set-Content -Path "shared\config.py" -Value $content
Write-Host "config.py created successfully!" -ForegroundColor Green