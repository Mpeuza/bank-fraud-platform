$content = @"
# Card Domain — Table Definitions

---

## 1. ``bronze_card_txn`` — Raw card transactions

| Column | Type | Description |
|---|---|---|
| ``transaction_id`` | STRING | Unique transaction ID |
| ``account_id`` | STRING | Card account reference |
| ``customer_id`` | STRING | Customer reference |
| ``transaction_date`` | STRING | Raw date string (cleaned in silver) |
| ``transaction_time`` | STRING | Raw time string |
| ``amount`` | STRING | Raw amount (cleaned in silver) |
| ``currency`` | STRING | e.g. ZAR, USD, KES |
| ``merchant_id`` | STRING | Merchant reference |
| ``merchant_name`` | STRING | Merchant display name |
| ``merchant_category_code`` | STRING | MCC — type of business |
| ``merchant_country`` | STRING | Country of transaction |
| ``merchant_city`` | STRING | City of transaction |
| ``transaction_type`` | STRING | e.g. purchase, withdrawal, refund |
| ``channel`` | STRING | e.g. online, ATM, POS, contactless |
| ``device_id`` | STRING | Device used (for online txns) |
| ``ip_address`` | STRING | IP address (online only) |
| ``status`` | STRING | e.g. approved, declined, pending |
| ``decline_reason`` | STRING | Reason if declined |
| ``_ingested_at`` | TIMESTAMP | When the record was loaded |
| ``_source_file`` | STRING | Source CSV filename |

---

## 2. ``bronze_card_acc`` — Raw card accounts

| Column | Type | Description |
|---|---|---|
| ``account_id`` | STRING | Unique account ID |
| ``customer_id`` | STRING | Customer reference |
| ``card_number_masked`` | STRING | Last 4 digits only e.g. ****1234 |
| ``card_type`` | STRING | e.g. Visa, Mastercard |
| ``card_tier`` | STRING | e.g. Standard, Gold, Platinum |
| ``account_open_date`` | STRING | Raw date string |
| ``account_status`` | STRING | e.g. active, suspended, closed |
| ``credit_limit`` | STRING | Raw credit limit |
| ``billing_country`` | STRING | Country of card holder |
| ``billing_city`` | STRING | City of card holder |
| ``_ingested_at`` | TIMESTAMP | When the record was loaded |
| ``_source_file`` | STRING | Source CSV filename |

---

## 3. ``silver_card_txn`` — Cleaned card transactions

| Column | Type | Description | Fraud Signal? |
|---|---|---|---|
| ``transaction_id`` | STRING | Unique transaction ID | — |
| ``account_id`` | STRING | Card account reference | — |
| ``customer_id`` | STRING | Customer reference | — |
| ``transaction_ts`` | TIMESTAMP | Combined date + time | — |
| ``amount`` | DOUBLE | Cleaned numeric amount | — |
| ``currency`` | STRING | Standardised currency code | — |
| ``merchant_id`` | STRING | Merchant reference | — |
| ``merchant_name`` | STRING | Cleaned merchant name | — |
| ``merchant_category_code`` | STRING | MCC code | — |
| ``merchant_country`` | STRING | Country of transaction | Yes - Geo anomaly |
| ``merchant_city`` | STRING | City of transaction | Yes - Geo anomaly |
| ``transaction_type`` | STRING | Standardised type | — |
| ``channel`` | STRING | Standardised channel | Yes - Online higher risk |
| ``device_id`` | STRING | Device fingerprint | Yes - Unknown device |
| ``ip_address`` | STRING | Masked IP | Yes - Blacklist check |
| ``status`` | STRING | Approved / Declined | — |
| ``decline_reason`` | STRING | Standardised reason | Yes - Repeated declines |
| ``is_international`` | BOOLEAN | merchant_country not equal billing_country | Yes - Fraud signal |
| ``amount_zscore`` | DOUBLE | Deviation from customer avg | Yes - Anomaly signal |
| ``hour_of_day`` | INT | Transaction hour | Yes - Late-night flag |
| ``is_weekend`` | BOOLEAN | Weekend transaction | Yes - Pattern signal |
| ``_ingested_at`` | TIMESTAMP | Bronze load timestamp | — |

---

## 4. ``silver_card_features`` — Card fraud features per customer

| Column | Type | Description | Fraud Signal? |
|---|---|---|---|
| ``customer_id`` | STRING | Customer reference | — |
| ``account_id`` | STRING | Card account | — |
| ``avg_txn_amount_30d`` | DOUBLE | Avg spend last 30 days | Yes - Baseline |
| ``max_txn_amount_30d`` | DOUBLE | Max single txn last 30 days | Yes - Spike detect |
| ``txn_count_30d`` | INT | Number of transactions last 30 days | Yes - Velocity |
| ``txn_count_1h`` | INT | Transactions in last 1 hour | Yes - High velocity |
| ``txn_count_10min`` | INT | Transactions in last 10 minutes | Yes - Very high velocity |
| ``distinct_countries_30d`` | INT | Number of countries used | Yes - Geo anomaly |
| ``distinct_merchants_7d`` | INT | Unique merchants last 7 days | Yes - Spray pattern |
| ``online_txn_ratio`` | DOUBLE | Percent of txns via online channel | Yes - Online fraud risk |
| ``international_txn_ratio`` | DOUBLE | Percent international transactions | Yes - Geo risk |
| ``declined_txn_count_7d`` | INT | Declined transactions last 7 days | Yes - Card testing |
| ``unknown_device_flag`` | BOOLEAN | New or unrecognised device used | Yes - Takeover signal |
| ``late_night_txn_ratio`` | DOUBLE | Percent txns between 00:00-04:00 | Yes - Pattern signal |
| ``merchant_risk_score`` | DOUBLE | Avg MCC risk score | Yes - High-risk merchants |
| ``card_fraud_score`` | DOUBLE | ML model output 0 to 1 | Yes - Final score |
| ``card_risk_tier`` | STRING | low / medium / high | Yes - Decision tier |
| ``feature_updated_at`` | TIMESTAMP | When features were last computed | — |

---

## Fraud Scenarios Covered

| Scenario | Key Signals |
|---|---|
| Card transaction fraud | amount_zscore, merchant_risk_score, is_international |
| Card account takeover | unknown_device_flag, txn_count_10min, distinct_countries_30d, declined_txn_count_7d |
"@

Set-Content -Path "card\CARD_DATA_MODEL.md" -Value $content
Write-Host "CARD_DATA_MODEL.md created successfully!" -ForegroundColor Green