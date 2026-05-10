$content = @"
# Gold Layer — Table Definitions
# Where Card and Loan pipelines merge into one customer risk view

---

## 1. ``gold_card_scores`` — Card fraud scores per customer

| Column | Type | Description |
|---|---|---|
| ``customer_id`` | STRING | Customer reference |
| ``account_id`` | STRING | Card account reference |
| ``card_fraud_score`` | DOUBLE | ML score from 0 to 1 |
| ``card_risk_tier`` | STRING | low / medium / high |
| ``top_fraud_signal`` | STRING | The single strongest fraud signal detected |
| ``velocity_flag`` | BOOLEAN | True if txn velocity is abnormal |
| ``geo_anomaly_flag`` | BOOLEAN | True if international anomaly detected |
| ``device_anomaly_flag`` | BOOLEAN | True if unknown device used |
| ``declined_txn_flag`` | BOOLEAN | True if repeated declines detected |
| ``score_updated_at`` | TIMESTAMP | When score was last computed |

---

## 2. ``gold_loan_scores`` — Loan fraud scores per customer

| Column | Type | Description |
|---|---|---|
| ``customer_id`` | STRING | Customer reference |
| ``application_id`` | STRING | Most recent loan application |
| ``loan_fraud_score`` | DOUBLE | ML score from 0 to 1 |
| ``loan_risk_tier`` | STRING | low / medium / high |
| ``top_fraud_signal`` | STRING | The single strongest fraud signal detected |
| ``income_fraud_flag`` | BOOLEAN | True if income looks overstated |
| ``identity_fraud_flag`` | BOOLEAN | True if ID shared across customers |
| ``loan_stacking_flag`` | BOOLEAN | True if multiple loans applied recently |
| ``agent_fraud_flag`` | BOOLEAN | True if linked to high-risk agent |
| ``repayment_risk_flag`` | BOOLEAN | True if repayment behaviour is poor |
| ``score_updated_at`` | TIMESTAMP | When score was last computed |

---

## 3. ``gold_fraud_alerts`` — All flagged transactions and applications

| Column | Type | Description |
|---|---|---|
| ``alert_id`` | STRING | Unique alert ID |
| ``customer_id`` | STRING | Customer reference |
| ``alert_type`` | STRING | card_transaction / card_takeover / loan_application / loan_repayment |
| ``alert_source`` | STRING | card / loan |
| ``reference_id`` | STRING | transaction_id or application_id that triggered alert |
| ``fraud_score`` | DOUBLE | Score at time of alert |
| ``risk_tier`` | STRING | low / medium / high |
| ``top_signal`` | STRING | What triggered the alert |
| ``alert_status`` | STRING | open / under_review / confirmed_fraud / false_positive |
| ``assigned_to`` | STRING | Analyst assigned to review |
| ``created_at`` | TIMESTAMP | When alert was raised |
| ``resolved_at`` | TIMESTAMP | When alert was closed |
| ``resolution_note`` | STRING | Analyst notes on outcome |

---

## 4. ``gold_customer_risk`` — Unified customer risk view (MAIN TABLE)

> This is the most important table. One row per customer.
> Merges card scores + loan scores into a single risk profile.
> This is what Power BI connects to directly.

| Column | Type | Description |
|---|---|---|
| ``customer_id`` | STRING | Customer reference |
| ``full_name`` | STRING | Customer full name |
| ``id_number`` | STRING | Masked national ID |
| ``date_of_birth`` | DATE | Customer date of birth |
| ``age`` | INT | Derived age |
| ``country`` | STRING | Country of residence |
| ``city`` | STRING | City of residence |
| ``account_status`` | STRING | active / suspended / closed |
| ``card_fraud_score`` | DOUBLE | Score from card pipeline 0 to 1 |
| ``loan_fraud_score`` | DOUBLE | Score from loan pipeline 0 to 1 |
| ``overall_risk_score`` | DOUBLE | MAX of card and loan scores |
| ``overall_risk_tier`` | STRING | low / medium / high |
| ``flagged_in_card`` | BOOLEAN | True if card risk tier is high |
| ``flagged_in_loan`` | BOOLEAN | True if loan risk tier is high |
| ``flagged_in_both`` | BOOLEAN | True if flagged in card AND loan - highest priority |
| ``total_alerts`` | INT | Total number of fraud alerts raised |
| ``open_alerts`` | INT | Alerts not yet resolved |
| ``last_alert_date`` | TIMESTAMP | Most recent alert timestamp |
| ``last_card_transaction`` | TIMESTAMP | Most recent card transaction |
| ``last_loan_application`` | TIMESTAMP | Most recent loan application |
| ``risk_updated_at`` | TIMESTAMP | When this row was last refreshed |

---

## How the Gold Layer is Built

``gold_card_scores`` comes from ``silver_card_features``
``gold_loan_scores`` comes from ``silver_loan_features``
``gold_fraud_alerts`` is written by both card and loan pipelines
``gold_customer_risk`` joins all of the above on ``customer_id``

---

## SQL Logic for gold_customer_risk

SELECT
    c.customer_id,
    c.full_name,
    c.id_number,
    c.date_of_birth,
    c.age,
    c.country,
    c.city,
    c.account_status,
    card.card_fraud_score,
    loan.loan_fraud_score,
    GREATEST(COALESCE(card.card_fraud_score, 0), COALESCE(loan.loan_fraud_score, 0)) AS overall_risk_score,
    CASE
        WHEN GREATEST(COALESCE(card.card_fraud_score, 0), COALESCE(loan.loan_fraud_score, 0)) >= 0.7 THEN 'high'
        WHEN GREATEST(COALESCE(card.card_fraud_score, 0), COALESCE(loan.loan_fraud_score, 0)) >= 0.4 THEN 'medium'
        ELSE 'low'
    END AS overall_risk_tier,
    card.card_risk_tier = 'high' AS flagged_in_card,
    loan.loan_risk_tier = 'high' AS flagged_in_loan,
    (card.card_risk_tier = 'high' AND loan.loan_risk_tier = 'high') AS flagged_in_both,
    current_timestamp() AS risk_updated_at
FROM customers c
LEFT JOIN gold_card_scores card ON c.customer_id = card.customer_id
LEFT JOIN gold_loan_scores loan ON c.customer_id = loan.customer_id

---

## Power BI Pages fed by Gold Layer

| Power BI Page | Source Table |
|---|---|
| Card Fraud Overview | gold_card_scores + gold_fraud_alerts |
| Loan Fraud Overview | gold_loan_scores + gold_fraud_alerts |
| Customer Risk (main) | gold_customer_risk |
| Alert Management | gold_fraud_alerts |
"@

Set-Content -Path "gold\GOLD_DATA_MODEL.md" -Value $content
Write-Host "GOLD_DATA_MODEL.md created successfully!" -ForegroundColor Green