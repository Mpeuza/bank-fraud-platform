# Loan Domain — Table Definitions

---

## 1. `bronze_loan_app` — Raw loan applications

| Column | Type | Description |
|---|---|---|
| `application_id` | STRING | Unique loan application ID |
| `customer_id` | STRING | Customer reference |
| `application_date` | STRING | Raw date string (cleaned in silver) |
| `loan_type` | STRING | e.g. personal, mortgage, vehicle, business |
| `loan_amount_requested` | STRING | Raw amount requested |
| `loan_term_months` | STRING | Raw repayment period |
| `purpose` | STRING | Reason for loan e.g. home, education, debt |
| `stated_income` | STRING | Income declared by applicant |
| `stated_employment_status` | STRING | e.g. employed, self-employed, unemployed |
| `stated_employer` | STRING | Employer name as stated |
| `stated_years_employed` | STRING | Years at current employer |
| `id_number` | STRING | National ID / passport number |
| `id_type` | STRING | e.g. national_id, passport, drivers_license |
| `date_of_birth` | STRING | Raw DOB string |
| `marital_status` | STRING | e.g. single, married, divorced |
| `num_dependants` | STRING | Number of dependants declared |
| `residential_status` | STRING | e.g. owner, renting, living with family |
| `address` | STRING | Physical address |
| `branch_id` | STRING | Branch where application was submitted |
| `agent_id` | STRING | Loan officer / agent reference |
| `application_channel` | STRING | e.g. branch, online, mobile, agent |
| `application_status` | STRING | e.g. pending, approved, declined, disbursed |
| `_ingested_at` | TIMESTAMP | When the record was loaded |
| `_source_file` | STRING | Source CSV filename |

---

## 2. `bronze_loan_rep` — Raw loan repayments

| Column | Type | Description |
|---|---|---|
| `repayment_id` | STRING | Unique repayment record ID |
| `application_id` | STRING | Loan application reference |
| `customer_id` | STRING | Customer reference |
| `due_date` | STRING | Raw expected payment date |
| `payment_date` | STRING | Raw actual payment date |
| `due_amount` | STRING | Raw amount due |
| `paid_amount` | STRING | Raw amount actually paid |
| `payment_method` | STRING | e.g. bank transfer, cash, mobile money |
| `payment_status` | STRING | e.g. paid, missed, partial, late |
| `days_past_due` | STRING | Raw number of days overdue |
| `outstanding_balance` | STRING | Remaining loan balance |
| `_ingested_at` | TIMESTAMP | When the record was loaded |
| `_source_file` | STRING | Source CSV filename |

---

## 3. `silver_loan_app` — Cleaned loan applications

| Column | Type | Description | Fraud Signal? |
|---|---|---|---|
| `application_id` | STRING | Unique loan application ID | — |
| `customer_id` | STRING | Customer reference | — |
| `application_ts` | TIMESTAMP | Cleaned application datetime | — |
| `loan_type` | STRING | Standardised loan type | — |
| `loan_amount_requested` | DOUBLE | Cleaned numeric amount | — |
| `loan_term_months` | INT | Cleaned repayment period | — |
| `purpose` | STRING | Standardised loan purpose | — |
| `stated_income` | DOUBLE | Cleaned declared income | — |
| `stated_employment_status` | STRING | Standardised employment | — |
| `stated_employer` | STRING | Cleaned employer name | — |
| `stated_years_employed` | INT | Cleaned years employed | — |
| `id_number` | STRING | Masked national ID | — |
| `id_type` | STRING | Standardised ID type | — |
| `age` | INT | Derived from date_of_birth | ✅ Age anomaly |
| `residential_status` | STRING | Standardised status | — |
| `application_channel` | STRING | Standardised channel | ✅ Online = higher risk |
| `agent_id` | STRING | Loan officer reference | ✅ Agent fraud pattern |
| `loan_to_income_ratio` | DOUBLE | loan_amount / stated_income | ✅ Overstatement signal |
| `is_income_realistic` | BOOLEAN | Income vs employment check | ✅ Fraud signal |
| `applications_last_30d` | INT | Number of applications by same customer | ✅ Stacking signal |
| `applications_last_30d_diff_branch` | INT | Applications at different branches | ✅ Fraud ring signal |
| `id_used_by_multiple_customers` | BOOLEAN | Same ID on multiple accounts | ✅ Identity fraud |
| `address_used_by_multiple_customers` | BOOLEAN | Same address on multiple accounts | ✅ Fraud ring |
| `agent_approval_rate` | DOUBLE | % of agent's applications approved | ✅ Corrupt agent signal |
| `application_status` | STRING | Standardised status | — |
| `_ingested_at` | TIMESTAMP | Bronze load timestamp | — |

---

## 4. `silver_loan_rep` — Cleaned repayments + risk signals

| Column | Type | Description | Fraud Signal? |
|---|---|---|---|
| `repayment_id` | STRING | Unique repayment record ID | — |
| `application_id` | STRING | Loan application reference | — |
| `customer_id` | STRING | Customer reference | — |
| `due_date` | DATE | Cleaned due date | — |
| `payment_date` | DATE | Cleaned payment date | — |
| `due_amount` | DOUBLE | Cleaned amount due | — |
| `paid_amount` | DOUBLE | Cleaned amount paid | — |
| `payment_status` | STRING | Standardised status | — |
| `days_past_due` | INT | Cleaned days overdue | ✅ Default signal |
| `outstanding_balance` | DOUBLE | Cleaned remaining balance | — |
| `payment_shortfall` | DOUBLE | due_amount - paid_amount | ✅ Partial pay pattern |
| `is_missed` | BOOLEAN | Payment completely missed | ✅ Default signal |
| `is_partial` | BOOLEAN | Paid less than due | ✅ Stress signal |
| `is_late` | BOOLEAN | Paid after due date | ✅ Behaviour signal |
| `consecutive_missed_count` | INT | Streak of missed payments | ✅ Strong default signal |
| `_ingested_at` | TIMESTAMP | Bronze load timestamp | — |

---

## 5. `silver_loan_features` — Loan fraud features per customer

> One row per customer. Updated on every pipeline run.

| Column | Type | Description | Fraud Signal? |
|---|---|---|---|
| `customer_id` | STRING | Customer reference | — |
| `application_id` | STRING | Most recent loan application | — |
| `loan_to_income_ratio` | DOUBLE | Loan amount vs stated income | ✅ Overstatement |
| `applications_last_30d` | INT | How many loans applied for recently | ✅ Loan stacking |
| `distinct_branches_30d` | INT | Applications across different branches | ✅ Fraud ring |
| `id_shared_flag` | BOOLEAN | ID used by another customer | ✅ Identity fraud |
| `address_shared_flag` | BOOLEAN | Address used by multiple customers | ✅ Fraud ring |
| `agent_risk_flag` | BOOLEAN | Agent has abnormally high approval rate | ✅ Corrupt agent |
| `missed_payment_count` | INT | Total missed payments on record | ✅ Default risk |
| `consecutive_missed_count` | INT | Longest streak of missed payments | ✅ Strong default |
| `partial_payment_ratio` | DOUBLE | % of payments that were partial | ✅ Financial stress |
| `avg_days_past_due` | DOUBLE | Average lateness of payments | ✅ Behaviour pattern |
| `total_outstanding_balance` | DOUBLE | All unpaid loan balances combined | ✅ Overexposure |
| `income_realistic_flag` | BOOLEAN | Stated income checks out | ✅ Fraud signal |
| `loan_fraud_score` | DOUBLE | ML model output (0–1) | ✅ Final score |
| `loan_risk_tier` | STRING | low / medium / high | ✅ Decision tier |
| `feature_updated_at` | TIMESTAMP | When features were last computed | — |

---

## Fraud Scenarios Covered by These Tables

| Scenario | Key Signals |
|---|---|
| **Loan application fraud** | loan_to_income_ratio, id_shared_flag, address_shared_flag, applications_last_30d, agent_risk_flag |
| **Loan repayment fraud** | consecutive_missed_count, partial_payment_ratio, avg_days_past_due, total_outstanding_balance |