$script = @"
import csv
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)

# ── Helpers ───────────────────────────────────────────────────
def rand_date(start, end):
    delta = end - start
    return (start + timedelta(days=random.randint(0, delta.days))).strftime("%Y-%m-%d")

def rand_ts(start, end):
    delta = end - start
    secs  = random.randint(0, int(delta.total_seconds()))
    return (start + timedelta(seconds=secs)).strftime("%Y-%m-%d %H:%M:%S")

def rand_id(prefix=""):
    return prefix + str(uuid.uuid4())[:8].upper()

START = datetime(2023, 1, 1)
END   = datetime(2024, 12, 31)

# ── 1. customers.csv ─────────────────────────────────────────
countries = ["ZA", "KE", "GH", "NG", "UG"]
cities    = {"ZA": ["Johannesburg","Cape Town","Durban"],
             "KE": ["Nairobi","Mombasa"],
             "GH": ["Accra","Kumasi"],
             "NG": ["Lagos","Abuja"],
             "UG": ["Kampala","Entebbe"]}
employment = ["employed","self-employed","unemployed","student"]

customers = []
for _ in range(500):
    cid     = rand_id("C")
    country = random.choice(countries)
    dob     = rand_date(datetime(1960,1,1), datetime(2000,1,1))
    customers.append({
        "customer_id": cid,
        "full_name": f"Customer {cid}",
        "id_number": rand_id("ID"),
        "date_of_birth": dob,
        "age": 2024 - int(dob[:4]),
        "country": country,
        "city": random.choice(cities[country]),
        "account_status": random.choices(["active","suspended","closed"],[0.85,0.1,0.05])[0],
        "employment_status": random.choice(employment),
        "stated_income": round(random.uniform(5000, 150000), 2)
    })

with open("data/sample/customers.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=customers[0].keys())
    w.writeheader(); w.writerows(customers)
print("customers.csv done — 500 rows")

# ── 2. card_accounts.csv ─────────────────────────────────────
card_types  = ["Visa","Mastercard"]
card_tiers  = ["Standard","Gold","Platinum"]
cust_ids    = [c["customer_id"] for c in customers]

accounts = []
for cid in cust_ids:
    aid = rand_id("ACC")
    accounts.append({
        "account_id": aid,
        "customer_id": cid,
        "card_number_masked": "****" + str(random.randint(1000,9999)),
        "card_type": random.choice(card_types),
        "card_tier": random.choice(card_tiers),
        "account_open_date": rand_date(datetime(2015,1,1), datetime(2023,1,1)),
        "account_status": random.choices(["active","suspended","closed"],[0.85,0.1,0.05])[0],
        "credit_limit": round(random.uniform(5000,100000),2),
        "billing_country": next(c["country"] for c in customers if c["customer_id"]==cid),
        "billing_city": next(c["city"] for c in customers if c["customer_id"]==cid)
    })

with open("data/sample/card_accounts.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=accounts[0].keys())
    w.writeheader(); w.writerows(accounts)
print("card_accounts.csv done — 500 rows")

# ── 3. card_transactions.csv ─────────────────────────────────
merchants   = ["ShopRite","PnP","Woolworths","Amazon","Uber","Netflix",
               "Shell","BP","KFC","Steers","Unknown Merchant","FX Store"]
mcc_codes   = ["5411","5912","5999","7011","4121","7832","5541","5812","5814","6011"]
channels    = ["POS","ATM","online","contactless"]
txn_types   = ["purchase","withdrawal","refund"]
countries_t = ["ZA","KE","GH","NG","UG","US","GB","CN","AE"]

transactions = []
acc_map = {a["customer_id"]: a["account_id"] for a in accounts}

for _ in range(5000):
    cid = random.choice(cust_ids)
    aid = acc_map[cid]
    ts  = rand_ts(START, END)
    is_fraud = random.random() < 0.08
    transactions.append({
        "transaction_id": rand_id("TXN"),
        "account_id": aid,
        "customer_id": cid,
        "transaction_date": ts[:10],
        "transaction_time": ts[11:],
        "amount": round(random.uniform(500,80000),2) if is_fraud else round(random.uniform(10,5000),2),
        "currency": "ZAR",
        "merchant_id": rand_id("MER"),
        "merchant_name": random.choice(merchants),
        "merchant_category_code": random.choice(mcc_codes),
        "merchant_country": random.choice(countries_t) if is_fraud else random.choices(countries_t[:5],[0.6,0.1,0.1,0.1,0.1])[0],
        "merchant_city": "Unknown" if is_fraud else "Local",
        "transaction_type": random.choice(txn_types),
        "channel": random.choice(channels),
        "device_id": rand_id("DEV") if is_fraud else rand_id("DEV"),
        "ip_address": f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}",
        "status": random.choices(["approved","declined","pending"],[0.75,0.2,0.05])[0] if is_fraud else "approved",
        "decline_reason": random.choice(["insufficient_funds","fraud_suspected","limit_exceeded",""]) if is_fraud else "",
        "is_fraud_label": 1 if is_fraud else 0
    })

with open("data/sample/card_transactions.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=transactions[0].keys())
    w.writeheader(); w.writerows(transactions)
print("card_transactions.csv done — 5000 rows")

# ── 4. loan_applications.csv ─────────────────────────────────
loan_types  = ["personal","mortgage","vehicle","business"]
purposes    = ["home","education","debt_consolidation","business","vehicle","medical"]
id_types    = ["national_id","passport","drivers_license"]
res_status  = ["owner","renting","living_with_family"]
channels_l  = ["branch","online","mobile","agent"]
app_status  = ["approved","declined","pending","disbursed"]

agents      = [rand_id("AGT") for _ in range(20)]
loan_apps   = []

for _ in range(1000):
    cid      = random.choice(cust_ids)
    cust     = next(c for c in customers if c["customer_id"]==cid)
    is_fraud = random.random() < 0.1
    income   = cust["stated_income"]
    amount   = round(random.uniform(income*5, income*20),2) if is_fraud else round(random.uniform(1000, income*5),2)
    loan_apps.append({
        "application_id": rand_id("APP"),
        "customer_id": cid,
        "application_date": rand_date(START, END),
        "loan_type": random.choice(loan_types),
        "loan_amount_requested": amount,
        "loan_term_months": random.choice([12,24,36,48,60]),
        "purpose": random.choice(purposes),
        "stated_income": round(income * random.uniform(1.5,3.0),2) if is_fraud else income,
        "stated_employment_status": random.choice(["employed","self-employed"]) if is_fraud else cust["employment_status"],
        "stated_employer": rand_id("EMP"),
        "stated_years_employed": random.randint(0,2) if is_fraud else random.randint(1,20),
        "id_number": cust["id_number"],
        "id_type": random.choice(id_types),
        "date_of_birth": cust["date_of_birth"],
        "marital_status": random.choice(["single","married","divorced"]),
        "num_dependants": random.randint(0,6),
        "residential_status": random.choice(res_status),
        "address": f"{random.randint(1,999)} Main St, {cust['city']}",
        "branch_id": rand_id("BRN"),
        "agent_id": random.choice(agents),
        "application_channel": random.choice(channels_l),
        "application_status": random.choice(app_status),
        "is_fraud_label": 1 if is_fraud else 0
    })

with open("data/sample/loan_applications.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=loan_apps[0].keys())
    w.writeheader(); w.writerows(loan_apps)
print("loan_applications.csv done — 1000 rows")

# ── 5. loan_repayments.csv ───────────────────────────────────
pay_methods = ["bank_transfer","cash","mobile_money","debit_order"]
pay_status  = ["paid","missed","partial","late"]
repayments  = []

for app in loan_apps[:800]:
    num_payments = random.randint(3,12)
    is_bad       = app["is_fraud_label"] == 1
    due_date     = datetime.strptime(app["application_date"],"%Y-%m-%d") + timedelta(days=30)
    for i in range(num_payments):
        due_amt  = round(app["loan_amount_requested"] / app["loan_term_months"], 2)
        status   = random.choices(["missed","partial","late"],[0.4,0.3,0.3])[0] if is_bad else random.choices(pay_status,[0.8,0.05,0.1,0.05])[0]
        paid_amt = 0 if status=="missed" else round(due_amt*random.uniform(0.3,0.9),2) if status=="partial" else due_amt
        dpd      = random.randint(5,90) if status in ["missed","late"] else 0
        repayments.append({
            "repayment_id": rand_id("REP"),
            "application_id": app["application_id"],
            "customer_id": app["customer_id"],
            "due_date": due_date.strftime("%Y-%m-%d"),
            "payment_date": (due_date + timedelta(days=dpd)).strftime("%Y-%m-%d") if status != "missed" else "",
            "due_amount": due_amt,
            "paid_amount": paid_amt,
            "payment_method": random.choice(pay_methods),
            "payment_status": status,
            "days_past_due": dpd,
            "outstanding_balance": round(app["loan_amount_requested"] - (due_amt * i), 2)
        })
        due_date += timedelta(days=30)

with open("data/sample/loan_repayments.csv","w",newline="") as f:
    w = csv.DictWriter(f, fieldnames=repayments[0].keys())
    w.writeheader(); w.writerows(repayments)
print("loan_repayments.csv done — " + str(len(repayments)) + " rows")

print("")
print("ALL 5 CSV FILES GENERATED SUCCESSFULLY!")
print("Location: data/sample/")
"@

Set-Content -Path "generate_sample_data.py" -Value $script
Write-Host "Script ready! Now running it..." -ForegroundColor Yellow
python generate_sample_data.py