import csv
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)

def rand_date(s, e):
    return (s + timedelta(days=random.randint(0, (e - s).days))).strftime("%Y-%m-%d")

def rand_id(p=""):
    return p + str(uuid.uuid4())[:8].upper()

START = datetime(2023, 1, 1)
END   = datetime(2024, 12, 31)

countries = ["ZA", "KE", "GH", "NG", "UG"]
cities = {
    "ZA": ["Johannesburg", "Cape Town"],
    "KE": ["Nairobi", "Mombasa"],
    "GH": ["Accra", "Kumasi"],
    "NG": ["Lagos", "Abuja"],
    "UG": ["Kampala", "Entebbe"]
}

# ── 1. customers.csv ─────────────────────────────────────────
customers = []
for _ in range(500):
    cid     = rand_id("C")
    country = random.choice(countries)
    dob     = rand_date(datetime(1960, 1, 1), datetime(2000, 1, 1))
    customers.append({
        "customer_id":       cid,
        "full_name":         "Customer " + cid,
        "id_number":         rand_id("ID"),
        "date_of_birth":     dob,
        "age":               2024 - int(dob[:4]),
        "country":           country,
        "city":              random.choice(cities[country]),
        "account_status":    random.choices(["active","suspended","closed"], [85,10,5])[0],
        "employment_status": random.choice(["employed","self-employed","unemployed"]),
        "stated_income":     round(random.uniform(5000, 150000), 2)
    })

with open("data/sample/customers.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=customers[0].keys())
    w.writeheader()
    w.writerows(customers)
print("customers.csv done - 500 rows")

# ── 2. card_accounts.csv ─────────────────────────────────────
accounts = []
for c in customers:
    accounts.append({
        "account_id":        rand_id("ACC"),
        "customer_id":       c["customer_id"],
        "card_number_masked":"****" + str(random.randint(1000, 9999)),
        "card_type":         random.choice(["Visa", "Mastercard"]),
        "card_tier":         random.choice(["Standard", "Gold", "Platinum"]),
        "account_open_date": rand_date(datetime(2015,1,1), datetime(2023,1,1)),
        "account_status":    c["account_status"],
        "credit_limit":      round(random.uniform(5000, 100000), 2),
        "billing_country":   c["country"],
        "billing_city":      c["city"]
    })

with open("data/sample/card_accounts.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=accounts[0].keys())
    w.writeheader()
    w.writerows(accounts)
print("card_accounts.csv done - 500 rows")

# ── 3. card_transactions.csv ─────────────────────────────────
cust_ids = [c["customer_id"] for c in customers]
acc_map  = {a["customer_id"]: a["account_id"] for a in accounts}
txns     = []

for _ in range(5000):
    cid      = random.choice(cust_ids)
    is_fraud = random.random() < 0.08
    secs     = random.randint(0, int((END - START).total_seconds()))
    ts       = (START + timedelta(seconds=secs)).strftime("%Y-%m-%d %H:%M:%S")
    txns.append({
        "transaction_id":        rand_id("TXN"),
        "account_id":            acc_map[cid],
        "customer_id":           cid,
        "transaction_date":      ts[:10],
        "transaction_time":      ts[11:],
        "amount":                round(random.uniform(500,80000),2) if is_fraud else round(random.uniform(10,5000),2),
        "currency":              "ZAR",
        "merchant_id":           rand_id("MER"),
        "merchant_name":         random.choice(["ShopRite","PnP","Amazon","Uber","Shell","KFC","Unknown"]),
        "merchant_category_code":random.choice(["5411","5912","5999","4121","5541"]),
        "merchant_country":      random.choice(["US","GB","CN","AE"]) if is_fraud else "ZA",
        "merchant_city":         "Unknown" if is_fraud else "Local",
        "transaction_type":      random.choice(["purchase","withdrawal","refund"]),
        "channel":               random.choice(["POS","ATM","online","contactless"]),
        "device_id":             rand_id("DEV"),
        "ip_address":            "192.168.1.1",
        "status":                random.choices(["approved","declined","pending"],[75,20,5])[0] if is_fraud else "approved",
        "decline_reason":        "fraud_suspected" if is_fraud else "",
        "is_fraud_label":        1 if is_fraud else 0
    })

with open("data/sample/card_transactions.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=txns[0].keys())
    w.writeheader()
    w.writerows(txns)
print("card_transactions.csv done - 5000 rows")

# ── 4. loan_applications.csv ─────────────────────────────────
agents = [rand_id("AGT") for _ in range(20)]
apps   = []

for _ in range(1000):
    cid      = random.choice(cust_ids)
    cust     = next(c for c in customers if c["customer_id"] == cid)
    is_fraud = random.random() < 0.1
    income   = cust["stated_income"]
    apps.append({
        "application_id":          rand_id("APP"),
        "customer_id":             cid,
        "application_date":        rand_date(START, END),
        "loan_type":               random.choice(["personal","mortgage","vehicle","business"]),
        "loan_amount_requested":   round(random.uniform(income*5, income*20),2) if is_fraud else round(random.uniform(1000, income*5),2),
        "loan_term_months":        random.choice([12,24,36,48,60]),
        "purpose":                 random.choice(["home","education","debt","business"]),
        "stated_income":           round(income*random.uniform(1.5,3),2) if is_fraud else income,
        "stated_employment_status":"employed",
        "stated_employer":         rand_id("EMP"),
        "stated_years_employed":   random.randint(0,2) if is_fraud else random.randint(1,20),
        "id_number":               cust["id_number"],
        "id_type":                 random.choice(["national_id","passport"]),
        "date_of_birth":           cust["date_of_birth"],
        "marital_status":          random.choice(["single","married","divorced"]),
        "num_dependants":          random.randint(0,6),
        "residential_status":      random.choice(["owner","renting","living_with_family"]),
        "address":                 str(random.randint(1,999)) + " Main St",
        "branch_id":               rand_id("BRN"),
        "agent_id":                random.choice(agents),
        "application_channel":     random.choice(["branch","online","mobile","agent"]),
        "application_status":      random.choice(["approved","declined","pending","disbursed"]),
        "is_fraud_label":          1 if is_fraud else 0
    })

with open("data/sample/loan_applications.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=apps[0].keys())
    w.writeheader()
    w.writerows(apps)
print("loan_applications.csv done - 1000 rows")

# ── 5. loan_repayments.csv ───────────────────────────────────
reps = []

for app in apps[:800]:
    is_bad  = app["is_fraud_label"] == 1
    due     = datetime.strptime(app["application_date"], "%Y-%m-%d") + timedelta(days=30)
    for i in range(random.randint(3, 12)):
        due_amt = round(app["loan_amount_requested"] / app["loan_term_months"], 2)
        status  = random.choices(["missed","partial","late"],[40,30,30])[0] if is_bad else random.choices(["paid","missed","partial","late"],[80,5,10,5])[0]
        paid    = 0 if status == "missed" else round(due_amt*random.uniform(0.3,0.9),2) if status == "partial" else due_amt
        dpd     = random.randint(5,90) if status in ["missed","late"] else 0
        reps.append({
            "repayment_id":       rand_id("REP"),
            "application_id":     app["application_id"],
            "customer_id":        app["customer_id"],
            "due_date":           due.strftime("%Y-%m-%d"),
            "payment_date":       (due + timedelta(days=dpd)).strftime("%Y-%m-%d") if status != "missed" else "",
            "due_amount":         due_amt,
            "paid_amount":        paid,
            "payment_method":     random.choice(["bank_transfer","cash","mobile_money"]),
            "payment_status":     status,
            "days_past_due":      dpd,
            "outstanding_balance":round(app["loan_amount_requested"] - (due_amt * i), 2)
        })
        due += timedelta(days=30)

with open("data/sample/loan_repayments.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=reps[0].keys())
    w.writeheader()
    w.writerows(reps)
print("loan_repayments.csv done -", len(reps), "rows")
print("")
print("ALL 5 FILES GENERATED SUCCESSFULLY!")