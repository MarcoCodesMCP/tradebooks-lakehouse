# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 · Generate Source Data (pandas)
# MAGIC Simulates raw exports from the bookkeeping systems of **trade-contractor clients**
# MAGIC (plumbing, electrical, HVAC, roofing). Data is intentionally messy — duplicates,
# MAGIC inconsistent casing, bad dates, negative amounts, missing keys — so the Silver layer
# MAGIC has real cleaning work to do.
# MAGIC
# MAGIC Output: CSV / JSON files landed in a Unity Catalog Volume (`/Volumes/<catalog>/<schema>/landing/`).

# COMMAND ----------

import os
import json
import numpy as np
import pandas as pd
from datetime import date, timedelta

try:
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("schema", "tradebooks")
    CATALOG = dbutils.widgets.get("catalog")
    SCHEMA = dbutils.widgets.get("schema")
    LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
except NameError:  # running locally
    LANDING = os.environ.get("LANDING", "./landing")

SEED = 42
rng = np.random.default_rng(SEED)
START, END = date(2025, 1, 1), date(2026, 9, 30)

# COMMAND ----------

# MAGIC %md ## Clients (the contractors the bookkeeping firm serves)

# COMMAND ----------

clients = pd.DataFrame([
    ("C001", "Shaik Plumbing & Heating", "Plumbing",   "South Windsor, CT", 6,  95),
    ("C002", "Bright Line Electric",     "Electrical", "Manchester, CT",    9,  110),
    ("C003", "NorthEast Air HVAC",       "HVAC",       "Hartford, CT",      12, 105),
    ("C004", "Summit Roofing Co",        "Roofing",    "Glastonbury, CT",   15, 85),
    ("C005", "Keystone Carpentry",       "Carpentry",  "Vernon, CT",        5,  80),
], columns=["client_id", "client_name", "trade", "city", "num_techs", "labor_rate"])

JOB_TYPES = {
    "Plumbing":   [("Service Call", 350, 0.35), ("Water Heater Install", 2400, 0.55), ("Repipe", 9000, 0.5)],
    "Electrical": [("Service Call", 300, 0.30), ("Panel Upgrade", 3200, 0.45), ("EV Charger Install", 1800, 0.5)],
    "HVAC":       [("Maintenance", 250, 0.25), ("AC Install", 7500, 0.6), ("Furnace Replace", 6000, 0.55)],
    "Roofing":    [("Repair", 900, 0.4), ("Full Replacement", 14000, 0.65), ("Inspection", 200, 0.1)],
    "Carpentry":  [("Deck Build", 8000, 0.55), ("Trim Work", 1500, 0.4), ("Framing", 5000, 0.5)],
}
# Seasonality multiplier by month per trade (HVAC peaks summer/winter, roofing peaks summer, etc.)
SEASON = {
    "Plumbing":   [1.2, 1.2, 1.0, 0.9, 0.9, 0.9, 0.9, 0.9, 1.0, 1.0, 1.1, 1.2],
    "Electrical": [0.9, 0.9, 1.0, 1.0, 1.1, 1.1, 1.1, 1.1, 1.0, 1.0, 0.9, 0.9],
    "HVAC":       [1.3, 1.2, 0.8, 0.7, 1.0, 1.5, 1.6, 1.5, 0.9, 0.8, 1.0, 1.3],
    "Roofing":    [0.4, 0.4, 0.8, 1.1, 1.4, 1.5, 1.5, 1.4, 1.3, 1.1, 0.7, 0.4],
    "Carpentry":  [0.6, 0.6, 0.9, 1.1, 1.3, 1.4, 1.4, 1.3, 1.2, 1.0, 0.8, 0.6],
}

# COMMAND ----------

# MAGIC %md ## Customers, Jobs, Invoices, Payments, Expenses, Timesheets

# COMMAND ----------

def daterange(start, end):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)

customers, jobs, invoices, payments, expenses, timesheets = [], [], [], [], [], []
cust_n = job_n = inv_n = pay_n = exp_n = ts_n = 0
FIRST = ["James", "Maria", "Robert", "Priya", "Kevin", "Aisha", "Daniel", "Lena", "Omar", "Grace"]
LAST = ["Smith", "Patel", "Johnson", "Garcia", "Nguyen", "Brown", "Khan", "Lopez", "Miller", "Davis"]
TOWNS = ["Hartford", "West Hartford", "Glastonbury", "Manchester", "Vernon", "Enfield", "Windsor", "Bolton"]

for c in clients.itertuples():
    # customer book per client
    n_cust = int(rng.integers(120, 260))
    cust_ids = []
    for _ in range(n_cust):
        cust_n += 1
        cid = f"CU{cust_n:05d}"
        cust_ids.append(cid)
        ctype = rng.choice(["Residential", "Commercial", "Property Mgmt"], p=[0.7, 0.2, 0.1])
        customers.append({
            "customer_id": cid, "client_id": c.client_id,
            "customer_name": f"{rng.choice(FIRST)} {rng.choice(LAST)}" if ctype == "Residential"
                             else f"{rng.choice(LAST)} {rng.choice(['LLC', 'Properties', 'Group'])}",
            "customer_type": ctype, "town": rng.choice(TOWNS),
            # slow payers drive AR risk analytics
            "_pay_delay_mu": float(rng.choice([8, 20, 45], p=[0.6, 0.3, 0.1])),
        })

    techs = [f"{c.client_id}-T{i:02d}" for i in range(1, c.num_techs + 1)]

    for d in daterange(START, END):
        if d.weekday() == 6:
            continue
        lam = c.num_techs * 0.28 * SEASON[c.trade][d.month - 1] * (0.4 if d.weekday() == 5 else 1)
        for _ in range(rng.poisson(lam)):
            job_n += 1
            jtype, base_price, mat_pct = JOB_TYPES[c.trade][rng.integers(0, 3)]
            cust = rng.choice(cust_ids)
            ctype = next(x["customer_type"] for x in customers[-n_cust:] if x["customer_id"] == cust)
            price = round(float(base_price * rng.lognormal(0, 0.25) * (1.25 if ctype == "Commercial" else 1)), 2)
            hours = max(1.0, round(price * (1 - mat_pct) / c.labor_rate * rng.uniform(0.7, 1.3), 1))
            job_id = f"J{job_n:06d}"
            status = "Completed" if d < END - timedelta(days=10) else rng.choice(["Completed", "In Progress"])
            jobs.append({
                "job_id": job_id, "client_id": c.client_id, "customer_id": cust,
                "job_type": jtype, "scheduled_date": d.isoformat(), "status": status,
                "quoted_amount": price, "lead_source": rng.choice(
                    ["Google", "Referral", "Angi", "Repeat", "Facebook"], p=[0.35, 0.25, 0.15, 0.15, 0.1]),
            })

            # timesheets (labor)
            tech = rng.choice(techs)
            ts_n += 1
            timesheets.append({"timesheet_id": f"TS{ts_n:07d}", "client_id": c.client_id, "job_id": job_id,
                               "tech_id": tech, "work_date": d.isoformat(), "hours": hours,
                               "hourly_cost": round(c.labor_rate * 0.42, 2)})

            # expenses (materials + occasional sub / permit)
            exp_n += 1
            mat_cost = round(price * mat_pct * rng.uniform(0.75, 1.15), 2)
            expenses.append({"expense_id": f"E{exp_n:07d}", "client_id": c.client_id, "job_id": job_id,
                             "expense_date": d.isoformat(), "category": rng.choice(["Materials", "materials", "MATERIALS "]),
                             "vendor": rng.choice(["Ferguson", "Home Depot", "Grainger", "Supply House", "Lowe's"]),
                             "amount": mat_cost})
            if rng.random() < 0.12:
                exp_n += 1
                expenses.append({"expense_id": f"E{exp_n:07d}", "client_id": c.client_id, "job_id": job_id,
                                 "expense_date": d.isoformat(), "category": rng.choice(["Subcontractor", "Permits"]),
                                 "vendor": "Various", "amount": round(price * rng.uniform(0.05, 0.2), 2)})

            # invoice + payment
            if status == "Completed":
                inv_n += 1
                inv_id = f"INV{inv_n:07d}"
                inv_date = min(d + timedelta(days=int(rng.integers(0, 4))), END)
                invoices.append({"invoice_id": inv_id, "client_id": c.client_id, "job_id": job_id,
                                 "customer_id": cust, "invoice_date": inv_date.isoformat(),
                                 "due_date": (inv_date + timedelta(days=30)).isoformat(),
                                 "amount": price, "tax": round(price * 0.0635, 2)})
                mu = next(x["_pay_delay_mu"] for x in customers[-n_cust:] if x["customer_id"] == cust)
                if rng.random() < 0.97:  # 3% never paid (bad debt / still open)
                    pay_date = inv_date + timedelta(days=int(rng.exponential(mu)))
                    if pay_date <= END:
                        pay_n += 1
                        payments.append({"payment_id": f"P{pay_n:07d}", "client_id": c.client_id,
                                         "invoice_id": inv_id, "payment_date": pay_date.isoformat(),
                                         "amount": round(price * 1.0635, 2),
                                         "method": rng.choice(["Card", "Check", "ACH", "Cash"], p=[0.4, 0.3, 0.2, 0.1])})

    # overhead expenses (not tied to a job)
    for m in pd.date_range(START, END, freq="MS"):
        for cat, amt in [("Fuel", 180 * c.num_techs), ("Insurance", 900 + 60 * c.num_techs),
                         ("Vehicle Lease", 650 * max(1, c.num_techs // 2)), ("Software", 250), ("Rent", 2200)]:
            exp_n += 1
            expenses.append({"expense_id": f"E{exp_n:07d}", "client_id": c.client_id, "job_id": None,
                             "expense_date": m.date().isoformat(), "category": cat, "vendor": "Overhead",
                             "amount": round(amt * rng.uniform(0.9, 1.1), 2)})

customers_df = pd.DataFrame(customers).drop(columns=["_pay_delay_mu"])
jobs_df, invoices_df = pd.DataFrame(jobs), pd.DataFrame(invoices)
payments_df, expenses_df, timesheets_df = pd.DataFrame(payments), pd.DataFrame(expenses), pd.DataFrame(timesheets)

# COMMAND ----------

# MAGIC %md ## Inject real-world mess (so Silver has work to do)

# COMMAND ----------

def inject_mess(df, frac_dupes=0.02, date_col=None, amt_col=None):
    df = pd.concat([df, df.sample(frac=frac_dupes, random_state=SEED)], ignore_index=True)  # duplicate rows
    if date_col:  # some dates in MM/DD/YYYY, some blank
        idx = df.sample(frac=0.03, random_state=1).index
        df.loc[idx, date_col] = pd.to_datetime(df.loc[idx, date_col]).dt.strftime("%m/%d/%Y")
        df.loc[df.sample(frac=0.003, random_state=2).index, date_col] = None
    if amt_col:  # sign errors and string amounts
        df.loc[df.sample(frac=0.005, random_state=3).index, amt_col] *= -1
        df[amt_col] = df[amt_col].astype(object)
        idx = df.sample(frac=0.01, random_state=4).index
        df.loc[idx, amt_col] = df.loc[idx, amt_col].map(lambda v: f"${v:,.2f}" if pd.notna(v) else v)
    return df.sample(frac=1, random_state=SEED).reset_index(drop=True)

invoices_df = inject_mess(invoices_df, date_col="invoice_date", amt_col="amount")
payments_df = inject_mess(payments_df, date_col="payment_date", amt_col="amount")
expenses_df = inject_mess(expenses_df, date_col="expense_date", amt_col="amount")
jobs_df = inject_mess(jobs_df, date_col="scheduled_date")
timesheets_df = inject_mess(timesheets_df, frac_dupes=0.01)
jobs_df.loc[jobs_df.sample(frac=0.002, random_state=5).index, "customer_id"] = None  # orphan keys

# COMMAND ----------

# MAGIC %md ## Land files — one folder per entity, split into monthly batches (simulates daily/monthly drops)

# COMMAND ----------

def land(df, entity, date_col, fmt="csv"):
    path = f"{LANDING}/{entity}"
    os.makedirs(path, exist_ok=True)
    month = pd.to_datetime(df[date_col], errors="coerce", format="mixed").dt.strftime("%Y_%m").fillna("unknown")
    for m, part in df.groupby(month):
        f = f"{path}/{entity}_{m}.{fmt}"
        if fmt == "csv":
            part.to_csv(f, index=False)
        else:
            part.to_json(f, orient="records", lines=True)
    return len(df)

for entity, df in [("clients", clients), ("customers", customers_df)]:
    os.makedirs(f"{LANDING}/{entity}", exist_ok=True)
    df.to_csv(f"{LANDING}/{entity}/{entity}.csv", index=False)

counts = {
    "clients": len(clients),
    "customers": len(customers_df),
    "jobs": land(jobs_df, "jobs", "scheduled_date"),
    "invoices": land(invoices_df, "invoices", "invoice_date"),
    "payments": land(payments_df, "payments", "payment_date", fmt="json"),  # payments arrive as JSON from the processor API
    "expenses": land(expenses_df, "expenses", "expense_date"),
    "timesheets": land(timesheets_df, "timesheets", "work_date"),
}
print(json.dumps(counts, indent=2))