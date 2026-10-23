# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Analytics & Insights (pandas)
# MAGIC Gold tables are small and aggregated, so we pull them into **pandas** for analysis.
# MAGIC
# MAGIC 1. Executive scorecard
# MAGIC 2. Job-type margin analysis → **pricing recommendations**
# MAGIC 3. Seasonality index by trade
# MAGIC 4. 3-month **revenue forecast** (trend × seasonality) with backtest error (MAPE)
# MAGIC 5. **RFM customer segmentation**
# MAGIC 6. **Collections risk** ranking
# MAGIC
# MAGIC Results are written back to Delta as `insight_*` tables for the dashboard.

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

TARGET_MARGIN = 40.0  # % gross margin the bookkeeping firm recommends to clients

try:
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("schema", "tradebooks")
    CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
    load = lambda name: spark.table(f"{CATALOG}.{SCHEMA}.{name}").toPandas()
    def publish(pdf, name):
        spark.createDataFrame(pdf).write.mode("overwrite").option("overwriteSchema", "true") \
             .saveAsTable(f"{CATALOG}.{SCHEMA}.{name}")
    ON_DATABRICKS = True
except NameError:                       # local run: gold tables exported as pickles
    import os
    GOLD = os.environ.get("GOLD_DIR", "./gold")
    load = lambda name: pd.read_pickle(f"{GOLD}/{name}.pkl")
    publish = lambda pdf, name: pdf.to_csv(f"{GOLD}/{name}.csv", index=False)
    display = print
    ON_DATABRICKS = False

jobs = load("gold_job_profitability")
pnl = load("gold_monthly_pnl")
ar = load("gold_ar_aging")
kpis = load("gold_client_kpis")

# Spark DECIMAL -> pandas object; convert to float for math
from decimal import Decimal
for df in (jobs, pnl, ar, kpis):
    for c in df.columns:
        vals = df[c].dropna()
        if df[c].dtype == object and len(vals) and vals.map(lambda v: isinstance(v, Decimal)).all():
            df[c] = df[c].astype(float)
pnl["month"] = pd.to_datetime(pnl["month"])
jobs["scheduled_date"] = pd.to_datetime(jobs["scheduled_date"])

# COMMAND ----------

# MAGIC %md ## 1 · Executive scorecard

# COMMAND ----------

scorecard = kpis[["client_name", "trade", "num_techs", "revenue_ttm", "net_profit_ttm", "net_margin_pct_ttm",
                  "avg_ticket", "avg_job_margin_pct", "dso_days", "ar_over_60", "repeat_customer_pct",
                  "revenue_per_tech"]].sort_values("revenue_ttm", ascending=False)

# Simple 0–100 health score: profitability 40%, collections 30%, loyalty 30%
def scale(s, invert=False):
    r = (s - s.min()) / (s.max() - s.min() + 1e-9)
    return 1 - r if invert else r
scorecard["health_score"] = (100 * (0.4 * scale(scorecard["net_margin_pct_ttm"])
                                    + 0.3 * scale(scorecard["dso_days"], invert=True)
                                    + 0.3 * scale(scorecard["repeat_customer_pct"]))).round(0)
display(scorecard)
publish(scorecard, "insight_client_scorecard")

# COMMAND ----------

# MAGIC %md ## 2 · Margin by job type → pricing recommendations
# MAGIC For each job type below the target margin, the price increase needed to hit it:
# MAGIC `required_price = cost / (1 - target)` → `increase % = required_price / avg_price - 1`.

# COMMAND ----------

jt = (jobs.groupby(["client_name", "trade", "job_type"])
      .agg(jobs=("job_id", "count"), revenue=("revenue", "sum"), cost=("total_cost", "sum"),
           avg_price=("revenue", "mean"), avg_cost=("total_cost", "mean"))
      .reset_index())
jt["gross_margin_pct"] = ((jt["revenue"] - jt["cost"]) / jt["revenue"] * 100).round(1)
jt["required_price"] = (jt["avg_cost"] / (1 - TARGET_MARGIN / 100)).round(2)
jt["price_increase_pct"] = ((jt["required_price"] / jt["avg_price"] - 1) * 100).clip(lower=0).round(1)
jt["annualized_profit_uplift"] = ((jt["required_price"] - jt["avg_price"]).clip(lower=0) * jt["jobs"] * 12 / 21).round(0)
jt["action"] = np.where(jt["gross_margin_pct"] < TARGET_MARGIN, "Raise price / cut cost", "On target")

pricing = jt.sort_values("annualized_profit_uplift", ascending=False)
display(pricing)
publish(pricing, "insight_pricing")

fig, ax = plt.subplots(figsize=(10, 5))
piv = jt.pivot_table(index="job_type", columns="trade", values="gross_margin_pct")
piv.mean(axis=1).sort_values().plot.barh(ax=ax, color="#4C78A8")
ax.axvline(TARGET_MARGIN, color="#E45756", ls="--", label=f"Target {TARGET_MARGIN:.0f}%")
ax.set_xlabel("Gross margin %"); ax.set_title("Gross margin by job type"); ax.legend()
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md ## 3 · Seasonality index by trade
# MAGIC Index = average revenue in that calendar month ÷ average monthly revenue. 1.30 = 30% busier than normal.

# COMMAND ----------

pnl["cal_month"] = pnl["month"].dt.month
season = (pnl.groupby(["trade", "cal_month"])["revenue"].mean()
          / pnl.groupby("trade")["revenue"].mean()).rename("seasonality_index").reset_index()
season_wide = season.pivot(index="cal_month", columns="trade", values="seasonality_index").round(2)
display(season_wide)
publish(season, "insight_seasonality")

ax = season_wide.plot(figsize=(10, 5), marker="o", title="Seasonality index by trade (1.0 = average month)")
ax.axhline(1, color="grey", lw=0.8); ax.set_xticks(range(1, 13)); plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md ## 4 · 3-month revenue forecast
# MAGIC Model: **deseasonalize → linear trend → reseasonalize**. Backtested on the last 3 actual months.

# COMMAND ----------

def forecast_client(df: pd.DataFrame, horizon: int = 3):
    df = df.sort_values("month").reset_index(drop=True)
    idx = df.groupby("cal_month")["revenue"].mean() / df["revenue"].mean()
    df["deseason"] = df["revenue"] / df["cal_month"].map(idx)
    t = np.arange(len(df))
    slope, intercept = np.polyfit(t, df["deseason"], 1)
    future_months = pd.date_range(df["month"].max() + pd.offsets.MonthBegin(1), periods=horizon, freq="MS")
    tf = np.arange(len(df), len(df) + horizon)
    fc = (intercept + slope * tf) * future_months.month.map(idx).values
    return pd.DataFrame({"month": future_months, "forecast_revenue": fc.round(0)}), slope

def backtest_mape(df: pd.DataFrame, holdout: int = 3):
    df = df.sort_values("month")
    train, test = df.iloc[:-holdout].copy(), df.iloc[-holdout:]
    fc, _ = forecast_client(train, holdout)
    return float((abs(fc["forecast_revenue"].values - test["revenue"].values) / test["revenue"].values).mean() * 100)

rows = []
for (cid, name), g in pnl.groupby(["client_id", "client_name"]):
    fc, slope = forecast_client(g)
    mape = backtest_mape(g)
    fc["client_id"], fc["client_name"] = cid, name
    fc["monthly_trend_$"] = round(slope, 0)
    fc["backtest_mape_pct"] = round(mape, 1)
    rows.append(fc)
forecast = pd.concat(rows, ignore_index=True)
display(forecast)
publish(forecast, "insight_revenue_forecast")

fig, ax = plt.subplots(figsize=(11, 5))
for name, g in pnl.groupby("client_name"):
    line, = ax.plot(g["month"], g["revenue"] / 1000, label=name)
    f = forecast[forecast.client_name == name]
    ax.plot(f["month"], f["forecast_revenue"] / 1000, ls="--", color=line.get_color())
ax.set_ylabel("Revenue ($K)"); ax.set_title("Monthly revenue — actual (solid) vs forecast (dashed)")
ax.legend(fontsize=8); plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md ## 5 · RFM customer segmentation
# MAGIC **R**ecency (days since last job), **F**requency (# jobs), **M**onetary (total revenue) — scored 1–4 by quartile.

# COMMAND ----------

as_of = jobs["scheduled_date"].max()
rfm = (jobs.groupby(["client_name", "customer_id", "customer_type"])
       .agg(last_job=("scheduled_date", "max"), frequency=("job_id", "count"), monetary=("revenue", "sum"))
       .reset_index())
rfm["recency_days"] = (as_of - rfm["last_job"]).dt.days
rfm["R"] = pd.qcut(rfm["recency_days"].rank(method="first"), 4, labels=[4, 3, 2, 1]).astype(int)
rfm["F"] = pd.qcut(rfm["frequency"].rank(method="first"), 4, labels=[1, 2, 3, 4]).astype(int)
rfm["M"] = pd.qcut(rfm["monetary"].rank(method="first"), 4, labels=[1, 2, 3, 4]).astype(int)

def segment(r):
    if r.R >= 3 and r.F >= 3 and r.M >= 3: return "Champions"
    if r.F >= 3 and r.R <= 2:              return "At Risk (was loyal)"
    if r.R >= 3 and r.F <= 2:              return "New / Promising"
    if r.M >= 4:                           return "Big Spenders"
    if r.R == 1:                           return "Lost"
    return "Needs Attention"
rfm["segment"] = rfm.apply(segment, axis=1)

seg_summary = (rfm.groupby("segment")
               .agg(customers=("customer_id", "count"), revenue=("monetary", "sum"), avg_jobs=("frequency", "mean"))
               .assign(revenue_share_pct=lambda d: (d.revenue / d.revenue.sum() * 100).round(1))
               .sort_values("revenue", ascending=False))
display(seg_summary)
publish(rfm.drop(columns=["last_job"]), "insight_customer_rfm")

# COMMAND ----------

# MAGIC %md ## 6 · Collections risk
# MAGIC Risk score weights how much is owed and how late it is, so the bookkeeper knows **who to call first**.

# COMMAND ----------

risk = (ar.groupby(["client_name", "customer_id", "customer_name", "customer_type"])
        .agg(open_invoices=("invoice_id", "count"), balance_due=("balance_due", "sum"),
             max_days_past_due=("days_past_due", "max"))
        .reset_index())
risk["risk_score"] = (100 * (0.5 * scale(np.log1p(risk["balance_due"]))
                             + 0.5 * scale(risk["max_days_past_due"].clip(upper=180)))).round(0)
risk["priority"] = pd.cut(risk["risk_score"], [-1, 40, 70, 100], labels=["Low", "Medium", "High"]).astype(str)
risk = risk.sort_values("risk_score", ascending=False)
display(risk.head(20))
publish(risk, "insight_collections_risk")

aging = ar.pivot_table(index="client_name", columns="aging_bucket", values="balance_due", aggfunc="sum", fill_value=0)
aging = aging.reindex(columns=[c for c in ["Current", "1-30", "31-60", "61-90", "90+"] if c in aging.columns])
aging.plot.barh(stacked=True, figsize=(10, 4), title="Open A/R by aging bucket ($)",
                color=["#54A24B", "#EECA3B", "#F58518", "#E45756", "#9D3D3D"][:len(aging.columns)])
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key findings (auto-generated)

# COMMAND ----------

top_price = pricing.iloc[0]
champ = seg_summary.loc["Champions"] if "Champions" in seg_summary.index else None
findings = [
    f"Total TTM revenue across {len(kpis)} contractor clients: ${kpis['revenue_ttm'].sum():,.0f} "
    f"at a {kpis['net_profit_ttm'].sum() / kpis['revenue_ttm'].sum() * 100:.1f}% net margin.",
    f"Biggest pricing gap: {top_price.client_name} – {top_price.job_type} runs at {top_price.gross_margin_pct:.1f}% margin; "
    f"a {top_price.price_increase_pct:.1f}% price increase adds ~${top_price.annualized_profit_uplift:,.0f}/yr.",
    f"{(pricing.action != 'On target').sum()} of {len(pricing)} client job types are below the {TARGET_MARGIN:.0f}% margin target.",
    f"Open A/R: ${ar['balance_due'].sum():,.0f}, of which ${ar.loc[ar.days_past_due > 60, 'balance_due'].sum():,.0f} is 60+ days late; "
    f"{(risk.priority == 'High').sum()} customers flagged high-priority for collections.",
]
if champ is not None:
    findings.append(f"'Champions' are {champ.customers / seg_summary.customers.sum() * 100:.0f}% of customers "
                    f"but {champ.revenue_share_pct:.0f}% of revenue.")
findings.append(f"Forecast backtest error (MAPE) averages {forecast.groupby('client_name')['backtest_mape_pct'].first().mean():.1f}%.")
for f in findings:
    print("•", f)
