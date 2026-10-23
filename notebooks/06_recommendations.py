# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Recommendations (pandas + optional AI wording)
# MAGIC Turns Gold + Insight tables into **3–5 plain-English insights per client, each with an action**,
# MAGIC written to `insight_recommendations` for the dashboard, Genie, and the monthly client report.
# MAGIC
# MAGIC **Design rule:** every number is calculated by code from the data — never by AI.
# MAGIC AI (optional) only rewrites the sentence; if the rewrite changes or drops any number, it is rejected.
# MAGIC
# MAGIC Every recommendation carries a **confidence** level, the **data it is based on**, and a disclaimer:
# MAGIC recommendations are estimates based on the data provided, not guarantees.

# COMMAND ----------

import re
import pandas as pd
from decimal import Decimal
from datetime import date

TARGET_MARGIN = 40.0
DISCLAIMER = ("Based on the data provided through the as-of date. This is an estimate, not a guarantee — "
              "review with your TradeBooks advisor before making changes.")

try:
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("schema", "tradebooks")
    dbutils.widgets.text("as_of_date", "2026-09-30")
    dbutils.widgets.dropdown("use_ai_wording", "false", ["false", "true"])
    dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")
    CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
    AS_OF = dbutils.widgets.get("as_of_date")
    USE_AI = dbutils.widgets.get("use_ai_wording") == "true"
    LLM = dbutils.widgets.get("llm_endpoint")
    load = lambda name: spark.table(f"{CATALOG}.{SCHEMA}.{name}").toPandas()
    def publish(pdf, name):
        spark.createDataFrame(pdf).write.mode("overwrite").option("overwriteSchema", "true") \
             .saveAsTable(f"{CATALOG}.{SCHEMA}.{name}")
except NameError:  # local test run
    import os
    GOLD = os.environ.get("GOLD_DIR", "./gold")
    AS_OF, USE_AI, LLM = "2026-09-30", False, ""
    def load(name):
        for ext, fn in ((".pkl", pd.read_pickle), (".csv", pd.read_csv)):
            if os.path.exists(f"{GOLD}/{name}{ext}"):
                return fn(f"{GOLD}/{name}{ext}")
        raise FileNotFoundError(name)
    publish = lambda pdf, name: pdf.to_csv(f"{GOLD}/{name}.csv", index=False)
    display = print

def to_float(df):
    for c in df.columns:
        vals = df[c].dropna()
        if df[c].dtype == object and len(vals) and vals.map(lambda v: isinstance(v, Decimal)).all():
            df[c] = df[c].astype(float)
    return df

kpis       = to_float(load("gold_client_kpis"))
pnl        = to_float(load("gold_monthly_pnl"))
pricing    = to_float(load("insight_pricing"))
forecast   = to_float(load("insight_revenue_forecast"))
rfm        = to_float(load("insight_customer_rfm"))
collect    = to_float(load("insight_collections_risk"))
lead       = to_float(load("gold_lead_source"))
pnl["month"] = pd.to_datetime(pnl["month"])
forecast["month"] = pd.to_datetime(forecast["month"])

money = lambda v: f"${v/1e6:.1f}M" if abs(v) >= 1e6 else f"${v/1e3:.1f}K" if abs(v) >= 1e3 else f"${v:,.0f}"
months_of_data = pnl["month"].nunique()

# COMMAND ----------

# MAGIC %md ## Rules — each returns (priority, category, insight, action, confidence, based_on) or None

# COMMAND ----------

def rule_pricing(client):
    p = pricing[(pricing.client_name == client) & (pricing.gross_margin_pct < TARGET_MARGIN)]
    if p.empty:
        return None
    r = p.sort_values("annualized_profit_uplift", ascending=False).iloc[0]
    conf = "High" if r.jobs >= 50 else "Medium" if r.jobs >= 20 else "Low"
    return (1, "Pricing",
            f"{r.job_type} jobs run at a {r.gross_margin_pct:.1f}% gross margin, below the {TARGET_MARGIN:.0f}% target.",
            f"Raise {r.job_type} prices about {r.price_increase_pct:.1f}% or cut its costs; "
            f"that could add about {money(r.annualized_profit_uplift)} in profit per year.",
            conf, f"{int(r.jobs)} completed {r.job_type} jobs over {months_of_data} months")

def rule_collections(client):
    k = kpis[kpis.client_name == client].iloc[0]
    c = collect[(collect.client_name == client) & (collect.priority == "High")].sort_values("risk_score", ascending=False)
    if k.ar_over_60 < 1000 or c.empty:
        return None
    top = c.head(3)
    names = ", ".join(top.customer_name.astype(str))
    return (2, "Collections",
            f"{money(k.ar_over_60)} is more than 60 days overdue, and it takes about {k.dso_days:.0f} days on average to get paid.",
            f"Call these customers first: {names} (together {money(top.balance_due.sum())} owed).",
            "High", f"Open invoices and payments as of {AS_OF}")

def rule_cash_season(client):
    f = forecast[forecast.client_name == client]
    hist = pnl[pnl.client_name == client].sort_values("month")
    if f.empty or len(hist) < 6:
        return None
    last3, next3 = hist.revenue.tail(3).sum(), f.forecast_revenue.sum()
    change = (next3 / last3 - 1) * 100
    if change > -15:
        return None
    mape = float(f.backtest_mape_pct.iloc[0])
    conf = "High" if mape < 15 else "Medium" if mape < 30 else "Low"
    return (3, "Cash Planning",
            f"Revenue for the next 3 months is forecast at {money(next3)}, about {abs(change):.0f}% lower than the last 3 months.",
            f"Set aside cash now to cover rent, insurance and payroll; monthly overhead averages "
            f"{money(hist.overhead.tail(12).mean())}.",
            conf, f"Seasonal forecast from {months_of_data} months of history (backtest error {mape:.0f}%)")

def rule_customers(client):
    r = rfm[rfm.client_name == client]
    risk = r[r.segment == "At Risk (was loyal)"]
    champs = r[r.segment == "Champions"]
    if risk.empty:
        return None
    share = champs.monetary.sum() / r.monetary.sum() * 100 if r.monetary.sum() else 0
    return (4, "Customers",
            f"{len(risk)} once-loyal customers have gone quiet; they spent {money(risk.monetary.sum())} with you. "
            f"Your top customers ({len(champs)}) bring in {share:.0f}% of revenue.",
            f"Reach out to the {len(risk)} quiet customers with a check-in or maintenance offer, "
            f"and ask your top customers for referrals.",
            "Medium", f"Job history for {len(r)} customers")

def rule_marketing(client):
    l = lead[lead.client_name == client]
    if len(l) < 2:
        return None
    best = l.sort_values("avg_margin_pct", ascending=False).iloc[0]
    worst = l.sort_values("avg_margin_pct").iloc[0]
    if best.avg_margin_pct - worst.avg_margin_pct < 3:
        return None
    return (5, "Marketing",
            f"{best.lead_source} jobs earn a {best.avg_margin_pct:.1f}% average margin vs {worst.avg_margin_pct:.1f}% from {worst.lead_source}.",
            f"Put more of your marketing time and budget into {best.lead_source}.",
            "Medium" if best.jobs >= 30 else "Low", f"{int(l.jobs.sum())} jobs by lead source")

RULES = [rule_pricing, rule_collections, rule_cash_season, rule_customers, rule_marketing]

# COMMAND ----------

# MAGIC %md ## Optional: AI rewording with a number-safety check

# COMMAND ----------

NUM = re.compile(r"\d[\d,]*\.?\d*")

def ai_reword(text: str) -> str:
    """Ask an LLM to make the sentence friendlier. Reject the result if any number changed."""
    if not USE_AI:
        return text
    try:
        prompt = ("Rewrite for a busy trade contractor in plain, friendly English. Keep every number exactly "
                  "as written. One or two sentences. Text: " + text)
        out = spark.sql("SELECT ai_query(:ep, :p) AS r", args={"ep": LLM, "p": prompt}).collect()[0]["r"]
        if sorted(NUM.findall(text)) == sorted(NUM.findall(out)):
            return out.strip()
    except Exception as e:
        print("AI wording skipped:", str(e)[:120])
    return text

# COMMAND ----------

# MAGIC %md ## Generate recommendations

# COMMAND ----------

rows = []
for client in kpis.client_name:
    recs = [r for r in (rule(client) for rule in RULES) if r][:5]
    for rank, (prio, cat, insight, action, conf, basis) in enumerate(sorted(recs), start=1):
        rows.append({
            "as_of_date": AS_OF, "client_name": client, "rank": rank, "category": cat,
            "insight": ai_reword(insight), "recommended_action": ai_reword(action),
            "confidence": conf, "based_on": basis, "disclaimer": DISCLAIMER,
            "generated_by": "rules + AI wording" if USE_AI else "rules",
            "generated_at": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M UTC"),
        })

recs = pd.DataFrame(rows)
publish(recs, "insight_recommendations")
display(recs[["client_name", "rank", "category", "insight", "recommended_action", "confidence"]])

# COMMAND ----------

# MAGIC %md ## Preview: one client's report section

# COMMAND ----------

sample = recs.client_name.iloc[0]
print(f"{sample} — Monthly insights (as of {AS_OF})\n")
for r in recs[recs.client_name == sample].itertuples():
    print(f"{r.rank}. [{r.category} · {r.confidence} confidence] {r.insight}\n   → {r.recommended_action}\n   Based on: {r.based_on}\n")
print("Note:", DISCLAIMER)
