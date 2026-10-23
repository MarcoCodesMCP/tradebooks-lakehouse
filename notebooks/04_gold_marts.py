# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold — Business-Ready Marts (PySpark)
# MAGIC The tables a contractor (or the bookkeeping firm) actually looks at.
# MAGIC
# MAGIC | Gold table | Business question |
# MAGIC |---|---|
# MAGIC | `gold_job_profitability` | Which jobs / job types actually make money after materials + labor? |
# MAGIC | `gold_monthly_pnl` | Revenue, COGS, overhead and net profit per client per month |
# MAGIC | `gold_ar_aging` | Who owes money, and how overdue is it? |
# MAGIC | `gold_cash_flow` | Cash in vs cash out each month (cash ≠ profit) |
# MAGIC | `gold_tech_utilization` | Are technicians busy or idle? |
# MAGIC | `gold_lead_source` | Which marketing channels bring the most profitable work? |
# MAGIC | `gold_client_kpis` | One scorecard row per contractor client |

# COMMAND ----------

from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "tradebooks")
dbutils.widgets.text("as_of_date", "2026-09-30")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
AS_OF = F.lit(dbutils.widgets.get("as_of_date")).cast("date")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

clients    = spark.table(T("silver_clients"))
customers  = spark.table(T("silver_customers"))
jobs       = spark.table(T("silver_jobs"))
invoices   = spark.table(T("silver_invoices"))
payments   = spark.table(T("silver_payments"))
expenses   = spark.table(T("silver_expenses"))
timesheets = spark.table(T("silver_timesheets"))

def save(df, name, partition=None):
    w = df.write.mode("overwrite").option("overwriteSchema", "true")
    (w.partitionBy(partition) if partition else w).saveAsTable(T(name))
    print(f"✓ {name}: {spark.table(T(name)).count():,} rows")

# COMMAND ----------

# MAGIC %md ## Job profitability (job-level fact)

# COMMAND ----------

job_cost = (expenses.filter("cost_type = 'Job Cost'")
            .groupBy("job_id")
            .agg(F.sum(F.when(F.col("category") == "Materials", F.col("amount")).otherwise(0)).alias("materials_cost"),
                 F.sum(F.when(F.col("category") != "Materials", F.col("amount")).otherwise(0)).alias("sub_permit_cost")))

labor = (timesheets.groupBy("job_id")
         .agg(F.sum("hours").alias("labor_hours"),
              F.sum(F.col("hours") * F.col("hourly_cost")).alias("labor_cost")))

job_rev = invoices.groupBy("job_id").agg(F.sum("amount").alias("revenue"), F.min("invoice_date").alias("invoice_date"))

job_profit = (jobs.filter("status = 'Completed'")
    .join(job_rev, "job_id", "left").join(job_cost, "job_id", "left").join(labor, "job_id", "left")
    .join(clients.select("client_id", "client_name", "trade"), "client_id")
    .join(customers.select("customer_id", "customer_type"), "customer_id", "left")
    .fillna(0, ["revenue", "materials_cost", "sub_permit_cost", "labor_cost", "labor_hours"])
    .withColumn("total_cost", F.col("materials_cost") + F.col("sub_permit_cost") + F.col("labor_cost"))
    .withColumn("gross_profit", F.col("revenue") - F.col("total_cost"))
    .withColumn("gross_margin_pct", F.round(F.col("gross_profit") / F.nullif(F.col("revenue"), F.lit(0)) * 100, 2))
    .withColumn("revenue_per_labor_hour", F.round(F.col("revenue") / F.nullif(F.col("labor_hours"), F.lit(0)), 2))
    .withColumn("job_month", F.date_trunc("month", "scheduled_date").cast("date"))
    .select("job_id", "client_id", "client_name", "trade", "customer_id", "customer_type", "job_type",
            "lead_source", "scheduled_date", "job_month", "revenue", "materials_cost", "sub_permit_cost",
            "labor_hours", "labor_cost", "total_cost", "gross_profit", "gross_margin_pct", "revenue_per_labor_hour"))

save(job_profit, "gold_job_profitability", partition="client_id")

# COMMAND ----------

# MAGIC %md ## Monthly P&L per client

# COMMAND ----------

month = lambda c: F.date_trunc("month", c).cast("date")

rev_m = invoices.groupBy("client_id", month("invoice_date").alias("month")).agg(F.sum("amount").alias("revenue"))
cogs_m = (expenses.filter("cost_type = 'Job Cost'").groupBy("client_id", month("expense_date").alias("month"))
          .agg(F.sum("amount").alias("job_expenses")))
labor_m = (timesheets.groupBy("client_id", month("work_date").alias("month"))
           .agg(F.sum(F.col("hours") * F.col("hourly_cost")).alias("labor_cost")))
ovh_m = (expenses.filter("cost_type = 'Overhead'").groupBy("client_id", month("expense_date").alias("month"))
         .agg(F.sum("amount").alias("overhead")))

pnl = (rev_m.join(cogs_m, ["client_id", "month"], "full").join(labor_m, ["client_id", "month"], "full")
       .join(ovh_m, ["client_id", "month"], "full").fillna(0)
       .filter(F.col("month") <= AS_OF)                       # no partial future months
       .withColumn("cogs", F.col("job_expenses") + F.col("labor_cost"))
       .withColumn("gross_profit", F.col("revenue") - F.col("cogs"))
       .withColumn("net_profit", F.col("gross_profit") - F.col("overhead"))
       .withColumn("net_margin_pct", F.round(F.col("net_profit") / F.nullif(F.col("revenue"), F.lit(0)) * 100, 2))
       .join(clients.select("client_id", "client_name", "trade"), "client_id")
       .withColumn("revenue_mom_pct", F.round(
           (F.col("revenue") / F.nullif(F.lag("revenue").over(Window.partitionBy("client_id").orderBy("month")), F.lit(0)) - 1) * 100, 2))
       .withColumn("revenue_ytd", F.sum("revenue").over(
           Window.partitionBy("client_id", F.year("month")).orderBy("month")
           .rowsBetween(Window.unboundedPreceding, Window.currentRow)))
       .orderBy("client_id", "month"))

save(pnl, "gold_monthly_pnl")

# COMMAND ----------

# MAGIC %md ## Accounts-receivable aging (as of a date)

# COMMAND ----------

paid = (payments.filter(F.col("payment_date") <= AS_OF)
        .groupBy("invoice_id").agg(F.sum("amount").alias("paid_amount"), F.max("payment_date").alias("last_payment_date")))

ar = (invoices.filter(F.col("invoice_date") <= AS_OF)
      .join(paid, "invoice_id", "left").fillna(0, ["paid_amount"])
      .withColumn("invoice_total", F.col("amount") + F.coalesce("tax", F.lit(0)))
      .withColumn("balance_due", F.round(F.col("invoice_total") - F.col("paid_amount"), 2))
      .filter("balance_due > 1")
      .withColumn("days_past_due", F.greatest(F.datediff(AS_OF, "due_date"), F.lit(0)))
      .withColumn("aging_bucket", F.when(F.col("days_past_due") == 0, "Current")
                  .when(F.col("days_past_due") <= 30, "1-30").when(F.col("days_past_due") <= 60, "31-60")
                  .when(F.col("days_past_due") <= 90, "61-90").otherwise("90+"))
      .join(customers.select("customer_id", "customer_name", "customer_type"), "customer_id", "left")
      .join(clients.select("client_id", "client_name"), "client_id")
      .withColumn("as_of_date", AS_OF)
      .select("as_of_date", "client_id", "client_name", "customer_id", "customer_name", "customer_type",
              "invoice_id", "invoice_date", "due_date", "invoice_total", "paid_amount", "balance_due",
              "days_past_due", "aging_bucket"))

save(ar, "gold_ar_aging")

# COMMAND ----------

# MAGIC %md ## Cash flow (cash in vs cash out)

# COMMAND ----------

cash_in = payments.groupBy("client_id", month("payment_date").alias("month")).agg(F.sum("amount").alias("cash_in"))
cash_out = (expenses.groupBy("client_id", month("expense_date").alias("month")).agg(F.sum("amount").alias("exp_out"))
            .join(labor_m, ["client_id", "month"], "full").fillna(0)
            .withColumn("cash_out", F.col("exp_out") + F.col("labor_cost")).select("client_id", "month", "cash_out"))

w_cum = Window.partitionBy("client_id").orderBy("month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cash = (cash_in.join(cash_out, ["client_id", "month"], "full").fillna(0)
        .filter(F.col("month") <= AS_OF)
        .withColumn("net_cash_flow", F.col("cash_in") - F.col("cash_out"))
        .withColumn("cumulative_cash", F.sum("net_cash_flow").over(w_cum))
        .join(clients.select("client_id", "client_name"), "client_id"))

save(cash, "gold_cash_flow")

# COMMAND ----------

# MAGIC %md ## Technician utilization
# MAGIC Capacity = working weekdays in the month × 8 hrs. Billable hours / capacity = utilization.

# COMMAND ----------

days = spark.sql("SELECT explode(sequence(DATE'2025-01-01', DATE'2026-09-30', INTERVAL 1 DAY)) AS d")
capacity = (days.filter(F.dayofweek("d").between(2, 6))
            .groupBy(month("d").alias("month")).agg((F.count("*") * 8).alias("capacity_hours")))

util = (timesheets.groupBy("client_id", "tech_id", month("work_date").alias("month"))
        .agg(F.sum("hours").alias("billable_hours"), F.countDistinct("job_id").alias("jobs_completed"))
        .join(capacity, "month")
        .withColumn("utilization_pct", F.round(F.col("billable_hours") / F.col("capacity_hours") * 100, 1))
        .withColumn("utilization_band", F.when(F.col("utilization_pct") >= 85, "Overloaded")
                    .when(F.col("utilization_pct") >= 60, "Healthy").otherwise("Under-utilized")))

save(util, "gold_tech_utilization")

# COMMAND ----------

# MAGIC %md ## Lead-source performance

# COMMAND ----------

lead = (job_profit.groupBy("client_id", "client_name", "lead_source")
        .agg(F.count("*").alias("jobs"), F.sum("revenue").alias("revenue"),
             F.sum("gross_profit").alias("gross_profit"), F.avg("revenue").alias("avg_ticket"),
             F.avg("gross_margin_pct").alias("avg_margin_pct"))
        .withColumn("share_of_revenue_pct", F.round(
            F.col("revenue") / F.sum("revenue").over(Window.partitionBy("client_id")) * 100, 2)))

save(lead, "gold_lead_source")

# COMMAND ----------

# MAGIC %md ## Client KPI scorecard

# COMMAND ----------

last12 = F.col("month") > F.add_months(AS_OF, -12)
pnl_12 = (spark.table(T("gold_monthly_pnl")).filter(last12)
          .groupBy("client_id").agg(F.sum("revenue").alias("revenue_ttm"), F.sum("net_profit").alias("net_profit_ttm")))

ar_tot = (spark.table(T("gold_ar_aging")).groupBy("client_id")
          .agg(F.sum("balance_due").alias("ar_balance"),
               F.sum(F.when(F.col("days_past_due") > 60, F.col("balance_due")).otherwise(0)).alias("ar_over_60")))

repeat = (jobs.groupBy("client_id", "customer_id").count()
          .groupBy("client_id").agg(F.round(F.avg((F.col("count") > 1).cast("int")) * 100, 1).alias("repeat_customer_pct")))

kpis = (clients.select("client_id", "client_name", "trade", "num_techs")
        .join(pnl_12, "client_id", "left").join(ar_tot, "client_id", "left").join(repeat, "client_id", "left")
        .join(job_profit.groupBy("client_id").agg(F.round(F.avg("revenue"), 2).alias("avg_ticket"),
                                                  F.round(F.avg("gross_margin_pct"), 2).alias("avg_job_margin_pct")),
              "client_id", "left")
        .withColumn("net_margin_pct_ttm", F.round(F.col("net_profit_ttm") / F.col("revenue_ttm") * 100, 2))
        .withColumn("dso_days", F.round(F.col("ar_balance") / (F.col("revenue_ttm") / 365), 1))  # Days Sales Outstanding
        .withColumn("revenue_per_tech", F.round(F.col("revenue_ttm") / F.col("num_techs"), 0))
        .withColumn("as_of_date", AS_OF))

save(kpis, "gold_client_kpis")
display(kpis)
