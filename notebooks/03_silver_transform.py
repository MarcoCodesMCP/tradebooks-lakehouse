# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver — Clean, Validate, Deduplicate, Upsert (PySpark)
# MAGIC | Step | Technique |
# MAGIC |---|---|
# MAGIC | Type casting | `try_cast` / `try_to_timestamp` — bad values become NULL instead of failing the job |
# MAGIC | Mixed date formats | `coalesce()` across `yyyy-MM-dd` and `MM/dd/yyyy` |
# MAGIC | Currency strings | strip `$` and `,` with `regexp_replace` |
# MAGIC | Sign errors | `abs()` + `_amount_corrected` flag (kept for audit) |
# MAGIC | Duplicates | `row_number()` window over business key, keep latest ingest |
# MAGIC | Data quality | rules → failing rows go to `silver_quarantine` with a reason |
# MAGIC | Loading | Delta `MERGE` (upsert) — idempotent, safe to re-run |

# COMMAND ----------

from pyspark.sql import functions as F, Window, DataFrame
from delta.tables import DeltaTable

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "tradebooks")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

# COMMAND ----------

# MAGIC %md ## Reusable cleaning functions

# COMMAND ----------

def parse_date(col: str):
    """Handles ISO and US date formats; returns NULL if neither parses."""
    return F.coalesce(
        F.to_date(F.expr(f"try_to_timestamp({col}, 'yyyy-MM-dd')")),
        F.to_date(F.expr(f"try_to_timestamp({col}, 'MM/dd/yyyy')")),
    )

def parse_amount(col: str):
    """'$1,234.50' -> 1234.50 ; garbage -> NULL."""
    return F.expr(f"try_cast(regexp_replace({col}, '[$, ]', '') AS DECIMAL(12,2))")

def dedupe(df: DataFrame, key: str) -> DataFrame:
    w = Window.partitionBy(key).orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

def split_valid(df: DataFrame, entity: str, rules: dict):
    """rules = {reason: boolean Column that must be TRUE}. Returns (valid_df, quarantine_df)."""
    reasons = F.array_compact(F.array(*[F.when(~F.coalesce(cond, F.lit(False)), F.lit(r)) for r, cond in rules.items()]))
    tagged = df.withColumn("_dq_failures", reasons)
    valid = tagged.filter(F.size("_dq_failures") == 0).drop("_dq_failures")
    bad = (tagged.filter(F.size("_dq_failures") > 0)
           .select(F.lit(entity).alias("entity"),
                   F.to_json(F.struct(*[c for c in df.columns])).alias("record"),
                   F.concat_ws("; ", "_dq_failures").alias("reason"),
                   F.current_timestamp().alias("_quarantined_at")))
    return valid, bad

def merge_upsert(df: DataFrame, table: str, key: str):
    """Idempotent upsert into a Silver Delta table."""
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        return
    (DeltaTable.forName(spark, table).alias("t")
        .merge(df.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

dq_log, quarantine = [], []

def process(entity: str, key: str, transform, rules: dict):
    raw = spark.table(T(f"bronze_{entity}"))
    deduped = dedupe(raw, key)
    typed = transform(deduped)
    valid, bad = split_valid(typed, entity, rules)
    valid = valid.drop("_rescued_data", "_source_file").withColumn("_updated_at", F.current_timestamp())
    merge_upsert(valid, T(f"silver_{entity}"), key)
    quarantine.append(bad)
    n_raw, n_dedup, n_valid = raw.count(), deduped.count(), valid.count()
    dq_log.append((entity, n_raw, n_raw - n_dedup, n_dedup - n_valid, n_valid))

# COMMAND ----------

# MAGIC %md ## Dimensions

# COMMAND ----------

process("clients", "client_id",
        lambda df: df.select("client_id", "client_name", "trade", "city",
                             F.col("num_techs").cast("int").alias("num_techs"),
                             F.col("labor_rate").cast("decimal(8,2)").alias("labor_rate"),
                             "_ingested_at", "_source_file", "_rescued_data"),
        {"missing_client_id": F.col("client_id").isNotNull()})

process("customers", "customer_id",
        lambda df: df.select("customer_id", "client_id", F.initcap(F.trim("customer_name")).alias("customer_name"),
                             "customer_type", F.initcap(F.trim("town")).alias("town"),
                             "_ingested_at", "_source_file", "_rescued_data"),
        {"missing_customer_id": F.col("customer_id").isNotNull()})

# COMMAND ----------

# MAGIC %md ## Facts

# COMMAND ----------

process("jobs", "job_id",
        lambda df: df.select("job_id", "client_id", "customer_id", F.trim("job_type").alias("job_type"),
                             parse_date("scheduled_date").alias("scheduled_date"), "status",
                             parse_amount("quoted_amount").alias("quoted_amount"), "lead_source",
                             "_ingested_at", "_source_file", "_rescued_data"),
        {"missing_customer_id": F.col("customer_id").isNotNull(),
         "bad_scheduled_date": F.col("scheduled_date").isNotNull()})

def tx_invoices(df):
    amt = parse_amount("amount")
    return df.select("invoice_id", "client_id", "job_id", "customer_id",
                     parse_date("invoice_date").alias("invoice_date"), parse_date("due_date").alias("due_date"),
                     F.abs(amt).alias("amount"), (amt < 0).alias("_amount_corrected"),
                     parse_amount("tax").alias("tax"), "_ingested_at", "_source_file", "_rescued_data")

process("invoices", "invoice_id", tx_invoices,
        {"bad_invoice_date": F.col("invoice_date").isNotNull(),
         "bad_amount": F.col("amount").isNotNull() & (F.col("amount") > 0)})

def tx_payments(df):
    amt = parse_amount("amount")
    return df.select("payment_id", "client_id", "invoice_id", parse_date("payment_date").alias("payment_date"),
                     F.abs(amt).alias("amount"), (amt < 0).alias("_amount_corrected"), "method",
                     "_ingested_at", "_source_file", "_rescued_data")

process("payments", "payment_id", tx_payments,
        {"bad_payment_date": F.col("payment_date").isNotNull(),
         "bad_amount": F.col("amount").isNotNull() & (F.col("amount") > 0)})

def tx_expenses(df):
    amt = parse_amount("amount")
    return df.select("expense_id", "client_id", F.nullif(F.col("job_id"), F.lit("")).alias("job_id"),
                     parse_date("expense_date").alias("expense_date"),
                     F.initcap(F.trim("category")).alias("category"),   # 'MATERIALS ' / 'materials' -> 'Materials'
                     "vendor", F.abs(amt).alias("amount"), (amt < 0).alias("_amount_corrected"),
                     F.when(F.col("job_id").isNull() | (F.col("job_id") == ""), "Overhead").otherwise("Job Cost").alias("cost_type"),
                     "_ingested_at", "_source_file", "_rescued_data")

process("expenses", "expense_id", tx_expenses,
        {"bad_expense_date": F.col("expense_date").isNotNull(),
         "bad_amount": F.col("amount").isNotNull()})

process("timesheets", "timesheet_id",
        lambda df: df.select("timesheet_id", "client_id", "job_id", "tech_id", parse_date("work_date").alias("work_date"),
                             F.col("hours").cast("decimal(6,2)").alias("hours"),
                             F.col("hourly_cost").cast("decimal(8,2)").alias("hourly_cost"),
                             "_ingested_at", "_source_file", "_rescued_data"),
        {"bad_hours": F.col("hours").between(0.25, 16)})

# COMMAND ----------

# MAGIC %md ## Quarantine + Data Quality log

# COMMAND ----------

from functools import reduce
reduce(DataFrame.unionByName, quarantine).write.mode("overwrite").saveAsTable(T("silver_quarantine"))

dq = (spark.createDataFrame(dq_log, "entity string, bronze_rows long, duplicates_removed long, quarantined long, silver_rows long")
      .withColumn("pct_clean", F.round(F.col("silver_rows") / F.col("bronze_rows") * 100, 2))
      .withColumn("run_at", F.current_timestamp()))
dq.write.mode("append").saveAsTable(T("dq_run_log"))
display(dq)

# COMMAND ----------

display(spark.sql(f"SELECT entity, reason, COUNT(*) AS rows FROM {T('silver_quarantine')} GROUP BY ALL ORDER BY rows DESC"))
