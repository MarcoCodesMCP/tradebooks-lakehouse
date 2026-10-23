# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze — Incremental Ingestion with Auto Loader (PySpark)
# MAGIC Raw files → Delta tables, **as-is** (every column kept as string) plus lineage metadata.
# MAGIC
# MAGIC **Why Auto Loader?** It tracks which files were already processed (checkpoint), so re-running
# MAGIC this notebook only picks up *new* files — exactly how new monthly client exports would arrive.
# MAGIC `trigger(availableNow=True)` processes the backlog and stops, so it runs as a cheap batch job.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "tradebooks")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
CHECKPOINTS = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# entity -> source file format
SOURCES = {
    "clients": "csv", "customers": "csv", "jobs": "csv", "invoices": "csv",
    "payments": "json", "expenses": "csv", "timesheets": "csv",
}

# COMMAND ----------

def ingest_bronze(entity: str, fmt: str):
    target = f"{CATALOG}.{SCHEMA}.bronze_{entity}"
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/schemas/{entity}")
        .option("cloudFiles.inferColumnTypes", "false")      # keep raw values as strings in Bronze
        .option("cloudFiles.schemaEvolutionMode", "rescue")   # unexpected columns -> _rescued_data
    )
    if fmt == "csv":
        reader = reader.option("header", "true")

    query = (
        reader.load(f"{LANDING}/{entity}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{CHECKPOINTS}/bronze/{entity}")
        .trigger(availableNow=True)
        .toTable(target)
    )
    query.awaitTermination()
    return target

# COMMAND ----------

results = []
for entity, fmt in SOURCES.items():
    table = ingest_bronze(entity, fmt)
    results.append((entity, table, spark.table(table).count()))

display(spark.createDataFrame(results, "entity string, table string, row_count long"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check: Bronze keeps the mess
# MAGIC Notice the `$1,234.00` strings, `MM/DD/YYYY` dates and negative amounts — Bronze never fixes data,
# MAGIC it only preserves it so we can always replay Silver.

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{SCHEMA}.bronze_invoices")
    .filter("amount LIKE '$%' OR amount LIKE '-%' OR invoice_date LIKE '%/%'")
    .limit(10)
)
