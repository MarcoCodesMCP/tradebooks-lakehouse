# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates the Unity Catalog schema and Volumes used by the pipeline.
# MAGIC
# MAGIC | Object | Purpose |
# MAGIC |---|---|
# MAGIC | `<catalog>.<schema>` | Holds bronze / silver / gold Delta tables |
# MAGIC | Volume `landing` | Raw files dropped by client bookkeeping exports |
# MAGIC | Volume `checkpoints` | Auto Loader schema + streaming checkpoints |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "tradebooks")
dbutils.widgets.dropdown("reset", "false", ["false", "true"])
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")

# COMMAND ----------

if dbutils.widgets.get("reset") == "true":
    spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} "
          "COMMENT 'TradeBooks: bookkeeping + analytics lakehouse for trade contractors'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}"))
