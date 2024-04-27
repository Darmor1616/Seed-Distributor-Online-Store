# Databricks notebook source
# MAGIC %run "../Configurations/ConnectAzureStorage"

# COMMAND ----------

# Get workflow parameters
dbutils.widgets.text("table_name", "addresses")
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

# Source table
source = spark.read.parquet(f'abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/adv-dse/{table_name}/')

# COMMAND ----------

# Ingestion table
target = spark.read.format("delta").load(f'abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/Team_A/damir_aliyev/ingestion_layer/{table_name}/')

# COMMAND ----------

# Checking for row count match
source_count = source.count()
target_count = target.count()

if source_count == target_count:
    print("Row count match")
else:
    raise Exception(f"Row count not match source count: {source_count}, target count: {target_count}")
