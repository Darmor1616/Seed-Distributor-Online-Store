# Databricks notebook source
# MAGIC %run "../Configurations/ConnectAzureStorage"

# COMMAND ----------

# Get workflow parameters
dbutils.widgets.text("table_name", "addresses")
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

# Read source table
df = spark.read.parquet(f'abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/adv-dse/{table_name}/')

# COMMAND ----------

# Write delta table if not exists
df.write.mode('ignore').format("delta").save(f"abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/Team_A/damir_aliyev/ingestion_layer/{table_name}")

# COMMAND ----------

# Write delta table if not exists
df.write.mode('overwrite').format("delta").save(f"abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/Team_A/damir_aliyev/ingestion_layer/{table_name}")
