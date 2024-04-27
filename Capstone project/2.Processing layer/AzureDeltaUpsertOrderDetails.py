# Databricks notebook source
# MAGIC %run "../Configurations/ConnectAzureStorage"

# COMMAND ----------

# Get workflow parameters
dbutils.widgets.text("table_name", "addresses")
table_name = dbutils.widgets.get("table_name")
dbutils.widgets.text("table_prefix", "fct")
table_prefix = dbutils.widgets.get("table_prefix")

# COMMAND ----------

# Read source table from ingestion layer
orderDetails = spark.read.format("delta").load(f'abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/Team_A/damir_aliyev/ingestion_layer/{table_name}/')

# COMMAND ----------

from pyspark.sql.functions import concat_ws, col, sum, hash
orderDetails_sum = orderDetails.groupBy('orderId','itemId').agg(sum("quantity").alias("totalQuantity")).withColumn("id", concat_ws("_", col("itemId"), col("orderId"))).select('id','orderId','itemId','totalQuantity')

# COMMAND ----------

# Write delta table if not exists
orderDetails_sum.write.mode('ignore').format("delta").save(f"abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/Team_A/damir_aliyev/processing_layer/{table_prefix}_{table_name}")

# COMMAND ----------

from delta.tables import DeltaTable
# Write to delta table
deltaTable = DeltaTable.forPath(spark, f"abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/Team_A/damir_aliyev/processing_layer/{table_prefix}_{table_name}")

# Merge logic
deltaTable.alias("target").merge(
    orderDetails_sum.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(
    condition="target.totalQuantity != source.totalQuantity",
    set = {"target.totalQuantity": "source.totalQuantity"}
).whenNotMatchedInsertAll().execute()
