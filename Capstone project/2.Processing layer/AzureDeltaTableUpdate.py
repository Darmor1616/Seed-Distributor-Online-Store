# Databricks notebook source
# MAGIC %run "../Configurations/ConnectAzureStorage"

# COMMAND ----------

# Get workflow parameters
dbutils.widgets.text("table_name", "addresses")
table_name = dbutils.widgets.get("table_name")
dbutils.widgets.text("table_prefix", "fct")
table_prefix = dbutils.widgets.get("table_prefix")
source_path = f'abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/Team_A/damir_aliyev/ingestion_layer/{table_name}/'
target_path = f"abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/Team_A/damir_aliyev/processing_layer/{table_prefix}_{table_name}"

# COMMAND ----------

# Read source table
df = spark.read.format("delta").load(source_path)

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, expr, current_date, lit
from delta.tables import DeltaTable
cur_date = current_date()

# Columns for hash
columns = [field.name for field in df.schema]
columns.remove('id')

# Merge columns for insert
insert_column_names = df.columns
insert_values = {col: f"source.{col}" for col in insert_column_names}

# Merge columns for update
update_values = insert_values
del update_values['id']

df = df.withColumn("hash", lit(sha2(concat_ws("~", *columns), 256)))

# COMMAND ----------

# Write delta table if not exists
df.write.mode('ignore').format("delta").save(target_path)

# COMMAND ----------


# Write to delta table
deltaTable = DeltaTable.forPath(spark, target_path)

# Perform the merge operation to update existing rows with different hash
deltaTable.alias("target").merge(
    df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(
    condition="target.hash <> source.hash",
    set=update_values
).whenNotMatchedInsert(values=insert_values).execute()
