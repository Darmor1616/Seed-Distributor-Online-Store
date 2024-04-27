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

columns = [field.name for field in df.schema]
df = df.withColumn("hash", lit(sha2(concat_ws("~", *columns), 256))).withColumn("beg_date", expr("date('1980-01-01')") ).withColumn("exp_date", expr("date('4444-01-01')"))

# COMMAND ----------

# Write delta table if not exists
df.write.mode('ignore').format("delta").save(target_path)

# COMMAND ----------

# Write to delta table
deltaTable = DeltaTable.forPath(spark, target_path)

cur_date = current_date()
insert_column_names = df.columns
insert_values = {col: f"source.{col}" for col in insert_column_names}
# Perform the merge operation to update existing rows with different hash
deltaTable.alias("target").merge(
    df.alias("source"),
    "target.id = source.id and target.exp_date = date('4444-01-01')"
).whenMatchedUpdate(
    condition="target.hash <> source.hash",
    set={
        "exp_date": cur_date
    }
).whenNotMatchedInsert(values=insert_values).execute()


# COMMAND ----------

# Insert new rows with a beg_date of today, and exp_date set far in the future
df = df.withColumn("beg_date", cur_date)

rows_to_insert = df.join(
    deltaTable.toDF(),
    (df["id"] == deltaTable.toDF()["id"]) & (deltaTable.toDF()["exp_date"] == cur_date),
    "inner"
).where(
    df["hash"] != deltaTable.toDF()["hash"]
).select(df["*"])

# Insert these new rows into the target table
rows_to_insert.write.format("delta").mode("append").save(target_path)

