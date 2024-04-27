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

from pyspark.sql.functions import regexp_replace, trim, initcap, col, when, lit 
from pyspark.sql.types import StringType

# All String type fields to clean 
string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
print(string_columns)
valid_flag = lit(True)
cleaned_df = df
for string_col in string_columns:
    # Remove special symbols, extra spaces, and capitalize properly
    cleaned_col_name = f"Cleaned{string_col}"

    cleaned_df = cleaned_df.withColumn(cleaned_col_name, trim(regexp_replace(col(string_col), r"[^a-zA-Z0-9']", " ")))
    cleaned_df = cleaned_df.withColumn(cleaned_col_name, regexp_replace(col(cleaned_col_name), r"\s+", " "))
    # cleaned_df = cleaned_df.withColumn(cleaned_col_name, trim(regexp_replace(regexp_replace(col(string_col), r"[!$#/*,.|]", ""), r"\s+", " ")))
    cleaned_df = cleaned_df.withColumn(cleaned_col_name, initcap(col(cleaned_col_name)))
    valid_flag = valid_flag & (col(cleaned_col_name) == col(string_col)
    )

# Define a flag for valid addresses based on your criteria
cleaned_df = cleaned_df.withColumn("validFlag",when(valid_flag,1).otherwise(0))

for string_col in string_columns:
    cleaned_col_name = f"Cleaned{string_col}"
    cleaned_df = cleaned_df.drop(string_col).withColumnRenamed(cleaned_col_name, string_col)

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, expr
columns = [field.name for field in cleaned_df.schema]
columns.remove('id')
cleaned_df = cleaned_df.withColumn("hash", lit(sha2(concat_ws("~", *columns), 256))).withColumn("beg_date", expr("date('1980-01-01')") ).withColumn("exp_date", expr("date('4444-01-01')"))

# COMMAND ----------

# Write delta table if not exists
cleaned_df.write.mode('ignore').format("delta").save(target_path)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, lit
# Write to delta table
deltaTable = DeltaTable.forPath(spark, target_path)

cur_date = current_date()
insert_column_names = cleaned_df.columns
insert_values = {col: f"source.{col}" for col in insert_column_names}
# Perform the merge operation to update existing rows with different hash
deltaTable.alias("target").merge(
    cleaned_df.alias("source"),
    "target.id = source.id and target.exp_date = date('4444-01-01')"
).whenMatchedUpdate(
    condition="target.hash <> source.hash",
    set={
        "exp_date": cur_date
    }
).whenNotMatchedInsert(values=insert_values).execute()


# COMMAND ----------

# Insert new rows with a beg_date of today, and exp_date set far in the future
cleaned_df = cleaned_df.withColumn("beg_date", cur_date)

rows_to_insert = cleaned_df.join(
    deltaTable.toDF(),
    (cleaned_df["id"] == deltaTable.toDF()["id"]) & (deltaTable.toDF()["exp_date"] == cur_date),
    "inner"
).where(
    cleaned_df["hash"] != deltaTable.toDF()["hash"]
).select(cleaned_df["*"])

# Insert these new rows into the target table
rows_to_insert.write.format("delta").mode("append").save(target_path)

