# Databricks notebook source
# MAGIC %run "../Configurations/ConnectAzureStorage"

# COMMAND ----------

# Get workflow parameters
dbutils.widgets.text("sources_name", "dummy")
sources_name = dbutils.widgets.get("sources_name")

dbutils.widgets.text("dm_name", "dummy")
dm_name = dbutils.widgets.get("dm_name")

dbutils.widgets.text("beg_date", "dummy")
beg_date = dbutils.widgets.get("beg_date")

dbutils.widgets.text("exp_date", "dummy")
exp_date = dbutils.widgets.get("exp_date")

dbutils.widgets.text("report_date", "dummy")
report_date = dbutils.widgets.get("report_date")

target_path = f"abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/Team_A/damir_aliyev/aggregation_layer/{dm_name}"

# COMMAND ----------

arr_sources_name = sources_name.split(',')
source_path = f'abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/Team_A/damir_aliyev/processing_layer/'
for source_name in arr_sources_name:
    spark.read.format("delta").load(source_path + source_name).createOrReplaceTempView(source_name)

# COMMAND ----------

# Sql for datamart
target = spark.sql(f'''
          select date_format(date_trunc('MONTH', c.CreatedOn),'yyyy-MMM') as ReportingPeriod , type, status, count(1) as NewCustomerNumber
          from dim_customers c
          where c.createdOn between '{beg_date}' and '{exp_date}'
          and '{report_date}' between c.beg_date and c.exp_date 
          group by date_trunc('MONTH', c.CreatedOn), type, status
          ''')

# COMMAND ----------

# Write delta table if not exists
target.write.mode('ignore').format("delta").save(target_path)

# COMMAND ----------

from delta.tables import DeltaTable
# Write to delta table based on ReportingPeriod
deltaTable = DeltaTable.forPath(spark, target_path)

# Merge columns for update
update_column_names = target.columns
update_values = {col: f"source.{col}" for col in update_column_names}
del update_values['ReportingPeriod']

# Merge logic
deltaTable.alias("target").merge(
    target.alias("source"),
    "target.ReportingPeriod = source.ReportingPeriod"
).whenMatchedUpdate(
    set = update_values
).whenNotMatchedInsertAll().execute()
