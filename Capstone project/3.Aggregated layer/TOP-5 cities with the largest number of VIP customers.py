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

dbutils.widgets.text("report_date", "")
report_date = dbutils.widgets.get("report_date")

target_path = f"abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/Team_A/damir_aliyev/aggregation_layer/{dm_name}"
print(sources_name,dm_name,beg_date,exp_date,report_date)

# COMMAND ----------

arr_sources_name = sources_name.split(',')
source_path = f'abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/Team_A/damir_aliyev/processing_layer/'
for source_name in arr_sources_name:
    spark.read.format("delta").load(source_path + source_name).createOrReplaceTempView(source_name)

# COMMAND ----------

# Sql for datamart
target = spark.sql(f'''
    select k.city, count(1) as NumberOfVIPCustomers
    from (select distinct a.city, 
                 c.id
        from dim_customers c

        inner join fct_orders o 
        on o.customerId = c.id

        inner join dim_addresses a
        on a.id = o.addressId
        and '{report_date}' between a.beg_date and a.exp_date  

        where c.status = 'VIP'
        and '{report_date}' between c.beg_date and c.exp_date ) k
    group by k.city
    order by count(1) desc
limit 5''')

# COMMAND ----------

# Write delta table if not exists
target.write.mode('overwrite').format("delta").save(target_path)
