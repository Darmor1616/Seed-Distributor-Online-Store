# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AzureBlobStorageExample") \
    .getOrCreate()
# Enable auto optimize
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")


# COMMAND ----------

sa_name = "dextestwesteurope"
sas_token = dbutils.secrets.get(scope="adls-access-token", key="Seed_Distributor_Online_Store ")
spark.conf.set(f"fs.azure.account.auth.type.{sa_name}.dfs.core.windows.net","SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{sa_name}.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{sa_name}.dfs.core.windows.net", sas_token)
