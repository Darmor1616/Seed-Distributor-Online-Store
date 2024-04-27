# Databricks notebook source
'''
Notebook load jobs run time to log table

Input example:
Output:
'''

# COMMAND ----------

# MAGIC %run "./ConnectAzureStorage"

# COMMAND ----------

workflow_id_arr = {"monitoring":"445096163559636","ingestion":"51174241710901","processing":"632165797969304"}
target_path = f"abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/Team_A/damir_aliyev/processing_layer/"

# COMMAND ----------

import requests
import json

# Example for Azure Databricks
domain = "https://adb-7218202347847239.19.azuredatabricks.net"
# Retrieve the secret value
access_token = dbutils.secrets.get(scope="workflow-access-token", key="Seed_Distributor_Online_Store ")
headers = {"Authorization": f"Bearer {access_token}"}

def get_job_runs(job_id):
    """Retrieve runs for a specific job ID."""
    endpoint = f"{domain}/api/2.0/jobs/runs/list?job_id={job_id}"
    response = requests.get(endpoint, headers=headers)
    if response.status_code == 200:
        return response.json()["runs"]
    else:
        print(response.text)
        raise Exception("Failed to retrieve job runs:")
job_runs = []
for workflow_name in workflow_id_arr:
    job_runs.append(get_job_runs(workflow_id_arr[workflow_name]))

# COMMAND ----------

import requests
import datetime

def get_job_run_details(domain, token, run_id):
    headers = {"Authorization": f"Bearer {token}"}
    domain = "https://adb-7218202347847239.19.azuredatabricks.net"
    url = f"{domain}/api/2.0/jobs/runs/get?run_id={run_id}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("Failed to retrieve job run details")

def parse_task_details(job_details):
    tasks = job_details.get('tasks', [])
    return tasks


# COMMAND ----------

import datetime  
import pandas as pd

job_logs_df = pd.DataFrame(columns = ['job_run_id','job_id','job_name','date','start_time','end_time','duration','status'])
task_logs_df = pd.DataFrame(columns = ['task_key','job_run_id','start_time','end_time','status'])

for job_run2 in job_runs:
    for job_run in job_run2:
        # Assuming job_run is a dictionary that contains job run details
        job_name = next((k for k, v in workflow_id_arr.items() if int(v) == int(job_run['job_id'])), None)
        
        # Convert epoch to datetime
        start_time = datetime.datetime.fromtimestamp(int(job_run['start_time']) / 1000)  # Assuming epoch is in milliseconds
        end_time = datetime.datetime.fromtimestamp(int(job_run['end_time']) / 1000)  # Adjust here as well
        
        # Calculate duration
        duration = end_time - start_time
        if 'result_state' in job_run['state']:
            status = job_run['state']['result_state']
        else:
            status = 'SUCCESS'
        # Prepare the row to be appended
        new_row = {
            'job_run_id': job_run['run_id'], 
            'job_id': job_run['job_id'], 
            'job_name': job_name, 
            'date': start_time.date(), 
            'start_time': start_time, 
            'end_time': end_time, 
            'duration': duration.total_seconds(),  # Convert timedelta to seconds
            'status': status
        }
        
        # Append the new row to the DataFrame
        job_logs_df = job_logs_df.append(new_row, ignore_index=True)

        job_details = get_job_run_details(domain, access_token, job_run['run_id'])
        task_details = parse_task_details(job_details)
        for task_detail in task_details:
            # Prepare the row to be appended
            if 'result_state' in task_detail['state']:
                task_status = task_detail['state']['result_state']
            else:
                task_status = 'SUCCESS'
            new_row = {
                'task_key': task_detail['task_key'],
                'job_run_id': job_run['run_id'], 
                'start_time': datetime.datetime.fromtimestamp(int(task_detail['start_time']) / 1000),
                'end_time': datetime.datetime.fromtimestamp(int(task_detail['end_time']) / 1000),
                'status': task_status
            }
            # Append the new row to the DataFrame
            task_logs_df = task_logs_df.append(new_row, ignore_index=True)

spark_job_logs_df = spark.createDataFrame(job_logs_df)
spark_task_logs_df = spark.createDataFrame(task_logs_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
# Define the window specification
windowSpec = Window.partitionBy("task_key", "job_run_id").orderBy(col("start_time").desc())

# Apply the window function to add row numbers
df_with_row_number = spark_task_logs_df.withColumn("row_number", row_number().over(windowSpec))

# Filter to keep only the top row for each partition (latest start_time)
spark_task_logs_df = df_with_row_number.filter(col("row_number") == 1).drop("row_number")

# COMMAND ----------

# Write delta table if not exists
spark_job_logs_df.write.mode('ignore').format("delta").save(target_path + 'jobs_log')

# COMMAND ----------

from delta.tables import DeltaTable
# Write to delta table
deltaTable = DeltaTable.forPath(spark, target_path + 'jobs_log')

# Merge logic
deltaTable.alias("target").merge(
    spark_job_logs_df.alias("source"),
    "target.job_run_id = source.job_run_id" 
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# COMMAND ----------

# Write delta table if not exists
spark_task_logs_df.write.mode('ignore').format("delta").save(target_path + 'tasks_log')

# COMMAND ----------

from delta.tables import DeltaTable
# Write to delta table
deltaTable = DeltaTable.forPath(spark, target_path + 'tasks_log')

# Merge logic
deltaTable.alias("target").merge(
    spark_task_logs_df.alias("source"),
    "target.job_run_id = source.job_run_id and target.task_key = source.task_key" 
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

