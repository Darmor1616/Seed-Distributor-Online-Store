# Databricks notebook source
'''
Notebook start a job with dynamic values based on frequency(daily,monthly,weekly,yearly)

Input example:
frequency:daily
job_id: 1081793450465882

Output:
'''

# COMMAND ----------

# MAGIC %run "./ConnectAzureStorage"

# COMMAND ----------

# Get workflow parameters
dbutils.widgets.text("frequency", "dummy")
frequency = dbutils.widgets.get("frequency")

dbutils.widgets.text("job_id", "dummy")
job_id = dbutils.widgets.get("job_id")

# COMMAND ----------

import requests
import json
import time

# Retrieve the secret value
access_token = dbutils.secrets.get(scope="workflow-access-token", key="Seed_Distributor_Online_Store ")

def wait_for_job_completion(run_id):
    status_url = f"https://adb-7218202347847239.19.azuredatabricks.net/api/2.0/jobs/runs/get?run_id={run_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    while True:
        response = requests.get(status_url, headers=headers)
        if response.status_code == 200:
            status = response.json()['state']['life_cycle_state']
            print(f"Current job run state: {status}")
            if status in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                result_state = response.json()['state'].get('result_state', 'No result state')
                if result_state != 'SUCCESS':
                    raise Exception(f"Job run completed with result state: {result_state}")
                else:
                    print(f"Job run completed with result state: {result_state}")
                break
            elif status == "RUNNING":
                # Adjust the sleep time as needed for your context
                time.sleep(5)  # Wait for 10 seconds before checking again
        else:
            print(f"Failed to get job run status: {response.text}")
            break


def trigger_job(beg_date, exp_date, report_date):
    # Convert dates to string for easier handling
    beg_date_str = beg_date.strftime('%Y-%m-%d')
    exp_date_str = exp_date.strftime('%Y-%m-%d')
    report_date_str = report_date.strftime('%Y-%m-%d')

    # Define the API endpoint (make sure to replace <DATABRICKS_WORKSPACE_URL> with your actual Databricks workspace URL)
    api_url = "https://adb-7218202347847239.19.azuredatabricks.net/api/2.0/jobs/run-now"

    # Headers for the API request
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Prepare your dynamic parameters
    # Adjust these parameter names to match those defined in your job's configuration
    dynamic_parameters = {
        "beg_date": beg_date_str,
        "exp_date": exp_date_str,
        "report_date": report_date_str
    }

    # The payload containing the job ID and the dynamic parameters
    # Use 'parameters' to specify the parameters for jobs configured with the new job parameters feature
    payload = {
        "job_id": job_id,
        "job_parameters": dynamic_parameters  # Updated to use 'parameters' for new job parameters
    }
    print(payload)
    # Make the API request to trigger the job
    response = requests.post(api_url, headers=headers, json=payload)
    # Check the response
    if response.status_code == 200:
        print(f"Job triggered successfully: {response.text}")
        wait_for_job_completion(response.json()['run_id'])
    else:
        print(f"Failed to trigger job: {response.text}")


# COMMAND ----------

import datetime

# Helper function to find the first and last day of the previous month
def get_previous_month_range(today):
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - datetime.timedelta(days=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
    return first_day_of_previous_month, last_day_of_previous_month

# Helper function to find the Monday (start) and Sunday (end) of the previous week
def get_previous_week_range(today):
    last_week_end = today - datetime.timedelta(days=today.weekday() + 1)  # Previous Sunday
    last_week_start = last_week_end - datetime.timedelta(days=6)  # Previous Monday
    return last_week_start, last_week_end

# Helper function to find the first and last day of the previous year
def get_previous_year_range(today):
    first_day_of_previous_year = datetime.date(today.year - 1, 1, 1)
    last_day_of_previous_year = datetime.date(today.year - 1, 12, 31)
    return first_day_of_previous_year, last_day_of_previous_year

# Get today's date
today = datetime.date.today()

# Execution criteria check
if frequency == 'daily':
    # For daily, consider "yesterday" as the last complete period
    beg_date = exp_date = today - datetime.timedelta(days=1)
    trigger_job(beg_date, exp_date, today)

elif frequency == 'monthly' and today.day == 1:
    beg_date, exp_date = get_previous_month_range(today)
    trigger_job(beg_date, exp_date, today)

elif frequency == 'weekly' and today.weekday() == 0:  # 0: Monday
    beg_date, exp_date = get_previous_week_range(today)
    trigger_job(beg_date, exp_date, today)

elif frequency == 'yearly' and today == datetime.date(today.year, 1, 1):
    beg_date, exp_date = get_previous_year_range(today)
    trigger_job(beg_date, exp_date, today)

else:
    print("Today does not meet the criteria for executing the job based on the specified frequency.")

