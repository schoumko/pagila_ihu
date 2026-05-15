#!/usr/bin/env python
# coding: utf-8

# In[1]:


from google.cloud import bigquery
import pandas as pd
from pandas_gbq import to_gbq
import os

print('Libraries imported successfully')


# In[2]:


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/User1/Documents/pagila_course/pagila-e61a459f9ecf.json"


# In[3]:


project_id = 'pagila' # Edit with your project id
dataset_id = 'reporting_db' # Modify the necessary schema name: staging_db, reporting_db etc.
table_id = 'rep_revenue_per_period' # Modify the necessary table name: stg_customer, stg_city etc.g


# In[6]:


client = bigquery.Client(project=project_id)
query = """
with payments as (
  select * 
  from pagila.staging_db.stg_payment
)

, reporting_dates as (
  select * 
  from pagila.reporting_db.reporting_periods_table
  where reporting_period in ('Day', 'Month', 'Year')
)

, revenue_per_period as (
  select
  'Day' as reporting_period, 
  date_trunc(payment_date,day) as reporting_date, 
  sum(payment_amount) as total_revenue
  from payments 
  group by 1, 2

  UNION ALL

  SELECT 
 'Month' AS reporting_period, 
  DATE_TRUNC(payment_date, month) AS reporting_date, 
  SUM(payment_amount) AS total_revenue
  FROM payments 
  GROUP BY 1, 2

  UNION ALL

  SELECT 
 'Year' AS reporting_period, 
  DATE_TRUNC(payment_date, year) AS reporting_date, 
  SUM(payment_amount) AS total_revenue
  FROM payments 
  GROUP BY 1, 2
),

final AS (
  SELECT 
  rd.reporting_period
 , rd.reporting_date
 , COALESCE(rpp.total_revenue, 0) AS total_revenue
  FROM reporting_dates rd
  LEFT JOIN revenue_per_period rpp 
 ON rd.reporting_date = rpp.reporting_date 
 AND rd.reporting_period = rpp.reporting_period
  WHERE rd.reporting_period = 'Day'

  UNION ALL

  SELECT 
  rd.reporting_period
 , rd.reporting_date
 , COALESCE(rpp.total_revenue, 0) AS total_revenue
  FROM reporting_dates rd
  LEFT JOIN revenue_per_period rpp 
 ON rd.reporting_date = rpp.reporting_date 
  AND rd.reporting_period = rpp.reporting_period
  WHERE rd.reporting_period = 'Month'

  UNION ALL

  SELECT 
  rd.reporting_period
  , rd.reporting_date
  , COALESCE(rpp.total_revenue, 0) AS total_revenue
  FROM reporting_dates rd
  LEFT JOIN revenue_per_period rpp 
 ON rd.reporting_date = rpp.reporting_date 
 AND rd.reporting_period = rpp.reporting_period
  WHERE rd.reporting_period = 'Year'
)

SELECT * FROM final 
"""
df = client.query(query).to_dataframe()

# Explore some records
df.head()


# In[7]:


full_table_id = f"{project_id}.{dataset_id}.{table_id}"

# -- YOUR CODE GOES BELOW THIS LINE
# Define table schema based on the project description

schema = [
    bigquery.SchemaField('reporting_period', 'STRING'),
    bigquery.SchemaField('reporting_date', 'DATETIME'),
    bigquery.SchemaField('total_revenue', 'NUMERIC'),
    ]
client = bigquery.Client(project=project_id)

# Check if the table exists
def table_exists(client, full_table_id):
    try:
        client.get_table(full_table_id)
        return True
    except Exception:
        return False

# Write the dataframe to the table (overwrite if it exists, create if it doesn't)
if table_exists(client, full_table_id):
    # If the table exists, overwrite it
    destination_table = f"{dataset_id}.{table_id}"
    # Write the dataframe to the table (overwrite if it exists)
    to_gbq(df, destination_table, project_id=project_id, if_exists='replace')
    print(f"Table {full_table_id} exists. Overwritten.")
else:
    # If the table does not exist, create it
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Table {full_table_id} did not exist. Created and data loaded.")

