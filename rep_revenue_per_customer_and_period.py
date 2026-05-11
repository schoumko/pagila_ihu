#!/usr/bin/env python
# coding: utf-8

# # Setup - Install Libraries

# In[1]:


import os
from google.cloud import bigquery
# Προσοχή: Το .json πρέπει να είναι στον ίδιο φάκελο
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "pagila-e61a459f9ecf.json"
client = bigquery.Client(project="pagila")


# # Import libraries

# In[4]:


from google.cloud import bigquery

client = bigquery.Client()

sql_query = """
WITH payments AS (
  select * from `pagila.staging_db.stg_payment`
)
, customers as (
  select * from `pagila.staging_db.stg_customer`
)
, reporting_dates as (
  select * from `pagila.reporting_db.reporting_periods_table`
  where reporting_period in ('Day', 'Month', 'Year')
)
, revenue_per_period as (
  select
  'Day' as reporting_period, 
  date_trunc(payment_date, day) as reporting_date, 
  customers.customer_id,
  sum(payment_amount) as total_revenue
  from payments 
  left join customers on payments.customer_id = customers.customer_id
  group by 1, 2, 3

  UNION ALL

  SELECT 
  'Month' AS reporting_period, 
  DATE_TRUNC(payment_date, month) AS reporting_date,
  customers.customer_id, 
  SUM(payment_amount) AS total_revenue
  FROM payments
  left join customers on payments.customer_id = customers.customer_id 
  GROUP BY 1, 2, 3

  UNION ALL

  SELECT 
  'Year' AS reporting_period, 
  DATE_TRUNC(payment_date, year) AS reporting_date, 
  customers.customer_id, 
  SUM(payment_amount) AS total_revenue
  FROM payments 
  left join customers on payments.customer_id = customers.customer_id 
  GROUP BY 1, 2, 3
),
final AS (
  SELECT 
  rd.reporting_period,
  rd.reporting_date,
  rpp.customer_id,
  rpp.total_revenue
  FROM reporting_dates rd
  INNER JOIN revenue_per_period rpp 
  ON rd.reporting_date = rpp.reporting_date 
  AND rd.reporting_period = rpp.reporting_period
  WHERE rd.reporting_period = 'Day'

  UNION ALL

  SELECT 
  rd.reporting_period,
  rd.reporting_date,
  rpp.customer_id,
  rpp.total_revenue
  FROM reporting_dates rd
  INNER JOIN revenue_per_period rpp 
  ON rd.reporting_date = rpp.reporting_date 
  AND rd.reporting_period = rpp.reporting_period
  WHERE rd.reporting_period = 'Month'

  UNION ALL

  SELECT 
  rd.reporting_period,
  rd.reporting_date,
  rpp.customer_id,
  rpp.total_revenue
  FROM reporting_dates rd
  INNER JOIN revenue_per_period rpp 
  ON rd.reporting_date = rpp.reporting_date 
  AND rd.reporting_period = rpp.reporting_period
  WHERE rd.reporting_period = 'Year'
)
SELECT * FROM final
"""

# Εκτέλεση
df_customer = client.query(sql_query).to_dataframe()

# Εμφάνιση
df_customer.head()


# In[15]:


# Set the environment variable for Google Cloud credentials
# Place the path in which the .json file is located.

# Example (if .json is located in the same directory with the notebook)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "at-arch-416714-6f9900ec7.json"

# -- YOUR CODE GOES BELOW THIS LINE
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/User1/Documents/pagila_course/pagila-e61a459f9ecf.json" # Edit path
# -- YOUR CODE GOES ABOVE THIS LINE


# In[17]:


# Set your Google Cloud project ID and BigQuery dataset details

# -- YOUR CODE GOES BELOW THIS

project_id = 'pagila' # Edit with your project id
dataset_id = 'staging_db' # Modify the necessary schema name: staging_db, reporting_db etc.
table_id = 'stg_actor' # Modify the necessary table name: stg_customer, stg_city etc.

# -- YOUR CODE GOES ABOVE THIS LINE


# # SQL Query

# In[21]:


# Create a BigQuery client
client = bigquery.Client(project=project_id)

# -- YOUR CODE GOES BELOW THIS LINE

# Define your SQL query here
query = """
with base as (
  select *
  from `pagila.public.actor` --Your table path
  )

  , final as (
    select
        actor_id
        , first_name as actor_first_name
        , last_name as actor_last_name
        , last_update as actor_last_update
   FROM base
  )

  select * from final
"""

# -- YOUR CODE GOES ABOVE THIS LINE

# Execute the query and store the result in a dataframe
df = client.query(query).to_dataframe()

# Explore some records
df.head()


# # Write to BigQuery

# In[26]:


# Define the full table ID
full_table_id = f"{project_id}.{dataset_id}.{table_id}"

# -- YOUR CODE GOES BELOW THIS LINE
# Define table schema based on the project description

schema = [
    bigquery.SchemaField('actor_id', 'INTEGER'),
    bigquery.SchemaField('actor_first_name', 'STRING'),
    bigquery.SchemaField('actor_last_name', 'STRING'),
    bigquery.SchemaField('actor_last_update', 'DATETIME'),
    ]

# -- YOUR CODE GOES ABOVE THIS LINE


# In[27]:


# Create a BigQuery client
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


# In[2]:


# Below line converts your i.pynb file to .py python executable file. Modify the input and output names based
# on the table you are processing.
# Example:
# ! jupyter nbconvert stg_customer.ipynb --to python

# -- YOUR CODE GOES BELOW THIS LINE

get_ipython().system('python3 -m jupyter nbconvert stg_actor.ipynb --to python')

# -- YOUR CODE GOES ABOVE THIS LINE


# In[32]:


get_ipython().system('python3 -m pip install nbconvert')


# In[1]:


get_ipython().system('python3 -m pip install nbconvert -U')


# In[ ]:




