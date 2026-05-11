#!/usr/bin/env python
# coding: utf-8

# # Setup - Install Libraries

# In[17]:


import os
from google.cloud import bigquery

# Ορισμός του κλειδιού χρησιμοποιώντας τη δική σου διαδρομή
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\User1\Documents\pagila_course\staging\notebook\pagila-e61a459f9ecf.json"

# Δημιουργία του client
client = bigquery.Client()

# Τώρα μπορείς να τρέξεις το query σου παρακάτω...
print("Credentials set successfully!")


# # Import libraries

# In[16]:


from google.cloud import bigquery
import pandas as pd

# 1. Αρχικοποίηση του BigQuery Client
client = bigquery.Client()

# 2. Το SQL Query σου ακριβώς όπως το έφτιαξες στο BigQuery
sql_query = """
with payments as (
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


# In[18]:


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

