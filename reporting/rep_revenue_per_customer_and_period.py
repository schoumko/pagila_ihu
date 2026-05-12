#!/usr/bin/env python
# coding: utf-8

# # Setup - Install Libraries

# In[1]:


import os
from google.cloud import bigquery
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\User1\Documents\pagila_course\staging\notebook\pagila-e61a459f9ecf.json"
client = bigquery.Client()


# # Import libraries

# In[2]:


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
  having sum(payments.payment_amount) > 0

  UNION ALL

  SELECT 
  'Month' AS reporting_period, 
  DATE_TRUNC(payment_date, month) AS reporting_date,
  customers.customer_id, 
  SUM(payment_amount) AS total_revenue
  FROM payments
  left join customers on payments.customer_id = customers.customer_id 
  GROUP BY 1, 2, 3
  having sum(payments.payment_amount) > 0

  UNION ALL

  SELECT 
  'Year' AS reporting_period, 
  DATE_TRUNC(payment_date, year) AS reporting_date, 
  customers.customer_id, 
  SUM(payment_amount) AS total_revenue
  FROM payments 
  left join customers on payments.customer_id = customers.customer_id 
  GROUP BY 1, 2, 3
  having sum(payments.payment_amount) > 0
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
df_customer.head(30)

