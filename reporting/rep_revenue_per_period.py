#!/usr/bin/env python
# coding: utf-8

# # Setup - Install Libraries

# In[4]:


import os

from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\User1\Documents\pagila_course\staging\notebook\pagila-e61a459f9ecf.json"

client = bigquery.Client()

sql_query = """
WITH payments AS (
  SELECT * FROM `pagila.staging_db.stg_payment`
),

reporting_dates AS (
  SELECT * FROM `pagila.reporting_db.reporting_periods_table`
  WHERE reporting_period IN ('Day', 'Month', 'Year')
),

revenue_per_period AS (
  SELECT 'Day' AS reporting_period, DATE_TRUNC(payment_date, DAY) AS reporting_date, SUM(payment_amount) AS total_revenue
  FROM payments GROUP BY 1, 2
  UNION ALL
  SELECT 'Month', DATE_TRUNC(payment_date, MONTH), SUM(payment_amount)
  FROM payments GROUP BY 1, 2
  UNION ALL
  SELECT 'Year', DATE_TRUNC(payment_date, YEAR), SUM(payment_amount)
  FROM payments GROUP BY 1, 2
)

SELECT 
    rd.reporting_period,
    rd.reporting_date,
    COALESCE(rpp.total_revenue, 0) AS total_revenue
FROM reporting_dates rd
LEFT JOIN revenue_per_period rpp 
    ON rd.reporting_date = rpp.reporting_date 
    AND rd.reporting_period = rpp.reporting_period
"""

df = client.query(sql_query).to_dataframe()

df.head(100)


# In[5]:


from google.cloud import bigquery

client = bigquery.Client()

sql_query = """
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


df = client.query(sql_query).to_dataframe()


df.head(30)

