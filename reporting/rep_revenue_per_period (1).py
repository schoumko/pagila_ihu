#!/usr/bin/env python
# coding: utf-8

# # Setup - Install Libraries

# In[6]:


import os
from google.cloud import bigquery

# Ορισμός του κλειδιού χρησιμοποιώντας τη δική σου διαδρομή
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\User1\Documents\pagila_course\staging\notebook\pagila-e61a459f9ecf.json"

# Δημιουργία του client
client = bigquery.Client()

# Τώρα μπορείς να τρέξεις το query σου παρακάτω...
print("Credentials set successfully!")


# In[7]:


from google.cloud import bigquery

# Initialize the BigQuery client
client = bigquery.Client()

# Το SQL query σου ακριβώς όπως το δουλέψαμε
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

# Εκτέλεση και μετατροπή σε DataFrame για να το δούμε στο Notebook
df = client.query(sql_query).to_dataframe()

# Εμφάνιση των πρώτων γραμμών
df.head(30)

