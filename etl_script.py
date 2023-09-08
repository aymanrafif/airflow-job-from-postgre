import sys
import psycopg2
import pandas as pd
from snowflake.snowpark.session import Session

conn = psycopg2.connect(dbname='defaultdb', user='doadmin', host='db-postgresql-nyc1-45157-do-user-8304997-0.b.db.ondigitalocean.com', password='AVNS_ZcJYYS7gzye8fP8vpbf', port='25060')
cur = conn.cursor()
cur.execute("select order_id ,product_id ,unit_price ,quantity ,discount from defaultdb.public.order_details;")
df = pd.DataFrame(cur.fetchall(), columns=['order_id', 'product_id', 'unit_price','quantity','discount'])
cur.close()
conn.close()

snowflake_conn_prop = {
   "account": "ie97047.ap-southeast-1",
   "user": "company2",
   "password": "Password*1",
   "role": "ACCOUNTADMIN",
   "database": "PROJECT_5",
   "schema": "PUBLIC",
   "warehouse": "COMPUTE_WH",
}

session = Session.builder.configs(snowflake_conn_prop).create()
session.sql("use role accountadmin").collect()
session.sql("use database {}".format(snowflake_conn_prop['database'])).collect()
session.sql("use schema {}".format(snowflake_conn_prop['schema'])).collect()
session.sql("use warehouse {}".format(snowflake_conn_prop['warehouse']))
print(session.sql('select current_warehouse(), current_database(), current_schema()').collect())


the_list = list(df.itertuples(index=False))

snowpark_df = session.create_dataframe(the_list)
snowpark_df.write.mode("overwrite").saveAsTable("ayman_gross_revenue")
session.close()

