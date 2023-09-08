import sys
import psycopg2
import pandas as pd
from snowflake.snowpark.session import Session

conn = psycopg2.connect(db_name='defaultdb', user ='doadmin', host = 'db-posgresql-nyc1-45157-do-user-8304997-0.b.db.ondigitalocean.com',
                        password = 'AVNS_ZcJYYS7gzye8fP8vpbf', port ='25060')

cur = conn.cursor()
cur.execute("select")