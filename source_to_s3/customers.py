import pandas as pd
import os
import io
import psycopg2
import oracledb
import boto3
import sys
from dotenv import load_dotenv
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_connection,upload_to_s3,prepare_dblink,get_redshift_connection

# Load environment variables
load_dotenv()
 
TABLE = "customers"
CUSTOMERS_COL=os.getenv("CUSTOMERS_COL")

BATCH_DATE =os.getenv("BATCH_DATE") 


def customers():
    """Extract data from Oracle and upload to S3"""
    print("Connecting to Oracle...")
    conn = get_connection()
    cur = conn.cursor()

    conn_redshift = get_redshift_connection()

    cur_redshift = conn_redshift.cursor()

    cur_redshift.execute("SELECT etl_batch_date FROM j25amit_etl_metadata.batch_control;")

    # --- Fetch the single value ---
    BATCH_DATE = cur_redshift.fetchone()[0]

    prepare_dblink(cur,BATCH_DATE)

    query = f"""
        SELECT
             {CUSTOMERS_COL}
        FROM {TABLE}@amit_dblink
        WHERE UPDATE_TIMESTAMP >= TO_DATE('{BATCH_DATE}','YYYY-MM-DD')
        
    """
    
    df = pd.read_sql_query(query, conn, dtype_backend = "pyarrow")
    print(f"Fetched {len(df)} rows from {TABLE}@amit_dblink")

    # Upload to S3
    bucket_name = os.getenv("S3_BUCKET_NAME")
    s3_key = f"{TABLE.upper()}/{BATCH_DATE}/{TABLE}.csv"
    upload_to_s3(df, bucket_name, s3_key)




    conn.close()
    print("Connection closed.")


if __name__ == "__main__":
    customers()
