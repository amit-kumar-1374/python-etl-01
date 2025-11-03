import pandas as pd
import os
import io
import oracledb
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SCHEMA = os.getenv("SCHEMA")
TABLE = "orderdetails"
ORDERDETAILS_COL=os.getenv("ORDERDETAILS_COL")

user = os.getenv("ORACLE_USER")
password = os.getenv("ORACLE_PASSWORD")
host = os.getenv("ORACLE_HOST")
port = os.getenv("ORACLE_PORT")        
service = os.getenv("ORACLE_SERVICE")


BATCH_DATE =os.getenv("BATCH_DATE")   

if BATCH_DATE == "2001-01-01":
    remote_schema = "CM_20050609"
    remote_password = "CM_20050609123"
else:
    remote_schema = f"CM_{BATCH_DATE.replace('-', '')}"
    remote_password = f"{remote_schema}123"

def prepare_dblink(cursor):
    cursor.execute("ALTER SESSION SET CURRENT_SCHEMA = j25Amit")
    try: 
        cursor.execute("DROP PUBLIC DATABASE LINK amit_dblink")
    except Exception: 
        pass
    sql = f"""
    CREATE PUBLIC DATABASE LINK amit_dblink
    CONNECT TO {remote_schema} IDENTIFIED BY "{remote_password}"
    USING '(DESCRIPTION=
      (ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))
      (CONNECT_DATA=(SERVICE_NAME={service}))
    )'
    """
    cursor.execute(sql)
    print(f"DBLink created for {remote_schema}")


def get_connection():
    """Create Oracle connection"""
    

    dsn = f"{host}:{port}/{service}"
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    return conn


def upload_to_s3(df, bucket_name, s3_key):
    """Upload DataFrame as CSV directly to S3"""
    # s3_client = boto3.client(
    #     "s3",
    #     aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    #     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    #     region_name=os.getenv("AWS_REGION")
    # )
    s3_client = boto3.client("s3")
    # Write DataFrame to CSV in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Upload to S3
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    print(f"✅ {s3_key} uploaded successfully to S3 bucket: {bucket_name}")


def orderdetails():
    """Extract orderdetails data from Oracle and upload to S3"""
    print("Connecting to Oracle...")
    conn = get_connection()
    cur = conn.cursor()
    prepare_dblink(cur)

    query = f"""
        SELECT
            {ORDERDETAILS_COL}
        FROM {TABLE}@amit_dblink
        WHERE UPDATE_TIMESTAMP >= TO_DATE('{BATCH_DATE}','YYYY-MM-DD')
    """

    df = pd.read_sql(query, conn)
    print(f"Fetched {len(df)} records from {TABLE}@amit_dblink")

    # Upload directly to S3
    bucket_name = os.getenv("S3_BUCKET_NAME")
    s3_key = f"{TABLE.upper()}/{BATCH_DATE}/{TABLE}.csv"
    upload_to_s3(df, bucket_name, s3_key)

    conn.close()
    print("Oracle connection closed.")


if __name__ == "__main__":
    orderdetails()
