import pandas as pd
import os
import io
import oracledb
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SCHEMA = os.getenv("SCHEMA")
TABLE = "employees"
EMPLOYEES_COL=os.getenv("EMPLOYEES_COL")

BATCH_DATE =os.getenv("BATCH_DATE")  # Example input

if BATCH_DATE == "2001-01-01":
    SCHEMA = os.getenv("SCHEMA1")
elif BATCH_DATE == "2005-06-10":
    SCHEMA = os.getenv("SCHEMA2")
elif BATCH_DATE == "2005-06-11":
    SCHEMA = os.getenv("SCHEMA3")
elif BATCH_DATE == "2005-06-12":
    SCHEMA = os.getenv("SCHEMA4")
elif BATCH_DATE == "2005-06-13":
    SCHEMA = os.getenv("SCHEMA5")
elif BATCH_DATE == "2005-06-14":
    SCHEMA = os.getenv("SCHEMA6")
else:
    SCHEMA = os.getenv("DEFAULT_SCHEMA")  

print(f"Using schema: {SCHEMA}")


def get_connection():
    """Create Oracle DB connection"""
    user = os.getenv("ORACLE_USER")
    password = os.getenv("ORACLE_PASSWORD")
    host = os.getenv("ORACLE_HOST")
    port = os.getenv("ORACLE_PORT")
    service = os.getenv("ORACLE_SERVICE")

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
    # Write DataFrame to in-memory CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Upload to S3
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    print(f"✅ {s3_key} uploaded successfully to S3 bucket: {bucket_name}")


def employees():
    """Extract employees data from Oracle and upload to S3"""
    print("Connecting to Oracle...")
    conn = get_connection()

    query = f"""
        SELECT
            {EMPLOYEES_COL}
        FROM {SCHEMA}.{TABLE}
        WHERE UPDATE_TIMESTAMP >= TO_DATE('{BATCH_DATE}','YYYY-MM-DD')
    """

    df = pd.read_sql(query, conn)
    print(f"Fetched {len(df)} records from {SCHEMA}.{TABLE}")

    # Upload directly to S3
    bucket_name = os.getenv("S3_BUCKET_NAME")
    s3_key = f"{TABLE.upper()}/{BATCH_DATE}/{TABLE}.csv"
    upload_to_s3(df, bucket_name, s3_key)

    conn.close()
    print("Oracle connection closed.")


if __name__ == "__main__":
    employees()
