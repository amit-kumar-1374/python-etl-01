import pandas as pd
import os
import io
import oracledb
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SCHEMA = os.getenv("SCHEMA")
TABLE = "productlines"

def get_connection():
    """Create Oracle DB connection using environment variables."""
    user = os.getenv("ORACLE_USER")
    password = os.getenv("ORACLE_PASSWORD")
    host = os.getenv("ORACLE_HOST")
    port = os.getenv("ORACLE_PORT")
    service = os.getenv("ORACLE_SERVICE")

    dsn = f"{host}:{port}/{service}"
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    return conn


def upload_to_s3(df, bucket_name, s3_key):
    """Upload DataFrame as CSV directly to S3."""
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "ap-south-1")
    )

    # Convert DataFrame to in-memory CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Upload directly to S3
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    print(f"✅ {s3_key} uploaded successfully to S3 bucket: {bucket_name}")


def cm_productlines():
    """Extract productlines data from Oracle and upload to S3."""
    print("Connecting to Oracle...")
    conn = get_connection()

    query = f"""
        SELECT
            PRODUCTLINE,
            TEXTDESCRIPTION,
            HTMLDESCRIPTION,
            IMAGE
        FROM {SCHEMA}.{TABLE}
    """

    df = pd.read_sql(query, conn)
    print(f"Fetched {len(df)} rows from {SCHEMA}.{TABLE}")

    # Upload directly to S3
    bucket_name = os.getenv("S3_BUCKET_NAME")
    s3_key = f"{TABLE}.csv"
    upload_to_s3(df, bucket_name, s3_key)

    conn.close()
    print("Oracle connection closed.")


if __name__ == "__main__":
    cm_productlines()
