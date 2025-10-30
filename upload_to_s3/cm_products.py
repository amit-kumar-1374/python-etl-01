import pandas as pd
import os
import oracledb
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SCHEMA = os.getenv("SCHEMA")
TABLE = "products"

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


def get_s3_client():
    """Return boto3 S3 client using environment credentials."""
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_REGION", "ap-south-1")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region
    )
    return s3


def cm_products():
    print("Connecting to Oracle...")
    conn = get_connection()

    query = f"""
        SELECT
            PRODUCTCODE,
            PRODUCTNAME,
            PRODUCTLINE,
            PRODUCTSCALE,
            PRODUCTVENDOR,
            PRODUCTDESCRIPTION,
            QUANTITYINSTOCK,
            BUYPRICE,
            MSRP
        FROM {SCHEMA}.{TABLE}
    """

    df = pd.read_sql(query, conn)
    print(f"Fetched {len(df)} rows from {SCHEMA}.{TABLE}")

    # Save CSV temporarily before upload
    temp_csv = f"/tmp/{TABLE}.csv"
    df.to_csv(temp_csv, index=False)

    # Upload to S3
    s3 = get_s3_client()
    bucket_name = os.getenv("S3_BUCKET_NAME")
    s3_key = f"{TABLE}.csv"

    s3.upload_file(temp_csv, bucket_name, s3_key)

    print(f"✅ {TABLE}.csv uploaded successfully to s3://{bucket_name}/{s3_key}")

    # Clean up local file
    os.remove(temp_csv)

    conn.close()


if __name__ == "__main__":
    cm_products()
