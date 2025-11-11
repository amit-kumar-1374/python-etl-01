import os
import psycopg2
import boto3
import sys
from dotenv import load_dotenv
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_connection,upload_to_s3,prepare_dblink,get_redshift_connection



# Load environment variables
load_dotenv()


# Config
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
TABLE = "offices"
#BATCH_DATE = os.getenv("BATCH_DATE")
SCHEMA = os.getenv("REDSHIFT_SCHEMA")
REGION = os.getenv("AWS_REGION")
DATABASE = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASS = os.getenv("REDSHIFT_PASS")
IAM_ROLE_ARN = os.getenv("REDSHIFT_IAM_ROLE_ARN")
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT"))


def load_from_s3_to_redshift():
    """Truncate Redshift table and load CSV data from S3"""
    print(f" Connecting to Redshift at host {REDSHIFT_HOST} ...")
    conn = get_redshift_connection()
    cur = conn.cursor()

    cur.execute("SELECT etl_batch_date FROM j25amit_etl_metadata.batch_control;")

    # --- Fetch the single value ---
    BATCH_DATE = cur.fetchone()[0]

    S3_PATH = f"s3://{S3_BUCKET}/{TABLE.upper()}/{BATCH_DATE}/{TABLE}.csv"



    print(f" Target: {SCHEMA}.{TABLE}")
    print(f" Source: {S3_PATH}")

    # Step 1: Truncate table (quote schema for case sensitivity)
    truncate_sql = f'TRUNCATE TABLE {SCHEMA}.{TABLE};'
    print(" Truncating table before load...")
    cur.execute(truncate_sql)
    conn.commit()

    # Step 2: COPY data from S3 (quote schema for case sensitivity)
    copy_sql = f"""
    COPY {SCHEMA}.{TABLE}
    FROM '{S3_PATH}'
    IAM_ROLE '{IAM_ROLE_ARN}'
    REGION '{REGION}'
    FORMAT AS CSV
    IGNOREHEADER 1
    TIMEFORMAT 'auto'
    ACCEPTINVCHARS;
    """
    print(" Running COPY command ...")
    cur.execute(copy_sql)
    conn.commit()

    print(f" Successfully loaded data into {SCHEMA}.{TABLE}")
    cur.close()
    conn.close()


if __name__ == "__main__":
    load_from_s3_to_redshift()
