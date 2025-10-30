import pandas as pd
import os
import oracledb
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SCHEMA = os.getenv("SCHEMA")
TABLE = "productlines"

def get_connection():
    """Connect to Oracle database."""
    user = os.getenv("ORACLE_USER")
    password = os.getenv("ORACLE_PASSWORD")
    host = os.getenv("ORACLE_HOST")
    port = os.getenv("ORACLE_PORT")
    service = os.getenv("ORACLE_SERVICE")

    dsn = f"{host}:{port}/{service}"
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    return conn


def get_s3_client():
    """Initialize and return a boto3 S3 client."""
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


def cm_productlines():
    print("Connecting to Oracle...")
    conn = get_connection()

    query = f"""
        SELECT
            PRODUCTLINE,
            TEXTDESCRIPTION,
            HTMLDESCR
