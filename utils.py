import os
import io
import psycopg2
import oracledb
import boto3
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd

load_dotenv()

# ==========================
# Config
# ==========================
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
REGION = os.getenv("AWS_REGION", "eu-north-1")
DATABASE = os.getenv("REDSHIFT_DB", "dev")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASS = os.getenv("REDSHIFT_PASS")
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", 5439))
IAM_ROLE_ARN = os.getenv("REDSHIFT_IAM_ROLE_ARN")
SCHEMA = "j25amit_devstage"  # Redshift schema

# Oracle
ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_HOST = os.getenv("ORACLE_HOST")
ORACLE_PORT = os.getenv("ORACLE_PORT")        
ORACLE_SERVICE = os.getenv("ORACLE_SERVICE")


# ==========================
# Redshift connection
# ==========================
def get_redshift_connection():
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=DATABASE,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASS
    )


# ==========================
# Oracle connection
# ==========================
def get_connection():
    dsn = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"
    return oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn)


def prepare_dblink(cursor, BATCH_DATE):
    BATCH_DATE = str(BATCH_DATE)
    if BATCH_DATE == "2001-01-01":
        remote_schema = "CM_20050609"
        remote_password = "CM_20050609123"
    else:
        remote_schema = f"CM_{BATCH_DATE.replace('-', '')}"
        remote_password = f"{remote_schema}123"

    cursor.execute("ALTER SESSION SET CURRENT_SCHEMA = j25Amit")
    try: 
        cursor.execute("DROP PUBLIC DATABASE LINK amit_dblink")
    except Exception: 
        pass

    sql = f"""
    CREATE PUBLIC DATABASE LINK amit_dblink
    CONNECT TO {remote_schema} IDENTIFIED BY "{remote_password}"
    USING '(DESCRIPTION=
      (ADDRESS=(PROTOCOL=TCP)(HOST={ORACLE_HOST})(PORT={ORACLE_PORT}))
      (CONNECT_DATA=(SERVICE_NAME={ORACLE_SERVICE}))
    )'
    """
    cursor.execute(sql)
    print(f"DBLink created for {remote_schema}")


# ==========================
# Upload DataFrame to S3
# ==========================
def upload_to_s3(df, bucket_name, s3_key):
    s3_client = boto3.client("s3")
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    print(f"Successfully uploaded {s3_key} to S3 bucket '{bucket_name}'")


# ==========================
# ETL Batch Logging
# ==========================
 

def get_new_batch():
    """
    Get current batch_no and batch_date from batch_control.
    Insert into batch_control_log if not exists, else update it.
    Returns batch_no, batch_date
    """
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()
        
        # Fetch batch_no and batch_date from batch_control (assumes only one row)
        cur.execute("SELECT etl_batch_no, etl_batch_date FROM j25amit_etl_metadata.batch_control LIMIT 1")
        row = cur.fetchone()
        if not row:
            raise Exception("No batch found in batch_control")
        
        batch_no, batch_date = row

        # Check if this batch_no exists in batch_control_log
        cur.execute("""
            SELECT 1 FROM j25amit_etl_metadata.batch_control_log
            WHERE etl_batch_no = %s
        """, (batch_no,))
        exists = cur.fetchone()
        
        if exists:
            # Update existing log
            cur.execute("""
                UPDATE j25amit_etl_metadata.batch_control_log
                SET etl_batch_status = %s,
                    etl_batch_start_time = %s
                WHERE etl_batch_no = %s
            """, ('S', datetime.now(), batch_no))
        else:
            # Insert new log
            cur.execute("""
                INSERT INTO j25amit_etl_metadata.batch_control_log 
                    (etl_batch_no, etl_batch_date, etl_batch_status, etl_batch_start_time)
                VALUES (%s, %s, %s, %s)
            """, (batch_no, batch_date, 'S', datetime.now()))
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"[BATCH LOG] Batch ready: {batch_no} ({batch_date})")
        return batch_no, batch_date

    except Exception as e:
        print(f"[BATCH LOG] Failed to create/update batch: {e}")
        raise

def end_batch(batch_no, status):
    """Update batch with status 'C' (Completed) or 'F' (Failed)"""
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE j25amit_etl_metadata.batch_control_log
            SET etl_batch_status = %s, etl_batch_end_time = %s
            WHERE etl_batch_no = %s
        """, (status, datetime.now(), batch_no))
        conn.commit()
        cur.close()
        conn.close()
        print(f"[BATCH LOG] Batch {batch_no} ended with status: {status}")
    except Exception as e:
        print(f"[BATCH LOG] Failed to update batch {batch_no}: {e}")
        raise
