import pandas as pd
import os
import oracledb

from dotenv import load_dotenv

load_dotenv()


SCHEMA = os.getenv("SCHEMA")
TABLE = "payments"

def get_connection():
    
    user = os.getenv("ORACLE_USER")
    password = os.getenv("ORACLE_PASSWORD")
    host = os.getenv("ORACLE_HOST")
    port = os.getenv("ORACLE_PORT")        
    service = os.getenv("ORACLE_SERVICE")

    dsn = f"{host}:{port}/{service}"
    conn = oracledb.connect(user=user, password=password, dsn=dsn)

    return conn


def get_output_path():
    """Returns the CSV output directory (creates if missing)."""
    output_path = os.getenv("CSV_OUTPUT_PATH", "./output")
    os.makedirs(output_path, exist_ok=True)
    return output_path


def cm_payments():
    print("connenting to oracle")
    conn=get_connection()
    query=f"""SELECT
    CUSTOMERNUMBER,
    CHECKNUMBER,
    PAYMENTDATE,
    AMOUNT
     FROM {SCHEMA}.{TABLE}"""
    df=pd.read_sql(query,conn)

    output_dir=get_output_path()
    output_file=os.path.join(output_dir,f"{TABLE}.csv")
    df.to_csv(output_file,index=False)

    print(f" {TABLE}.csv downloaded successfully to : {output_file}")
    conn.close()

if __name__ == "__main__":
     
    cm_payments()

