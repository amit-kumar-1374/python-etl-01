import oracledb
import os
from dotenv import load_dotenv

load_dotenv()

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