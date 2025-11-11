import psycopg2
import sys
import os 
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_redshift_connection

SRC_SCHEMA = os.getenv("REDSHIFT_SCHEMA")
TGT_SCHEMA = os.getenv("REDSHIFT_SCHEMA1")
SRC_TABLE = "productlines"
TGT_TABLE = "productlines"
BATCH_CONTROL_TABLE = "j25amit_etl_metadata.batch_control"

def productlines():
    print("Starting Product Lines ETL...")

    conn = get_redshift_connection()
    cur = conn.cursor()

    try:
        # Get current batch info
        cur.execute(f"SELECT etl_batch_no, etl_batch_date FROM {BATCH_CONTROL_TABLE};")
        result = cur.fetchone()
        if not result:
            raise Exception("No batch record found in batch_control table")

        ETL_BATCH_NO, ETL_BATCH_DATE = result
        print(f"ETL Batch No: {ETL_BATCH_NO}, Batch Date: {ETL_BATCH_DATE}")

        # 1️⃣ Update existing product lines
        update_sql = f"""
            UPDATE {TGT_SCHEMA}.{TGT_TABLE} d
            SET
                textDescription = s.textDescription,
                htmlDescription = s.htmlDescription,
                image = s.image,
                src_update_timestamp = s.update_timestamp,
                dw_update_timestamp = CURRENT_TIMESTAMP,
                etl_batch_no = {ETL_BATCH_NO},
                etl_batch_date = '{ETL_BATCH_DATE}'
            FROM {SRC_SCHEMA}.{SRC_TABLE} s
            WHERE s.productLine = d.productLine
              AND s.update_timestamp >= '{ETL_BATCH_DATE}';
        """
        cur.execute(update_sql)
        print(f"Updated product lines: {cur.rowcount}")

        # 2️⃣ Insert new product lines
        insert_sql = f"""
            INSERT INTO {TGT_SCHEMA}.{TGT_TABLE} (
                productLine, textDescription, htmlDescription, image,
                src_create_timestamp, src_update_timestamp,
                dw_create_timestamp, dw_update_timestamp,
                etl_batch_no, etl_batch_date
            )
            SELECT 
                s.productLine, s.textDescription, s.htmlDescription, s.image,
                s.create_timestamp, s.update_timestamp,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                {ETL_BATCH_NO}, '{ETL_BATCH_DATE}'
            FROM {SRC_SCHEMA}.{SRC_TABLE} s
            LEFT JOIN {TGT_SCHEMA}.{TGT_TABLE} d
              ON s.productLine = d.productLine
            WHERE s.create_timestamp >= '{ETL_BATCH_DATE}'
              AND d.productLine IS NULL;
        """
        cur.execute(insert_sql)
        print(f"Inserted product lines: {cur.rowcount}")

        conn.commit()
        print("Product Lines ETL completed successfully.")

    except Exception as e:
        conn.rollback()
        print("Error during Product Lines ETL:", str(e))

    finally:
        cur.close()
        conn.close()
        print("Redshift connection closed.")


if __name__ == "__main__":
    productlines()
