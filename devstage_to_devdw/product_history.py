import psycopg2
import sys
import os 
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_redshift_connection

SRC_SCHEMA = os.getenv("REDSHIFT_SCHEMA")
TGT_SCHEMA = os.getenv("REDSHIFT_SCHEMA1")
PRODUCTS_TABLE = "products"
PRODUCT_HISTORY_TABLE = "product_history"
BATCH_CONTROL_TABLE = "j25amit_etl_metadata.batch_control"

def product_history():
    print("Starting Product History ETL...")

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

        # 1️⃣ Update historical product records where MSRP changed
        update_sql = f"""
            UPDATE {TGT_SCHEMA}.{PRODUCT_HISTORY_TABLE} ph
            SET
                dw_active_record_ind = 0,
                effective_to_date = DATEADD(day, -1, '{ETL_BATCH_DATE}'),
                dw_update_timestamp = CURRENT_TIMESTAMP,
                update_etl_batch_no = {ETL_BATCH_NO},
                update_etl_batch_date = '{ETL_BATCH_DATE}'
            FROM (
                SELECT dw_product_id, MSRP
                FROM {TGT_SCHEMA}.{PRODUCTS_TABLE}
                WHERE src_update_timestamp >= '{ETL_BATCH_DATE}'
                  AND src_create_timestamp < '{ETL_BATCH_DATE}'
            ) p
            WHERE ph.dw_product_id = p.dw_product_id
              AND ph.MSRP <> p.MSRP
              AND ph.dw_active_record_ind = 1;
        """
        cur.execute(update_sql)
        print(f"Updated product history records: {cur.rowcount}")

        # 2️⃣ Insert new active product history records
        insert_sql = f"""
            INSERT INTO {TGT_SCHEMA}.{PRODUCT_HISTORY_TABLE} (
                dw_product_id,
                MSRP,
                effective_from_date,
                effective_to_date,
                dw_active_record_ind,
                dw_create_timestamp,
                dw_update_timestamp,
                create_etl_batch_no,
                create_etl_batch_date,
                update_etl_batch_no,
                update_etl_batch_date
            )
            SELECT 
                p.dw_product_id,
                p.MSRP,
                '{ETL_BATCH_DATE}',
                NULL,
                1,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                {ETL_BATCH_NO},
                '{ETL_BATCH_DATE}',
                NULL,
                NULL
            FROM {TGT_SCHEMA}.{PRODUCTS_TABLE} p
            LEFT JOIN (
                SELECT dw_product_id
                FROM {TGT_SCHEMA}.{PRODUCT_HISTORY_TABLE}
                WHERE dw_active_record_ind = 1
            ) ph
            ON p.dw_product_id = ph.dw_product_id
            WHERE ph.dw_product_id IS NULL;
        """
        cur.execute(insert_sql)
        print(f"Inserted new product history records: {cur.rowcount}")

        conn.commit()
        print("Product History ETL completed successfully.")

    except Exception as e:
        conn.rollback()
        print("Error during Product History ETL:", str(e))

    finally:
        cur.close()
        conn.close()
        print("Redshift connection closed.")


if __name__ == "__main__":
    product_history()
