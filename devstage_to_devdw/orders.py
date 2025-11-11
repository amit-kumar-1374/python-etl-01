import psycopg2
import sys
import os 
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_redshift_connection

SRC_SCHEMA = os.getenv("REDSHIFT_SCHEMA")
TGT_SCHEMA = os.getenv("REDSHIFT_SCHEMA1")
SRC_TABLE = "orders"
TGT_TABLE = "orders"
CUSTOMERS_TABLE = "customers"
BATCH_CONTROL_TABLE = "j25amit_etl_metadata.batch_control"

def orders():
    print("Starting Orders ETL...")

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

        # 1️⃣ Update existing orders
        update_sql = f"""
            UPDATE {TGT_SCHEMA}.{TGT_TABLE} d
            SET
                orderDate = s.orderDate,
                requiredDate = s.requiredDate,
                shippedDate = s.shippedDate,
                status = s.status,
                comments = s.comments,
                src_customerNumber = s.customerNumber,
                dw_customer_id = c.dw_customer_id,
                cancelledDate = s.cancelledDate,
                src_update_timestamp = s.update_timestamp,
                dw_update_timestamp = CURRENT_TIMESTAMP,
                etl_batch_no = {ETL_BATCH_NO},
                etl_batch_date = '{ETL_BATCH_DATE}'
            FROM {SRC_SCHEMA}.{SRC_TABLE} s
            JOIN {TGT_SCHEMA}.{CUSTOMERS_TABLE} c
              ON s.customerNumber = c.src_customerNumber
            WHERE s.orderNumber = d.src_orderNumber
              AND s.update_timestamp >= '{ETL_BATCH_DATE}';
        """
        cur.execute(update_sql)
        print(f"Updated orders: {cur.rowcount}")

        # 2️⃣ Insert new orders
        insert_sql = f"""
            INSERT INTO {TGT_SCHEMA}.{TGT_TABLE} (
                src_orderNumber, orderDate, requiredDate, shippedDate,
                status, comments, src_customerNumber, dw_customer_id,
                cancelledDate,
                src_create_timestamp, src_update_timestamp,
                dw_create_timestamp, dw_update_timestamp,
                etl_batch_no, etl_batch_date
            )
            SELECT 
                s.orderNumber, s.orderDate, s.requiredDate, s.shippedDate,
                s.status, s.comments, s.customerNumber, c.dw_customer_id,
                s.cancelledDate,
                s.create_timestamp, s.update_timestamp,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                {ETL_BATCH_NO}, '{ETL_BATCH_DATE}'
            FROM {SRC_SCHEMA}.{SRC_TABLE} s
            JOIN {TGT_SCHEMA}.{CUSTOMERS_TABLE} c
              ON s.customerNumber = c.src_customerNumber
            LEFT JOIN {TGT_SCHEMA}.{TGT_TABLE} d
              ON s.orderNumber = d.src_orderNumber
            WHERE s.create_timestamp >= '{ETL_BATCH_DATE}'
              AND d.src_orderNumber IS NULL;
        """
        cur.execute(insert_sql)
        print(f"Inserted orders: {cur.rowcount}")

        conn.commit()
        print("Orders ETL completed successfully.")

    except Exception as e:
        conn.rollback()
        print("Error during Orders ETL:", str(e))

    finally:
        cur.close()
        conn.close()
        print("Redshift connection closed.")


if __name__ == "__main__":
    orders()
