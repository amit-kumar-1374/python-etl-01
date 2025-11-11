import psycopg2
import sys
import os 
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_redshift_connection

SRC_SCHEMA = os.getenv("REDSHIFT_SCHEMA")
TGT_SCHEMA = os.getenv("REDSHIFT_SCHEMA1")
ORDERS_SRC = "orderdetails"
ORDERS_TGT = "orderdetails"
ORDERS_TABLE = "orders"
PRODUCTS_TABLE = "products"
BATCH_CONTROL_TABLE = "j25amit_etl_metadata.batch_control"

def orderdetails():
    print("Starting Order Details ETL...")

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

        # 1️⃣ Update existing order details
        update_sql = f"""
            UPDATE {TGT_SCHEMA}.{ORDERS_TGT} d
            SET
                quantityOrdered = s.quantityOrdered,
                priceEach = s.priceEach,
                orderLineNumber = s.orderLineNumber,
                dw_order_id = o.dw_order_id,
                dw_product_id = p.dw_product_id,
                src_update_timestamp = s.update_timestamp,
                dw_update_timestamp = CURRENT_TIMESTAMP,
                etl_batch_no = {ETL_BATCH_NO},
                etl_batch_date = '{ETL_BATCH_DATE}'
            FROM {SRC_SCHEMA}.{ORDERS_SRC} s
            JOIN {TGT_SCHEMA}.{ORDERS_TABLE} o
              ON s.orderNumber = o.src_orderNumber
            JOIN {TGT_SCHEMA}.{PRODUCTS_TABLE} p
              ON s.productCode = p.src_productCode
            WHERE s.orderNumber = d.src_orderNumber
              AND s.productCode = d.src_productCode
              AND s.update_timestamp >= '{ETL_BATCH_DATE}';
        """
        cur.execute(update_sql)
        print(f"Updated order details: {cur.rowcount}")

        # 2️⃣ Insert new order details
        insert_sql = f"""
            INSERT INTO {TGT_SCHEMA}.{ORDERS_TGT} (
                src_orderNumber, src_productCode, quantityOrdered, priceEach, orderLineNumber,
                dw_order_id, dw_product_id,
                src_create_timestamp, src_update_timestamp,
                dw_create_timestamp, dw_update_timestamp,
                etl_batch_no, etl_batch_date
            )
            SELECT 
                s.orderNumber, s.productCode, s.quantityOrdered, s.priceEach, s.orderLineNumber,
                o.dw_order_id, p.dw_product_id,
                s.create_timestamp, s.update_timestamp,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                {ETL_BATCH_NO}, '{ETL_BATCH_DATE}'
            FROM {SRC_SCHEMA}.{ORDERS_SRC} s
            JOIN {TGT_SCHEMA}.{ORDERS_TABLE} o
              ON s.orderNumber = o.src_orderNumber
            JOIN {TGT_SCHEMA}.{PRODUCTS_TABLE} p
              ON s.productCode = p.src_productCode
            LEFT JOIN {TGT_SCHEMA}.{ORDERS_TGT} d
              ON s.orderNumber = d.src_orderNumber
             AND s.productCode = d.src_productCode
            WHERE s.create_timestamp >= '{ETL_BATCH_DATE}'
              AND d.src_orderNumber IS NULL;
        """
        cur.execute(insert_sql)
        print(f"Inserted order details: {cur.rowcount}")

        conn.commit()
        print("Order Details ETL completed successfully.")

    except Exception as e:
        conn.rollback()
        print("Error during Order Details ETL:", str(e))

    finally:
        cur.close()
        conn.close()
        print("Redshift connection closed.")


if __name__ == "__main__":
    orderdetails()
