import psycopg2
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_redshift_connection
SRC_SCHEMA = os.getenv("REDSHIFT_SCHEMA")
TGT_SCHEMA = os.getenv("REDSHIFT_SCHEMA1")
BATCH_CONTROL_TABLE = "j25amit_etl_metadata.batch_control"

def daily_product_summary():
    print("Starting Daily Product Summary ETL...")

    conn = get_redshift_connection()
    cur = conn.cursor()

    try:
        # 🔹 1. Get current batch info
        cur.execute(f"SELECT etl_batch_no, etl_batch_date FROM {BATCH_CONTROL_TABLE};")
        result = cur.fetchone()
        if not result:
            raise Exception("No batch record found in batch_control table")

        ETL_BATCH_NO, ETL_BATCH_DATE = result
        print(f"ETL Batch No: {ETL_BATCH_NO}, Batch Date: {ETL_BATCH_DATE}")

        # 🔹 2. Build the SQL for daily_product_summary
        insert_sql = f"""
        INSERT INTO {TGT_SCHEMA}.daily_product_summary (
            summary_date,
            dw_product_id,
            customer_apd,
            product_cost_amount,
            product_mrp_amount,
            cancelled_product_qty,
            cancelled_cost_amount,
            cancelled_mrp_amount,
            cancelled_order_apd,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        WITH product_sales_cte AS (
            SELECT
                CAST(o.orderDate AS DATE) AS summary_date,
                od.dw_product_id,
                COUNT(DISTINCT o.dw_customer_id) AS customer_apd,
                SUM(od.priceEach * od.quantityOrdered) AS product_cost_amount,
                SUM(p.MSRP * od.quantityOrdered) AS product_mrp_amount,
                0 AS cancelled_product_qty,
                0 AS cancelled_cost_amount,
                0 AS cancelled_mrp_amount,
                0 AS cancelled_order_apd
            FROM {TGT_SCHEMA}.orders o
            JOIN {TGT_SCHEMA}.orderdetails od ON o.dw_order_id = od.dw_order_id
            JOIN {TGT_SCHEMA}.products p ON od.dw_product_id = p.dw_product_id
            WHERE CAST(o.orderDate AS DATE) >= '{ETL_BATCH_DATE}'
            GROUP BY 1,2
        ),
        cancelled_products_cte AS (
            SELECT
                CAST(o.cancelledDate AS DATE) AS summary_date,
                od.dw_product_id,
                0 AS customer_apd,
                0 AS product_cost_amount,
                0 AS product_mrp_amount,
                SUM(od.quantityOrdered) AS cancelled_product_qty,
                SUM(od.priceEach * od.quantityOrdered) AS cancelled_cost_amount,
                SUM(p.MSRP * od.quantityOrdered) AS cancelled_mrp_amount,
                COUNT(DISTINCT o.dw_order_id) AS cancelled_order_apd
            FROM {TGT_SCHEMA}.orders o
            JOIN {TGT_SCHEMA}.orderdetails od ON o.dw_order_id = od.dw_order_id
            JOIN {TGT_SCHEMA}.products p ON od.dw_product_id = p.dw_product_id
            WHERE LOWER(TRIM(o.status)) = 'cancelled'
              AND CAST(o.cancelledDate AS DATE) >= '{ETL_BATCH_DATE}'
            GROUP BY 1,2
        ),
        combined_cte AS (
            SELECT * FROM product_sales_cte
            UNION ALL
            SELECT * FROM cancelled_products_cte
        )
        SELECT
            summary_date,
            dw_product_id,
            MAX(customer_apd) AS customer_apd,
            MAX(product_cost_amount) AS product_cost_amount,
            MAX(product_mrp_amount) AS product_mrp_amount,
            MAX(cancelled_product_qty) AS cancelled_product_qty,
            MAX(cancelled_cost_amount) AS cancelled_cost_amount,
            MAX(cancelled_mrp_amount) AS cancelled_mrp_amount,
            MAX(cancelled_order_apd) AS cancelled_order_apd,
            CURRENT_TIMESTAMP AS dw_create_timestamp,
            CURRENT_TIMESTAMP AS dw_update_timestamp,
            {ETL_BATCH_NO} AS etl_batch_no,
            '{ETL_BATCH_DATE}' AS etl_batch_date
        FROM combined_cte
        GROUP BY summary_date, dw_product_id;
        """

        # 🔹 3. Execute the insert
        cur.execute(insert_sql)
        conn.commit()
        print(f"Inserted {cur.rowcount} records into {TGT_SCHEMA}.daily_product_summary")
        print(" Daily Product Summary ETL completed successfully.")

    except Exception as e:
        conn.rollback()
        print(" Error during Daily Product Summary ETL:", str(e))

    finally:
        cur.close()
        conn.close()
        print(" Redshift connection closed.")


if __name__ == "__main__":
    daily_product_summary()
