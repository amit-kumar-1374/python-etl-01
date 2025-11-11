import psycopg2
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_redshift_connection

SRC_SCHEMA = os.getenv("REDSHIFT_SCHEMA")
TGT_SCHEMA = os.getenv("REDSHIFT_SCHEMA1")
BATCH_CONTROL_TABLE = "j25amit_etl_metadata.batch_control"

def daily_customer_summary():
    print("Starting Daily Customer Summary ETL...")

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

        # 🔹 2. Build the full SQL
        insert_sql = f"""
        INSERT INTO {TGT_SCHEMA}.daily_customer_summary (
            summary_date,
            dw_customer_id,
            order_count,
            order_apd,
            order_cost_amount,
            cancelled_order_count,
            cancelled_order_amount,
            cancelled_order_apd,
            shipped_order_count,
            shipped_order_amount,
            shipped_order_apd,
            payment_apd,
            payment_amount,
            products_ordered_qty,
            products_items_qty,
            order_mrp_amount,
            new_customer_apd,
            new_customer_paid_apd,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        WITH orders_cte AS (
            SELECT 
                CAST(o.orderDate AS DATE) AS summary_date,
                o.dw_customer_id,
                COUNT(DISTINCT o.dw_order_id) AS order_count,
                1 AS order_apd,
                SUM(od.priceEach * od.quantityOrdered) AS order_cost_amount,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                COUNT(DISTINCT od.dw_product_id) AS products_ordered_qty,
                COUNT(od.quantityOrdered) AS products_items_qty,
                SUM(p.MSRP * od.quantityOrdered) AS order_mrp_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM {TGT_SCHEMA}.orders o
            JOIN {TGT_SCHEMA}.orderdetails od ON o.dw_order_id = od.dw_order_id
            JOIN {TGT_SCHEMA}.products p ON od.dw_product_id = p.dw_product_id
            WHERE CAST(o.orderDate AS DATE) >= '{ETL_BATCH_DATE}'
            GROUP BY 1,2
        ),
        customers_cte AS (
            SELECT 
                CAST(c.src_create_timestamp AS DATE) AS summary_date,
                c.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_cost_amount,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                0 AS order_mrp_amount,
                1 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM {TGT_SCHEMA}.customers c
            WHERE CAST(c.src_create_timestamp AS DATE) >= '{ETL_BATCH_DATE}'
        ),
        cancelled_cte AS (
            SELECT 
                CAST(o.cancelledDate AS DATE) AS summary_date,
                o.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_cost_amount,
                COUNT(o.dw_order_id) AS cancelled_order_count,
                SUM(od.priceEach * od.quantityOrdered) AS cancelled_order_amount,
                1 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                0 AS order_mrp_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM {TGT_SCHEMA}.orders o
            JOIN {TGT_SCHEMA}.orderdetails od ON o.dw_order_id = od.dw_order_id
            WHERE CAST(o.cancelledDate AS DATE) >= '{ETL_BATCH_DATE}'
              AND o.status = 'Cancelled'
            GROUP BY 1,2
        ),
        payments_cte AS (
            SELECT 
                CAST(p.paymentDate AS DATE) AS summary_date,
                p.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_cost_amount,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                1 AS payment_apd,
                SUM(p.amount) AS payment_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                0 AS order_mrp_amount,
                0 AS new_customer_apd,
                1 AS new_customer_paid_apd
            FROM {TGT_SCHEMA}.payments p
            WHERE CAST(p.paymentDate AS DATE) >= '{ETL_BATCH_DATE}'
            GROUP BY 1,2
        ),
        shipped_cte AS (
            SELECT 
                CAST(o.shippedDate AS DATE) AS summary_date,
                o.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_cost_amount,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                COUNT(o.dw_order_id) AS shipped_order_count,
                SUM(od.priceEach * od.quantityOrdered) AS shipped_order_amount,
                1 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                0 AS order_mrp_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM {TGT_SCHEMA}.orders o
            JOIN {TGT_SCHEMA}.orderdetails od ON o.dw_order_id = od.dw_order_id
            WHERE CAST(o.shippedDate AS DATE) >= '{ETL_BATCH_DATE}'
              AND o.status = 'Shipped'
            GROUP BY 1,2
        ),
        combined_cte AS (
            SELECT * FROM orders_cte
            UNION ALL
            SELECT * FROM customers_cte
            UNION ALL
            SELECT * FROM cancelled_cte
            UNION ALL
            SELECT * FROM payments_cte
            UNION ALL
            SELECT * FROM shipped_cte
        )
        SELECT
            summary_date,
            dw_customer_id,
            MAX(order_count),
            MAX(order_apd),
            MAX(order_cost_amount),
            MAX(cancelled_order_count),
            MAX(cancelled_order_amount),
            MAX(cancelled_order_apd),
            MAX(shipped_order_count),
            MAX(shipped_order_amount),
            MAX(shipped_order_apd),
            MAX(payment_apd),
            MAX(payment_amount),
            MAX(products_ordered_qty),
            MAX(products_items_qty),
            MAX(order_mrp_amount),
            MAX(new_customer_apd),
            MAX(new_customer_paid_apd),
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            {ETL_BATCH_NO},
            '{ETL_BATCH_DATE}'
        FROM combined_cte
        GROUP BY summary_date, dw_customer_id;
        """

        # 🔹 3. Execute
        cur.execute(insert_sql)
        conn.commit()
        print(f"Inserted {cur.rowcount} records into {TGT_SCHEMA}.daily_customer_summary")
        print(" Daily Customer Summary ETL completed successfully.")

    except Exception as e:
        conn.rollback()
        print(" Error during Daily Summary ETL:", str(e))

    finally:
        cur.close()
        conn.close()
        print(" Redshift connection closed.")


if __name__ == "__main__":
    daily_customer_summary()
