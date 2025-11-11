import psycopg2
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_redshift_connection

SRC_SCHEMA = os.getenv("REDSHIFT_SCHEMA")
TGT_SCHEMA = os.getenv("REDSHIFT_SCHEMA1")
BATCH_CONTROL_TABLE = "j25amit_etl_metadata.batch_control"

def monthly_customer_summary():
    print("Starting Monthly Customer Summary ETL...")

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

        # 🔹 2. UPDATE existing monthly summary
        update_sql = f"""
        UPDATE {TGT_SCHEMA}.monthly_customer_summary m
        SET
            order_count = m.order_count + d.order_count,
            order_apd = m.order_apd + d.order_apd,
            order_apm = m.order_apm + d.order_apm,
            order_cost_amount = m.order_cost_amount + d.order_cost_amount,
            cancelled_order_count = m.cancelled_order_count + d.cancelled_order_count,
            cancelled_order_amount = m.cancelled_order_amount + d.cancelled_order_amount,
            cancelled_order_apd = m.cancelled_order_apd + d.cancelled_order_apd,
            cancelled_order_apm = m.cancelled_order_apm + d.cancelled_order_apm,
            shipped_order_count = m.shipped_order_count + d.shipped_order_count,
            shipped_order_amount = m.shipped_order_amount + d.shipped_order_amount,
            shipped_order_apd = m.shipped_order_apd + d.shipped_order_apd,
            shipped_order_apm = m.shipped_order_apm + d.shipped_order_apm,
            payment_apd = m.payment_apd + d.payment_apd,
            payment_apm = m.payment_apm + d.payment_apm,
            payment_amount = m.payment_amount + d.payment_amount,
            products_ordered_qty = m.products_ordered_qty + d.products_ordered_qty,
            products_items_qty = m.products_items_qty + d.products_items_qty,
            order_mrp_amount = m.order_mrp_amount + d.order_mrp_amount,
            new_customer_apd = m.new_customer_apd + d.new_customer_apd,
            new_customer_apm = m.new_customer_apm + d.new_customer_apm,
            new_customer_paid_apd = m.new_customer_paid_apd + d.new_customer_paid_apd,
            new_customer_paid_apm = m.new_customer_paid_apm + d.new_customer_paid_apm,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = d.etl_batch_no,
            etl_batch_date = d.etl_batch_date
        FROM (
            SELECT
                DATE_TRUNC('month', summary_date)::DATE AS start_of_the_month_date,
                dw_customer_id,
                SUM(order_count) AS order_count,
                SUM(order_apd) AS order_apd,
                COUNT(DISTINCT summary_date) AS order_apm,
                SUM(order_cost_amount) AS order_cost_amount,
                SUM(cancelled_order_count) AS cancelled_order_count,
                SUM(cancelled_order_amount) AS cancelled_order_amount,
                SUM(cancelled_order_apd) AS cancelled_order_apd,
                COUNT(DISTINCT CASE WHEN cancelled_order_count > 0 THEN summary_date END) AS cancelled_order_apm,
                SUM(shipped_order_count) AS shipped_order_count,
                SUM(shipped_order_amount) AS shipped_order_amount,
                SUM(shipped_order_apd) AS shipped_order_apd,
                COUNT(DISTINCT CASE WHEN shipped_order_count > 0 THEN summary_date END) AS shipped_order_apm,
                SUM(payment_apd) AS payment_apd,
                COUNT(DISTINCT CASE WHEN payment_amount > 0 THEN summary_date END) AS payment_apm,
                SUM(payment_amount) AS payment_amount,
                SUM(products_ordered_qty) AS products_ordered_qty,
                SUM(products_items_qty) AS products_items_qty,
                SUM(order_mrp_amount) AS order_mrp_amount,
                SUM(new_customer_apd) AS new_customer_apd,
                COUNT(DISTINCT CASE WHEN new_customer_apd > 0 THEN summary_date END) AS new_customer_apm,
                SUM(new_customer_paid_apd) AS new_customer_paid_apd,
                COUNT(DISTINCT CASE WHEN new_customer_paid_apd > 0 THEN summary_date END) AS new_customer_paid_apm,
                MAX(dw_create_timestamp) AS dw_create_timestamp,
                MAX(dw_update_timestamp) AS dw_update_timestamp,
                MAX(etl_batch_no) AS etl_batch_no,
                MAX(etl_batch_date) AS etl_batch_date
            FROM {TGT_SCHEMA}.daily_customer_summary
            WHERE etl_batch_date >= '{ETL_BATCH_DATE}'
            GROUP BY 1,2
        ) d
        WHERE m.start_of_the_month_date = d.start_of_the_month_date
          AND m.dw_customer_id = d.dw_customer_id;
        """
        cur.execute(update_sql)
        print(f"Updated records: {cur.rowcount}")

        # 🔹 3. INSERT new monthly summary rows
        insert_sql = f"""
        INSERT INTO {TGT_SCHEMA}.monthly_customer_summary (
            start_of_the_month_date,
            dw_customer_id,
            order_count,
            order_apd,
            order_apm,
            order_cost_amount,
            cancelled_order_count,
            cancelled_order_amount,
            cancelled_order_apd,
            cancelled_order_apm,
            shipped_order_count,
            shipped_order_amount,
            shipped_order_apd,
            shipped_order_apm,
            payment_apd,
            payment_apm,
            payment_amount,
            products_ordered_qty,
            products_items_qty,
            order_mrp_amount,
            new_customer_apd,
            new_customer_apm,
            new_customer_paid_apd,
            new_customer_paid_apm,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            d.start_of_the_month_date,
            d.dw_customer_id,
            d.order_count,
            d.order_apd,
            d.order_apm,
            d.order_cost_amount,
            d.cancelled_order_count,
            d.cancelled_order_amount,
            d.cancelled_order_apd,
            d.cancelled_order_apm,
            d.shipped_order_count,
            d.shipped_order_amount,
            d.shipped_order_apd,
            d.shipped_order_apm,
            d.payment_apd,
            d.payment_apm,
            d.payment_amount,
            d.products_ordered_qty,
            d.products_items_qty,
            d.order_mrp_amount,
            d.new_customer_apd,
            d.new_customer_apm,
            d.new_customer_paid_apd,
            d.new_customer_paid_apm,
            d.dw_create_timestamp,
            d.dw_update_timestamp,
            d.etl_batch_no,
            d.etl_batch_date
        FROM (
            SELECT
                DATE_TRUNC('month', summary_date)::DATE AS start_of_the_month_date,
                dw_customer_id,
                SUM(order_count) AS order_count,
                SUM(order_apd) AS order_apd,
                COUNT(DISTINCT summary_date) AS order_apm,
                SUM(order_cost_amount) AS order_cost_amount,
                SUM(cancelled_order_count) AS cancelled_order_count,
                SUM(cancelled_order_amount) AS cancelled_order_amount,
                SUM(cancelled_order_apd) AS cancelled_order_apd,
                COUNT(DISTINCT CASE WHEN cancelled_order_count > 0 THEN summary_date END) AS cancelled_order_apm,
                SUM(shipped_order_count) AS shipped_order_count,
                SUM(shipped_order_amount) AS shipped_order_amount,
                SUM(shipped_order_apd) AS shipped_order_apd,
                COUNT(DISTINCT CASE WHEN shipped_order_count > 0 THEN summary_date END) AS shipped_order_apm,
                SUM(payment_apd) AS payment_apd,
                COUNT(DISTINCT CASE WHEN payment_amount > 0 THEN summary_date END) AS payment_apm,
                SUM(payment_amount) AS payment_amount,
                SUM(products_ordered_qty) AS products_ordered_qty,
                SUM(products_items_qty) AS products_items_qty,
                SUM(order_mrp_amount) AS order_mrp_amount,
                SUM(new_customer_apd) AS new_customer_apd,
                COUNT(DISTINCT CASE WHEN new_customer_apd > 0 THEN summary_date END) AS new_customer_apm,
                SUM(new_customer_paid_apd) AS new_customer_paid_apd,
                COUNT(DISTINCT CASE WHEN new_customer_paid_apd > 0 THEN summary_date END) AS new_customer_paid_apm,
                MAX(dw_create_timestamp) AS dw_create_timestamp,
                MAX(dw_update_timestamp) AS dw_update_timestamp,
                MAX(etl_batch_no) AS etl_batch_no,
                MAX(etl_batch_date) AS etl_batch_date
            FROM {TGT_SCHEMA}.daily_customer_summary
            WHERE etl_batch_date >= '{ETL_BATCH_DATE}'
            GROUP BY 1,2
        ) d
        LEFT JOIN {TGT_SCHEMA}.monthly_customer_summary m
          ON m.start_of_the_month_date = d.start_of_the_month_date
         AND m.dw_customer_id = d.dw_customer_id
        WHERE m.dw_customer_id IS NULL;
        """
        cur.execute(insert_sql)
        print(f"Inserted records: {cur.rowcount}")

        # 🔹 4. Commit
        conn.commit()
        print("Monthly Customer Summary ETL completed successfully.")

    except Exception as e:
        conn.rollback()
        print(" Error during Monthly Summary ETL:", str(e))

    finally:
        cur.close()
        conn.close()
        print(" Redshift connection closed.")


if __name__ == "__main__":
    monthly_customer_summary()
