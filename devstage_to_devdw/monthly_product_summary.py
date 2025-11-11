import psycopg2
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_redshift_connection

SRC_SCHEMA = os.getenv("REDSHIFT_SCHEMA")
TGT_SCHEMA = os.getenv("REDSHIFT_SCHEMA1")
BATCH_CONTROL_TABLE = "j25amit_etl_metadata.batch_control"
MONTHLY_SUMMARY_TABLE = f"{TGT_SCHEMA}.monthly_product_summary"
DAILY_SUMMARY_TABLE = f"{TGT_SCHEMA}.daily_product_summary"

def monthly_product_summary():
    print("Starting Monthly Product Summary ETL...")

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

        # 🔹 2. Update existing monthly records
        update_sql = f"""
        UPDATE {MONTHLY_SUMMARY_TABLE} m
        SET
            customer_apd = m.customer_apd + d.customer_apd,
            product_cost_amount = m.product_cost_amount + d.product_cost_amount,
            product_mrp_amount = m.product_mrp_amount + d.product_mrp_amount,
            cancelled_product_qty = m.cancelled_product_qty + d.cancelled_product_qty,
            cancelled_cost_amount = m.cancelled_cost_amount + d.cancelled_cost_amount,
            cancelled_mrp_amount = m.cancelled_mrp_amount + d.cancelled_mrp_amount,
            cancelled_order_apd = m.cancelled_order_apd + d.cancelled_order_apd,
            cancelled_order_apm = m.cancelled_order_apm + d.cancelled_order_apm,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = d.etl_batch_no,
            etl_batch_date = d.etl_batch_date
        FROM (
            SELECT
                DATE_TRUNC('month', summary_date) AS start_of_the_month_date,
                dw_product_id,
                MAX(customer_apd) AS customer_apd,
                1 AS customer_apm,
                SUM(product_cost_amount) AS product_cost_amount,
                SUM(product_mrp_amount) AS product_mrp_amount,
                SUM(cancelled_product_qty) AS cancelled_product_qty,
                SUM(cancelled_cost_amount) AS cancelled_cost_amount,
                SUM(cancelled_mrp_amount) AS cancelled_mrp_amount,
                MAX(cancelled_order_apd) AS cancelled_order_apd,
                SUM(cancelled_order_apd) AS cancelled_order_apm,
                MAX(dw_create_timestamp) AS dw_create_timestamp,
                MAX(dw_update_timestamp) AS dw_update_timestamp,
                MAX(etl_batch_no) AS etl_batch_no,
                MAX(etl_batch_date) AS etl_batch_date
            FROM {DAILY_SUMMARY_TABLE}
            WHERE etl_batch_date >= '{ETL_BATCH_DATE}'
            GROUP BY start_of_the_month_date, dw_product_id
        ) d
        WHERE m.start_of_the_month_date = d.start_of_the_month_date
          AND m.dw_product_id = d.dw_product_id;
        """
        cur.execute(update_sql)
        update_count = cur.rowcount
        print(f" Updated {update_count} existing monthly records.")

        # 🔹 3. Insert new monthly records (not existing)
        insert_sql = f"""
        INSERT INTO {MONTHLY_SUMMARY_TABLE} (
            start_of_the_month_date,
            dw_product_id,
            customer_apd,
            customer_apm,
            product_cost_amount,
            product_mrp_amount,
            cancelled_product_qty,
            cancelled_cost_amount,
            cancelled_mrp_amount,
            cancelled_order_apd,
            cancelled_order_apm,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            d.start_of_the_month_date,
            d.dw_product_id,
            d.customer_apd,
            d.customer_apm,
            d.product_cost_amount,
            d.product_mrp_amount,
            d.cancelled_product_qty,
            d.cancelled_cost_amount,
            d.cancelled_mrp_amount,
            d.cancelled_order_apd,
            d.cancelled_order_apm,
            d.dw_create_timestamp,
            d.dw_update_timestamp,
            d.etl_batch_no,
            d.etl_batch_date
        FROM (
            SELECT
                DATE_TRUNC('month', summary_date) AS start_of_the_month_date,
                dw_product_id,
                MAX(customer_apd) AS customer_apd,
                1 AS customer_apm,
                SUM(product_cost_amount) AS product_cost_amount,
                SUM(product_mrp_amount) AS product_mrp_amount,
                SUM(cancelled_product_qty) AS cancelled_product_qty,
                SUM(cancelled_cost_amount) AS cancelled_cost_amount,
                SUM(cancelled_mrp_amount) AS cancelled_mrp_amount,
                MAX(cancelled_order_apd) AS cancelled_order_apd,
                SUM(cancelled_order_apd) AS cancelled_order_apm,
                MAX(dw_create_timestamp) AS dw_create_timestamp,
                MAX(dw_update_timestamp) AS dw_update_timestamp,
                MAX(etl_batch_no) AS etl_batch_no,
                MAX(etl_batch_date) AS etl_batch_date
            FROM {DAILY_SUMMARY_TABLE}
            WHERE etl_batch_date >= '{ETL_BATCH_DATE}'
            GROUP BY start_of_the_month_date, dw_product_id
        ) d
        LEFT JOIN {MONTHLY_SUMMARY_TABLE} m
          ON m.start_of_the_month_date = d.start_of_the_month_date
         AND m.dw_product_id = d.dw_product_id
        WHERE m.dw_product_id IS NULL;
        """
        cur.execute(insert_sql)
        insert_count = cur.rowcount
        conn.commit()
        print(f" Inserted {insert_count} new monthly records.")

        print(" Monthly Product Summary ETL completed successfully.")

    except Exception as e:
        conn.rollback()
        print(" Error during Monthly Product Summary ETL:", str(e))

    finally:
        cur.close()
        conn.close()
        print(" Redshift connection closed.")


if __name__ == "__main__":
    monthly_product_summary()
