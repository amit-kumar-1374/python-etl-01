import psycopg2
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from utils import get_redshift_connection

SRC_SCHEMA = os.getenv("REDSHIFT_SCHEMA")
TGT_SCHEMA = os.getenv("REDSHIFT_SCHEMA1")
CUSTOMERS_TABLE = "customers"
CUSTOMER_HISTORY_TABLE = "customer_history"
BATCH_CONTROL_TABLE = "j25amit_etl_metadata.batch_control"

def customer_history():
    print("Starting Customer History ETL...")

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

        #  Update historical records where creditLimit changed
        update_sql = f"""
            UPDATE {TGT_SCHEMA}.{CUSTOMER_HISTORY_TABLE} c
            SET
                dw_active_record_ind = 0,
                effective_to_date = DATEADD(day, -1, '{ETL_BATCH_DATE}'),
                dw_update_timestamp = CURRENT_TIMESTAMP,
                update_etl_batch_no = {ETL_BATCH_NO},
                update_etl_batch_date = '{ETL_BATCH_DATE}'
            FROM (
                SELECT dw_customer_id, creditLimit
                FROM {TGT_SCHEMA}.{CUSTOMERS_TABLE}
                WHERE src_update_timestamp >= '{ETL_BATCH_DATE}'
                  AND src_create_timestamp < '{ETL_BATCH_DATE}'
            ) d
            WHERE c.dw_customer_id = d.dw_customer_id
              AND c.creditLimit <> d.creditLimit
              AND c.dw_active_record_ind = 1;
        """
        cur.execute(update_sql)
        print(f"Updated customer history records: {cur.rowcount}")

        # Insert new active records
        insert_sql = f"""
            INSERT INTO {TGT_SCHEMA}.{CUSTOMER_HISTORY_TABLE} (
                dw_customer_id,
                creditLimit,
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
                c.dw_customer_id,
                c.creditLimit,
                '{ETL_BATCH_DATE}',
                NULL,
                1,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                {ETL_BATCH_NO},
                '{ETL_BATCH_DATE}',
                NULL,
                NULL
            FROM {TGT_SCHEMA}.{CUSTOMERS_TABLE} c
            LEFT JOIN (
                SELECT dw_customer_id
                FROM {TGT_SCHEMA}.{CUSTOMER_HISTORY_TABLE}
                WHERE dw_active_record_ind = 1
            ) d
            ON c.dw_customer_id = d.dw_customer_id
            WHERE d.dw_customer_id IS NULL;
        """
        cur.execute(insert_sql)
        print(f"Inserted new customer history records: {cur.rowcount}")

        conn.commit()
        print("Customer History ETL completed successfully.")

    except Exception as e:
        conn.rollback()
        print("Error during Customer History ETL:", str(e))

    finally:
        cur.close()
        conn.close()
        print("Redshift connection closed.")


if __name__ == "__main__":
    customer_history()
