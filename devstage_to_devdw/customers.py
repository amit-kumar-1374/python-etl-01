import psycopg2
import sys
import os 

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_redshift_connection

SRC_SCHEMA = os.getenv("REDSHIFT_SCHEMA")
TGT_SCHEMA = os.getenv("REDSHIFT_SCHEMA1")
SRC_TABLE = "customers"
TGT_TABLE = "customers"
BATCH_CONTROL_TABLE = "j25amit_etl_metadata.batch_control"

def customers():
    print("Starting Customers ETL...")

    conn = get_redshift_connection()
    cur = conn.cursor()

    try:
        cur.execute(f"SELECT etl_batch_no, etl_batch_date FROM {BATCH_CONTROL_TABLE};")
        result = cur.fetchone()
        if not result:
            raise Exception("No batch record found in batch_control table")

        ETL_BATCH_NO, ETL_BATCH_DATE = result
        print(f"ETL Batch No: {ETL_BATCH_NO}, Batch Date: {ETL_BATCH_DATE}")

        update_sql = f"""
            UPDATE {TGT_SCHEMA}.{TGT_TABLE} d
            SET
                customerName = s.customerName,
                contactLastName = s.contactLastName,
                contactFirstName = s.contactFirstName,
                phone = s.phone,
                addressLine1 = s.addressLine1,
                addressLine2 = s.addressLine2,
                city = s.city,
                state = s.state,
                postalCode = s.postalCode,
                country = s.country,
                salesRepEmployeeNumber = s.salesRepEmployeeNumber,
                creditLimit = s.creditLimit,
                src_update_timestamp = s.update_timestamp,
                dw_update_timestamp = CURRENT_TIMESTAMP,
                etl_batch_no = {ETL_BATCH_NO},
                etl_batch_date = '{ETL_BATCH_DATE}'
            FROM {SRC_SCHEMA}.{SRC_TABLE} s
            WHERE s.customerNumber = d.src_customerNumber
              AND s.update_timestamp >= '{ETL_BATCH_DATE}';
        """
        cur.execute(update_sql)
        print(f"Updated customers: {cur.rowcount}")

        insert_sql = f"""
            INSERT INTO {TGT_SCHEMA}.{TGT_TABLE} (
                src_customerNumber, customerName, contactLastName, contactFirstName,
                phone, addressLine1, addressLine2, city, state, postalCode,
                country, salesRepEmployeeNumber, creditLimit,
                src_create_timestamp, src_update_timestamp, dw_create_timestamp, dw_update_timestamp,
                etl_batch_no, etl_batch_date
            )
            SELECT 
                s.customerNumber, s.customerName, s.contactLastName, s.contactFirstName,
                s.phone, s.addressLine1, s.addressLine2, s.city, s.state, s.postalCode,
                s.country, s.salesRepEmployeeNumber, s.creditLimit,
                s.create_timestamp, s.update_timestamp,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                {ETL_BATCH_NO}, '{ETL_BATCH_DATE}'
            FROM {SRC_SCHEMA}.{SRC_TABLE} s
            LEFT JOIN {TGT_SCHEMA}.{TGT_TABLE} d
              ON s.customerNumber = d.src_customerNumber
            WHERE s.create_timestamp >= '{ETL_BATCH_DATE}'
              AND d.src_customerNumber IS NULL;
        """
        cur.execute(insert_sql)
        print(f"Inserted customers: {cur.rowcount}")

        conn.commit()
        print("Customers ETL completed successfully.")

    except Exception as e:
        conn.rollback()
        print("Error during Customers ETL:", str(e))

    finally:
        cur.close()
        conn.close()
        print("Redshift connection closed.")


if __name__ == "__main__":
    customers()
