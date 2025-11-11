import psycopg2
import sys
import os 
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_redshift_connection

SRC_SCHEMA = os.getenv("REDSHIFT_SCHEMA")
TGT_SCHEMA = os.getenv("REDSHIFT_SCHEMA1")
SRC_TABLE = "employees"
TGT_TABLE = "employees"
OFFICES_TABLE = "offices"
BATCH_CONTROL_TABLE = "j25amit_etl_metadata.batch_control"

def employees():
    print("Starting Employees ETL...")

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

        # 1️⃣ Count employees to update
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {TGT_SCHEMA}.{TGT_TABLE} d
            JOIN {SRC_SCHEMA}.{SRC_TABLE} s
              ON s.employeeNumber = d.employeeNumber
            WHERE s.update_timestamp::DATE >= '{ETL_BATCH_DATE}';
        """)
        update_count = cur.fetchone()[0]
        print(f"Employees to update: {update_count}")

        # 2️⃣ Update existing employees
        update_sql = f"""
            UPDATE {TGT_SCHEMA}.{TGT_TABLE} d
            SET
                lastName = s.lastName,
                firstName = s.firstName,
                extension = s.extension,
                email = s.email,
                officeCode = s.officeCode,
                reportsTo = s.reportsTo,
                jobTitle = s.jobTitle,
                dw_office_id = o.dw_office_id,
                src_update_timestamp = s.update_timestamp,
                dw_update_timestamp = CURRENT_TIMESTAMP,
                etl_batch_no = {ETL_BATCH_NO},
                etl_batch_date = '{ETL_BATCH_DATE}'
            FROM {SRC_SCHEMA}.{SRC_TABLE} s
            JOIN {TGT_SCHEMA}.{OFFICES_TABLE} o
              ON s.officeCode = o.officeCode
            WHERE s.employeeNumber = d.employeeNumber
              AND s.update_timestamp::DATE >= '{ETL_BATCH_DATE}';
        """
        cur.execute(update_sql)
        print("Update completed.")

        # 3️⃣ Count new employees to insert
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {SRC_SCHEMA}.{SRC_TABLE} s
            LEFT JOIN {TGT_SCHEMA}.{TGT_TABLE} d
              ON s.employeeNumber = d.employeeNumber
            WHERE s.create_timestamp::DATE >= '{ETL_BATCH_DATE}'
              AND d.employeeNumber IS NULL;
        """)
        insert_count = cur.fetchone()[0]
        print(f"Employees to insert: {insert_count}")

        # 4️⃣ Insert new employees
        insert_sql = f"""
            INSERT INTO {TGT_SCHEMA}.{TGT_TABLE} (
                employeeNumber, lastName, firstName, extension, email,
                officeCode, reportsTo, jobTitle, dw_office_id,
                src_create_timestamp, src_update_timestamp, dw_create_timestamp, dw_update_timestamp,
                etl_batch_no, etl_batch_date
            )
            SELECT 
                s.employeeNumber, s.lastName, s.firstName, s.extension, s.email,
                s.officeCode, s.reportsTo, s.jobTitle, o.dw_office_id,
                s.create_timestamp, s.update_timestamp, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                {ETL_BATCH_NO}, '{ETL_BATCH_DATE}'
            FROM {SRC_SCHEMA}.{SRC_TABLE} s
            JOIN {TGT_SCHEMA}.{OFFICES_TABLE} o
              ON s.officeCode = o.officeCode
            LEFT JOIN {TGT_SCHEMA}.{TGT_TABLE} d
              ON s.employeeNumber = d.employeeNumber
            WHERE s.create_timestamp::DATE >= '{ETL_BATCH_DATE}'
              AND d.employeeNumber IS NULL;
        """
        cur.execute(insert_sql)
        print("Insert completed.")

        # 5️⃣ Update dw_reporting_employee_id
        reporting_sql = f"""
            UPDATE {TGT_SCHEMA}.{TGT_TABLE} d
            SET dw_reporting_employee_id = s.dw_employee_id
            FROM {TGT_SCHEMA}.{TGT_TABLE} s
            WHERE d.reportsTo = s.employeeNumber;
        """
        cur.execute(reporting_sql)
        print("Updated dw_reporting_employee_id.")

        # 6️⃣ Update dw_office_id from offices table
        office_sql = f"""
            UPDATE {TGT_SCHEMA}.{TGT_TABLE} d
            SET dw_office_id = o.dw_office_id
            FROM {TGT_SCHEMA}.{OFFICES_TABLE} o
            WHERE d.officeCode = o.officeCode;
        """
        cur.execute(office_sql)
        print("Updated dw_office_id for employees.")

        conn.commit()
        print("Employees ETL completed successfully.")

    except Exception as e:
        conn.rollback()
        print("Error during Employees ETL:", str(e))

    finally:
        cur.close()
        conn.close()
        print("Redshift connection closed.")


if __name__ == "__main__":
    employees()
