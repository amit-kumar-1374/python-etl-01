import psycopg2
import sys
import os 
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_redshift_connection

SRC_SCHEMA = os.getenv("REDSHIFT_SCHEMA")
TGT_SCHEMA = os.getenv("REDSHIFT_SCHEMA1")
PRODUCTLINES_SRC = "productlines"
PRODUCTLINES_TGT = "productlines"
PRODUCTS_SRC = "products"
PRODUCTS_TGT = "products"
BATCH_CONTROL_TABLE = "j25amit_etl_metadata.batch_control"

def products():
    print("Starting Products ETL...")

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

        # 1️⃣ Update existing product lines
        update_productlines_sql = f"""
            UPDATE {TGT_SCHEMA}.{PRODUCTLINES_TGT} d
            SET
                textDescription = s.textDescription,
                htmlDescription = s.htmlDescription,
                image = s.image,
                src_update_timestamp = s.update_timestamp,
                dw_update_timestamp = CURRENT_TIMESTAMP,
                etl_batch_no = {ETL_BATCH_NO},
                etl_batch_date = '{ETL_BATCH_DATE}'
            FROM {SRC_SCHEMA}.{PRODUCTLINES_SRC} s
            WHERE s.productLine = d.productLine
              AND s.update_timestamp >= '{ETL_BATCH_DATE}';
        """
        cur.execute(update_productlines_sql)
        print(f"Updated product lines: {cur.rowcount}")

        # 2️⃣ Insert new product lines
        insert_productlines_sql = f"""
            INSERT INTO {TGT_SCHEMA}.{PRODUCTLINES_TGT} (
                productLine, textDescription, htmlDescription, image,
                src_create_timestamp, src_update_timestamp,
                dw_create_timestamp, dw_update_timestamp,
                etl_batch_no, etl_batch_date
            )
            SELECT 
                s.productLine, s.textDescription, s.htmlDescription, s.image,
                s.create_timestamp, s.update_timestamp,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                {ETL_BATCH_NO}, '{ETL_BATCH_DATE}'
            FROM {SRC_SCHEMA}.{PRODUCTLINES_SRC} s
            LEFT JOIN {TGT_SCHEMA}.{PRODUCTLINES_TGT} d
              ON s.productLine = d.productLine
            WHERE s.create_timestamp >= '{ETL_BATCH_DATE}'
              AND d.productLine IS NULL;
        """
        cur.execute(insert_productlines_sql)
        print(f"Inserted product lines: {cur.rowcount}")

        # 3️⃣ Update existing products
        update_products_sql = f"""
            UPDATE {TGT_SCHEMA}.{PRODUCTS_TGT} d
            SET
                productName = s.productName,
                productLine = s.productLine,
                productScale = s.productScale,
                productVendor = s.productVendor,
                productDescription = s.productDescription,
                quantityInStock = s.quantityInStock,
                buyPrice = s.buyPrice,
                MSRP = s.MSRP,
                dw_product_line_id = pl.dw_product_line_id,
                src_update_timestamp = s.update_timestamp,
                dw_update_timestamp = CURRENT_TIMESTAMP,
                etl_batch_no = {ETL_BATCH_NO},
                etl_batch_date = '{ETL_BATCH_DATE}'
            FROM {SRC_SCHEMA}.{PRODUCTS_SRC} s
            JOIN {TGT_SCHEMA}.{PRODUCTLINES_TGT} pl
              ON s.productLine = pl.productLine
            WHERE s.productCode = d.src_productCode
              AND s.update_timestamp >= '{ETL_BATCH_DATE}';
        """
        cur.execute(update_products_sql)
        print(f"Updated products: {cur.rowcount}")

        # 4️⃣ Insert new products
        insert_products_sql = f"""
            INSERT INTO {TGT_SCHEMA}.{PRODUCTS_TGT} (
                src_productCode, productName, productLine, productScale,
                productVendor, productDescription, quantityInStock, buyPrice, MSRP,
                dw_product_line_id,
                src_create_timestamp, src_update_timestamp,
                dw_create_timestamp, dw_update_timestamp,
                etl_batch_no, etl_batch_date
            )
            SELECT 
                s.productCode, s.productName, s.productLine, s.productScale,
                s.productVendor, s.productDescription, s.quantityInStock,
                s.buyPrice, s.MSRP, pl.dw_product_line_id,
                s.create_timestamp, s.update_timestamp,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                {ETL_BATCH_NO}, '{ETL_BATCH_DATE}'
            FROM {SRC_SCHEMA}.{PRODUCTS_SRC} s
            JOIN {TGT_SCHEMA}.{PRODUCTLINES_TGT} pl
              ON s.productLine = pl.productLine
            LEFT JOIN {TGT_SCHEMA}.{PRODUCTS_TGT} d
              ON s.productCode = d.src_productCode
            WHERE s.create_timestamp >= '{ETL_BATCH_DATE}'
              AND d.src_productCode IS NULL;
        """
        cur.execute(insert_products_sql)
        print(f"Inserted products: {cur.rowcount}")

        conn.commit()
        print("Products ETL completed successfully.")

    except Exception as e:
        conn.rollback()
        print("Error during Products ETL:", str(e))

    finally:
        cur.close()
        conn.close()
        print("Redshift connection closed.")


if __name__ == "__main__":
    products()
