"""
Verify Data Loading Results
Description: Check what data has been successfully loaded
"""

import os
import snowflake.connector
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_snowflake():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE')
    )

def main():
    try:
        conn = connect_snowflake()
        cursor = conn.cursor()
        
        logger.info("📊 Data Loading Verification Report")
        logger.info("="*50)
        
        # Check each schema and table
        tables_to_check = [
            ('CUSTOMERS_SCHEMA_DEV', 'CUSTOMER_SEGMENTS'),
            ('CUSTOMERS_SCHEMA_DEV', 'CUSTOMERS'),
            ('CUSTOMERS_SCHEMA_DEV', 'CUSTOMER_ADDRESSES'),
            ('PRODUCTS_SCHEMA_DEV', 'CATEGORIES'),
            ('PRODUCTS_SCHEMA_DEV', 'SUPPLIERS'),
            ('PRODUCTS_SCHEMA_DEV', 'PRODUCTS'),
            ('HR_SCHEMA_DEV', 'DEPARTMENTS'),
            ('HR_SCHEMA_DEV', 'POSITIONS'),
            ('SALES_SCHEMA_DEV', 'SALES_TERRITORIES'),
            ('ANALYTICS_SCHEMA_DEV', 'DATE_DIM')
        ]
        
        for schema, table in tables_to_check:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                count = cursor.fetchone()[0]
                logger.info(f"✅ {schema}.{table}: {count:,} records")
            except Exception as e:
                logger.info(f"❌ {schema}.{table}: {str(e)}")
        
        # Show sample data from loaded tables
        logger.info("\n📋 Sample Data Preview:")
        logger.info("="*30)
        
        try:
            cursor.execute("SELECT * FROM CUSTOMERS_SCHEMA_DEV.CUSTOMERS LIMIT 3")
            results = cursor.fetchall()
            logger.info(f"Sample Customers: {len(results)} rows")
            for row in results:
                logger.info(f"  Customer: {row[1]} - {row[4]} {row[5]}")
        except Exception as e:
            logger.info(f"Could not fetch customer samples: {str(e)}")
        
        try:
            cursor.execute("SELECT * FROM PRODUCTS_SCHEMA_DEV.PRODUCTS LIMIT 3")  
            results = cursor.fetchall()
            logger.info(f"Sample Products: {len(results)} rows")
            for row in results:
                logger.info(f"  Product: {row[1]} - {row[2]}")
        except Exception as e:
            logger.info(f"Could not fetch product samples: {str(e)}")
        
        logger.info("\n🎉 Verification completed!")
        
    except Exception as e:
        logger.error(f"❌ Verification failed: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()