"""
Test Discount Column Addition
Description: Tests the new discount column in SUPPLIERS table
"""

import os
import snowflake.connector
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_discount_column():
    """Test the discount column functionality"""
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            role=os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'RETAILWORKS_DB_DEV')
        )
        
        cursor = conn.cursor()
        
        # Test 1: Check if discount column exists
        logger.info("🔍 Testing discount column existence...")
        cursor.execute("DESCRIBE TABLE PRODUCTS_SCHEMA_DEV.SUPPLIERS")
        columns = {row[0]: row[1] for row in cursor.fetchall()}
        
        if 'DISCOUNT' in columns:
            logger.info("✅ DISCOUNT column exists with type: " + columns['DISCOUNT'])
        else:
            logger.error("❌ DISCOUNT column not found")
            return False
        
        # Test 2: Check discount values
        logger.info("🔍 Testing discount values...")
        cursor.execute("""
            SELECT 
                COUNT(*) as total_suppliers,
                COUNT(DISCOUNT) as suppliers_with_discount,
                AVG(DISCOUNT) as avg_discount,
                MIN(DISCOUNT) as min_discount,
                MAX(DISCOUNT) as max_discount
            FROM PRODUCTS_SCHEMA_DEV.SUPPLIERS
        """)
        
        result = cursor.fetchone()
        if result:
            total, with_discount, avg_disc, min_disc, max_disc = result
            logger.info(f"📊 Suppliers: {total} total, {with_discount} with discount")
            logger.info(f"📊 Discount range: {min_disc}% - {max_disc}% (avg: {avg_disc:.2f}%)")
            
            if with_discount > 0:
                logger.info("✅ Discount values found")
            else:
                logger.warning("⚠️ No discount values found")
        
        # Test 3: Show sample data
        logger.info("🔍 Sample suppliers with discount...")
        cursor.execute("""
            SELECT SUPPLIER_NAME, DISCOUNT, STATUS, RATING
            FROM PRODUCTS_SCHEMA_DEV.SUPPLIERS
            LIMIT 5
        """)
        
        for row in cursor.fetchall():
            name, discount, status, rating = row
            logger.info(f"  • {name}: {discount}% discount, {rating} rating, {status}")
        
        cursor.close()
        conn.close()
        
        logger.info("🎉 Discount column test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_discount_column()
    exit(0 if success else 1)