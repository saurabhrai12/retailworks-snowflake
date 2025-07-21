"""
Show Exact Data Locations
Description: Shows exactly where sample data was loaded in Snowflake
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
        
        print("🔍 SAMPLE DATA LOCATION REPORT")
        print("=" * 60)
        print(f"Database: {os.getenv('SNOWFLAKE_DATABASE', 'RETAILWORKS_DB_DEV')}")
        print("=" * 60)
        
        # Check what schemas exist
        cursor.execute("SHOW SCHEMAS")
        schemas = [row[1] for row in cursor.fetchall()]
        print(f"Available Schemas: {schemas}")
        print()
        
        # Define expected tables and check them
        table_locations = [
            ('CUSTOMERS_SCHEMA_DEV', 'CUSTOMER_SEGMENTS', 'Customer segment types'),
            ('CUSTOMERS_SCHEMA_DEV', 'CUSTOMERS', 'Customer master data'),
            ('CUSTOMERS_SCHEMA_DEV', 'CUSTOMER_ADDRESSES', 'Customer addresses'),
            ('PRODUCTS_SCHEMA_DEV', 'CATEGORIES', 'Product categories'),
            ('PRODUCTS_SCHEMA_DEV', 'SUPPLIERS', 'Supplier information'),
            ('PRODUCTS_SCHEMA_DEV', 'PRODUCTS', 'Product catalog'),
            ('HR_SCHEMA_DEV', 'DEPARTMENTS', 'HR departments'),
            ('HR_SCHEMA_DEV', 'POSITIONS', 'Job positions'),
            ('SALES_SCHEMA_DEV', 'SALES_TERRITORIES', 'Sales territories'),
            ('ANALYTICS_SCHEMA_DEV', 'DATE_DIM', 'Date dimension'),
            ('ANALYTICS_SCHEMA_DEV', 'SALES_FACT', 'Sales transactions'),
            ('ANALYTICS_SCHEMA_DEV', 'CUSTOMER_ANALYTICS_FACT', 'Customer analytics')
        ]
        
        loaded_tables = []
        empty_tables = []
        missing_tables = []
        
        for schema, table, description in table_locations:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                count = cursor.fetchone()[0]
                
                full_name = f"{os.getenv('SNOWFLAKE_DATABASE')}.{schema}.{table}"
                
                if count > 0:
                    loaded_tables.append((full_name, count, description))
                    print(f"✅ {full_name}")
                    print(f"   📊 {count:,} records - {description}")
                    print()
                else:
                    empty_tables.append((full_name, description))
                    
            except Exception as e:
                missing_tables.append((f"{schema}.{table}", str(e)))
        
        # Show summary
        print("📋 SUMMARY:")
        print("-" * 40)
        print(f"✅ Tables with Data: {len(loaded_tables)}")
        print(f"⚠️  Empty Tables: {len(empty_tables)}")
        print(f"❌ Missing Tables: {len(missing_tables)}")
        print()
        
        if loaded_tables:
            print("🎯 TABLES WITH DATA (Copy these paths for queries):")
            print("-" * 50)
            for table_path, count, desc in loaded_tables:
                print(f"SELECT * FROM {table_path}; -- {count:,} records")
            print()
        
        if empty_tables:
            print("⚠️  EMPTY TABLES:")
            print("-" * 20)
            for table_path, desc in empty_tables:
                print(f"   {table_path} - {desc}")
            print()
        
        if missing_tables:
            print("❌ MISSING/ERROR TABLES:")
            print("-" * 25)
            for table_path, error in missing_tables:
                print(f"   {table_path}: {error}")
            print()
        
        # Show sample queries
        if loaded_tables:
            print("🔍 SAMPLE QUERIES TO VIEW DATA:")
            print("-" * 35)
            print(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE')};")
            print()
            for table_path, count, desc in loaded_tables[:5]:  # Show first 5
                schema_table = table_path.split('.', 1)[1]  # Remove database part
                print(f"-- View {desc}")
                print(f"SELECT * FROM {schema_table} LIMIT 10;")
                print()
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()