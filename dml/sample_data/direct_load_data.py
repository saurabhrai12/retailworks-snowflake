"""
Direct CSV to Tables Loader
Description: Loads CSV data directly into dimensional tables
Version: 1.0
Date: 2025-07-20
"""

import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
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

def load_csv_to_table(conn, csv_file, schema, table, column_mapping=None):
    """Load CSV directly to target table"""
    try:
        # Set database and schema context
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE')}")
        cursor.execute(f"USE SCHEMA {schema}")
        cursor.close()
        
        df = pd.read_csv(f'./dml/sample_data/{csv_file}')
        logger.info(f"Loaded {len(df)} records from {csv_file}")
        
        if column_mapping:
            # Check which columns exist in the dataframe
            available_cols = [col for col in column_mapping.keys() if col in df.columns]
            df = df[available_cols].rename(columns={col: column_mapping[col] for col in available_cols})
            
            # Truncate long strings to fit column limits
            string_columns = df.select_dtypes(include=['object']).columns
            for col in string_columns:
                if col == 'COUNTRY':
                    df[col] = df[col].astype(str).str[:30]  # COUNTRY limit
                elif col == 'PHONE':
                    df[col] = df[col].astype(str).str[:20]  # PHONE limit  
                elif col == 'REGION':
                    df[col] = df[col].astype(str).str[:50]  # REGION limit
        
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table,
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=schema,
            auto_create_table=False,
            overwrite=True
        )
        
        if success:
            logger.info(f"✅ Loaded {nrows} rows to {schema}.{table}")
        return success
        
    except Exception as e:
        logger.error(f"❌ Failed to load {csv_file}: {str(e)}")
        return False

def main():
    try:
        conn = connect_snowflake()
        logger.info("Connected to Snowflake")
        
        # Load customer segments
        load_csv_to_table(
            conn, 'customer_segments.csv', 'CUSTOMERS_SCHEMA_DEV', 'CUSTOMER_SEGMENTS',
            {'SEGMENT_NAME': 'SEGMENT_NAME', 'DESCRIPTION': 'DESCRIPTION', 
             'MIN_ANNUAL_REVENUE': 'MIN_ANNUAL_REVENUE', 'MAX_ANNUAL_REVENUE': 'MAX_ANNUAL_REVENUE',
             'DISCOUNT_RATE': 'DISCOUNT_RATE'}
        )
        
        # Load categories  
        load_csv_to_table(
            conn, 'categories.csv', 'PRODUCTS_SCHEMA_DEV', 'CATEGORIES',
            {'CATEGORY_NAME': 'CATEGORY_NAME', 'DESCRIPTION': 'DESCRIPTION',
             'PARENT_CATEGORY_ID': 'PARENT_CATEGORY_ID'}
        )
        
        # Load suppliers
        supplier_mapping = {
            'SUPPLIER_NAME': 'SUPPLIER_NAME', 'CONTACT_NAME': 'CONTACT_NAME',
            'CONTACT_TITLE': 'CONTACT_TITLE', 'ADDRESS': 'ADDRESS', 'CITY': 'CITY',
            'REGION': 'REGION', 'POSTAL_CODE': 'POSTAL_CODE', 'COUNTRY': 'COUNTRY',
            'PHONE': 'PHONE', 'EMAIL': 'EMAIL', 'WEBSITE': 'WEBSITE',
            'STATUS': 'STATUS', 'RATING': 'RATING', 'DISCOUNT': 'DISCOUNT'
        }
        load_csv_to_table(conn, 'suppliers.csv', 'PRODUCTS_SCHEMA_DEV', 'SUPPLIERS', supplier_mapping)
        
        # Load departments
        dept_mapping = {
            'DEPARTMENT_NAME': 'DEPARTMENT_NAME', 'DEPARTMENT_CODE': 'DEPARTMENT_CODE',
            'DESCRIPTION': 'DESCRIPTION', 'BUDGET': 'BUDGET', 'LOCATION': 'LOCATION',
            'PHONE': 'PHONE', 'EMAIL': 'EMAIL'
        }
        load_csv_to_table(conn, 'departments.csv', 'HR_SCHEMA_DEV', 'DEPARTMENTS', dept_mapping)
        
        # Load customers
        customer_mapping = {
            'CUSTOMER_NUMBER': 'CUSTOMER_NUMBER', 'CUSTOMER_TYPE': 'CUSTOMER_TYPE',
            'COMPANY_NAME': 'COMPANY_NAME', 'FIRST_NAME': 'FIRST_NAME', 'LAST_NAME': 'LAST_NAME',
            'EMAIL': 'EMAIL', 'PHONE': 'PHONE', 'BIRTH_DATE': 'BIRTH_DATE',
            'GENDER': 'GENDER', 'ANNUAL_INCOME': 'ANNUAL_INCOME', 'STATUS': 'STATUS',
            'PREFERRED_LANGUAGE': 'PREFERRED_LANGUAGE', 'MARKETING_OPT_IN': 'MARKETING_OPT_IN',
            'REGISTRATION_DATE': 'REGISTRATION_DATE'
        }
        load_csv_to_table(conn, 'customers.csv', 'CUSTOMERS_SCHEMA_DEV', 'CUSTOMERS', customer_mapping)
        
        # Load products (this will reference the categories and suppliers we just loaded)
        product_mapping = {
            'PRODUCT_NUMBER': 'PRODUCT_NUMBER', 'PRODUCT_NAME': 'PRODUCT_NAME',
            'DESCRIPTION': 'DESCRIPTION', 'COLOR': 'COLOR', 'SIZE': 'SIZE',
            'WEIGHT': 'WEIGHT', 'UNIT_PRICE': 'UNIT_PRICE', 'COST': 'COST',
            'LIST_PRICE': 'LIST_PRICE', 'DISCONTINUED': 'DISCONTINUED'
        }
        load_csv_to_table(conn, 'products.csv', 'PRODUCTS_SCHEMA_DEV', 'PRODUCTS', product_mapping)
        
        logger.info("🎉 All data loading completed!")
        
    except Exception as e:
        logger.error(f"❌ Data loading failed: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()