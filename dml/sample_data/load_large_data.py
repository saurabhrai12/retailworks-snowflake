"""
Load Large Datasets to Snowflake
Description: Loads customers and products data efficiently
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

def load_customers(conn):
    """Load customers data with proper data types"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE')}")
        cursor.execute("USE SCHEMA CUSTOMERS_SCHEMA_DEV")
        cursor.close()
        
        df = pd.read_csv('./dml/sample_data/customers.csv')
        logger.info(f"Loading {len(df)} customers...")
        
        # Select only the columns that exist in the target table
        customer_columns = [
            'CUSTOMER_NUMBER', 'CUSTOMER_TYPE', 'COMPANY_NAME', 'FIRST_NAME', 'LAST_NAME',
            'EMAIL', 'PHONE', 'BIRTH_DATE', 'GENDER', 'ANNUAL_INCOME', 'STATUS',
            'PREFERRED_LANGUAGE', 'MARKETING_OPT_IN', 'REGISTRATION_DATE'
        ]
        
        # Create a new dataframe with only the required columns
        customer_df = pd.DataFrame()
        for col in customer_columns:
            if col in df.columns:
                customer_df[col] = df[col]
            else:
                # Handle missing columns with defaults
                if col == 'STATUS':
                    customer_df[col] = 'ACTIVE'
                elif col == 'PREFERRED_LANGUAGE':
                    customer_df[col] = 'EN'
                elif col == 'MARKETING_OPT_IN':
                    customer_df[col] = True
                else:
                    customer_df[col] = None
        
        # Handle data type conversions
        # Date columns - convert to proper format
        for date_col in ['BIRTH_DATE', 'REGISTRATION_DATE']:
            if date_col in customer_df.columns:
                customer_df[date_col] = pd.to_datetime(customer_df[date_col], errors='coerce').dt.date
        
        # String columns - truncate to fit limits
        customer_df['PHONE'] = customer_df['PHONE'].astype(str).str[:20]
        customer_df['EMAIL'] = customer_df['EMAIL'].astype(str).str[:100]
        
        # Numeric columns
        if 'ANNUAL_INCOME' in customer_df.columns:
            customer_df['ANNUAL_INCOME'] = pd.to_numeric(customer_df['ANNUAL_INCOME'], errors='coerce')
        
        # Boolean columns
        if 'MARKETING_OPT_IN' in customer_df.columns:
            customer_df['MARKETING_OPT_IN'] = customer_df['MARKETING_OPT_IN'].map({
                'True': True, 'true': True, True: True, 1: True,
                'False': False, 'false': False, False: False, 0: False
            }).fillna(True)
        
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=customer_df,
            table_name='CUSTOMERS',
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema='CUSTOMERS_SCHEMA_DEV',
            auto_create_table=False,
            overwrite=True,
            chunk_size=1000  # Process in smaller chunks
        )
        
        if success:
            logger.info(f"✅ Loaded {nrows} customers")
        return success
        
    except Exception as e:
        logger.error(f"❌ Failed to load customers: {str(e)}")
        return False

def load_products(conn):
    """Load products data with category/supplier lookup"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE')}")
        cursor.execute("USE SCHEMA PRODUCTS_SCHEMA_DEV")
        
        # Get category and supplier mappings
        cursor.execute("SELECT CATEGORY_ID, CATEGORY_NAME FROM CATEGORIES")
        categories = {name: id for id, name in cursor.fetchall()}
        
        cursor.execute("SELECT SUPPLIER_ID, SUPPLIER_NAME FROM SUPPLIERS")
        suppliers = {name: id for id, name in cursor.fetchall()}
        
        cursor.close()
        
        df = pd.read_csv('./dml/sample_data/products.csv')
        logger.info(f"Loading {len(df)} products...")
        
        # Check what columns we actually have
        logger.info(f"Available columns: {list(df.columns)}")
        
        # Create product dataframe with proper column mapping
        product_df = pd.DataFrame()
        
        # Map the CSV columns to database columns
        column_mapping = {
            'PRODUCT_NUMBER': 'PRODUCT_NUMBER',
            'PRODUCT_NAME': 'PRODUCT_NAME', 
            'DESCRIPTION': 'DESCRIPTION',
            'COLOR': 'COLOR',
            'SIZE': 'SIZE',
            'WEIGHT': 'WEIGHT',
            'UNIT_PRICE': 'UNIT_PRICE',
            'COST': 'COST',
            'LIST_PRICE': 'LIST_PRICE',
            'DISCONTINUED': 'DISCONTINUED'
        }
        
        # Copy available columns
        for csv_col, db_col in column_mapping.items():
            if csv_col in df.columns:
                product_df[db_col] = df[csv_col]
        
        # Handle category and supplier mapping
        if 'CATEGORY_NAME' in df.columns:
            product_df['CATEGORY_ID'] = df['CATEGORY_NAME'].map(categories).fillna(1)
        else:
            product_df['CATEGORY_ID'] = 1  # Default category
            
        if 'SUPPLIER_NAME' in df.columns:
            product_df['SUPPLIER_ID'] = df['SUPPLIER_NAME'].map(suppliers).fillna(1)
        else:
            product_df['SUPPLIER_ID'] = 1  # Default supplier
        
        # Convert numeric columns with proper error handling
        numeric_cols = ['WEIGHT', 'UNIT_PRICE', 'COST', 'LIST_PRICE']
        for col in numeric_cols:
            if col in product_df.columns:
                product_df[col] = pd.to_numeric(product_df[col], errors='coerce')
        
        # Convert boolean column
        if 'DISCONTINUED' in product_df.columns:
            product_df['DISCONTINUED'] = product_df['DISCONTINUED'].map({
                'True': True, 'true': True, True: True, 1: True,
                'False': False, 'false': False, False: False, 0: False
            }).fillna(False)
        
        # Ensure we have all required columns
        required_cols = ['PRODUCT_NUMBER', 'PRODUCT_NAME', 'CATEGORY_ID', 'SUPPLIER_ID']
        for col in required_cols:
            if col not in product_df.columns:
                if col == 'CATEGORY_ID':
                    product_df[col] = 1
                elif col == 'SUPPLIER_ID':
                    product_df[col] = 1
                else:
                    product_df[col] = None
        
        logger.info(f"Final product columns: {list(product_df.columns)}")
        
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=product_df,
            table_name='PRODUCTS',
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema='PRODUCTS_SCHEMA_DEV',
            auto_create_table=False,
            overwrite=True,
            chunk_size=1000
        )
        
        if success:
            logger.info(f"✅ Loaded {nrows} products")
        return success
        
    except Exception as e:
        logger.error(f"❌ Failed to load products: {str(e)}")
        return False

def load_addresses(conn):
    """Load customer addresses data"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE')}")
        cursor.execute("USE SCHEMA CUSTOMERS_SCHEMA_DEV")
        
        # Get customer ID mappings
        cursor.execute("SELECT CUSTOMER_ID, CUSTOMER_NUMBER FROM CUSTOMERS")
        customers = {num: id for id, num in cursor.fetchall()}
        cursor.close()
        
        df = pd.read_csv('./dml/sample_data/addresses.csv')
        logger.info(f"Loading {len(df)} addresses...")
        
        # Create address dataframe
        address_df = pd.DataFrame()
        
        # Map customer IDs
        if 'CUSTOMER_ID' in df.columns:
            # If we have direct customer ID mapping
            address_df['CUSTOMER_ID'] = df['CUSTOMER_ID']
        elif 'CUSTOMER_NUMBER' in df.columns:
            # Map from customer number to ID
            address_df['CUSTOMER_ID'] = df['CUSTOMER_NUMBER'].map(customers)
        else:
            logger.warning("No customer mapping found in addresses CSV")
            return False
        
        # Map address columns
        address_columns = {
            'ADDRESS_TYPE': 'ADDRESS_TYPE',
            'ADDRESS_LINE_1': 'ADDRESS_LINE_1', 
            'ADDRESS_LINE_2': 'ADDRESS_LINE_2',
            'CITY': 'CITY',
            'STATE_PROVINCE': 'STATE_PROVINCE',
            'POSTAL_CODE': 'POSTAL_CODE',
            'COUNTRY': 'COUNTRY',
            'IS_DEFAULT': 'IS_DEFAULT'
        }
        
        for csv_col, db_col in address_columns.items():
            if csv_col in df.columns:
                address_df[db_col] = df[csv_col]
        
        # Handle boolean column
        if 'IS_DEFAULT' in address_df.columns:
            address_df['IS_DEFAULT'] = address_df['IS_DEFAULT'].map({
                'True': True, 'true': True, True: True, 1: True,
                'False': False, 'false': False, False: False, 0: False
            }).fillna(False)
        
        # Truncate string columns to fit limits
        address_df['COUNTRY'] = address_df['COUNTRY'].astype(str).str[:30]
        address_df['STATE_PROVINCE'] = address_df['STATE_PROVINCE'].astype(str).str[:50]
        
        # Remove rows with missing customer IDs
        address_df = address_df.dropna(subset=['CUSTOMER_ID'])
        
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=address_df,
            table_name='CUSTOMER_ADDRESSES',
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema='CUSTOMERS_SCHEMA_DEV',
            auto_create_table=False,
            overwrite=True,
            chunk_size=1000
        )
        
        if success:
            logger.info(f"✅ Loaded {nrows} addresses")
        return success
        
    except Exception as e:
        logger.error(f"❌ Failed to load addresses: {str(e)}")
        return False

def main():
    try:
        conn = connect_snowflake()
        logger.info("Connected to Snowflake")
        
        # Load customers first (needed for address mapping)
        logger.info("Loading customers...")
        load_customers(conn)
        
        # Load products  
        logger.info("Loading products...")
        load_products(conn)
        
        # Load addresses (depends on customers being loaded)
        logger.info("Loading addresses...")
        load_addresses(conn)
        
        logger.info("🎉 All large data loading completed!")
        
    except Exception as e:
        logger.error(f"❌ Data loading failed: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()