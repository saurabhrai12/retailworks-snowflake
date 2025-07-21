"""
Load Addresses with Customer Assignment
Description: Assigns addresses to customers and loads them
"""

import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging
import random

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
        logger.info("Connected to Snowflake")
        
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE')}")
        cursor.execute("USE SCHEMA CUSTOMERS_SCHEMA_DEV")
        
        # Get customer IDs
        cursor.execute("SELECT CUSTOMER_ID FROM CUSTOMERS ORDER BY CUSTOMER_ID")
        customer_ids = [row[0] for row in cursor.fetchall()]
        logger.info(f"Found {len(customer_ids)} customers")
        
        cursor.close()
        
        # Load addresses CSV
        df = pd.read_csv('./dml/sample_data/addresses.csv')
        logger.info(f"Loading {len(df)} addresses...")
        
        # Randomly assign customers to addresses (each customer can have 1-3 addresses)
        address_assignments = []
        random.seed(42)  # For reproducible results
        
        for customer_id in customer_ids:
            num_addresses = random.randint(1, 3)  # 1-3 addresses per customer
            available_addresses = df[~df.index.isin([a[0] for a in address_assignments])]
            
            if len(available_addresses) < num_addresses:
                num_addresses = len(available_addresses)
            
            if num_addresses > 0:
                selected = available_addresses.sample(n=num_addresses)
                for idx, addr in selected.iterrows():
                    is_default = len(address_assignments) == 0 or random.random() < 0.3
                    address_assignments.append((idx, customer_id, is_default))
        
        # Create address dataframe with customer assignments
        address_data = []
        for addr_idx, customer_id, is_default in address_assignments:
            addr = df.loc[addr_idx]
            address_data.append({
                'CUSTOMER_ID': customer_id,
                'ADDRESS_TYPE': addr['ADDRESS_TYPE'],
                'ADDRESS_LINE_1': addr['ADDRESS_LINE_1'],
                'ADDRESS_LINE_2': addr.get('ADDRESS_LINE_2', ''),
                'CITY': addr['CITY'],
                'STATE_PROVINCE': addr['STATE_PROVINCE'][:50],  # Truncate
                'POSTAL_CODE': addr['POSTAL_CODE'],
                'COUNTRY': addr['COUNTRY'][:30],  # Truncate
                'IS_DEFAULT': is_default
            })
        
        address_df = pd.DataFrame(address_data)
        logger.info(f"Created {len(address_df)} customer address records")
        
        # Load to Snowflake
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
            logger.info(f"✅ Loaded {nrows} customer addresses")
        
        logger.info("🎉 Address loading completed!")
        
    except Exception as e:
        logger.error(f"❌ Address loading failed: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()