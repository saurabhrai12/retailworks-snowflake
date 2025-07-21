"""
CSV Data Loader for Snowflake
Description: Loads generated CSV data into Snowflake staging and target tables
Version: 1.0
Date: 2025-07-20
"""

import os
import sys
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging
from typing import Dict, List
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SnowflakeCSVLoader:
    def __init__(self):
        self.connection = None
        self.csv_directory = Path(__file__).parent
        self.csv_files = {
            'customer_segments': 'customer_segments.csv',
            'categories': 'categories.csv', 
            'suppliers': 'suppliers.csv',
            'products': 'products.csv',
            'addresses': 'addresses.csv',
            'customers': 'customers.csv',
            'departments': 'departments.csv',
            'positions': 'positions.csv'
        }
        
    def connect_to_snowflake(self):
        """Establish connection to Snowflake"""
        try:
            connection_params = {
                'account': os.getenv('SNOWFLAKE_ACCOUNT', 'your_account'),
                'user': os.getenv('SNOWFLAKE_USER', 'your_user'),
                'password': os.getenv('SNOWFLAKE_PASSWORD', 'your_password'),
                'role': os.getenv('SNOWFLAKE_ROLE', 'PUBLIC'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
                'database': os.getenv('SNOWFLAKE_DATABASE', 'RETAILWORKS_DB'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA', 'STAGING_SCHEMA')
            }
            
            self.connection = snowflake.connector.connect(**connection_params)
            logger.info("Successfully connected to Snowflake")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            return False
    
    def execute_sql(self, sql: str, params=None):
        """Execute SQL statement"""
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()
            return result
        except Exception as e:
            logger.error(f"SQL execution failed: {str(e)}")
            raise
    
    def load_csv_to_staging(self, csv_name: str, table_name: str, schema: str = "STAGING_SCHEMA"):
        """Load CSV file to staging table"""
        csv_path = self.csv_directory / self.csv_files[csv_name]
        
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            return False
            
        try:
            # Read CSV
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df)} records from {csv_path}")
            
            # Add metadata columns
            df['LOAD_TIMESTAMP'] = pd.Timestamp.now()
            df['FILE_NAME'] = csv_path.name
            df['ROW_NUMBER'] = range(1, len(df) + 1)
            
            # Use write_pandas to load data
            success, nchunks, nrows, _ = write_pandas(
                conn=self.connection,
                df=df,
                table_name=table_name,
                database=os.getenv('SNOWFLAKE_DATABASE', 'RETAILWORKS_DB'),
                schema=schema,
                auto_create_table=False,
                overwrite=True
            )
            
            if success:
                logger.info(f"Successfully loaded {nrows} rows to {schema}.{table_name}")
                return True
            else:
                logger.error(f"Failed to load data to {schema}.{table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error loading {csv_name}: {str(e)}")
            return False
    
    def transform_and_load_clean_data(self):
        """Transform raw staging data to clean tables"""
        transformations = {
            'customer_segments': """
                INSERT INTO STG_CUSTOMER_SEGMENTS_CLEAN 
                (SEGMENT_ID, SEGMENT_NAME, DESCRIPTION, MIN_ANNUAL_REVENUE, MAX_ANNUAL_REVENUE, DISCOUNT_RATE, SOURCE_FILE)
                SELECT 
                    TRY_CAST(SEGMENT_ID AS NUMBER) as SEGMENT_ID,
                    SEGMENT_NAME,
                    DESCRIPTION,
                    TRY_CAST(MIN_ANNUAL_REVENUE AS DECIMAL(15,2)) as MIN_ANNUAL_REVENUE,
                    TRY_CAST(MAX_ANNUAL_REVENUE AS DECIMAL(15,2)) as MAX_ANNUAL_REVENUE,
                    TRY_CAST(DISCOUNT_RATE AS DECIMAL(5,4)) as DISCOUNT_RATE,
                    FILE_NAME
                FROM STG_CUSTOMER_SEGMENTS_RAW 
                WHERE SEGMENT_ID IS NOT NULL
            """,
            'categories': """
                INSERT INTO STG_CATEGORIES_CLEAN 
                (CATEGORY_ID, CATEGORY_NAME, DESCRIPTION, PARENT_CATEGORY_ID, SOURCE_FILE)
                SELECT 
                    TRY_CAST(CATEGORY_ID AS NUMBER) as CATEGORY_ID,
                    CATEGORY_NAME,
                    DESCRIPTION,
                    TRY_CAST(NULLIF(PARENT_CATEGORY_ID, '') AS NUMBER) as PARENT_CATEGORY_ID,
                    FILE_NAME
                FROM STG_CATEGORIES_RAW 
                WHERE CATEGORY_ID IS NOT NULL
            """,
            'suppliers': """
                INSERT INTO STG_SUPPLIERS_CLEAN 
                (SUPPLIER_ID, SUPPLIER_NAME, CONTACT_NAME, CONTACT_TITLE, ADDRESS, CITY, REGION, 
                 POSTAL_CODE, COUNTRY, PHONE, EMAIL, WEBSITE, STATUS, RATING, SOURCE_FILE)
                SELECT 
                    TRY_CAST(SUPPLIER_ID AS NUMBER) as SUPPLIER_ID,
                    SUPPLIER_NAME, CONTACT_NAME, CONTACT_TITLE, ADDRESS, CITY, REGION,
                    POSTAL_CODE, COUNTRY, PHONE, EMAIL, WEBSITE, STATUS,
                    TRY_CAST(RATING AS DECIMAL(3,1)) as RATING,
                    FILE_NAME
                FROM STG_SUPPLIERS_RAW 
                WHERE SUPPLIER_ID IS NOT NULL
            """,
            'addresses': """
                INSERT INTO STG_ADDRESSES_CLEAN 
                (ADDRESS_ID, CUSTOMER_ID, ADDRESS_TYPE, ADDRESS_LINE_1, ADDRESS_LINE_2, 
                 CITY, STATE_PROVINCE, POSTAL_CODE, COUNTRY, IS_DEFAULT, SOURCE_FILE)
                SELECT 
                    TRY_CAST(ADDRESS_ID AS NUMBER) as ADDRESS_ID,
                    TRY_CAST(CUSTOMER_ID AS NUMBER) as CUSTOMER_ID,
                    ADDRESS_TYPE, ADDRESS_LINE_1, ADDRESS_LINE_2, CITY, STATE_PROVINCE,
                    POSTAL_CODE, COUNTRY,
                    TRY_CAST(IS_DEFAULT AS BOOLEAN) as IS_DEFAULT,
                    FILE_NAME
                FROM STG_ADDRESSES_RAW 
                WHERE ADDRESS_ID IS NOT NULL
            """,
            'departments': """
                INSERT INTO STG_DEPARTMENTS_CLEAN 
                (DEPARTMENT_ID, DEPARTMENT_NAME, DEPARTMENT_CODE, DESCRIPTION, BUDGET, 
                 LOCATION, PHONE, EMAIL, SOURCE_FILE)
                SELECT 
                    TRY_CAST(DEPARTMENT_ID AS NUMBER) as DEPARTMENT_ID,
                    DEPARTMENT_NAME, DEPARTMENT_CODE, DESCRIPTION,
                    TRY_CAST(BUDGET AS DECIMAL(15,2)) as BUDGET,
                    LOCATION, PHONE, EMAIL, FILE_NAME
                FROM STG_DEPARTMENTS_RAW 
                WHERE DEPARTMENT_ID IS NOT NULL
            """,
            'positions': """
                INSERT INTO STG_POSITIONS_CLEAN 
                (POSITION_ID, POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, 
                 MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS, SOURCE_FILE)
                SELECT 
                    TRY_CAST(POSITION_ID AS NUMBER) as POSITION_ID,
                    POSITION_TITLE, POSITION_CODE,
                    TRY_CAST(DEPARTMENT_ID AS NUMBER) as DEPARTMENT_ID,
                    TRY_CAST(JOB_LEVEL AS NUMBER) as JOB_LEVEL,
                    TRY_CAST(MIN_SALARY AS DECIMAL(10,2)) as MIN_SALARY,
                    TRY_CAST(MAX_SALARY AS DECIMAL(10,2)) as MAX_SALARY,
                    DESCRIPTION, STATUS, FILE_NAME
                FROM STG_POSITIONS_RAW 
                WHERE POSITION_ID IS NOT NULL
            """
        }
        
        # Clear clean tables first
        for table in transformations.keys():
            try:
                self.execute_sql(f"DELETE FROM STG_{table.upper()}_CLEAN")
                logger.info(f"Cleared STG_{table.upper()}_CLEAN table")
            except Exception as e:
                logger.warning(f"Could not clear STG_{table.upper()}_CLEAN: {str(e)}")
        
        # Execute transformations
        for table, sql in transformations.items():
            try:
                self.execute_sql(sql)
                logger.info(f"Successfully transformed {table} data to clean table")
            except Exception as e:
                logger.error(f"Failed to transform {table}: {str(e)}")
    
    def load_to_dimensional_tables(self):
        """Load data from staging to dimensional tables"""
        
        # Load customer segments
        try:
            sql = """
            USE SCHEMA CUSTOMERS_SCHEMA;
            DELETE FROM CUSTOMER_SEGMENTS WHERE SEGMENT_ID > 1000;
            INSERT INTO CUSTOMER_SEGMENTS (SEGMENT_NAME, DESCRIPTION, MIN_ANNUAL_REVENUE, MAX_ANNUAL_REVENUE, DISCOUNT_RATE)
            SELECT SEGMENT_NAME, DESCRIPTION, MIN_ANNUAL_REVENUE, MAX_ANNUAL_REVENUE, DISCOUNT_RATE
            FROM STAGING_SCHEMA.STG_CUSTOMER_SEGMENTS_CLEAN 
            WHERE VALIDATION_STATUS = 'VALID' AND SEGMENT_ID > 4;
            """
            self.execute_sql(sql)
            logger.info("Loaded customer segments to dimensional table")
        except Exception as e:
            logger.error(f"Failed to load customer segments: {str(e)}")
        
        # Load categories
        try:
            sql = """
            USE SCHEMA PRODUCTS_SCHEMA;
            DELETE FROM CATEGORIES WHERE CATEGORY_ID > 1000;
            INSERT INTO CATEGORIES (CATEGORY_NAME, DESCRIPTION, PARENT_CATEGORY_ID)
            SELECT CATEGORY_NAME, DESCRIPTION, PARENT_CATEGORY_ID
            FROM STAGING_SCHEMA.STG_CATEGORIES_CLEAN 
            WHERE VALIDATION_STATUS = 'VALID' AND CATEGORY_ID > 12;
            """
            self.execute_sql(sql)
            logger.info("Loaded categories to dimensional table")
        except Exception as e:
            logger.error(f"Failed to load categories: {str(e)}")
        
        # Load suppliers  
        try:
            sql = """
            USE SCHEMA PRODUCTS_SCHEMA;
            DELETE FROM SUPPLIERS WHERE SUPPLIER_ID > 1000;
            INSERT INTO SUPPLIERS (SUPPLIER_NAME, CONTACT_NAME, CONTACT_TITLE, ADDRESS, CITY, REGION, 
                                 POSTAL_CODE, COUNTRY, PHONE, EMAIL, WEBSITE, STATUS, RATING)
            SELECT SUPPLIER_NAME, CONTACT_NAME, CONTACT_TITLE, ADDRESS, CITY, REGION,
                   POSTAL_CODE, COUNTRY, PHONE, EMAIL, WEBSITE, STATUS, RATING
            FROM STAGING_SCHEMA.STG_SUPPLIERS_CLEAN 
            WHERE VALIDATION_STATUS = 'VALID';
            """
            self.execute_sql(sql)
            logger.info("Loaded suppliers to dimensional table")
        except Exception as e:
            logger.error(f"Failed to load suppliers: {str(e)}")
        
        # Load departments
        try:
            sql = """
            USE SCHEMA HR_SCHEMA;
            DELETE FROM DEPARTMENTS WHERE DEPARTMENT_ID > 1000;
            INSERT INTO DEPARTMENTS (DEPARTMENT_NAME, DEPARTMENT_CODE, DESCRIPTION, BUDGET, LOCATION, PHONE, EMAIL)
            SELECT DEPARTMENT_NAME, DEPARTMENT_CODE, DESCRIPTION, BUDGET, LOCATION, PHONE, EMAIL
            FROM STAGING_SCHEMA.STG_DEPARTMENTS_CLEAN 
            WHERE VALIDATION_STATUS = 'VALID';
            """
            self.execute_sql(sql)
            logger.info("Loaded departments to dimensional table")
        except Exception as e:
            logger.error(f"Failed to load departments: {str(e)}")
        
        # Load positions
        try:
            sql = """
            USE SCHEMA HR_SCHEMA;
            DELETE FROM POSITIONS WHERE POSITION_ID > 1000;
            INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, 
                                 MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
            SELECT p.POSITION_TITLE, p.POSITION_CODE, d.DEPARTMENT_ID, p.JOB_LEVEL,
                   p.MIN_SALARY, p.MAX_SALARY, p.DESCRIPTION, p.STATUS
            FROM STAGING_SCHEMA.STG_POSITIONS_CLEAN p
            JOIN DEPARTMENTS d ON d.DEPARTMENT_ID = p.DEPARTMENT_ID  
            WHERE p.VALIDATION_STATUS = 'VALID';
            """
            self.execute_sql(sql)
            logger.info("Loaded positions to dimensional table")
        except Exception as e:
            logger.error(f"Failed to load positions: {str(e)}")
    
    def load_all_data(self):
        """Main method to load all CSV data"""
        if not self.connect_to_snowflake():
            return False
        
        try:
            # Set context
            self.execute_sql(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE', 'RETAILWORKS_DB')}")
            self.execute_sql("USE SCHEMA STAGING_SCHEMA")
            
            # Load raw CSV data to staging tables
            staging_mappings = {
                'customer_segments': 'STG_CUSTOMER_SEGMENTS_RAW',
                'categories': 'STG_CATEGORIES_RAW', 
                'suppliers': 'STG_SUPPLIERS_RAW',
                'addresses': 'STG_ADDRESSES_RAW',
                'departments': 'STG_DEPARTMENTS_RAW',
                'positions': 'STG_POSITIONS_RAW'
            }
            
            for csv_name, table_name in staging_mappings.items():
                success = self.load_csv_to_staging(csv_name, table_name)
                if not success:
                    logger.warning(f"Failed to load {csv_name}, continuing with others...")
            
            # Transform to clean tables
            logger.info("Transforming data to clean staging tables...")
            self.transform_and_load_clean_data()
            
            # Load to dimensional tables
            logger.info("Loading data to dimensional tables...")
            self.load_to_dimensional_tables()
            
            logger.info("Data loading process completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Data loading failed: {str(e)}")
            return False
        
        finally:
            if self.connection:
                self.connection.close()
                logger.info("Snowflake connection closed")

def main():
    """Main execution function"""
    loader = SnowflakeCSVLoader()
    
    # Check if CSV files exist
    missing_files = []
    for name, filename in loader.csv_files.items():
        if not (loader.csv_directory / filename).exists():
            missing_files.append(filename)
    
    if missing_files:
        logger.error(f"Missing CSV files: {', '.join(missing_files)}")
        logger.info("Please run generate_sample_data.py first to create the CSV files")
        return False
    
    # Load data
    success = loader.load_all_data()
    
    if success:
        print("✅ Data loading completed successfully!")
        return True
    else:
        print("❌ Data loading failed. Check logs for details.")
        return False

if __name__ == "__main__":
    main()