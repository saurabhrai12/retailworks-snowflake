"""
ETL Pipeline - Snowpark Application
Description: Data transformation and loading pipeline using Snowpark
Version: 1.0
Date: 2025-07-19
"""

import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, when, regexp_replace, upper, lower, trim
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
import logging
from typing import Dict, List, Optional
from datetime import datetime

class RetailWorksETL:
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)
        
    def extract_staging_data(self, table_name: str) -> Dict:
        """Extract data from staging tables"""
        try:
            # Get raw data from staging
            raw_df = self.session.table(f"RETAILWORKS_DB.STAGING_SCHEMA.STG_{table_name}_RAW")
            
            # Get metadata
            record_count = raw_df.count()
            
            self.logger.info(f"Extracted {record_count} records from STG_{table_name}_RAW")
            
            return {
                'dataframe': raw_df,
                'record_count': record_count,
                'extraction_time': datetime.now()
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting staging data for {table_name}: {str(e)}")
            raise
    
    def transform_customers(self, raw_df) -> Dict:
        """Transform customer data with validation and cleansing"""
        try:
            # Clean and transform customer data
            transformed_df = raw_df.select(
                trim(upper(col("CUSTOMER_NUMBER"))).alias("CUSTOMER_NUMBER"),
                trim(upper(col("CUSTOMER_TYPE"))).alias("CUSTOMER_TYPE"),
                trim(col("COMPANY_NAME")).alias("COMPANY_NAME"),
                trim(col("FIRST_NAME")).alias("FIRST_NAME"),
                trim(col("LAST_NAME")).alias("LAST_NAME"),
                lower(trim(col("EMAIL"))).alias("EMAIL"),
                regexp_replace(col("PHONE"), "[^0-9]", "").alias("PHONE"),
                col("BIRTH_DATE").cast(DateType()).alias("BIRTH_DATE"),
                upper(col("GENDER")).alias("GENDER"),
                col("ANNUAL_INCOME").cast(DecimalType(15, 2)).alias("ANNUAL_INCOME"),
                trim(col("ADDRESS_LINE_1")).alias("ADDRESS_LINE_1"),
                trim(col("ADDRESS_LINE_2")).alias("ADDRESS_LINE_2"),
                trim(col("CITY")).alias("CITY"),
                trim(col("STATE_PROVINCE")).alias("STATE_PROVINCE"),
                trim(col("POSTAL_CODE")).alias("POSTAL_CODE"),
                trim(upper(col("COUNTRY"))).alias("COUNTRY"),
                col("REGISTRATION_DATE").cast(DateType()).alias("REGISTRATION_DATE"),
                lit("VALID").alias("VALIDATION_STATUS"),
                lit(datetime.now()).alias("PROCESSED_DATE"),
                col("FILE_NAME").alias("SOURCE_FILE")
            ).filter(
                # Data quality filters
                (col("EMAIL").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")) &
                (col("CUSTOMER_NUMBER").isNotNull()) &
                (col("CUSTOMER_TYPE").isin(["INDIVIDUAL", "BUSINESS"]))
            )
            
            # Count valid and invalid records
            valid_count = transformed_df.count()
            total_count = raw_df.count()
            invalid_count = total_count - valid_count
            
            self.logger.info(f"Customer transformation completed. Valid: {valid_count}, Invalid: {invalid_count}")
            
            return {
                'dataframe': transformed_df,
                'valid_count': valid_count,
                'invalid_count': invalid_count,
                'transformation_time': datetime.now()
            }
            
        except Exception as e:
            self.logger.error(f"Error transforming customer data: {str(e)}")
            raise
    
    def transform_products(self, raw_df) -> Dict:
        """Transform product data with validation and cleansing"""
        try:
            transformed_df = raw_df.select(
                trim(upper(col("PRODUCT_NUMBER"))).alias("PRODUCT_NUMBER"),
                trim(col("PRODUCT_NAME")).alias("PRODUCT_NAME"),
                trim(col("CATEGORY_NAME")).alias("CATEGORY_NAME"),
                trim(col("SUPPLIER_NAME")).alias("SUPPLIER_NAME"),
                col("DESCRIPTION").alias("DESCRIPTION"),
                trim(upper(col("COLOR"))).alias("COLOR"),
                trim(upper(col("SIZE"))).alias("SIZE"),
                col("WEIGHT").cast(DecimalType(8, 2)).alias("WEIGHT"),
                col("UNIT_PRICE").cast(DecimalType(10, 2)).alias("UNIT_PRICE"),
                col("COST").cast(DecimalType(10, 2)).alias("COST"),
                col("LIST_PRICE").cast(DecimalType(10, 2)).alias("LIST_PRICE"),
                when(upper(col("DISCONTINUED")) == "TRUE", lit(True))
                .when(upper(col("DISCONTINUED")) == "FALSE", lit(False))
                .otherwise(lit(False)).alias("DISCONTINUED"),
                lit("VALID").alias("VALIDATION_STATUS"),
                lit(datetime.now()).alias("PROCESSED_DATE"),
                col("FILE_NAME").alias("SOURCE_FILE")
            ).filter(
                # Data quality filters
                (col("PRODUCT_NUMBER").isNotNull()) &
                (col("PRODUCT_NAME").isNotNull()) &
                (col("UNIT_PRICE") > 0) &
                (col("COST") >= 0)
            )
            
            valid_count = transformed_df.count()
            total_count = raw_df.count()
            invalid_count = total_count - valid_count
            
            self.logger.info(f"Product transformation completed. Valid: {valid_count}, Invalid: {invalid_count}")
            
            return {
                'dataframe': transformed_df,
                'valid_count': valid_count,
                'invalid_count': invalid_count,
                'transformation_time': datetime.now()
            }
            
        except Exception as e:
            self.logger.error(f"Error transforming product data: {str(e)}")
            raise
    
    def load_clean_data(self, table_name: str, transformed_data: Dict) -> Dict:
        """Load transformed data into clean staging tables"""
        try:
            clean_table = f"RETAILWORKS_DB.STAGING_SCHEMA.STG_{table_name}_CLEAN"
            
            # Truncate clean table
            self.session.sql(f"TRUNCATE TABLE {clean_table}").collect()
            
            # Load transformed data
            transformed_data['dataframe'].write.mode("append").save_as_table(clean_table)
            
            # Verify load
            loaded_count = self.session.table(clean_table).count()
            
            self.logger.info(f"Loaded {loaded_count} records into {clean_table}")
            
            return {
                'table_name': clean_table,
                'loaded_count': loaded_count,
                'load_time': datetime.now()
            }
            
        except Exception as e:
            self.logger.error(f"Error loading clean data for {table_name}: {str(e)}")
            raise
    
    def update_dimensional_tables(self) -> Dict:
        """Update dimensional tables in analytics schema"""
        try:
            results = {}
            
            # Update Customer Dimension (SCD Type 2)
            customer_updates = self.session.sql("""
                MERGE INTO RETAILWORKS_DB.ANALYTICS_SCHEMA.CUSTOMER_DIM tgt
                USING (
                    SELECT 
                        c.CUSTOMER_ID,
                        c.CUSTOMER_NUMBER,
                        COALESCE(c.COMPANY_NAME, c.FIRST_NAME || ' ' || c.LAST_NAME) AS CUSTOMER_NAME,
                        c.CUSTOMER_TYPE,
                        c.COMPANY_NAME,
                        c.EMAIL,
                        c.PHONE,
                        c.BIRTH_DATE,
                        c.GENDER,
                        CASE 
                            WHEN c.BIRTH_DATE IS NOT NULL THEN
                                CASE 
                                    WHEN DATEDIFF('year', c.BIRTH_DATE, CURRENT_DATE()) < 25 THEN '18-24'
                                    WHEN DATEDIFF('year', c.BIRTH_DATE, CURRENT_DATE()) < 35 THEN '25-34'
                                    WHEN DATEDIFF('year', c.BIRTH_DATE, CURRENT_DATE()) < 45 THEN '35-44'
                                    WHEN DATEDIFF('year', c.BIRTH_DATE, CURRENT_DATE()) < 55 THEN '45-54'
                                    WHEN DATEDIFF('year', c.BIRTH_DATE, CURRENT_DATE()) < 65 THEN '55-64'
                                    ELSE '65+'
                                END
                            ELSE 'Unknown'
                        END AS AGE_GROUP,
                        c.MARITAL_STATUS,
                        c.EDUCATION,
                        c.OCCUPATION,
                        c.ANNUAL_INCOME,
                        CASE 
                            WHEN c.ANNUAL_INCOME < 30000 THEN 'Low'
                            WHEN c.ANNUAL_INCOME < 75000 THEN 'Medium'
                            WHEN c.ANNUAL_INCOME < 150000 THEN 'High'
                            ELSE 'Very High'
                        END AS INCOME_CATEGORY,
                        cs.SEGMENT_NAME,
                        ba.CITY AS BILLING_CITY,
                        ba.STATE_PROVINCE AS BILLING_STATE,
                        ba.COUNTRY AS BILLING_COUNTRY,
                        sa.CITY AS SHIPPING_CITY,
                        sa.STATE_PROVINCE AS SHIPPING_STATE,
                        sa.COUNTRY AS SHIPPING_COUNTRY,
                        c.REGISTRATION_DATE,
                        c.STATUS,
                        CURRENT_DATE() AS EFFECTIVE_DATE
                    FROM RETAILWORKS_DB.CUSTOMERS_SCHEMA.CUSTOMERS c
                    LEFT JOIN RETAILWORKS_DB.CUSTOMERS_SCHEMA.CUSTOMER_SEGMENTS cs ON c.SEGMENT_ID = cs.SEGMENT_ID
                    LEFT JOIN RETAILWORKS_DB.CUSTOMERS_SCHEMA.ADDRESSES ba ON c.BILLING_ADDRESS_ID = ba.ADDRESS_ID
                    LEFT JOIN RETAILWORKS_DB.CUSTOMERS_SCHEMA.ADDRESSES sa ON c.SHIPPING_ADDRESS_ID = sa.ADDRESS_ID
                ) src ON tgt.CUSTOMER_ID = src.CUSTOMER_ID AND tgt.IS_CURRENT = TRUE
                WHEN MATCHED AND (
                    tgt.CUSTOMER_NAME != src.CUSTOMER_NAME OR
                    tgt.EMAIL != src.EMAIL OR
                    tgt.ANNUAL_INCOME != src.ANNUAL_INCOME OR
                    tgt.STATUS != src.STATUS
                ) THEN
                    UPDATE SET IS_CURRENT = FALSE, EXPIRY_DATE = CURRENT_DATE()
                WHEN NOT MATCHED THEN
                    INSERT (CUSTOMER_ID, CUSTOMER_NUMBER, CUSTOMER_NAME, CUSTOMER_TYPE, COMPANY_NAME,
                           EMAIL, PHONE, BIRTH_DATE, GENDER, AGE_GROUP, MARITAL_STATUS, EDUCATION, OCCUPATION,
                           ANNUAL_INCOME, INCOME_CATEGORY, SEGMENT_NAME, BILLING_CITY, BILLING_STATE, BILLING_COUNTRY,
                           SHIPPING_CITY, SHIPPING_STATE, SHIPPING_COUNTRY, REGISTRATION_DATE, STATUS,
                           EFFECTIVE_DATE, IS_CURRENT, VERSION)
                    VALUES (src.CUSTOMER_ID, src.CUSTOMER_NUMBER, src.CUSTOMER_NAME, src.CUSTOMER_TYPE, src.COMPANY_NAME,
                           src.EMAIL, src.PHONE, src.BIRTH_DATE, src.GENDER, src.AGE_GROUP, src.MARITAL_STATUS, 
                           src.EDUCATION, src.OCCUPATION, src.ANNUAL_INCOME, src.INCOME_CATEGORY, src.SEGMENT_NAME,
                           src.BILLING_CITY, src.BILLING_STATE, src.BILLING_COUNTRY, src.SHIPPING_CITY, 
                           src.SHIPPING_STATE, src.SHIPPING_COUNTRY, src.REGISTRATION_DATE, src.STATUS,
                           src.EFFECTIVE_DATE, TRUE, 1)
            """).collect()
            
            results['customer_dim_updates'] = len(customer_updates)
            
            # Update Product Dimension (SCD Type 2)
            product_updates = self.session.sql("""
                MERGE INTO RETAILWORKS_DB.ANALYTICS_SCHEMA.PRODUCT_DIM tgt
                USING (
                    SELECT 
                        p.PRODUCT_ID,
                        p.PRODUCT_NUMBER,
                        p.PRODUCT_NAME,
                        c.CATEGORY_NAME,
                        c.CATEGORY_NAME AS CATEGORY_HIERARCHY, -- Simplified for now
                        s.SUPPLIER_NAME,
                        s.COUNTRY AS SUPPLIER_COUNTRY,
                        p.COLOR,
                        p.SIZE,
                        p.WEIGHT,
                        p.UNIT_PRICE,
                        p.COST,
                        p.LIST_PRICE,
                        p.PRODUCT_LINE,
                        p.CLASS,
                        p.STYLE,
                        p.DISCONTINUED,
                        CURRENT_DATE() AS EFFECTIVE_DATE
                    FROM RETAILWORKS_DB.PRODUCTS_SCHEMA.PRODUCTS p
                    LEFT JOIN RETAILWORKS_DB.PRODUCTS_SCHEMA.CATEGORIES c ON p.CATEGORY_ID = c.CATEGORY_ID
                    LEFT JOIN RETAILWORKS_DB.PRODUCTS_SCHEMA.SUPPLIERS s ON p.SUPPLIER_ID = s.SUPPLIER_ID
                ) src ON tgt.PRODUCT_ID = src.PRODUCT_ID AND tgt.IS_CURRENT = TRUE
                WHEN MATCHED AND (
                    tgt.PRODUCT_NAME != src.PRODUCT_NAME OR
                    tgt.UNIT_PRICE != src.UNIT_PRICE OR
                    tgt.COST != src.COST OR
                    tgt.DISCONTINUED != src.DISCONTINUED
                ) THEN
                    UPDATE SET IS_CURRENT = FALSE, EXPIRY_DATE = CURRENT_DATE()
                WHEN NOT MATCHED THEN
                    INSERT (PRODUCT_ID, PRODUCT_NUMBER, PRODUCT_NAME, CATEGORY_NAME, CATEGORY_HIERARCHY,
                           SUPPLIER_NAME, SUPPLIER_COUNTRY, COLOR, SIZE, WEIGHT, UNIT_PRICE, COST, LIST_PRICE,
                           PRODUCT_LINE, CLASS, STYLE, DISCONTINUED, EFFECTIVE_DATE, IS_CURRENT, VERSION)
                    VALUES (src.PRODUCT_ID, src.PRODUCT_NUMBER, src.PRODUCT_NAME, src.CATEGORY_NAME, src.CATEGORY_HIERARCHY,
                           src.SUPPLIER_NAME, src.SUPPLIER_COUNTRY, src.COLOR, src.SIZE, src.WEIGHT, src.UNIT_PRICE, 
                           src.COST, src.LIST_PRICE, src.PRODUCT_LINE, src.CLASS, src.STYLE, src.DISCONTINUED,
                           src.EFFECTIVE_DATE, TRUE, 1)
            """).collect()
            
            results['product_dim_updates'] = len(product_updates)
            
            self.logger.info(f"Dimensional tables updated: {results}")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error updating dimensional tables: {str(e)}")
            raise
    
    def log_etl_process(self, process_name: str, status: str, 
                       records_processed: int = 0, records_inserted: int = 0, 
                       records_updated: int = 0, records_rejected: int = 0,
                       error_message: str = None) -> None:
        """Log ETL process execution"""
        try:
            self.session.sql(f"""
                INSERT INTO RETAILWORKS_DB.STAGING_SCHEMA.ETL_PROCESS_LOG
                (PROCESS_NAME, START_TIME, END_TIME, STATUS, RECORDS_PROCESSED, 
                 RECORDS_INSERTED, RECORDS_UPDATED, RECORDS_REJECTED, ERROR_MESSAGE)
                VALUES ('{process_name}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 
                       '{status}', {records_processed}, {records_inserted}, 
                       {records_updated}, {records_rejected}, 
                       {f"'{error_message}'" if error_message else "NULL"})
            """).collect()
            
        except Exception as e:
            self.logger.error(f"Error logging ETL process: {str(e)}")
    
    def run_full_etl_pipeline(self, table_names: List[str]) -> Dict:
        """Run the complete ETL pipeline for specified tables"""
        try:
            pipeline_results = {}
            total_processed = 0
            total_loaded = 0
            
            for table_name in table_names:
                self.logger.info(f"Starting ETL for {table_name}")
                
                try:
                    # Extract
                    extracted_data = self.extract_staging_data(table_name)
                    
                    # Transform based on table type
                    if table_name.upper() == "CUSTOMERS":
                        transformed_data = self.transform_customers(extracted_data['dataframe'])
                    elif table_name.upper() == "PRODUCTS":
                        transformed_data = self.transform_products(extracted_data['dataframe'])
                    else:
                        self.logger.warning(f"No specific transformation for {table_name}")
                        continue
                    
                    # Load
                    loaded_data = self.load_clean_data(table_name, transformed_data)
                    
                    # Log success
                    self.log_etl_process(
                        f"ETL_{table_name}",
                        "SUCCESS",
                        extracted_data['record_count'],
                        loaded_data['loaded_count'],
                        0,
                        transformed_data['invalid_count']
                    )
                    
                    pipeline_results[table_name] = {
                        'extracted': extracted_data['record_count'],
                        'valid': transformed_data['valid_count'],
                        'invalid': transformed_data['invalid_count'],
                        'loaded': loaded_data['loaded_count']
                    }
                    
                    total_processed += extracted_data['record_count']
                    total_loaded += loaded_data['loaded_count']
                    
                except Exception as e:
                    error_msg = str(e)
                    self.logger.error(f"Error in ETL for {table_name}: {error_msg}")
                    
                    # Log error
                    self.log_etl_process(
                        f"ETL_{table_name}",
                        "ERROR",
                        error_message=error_msg
                    )
                    
                    pipeline_results[table_name] = {'error': error_msg}
            
            # Update dimensional tables
            dim_results = self.update_dimensional_tables()
            pipeline_results['dimensional_updates'] = dim_results
            
            # Log overall pipeline success
            self.log_etl_process(
                "FULL_ETL_PIPELINE",
                "SUCCESS",
                total_processed,
                total_loaded
            )
            
            return pipeline_results
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Error in full ETL pipeline: {error_msg}")
            
            # Log pipeline error
            self.log_etl_process(
                "FULL_ETL_PIPELINE",
                "ERROR",
                error_message=error_msg
            )
            
            raise


def main():
    """Main function to run ETL pipeline"""
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Connection parameters (to be configured)
    connection_parameters = {
        "account": "your_account",
        "user": "your_user",
        "password": "your_password",
        "role": "your_role",
        "warehouse": "your_warehouse",
        "database": "RETAILWORKS_DB",
        "schema": "STAGING_SCHEMA"
    }
    
    try:
        # Create Snowpark session
        session = Session.builder.configs(connection_parameters).create()
        
        # Initialize ETL pipeline
        etl_pipeline = RetailWorksETL(session)
        
        # Run ETL for customers and products
        table_names = ["CUSTOMERS", "PRODUCTS"]
        results = etl_pipeline.run_full_etl_pipeline(table_names)
        
        print("ETL Pipeline Results:")
        for table, result in results.items():
            print(f"{table}: {result}")
            
    except Exception as e:
        print(f"ETL Pipeline failed: {str(e)}")
        raise
    finally:
        if 'session' in locals():
            session.close()


if __name__ == "__main__":
    main()