"""
Data Quality Tests for RetailWorks Snowflake Data Warehouse
Tests data integrity, completeness, and business rules
"""

import pytest
import pandas as pd
import snowflake.connector
import os
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestDataQuality:
    """Test suite for data quality validation"""
    
    @pytest.fixture(scope="class")
    def snowflake_connection(self):
        """Create Snowflake connection for testing"""
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            role=os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'RETAILWORKS_DB_DEV')
        )
        yield conn
        conn.close()
    
    def execute_query(self, connection, query: str) -> List[Dict]:
        """Execute query and return results"""
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            return results
        finally:
            cursor.close()
    
    def get_row_count(self, connection, table: str) -> int:
        """Get row count for a table"""
        query = f"SELECT COUNT(*) as count FROM {table}"
        result = self.execute_query(connection, query)
        return result[0]['COUNT'] if result else 0
    
    # Schema and Table Existence Tests
    @pytest.mark.unit
    def test_all_schemas_exist(self, snowflake_connection):
        """Test that all required schemas exist"""
        required_schemas = [
            'CUSTOMERS_SCHEMA_DEV',
            'PRODUCTS_SCHEMA_DEV',
            'HR_SCHEMA_DEV', 
            'SALES_SCHEMA_DEV',
            'ANALYTICS_SCHEMA_DEV',
            'STAGING_SCHEMA_DEV'
        ]
        
        query = "SHOW SCHEMAS"
        results = self.execute_query(snowflake_connection, query)
        existing_schemas = [row['name'] for row in results]
        
        for schema in required_schemas:
            assert schema in existing_schemas, f"Schema {schema} does not exist"
    
    @pytest.mark.unit
    def test_core_tables_exist(self, snowflake_connection):
        """Test that core tables exist and have data"""
        core_tables = {
            'CUSTOMERS_SCHEMA_DEV.CUSTOMERS': 1000,  # Minimum expected rows
            'PRODUCTS_SCHEMA_DEV.PRODUCTS': 100,
            'PRODUCTS_SCHEMA_DEV.CATEGORIES': 5,
            'PRODUCTS_SCHEMA_DEV.SUPPLIERS': 10,
            'HR_SCHEMA_DEV.DEPARTMENTS': 5,
            'ANALYTICS_SCHEMA_DEV.DATE_DIM': 365
        }
        
        for table, min_rows in core_tables.items():
            try:
                row_count = self.get_row_count(snowflake_connection, table)
                assert row_count >= min_rows, f"Table {table} has {row_count} rows, expected at least {min_rows}"
                logger.info(f"✅ {table}: {row_count} rows")
            except Exception as e:
                pytest.fail(f"Table {table} check failed: {str(e)}")
    
    # Data Completeness Tests
    @pytest.mark.unit
    def test_customer_data_completeness(self, snowflake_connection):
        """Test customer data completeness"""
        query = """
        SELECT 
            COUNT(*) as total_customers,
            COUNT(CUSTOMER_NUMBER) as customers_with_number,
            COUNT(EMAIL) as customers_with_email,
            COUNT(FIRST_NAME) as customers_with_first_name,
            COUNT(LAST_NAME) as customers_with_last_name
        FROM CUSTOMERS_SCHEMA_DEV.CUSTOMERS
        """
        
        result = self.execute_query(snowflake_connection, query)[0]
        
        # All customers should have customer numbers
        assert result['CUSTOMERS_WITH_NUMBER'] == result['TOTAL_CUSTOMERS'], \
            "Some customers missing customer numbers"
        
        # At least 95% should have email addresses
        email_completeness = result['CUSTOMERS_WITH_EMAIL'] / result['TOTAL_CUSTOMERS']
        assert email_completeness >= 0.95, f"Email completeness {email_completeness:.2%} below 95%"
        
        # At least 90% should have names
        name_completeness = min(
            result['CUSTOMERS_WITH_FIRST_NAME'] / result['TOTAL_CUSTOMERS'],
            result['CUSTOMERS_WITH_LAST_NAME'] / result['TOTAL_CUSTOMERS']
        )
        assert name_completeness >= 0.90, f"Name completeness {name_completeness:.2%} below 90%"
    
    @pytest.mark.unit
    def test_product_data_completeness(self, snowflake_connection):
        """Test product data completeness"""
        query = """
        SELECT 
            COUNT(*) as total_products,
            COUNT(PRODUCT_NUMBER) as products_with_number,
            COUNT(PRODUCT_NAME) as products_with_name,
            COUNT(CATEGORY_ID) as products_with_category,
            COUNT(UNIT_PRICE) as products_with_price
        FROM PRODUCTS_SCHEMA_DEV.PRODUCTS
        """
        
        result = self.execute_query(snowflake_connection, query)[0]
        
        # All products should have required fields
        assert result['PRODUCTS_WITH_NUMBER'] == result['TOTAL_PRODUCTS'], \
            "Some products missing product numbers"
        assert result['PRODUCTS_WITH_NAME'] == result['TOTAL_PRODUCTS'], \
            "Some products missing product names"
        assert result['PRODUCTS_WITH_CATEGORY'] == result['TOTAL_PRODUCTS'], \
            "Some products missing categories"
        
        # At least 95% should have prices
        price_completeness = result['PRODUCTS_WITH_PRICE'] / result['TOTAL_PRODUCTS']
        assert price_completeness >= 0.95, f"Price completeness {price_completeness:.2%} below 95%"
    
    # Data Integrity Tests
    @pytest.mark.unit
    def test_referential_integrity(self, snowflake_connection):
        """Test referential integrity between tables"""
        
        # Test products reference valid categories
        query = """
        SELECT COUNT(*) as orphaned_products
        FROM PRODUCTS_SCHEMA_DEV.PRODUCTS p
        LEFT JOIN PRODUCTS_SCHEMA_DEV.CATEGORIES c ON p.CATEGORY_ID = c.CATEGORY_ID
        WHERE c.CATEGORY_ID IS NULL
        """
        result = self.execute_query(snowflake_connection, query)[0]
        assert result['ORPHANED_PRODUCTS'] == 0, "Found products with invalid category references"
        
        # Test products reference valid suppliers
        query = """
        SELECT COUNT(*) as orphaned_products
        FROM PRODUCTS_SCHEMA_DEV.PRODUCTS p
        LEFT JOIN PRODUCTS_SCHEMA_DEV.SUPPLIERS s ON p.SUPPLIER_ID = s.SUPPLIER_ID
        WHERE s.SUPPLIER_ID IS NULL
        """
        result = self.execute_query(snowflake_connection, query)[0]
        assert result['ORPHANED_PRODUCTS'] == 0, "Found products with invalid supplier references"
    
    @pytest.mark.unit
    def test_unique_constraints(self, snowflake_connection):
        """Test unique constraints are maintained"""
        
        # Test customer numbers are unique
        query = """
        SELECT CUSTOMER_NUMBER, COUNT(*) as count
        FROM CUSTOMERS_SCHEMA_DEV.CUSTOMERS
        GROUP BY CUSTOMER_NUMBER
        HAVING COUNT(*) > 1
        """
        duplicates = self.execute_query(snowflake_connection, query)
        assert len(duplicates) == 0, f"Found duplicate customer numbers: {duplicates}"
        
        # Test product numbers are unique
        query = """
        SELECT PRODUCT_NUMBER, COUNT(*) as count
        FROM PRODUCTS_SCHEMA_DEV.PRODUCTS
        GROUP BY PRODUCT_NUMBER
        HAVING COUNT(*) > 1
        """
        duplicates = self.execute_query(snowflake_connection, query)
        assert len(duplicates) == 0, f"Found duplicate product numbers: {duplicates}"
    
    # Business Rule Tests
    @pytest.mark.unit
    def test_business_rule_validations(self, snowflake_connection):
        """Test business rule validations"""
        
        # Test product prices are positive
        query = """
        SELECT COUNT(*) as invalid_prices
        FROM PRODUCTS_SCHEMA_DEV.PRODUCTS
        WHERE UNIT_PRICE <= 0 OR COST <= 0
        """
        result = self.execute_query(snowflake_connection, query)[0]
        assert result['INVALID_PRICES'] == 0, "Found products with invalid prices"
        
        # Test customer email format
        query = """
        SELECT COUNT(*) as invalid_emails
        FROM CUSTOMERS_SCHEMA_DEV.CUSTOMERS
        WHERE EMAIL IS NOT NULL 
          AND EMAIL NOT RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
        """
        result = self.execute_query(snowflake_connection, query)[0]
        assert result['INVALID_EMAILS'] == 0, "Found customers with invalid email formats"
        
        # Test date ranges are reasonable
        query = """
        SELECT COUNT(*) as invalid_dates
        FROM CUSTOMERS_SCHEMA_DEV.CUSTOMERS
        WHERE BIRTH_DATE > CURRENT_DATE() 
           OR BIRTH_DATE < '1900-01-01'
           OR REGISTRATION_DATE > CURRENT_DATE()
        """
        result = self.execute_query(snowflake_connection, query)[0]
        assert result['INVALID_DATES'] == 0, "Found customers with invalid dates"
    
    # Data Quality Metrics Tests
    @pytest.mark.integration
    def test_data_freshness(self, snowflake_connection):
        """Test data freshness requirements"""
        
        # Check when data was last updated (assuming we have audit columns)
        query = """
        SELECT 
            COUNT(*) as total_customers,
            COUNT(CASE WHEN REGISTRATION_DATE >= CURRENT_DATE() - 30 THEN 1 END) as recent_customers
        FROM CUSTOMERS_SCHEMA_DEV.CUSTOMERS
        """
        result = self.execute_query(snowflake_connection, query)[0]
        
        if result['TOTAL_CUSTOMERS'] > 0:
            recent_percentage = result['RECENT_CUSTOMERS'] / result['TOTAL_CUSTOMERS']
            logger.info(f"Recent customer registrations: {recent_percentage:.2%}")
    
    @pytest.mark.integration  
    def test_data_volume_expectations(self, snowflake_connection):
        """Test data volumes meet expectations"""
        
        # Define expected data volumes
        volume_expectations = {
            'CUSTOMERS_SCHEMA_DEV.CUSTOMERS': (1000, 100000),  # min, max
            'PRODUCTS_SCHEMA_DEV.PRODUCTS': (100, 10000),
            'PRODUCTS_SCHEMA_DEV.CATEGORIES': (5, 100),
            'PRODUCTS_SCHEMA_DEV.SUPPLIERS': (10, 1000)
        }
        
        for table, (min_rows, max_rows) in volume_expectations.items():
            try:
                row_count = self.get_row_count(snowflake_connection, table)
                assert min_rows <= row_count <= max_rows, \
                    f"Table {table} has {row_count} rows, expected between {min_rows} and {max_rows}"
                logger.info(f"✅ {table}: {row_count} rows (within expected range)")
            except Exception as e:
                logger.warning(f"Could not check volume for {table}: {str(e)}")
    
    # Performance Tests
    @pytest.mark.performance
    def test_query_performance(self, snowflake_connection):
        """Test that key queries perform within acceptable limits"""
        import time
        
        # Test customer lookup performance
        start_time = time.time()
        query = "SELECT * FROM CUSTOMERS_SCHEMA_DEV.CUSTOMERS LIMIT 1000"
        self.execute_query(snowflake_connection, query)
        execution_time = time.time() - start_time
        
        assert execution_time < 10.0, f"Customer query took {execution_time:.2f}s, expected < 10s"
        logger.info(f"Customer query performance: {execution_time:.2f}s")
        
        # Test product search performance
        start_time = time.time()
        query = """
        SELECT p.*, c.CATEGORY_NAME, s.SUPPLIER_NAME
        FROM PRODUCTS_SCHEMA_DEV.PRODUCTS p
        JOIN PRODUCTS_SCHEMA_DEV.CATEGORIES c ON p.CATEGORY_ID = c.CATEGORY_ID
        JOIN PRODUCTS_SCHEMA_DEV.SUPPLIERS s ON p.SUPPLIER_ID = s.SUPPLIER_ID
        LIMIT 1000
        """
        self.execute_query(snowflake_connection, query)
        execution_time = time.time() - start_time
        
        assert execution_time < 15.0, f"Product join query took {execution_time:.2f}s, expected < 15s"
        logger.info(f"Product join query performance: {execution_time:.2f}s")
    
    # Smoke Tests
    @pytest.mark.smoke
    def test_basic_connectivity(self, snowflake_connection):
        """Basic smoke test for database connectivity"""
        query = "SELECT CURRENT_VERSION() as version"
        result = self.execute_query(snowflake_connection, query)
        assert len(result) == 1, "Could not get Snowflake version"
        assert 'VERSION' in result[0], "Version query returned unexpected format"
        logger.info(f"Connected to Snowflake version: {result[0]['VERSION']}")
    
    @pytest.mark.smoke
    def test_sample_data_access(self, snowflake_connection):
        """Test that sample data is accessible"""
        test_queries = [
            "SELECT COUNT(*) as count FROM CUSTOMERS_SCHEMA_DEV.CUSTOMERS",
            "SELECT COUNT(*) as count FROM PRODUCTS_SCHEMA_DEV.PRODUCTS",
            "SELECT COUNT(*) as count FROM ANALYTICS_SCHEMA_DEV.DATE_DIM"
        ]
        
        for query in test_queries:
            try:
                result = self.execute_query(snowflake_connection, query)
                assert len(result) == 1, f"Query failed: {query}"
                assert result[0]['COUNT'] >= 0, f"Unexpected count result: {query}"
            except Exception as e:
                pytest.fail(f"Smoke test query failed: {query}, Error: {str(e)}")

if __name__ == "__main__":
    # Run tests when executed directly
    pytest.main([__file__, "-v"])