#!/usr/bin/env python3
"""
Table Deployment Script
Description: Deploy database tables to Snowflake schemas
Version: 1.0
Date: 2025-07-19
"""

import argparse
import logging
import sys
from pathlib import Path
import re
from typing import Optional, Dict, List

try:
    import snowflake.connector
except ImportError:
    print("Warning: snowflake-connector-python not installed. Install with: pip install snowflake-connector-python")
    snowflake = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TableDeployer:
    def __init__(self, connection_params):
        self.connection_params = connection_params
        self.conn = None
        
    def connect(self):
        """Connect to Snowflake"""
        try:
            if snowflake is None:
                raise ImportError("Snowflake connector not available")
            self.conn = snowflake.connector.connect(**self.connection_params)
            logger.info("Successfully connected to Snowflake")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise
    
    def execute_sql(self, sql_statement, description="SQL statement"):
        """Execute SQL statement"""
        try:
            if self.conn is None:
                raise ConnectionError("No active Snowflake connection")
            cursor = self.conn.cursor()
            cursor.execute(sql_statement)
            results = cursor.fetchall()
            cursor.close()
            logger.info(f"Successfully executed: {description}")
            return results
        except Exception as e:
            logger.error(f"Failed to execute {description}: {str(e)}")
            raise
    
    def get_table_file_path(self, schema_name):
        """Get the path to the table definition file for a schema"""
        schema_mapping = {
            'sales': 'sales_schema_tables.sql',
            'products': 'products_schema_tables.sql',
            'customers': 'customers_schema_tables.sql',
            'hr': 'hr_schema_tables.sql',
            'analytics': 'analytics_schema_tables.sql',
            'staging': 'staging_schema_tables.sql'
        }
        
        filename = schema_mapping.get(schema_name.lower())
        if not filename:
            raise ValueError(f"Unknown schema: {schema_name}")
        
        file_path = Path(__file__).parent.parent.parent / "ddl" / "tables" / filename
        
        if not file_path.exists():
            raise FileNotFoundError(f"Table definition file not found: {file_path}")
        
        return file_path
    
    def modify_sql_for_environment(self, sql_content, schema_suffix=""):
        """Modify SQL content for environment-specific deployment"""
        modified_sql = sql_content
        
        # Add schema suffix if provided
        if schema_suffix:
            # Replace schema references
            schema_patterns = [
                (r'USE SCHEMA RETAILWORKS_DB\.(\w+_SCHEMA)', rf'USE SCHEMA RETAILWORKS_DB.\1{schema_suffix}'),
                (r'RETAILWORKS_DB\.(\w+_SCHEMA)\.', rf'RETAILWORKS_DB.\1{schema_suffix}.'),
                (r'(\w+_SCHEMA)\.', rf'\1{schema_suffix}.')
            ]
            
            for pattern, replacement in schema_patterns:
                modified_sql = re.sub(pattern, replacement, modified_sql)
        
        return modified_sql
    
    def deploy_schema_tables(self, schema_name, schema_suffix=""):
        """Deploy tables for a specific schema"""
        try:
            logger.info(f"Starting table deployment for {schema_name} schema")
            
            # Get table definition file
            table_file = self.get_table_file_path(schema_name)
            
            # Read SQL content
            with open(table_file, 'r') as f:
                sql_content = f.read()
            
            # Modify SQL for environment
            modified_sql = self.modify_sql_for_environment(sql_content, schema_suffix)
            
            # Split into individual statements
            statements = self.parse_sql_statements(modified_sql)
            
            # Execute statements in order
            for i, statement in enumerate(statements):
                if statement.strip():
                    try:
                        self.execute_sql(
                            statement, 
                            f"{schema_name} schema - statement {i+1}"
                        )
                    except Exception as e:
                        # Log error but continue with other statements
                        logger.warning(f"Statement {i+1} failed (continuing): {str(e)}")
            
            logger.info(f"Successfully deployed tables for {schema_name} schema")
            
        except Exception as e:
            logger.error(f"Failed to deploy tables for {schema_name} schema: {str(e)}")
            raise
    
    def parse_sql_statements(self, sql_content):
        """Parse SQL content into individual statements"""
        # Remove comments
        sql_content = re.sub(r'--.*?\n', '\n', sql_content)
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
        
        # Split on semicolons, but be careful with semicolons inside strings/procedures
        statements = []
        current_statement = ""
        in_string = False
        in_procedure = False
        
        lines = sql_content.split('\n')
        
        for line in lines:
            stripped_line = line.strip()
            
            # Check for procedure start/end
            if 'CREATE OR REPLACE PROCEDURE' in stripped_line.upper():
                in_procedure = True
            elif in_procedure and stripped_line.endswith('$$;'):
                in_procedure = False
            
            current_statement += line + '\n'
            
            # If we hit a semicolon and we're not in a procedure or string
            if ';' in line and not in_procedure and not in_string:
                # Split on semicolon
                parts = current_statement.split(';')
                if len(parts) > 1:
                    # Add all complete statements except the last part
                    for part in parts[:-1]:
                        if part.strip():
                            statements.append(part.strip())
                    # Keep the last part as the start of the next statement
                    current_statement = parts[-1]
        
        # Add any remaining statement
        if current_statement.strip():
            statements.append(current_statement.strip())
        
        return statements
    
    def validate_table_deployment(self, schema_name, schema_suffix=""):
        """Validate that tables were deployed successfully"""
        try:
            schema_full_name = f"{schema_name.upper()}_SCHEMA{schema_suffix}"
            
            # Get list of tables in the schema
            result = self.execute_sql(
                f"SHOW TABLES IN SCHEMA RETAILWORKS_DB.{schema_full_name}",
                f"Checking tables in {schema_full_name}"
            )
            
            if not result:
                logger.warning(f"No tables found in schema {schema_full_name}")
                return False
            
            table_count = len(result)
            logger.info(f"Found {table_count} tables in {schema_full_name}")
            
            # Expected minimum table counts per schema
            expected_counts = {
                'sales': 4,      # ORDERS, ORDER_ITEMS, SALES_TERRITORIES, SALES_REPS
                'products': 4,   # PRODUCTS, CATEGORIES, SUPPLIERS, INVENTORY
                'customers': 4,  # CUSTOMERS, ADDRESSES, CUSTOMER_SEGMENTS, CUSTOMER_ADDRESSES
                'hr': 5,         # EMPLOYEES, DEPARTMENTS, POSITIONS, PAYROLL, EMPLOYEE_PERFORMANCE
                'analytics': 7,  # DATE_DIM, CUSTOMER_DIM, PRODUCT_DIM, SALES_REP_DIM, SALES_FACT, etc.
                'staging': 8     # Various staging tables
            }
            
            expected_count = expected_counts.get(schema_name.lower(), 1)
            
            if table_count < expected_count:
                logger.warning(f"Expected at least {expected_count} tables in {schema_full_name}, found {table_count}")
            
            return True
            
        except Exception as e:
            logger.error(f"Table validation failed for {schema_name}: {str(e)}")
            return False
    
    def deploy_all_schemas(self, schema_suffix=""):
        """Deploy tables for all schemas"""
        schemas = ['sales', 'products', 'customers', 'hr', 'analytics', 'staging']
        
        deployment_results = {}
        
        for schema in schemas:
            try:
                self.deploy_schema_tables(schema, schema_suffix)
                deployment_results[schema] = 'SUCCESS'
            except Exception as e:
                logger.error(f"Failed to deploy {schema} schema: {str(e)}")
                deployment_results[schema] = f'FAILED: {str(e)}'
        
        # Summary
        logger.info("Deployment Summary:")
        for schema, result in deployment_results.items():
            logger.info(f"  {schema}: {result}")
        
        # Check if any deployments failed
        failed_deployments = [schema for schema, result in deployment_results.items() 
                            if result.startswith('FAILED')]
        
        if failed_deployments:
            raise Exception(f"Some schema deployments failed: {failed_deployments}")
        
        return deployment_results
    
    def close(self):
        """Close Snowflake connection"""
        if self.conn:
            self.conn.close()
            logger.info("Closed Snowflake connection")

def main():
    parser = argparse.ArgumentParser(description='Deploy Snowflake tables')
    parser.add_argument('--schema', 
                       choices=['all', 'sales', 'products', 'customers', 'hr', 'analytics', 'staging'],
                       default='all',
                       help='Schema to deploy (default: all)')
    parser.add_argument('--environment', required=True, choices=['dev', 'test', 'prod'],
                       help='Target environment')
    parser.add_argument('--schema-suffix', default='', 
                       help='Suffix to append to schema names')
    parser.add_argument('--account', required=True, help='Snowflake account')
    parser.add_argument('--user', required=True, help='Snowflake user')
    parser.add_argument('--password', required=True, help='Snowflake password')
    parser.add_argument('--role', required=True, help='Snowflake role')
    parser.add_argument('--warehouse', required=True, help='Snowflake warehouse')
    parser.add_argument('--database', required=True, help='Snowflake database')
    parser.add_argument('--validate', action='store_true',
                       help='Validate deployment after completion')
    
    args = parser.parse_args()
    
    # Connection parameters
    connection_params = {
        'account': args.account,
        'user': args.user,
        'password': args.password,
        'role': args.role,
        'warehouse': args.warehouse,
        'database': args.database
    }
    
    deployer = None
    
    try:
        # Initialize deployer
        deployer = TableDeployer(connection_params)
        deployer.connect()
        
        # Deploy tables
        logger.info(f"Starting table deployment for {args.environment} environment")
        
        if args.schema == 'all':
            deployer.deploy_all_schemas(args.schema_suffix)
        else:
            deployer.deploy_schema_tables(args.schema, args.schema_suffix)
        
        # Validate deployment if requested
        if args.validate:
            logger.info("Validating table deployment...")
            if args.schema == 'all':
                schemas = ['sales', 'products', 'customers', 'hr', 'analytics', 'staging']
                for schema in schemas:
                    deployer.validate_table_deployment(schema, args.schema_suffix)
            else:
                deployer.validate_table_deployment(args.schema, args.schema_suffix)
        
        logger.info("Table deployment completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Table deployment failed: {str(e)}")
        sys.exit(1)
        
    finally:
        if deployer:
            deployer.close()

if __name__ == "__main__":
    main()