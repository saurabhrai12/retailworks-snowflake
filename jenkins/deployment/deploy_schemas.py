#!/usr/bin/env python3
"""
Schema Deployment Script
Description: Deploy database schemas to Snowflake
Version: 1.0
Date: 2025-07-19
"""

import argparse
import snowflake.connector
import logging
import sys
import os
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SchemaDeployer:
    def __init__(self, connection_params):
        self.connection_params = connection_params
        self.conn = None
        
    def connect(self):
        """Connect to Snowflake"""
        try:
            self.conn = snowflake.connector.connect(**self.connection_params)
            logger.info("Successfully connected to Snowflake")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise
    
    def execute_sql(self, sql_statement, description="SQL statement"):
        """Execute SQL statement"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql_statement)
            results = cursor.fetchall()
            cursor.close()
            logger.info(f"Successfully executed: {description}")
            return results
        except Exception as e:
            logger.error(f"Failed to execute {description}: {str(e)}")
            raise
    
    def deploy_database_and_schemas(self, schema_suffix=""):
        """Deploy main database and schemas"""
        try:
            # Read the schema creation script
            schema_file = Path(__file__).parent.parent.parent / "ddl" / "schemas" / "01_create_database.sql"
            
            if not schema_file.exists():
                raise FileNotFoundError(f"Schema file not found: {schema_file}")
            
            with open(schema_file, 'r') as f:
                sql_content = f.read()
            
            # Replace schema names with environment-specific names if suffix provided
            if schema_suffix:
                schemas = [
                    'SALES_SCHEMA',
                    'PRODUCTS_SCHEMA', 
                    'CUSTOMERS_SCHEMA',
                    'HR_SCHEMA',
                    'ANALYTICS_SCHEMA',
                    'STAGING_SCHEMA'
                ]
                
                for schema in schemas:
                    sql_content = sql_content.replace(
                        f"CREATE SCHEMA IF NOT EXISTS {schema}",
                        f"CREATE SCHEMA IF NOT EXISTS {schema}{schema_suffix}"
                    )
            
            # Split SQL content into individual statements
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            # Execute each statement
            for i, statement in enumerate(statements):
                if statement:
                    self.execute_sql(statement, f"Schema creation statement {i+1}")
            
            logger.info(f"Successfully deployed database and schemas with suffix '{schema_suffix}'")
            
        except Exception as e:
            logger.error(f"Failed to deploy schemas: {str(e)}")
            raise
    
    def create_environment_specific_roles(self, environment, schema_suffix=""):
        """Create environment-specific roles and permissions"""
        try:
            # Define roles for the environment
            roles = {
                'dev': [
                    f'RETAILWORKS_DEV_ADMIN',
                    f'RETAILWORKS_DEV_DEVELOPER',
                    f'RETAILWORKS_DEV_ANALYST'
                ],
                'test': [
                    f'RETAILWORKS_TEST_ADMIN',
                    f'RETAILWORKS_TEST_TESTER',
                    f'RETAILWORKS_TEST_ANALYST'
                ],
                'prod': [
                    f'RETAILWORKS_PROD_ADMIN',
                    f'RETAILWORKS_PROD_ANALYST',
                    f'RETAILWORKS_PROD_READONLY'
                ]
            }
            
            env_roles = roles.get(environment, [])
            
            for role in env_roles:
                # Create role
                self.execute_sql(
                    f"CREATE ROLE IF NOT EXISTS {role}",
                    f"Creating role {role}"
                )
                
                # Grant database usage
                self.execute_sql(
                    f"GRANT USAGE ON DATABASE RETAILWORKS_DB TO ROLE {role}",
                    f"Granting database usage to {role}"
                )
                
                # Grant schema permissions based on role type
                schemas = [
                    f'SALES_SCHEMA{schema_suffix}',
                    f'PRODUCTS_SCHEMA{schema_suffix}',
                    f'CUSTOMERS_SCHEMA{schema_suffix}',
                    f'HR_SCHEMA{schema_suffix}',
                    f'ANALYTICS_SCHEMA{schema_suffix}',
                    f'STAGING_SCHEMA{schema_suffix}'
                ]
                
                for schema in schemas:
                    if 'ADMIN' in role:
                        # Admin gets all privileges
                        self.execute_sql(
                            f"GRANT ALL PRIVILEGES ON SCHEMA RETAILWORKS_DB.{schema} TO ROLE {role}",
                            f"Granting admin privileges on {schema} to {role}"
                        )
                    elif 'DEVELOPER' in role or 'TESTER' in role:
                        # Developers and testers get read/write
                        self.execute_sql(
                            f"GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE PROCEDURE ON SCHEMA RETAILWORKS_DB.{schema} TO ROLE {role}",
                            f"Granting developer privileges on {schema} to {role}"
                        )
                    else:
                        # Analysts and readonly get select only
                        self.execute_sql(
                            f"GRANT USAGE ON SCHEMA RETAILWORKS_DB.{schema} TO ROLE {role}",
                            f"Granting usage on {schema} to {role}"
                        )
            
            logger.info(f"Successfully created roles for {environment} environment")
            
        except Exception as e:
            logger.error(f"Failed to create environment roles: {str(e)}")
            raise
    
    def create_warehouses(self, environment):
        """Create environment-specific warehouses"""
        try:
            warehouse_configs = {
                'dev': {
                    'name': 'RETAILWORKS_DEV_WH',
                    'size': 'X-SMALL',
                    'auto_suspend': 60,
                    'auto_resume': True
                },
                'test': {
                    'name': 'RETAILWORKS_TEST_WH',
                    'size': 'SMALL',
                    'auto_suspend': 300,
                    'auto_resume': True
                },
                'prod': {
                    'name': 'RETAILWORKS_PROD_WH',
                    'size': 'MEDIUM',
                    'auto_suspend': 600,
                    'auto_resume': True
                }
            }
            
            config = warehouse_configs.get(environment)
            if not config:
                logger.warning(f"No warehouse configuration for environment: {environment}")
                return
            
            # Create warehouse
            warehouse_sql = f"""
                CREATE WAREHOUSE IF NOT EXISTS {config['name']}
                WITH WAREHOUSE_SIZE = '{config['size']}'
                AUTO_SUSPEND = {config['auto_suspend']}
                AUTO_RESUME = {config['auto_resume']}
                INITIALLY_SUSPENDED = TRUE
                COMMENT = 'RetailWorks {environment.upper()} environment warehouse'
            """
            
            self.execute_sql(warehouse_sql, f"Creating warehouse {config['name']}")
            
            logger.info(f"Successfully created warehouse for {environment} environment")
            
        except Exception as e:
            logger.error(f"Failed to create warehouse: {str(e)}")
            raise
    
    def validate_deployment(self, schema_suffix=""):
        """Validate schema deployment"""
        try:
            # Check if database exists
            result = self.execute_sql(
                "SHOW DATABASES LIKE 'RETAILWORKS_DB'",
                "Checking database existence"
            )
            
            if not result:
                raise Exception("RETAILWORKS_DB database not found")
            
            # Check if schemas exist
            schemas = [
                f'SALES_SCHEMA{schema_suffix}',
                f'PRODUCTS_SCHEMA{schema_suffix}',
                f'CUSTOMERS_SCHEMA{schema_suffix}',
                f'HR_SCHEMA{schema_suffix}',
                f'ANALYTICS_SCHEMA{schema_suffix}',
                f'STAGING_SCHEMA{schema_suffix}'
            ]
            
            for schema in schemas:
                result = self.execute_sql(
                    f"SHOW SCHEMAS LIKE '{schema}' IN DATABASE RETAILWORKS_DB",
                    f"Checking schema {schema}"
                )
                
                if not result:
                    raise Exception(f"Schema {schema} not found")
            
            logger.info("Schema deployment validation successful")
            return True
            
        except Exception as e:
            logger.error(f"Schema deployment validation failed: {str(e)}")
            raise
    
    def close(self):
        """Close Snowflake connection"""
        if self.conn:
            self.conn.close()
            logger.info("Closed Snowflake connection")

def main():
    parser = argparse.ArgumentParser(description='Deploy Snowflake schemas')
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
    parser.add_argument('--create-roles', action='store_true', 
                       help='Create environment-specific roles')
    parser.add_argument('--create-warehouses', action='store_true',
                       help='Create environment-specific warehouses')
    
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
        deployer = SchemaDeployer(connection_params)
        deployer.connect()
        
        # Deploy schemas
        logger.info(f"Starting schema deployment for {args.environment} environment")
        deployer.deploy_database_and_schemas(args.schema_suffix)
        
        # Create environment-specific roles if requested
        if args.create_roles:
            deployer.create_environment_specific_roles(args.environment, args.schema_suffix)
        
        # Create environment-specific warehouses if requested
        if args.create_warehouses:
            deployer.create_warehouses(args.environment)
        
        # Validate deployment
        deployer.validate_deployment(args.schema_suffix)
        
        logger.info("Schema deployment completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Schema deployment failed: {str(e)}")
        sys.exit(1)
        
    finally:
        if deployer:
            deployer.close()

if __name__ == "__main__":
    main()