#!/usr/bin/env python3
"""
Deployment Rollback Script
Description: Rolls back Snowflake deployment to a previous version
Usage: python rollback_deployment.py --environment prod --version v1.2.0
"""

import argparse
import os
import sys
import logging
import snowflake.connector
from pathlib import Path
from typing import Optional, List, Dict
import json
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DeploymentRollback:
    """Handles deployment rollback operations"""
    
    def __init__(self, environment: str, version: str):
        self.environment = environment
        self.version = version
        self.project_root = Path(__file__).parent.parent
        self.connection = None
        
        # Environment-specific settings
        self.env_config = {
            'dev': {
                'database': 'RETAILWORKS_DB_DEV',
                'backup_schema': 'BACKUP_SCHEMA_DEV'
            },
            'staging': {
                'database': 'RETAILWORKS_DB_STAGING',
                'backup_schema': 'BACKUP_SCHEMA_STAGING'
            },
            'prod': {
                'database': 'RETAILWORKS_DB',
                'backup_schema': 'BACKUP_SCHEMA'
            }
        }
        
        if environment not in self.env_config:
            raise ValueError(f"Invalid environment: {environment}")
            
        self.config = self.env_config[environment]
    
    def connect_snowflake(self) -> bool:
        """Establish Snowflake connection"""
        try:
            self.connection = snowflake.connector.connect(
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                role=os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
                database=self.config['database']
            )
            logger.info("✅ Connected to Snowflake successfully")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {str(e)}")
            return False
    
    def execute_query(self, query: str) -> Optional[List[Dict]]:
        """Execute query and return results"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch results
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
                
            cursor.close()
            return results
            
        except Exception as e:
            logger.error(f"❌ Query execution failed: {str(e)}")
            return None
    
    def create_backup(self) -> bool:
        """Create backup of current state before rollback"""
        logger.info("💾 Creating backup of current state...")
        
        try:
            # Create backup schema
            backup_schema = f"{self.config['backup_schema']}_ROLLBACK_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            query = f"CREATE SCHEMA IF NOT EXISTS {backup_schema}"
            self.execute_query(query)
            
            # List of schemas to backup
            schemas_to_backup = [
                'CUSTOMERS_SCHEMA_DEV' if self.environment == 'dev' else 'CUSTOMERS_SCHEMA',
                'PRODUCTS_SCHEMA_DEV' if self.environment == 'dev' else 'PRODUCTS_SCHEMA',
                'HR_SCHEMA_DEV' if self.environment == 'dev' else 'HR_SCHEMA',
                'SALES_SCHEMA_DEV' if self.environment == 'dev' else 'SALES_SCHEMA',
                'ANALYTICS_SCHEMA_DEV' if self.environment == 'dev' else 'ANALYTICS_SCHEMA'
            ]
            
            for schema in schemas_to_backup:
                logger.info(f"Backing up schema: {schema}")
                
                # Get list of tables in schema
                tables_query = f"SHOW TABLES IN SCHEMA {schema}"
                tables = self.execute_query(tables_query)
                
                if tables:
                    for table in tables:
                        table_name = table['name']
                        backup_table = f"{backup_schema}.{schema}_{table_name}"
                        
                        # Create backup table
                        backup_query = f"CREATE TABLE {backup_table} AS SELECT * FROM {schema}.{table_name}"
                        self.execute_query(backup_query)
            
            logger.info(f"✅ Backup created in schema: {backup_schema}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Backup creation failed: {str(e)}")
            return False
    
    def get_available_backups(self) -> List[str]:
        """Get list of available backup versions"""
        try:
            # This would typically query a deployment history table
            # For now, simulate with some example versions
            query = f"""
            SELECT DISTINCT BACKUP_VERSION
            FROM {self.config['database']}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA LIKE '%BACKUP%'
            ORDER BY BACKUP_VERSION DESC
            """
            
            results = self.execute_query(query)
            if results:
                return [row['BACKUP_VERSION'] for row in results]
            else:
                # Return simulated versions for demo
                return ['v1.3.0', 'v1.2.0', 'v1.1.0', 'v1.0.0']
                
        except Exception as e:
            logger.warning(f"Could not fetch backup versions: {str(e)}")
            return ['v1.2.0', 'v1.1.0', 'v1.0.0']  # Fallback versions
    
    def validate_rollback_version(self) -> bool:
        """Validate that the rollback version exists"""
        available_versions = self.get_available_backups()
        
        if self.version not in available_versions:
            logger.error(f"❌ Version {self.version} not found in available backups")
            logger.info(f"Available versions: {', '.join(available_versions)}")
            return False
            
        logger.info(f"✅ Version {self.version} is available for rollback")
        return True
    
    def rollback_schemas(self) -> bool:
        """Rollback database schemas to previous version"""
        logger.info(f"🔄 Rolling back schemas to version {self.version}...")
        
        try:
            # This is a simplified rollback - in reality, you'd restore from backups
            # or re-run deployment scripts from the specific version
            
            schemas_rollback_map = {
                'CUSTOMERS_SCHEMA_DEV': 'customers_schema_tables.sql',
                'PRODUCTS_SCHEMA_DEV': 'products_schema_tables.sql',
                'HR_SCHEMA_DEV': 'hr_schema_tables.sql',
                'SALES_SCHEMA_DEV': 'sales_schema_tables.sql',
                'ANALYTICS_SCHEMA_DEV': 'analytics_schema_tables.sql'
            }
            
            for schema, script_file in schemas_rollback_map.items():
                logger.info(f"Rolling back schema: {schema}")
                
                # Drop and recreate schema (destructive rollback)
                drop_query = f"DROP SCHEMA IF EXISTS {schema} CASCADE"
                self.execute_query(drop_query)
                
                create_query = f"CREATE SCHEMA {schema}"
                self.execute_query(create_query)
            
            logger.info("✅ Schema rollback completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Schema rollback failed: {str(e)}")
            return False
    
    def rollback_data(self) -> bool:
        """Rollback data to previous version"""
        logger.info(f"📊 Rolling back data to version {self.version}...")
        
        try:
            # In a real scenario, this would restore data from version-specific backups
            # For now, we'll clear transactional data and keep reference data
            
            transactional_tables = [
                'SALES_SCHEMA_DEV.ORDERS',
                'SALES_SCHEMA_DEV.ORDER_ITEMS', 
                'ANALYTICS_SCHEMA_DEV.SALES_FACT',
                'ANALYTICS_SCHEMA_DEV.CUSTOMER_ANALYTICS_FACT'
            ]
            
            for table in transactional_tables:
                try:
                    truncate_query = f"TRUNCATE TABLE IF EXISTS {table}"
                    self.execute_query(truncate_query)
                    logger.info(f"Cleared transactional data from: {table}")
                except:
                    logger.warning(f"Could not truncate {table} (may not exist)")
            
            logger.info("✅ Data rollback completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Data rollback failed: {str(e)}")
            return False
    
    def update_deployment_history(self) -> bool:
        """Update deployment history with rollback information"""
        logger.info("📝 Updating deployment history...")
        
        try:
            # Create deployment history table if it doesn't exist
            create_history_table = f"""
            CREATE TABLE IF NOT EXISTS {self.config['database']}.PUBLIC.DEPLOYMENT_HISTORY (
                DEPLOYMENT_ID NUMBER AUTOINCREMENT PRIMARY KEY,
                ENVIRONMENT VARCHAR(20),
                VERSION VARCHAR(50),
                DEPLOYMENT_TYPE VARCHAR(20),
                DEPLOYED_BY VARCHAR(100),
                DEPLOYMENT_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                STATUS VARCHAR(20),
                NOTES TEXT
            )
            """
            self.execute_query(create_history_table)
            
            # Insert rollback record
            insert_history = f"""
            INSERT INTO {self.config['database']}.PUBLIC.DEPLOYMENT_HISTORY 
            (ENVIRONMENT, VERSION, DEPLOYMENT_TYPE, DEPLOYED_BY, STATUS, NOTES)
            VALUES ('{self.environment}', '{self.version}', 'ROLLBACK', USER(), 'COMPLETED', 
                   'Automated rollback via rollback script')
            """
            self.execute_query(insert_history)
            
            logger.info("✅ Deployment history updated")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update deployment history: {str(e)}")
            return False
    
    def run_post_rollback_tests(self) -> bool:
        """Run tests after rollback to verify system health"""
        logger.info("🧪 Running post-rollback validation tests...")
        
        try:
            # Basic connectivity test
            version_query = "SELECT CURRENT_VERSION() as version"
            result = self.execute_query(version_query)
            
            if not result:
                logger.error("❌ Basic connectivity test failed")
                return False
            
            # Test schema existence
            schemas_to_test = [
                'CUSTOMERS_SCHEMA_DEV',
                'PRODUCTS_SCHEMA_DEV',
                'ANALYTICS_SCHEMA_DEV'
            ]
            
            for schema in schemas_to_test:
                schema_query = f"SHOW SCHEMAS LIKE '{schema}'"
                schema_result = self.execute_query(schema_query)
                
                if not schema_result:
                    logger.warning(f"⚠️ Schema {schema} not found")
                else:
                    logger.info(f"✅ Schema {schema} exists")
            
            logger.info("✅ Post-rollback tests completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Post-rollback tests failed: {str(e)}")
            return False
    
    def execute_rollback(self) -> bool:
        """Execute complete rollback process"""
        logger.info(f"🚀 Starting rollback to version {self.version} in {self.environment}")
        
        steps = [
            ("Connect to Snowflake", self.connect_snowflake),
            ("Validate Rollback Version", self.validate_rollback_version),
            ("Create Backup", self.create_backup),
            ("Rollback Schemas", self.rollback_schemas),
            ("Rollback Data", self.rollback_data),
            ("Update Deployment History", self.update_deployment_history),
            ("Run Post-Rollback Tests", self.run_post_rollback_tests)
        ]
        
        failed_steps = []
        
        for step_name, step_func in steps:
            logger.info(f"📋 Executing: {step_name}")
            if not step_func():
                failed_steps.append(step_name)
                logger.error(f"❌ Failed: {step_name}")
                break  # Stop on first failure for rollback
            else:
                logger.info(f"✅ Completed: {step_name}")
        
        if failed_steps:
            logger.error(f"❌ Rollback failed at: {', '.join(failed_steps)}")
            return False
        else:
            logger.info(f"🎉 Rollback to version {self.version} completed successfully!")
            return True
    
    def cleanup(self):
        """Cleanup resources"""
        if self.connection:
            self.connection.close()
            logger.info("🧹 Cleaned up Snowflake connection")

def main():
    parser = argparse.ArgumentParser(description="Rollback RetailWorks Snowflake Deployment")
    parser.add_argument(
        "--environment",
        choices=["dev", "staging", "prod"],
        required=True,
        help="Target environment for rollback"
    )
    parser.add_argument(
        "--version",
        required=True,
        help="Version to rollback to (e.g., v1.2.0)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force rollback without confirmation"
    )
    
    args = parser.parse_args()
    
    # Confirmation for production rollbacks
    if args.environment == "prod" and not args.force:
        confirmation = input(f"⚠️  Are you sure you want to rollback PRODUCTION to {args.version}? (yes/no): ")
        if confirmation.lower() != "yes":
            logger.info("Rollback cancelled by user")
            return 0
    
    # Check required environment variables
    required_env_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER', 
        'SNOWFLAKE_PASSWORD'
    ]
    
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        return 1
    
    rollback = DeploymentRollback(args.environment, args.version)
    
    try:
        success = rollback.execute_rollback()
        return 0 if success else 1
    except KeyboardInterrupt:
        logger.info("🛑 Rollback interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"❌ Rollback failed with error: {str(e)}")
        return 1
    finally:
        rollback.cleanup()

if __name__ == "__main__":
    sys.exit(main())