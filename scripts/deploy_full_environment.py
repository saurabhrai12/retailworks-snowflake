#!/usr/bin/env python3
"""
Full Environment Deployment Script
Description: Deploys complete RetailWorks environment including schemas, tables, and sample data
Usage: python deploy_full_environment.py --environment dev --load-sample-data
"""

import argparse
import os
import sys
import subprocess
import logging
from pathlib import Path
import snowflake.connector
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EnvironmentDeployer:
    """Handles full environment deployment"""
    
    def __init__(self, environment: str, load_sample_data: bool = False):
        self.environment = environment
        self.load_sample_data = load_sample_data
        self.project_root = Path(__file__).parent.parent
        self.connection = None
        
        # Environment-specific settings
        self.env_config = {
            'dev': {
                'database': 'RETAILWORKS_DB_DEV',
                'suffix': '_DEV'
            },
            'staging': {
                'database': 'RETAILWORKS_DB_STAGING', 
                'suffix': '_STAGING'
            },
            'prod': {
                'database': 'RETAILWORKS_DB',
                'suffix': ''
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
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
            )
            logger.info("✅ Connected to Snowflake successfully")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {str(e)}")
            return False
    
    def execute_sql_file(self, sql_file: Path, replace_vars: bool = True) -> bool:
        """Execute SQL file with variable replacement"""
        try:
            if not sql_file.exists():
                logger.warning(f"SQL file not found: {sql_file}")
                return False
                
            with open(sql_file, 'r') as f:
                sql_content = f.read()
            
            if replace_vars:
                # Replace template variables
                sql_content = sql_content.replace('<% database_name %>', self.config['database'])
                sql_content = sql_content.replace('<% schema_suffix %>', self.config['suffix'])
            
            cursor = self.connection.cursor()
            
            # Split and execute statements
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            for statement in statements:
                if statement:
                    cursor.execute(statement)
                    
            cursor.close()
            logger.info(f"✅ Executed SQL file: {sql_file.name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to execute {sql_file.name}: {str(e)}")
            return False
    
    def deploy_schemas(self) -> bool:
        """Deploy database schemas"""
        logger.info("🏗️  Deploying database schemas...")
        
        schema_file = self.project_root / "ddl" / "schemas" / "01_create_database.sql"
        return self.execute_sql_file(schema_file)
    
    def deploy_tables(self) -> bool:
        """Deploy all tables"""
        logger.info("📊 Deploying database tables...")
        
        tables_dir = self.project_root / "ddl" / "tables"
        table_files = [
            "analytics_schema_tables.sql",
            "customers_schema_tables.sql", 
            "hr_schema_tables.sql",
            "products_schema_tables.sql",
            "sales_schema_tables.sql",
            "staging_schema_tables.sql"
        ]
        
        success = True
        for table_file in table_files:
            file_path = tables_dir / table_file
            if not self.execute_sql_file(file_path):
                success = False
                
        return success
    
    def deploy_views(self) -> bool:
        """Deploy analytical views"""
        logger.info("👁️  Deploying analytical views...")
        
        views_dir = self.project_root / "ddl" / "views"
        view_files = list(views_dir.glob("*.sql"))
        
        success = True
        for view_file in view_files:
            if not self.execute_sql_file(view_file):
                success = False
                
        return success
    
    def deploy_procedures(self) -> bool:
        """Deploy stored procedures"""
        logger.info("⚙️  Deploying stored procedures...")
        
        procedures_dir = self.project_root / "ddl" / "procedures"
        procedure_files = list(procedures_dir.glob("*.sql"))
        
        success = True
        for procedure_file in procedure_files:
            if not self.execute_sql_file(procedure_file):
                success = False
                
        return success
    
    def load_dimensional_data(self) -> bool:
        """Load dimensional reference data"""
        logger.info("📋 Loading dimensional reference data...")
        
        dimensional_files = [
            "01_populate_dimensional_data.sql",
            "02_populate_master_data.sql"
        ]
        
        success = True
        for dim_file in dimensional_files:
            file_path = self.project_root / "dml" / "sample_data" / dim_file
            if file_path.exists():
                if not self.execute_sql_file(file_path):
                    success = False
            else:
                logger.warning(f"Dimensional data file not found: {dim_file}")
                
        return success
    
    def generate_sample_data(self) -> bool:
        """Generate sample data CSV files"""
        logger.info("🎲 Generating sample data...")
        
        try:
            result = subprocess.run([
                sys.executable,
                str(self.project_root / "dml" / "sample_data" / "generate_sample_data.py")
            ], capture_output=True, text=True, cwd=self.project_root)
            
            if result.returncode == 0:
                logger.info("✅ Sample data generated successfully")
                return True
            else:
                logger.error(f"❌ Sample data generation failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error generating sample data: {str(e)}")
            return False
    
    def load_sample_data(self) -> bool:
        """Load sample data to Snowflake"""
        logger.info("📤 Loading sample data to Snowflake...")
        
        try:
            # Set environment variables for the script
            env = os.environ.copy()
            env['SNOWFLAKE_DATABASE'] = self.config['database']
            
            result = subprocess.run([
                sys.executable,
                str(self.project_root / "dml" / "sample_data" / "load_large_data.py")
            ], capture_output=True, text=True, cwd=self.project_root, env=env)
            
            if result.returncode == 0:
                logger.info("✅ Sample data loaded successfully")
                return True
            else:
                logger.error(f"❌ Sample data loading failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error loading sample data: {str(e)}")
            return False
    
    def run_tests(self) -> bool:
        """Run deployment validation tests"""
        logger.info("🧪 Running deployment validation tests...")
        
        try:
            # Set environment variables for tests
            env = os.environ.copy()
            env['SNOWFLAKE_DATABASE'] = self.config['database']
            
            result = subprocess.run([
                sys.executable, "-m", "pytest",
                "snowpark/tests/test_data_quality.py",
                "-v", "--tb=short"
            ], capture_output=True, text=True, cwd=self.project_root, env=env)
            
            if result.returncode == 0:
                logger.info("✅ All tests passed")
                return True
            else:
                logger.warning(f"⚠️  Some tests failed: {result.stdout}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error running tests: {str(e)}")
            return False
    
    def deploy_full_environment(self) -> bool:
        """Deploy complete environment"""
        logger.info(f"🚀 Starting full environment deployment for: {self.environment}")
        logger.info(f"Database: {self.config['database']}")
        
        steps = [
            ("Connect to Snowflake", self.connect_snowflake),
            ("Deploy Schemas", self.deploy_schemas),
            ("Deploy Tables", self.deploy_tables),
            ("Deploy Views", self.deploy_views),
            ("Deploy Procedures", self.deploy_procedures),
            ("Load Dimensional Data", self.load_dimensional_data)
        ]
        
        if self.load_sample_data:
            steps.extend([
                ("Generate Sample Data", self.generate_sample_data),
                ("Load Sample Data", self.load_sample_data)
            ])
        
        steps.append(("Run Validation Tests", self.run_tests))
        
        failed_steps = []
        
        for step_name, step_func in steps:
            logger.info(f"📋 Executing: {step_name}")
            if not step_func():
                failed_steps.append(step_name)
                logger.error(f"❌ Failed: {step_name}")
            else:
                logger.info(f"✅ Completed: {step_name}")
        
        if failed_steps:
            logger.error(f"❌ Deployment completed with failures: {', '.join(failed_steps)}")
            return False
        else:
            logger.info(f"🎉 Environment deployment completed successfully!")
            logger.info(f"Database: {self.config['database']}")
            return True
    
    def cleanup(self):
        """Cleanup resources"""
        if self.connection:
            self.connection.close()
            logger.info("🧹 Cleaned up Snowflake connection")

def main():
    parser = argparse.ArgumentParser(description="Deploy RetailWorks Snowflake Environment")
    parser.add_argument(
        "--environment", 
        choices=["dev", "staging", "prod"],
        required=True,
        help="Target environment for deployment"
    )
    parser.add_argument(
        "--load-sample-data",
        action="store_true",
        help="Generate and load sample data"
    )
    parser.add_argument(
        "--skip-tests",
        action="store_true", 
        help="Skip validation tests"
    )
    
    args = parser.parse_args()
    
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
    
    deployer = EnvironmentDeployer(args.environment, args.load_sample_data)
    
    try:
        success = deployer.deploy_full_environment()
        return 0 if success else 1
    except KeyboardInterrupt:
        logger.info("🛑 Deployment interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"❌ Deployment failed with error: {str(e)}")
        return 1
    finally:
        deployer.cleanup()

if __name__ == "__main__":
    sys.exit(main())