"""
Snowflake Connection Utility
Description: Utility functions for Snowflake database connections
Version: 1.0
Date: 2025-07-19
"""

import snowflake.connector
import streamlit as st
import os
from typing import Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_snowflake_connection():
    """
    Create and return a Snowflake connection using Streamlit secrets or environment variables
    """
    try:
        # Try to get connection parameters from Streamlit secrets first
        if hasattr(st, 'secrets') and 'snowflake' in st.secrets:
            connection_params = {
                'account': st.secrets.snowflake.account,
                'user': st.secrets.snowflake.user,
                'password': st.secrets.snowflake.password,
                'role': st.secrets.snowflake.get('role', 'PUBLIC'),
                'warehouse': st.secrets.snowflake.get('warehouse', 'COMPUTE_WH'),
                'database': st.secrets.snowflake.get('database', 'RETAILWORKS_DB'),
                'schema': st.secrets.snowflake.get('schema', 'ANALYTICS_SCHEMA')
            }
        else:
            # Fall back to environment variables
            connection_params = {
                'account': os.getenv('SNOWFLAKE_ACCOUNT', 'your_account'),
                'user': os.getenv('SNOWFLAKE_USER', 'your_user'),
                'password': os.getenv('SNOWFLAKE_PASSWORD', 'your_password'),
                'role': os.getenv('SNOWFLAKE_ROLE', 'PUBLIC'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
                'database': os.getenv('SNOWFLAKE_DATABASE', 'RETAILWORKS_DB'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA', 'ANALYTICS_SCHEMA')
            }
        
        # Create connection
        conn = snowflake.connector.connect(**connection_params)
        
        logger.info("Successfully connected to Snowflake")
        return conn
        
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {str(e)}")
        raise

def get_snowpark_session():
    """
    Create and return a Snowpark session
    """
    try:
        from snowflake.snowpark import Session
        
        # Try to get connection parameters from Streamlit secrets first
        if hasattr(st, 'secrets') and 'snowflake' in st.secrets:
            connection_params = {
                'account': st.secrets.snowflake.account,
                'user': st.secrets.snowflake.user,
                'password': st.secrets.snowflake.password,
                'role': st.secrets.snowflake.get('role', 'PUBLIC'),
                'warehouse': st.secrets.snowflake.get('warehouse', 'COMPUTE_WH'),
                'database': st.secrets.snowflake.get('database', 'RETAILWORKS_DB'),
                'schema': st.secrets.snowflake.get('schema', 'ANALYTICS_SCHEMA')
            }
        else:
            # Fall back to environment variables
            connection_params = {
                'account': os.getenv('SNOWFLAKE_ACCOUNT', 'your_account'),
                'user': os.getenv('SNOWFLAKE_USER', 'your_user'),
                'password': os.getenv('SNOWFLAKE_PASSWORD', 'your_password'),
                'role': os.getenv('SNOWFLAKE_ROLE', 'PUBLIC'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
                'database': os.getenv('SNOWFLAKE_DATABASE', 'RETAILWORKS_DB'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA', 'ANALYTICS_SCHEMA')
            }
        
        # Create Snowpark session
        session = Session.builder.configs(connection_params).create()
        
        logger.info("Successfully created Snowpark session")
        return session
        
    except Exception as e:
        logger.error(f"Failed to create Snowpark session: {str(e)}")
        raise

@st.cache_resource
def init_connection():
    """
    Initialize cached Snowflake connection for Streamlit
    """
    return get_snowflake_connection()

@st.cache_data(ttl=600)  # Cache for 10 minutes
def run_query(query: str, _conn=None) -> Optional[list]:
    """
    Run a query against Snowflake and return results
    
    Args:
        query (str): SQL query to execute
        _conn: Snowflake connection object (cached)
    
    Returns:
        List of query results or None if error
    """
    try:
        if _conn is None:
            _conn = init_connection()
        
        cursor = _conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        
        return results
        
    except Exception as e:
        logger.error(f"Query execution failed: {str(e)}")
        return None

def test_connection():
    """
    Test the Snowflake connection
    """
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        print(f"Connection successful! Snowflake version: {result[0]}")
        return True
        
    except Exception as e:
        print(f"Connection failed: {str(e)}")
        return False

def create_secrets_template():
    """
    Create a template for Streamlit secrets.toml file
    """
    template = """
# .streamlit/secrets.toml
[snowflake]
account = "your_account_identifier"
user = "your_username"
password = "your_password"
role = "your_role"
warehouse = "your_warehouse"
database = "RETAILWORKS_DB"
schema = "ANALYTICS_SCHEMA"
"""
    
    print("Streamlit secrets.toml template:")
    print(template)
    
    return template

if __name__ == "__main__":
    # Test connection when run directly
    print("Testing Snowflake connection...")
    if test_connection():
        print("✅ Connection test passed!")
    else:
        print("❌ Connection test failed!")
        print("\nYou can create a secrets.toml file with:")
        create_secrets_template()