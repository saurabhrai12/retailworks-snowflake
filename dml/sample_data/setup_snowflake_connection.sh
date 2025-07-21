#!/bin/bash

# =====================================================
# Snowflake Connection Setup Script
# Description: Helps set up Snowflake connection credentials
# Version: 1.0
# Date: 2025-07-20
# =====================================================

echo "🏔️  Snowflake Connection Setup"
echo "=============================="
echo ""

# Check if environment variables are already set
if [[ -n "$SNOWFLAKE_ACCOUNT" && -n "$SNOWFLAKE_USER" && -n "$SNOWFLAKE_PASSWORD" ]]; then
    echo "✅ Snowflake environment variables already set:"
    echo "   Account: $SNOWFLAKE_ACCOUNT"
    echo "   User: $SNOWFLAKE_USER" 
    echo "   Database: ${SNOWFLAKE_DATABASE:-RETAILWORKS_DB}"
    echo ""
    
    read -p "Do you want to update these credentials? (y/N): " update_creds
    if [[ ! "$update_creds" =~ ^[Yy]$ ]]; then
        echo "Using existing credentials."
        exit 0
    fi
fi

echo "Please provide your Snowflake connection details:"
echo ""

# Get Snowflake account
read -p "Snowflake Account (e.g., abc123.us-east-1): " account
if [[ -z "$account" ]]; then
    echo "❌ Account is required"
    exit 1
fi

# Get username
read -p "Username: " username
if [[ -z "$username" ]]; then
    echo "❌ Username is required"
    exit 1
fi

# Get password (hidden)
echo -n "Password: "
read -s password
echo ""
if [[ -z "$password" ]]; then
    echo "❌ Password is required"
    exit 1
fi

# Get optional parameters with defaults
read -p "Role (default: PUBLIC): " role
role=${role:-PUBLIC}

read -p "Warehouse (default: COMPUTE_WH): " warehouse  
warehouse=${warehouse:-COMPUTE_WH}

read -p "Database (default: RETAILWORKS_DB): " database
database=${database:-RETAILWORKS_DB}

read -p "Schema (default: STAGING_SCHEMA): " schema
schema=${schema:-STAGING_SCHEMA}

echo ""
echo "Setting up environment variables..."

# Create or update .env file
cat > .env << EOF
# Snowflake Connection Configuration
SNOWFLAKE_ACCOUNT=$account
SNOWFLAKE_USER=$username
SNOWFLAKE_PASSWORD=$password
SNOWFLAKE_ROLE=$role
SNOWFLAKE_WAREHOUSE=$warehouse
SNOWFLAKE_DATABASE=$database
SNOWFLAKE_SCHEMA=$schema
EOF

echo "✅ Created .env file with Snowflake credentials"
echo ""

# Export for current session
export SNOWFLAKE_ACCOUNT="$account"
export SNOWFLAKE_USER="$username"
export SNOWFLAKE_PASSWORD="$password"
export SNOWFLAKE_ROLE="$role"
export SNOWFLAKE_WAREHOUSE="$warehouse" 
export SNOWFLAKE_DATABASE="$database"
export SNOWFLAKE_SCHEMA="$schema"

echo "✅ Environment variables set for current session"
echo ""

# Test connection
echo "🔍 Testing Snowflake connection..."
if python -c "
import os
import sys
sys.path.append('.')
from streamlit.utils.snowflake_connection import test_connection
if test_connection():
    print('✅ Connection test successful!')
    sys.exit(0)
else:
    print('❌ Connection test failed!')
    sys.exit(1)
" 2>/dev/null; then
    echo ""
    echo "🎉 Setup complete! You can now run the data loading scripts."
    echo ""
    echo "To load the CSV data, run:"
    echo "  uv run python ./dml/sample_data/load_csv_data.py"
    echo ""
    echo "Or use the SQL script in Snowflake:"
    echo "  ./dml/sample_data/05_load_sample_data_to_tables.sql"
else
    echo ""
    echo "⚠️  Connection test failed. Please check your credentials and try again."
    echo ""
    echo "Common issues:"
    echo "  - Account format should be: account.region (e.g., abc123.us-east-1)"
    echo "  - Ensure the user has access to the specified role and warehouse"
    echo "  - Check if the database and schema exist"
fi

echo ""
echo "📝 Note: To persist environment variables across sessions, add them to your shell profile:"
echo "   source .env  # or add to ~/.bashrc, ~/.zshrc, etc."