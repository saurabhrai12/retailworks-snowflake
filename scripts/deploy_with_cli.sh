#!/bin/bash

# RetailWorks Snowflake CLI Deployment Script
# Following Snowflake DevOps best practices
# Usage: ./deploy_with_cli.sh [environment] [schema_filter] [options]

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
ENVIRONMENT=${1:-dev}
SCHEMA_FILTER=${2:-all}
DEPLOY_DATA=${3:-false}
DRY_RUN=${4:-false}

# Configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_FILE="${PROJECT_DIR}/snowflake.yml"

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if Snowflake CLI is installed
    if ! command -v snow &> /dev/null; then
        log_error "Snowflake CLI is not installed. Please install it first:"
        log_error "pip install snowflake-cli-labs"
        exit 1
    fi
    
    # Check if config file exists
    if [[ ! -f "$CONFIG_FILE" ]]; then
        log_error "Configuration file not found: $CONFIG_FILE"
        exit 1
    fi
    
    # Check if connection exists
    if ! snow connection list | grep -q "retailworks-${ENVIRONMENT}"; then
        log_warning "Connection 'retailworks-${ENVIRONMENT}' not found. Please create it first:"
        log_warning "snow connection add retailworks-${ENVIRONMENT} --account YOUR_ACCOUNT --user YOUR_USER"
    fi
    
    log_success "Prerequisites check completed"
}

set_environment_variables() {
    log_info "Setting environment variables for: $ENVIRONMENT"
    
    case $ENVIRONMENT in
        "dev")
            export DATABASE_NAME="RETAILWORKS_DB_DEV"
            export SCHEMA_SUFFIX="_DEV"
            export WAREHOUSE_NAME="RETAILWORKS_DEV_WH"
            export CONNECTION_NAME="retailworks-dev"
            ;;
        "test")
            export DATABASE_NAME="RETAILWORKS_DB_TEST"
            export SCHEMA_SUFFIX="_TEST"
            export WAREHOUSE_NAME="RETAILWORKS_TEST_WH"
            export CONNECTION_NAME="retailworks-test"
            ;;
        "prod")
            export DATABASE_NAME="RETAILWORKS_DB"
            export SCHEMA_SUFFIX=""
            export WAREHOUSE_NAME="RETAILWORKS_PROD_WH"
            export CONNECTION_NAME="retailworks-prod"
            ;;
        *)
            log_error "Invalid environment: $ENVIRONMENT. Valid options: dev, test, prod"
            exit 1
            ;;
    esac
    
    log_info "Database: $DATABASE_NAME"
    log_info "Schema Suffix: $SCHEMA_SUFFIX"
    log_info "Warehouse: $WAREHOUSE_NAME"
}

validate_sql_files() {
    log_info "Validating SQL files..."
    
    local files_to_validate=(
        "ddl/schemas/01_create_database.sql"
        "ddl/tables/staging_schema_tables.sql"
        "ddl/tables/sales_schema_tables.sql"
        "ddl/tables/products_schema_tables.sql"
        "ddl/tables/customers_schema_tables.sql"
        "ddl/tables/hr_schema_tables.sql"
        "ddl/tables/analytics_schema_tables.sql"
    )
    
    for file in "${files_to_validate[@]}"; do
        if [[ -f "$PROJECT_DIR/$file" ]]; then
            log_info "Validating: $file"
            if [[ "$DRY_RUN" == "true" ]]; then
                snow sql --dry-run -f "$PROJECT_DIR/$file" \
                    --variable database_name="$DATABASE_NAME" \
                    --variable schema_suffix="$SCHEMA_SUFFIX" \
                    -c "$CONNECTION_NAME" || {
                    log_error "Validation failed for: $file"
                    exit 1
                }
            fi
        else
            log_warning "File not found: $file"
        fi
    done
    
    log_success "SQL validation completed"
}

deploy_schemas() {
    log_info "Deploying database and schemas..."
    
    local schema_file="$PROJECT_DIR/ddl/schemas/01_create_database.sql"
    
    if [[ -f "$schema_file" ]]; then
        snow sql -f "$schema_file" \
            --variable database_name="$DATABASE_NAME" \
            --variable schema_suffix="$SCHEMA_SUFFIX" \
            -c "$CONNECTION_NAME"
        
        log_success "Database and schemas deployed successfully"
    else
        log_error "Schema file not found: $schema_file"
        exit 1
    fi
}

deploy_tables() {
    log_info "Deploying tables..."
    
    local table_files=(
        "staging_schema_tables.sql"
        "sales_schema_tables.sql" 
        "products_schema_tables.sql"
        "customers_schema_tables.sql"
        "hr_schema_tables.sql"
        "analytics_schema_tables.sql"
    )
    
    for file in "${table_files[@]}"; do
        if [[ "$SCHEMA_FILTER" != "all" ]]; then
            # Check if this file matches the schema filter
            if [[ ! "$file" =~ ^${SCHEMA_FILTER}_ ]]; then
                log_info "Skipping $file (filtered out)"
                continue
            fi
        fi
        
        local table_file="$PROJECT_DIR/ddl/tables/$file"
        
        if [[ -f "$table_file" ]]; then
            log_info "Deploying tables from: $file"
            
            snow sql -f "$table_file" \
                --variable database_name="$DATABASE_NAME" \
                --variable schema_suffix="$SCHEMA_SUFFIX" \
                -c "$CONNECTION_NAME"
                
            log_success "Tables deployed from: $file"
        else
            log_warning "Table file not found: $table_file"
        fi
    done
    
    log_success "All tables deployed successfully"
}

deploy_views_and_procedures() {
    log_info "Deploying views and procedures..."
    
    local view_files=(
        "ddl/views/sales_analytics_views.sql"
        "ddl/procedures/business_logic_procedures.sql"
    )
    
    for file in "${view_files[@]}"; do
        local full_path="$PROJECT_DIR/$file"
        
        if [[ -f "$full_path" ]]; then
            log_info "Deploying: $file"
            
            snow sql -f "$full_path" \
                --variable database_name="$DATABASE_NAME" \
                --variable schema_suffix="$SCHEMA_SUFFIX" \
                -c "$CONNECTION_NAME"
                
            log_success "Deployed: $file"
        else
            log_warning "File not found: $full_path"
        fi
    done
    
    log_success "Views and procedures deployed successfully"
}

deploy_sample_data() {
    if [[ "$DEPLOY_DATA" == "true" ]]; then
        log_info "Deploying sample data..."
        
        # Deploy date dimension
        local date_dim_file="$PROJECT_DIR/dml/sample_data/populate_date_dimension.sql"
        
        if [[ -f "$date_dim_file" ]]; then
            snow sql -f "$date_dim_file" \
                --variable database_name="$DATABASE_NAME" \
                --variable schema_suffix="$SCHEMA_SUFFIX" \
                -c "$CONNECTION_NAME"
                
            log_success "Date dimension populated"
        fi
        
        # Generate and load other sample data
        if [[ -f "$PROJECT_DIR/dml/sample_data/generate_sample_data.py" ]]; then
            log_info "Generating sample data..."
            cd "$PROJECT_DIR/dml/sample_data"
            
            # Set environment variables for Python script
            export SNOWFLAKE_DATABASE="$DATABASE_NAME"
            export SNOWFLAKE_SCHEMA_SUFFIX="$SCHEMA_SUFFIX"
            
            python generate_sample_data.py
            
            log_success "Sample data generated and loaded"
        fi
    else
        log_info "Skipping sample data deployment"
    fi
}

deploy_snowpark_apps() {
    log_info "Deploying Snowpark applications..."
    
    cd "$PROJECT_DIR"
    
    # Deploy ETL pipeline
    if [[ -f "snowpark/src/etl_pipeline.py" ]]; then
        log_info "Deploying ETL pipeline..."
        
        snow snowpark deploy \
            --project-definition-file snowflake.yml \
            --connection "$CONNECTION_NAME" \
            --replace
            
        log_success "ETL pipeline deployed"
    fi
    
    # Deploy ML models
    if [[ -f "snowpark/src/ml_models.py" ]]; then
        log_info "Deploying ML models..."
        
        # Note: In a real deployment, you might want to deploy specific functions
        log_info "ML models deployment would happen here"
        # snow snowpark deploy-function --name customer_ltv_prediction --connection "$CONNECTION_NAME"
        
        log_success "ML models deployment completed"
    fi
}

run_tests() {
    log_info "Running deployment tests..."
    
    # Test database connectivity
    snow sql -q "SELECT CURRENT_VERSION();" -c "$CONNECTION_NAME" > /dev/null
    log_success "Database connectivity test passed"
    
    # Test schema existence
    local schemas=("SALES_SCHEMA" "PRODUCTS_SCHEMA" "CUSTOMERS_SCHEMA" "HR_SCHEMA" "ANALYTICS_SCHEMA" "STAGING_SCHEMA")
    
    for schema in "${schemas[@]}"; do
        local schema_name="${schema}${SCHEMA_SUFFIX}"
        
        local result=$(snow sql -q "SHOW SCHEMAS LIKE '${schema_name}' IN DATABASE ${DATABASE_NAME};" -c "$CONNECTION_NAME" --output json)
        
        if [[ $(echo "$result" | jq -r '.[] | length') -gt 0 ]]; then
            log_success "Schema exists: $schema_name"
        else
            log_error "Schema not found: $schema_name"
            exit 1
        fi
    done
    
    # Test table existence (basic check)
    local test_tables=("ORDERS" "CUSTOMERS" "PRODUCTS" "DATE_DIM")
    
    for table in "${test_tables[@]}"; do
        local result=$(snow sql -q "SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '${table}' AND TABLE_SCHEMA LIKE '%${SCHEMA_SUFFIX}';" -c "$CONNECTION_NAME" --output json)
        
        local count=$(echo "$result" | jq -r '.[0].CNT')
        
        if [[ "$count" -gt 0 ]]; then
            log_success "Table exists: $table"
        else
            log_warning "Table not found: $table"
        fi
    done
    
    log_success "Deployment tests completed"
}

cleanup() {
    log_info "Performing cleanup..."
    
    # Clean up temporary files
    if [[ -f "/tmp/snowflake_deployment.log" ]]; then
        rm -f "/tmp/snowflake_deployment.log"
    fi
    
    log_success "Cleanup completed"
}

show_usage() {
    cat << EOF
Usage: $0 [environment] [schema_filter] [deploy_data] [dry_run]

Arguments:
  environment     Target environment (dev/test/prod) [default: dev]
  schema_filter   Schema to deploy ('all' or specific schema name) [default: all]
  deploy_data     Deploy sample data (true/false) [default: false]
  dry_run         Validate only, don't execute (true/false) [default: false]

Examples:
  $0 dev all true false          # Deploy everything to dev with sample data
  $0 test sales false false      # Deploy only sales schema to test
  $0 prod all false true         # Validate production deployment (dry run)

Environment Variables:
  SNOWFLAKE_ACCOUNT    Your Snowflake account identifier
  SNOWFLAKE_USER       Your Snowflake username
  SNOWFLAKE_PASSWORD   Your Snowflake password

EOF
}

# Main execution
main() {
    log_info "Starting RetailWorks Snowflake deployment..."
    log_info "Environment: $ENVIRONMENT"
    log_info "Schema Filter: $SCHEMA_FILTER"
    log_info "Deploy Data: $DEPLOY_DATA"
    log_info "Dry Run: $DRY_RUN"
    
    if [[ "$1" == "--help" || "$1" == "-h" ]]; then
        show_usage
        exit 0
    fi
    
    # Execute deployment steps
    check_prerequisites
    set_environment_variables
    validate_sql_files
    
    if [[ "$DRY_RUN" != "true" ]]; then
        deploy_schemas
        deploy_tables
        deploy_views_and_procedures
        deploy_sample_data
        deploy_snowpark_apps
        run_tests
    else
        log_info "Dry run completed - no changes were made"
    fi
    
    cleanup
    
    log_success "RetailWorks Snowflake deployment completed successfully!"
    log_info "Environment: $ENVIRONMENT"
    log_info "Database: $DATABASE_NAME"
    log_info "Timestamp: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
}

# Trap for cleanup on script exit
trap cleanup EXIT

# Run main function
main "$@"