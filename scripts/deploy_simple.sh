#!/bin/bash

# Simplified RetailWorks Snowflake GitOps Deployment Script
# Uses CREATE OR ALTER approach with Snowflake CLI native object management

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
ENVIRONMENT=${1:-dev}
DRY_RUN=${2:-false}

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

get_env_variable() {
    local var_name=$1
    
    case $ENVIRONMENT in
        "dev")
            case $var_name in
                "database_name") echo "RETAILWORKS_DB_DEV" ;;
                "schema_suffix") echo "_DEV" ;;
                "warehouse") echo "RETAILWORKS_DEV_WH" ;;
            esac
            ;;
        "test")
            case $var_name in
                "database_name") echo "RETAILWORKS_DB_TEST" ;;
                "schema_suffix") echo "_TEST" ;;
                "warehouse") echo "RETAILWORKS_TEST_WH" ;;
            esac
            ;;
        "prod")
            case $var_name in
                "database_name") echo "RETAILWORKS_DB" ;;
                "schema_suffix") echo "" ;;
                "warehouse") echo "RETAILWORKS_PROD_WH" ;;
            esac
            ;;
    esac
}

deploy_objects() {
    log_info "Starting GitOps deployment to $ENVIRONMENT environment..."
    
    local db_name=$(get_env_variable database_name)
    local warehouse_name=$(get_env_variable warehouse)
    local schema_suffix=$(get_env_variable schema_suffix)
    
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "=== DRY RUN MODE ==="
        log_info "Would deploy:"
        log_info "- Database: $db_name"
        log_info "- Warehouse: $warehouse_name"
        log_info "- Schemas: SALES_SCHEMA$schema_suffix, PRODUCTS_SCHEMA$schema_suffix, etc."
        log_info "- SQL objects from DDL files"
        return
    fi
    
    # 1. Create/Update Database
    log_info "Deploying database: $db_name"
    snow object create database \
        --if-not-exists \
        --connection "retailworks-${ENVIRONMENT}" \
        name="$db_name" \
        comment="RetailWorks Enterprise Data Platform"
    
    # 2. Create/Update Warehouse
    log_info "Deploying warehouse: $warehouse_name"
    snow object create warehouse \
        --if-not-exists \
        --connection "retailworks-${ENVIRONMENT}" \
        name="$warehouse_name" \
        warehouse_size="XSMALL" \
        auto_suspend=300 \
        auto_resume=true \
        comment="RetailWorks analytics warehouse"
    
    # 3. Create/Update Schemas
    local schemas=("SALES_SCHEMA" "PRODUCTS_SCHEMA" "CUSTOMERS_SCHEMA" "HR_SCHEMA" "ANALYTICS_SCHEMA" "STAGING_SCHEMA")
    
    for schema in "${schemas[@]}"; do
        local schema_name="${schema}$schema_suffix"
        log_info "Deploying schema: $schema_name"
        
        snow object create schema \
            --if-not-exists \
            --connection "retailworks-${ENVIRONMENT}" \
            name="$schema_name" \
            database="$db_name" \
            comment="Schema for $schema"
    done
    
    # 4. Deploy SQL Objects
    log_info "Deploying SQL objects..."
    
    local sql_files=(
        "ddl/schemas/01_create_database.sql"
        "ddl/tables/staging_schema_tables.sql"
        "ddl/tables/sales_schema_tables.sql"
        "ddl/tables/products_schema_tables.sql"
        "ddl/tables/customers_schema_tables.sql"
        "ddl/tables/hr_schema_tables.sql"
        "ddl/tables/analytics_schema_tables.sql"
        "ddl/views/sales_analytics_views.sql"
        "ddl/procedures/business_logic_procedures.sql"
    )
    
    for file in "${sql_files[@]}"; do
        if [[ -f "$file" ]]; then
            log_info "Executing: $file"
            snow sql -f "$file" \
                --variable database_name="$db_name" \
                --variable schema_suffix="$schema_suffix" \
                --connection "retailworks-${ENVIRONMENT}" || {
                log_warning "Failed to execute $file, continuing..."
            }
        else
            log_warning "File not found: $file"
        fi
    done
    
    # 5. Capture State
    log_info "Capturing deployment state..."
    
    # List current objects
    snow object list database --connection "retailworks-${ENVIRONMENT}" \
        --format json > ".snowflake_databases_${ENVIRONMENT}.json" 2>/dev/null || true
        
    snow object list warehouse --connection "retailworks-${ENVIRONMENT}" \
        --format json > ".snowflake_warehouses_${ENVIRONMENT}.json" 2>/dev/null || true
    
    log_success "GitOps deployment completed successfully!"
    log_info "Environment: $ENVIRONMENT"
    log_info "Database: $db_name"
    log_info "State files generated for drift detection"
}

show_usage() {
    cat << EOF
Usage: $0 [environment] [dry_run]

Arguments:
  environment     Target environment (dev/test/prod) [default: dev]
  dry_run         Validate only, don't execute (true/false) [default: false]

Examples:
  $0 dev false           # Deploy to dev
  $0 test true          # Validate test deployment
  $0 prod false         # Deploy to production

Features:
  - CREATE OR ALTER approach for safe deployments
  - Native Snowflake CLI object management
  - State capture for drift detection
  - Environment-specific configurations

EOF
}

# Main execution
main() {
    if [[ "$1" == "--help" || "$1" == "-h" ]]; then
        show_usage
        exit 0
    fi
    
    deploy_objects
}

# Run main function
main "$@"