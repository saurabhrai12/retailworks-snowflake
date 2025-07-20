#!/bin/bash

# RetailWorks Snowflake GitOps Deployment Script
# Uses Snowflake CLI native object management with CREATE OR ALTER approach
# Usage: ./deploy_gitops.sh [environment] [options]

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
SYNC_MODE=${3:-true}

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
    
    # Check CLI version for object management support
    local version=$(snow --version 2>/dev/null | head -n1 | cut -d' ' -f3)
    log_info "Snowflake CLI version: $version"
    
    # Check if config file exists
    if [[ ! -f "$CONFIG_FILE" ]]; then
        log_error "Configuration file not found: $CONFIG_FILE"
        exit 1
    fi
    
    # Check if connection exists for environment
    if ! snow connection list | grep -q "retailworks-${ENVIRONMENT}"; then
        log_warning "Connection 'retailworks-${ENVIRONMENT}' not found."
        log_warning "Please create it first: snow connection add retailworks-${ENVIRONMENT}"
    fi
    
    log_success "Prerequisites check completed"
}

deploy_native_objects() {
    log_info "Deploying native Snowflake objects using CLI..."
    
    cd "$PROJECT_DIR"
    
    # Deploy objects defined in snowflake.yml
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "Dry run: Validating object definitions..."
        snow object list --connection "retailworks-${ENVIRONMENT}" \
            --environment "$ENVIRONMENT" \
            --validate-only
    else
        log_info "Deploying/updating objects..."
        
        # Deploy all objects defined in snowflake.yml
        snow object deploy --connection "retailworks-${ENVIRONMENT}" \
            --environment "$ENVIRONMENT" \
            --replace-always=false
            
        log_success "Native objects deployed successfully"
    fi
}

sync_objects_state() {
    if [[ "$SYNC_MODE" == "true" ]]; then
        log_info "Syncing object state with Git repository..."
        
        # Generate state file for drift detection
        snow object list --connection "retailworks-${ENVIRONMENT}" \
            --environment "$ENVIRONMENT" \
            --output-format json > "${PROJECT_DIR}/.snowflake_state_${ENVIRONMENT}.json"
            
        # Check for drift between Git and Snowflake
        snow object diff --connection "retailworks-${ENVIRONMENT}" \
            --environment "$ENVIRONMENT" \
            --show-changes
            
        log_success "Object state synchronized"
    else
        log_info "Skipping object state synchronization"
    fi
}

deploy_sql_objects() {
    log_info "Deploying SQL-defined objects..."
    
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
        local full_path="$PROJECT_DIR/$file"
        
        if [[ -f "$full_path" ]]; then
            log_info "Deploying: $file"
            
            if [[ "$DRY_RUN" == "true" ]]; then
                snow sql --dry-run -f "$full_path" \
                    --variable database_name="$(get_env_variable database_name)" \
                    --variable schema_suffix="$(get_env_variable schema_suffix)" \
                    --connection "retailworks-${ENVIRONMENT}"
            else
                snow sql -f "$full_path" \
                    --variable database_name="$(get_env_variable database_name)" \
                    --variable schema_suffix="$(get_env_variable schema_suffix)" \
                    --connection "retailworks-${ENVIRONMENT}"
                    
                log_success "Deployed: $file"
            fi
        else
            log_warning "File not found: $full_path"
        fi
    done
    
    log_success "SQL objects deployment completed"
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

deploy_snowpark_apps() {
    log_info "Deploying Snowpark applications..."
    
    cd "$PROJECT_DIR"
    
    # Check if Snowpark source exists
    if [[ -d "snowpark/src" ]]; then
        if [[ "$DRY_RUN" == "true" ]]; then
            log_info "Dry run: Validating Snowpark applications..."
            snow snowpark deploy \
                --connection "retailworks-${ENVIRONMENT}" \
                --environment "$ENVIRONMENT" \
                --validate-only
        else
            snow snowpark deploy \
                --connection "retailworks-${ENVIRONMENT}" \
                --environment "$ENVIRONMENT" \
                --replace
                
            log_success "Snowpark applications deployed"
        fi
    else
        log_warning "Snowpark source directory not found"
    fi
}

deploy_streamlit_apps() {
    log_info "Deploying Streamlit applications..."
    
    cd "$PROJECT_DIR"
    
    # Check if Streamlit apps exist
    if [[ -d "streamlit/dashboards" ]]; then
        if [[ "$DRY_RUN" == "true" ]]; then
            log_info "Dry run: Validating Streamlit applications..."
            snow streamlit deploy \
                --connection "retailworks-${ENVIRONMENT}" \
                --environment "$ENVIRONMENT" \
                --validate-only
        else
            snow streamlit deploy \
                --connection "retailworks-${ENVIRONMENT}" \
                --environment "$ENVIRONMENT" \
                --replace
                
            log_success "Streamlit applications deployed"
        fi
    else
        log_warning "Streamlit applications directory not found"
    fi
}

run_deployment_tests() {
    log_info "Running post-deployment validation..."
    
    # Test database connectivity
    snow sql -q "SELECT CURRENT_VERSION(), CURRENT_DATABASE(), CURRENT_SCHEMA();" \
        --connection "retailworks-${ENVIRONMENT}" > /dev/null
    log_success "Database connectivity test passed"
    
    # Test object existence
    local test_objects=(
        "DATABASE:$(get_env_variable database_name)"
        "SCHEMA:SALES_SCHEMA$(get_env_variable schema_suffix)"
        "SCHEMA:PRODUCTS_SCHEMA$(get_env_variable schema_suffix)"
        "SCHEMA:CUSTOMERS_SCHEMA$(get_env_variable schema_suffix)"
        "SCHEMA:ANALYTICS_SCHEMA$(get_env_variable schema_suffix)"
        "WAREHOUSE:$(get_env_variable warehouse)"
    )
    
    for obj in "${test_objects[@]}"; do
        local obj_type=$(echo "$obj" | cut -d':' -f1)
        local obj_name=$(echo "$obj" | cut -d':' -f2)
        
        local result=$(snow sql -q "SHOW ${obj_type}S LIKE '${obj_name}';" \
            --connection "retailworks-${ENVIRONMENT}" --output json 2>/dev/null || echo "[]")
        
        if [[ $(echo "$result" | jq '. | length') -gt 0 ]]; then
            log_success "$obj_type exists: $obj_name"
        else
            log_error "$obj_type not found: $obj_name"
            exit 1
        fi
    done
    
    log_success "Post-deployment validation completed"
}

generate_deployment_report() {
    log_info "Generating deployment report..."
    
    local report_file="${PROJECT_DIR}/deployment_report_${ENVIRONMENT}_$(date +%Y%m%d_%H%M%S).json"
    
    # Generate comprehensive report
    cat > "$report_file" << EOF
{
    "deployment": {
        "timestamp": "$(date -u '+%Y-%m-%d %H:%M:%S UTC')",
        "environment": "$ENVIRONMENT",
        "dry_run": $DRY_RUN,
        "sync_mode": $SYNC_MODE,
        "database": "$(get_env_variable database_name)",
        "warehouse": "$(get_env_variable warehouse)"
    },
    "objects_deployed": {
        "native_objects": true,
        "sql_objects": true,
        "snowpark_apps": true,
        "streamlit_apps": true
    },
    "git_state": {
        "commit_hash": "$(git rev-parse HEAD 2>/dev/null || echo 'unknown')",
        "branch": "$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo 'unknown')"
    }
}
EOF
    
    log_success "Deployment report generated: $report_file"
}

show_usage() {
    cat << EOF
Usage: $0 [environment] [dry_run] [sync_mode]

Arguments:
  environment     Target environment (dev/test/prod) [default: dev]
  dry_run         Validate only, don't execute (true/false) [default: false]
  sync_mode       Enable Git sync and drift detection (true/false) [default: true]

Examples:
  $0 dev false true          # Deploy to dev with Git sync
  $0 test true false         # Validate test deployment (dry run)
  $0 prod false true         # Deploy to production with sync

Features:
  - Native Snowflake CLI object management
  - CREATE OR ALTER approach for safe deployments
  - Git integration and drift detection
  - Comprehensive validation and testing
  - Environment-specific configurations

Environment Variables:
  SNOWFLAKE_ACCOUNT    Your Snowflake account identifier
  SNOWFLAKE_USER       Your Snowflake username
  SNOWFLAKE_PASSWORD   Your Snowflake password

EOF
}

cleanup() {
    log_info "Performing cleanup..."
    
    # Clean up temporary files
    if [[ -f "/tmp/snowflake_deployment.log" ]]; then
        rm -f "/tmp/snowflake_deployment.log"
    fi
    
    log_success "Cleanup completed"
}

# Main execution
main() {
    log_info "Starting RetailWorks GitOps deployment..."
    log_info "Environment: $ENVIRONMENT"
    log_info "Dry Run: $DRY_RUN"
    log_info "Sync Mode: $SYNC_MODE"
    
    if [[ "$1" == "--help" || "$1" == "-h" ]]; then
        show_usage
        exit 0
    fi
    
    # Execute deployment steps
    check_prerequisites
    
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "=== DRY RUN MODE - NO CHANGES WILL BE MADE ==="
    fi
    
    # Deploy using native CLI object management
    deploy_native_objects
    
    # Sync state with Git if enabled
    sync_objects_state
    
    # Deploy SQL-defined objects
    deploy_sql_objects
    
    if [[ "$DRY_RUN" != "true" ]]; then
        # Deploy applications
        deploy_snowpark_apps
        deploy_streamlit_apps
        
        # Run validation tests
        run_deployment_tests
        
        # Generate deployment report
        generate_deployment_report
    else
        log_info "Dry run completed - no changes were made"
    fi
    
    cleanup
    
    log_success "RetailWorks GitOps deployment completed successfully!"
    log_info "Environment: $ENVIRONMENT"
    log_info "Database: $(get_env_variable database_name)"
    log_info "Timestamp: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
}

# Trap for cleanup on script exit
trap cleanup EXIT

# Run main function
main "$@"