# RetailWorks Deployment Guide

This guide provides comprehensive instructions for deploying the RetailWorks Snowflake Enterprise Data Platform across different environments.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [Database Deployment](#database-deployment)
- [Application Deployment](#application-deployment)
- [CI/CD Pipeline Setup](#cicd-pipeline-setup)
- [Environment Management](#environment-management)
- [Troubleshooting](#troubleshooting)

## Prerequisites

### Snowflake Requirements
- Snowflake account with SYSADMIN or ACCOUNTADMIN privileges
- Appropriate compute warehouses for each environment
- Network connectivity and firewall configurations
- SSL/TLS certificates for secure connections

### Development Environment
- Python 3.8 or higher
- Git version control
- Jenkins 2.300+ (for CI/CD)
- Docker (optional, for containerized deployments)

### Required Permissions
```sql
-- Minimum required privileges
GRANT CREATE DATABASE ON ACCOUNT TO ROLE <your_role>;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE <your_role>;
GRANT CREATE ROLE ON ACCOUNT TO ROLE <your_role>;
GRANT CREATE USER ON ACCOUNT TO ROLE <your_role>;
```

## Environment Setup

### 1. Environment Configuration

Create environment-specific configuration files:

**Development Environment**
```bash
# .env.dev
SNOWFLAKE_ACCOUNT=your-dev-account
SNOWFLAKE_DATABASE=RETAILWORKS_DB_DEV
SNOWFLAKE_WAREHOUSE=RETAILWORKS_DEV_WH
SCHEMA_SUFFIX=_DEV
ENVIRONMENT=dev
```

**Test Environment**
```bash
# .env.test
SNOWFLAKE_ACCOUNT=your-test-account
SNOWFLAKE_DATABASE=RETAILWORKS_DB_TEST
SNOWFLAKE_WAREHOUSE=RETAILWORKS_TEST_WH
SCHEMA_SUFFIX=_TEST
ENVIRONMENT=test
```

**Production Environment**
```bash
# .env.prod
SNOWFLAKE_ACCOUNT=your-prod-account
SNOWFLAKE_DATABASE=RETAILWORKS_DB
SNOWFLAKE_WAREHOUSE=RETAILWORKS_PROD_WH
SCHEMA_SUFFIX=
ENVIRONMENT=prod
```

### 2. Credential Management

**Using Environment Variables**
```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_ROLE="your_role"
```

**Using Snowflake Config File**
```ini
# ~/.snowflake/config
[connections.dev]
account = your-dev-account
user = dev_user
password = dev_password
role = RETAILWORKS_DEV_ADMIN
warehouse = RETAILWORKS_DEV_WH

[connections.prod]
account = your-prod-account
user = prod_user
password = prod_password
role = RETAILWORKS_PROD_ADMIN
warehouse = RETAILWORKS_PROD_WH
```

## Database Deployment

### 1. Manual Deployment

**Step 1: Deploy Schemas**
```bash
python jenkins/deployment/deploy_schemas.py \
    --environment dev \
    --schema-suffix _DEV \
    --account your-dev-account \
    --user your_user \
    --password your_password \
    --role SYSADMIN \
    --warehouse COMPUTE_WH \
    --database RETAILWORKS_DB_DEV \
    --create-roles \
    --create-warehouses
```

**Step 2: Deploy Tables**
```bash
python jenkins/deployment/deploy_tables.py \
    --schema all \
    --environment dev \
    --schema-suffix _DEV \
    --account your-dev-account \
    --user your_user \
    --password your_password \
    --role RETAILWORKS_DEV_ADMIN \
    --warehouse RETAILWORKS_DEV_WH \
    --database RETAILWORKS_DB_DEV \
    --validate
```

**Step 3: Deploy Views and Procedures**
```bash
python jenkins/deployment/deploy_views_procedures.py \
    --environment dev \
    --schema-suffix _DEV \
    --account your-dev-account \
    --user your_user \
    --password your_password \
    --role RETAILWORKS_DEV_ADMIN \
    --warehouse RETAILWORKS_DEV_WH \
    --database RETAILWORKS_DB_DEV
```

**Step 4: Load Sample Data (Optional)**
```bash
python jenkins/deployment/deploy_sample_data.py \
    --environment dev \
    --schema-suffix _DEV \
    --account your-dev-account \
    --user your_user \
    --password your_password \
    --role RETAILWORKS_DEV_ADMIN \
    --warehouse RETAILWORKS_DEV_WH \
    --database RETAILWORKS_DB_DEV
```

### 2. Automated Deployment Script

Create a deployment wrapper script:

```bash
#!/bin/bash
# deploy.sh

set -e

ENVIRONMENT=${1:-dev}
SCHEMA=${2:-all}
LOAD_DATA=${3:-false}

echo "Deploying to $ENVIRONMENT environment..."

# Source environment configuration
source .env.$ENVIRONMENT

# Deploy schemas
python jenkins/deployment/deploy_schemas.py \
    --environment $ENVIRONMENT \
    --schema-suffix $SCHEMA_SUFFIX \
    --account $SNOWFLAKE_ACCOUNT \
    --user $SNOWFLAKE_USER \
    --password $SNOWFLAKE_PASSWORD \
    --role $SNOWFLAKE_ROLE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --database $SNOWFLAKE_DATABASE \
    --create-roles \
    --create-warehouses

# Deploy tables
python jenkins/deployment/deploy_tables.py \
    --schema $SCHEMA \
    --environment $ENVIRONMENT \
    --schema-suffix $SCHEMA_SUFFIX \
    --account $SNOWFLAKE_ACCOUNT \
    --user $SNOWFLAKE_USER \
    --password $SNOWFLAKE_PASSWORD \
    --role $SNOWFLAKE_ROLE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --database $SNOWFLAKE_DATABASE \
    --validate

# Deploy views and procedures
python jenkins/deployment/deploy_views_procedures.py \
    --environment $ENVIRONMENT \
    --schema-suffix $SCHEMA_SUFFIX \
    --account $SNOWFLAKE_ACCOUNT \
    --user $SNOWFLAKE_USER \
    --password $SNOWFLAKE_PASSWORD \
    --role $SNOWFLAKE_ROLE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --database $SNOWFLAKE_DATABASE

# Load sample data if requested
if [ "$LOAD_DATA" = "true" ]; then
    python jenkins/deployment/deploy_sample_data.py \
        --environment $ENVIRONMENT \
        --schema-suffix $SCHEMA_SUFFIX \
        --account $SNOWFLAKE_ACCOUNT \
        --user $SNOWFLAKE_USER \
        --password $SNOWFLAKE_PASSWORD \
        --role $SNOWFLAKE_ROLE \
        --warehouse $SNOWFLAKE_WAREHOUSE \
        --database $SNOWFLAKE_DATABASE
fi

echo "Deployment to $ENVIRONMENT completed successfully!"
```

**Usage:**
```bash
# Deploy all schemas to dev with sample data
./deploy.sh dev all true

# Deploy only sales schema to test
./deploy.sh test sales false

# Deploy to production (no sample data)
./deploy.sh prod all false
```

## Application Deployment

### 1. Snowpark Applications

**Deploy ETL Pipeline**
```bash
cd snowpark/src
python etl_pipeline.py --environment dev --config ../config/dev.json
```

**Deploy ML Models**
```bash
cd snowpark/src
python ml_models.py --train --environment dev
```

### 2. Streamlit Dashboards

**Local Development**
```bash
# Set environment variables
export SNOWFLAKE_ACCOUNT=your-dev-account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password

# Run executive dashboard
streamlit run streamlit/dashboards/executive_dashboard.py --server.port 8501

# Run sales dashboard
streamlit run streamlit/dashboards/sales_dashboard.py --server.port 8502
```

**Production Deployment**
```bash
# Using Docker
docker build -t retailworks-dashboard .
docker run -p 8501:8501 \
    -e SNOWFLAKE_ACCOUNT=your-prod-account \
    -e SNOWFLAKE_USER=your_user \
    -e SNOWFLAKE_PASSWORD=your_password \
    retailworks-dashboard
```

**Streamlit Cloud Deployment**
1. Push code to GitHub repository
2. Connect Streamlit Cloud to your repository
3. Configure secrets in Streamlit Cloud:
   ```toml
   [snowflake]
   account = "your-account"
   user = "your-user"
   password = "your-password"
   role = "your-role"
   warehouse = "your-warehouse"
   database = "RETAILWORKS_DB"
   ```

## CI/CD Pipeline Setup

### 1. Jenkins Configuration

**Install Required Plugins**
- Pipeline plugin
- Git plugin
- Credentials plugin
- HTML Publisher plugin
- Email Extension plugin

**Create Jenkins Credentials**
```bash
# Snowflake credentials
SNOWFLAKE_ACCOUNT (Secret text)
SNOWFLAKE_USER (Secret text)
SNOWFLAKE_PASSWORD (Secret text)
SNOWFLAKE_ROLE (Secret text)
SNOWFLAKE_WAREHOUSE (Secret text)

# Notification emails
NOTIFICATION_EMAIL_DEV (Secret text)
NOTIFICATION_EMAIL_PROD (Secret text)
```

**Create Pipeline Job**
1. New Item → Pipeline
2. Configure Git repository URL
3. Set branch specifier (*/main, */develop, etc.)
4. Pipeline script from SCM
5. Script path: `jenkins/Jenkinsfile`

### 2. Pipeline Parameters

The Jenkins pipeline supports the following parameters:

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| DEPLOYMENT_ENVIRONMENT | Choice | Target environment (dev/test/prod) | dev |
| SCHEMA_TO_DEPLOY | Choice | Schema to deploy or 'all' | all |
| RUN_TESTS | Boolean | Execute automated tests | true |
| DEPLOY_SAMPLE_DATA | Boolean | Load sample data | false |
| FORCE_DEPLOYMENT | Boolean | Deploy even if tests fail | false |

### 3. Branch Strategy

**GitFlow Strategy**
- `main` branch: Production deployments
- `develop` branch: Development integration
- `feature/*` branches: Feature development
- `release/*` branches: Release preparation
- `hotfix/*` branches: Production hotfixes

**Pipeline Triggers**
```groovy
pipeline {
    triggers {
        // Poll SCM every 5 minutes for changes
        pollSCM('H/5 * * * *')
        
        // Trigger on webhook (recommended)
        githubPush()
    }
}
```

## Environment Management

### 1. Environment Promotion

**Dev → Test Promotion**
```bash
# Run full deployment with tests
jenkins-cli build RetailWorks-Pipeline \
    -p DEPLOYMENT_ENVIRONMENT=test \
    -p SCHEMA_TO_DEPLOY=all \
    -p RUN_TESTS=true \
    -p DEPLOY_SAMPLE_DATA=false
```

**Test → Production Promotion**
```bash
# Production deployment (requires approval)
jenkins-cli build RetailWorks-Pipeline \
    -p DEPLOYMENT_ENVIRONMENT=prod \
    -p SCHEMA_TO_DEPLOY=all \
    -p RUN_TESTS=true \
    -p DEPLOY_SAMPLE_DATA=false
```

### 2. Blue-Green Deployment

**Setup Blue-Green Schemas**
```sql
-- Create blue and green environments
CREATE SCHEMA ANALYTICS_SCHEMA_BLUE;
CREATE SCHEMA ANALYTICS_SCHEMA_GREEN;

-- Create views that point to current active schema
CREATE VIEW ANALYTICS_SCHEMA.CURRENT_SALES_FACT AS 
SELECT * FROM ANALYTICS_SCHEMA_BLUE.SALES_FACT;
```

**Switch Environments**
```sql
-- Switch from blue to green
ALTER VIEW ANALYTICS_SCHEMA.CURRENT_SALES_FACT AS 
SELECT * FROM ANALYTICS_SCHEMA_GREEN.SALES_FACT;
```

### 3. Rollback Procedures

**Database Rollback**
```bash
# Rollback to previous schema version
python jenkins/deployment/rollback_deployment.py \
    --environment prod \
    --target-version v1.2.3 \
    --confirm-rollback
```

**Application Rollback**
```bash
# Rollback using Git tags
git checkout v1.2.3
./deploy.sh prod all false
```

## Monitoring and Validation

### 1. Deployment Validation

**Post-Deployment Checks**
```bash
# Run deployment validation
python jenkins/deployment/verify_deployment.py \
    --environment prod \
    --schema-suffix "" \
    --run-data-quality-checks \
    --check-performance
```

**Health Check Queries**
```sql
-- Check table row counts
SELECT 
    table_schema,
    table_name,
    row_count
FROM information_schema.tables 
WHERE table_schema LIKE '%RETAILWORKS%'
ORDER BY table_schema, table_name;

-- Check view definitions
SELECT 
    table_schema,
    table_name,
    is_updatable
FROM information_schema.views 
WHERE table_schema LIKE '%ANALYTICS%';

-- Performance check
SELECT 
    query_text,
    execution_time,
    warehouse_name
FROM snowflake.account_usage.query_history 
WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
ORDER BY execution_time DESC
LIMIT 10;
```

### 2. Monitoring Setup

**Snowflake Monitoring**
```sql
-- Create monitoring alerts
CREATE ALERT HIGH_WAREHOUSE_USAGE
WAREHOUSE = MONITOR_WH
SCHEDULE = 'USING CRON 0 * * * * UTC'
IF (SELECT AVG(credits_used_compute) FROM snowflake.account_usage.warehouse_metering_history 
    WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL '1 hour') > 10
THEN CALL send_notification('High warehouse usage detected');
```

**Application Monitoring**
```python
# Streamlit health check endpoint
@st.cache_data
def health_check():
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        result = cursor.fetchone()
        return {"status": "healthy", "snowflake_version": result[0]}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
```

## Troubleshooting

### Common Deployment Issues

**1. Permission Errors**
```bash
# Error: Insufficient privileges
# Solution: Grant required privileges
GRANT CREATE SCHEMA ON DATABASE RETAILWORKS_DB TO ROLE your_role;
GRANT USAGE ON WAREHOUSE your_warehouse TO ROLE your_role;
```

**2. Connection Timeouts**
```bash
# Error: Connection timeout
# Solution: Check network connectivity and increase timeout
export SNOWFLAKE_CONNECT_TIMEOUT=300
export SNOWFLAKE_NETWORK_TIMEOUT=300
```

**3. Schema Conflicts**
```bash
# Error: Schema already exists
# Solution: Use IF NOT EXISTS or drop and recreate
CREATE SCHEMA IF NOT EXISTS schema_name;
# OR
DROP SCHEMA IF EXISTS schema_name;
CREATE SCHEMA schema_name;
```

**4. Data Loading Errors**
```bash
# Error: File format mismatch
# Solution: Validate file format and data types
SELECT $1, $2, $3 FROM @stage_name/file.csv (FILE_FORMAT => 'CSV_FORMAT') LIMIT 10;
```

### Deployment Validation Checklist

- [ ] All required schemas created
- [ ] All tables created with correct structure
- [ ] All views compile and execute
- [ ] All stored procedures compile
- [ ] Sample data loaded successfully (if applicable)
- [ ] Analytical views return data
- [ ] Dashboard connections work
- [ ] User permissions configured correctly
- [ ] Performance meets requirements
- [ ] Monitoring alerts configured
- [ ] Backup procedures in place

### Emergency Procedures

**Critical Production Issue**
1. Assess impact and scope
2. Implement immediate workaround if possible
3. Roll back to previous stable version
4. Investigate root cause
5. Develop and test fix
6. Deploy fix through normal pipeline
7. Post-incident review and documentation

**Data Corruption**
1. Stop all data loading processes
2. Assess extent of corruption
3. Restore from backup/time travel
4. Validate data integrity
5. Resume normal operations
6. Review and improve data validation

For additional support, refer to the [Troubleshooting Guide](troubleshooting.md) or contact the development team.