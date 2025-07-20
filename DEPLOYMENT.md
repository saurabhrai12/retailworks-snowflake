# RetailWorks Snowflake GitOps Deployment Guide

This document describes how to deploy and manage the RetailWorks Snowflake data platform using infrastructure as code and GitOps principles with the Snowflake CLI.

## Overview

The project uses the Snowflake CLI's native object management capabilities with `CREATE OR ALTER` commands to maintain infrastructure as code and keep your Snowflake account in sync with Git.

## Key Features

- **Infrastructure as Code**: All Snowflake objects defined in version-controlled files
- **GitOps Workflow**: Automated deployments triggered by Git changes
- **Safe Deployments**: Uses `CREATE OR ALTER` to avoid data loss
- **Drift Detection**: Automatically detects differences between Git and Snowflake
- **Environment Management**: Separate configurations for dev/test/prod
- **Validation**: Pre and post-deployment checks ensure reliability

## Project Structure

```
retailworks-snowflake/
├── snowflake.yml              # Main CLI configuration with object definitions
├── schemas/                   # Object schema definitions for CLI management
│   ├── database.yaml         # Database and schema definitions
│   ├── warehouse.yaml        # Warehouse configuration
│   ├── stages.yaml           # Stage definitions
│   └── file_formats.yaml    # File format definitions
├── ddl/                      # SQL DDL files using CREATE OR ALTER
│   ├── schemas/             # Database and schema creation
│   ├── tables/              # Table definitions
│   ├── views/               # View definitions
│   └── procedures/          # Stored procedure definitions
├── scripts/
│   ├── deploy_gitops.sh     # GitOps deployment script
│   └── deploy_with_cli.sh   # Legacy deployment script
└── .gitops/
    └── deploy.yml           # Deployment pipeline configuration
```

## Prerequisites

1. **Snowflake CLI**: Install the latest version
   ```bash
   pip install snowflake-cli-labs
   ```

2. **Snowflake Connection**: Configure connection for each environment
   ```bash
   snow connection add retailworks-dev --account YOUR_ACCOUNT --user YOUR_USER
   snow connection add retailworks-test --account YOUR_ACCOUNT --user YOUR_USER
   snow connection add retailworks-prod --account YOUR_ACCOUNT --user YOUR_USER
   ```

3. **Git Repository**: Ensure you're working in a Git repository

## Deployment Methods

### 1. GitOps Deployment (Recommended)

Use the new GitOps deployment script that leverages Snowflake CLI's native object management:

```bash
# Deploy to development environment
./scripts/deploy_gitops.sh dev

# Validate test deployment (dry run)
./scripts/deploy_gitops.sh test true

# Deploy to production with full validation
./scripts/deploy_gitops.sh prod false true
```

#### GitOps Features:
- **Native Object Management**: Uses `snow object deploy` commands
- **State Synchronization**: Keeps Git and Snowflake in sync
- **Drift Detection**: Identifies configuration drift
- **Comprehensive Validation**: Pre and post-deployment checks
- **Deployment Reports**: Generates detailed deployment logs

### 2. Legacy SQL Deployment

Use the traditional SQL-based deployment:

```bash
# Deploy using SQL scripts
./scripts/deploy_with_cli.sh dev
```

## Configuration

### Environment Variables

The project supports three environments with different configurations:

| Environment | Database | Schema Suffix | Warehouse |
|-------------|----------|---------------|-----------|
| dev         | RETAILWORKS_DB_DEV | _DEV | RETAILWORKS_DEV_WH |
| test        | RETAILWORKS_DB_TEST | _TEST | RETAILWORKS_TEST_WH |
| prod        | RETAILWORKS_DB | (none) | RETAILWORKS_PROD_WH |

### Object Management

Objects are defined in `snowflake.yml` and managed by the CLI:

```yaml
objects:
  - type: database
    name: "<% ctx.env.database_name %>"
    comment: "RetailWorks Enterprise Data Platform"

  - type: schema
    name: "SALES_SCHEMA<% ctx.env.schema_suffix %>"
    database: "<% ctx.env.database_name %>"
    comment: "Sales data schema"
```

## CREATE OR ALTER Approach

All SQL objects use `CREATE OR ALTER` commands for safe deployments:

### Tables
```sql
-- Safe table creation - won't drop existing data
CREATE TABLE IF NOT EXISTS CUSTOMERS (
    CUSTOMER_ID NUMBER(10,0) AUTOINCREMENT PRIMARY KEY,
    -- ... other columns
);
```

### Views
```sql
-- Safe view updates - allows schema evolution
CREATE OR ALTER VIEW VW_MONTHLY_SALES AS
SELECT 
    -- ... view definition
FROM SALES_FACT;
```

### Procedures
```sql
-- Safe procedure updates - preserves existing functionality
CREATE OR ALTER PROCEDURE SP_PROCESS_ORDER(...)
RETURNS STRING
LANGUAGE SQL
AS
$$
-- ... procedure body
$$;
```

## Deployment Workflow

### 1. Development
1. Make changes to SQL files or object definitions
2. Test locally using dry run: `./scripts/deploy_gitops.sh dev true`
3. Deploy to dev: `./scripts/deploy_gitops.sh dev`
4. Validate changes in development environment

### 2. Testing
1. Create pull request with changes
2. Automated validation runs on PR
3. Deploy to test environment: `./scripts/deploy_gitops.sh test`
4. Run integration tests and quality checks

### 3. Production
1. Merge approved changes to main branch
2. Create release tag
3. Deploy to production: `./scripts/deploy_gitops.sh prod`
4. Monitor deployment and verify functionality

## Drift Detection

The GitOps script includes drift detection to identify when Snowflake objects differ from Git:

```bash
# Check for configuration drift
./scripts/deploy_gitops.sh dev false true
```

Drift detection generates a state file and compares it with object definitions:
- `.snowflake_state_dev.json` - Current state in Snowflake
- Automatic comparison with `snowflake.yml` definitions
- Notifications when drift is detected

## Validation and Testing

### Pre-deployment Validation
- SQL syntax validation
- Object dependency checks
- Security rule scanning
- Template variable validation

### Post-deployment Testing
- Database connectivity tests
- Object existence verification
- Data quality checks
- Performance validation

### Rollback Procedures

If deployment fails:
1. Automatic rollback triggers activate
2. Previous state is restored from Git history
3. Notifications sent to relevant teams
4. Manual intervention may be required for complex scenarios

## Monitoring and Observability

### Deployment Reports
Each deployment generates a comprehensive report:
```json
{
  "deployment": {
    "timestamp": "2025-01-20 10:30:00 UTC",
    "environment": "prod",
    "database": "RETAILWORKS_DB",
    "status": "success"
  },
  "objects_deployed": {
    "native_objects": true,
    "sql_objects": true,
    "snowpark_apps": true
  },
  "git_state": {
    "commit_hash": "abc123...",
    "branch": "main"
  }
}
```

### State Management
- Object state files track current configuration
- Git history provides change audit trail
- Deployment logs maintain operational history

## Best Practices

### 1. Code Organization
- Keep related objects in the same files
- Use consistent naming conventions
- Include comprehensive comments
- Version control all changes

### 2. Environment Management
- Use environment-specific variables
- Maintain separate connections per environment
- Test thoroughly in dev/test before production
- Follow approval workflows for production changes

### 3. Security
- Use managed access schemas for sensitive data
- Implement role-based access control
- Regularly audit permissions
- Monitor data access patterns

### 4. Performance
- Right-size warehouses for each environment
- Implement automatic scaling policies
- Monitor query performance
- Optimize data storage and clustering

## Troubleshooting

### Common Issues

1. **Connection Errors**
   ```bash
   # Verify connection configuration
   snow connection test retailworks-dev
   ```

2. **Template Variable Errors**
   ```bash
   # Check variable definitions in snowflake.yml
   snow object list --environment dev --validate-only
   ```

3. **Object Dependency Issues**
   ```bash
   # Validate dependencies before deployment
   snow object validate --check-dependencies
   ```

4. **Drift Detection Alerts**
   ```bash
   # Review current state vs Git
   snow object diff --environment prod --show-changes
   ```

### Getting Help

- Review deployment logs in `deployment_report_*.json`
- Check Snowflake CLI documentation
- Examine object state files for current configuration
- Contact the data platform team for assistance

## Advanced Features

### Custom Validation Rules
Add custom validation scripts in `.gitops/validations/`:
```bash
#!/bin/bash
# Custom data quality validation
snow sql -f tests/custom_validation.sql --connection retailworks-$ENVIRONMENT
```

### Automated Rollbacks
Configure automatic rollback triggers:
```yaml
rollback:
  enabled: true
  triggers:
    - "deployment-failure"
    - "validation-failure"
  maxRetries: 3
```

### Integration with CI/CD
Example GitHub Actions workflow:
```yaml
name: Deploy to Snowflake
on:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Deploy to Snowflake
        run: ./scripts/deploy_gitops.sh prod
```

This GitOps approach ensures your Snowflake infrastructure remains consistent, version-controlled, and aligned with modern DevOps practices.