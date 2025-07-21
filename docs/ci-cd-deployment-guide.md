# CI/CD Deployment Guide

## Overview

This document outlines the Continuous Integration and Continuous Deployment (CI/CD) pipeline for the RetailWorks Snowflake Data Warehouse project.

## Pipeline Architecture

### Environments

1. **Development (DEV)** - `RETAILWORKS_DB_DEV`
   - Feature development and initial testing
   - Automatic deployment from `develop` branch
   - Sample data available for testing

2. **Staging (STAGING)** - `RETAILWORKS_DB_STAGING`  
   - Pre-production testing environment
   - Integration and performance testing
   - Deployed from `main` branch

3. **Production (PROD)** - `RETAILWORKS_DB`
   - Live production environment
   - Manual approval required for deployment
   - Full monitoring and alerting

## Deployment Pipeline Stages

### 1. Test Stage
**Triggers**: All pushes and pull requests
**Duration**: ~5-10 minutes

- **Schema Validation**: Validates DDL scripts syntax
- **Unit Tests**: Tests individual components
- **Data Quality Tests**: Validates data integrity rules
- **Security Scan**: Checks for secrets and vulnerabilities

```bash
# Local testing
uv run python -m pytest snowpark/tests/ -v
```

### 2. Security Stage
**Triggers**: All branches
**Duration**: ~2-3 minutes

- Code security scanning
- Secret detection
- Dependency vulnerability check
- SQL injection pattern detection

### 3. Development Deployment
**Triggers**: Push to `develop` branch
**Duration**: ~10-15 minutes

```yaml
Environment: development
Database: RETAILWORKS_DB_DEV
Approval: Automatic
```

**Steps**:
1. Deploy database schemas
2. Deploy tables and views
3. Deploy stored procedures
4. Run post-deployment tests
5. Optional: Load sample data

### 4. Staging Deployment
**Triggers**: Push to `main` branch
**Duration**: ~15-20 minutes

```yaml
Environment: staging
Database: RETAILWORKS_DB_STAGING
Approval: Automatic (after dev success)
```

**Steps**:
1. Full schema deployment
2. Integration testing
3. Performance testing
4. Data migration validation

### 5. Production Deployment
**Triggers**: Manual approval after staging
**Duration**: ~20-30 minutes

```yaml
Environment: production
Database: RETAILWORKS_DB
Approval: Manual (required)
```

**Steps**:
1. Manual approval gate
2. Production deployment
3. Smoke tests
4. Release creation
5. Monitoring setup

## Environment Configuration

### GitHub Secrets Required

```bash
SNOWFLAKE_ACCOUNT=your-account.region
SNOWFLAKE_USER=ci-cd-user
SNOWFLAKE_PASSWORD=secure-password
SNOWFLAKE_ROLE=DEPLOYMENT_ROLE
SNOWFLAKE_WAREHOUSE=DEPLOYMENT_WH
```

### Environment-Specific Variables

```yaml
# Development
SNOWFLAKE_DATABASE: RETAILWORKS_DB_DEV
ENVIRONMENT: dev

# Staging  
SNOWFLAKE_DATABASE: RETAILWORKS_DB_STAGING
ENVIRONMENT: staging

# Production
SNOWFLAKE_DATABASE: RETAILWORKS_DB
ENVIRONMENT: prod
```

## Deployment Scripts

### 1. Schema Deployment
```bash
# Deploy all schemas
python jenkins/deployment/deploy_schemas.py --environment dev

# Deploy specific schema
python jenkins/deployment/deploy_schemas.py --schema CUSTOMERS_SCHEMA --environment dev
```

### 2. Table Deployment
```bash
# Deploy all tables
python jenkins/deployment/deploy_tables.py --environment dev

# Deploy with data validation
python jenkins/deployment/deploy_tables.py --environment dev --validate
```

### 3. Data Loading
```bash
# Load sample data (development only)
python dml/sample_data/load_large_data.py

# Load production data
python scripts/load_production_data.py --environment prod
```

## Testing Strategy

### Unit Tests
- Individual component testing
- Schema validation
- Function/procedure testing

### Integration Tests
- Cross-schema dependencies
- ETL pipeline validation
- Data flow testing

### Performance Tests
- Query performance benchmarks
- Load testing
- Resource utilization

### Smoke Tests
- Basic connectivity
- Core functionality
- Data availability

## Rollback Procedures

### Automatic Rollback Triggers
- Failed smoke tests
- Critical error detection
- Performance degradation

### Manual Rollback
```bash
# Rollback to previous version
python scripts/rollback_deployment.py --version v123

# Rollback specific component
python scripts/rollback_deployment.py --component tables --version v123
```

## Monitoring and Alerting

### Deployment Monitoring
- Deployment success/failure notifications
- Performance metrics
- Error rate monitoring

### Key Metrics
- Deployment frequency
- Lead time for changes
- Mean time to recovery (MTTR)
- Change failure rate

### Alerts
- Slack notifications for failures
- Email alerts for production issues
- Dashboard updates

## Best Practices

### Code Standards
1. **SQL Formatting**: Use consistent SQL formatting
2. **Naming Conventions**: Follow established naming patterns
3. **Documentation**: Document all changes
4. **Version Control**: Tag releases properly

### Security
1. **No Hardcoded Secrets**: Use environment variables
2. **Least Privilege**: Minimal required permissions
3. **Encryption**: Encrypt sensitive data
4. **Audit Logging**: Track all changes

### Testing
1. **Test Coverage**: Aim for >80% coverage
2. **Data Validation**: Validate all data transformations
3. **Performance Testing**: Test with realistic data volumes
4. **Regression Testing**: Prevent breaking changes

## Troubleshooting

### Common Issues

#### 1. Connection Failures
```bash
# Check credentials
echo $SNOWFLAKE_ACCOUNT
echo $SNOWFLAKE_USER

# Test connection
python streamlit/utils/snowflake_connection.py
```

#### 2. Schema Deployment Failures
```bash
# Check schema syntax
python scripts/validate_schemas.py

# Manual deployment
python jenkins/deployment/deploy_schemas.py --debug
```

#### 3. Test Failures
```bash
# Run specific test
python -m pytest snowpark/tests/test_database.py::test_schema_exists -v

# Run with debug output
python -m pytest snowpark/tests/ -v -s --tb=long
```

### Log Locations
- **GitHub Actions**: Actions tab in repository
- **Local Logs**: `logs/` directory
- **Snowflake Logs**: Query history in Snowflake UI

## Maintenance

### Regular Tasks
1. **Dependency Updates**: Monthly dependency review
2. **Performance Review**: Weekly performance analysis  
3. **Security Scan**: Daily security checks
4. **Backup Verification**: Daily backup validation

### Quarterly Reviews
1. Pipeline optimization
2. Security audit
3. Performance benchmarking
4. Process improvements

## Support and Escalation

### Level 1: Self-Service
- Check pipeline logs
- Review documentation
- Run diagnostic scripts

### Level 2: Team Support
- Post in team Slack channel
- Create GitHub issue
- Review with team lead

### Level 3: Critical Issues
- Page on-call engineer
- Escalate to architecture team
- Engage vendor support if needed

## References

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Snowflake DevOps Guide](https://docs.snowflake.com/en/user-guide/devops.html)
- [Project README](../README.md)
- [Testing Documentation](./testing-guide.md)