# Testing Guide

## Overview

This document outlines the comprehensive testing strategy for the RetailWorks Snowflake Data Warehouse project, including test types, frameworks, and execution procedures.

## Testing Pyramid

```
    ┌─────────────────┐
    │   E2E Tests     │  ← 10%
    │   (Integration) │
    ├─────────────────┤
    │ Component Tests │  ← 20%
    │   (API/ETL)     │
    ├─────────────────┤
    │   Unit Tests    │  ← 70%
    │ (Functions/SQL) │
    └─────────────────┘
```

## Test Categories

### 1. Unit Tests
**Purpose**: Test individual components in isolation
**Coverage**: 70% of testing effort
**Location**: `snowpark/tests/unit/`

#### Database Schema Tests
```python
# test_schemas.py
def test_customer_schema_exists():
    """Test that customer schema exists and is accessible"""
    
def test_table_structures():
    """Validate table structures match specifications"""
    
def test_data_types():
    """Verify column data types are correct"""
```

#### SQL Function Tests
```python
# test_functions.py
def test_calculate_total_price():
    """Test price calculation function with various inputs"""
    
def test_date_formatting():
    """Test date formatting functions"""
    
def test_data_validation_rules():
    """Test business rule validation functions"""
```

### 2. Integration Tests
**Purpose**: Test component interactions
**Coverage**: 20% of testing effort
**Location**: `snowpark/tests/integration/`

#### ETL Pipeline Tests
```python
# test_etl_pipeline.py
def test_customer_data_flow():
    """Test complete customer data pipeline"""
    
def test_product_data_transformation():
    """Test product data transformation accuracy"""
    
def test_sales_aggregation():
    """Test sales data aggregation process"""
```

#### Cross-Schema Tests
```python
# test_cross_schema.py
def test_foreign_key_relationships():
    """Test referential integrity across schemas"""
    
def test_view_dependencies():
    """Test that views work with underlying tables"""
```

### 3. Performance Tests
**Purpose**: Validate system performance
**Coverage**: 10% of testing effort
**Location**: `snowpark/tests/performance/`

#### Query Performance Tests
```python
# test_performance.py
def test_customer_query_performance():
    """Test customer queries meet SLA requirements"""
    
def test_large_dataset_operations():
    """Test operations on large datasets"""
    
def test_concurrent_user_load():
    """Test system under concurrent user load"""
```

## Test Framework Setup

### Dependencies
```toml
# pyproject.toml
[project.optional-dependencies]
test = [
    "pytest>=7.0.0",
    "pytest-cov>=4.0.0",
    "pytest-mock>=3.10.0",
    "pytest-xdist>=3.0.0",
    "snowflake-connector-python>=3.0.0",
    "pandas>=2.0.0",
    "great-expectations>=0.17.0"
]
```

### Configuration
```ini
# pytest.ini
[tool:pytest]
testpaths = snowpark/tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = 
    --verbose
    --tb=short
    --cov=snowpark
    --cov-report=html
    --cov-report=term-missing
    --cov-fail-under=80
markers =
    unit: Unit tests
    integration: Integration tests
    performance: Performance tests
    slow: Slow running tests
    smoke: Smoke tests for quick validation
```

## Test Data Management

### Test Data Strategy
1. **Synthetic Data**: Generated test data for consistent testing
2. **Sample Data**: Representative subset of production data
3. **Mocked Data**: Mock objects for isolated testing
4. **Fixtures**: Reusable test data sets

### Test Database Setup
```python
# conftest.py
@pytest.fixture(scope="session")
def test_database():
    """Setup test database for testing"""
    conn = get_test_connection()
    # Create test schemas and tables
    yield conn
    # Cleanup after tests
    
@pytest.fixture
def sample_customers():
    """Provide sample customer data for testing"""
    return [
        {"customer_id": 1, "name": "Test Customer 1"},
        {"customer_id": 2, "name": "Test Customer 2"}
    ]
```

## Data Quality Testing

### Great Expectations Integration
```python
# data_quality/expectations.py
import great_expectations as ge

def test_customer_data_quality():
    """Test customer data meets quality expectations"""
    df = ge.read_csv("customer_data.csv")
    
    # Expectations
    df.expect_column_to_exist("customer_id")
    df.expect_column_values_to_not_be_null("customer_id")
    df.expect_column_values_to_be_unique("customer_id")
    df.expect_column_values_to_match_regex("email", r"^[^@]+@[^@]+\.[^@]+$")
```

### Custom Data Validation
```python
# test_data_quality.py
def test_referential_integrity():
    """Test foreign key relationships are maintained"""
    
def test_data_completeness():
    """Test that required fields are populated"""
    
def test_data_accuracy():
    """Test that data values are within expected ranges"""
    
def test_data_consistency():
    """Test data consistency across related tables"""
```

## Test Execution

### Local Testing
```bash
# Run all tests
uv run pytest

# Run specific test category
uv run pytest -m unit
uv run pytest -m integration
uv run pytest -m performance

# Run tests with coverage
uv run pytest --cov=snowpark --cov-report=html

# Run parallel tests
uv run pytest -n auto

# Run specific test file
uv run pytest snowpark/tests/test_database.py

# Run with verbose output
uv run pytest -v -s
```

### CI/CD Testing
```yaml
# In GitHub Actions
- name: Run Unit Tests
  run: uv run pytest -m unit --junitxml=test-results/unit-results.xml

- name: Run Integration Tests  
  run: uv run pytest -m integration --junitxml=test-results/integration-results.xml

- name: Upload Test Results
  uses: actions/upload-artifact@v3
  with:
    name: test-results
    path: test-results/
```

## Environment-Specific Testing

### Development Environment
```bash
# Set test environment
export SNOWFLAKE_DATABASE=RETAILWORKS_DB_DEV
export TEST_ENVIRONMENT=development

# Run development tests
uv run pytest --environment=dev
```

### Staging Environment
```bash
# Staging-specific tests
export SNOWFLAKE_DATABASE=RETAILWORKS_DB_STAGING
export TEST_ENVIRONMENT=staging

# Run integration tests
uv run pytest -m integration --environment=staging
```

### Production Smoke Tests
```bash
# Production smoke tests (read-only)
export SNOWFLAKE_DATABASE=RETAILWORKS_DB
export TEST_ENVIRONMENT=production

# Run smoke tests only
uv run pytest -m smoke --environment=production
```

## Test Scenarios

### 1. Schema Validation Tests
```python
def test_all_schemas_exist():
    """Verify all required schemas exist"""
    required_schemas = [
        'CUSTOMERS_SCHEMA_DEV',
        'PRODUCTS_SCHEMA_DEV', 
        'HR_SCHEMA_DEV',
        'SALES_SCHEMA_DEV',
        'ANALYTICS_SCHEMA_DEV'
    ]
    for schema in required_schemas:
        assert schema_exists(schema)

def test_table_row_counts():
    """Verify tables have expected data"""
    assert get_row_count('CUSTOMERS_SCHEMA_DEV.CUSTOMERS') > 0
    assert get_row_count('PRODUCTS_SCHEMA_DEV.PRODUCTS') > 0
```

### 2. Data Pipeline Tests
```python
def test_etl_customer_pipeline():
    """Test customer ETL pipeline end-to-end"""
    # Load test data
    load_test_customers()
    
    # Run ETL process
    run_customer_etl()
    
    # Validate results
    assert customer_data_transformed_correctly()
    assert no_data_loss_occurred()
```

### 3. Performance Tests
```python
@pytest.mark.performance
def test_customer_query_performance():
    """Test customer queries complete within SLA"""
    start_time = time.time()
    
    result = execute_customer_query()
    
    execution_time = time.time() - start_time
    assert execution_time < 5.0  # 5 second SLA
    assert len(result) > 0
```

### 4. Security Tests
```python
def test_role_based_access():
    """Test that roles have appropriate access"""
    
def test_data_masking():
    """Test that sensitive data is properly masked"""
    
def test_sql_injection_prevention():
    """Test that SQL injection is prevented"""
```

## Test Data Scenarios

### Positive Test Cases
- Valid data inputs
- Expected business scenarios
- Normal load conditions

### Negative Test Cases
- Invalid data inputs
- Edge cases and boundary conditions
- Error handling scenarios

### Boundary Test Cases
- Maximum/minimum values
- Empty datasets
- Large data volumes

## Continuous Testing

### Automated Test Execution
1. **Pre-commit hooks**: Run unit tests before commits
2. **Pull request validation**: Full test suite on PRs
3. **Nightly builds**: Comprehensive testing overnight
4. **Production monitoring**: Continuous health checks

### Test Reporting
```python
# Generate test reports
pytest --html=reports/test_report.html --self-contained-html
pytest --junitxml=reports/junit.xml
pytest --cov-report=html:reports/coverage
```

### Metrics Tracking
- Test execution time
- Test pass/fail rates
- Code coverage percentages
- Defect detection rates

## Troubleshooting Tests

### Common Test Issues

#### 1. Connection Failures
```python
def test_database_connection():
    """Verify database connection works"""
    try:
        conn = get_snowflake_connection()
        assert conn is not None
        conn.close()
    except Exception as e:
        pytest.fail(f"Database connection failed: {e}")
```

#### 2. Data Setup Issues
```python
def setup_test_data():
    """Setup test data with proper cleanup"""
    try:
        create_test_tables()
        load_test_data()
        yield
    finally:
        cleanup_test_data()
```

#### 3. Environment Issues
```python
def test_environment_configuration():
    """Verify test environment is properly configured"""
    assert os.getenv('SNOWFLAKE_DATABASE') is not None
    assert os.getenv('TEST_ENVIRONMENT') in ['dev', 'staging', 'prod']
```

### Debugging Failed Tests
```bash
# Run failed tests only
uv run pytest --lf

# Run with pdb debugger
uv run pytest --pdb

# Increase verbosity
uv run pytest -vv --tb=long

# Run specific test with output
uv run pytest tests/test_specific.py::test_function -s
```

## Best Practices

### Test Writing Guidelines
1. **Test Naming**: Use descriptive test names
2. **Test Structure**: Follow Arrange-Act-Assert pattern
3. **Test Independence**: Tests should be independent
4. **Test Documentation**: Document complex test scenarios

### Data Management
1. **Test Isolation**: Use separate test data
2. **Data Cleanup**: Clean up after tests
3. **Data Versioning**: Version test datasets
4. **Data Privacy**: Use anonymized data

### Performance Considerations
1. **Parallel Execution**: Run tests in parallel when possible
2. **Test Caching**: Cache expensive setup operations
3. **Resource Management**: Clean up resources properly
4. **Test Optimization**: Optimize slow tests

## References

- [Pytest Documentation](https://docs.pytest.org/)
- [Great Expectations](https://greatexpectations.io/)
- [Snowflake Testing Best Practices](https://docs.snowflake.com/en/user-guide/testing.html)
- [CI/CD Deployment Guide](./ci-cd-deployment-guide.md)