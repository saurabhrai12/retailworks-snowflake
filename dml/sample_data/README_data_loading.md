# Sample Data Loading Guide

This guide explains how to load the generated sample data into your Snowflake database across all schemas.

## Files Generated

The sample data generation created the following CSV files:
- `customer_segments.csv` - 6 customer segment types
- `categories.csv` - 12 product categories  
- `suppliers.csv` - 200 supplier records
- `products.csv` - 5,000 product records
- `addresses.csv` - 75,000 address records
- `customers.csv` - 50,000 customer records
- `departments.csv` - 10 department records
- `positions.csv` - 15 position records

## Data Loading Options

### Option 1: Python Script (Automated)

1. **Setup Snowflake Connection**
   ```bash
   ./dml/sample_data/setup_snowflake_connection.sh
   ```

2. **Create Additional Staging Tables**
   Run this SQL in Snowflake first:
   ```bash
   # Execute in Snowflake worksheet:
   # ./dml/sample_data/04_create_additional_staging_tables.sql
   ```

3. **Load Data via Python**
   ```bash
   uv run python ./dml/sample_data/load_csv_data.py
   ```

### Option 2: Manual SQL Loading

1. **Upload CSV files to Snowflake stage**
   - Use Snowflake web interface to upload CSV files
   - Or use PUT commands in SnowSQL

2. **Run SQL Loading Script**
   ```sql
   -- Execute in Snowflake worksheet:
   -- ./dml/sample_data/05_load_sample_data_to_tables.sql
   ```

### Option 3: Snowflake Web Interface

1. Navigate to Snowflake web interface
2. Go to Databases > RETAILWORKS_DB > Schemas
3. Use the "Load Data" wizard for each table
4. Upload corresponding CSV files

## Data Distribution

### Target Schemas and Tables

**CUSTOMERS_SCHEMA:**
- CUSTOMER_SEGMENTS - Reference data for customer types
- CUSTOMERS - Main customer records  
- CUSTOMER_ADDRESSES - Customer address information

**PRODUCTS_SCHEMA:**
- CATEGORIES - Product categorization hierarchy
- SUPPLIERS - Vendor/supplier information
- PRODUCTS - Product catalog (references categories & suppliers)

**HR_SCHEMA:**
- DEPARTMENTS - Organizational departments
- POSITIONS - Job positions (references departments)

**SALES_SCHEMA:**
- Will be populated with transactional data based on customers/products

**ANALYTICS_SCHEMA:**
- SALES_FACT - Generated sales transactions
- CUSTOMER_ANALYTICS_FACT - Customer behavior analytics

## Verification Queries

After loading, verify the data with these queries:

```sql
-- Check record counts
SELECT 'Customer Segments' as TABLE_NAME, COUNT(*) as RECORD_COUNT 
FROM CUSTOMERS_SCHEMA.CUSTOMER_SEGMENTS
UNION ALL
SELECT 'Customers', COUNT(*) FROM CUSTOMERS_SCHEMA.CUSTOMERS
UNION ALL
SELECT 'Products', COUNT(*) FROM PRODUCTS_SCHEMA.PRODUCTS
UNION ALL
SELECT 'Suppliers', COUNT(*) FROM PRODUCTS_SCHEMA.SUPPLIERS
-- Add other tables as needed
;

-- Check sample data
SELECT * FROM CUSTOMERS_SCHEMA.CUSTOMERS LIMIT 10;
SELECT * FROM PRODUCTS_SCHEMA.PRODUCTS LIMIT 10;
```

## Data Relationships

The sample data maintains referential integrity:
- Products reference Categories and Suppliers
- Customer Addresses reference Customers  
- Positions reference Departments
- Analytics tables reference dimensional data

## Troubleshooting

**Connection Issues:**
- Verify Snowflake account, username, password
- Check role permissions for database access
- Ensure warehouse is available and accessible

**Data Loading Issues:**
- Check CSV file format (comma-delimited, header row)
- Verify table schemas match CSV structure
- Review ON_ERROR settings in COPY commands

**Performance:**
- Large datasets may take several minutes to load
- Consider loading in batches for very large files
- Monitor warehouse size for optimal performance

## Next Steps

After loading sample data:
1. Run analytical queries to explore the data
2. Create additional views and reports
3. Set up data pipelines for ongoing updates
4. Implement data quality monitoring