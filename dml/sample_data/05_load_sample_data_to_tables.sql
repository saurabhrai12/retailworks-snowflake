-- =====================================================
-- Load Sample Data to Tables
-- Description: Loads the generated CSV sample data into dimensional tables
-- Version: 1.0
-- Date: 2025-07-20
-- =====================================================

-- Note: Before running this script, you need to:
-- 1. Upload the CSV files to a Snowflake stage
-- 2. Or use Snowflake's web interface to load the files
-- 3. Or use the Python script with proper credentials

USE DATABASE <% database_name %>;

-- =====================================================
-- Load Customer Segments
-- =====================================================
USE SCHEMA CUSTOMERS_SCHEMA<% schema_suffix %>;

-- First, create a file format if it doesn't exist
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('NULL', 'null', '')
EMPTY_FIELD_AS_NULL = TRUE
COMPRESSION = AUTO;

-- Create stage for sample data
CREATE OR REPLACE STAGE SAMPLE_DATA_STAGE
FILE_FORMAT = CSV_FORMAT
COMMENT = 'Stage for sample data CSV files';

-- Load customer segments (assuming CSV has been uploaded to stage)
-- PUT file:///path/to/customer_segments.csv @SAMPLE_DATA_STAGE;

COPY INTO CUSTOMER_SEGMENTS (SEGMENT_NAME, DESCRIPTION, MIN_ANNUAL_REVENUE, MAX_ANNUAL_REVENUE, DISCOUNT_RATE)
FROM (
    SELECT 
        $2 as SEGMENT_NAME,
        $3 as DESCRIPTION, 
        TRY_CAST($4 AS DECIMAL(15,2)) as MIN_ANNUAL_REVENUE,
        TRY_CAST($5 AS DECIMAL(15,2)) as MAX_ANNUAL_REVENUE,
        TRY_CAST($6 AS DECIMAL(5,4)) as DISCOUNT_RATE
    FROM @SAMPLE_DATA_STAGE/customer_segments.csv
)
ON_ERROR = 'CONTINUE';

-- =====================================================
-- Load Categories 
-- =====================================================
USE SCHEMA PRODUCTS_SCHEMA<% schema_suffix %>;

COPY INTO CATEGORIES (CATEGORY_NAME, DESCRIPTION, PARENT_CATEGORY_ID)
FROM (
    SELECT 
        $2 as CATEGORY_NAME,
        $3 as DESCRIPTION,
        TRY_CAST(NULLIF($4, '') AS NUMBER) as PARENT_CATEGORY_ID
    FROM @CUSTOMERS_SCHEMA<% schema_suffix %>.SAMPLE_DATA_STAGE/categories.csv
)
ON_ERROR = 'CONTINUE';

-- =====================================================
-- Load Suppliers
-- =====================================================
COPY INTO SUPPLIERS (
    SUPPLIER_NAME, CONTACT_NAME, CONTACT_TITLE, ADDRESS, CITY, REGION, 
    POSTAL_CODE, COUNTRY, PHONE, EMAIL, WEBSITE, STATUS, RATING
)
FROM (
    SELECT 
        $2 as SUPPLIER_NAME,
        $3 as CONTACT_NAME,
        $4 as CONTACT_TITLE,
        $5 as ADDRESS,
        $6 as CITY,
        $7 as REGION,
        $8 as POSTAL_CODE,
        $9 as COUNTRY,
        $10 as PHONE,
        $11 as EMAIL,
        $12 as WEBSITE,
        $13 as STATUS,
        TRY_CAST($14 AS DECIMAL(3,1)) as RATING
    FROM @CUSTOMERS_SCHEMA<% schema_suffix %>.SAMPLE_DATA_STAGE/suppliers.csv
)
ON_ERROR = 'CONTINUE';

-- =====================================================
-- Load Products (using existing categories and suppliers)
-- =====================================================
-- Note: This assumes categories and suppliers are already loaded
-- We'll match by name since IDs might be different

COPY INTO PRODUCTS (
    PRODUCT_NUMBER, PRODUCT_NAME, CATEGORY_ID, SUPPLIER_ID, DESCRIPTION,
    COLOR, SIZE, WEIGHT, UNIT_PRICE, COST, LIST_PRICE, DISCONTINUED
)
FROM (
    SELECT 
        $2 as PRODUCT_NUMBER,
        $3 as PRODUCT_NAME,
        COALESCE(c.CATEGORY_ID, 1) as CATEGORY_ID,  -- Default to first category if not found
        COALESCE(s.SUPPLIER_ID, 1) as SUPPLIER_ID,  -- Default to first supplier if not found
        $6 as DESCRIPTION,
        $7 as COLOR,
        $8 as SIZE,
        TRY_CAST($9 AS DECIMAL(8,2)) as WEIGHT,
        TRY_CAST($10 AS DECIMAL(10,2)) as UNIT_PRICE,
        TRY_CAST($11 AS DECIMAL(10,2)) as COST,
        TRY_CAST($12 AS DECIMAL(10,2)) as LIST_PRICE,
        TRY_CAST($13 AS BOOLEAN) as DISCONTINUED
    FROM @CUSTOMERS_SCHEMA<% schema_suffix %>.SAMPLE_DATA_STAGE/products.csv p
    LEFT JOIN CATEGORIES c ON c.CATEGORY_NAME = $4
    LEFT JOIN SUPPLIERS s ON s.SUPPLIER_NAME = $5
)
ON_ERROR = 'CONTINUE';

-- =====================================================
-- Load Customers
-- =====================================================
USE SCHEMA CUSTOMERS_SCHEMA<% schema_suffix %>;

COPY INTO CUSTOMERS (
    CUSTOMER_NUMBER, CUSTOMER_TYPE, COMPANY_NAME, FIRST_NAME, LAST_NAME, 
    EMAIL, PHONE, BIRTH_DATE, GENDER, ANNUAL_INCOME, STATUS, 
    PREFERRED_LANGUAGE, MARKETING_OPT_IN, REGISTRATION_DATE
)
FROM (
    SELECT 
        $2 as CUSTOMER_NUMBER,
        $3 as CUSTOMER_TYPE,
        $4 as COMPANY_NAME,
        $5 as FIRST_NAME,
        $6 as LAST_NAME,
        $7 as EMAIL,
        $8 as PHONE,
        TRY_CAST($9 AS DATE) as BIRTH_DATE,
        $10 as GENDER,
        TRY_CAST($11 AS DECIMAL(15,2)) as ANNUAL_INCOME,
        $12 as STATUS,
        $13 as PREFERRED_LANGUAGE,
        TRY_CAST($14 AS BOOLEAN) as MARKETING_OPT_IN,
        TRY_CAST($15 AS DATE) as REGISTRATION_DATE
    FROM @SAMPLE_DATA_STAGE/customers.csv
)
ON_ERROR = 'CONTINUE';

-- =====================================================
-- Load Customer Addresses
-- =====================================================
COPY INTO CUSTOMER_ADDRESSES (
    CUSTOMER_ID, ADDRESS_TYPE, ADDRESS_LINE_1, ADDRESS_LINE_2,
    CITY, STATE_PROVINCE, POSTAL_CODE, COUNTRY, IS_DEFAULT
)
FROM (
    SELECT 
        c.CUSTOMER_ID,
        $3 as ADDRESS_TYPE,
        $4 as ADDRESS_LINE_1,
        $5 as ADDRESS_LINE_2,
        $6 as CITY,
        $7 as STATE_PROVINCE,
        $8 as POSTAL_CODE,
        $9 as COUNTRY,
        TRY_CAST($10 AS BOOLEAN) as IS_DEFAULT
    FROM @SAMPLE_DATA_STAGE/addresses.csv a
    JOIN CUSTOMERS c ON c.CUSTOMER_NUMBER = $2  -- Match by customer number
)
ON_ERROR = 'CONTINUE';

-- =====================================================
-- Load HR Data
-- =====================================================
USE SCHEMA HR_SCHEMA<% schema_suffix %>;

-- Load Departments
COPY INTO DEPARTMENTS (
    DEPARTMENT_NAME, DEPARTMENT_CODE, DESCRIPTION, BUDGET, LOCATION, PHONE, EMAIL
)
FROM (
    SELECT 
        $2 as DEPARTMENT_NAME,
        $3 as DEPARTMENT_CODE,
        $4 as DESCRIPTION,
        TRY_CAST($5 AS DECIMAL(15,2)) as BUDGET,
        $6 as LOCATION,
        $7 as PHONE,
        $8 as EMAIL
    FROM @CUSTOMERS_SCHEMA<% schema_suffix %>.SAMPLE_DATA_STAGE/departments.csv
)
ON_ERROR = 'CONTINUE';

-- Load Positions
COPY INTO POSITIONS (
    POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, 
    MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS
)
FROM (
    SELECT 
        $2 as POSITION_TITLE,
        $3 as POSITION_CODE,
        d.DEPARTMENT_ID,
        TRY_CAST($5 AS NUMBER) as JOB_LEVEL,
        TRY_CAST($6 AS DECIMAL(10,2)) as MIN_SALARY,
        TRY_CAST($7 AS DECIMAL(10,2)) as MAX_SALARY,
        $8 as DESCRIPTION,
        $9 as STATUS
    FROM @CUSTOMERS_SCHEMA<% schema_suffix %>.SAMPLE_DATA_STAGE/positions.csv p
    JOIN DEPARTMENTS d ON d.DEPARTMENT_ID = TRY_CAST($4 AS NUMBER)
)
ON_ERROR = 'CONTINUE';

-- =====================================================
-- Load Analytics Data (Fact Tables)
-- =====================================================
USE SCHEMA ANALYTICS_SCHEMA<% schema_suffix %>;

-- Generate sample sales fact data
-- This creates orders and order items based on the loaded customers and products

-- First, generate some orders
INSERT INTO SALES_FACT (
    DATE_ID, CUSTOMER_ID, PRODUCT_ID, SALES_REP_ID, TERRITORY_ID,
    ORDER_NUMBER, QUANTITY_ORDERED, UNIT_PRICE, DISCOUNT_AMOUNT,
    LINE_TOTAL, COST_AMOUNT, PROFIT_AMOUNT
)
WITH sample_orders AS (
    SELECT 
        d.DATE_ID,
        c.CUSTOMER_ID,
        p.PRODUCT_ID,
        1 as SALES_REP_ID,  -- Default sales rep
        1 as TERRITORY_ID,  -- Default territory
        'ORD-' || LPAD(ROW_NUMBER() OVER (ORDER BY c.CUSTOMER_ID, p.PRODUCT_ID), 8, '0') as ORDER_NUMBER,
        FLOOR(RANDOM() * 10) + 1 as QUANTITY_ORDERED,
        p.UNIT_PRICE,
        p.UNIT_PRICE * (RANDOM() * 0.2) as DISCOUNT_AMOUNT
    FROM CUSTOMERS_SCHEMA<% schema_suffix %>.CUSTOMERS c
    CROSS JOIN PRODUCTS_SCHEMA<% schema_suffix %>.PRODUCTS p
    CROSS JOIN DATE_DIM d
    WHERE d.DATE_ACTUAL >= '2024-01-01' 
      AND d.DATE_ACTUAL < '2025-01-01'
      AND RANDOM() < 0.001  -- Only 0.1% of possible combinations to keep data reasonable
    LIMIT 10000  -- Limit to 10,000 sales records
)
SELECT 
    DATE_ID, CUSTOMER_ID, PRODUCT_ID, SALES_REP_ID, TERRITORY_ID, ORDER_NUMBER,
    QUANTITY_ORDERED, UNIT_PRICE, DISCOUNT_AMOUNT,
    (QUANTITY_ORDERED * UNIT_PRICE) - DISCOUNT_AMOUNT as LINE_TOTAL,
    QUANTITY_ORDERED * (UNIT_PRICE * 0.7) as COST_AMOUNT,  -- Assume 70% cost ratio
    ((QUANTITY_ORDERED * UNIT_PRICE) - DISCOUNT_AMOUNT) - (QUANTITY_ORDERED * (UNIT_PRICE * 0.7)) as PROFIT_AMOUNT
FROM sample_orders;

-- Generate customer analytics
INSERT INTO CUSTOMER_ANALYTICS_FACT (
    DATE_ID, CUSTOMER_ID, SEGMENT_ID, TOTAL_ORDERS, TOTAL_AMOUNT,
    TOTAL_PROFIT, AVERAGE_ORDER_VALUE, LAST_ORDER_DATE
)
SELECT 
    DATE_TRUNC('MONTH', sf.DATE_ACTUAL)::DATE as month_date,
    dd.DATE_ID,
    sf.CUSTOMER_ID,
    cs.SEGMENT_ID,
    COUNT(DISTINCT sf.ORDER_NUMBER) as TOTAL_ORDERS,
    SUM(sf.LINE_TOTAL) as TOTAL_AMOUNT,
    SUM(sf.PROFIT_AMOUNT) as TOTAL_PROFIT,
    AVG(sf.LINE_TOTAL) as AVERAGE_ORDER_VALUE,
    MAX(sf.DATE_ACTUAL) as LAST_ORDER_DATE
FROM SALES_FACT sf
JOIN DATE_DIM dd ON dd.DATE_ACTUAL = DATE_TRUNC('MONTH', sf.DATE_ACTUAL)::DATE
JOIN CUSTOMERS_SCHEMA<% schema_suffix %>.CUSTOMERS c ON c.CUSTOMER_ID = sf.CUSTOMER_ID
LEFT JOIN CUSTOMERS_SCHEMA<% schema_suffix %>.CUSTOMER_SEGMENTS cs ON cs.SEGMENT_NAME = c.CUSTOMER_TYPE
GROUP BY month_date, dd.DATE_ID, sf.CUSTOMER_ID, cs.SEGMENT_ID;

-- =====================================================
-- Summary
-- =====================================================
-- Show loading summary
SELECT 'Customer Segments' as TABLE_NAME, COUNT(*) as RECORD_COUNT FROM CUSTOMERS_SCHEMA<% schema_suffix %>.CUSTOMER_SEGMENTS
UNION ALL
SELECT 'Categories', COUNT(*) FROM PRODUCTS_SCHEMA<% schema_suffix %>.CATEGORIES  
UNION ALL
SELECT 'Suppliers', COUNT(*) FROM PRODUCTS_SCHEMA<% schema_suffix %>.SUPPLIERS
UNION ALL
SELECT 'Products', COUNT(*) FROM PRODUCTS_SCHEMA<% schema_suffix %>.PRODUCTS
UNION ALL
SELECT 'Customers', COUNT(*) FROM CUSTOMERS_SCHEMA<% schema_suffix %>.CUSTOMERS
UNION ALL
SELECT 'Customer Addresses', COUNT(*) FROM CUSTOMERS_SCHEMA<% schema_suffix %>.CUSTOMER_ADDRESSES
UNION ALL
SELECT 'Departments', COUNT(*) FROM HR_SCHEMA<% schema_suffix %>.DEPARTMENTS
UNION ALL
SELECT 'Positions', COUNT(*) FROM HR_SCHEMA<% schema_suffix %>.POSITIONS
UNION ALL
SELECT 'Sales Fact', COUNT(*) FROM ANALYTICS_SCHEMA<% schema_suffix %>.SALES_FACT
UNION ALL
SELECT 'Customer Analytics', COUNT(*) FROM ANALYTICS_SCHEMA<% schema_suffix %>.CUSTOMER_ANALYTICS_FACT
ORDER BY TABLE_NAME;