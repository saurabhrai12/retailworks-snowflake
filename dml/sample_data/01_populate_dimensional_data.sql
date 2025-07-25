-- =====================================================
-- Sample Data Population - Dimensional Tables
-- Description: Populates date dimension, customer segments, and categories
-- Environment: Dev
-- =====================================================

USE DATABASE <% database_name %>;
USE SCHEMA ANALYTICS_SCHEMA<% schema_suffix %>;

-- Populate Date Dimension (2023-2025)
INSERT INTO DATE_DIM (
    DATE_KEY, DATE_ACTUAL, DAY_OF_WEEK, DAY_OF_WEEK_NAME, DAY_OF_MONTH, DAY_OF_YEAR,
    WEEK_OF_YEAR, MONTH_NUMBER, MONTH_NAME, MONTH_ABBR, QUARTER_NUMBER, QUARTER_NAME,
    YEAR_NUMBER, IS_WEEKEND, IS_HOLIDAY, HOLIDAY_NAME, SEASON
)
WITH date_range AS (
    SELECT DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY 1) - 1, '2023-01-01'::DATE) AS date_actual
    FROM TABLE(GENERATOR(ROWCOUNT => 1095)) -- 3 years
),
date_details AS (
    SELECT 
        TO_NUMBER(TO_CHAR(date_actual, 'YYYYMMDD')) AS date_key,
        date_actual,
        EXTRACT(DOW FROM date_actual) + 1 AS day_of_week,
        DAYNAME(date_actual) AS day_of_week_name,
        EXTRACT(DAY FROM date_actual) AS day_of_month,
        EXTRACT(DOY FROM date_actual) AS day_of_year,
        EXTRACT(WEEK FROM date_actual) AS week_of_year,
        EXTRACT(MONTH FROM date_actual) AS month_number,
        MONTHNAME(date_actual) AS month_name,
        LEFT(MONTHNAME(date_actual), 3) AS month_abbr,
        EXTRACT(QUARTER FROM date_actual) AS quarter_number,
        'Q' || EXTRACT(QUARTER FROM date_actual) AS quarter_name,
        EXTRACT(YEAR FROM date_actual) AS year_number,
        CASE WHEN EXTRACT(DOW FROM date_actual) IN (5, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE 
            WHEN date_actual IN ('2023-01-01', '2023-07-04', '2023-11-23', '2023-12-25',
                                 '2024-01-01', '2024-07-04', '2024-11-28', '2024-12-25',
                                 '2025-01-01', '2025-07-04', '2025-11-27', '2025-12-25') 
            THEN TRUE ELSE FALSE END AS is_holiday,
        CASE 
            WHEN date_actual IN ('2023-01-01', '2024-01-01', '2025-01-01') THEN 'New Year Day'
            WHEN date_actual IN ('2023-07-04', '2024-07-04', '2025-07-04') THEN 'Independence Day'
            WHEN date_actual IN ('2023-11-23', '2024-11-28', '2025-11-27') THEN 'Thanksgiving'
            WHEN date_actual IN ('2023-12-25', '2024-12-25', '2025-12-25') THEN 'Christmas'
            ELSE NULL
        END AS holiday_name,
        CASE 
            WHEN EXTRACT(MONTH FROM date_actual) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM date_actual) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM date_actual) IN (6, 7, 8) THEN 'Summer'
            WHEN EXTRACT(MONTH FROM date_actual) IN (9, 10, 11) THEN 'Fall'
        END AS season
    FROM date_range
)
SELECT * FROM date_details;

-- Switch to Customer Schema for customer segments
USE SCHEMA CUSTOMERS_SCHEMA<% schema_suffix %>;

-- Populate Customer Segments
INSERT INTO CUSTOMER_SEGMENTS (SEGMENT_NAME, DESCRIPTION, MIN_ANNUAL_REVENUE, MAX_ANNUAL_REVENUE, DISCOUNT_RATE) VALUES
('Enterprise', 'Large corporations with high volume purchases', 1000000, NULL, 0.15),
('Corporate', 'Mid-size companies with regular bulk orders', 100000, 999999, 0.10),
('Small Business', 'Small businesses and startups', 10000, 99999, 0.05),
('Individual', 'Individual consumers and freelancers', 0, 9999, 0.02);

-- Switch to Products Schema for categories and suppliers
USE SCHEMA PRODUCTS_SCHEMA<% schema_suffix %>;

-- Populate Categories (hierarchical structure)
INSERT INTO CATEGORIES (CATEGORY_NAME, DESCRIPTION, PARENT_CATEGORY_ID) VALUES
('Electronics', 'Electronic devices and accessories', NULL),
('Computers', 'Desktop and laptop computers', NULL),
('Software', 'Software applications and licenses', NULL),
('Office Supplies', 'General office and business supplies', NULL),
('Furniture', 'Office and home furniture', NULL);

-- Add subcategories
INSERT INTO CATEGORIES (CATEGORY_NAME, DESCRIPTION, PARENT_CATEGORY_ID) 
SELECT 'Laptops', 'Portable computers', CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Computers';

INSERT INTO CATEGORIES (CATEGORY_NAME, DESCRIPTION, PARENT_CATEGORY_ID) 
SELECT 'Desktops', 'Desktop computers', CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Computers';

INSERT INTO CATEGORIES (CATEGORY_NAME, DESCRIPTION, PARENT_CATEGORY_ID) 
SELECT 'Monitors', 'Computer monitors and displays', CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Electronics';

INSERT INTO CATEGORIES (CATEGORY_NAME, DESCRIPTION, PARENT_CATEGORY_ID) 
SELECT 'Keyboards', 'Computer keyboards', CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Electronics';

INSERT INTO CATEGORIES (CATEGORY_NAME, DESCRIPTION, PARENT_CATEGORY_ID) 
SELECT 'Mice', 'Computer mice and pointing devices', CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Electronics';

INSERT INTO CATEGORIES (CATEGORY_NAME, DESCRIPTION, PARENT_CATEGORY_ID) 
SELECT 'Office Chairs', 'Ergonomic office seating', CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Furniture';

INSERT INTO CATEGORIES (CATEGORY_NAME, DESCRIPTION, PARENT_CATEGORY_ID) 
SELECT 'Desks', 'Office desks and workstations', CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Furniture';

INSERT INTO CATEGORIES (CATEGORY_NAME, DESCRIPTION, PARENT_CATEGORY_ID) 
SELECT 'Productivity Software', 'Office productivity applications', CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Software';

INSERT INTO CATEGORIES (CATEGORY_NAME, DESCRIPTION, PARENT_CATEGORY_ID) 
SELECT 'Stationery', 'Pens, paper, and writing supplies', CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Office Supplies';

-- Populate Suppliers
INSERT INTO SUPPLIERS (
    SUPPLIER_NAME, CONTACT_NAME, CONTACT_TITLE, ADDRESS, CITY, REGION, POSTAL_CODE, COUNTRY,
    PHONE, EMAIL, WEBSITE, STATUS, RATING
) VALUES
('TechSource Inc.', 'John Smith', 'Sales Manager', '123 Tech Street', 'San Francisco', 'CA', '94105', 'USA', 
 '+1-555-0101', 'john.smith@techsource.com', 'www.techsource.com', 'ACTIVE', 4.5),
('Global Electronics', 'Maria Garcia', 'Account Executive', '456 Electronic Ave', 'Los Angeles', 'CA', '90210', 'USA',
 '+1-555-0102', 'maria.garcia@globalelec.com', 'www.globalelectronics.com', 'ACTIVE', 4.2),
('Office Solutions Ltd', 'David Johnson', 'Business Development', '789 Business Blvd', 'Chicago', 'IL', '60601', 'USA',
 '+1-555-0103', 'david.johnson@officesol.com', 'www.officesolutions.com', 'ACTIVE', 4.7),
('Furniture World', 'Sarah Wilson', 'Sales Director', '321 Furniture Row', 'Dallas', 'TX', '75201', 'USA',
 '+1-555-0104', 'sarah.wilson@furnitureworld.com', 'www.furnitureworld.com', 'ACTIVE', 4.3),
('Software Paradise', 'Michael Brown', 'Partner Manager', '654 Software Lane', 'Seattle', 'WA', '98101', 'USA',
 '+1-555-0105', 'michael.brown@softwareparadise.com', 'www.softwareparadise.com', 'ACTIVE', 4.6);

-- Switch to HR Schema for departments and positions
USE SCHEMA HR_SCHEMA<% schema_suffix %>;

-- Populate Departments
INSERT INTO DEPARTMENTS (DEPARTMENT_NAME, DEPARTMENT_CODE, DESCRIPTION, BUDGET, LOCATION, PHONE, EMAIL) VALUES
('Sales', 'SALES', 'Sales and customer acquisition', 500000, 'New York Office', '+1-555-1001', 'sales@retailworks.com'),
('Marketing', 'MKT', 'Marketing and brand management', 300000, 'Los Angeles Office', '+1-555-1002', 'marketing@retailworks.com'),
('Engineering', 'ENG', 'Product development and engineering', 800000, 'San Francisco Office', '+1-555-1003', 'engineering@retailworks.com'),
('Human Resources', 'HR', 'Human resources and talent management', 200000, 'Chicago Office', '+1-555-1004', 'hr@retailworks.com'),
('Finance', 'FIN', 'Financial planning and accounting', 250000, 'New York Office', '+1-555-1005', 'finance@retailworks.com'),
('Operations', 'OPS', 'Operations and logistics', 400000, 'Dallas Office', '+1-555-1006', 'operations@retailworks.com'),
('Customer Support', 'SUPPORT', 'Customer service and technical support', 180000, 'Phoenix Office', '+1-555-1007', 'support@retailworks.com');

-- Populate Positions
-- Sales Positions
INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
SELECT 'Sales Representative', 'SALES_REP', DEPARTMENT_ID, 2, 50000, 80000, 'Field sales representative', 'ACTIVE'
FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'SALES';

INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
SELECT 'Senior Sales Representative', 'SR_SALES_REP', DEPARTMENT_ID, 3, 70000, 100000, 'Senior field sales representative', 'ACTIVE'
FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'SALES';

INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
SELECT 'Sales Manager', 'SALES_MGR', DEPARTMENT_ID, 4, 90000, 130000, 'Regional sales manager', 'ACTIVE'
FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'SALES';

INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
SELECT 'Sales Director', 'SALES_DIR', DEPARTMENT_ID, 5, 120000, 180000, 'Sales organization director', 'ACTIVE'
FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'SALES';

-- Marketing Positions
INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
SELECT 'Marketing Specialist', 'MKT_SPEC', DEPARTMENT_ID, 2, 55000, 75000, 'Marketing campaigns specialist', 'ACTIVE'
FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'MKT';

INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
SELECT 'Marketing Manager', 'MKT_MGR', DEPARTMENT_ID, 4, 80000, 120000, 'Marketing team manager', 'ACTIVE'
FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'MKT';

-- Engineering Positions
INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
SELECT 'Software Engineer', 'SW_ENG', DEPARTMENT_ID, 3, 85000, 120000, 'Software development engineer', 'ACTIVE'
FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'ENG';

INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
SELECT 'Senior Software Engineer', 'SR_SW_ENG', DEPARTMENT_ID, 4, 110000, 150000, 'Senior software development engineer', 'ACTIVE'
FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'ENG';

INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
SELECT 'Engineering Manager', 'ENG_MGR', DEPARTMENT_ID, 5, 140000, 190000, 'Engineering team manager', 'ACTIVE'
FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'ENG';

-- HR Positions
INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
SELECT 'HR Specialist', 'HR_SPEC', DEPARTMENT_ID, 2, 50000, 70000, 'Human resources specialist', 'ACTIVE'
FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'HR';

INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
SELECT 'HR Manager', 'HR_MGR', DEPARTMENT_ID, 4, 75000, 110000, 'Human resources manager', 'ACTIVE'
FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'HR';

-- Finance Positions
INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
SELECT 'Financial Analyst', 'FIN_ANALYST', DEPARTMENT_ID, 3, 65000, 90000, 'Financial analysis and reporting', 'ACTIVE'
FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'FIN';

INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
SELECT 'Finance Manager', 'FIN_MGR', DEPARTMENT_ID, 4, 85000, 125000, 'Finance team manager', 'ACTIVE'
FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'FIN';

-- Operations Positions
INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
SELECT 'Operations Specialist', 'OPS_SPEC', DEPARTMENT_ID, 2, 48000, 68000, 'Operations and logistics specialist', 'ACTIVE'
FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'OPS';

INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
SELECT 'Operations Manager', 'OPS_MGR', DEPARTMENT_ID, 4, 75000, 110000, 'Operations team manager', 'ACTIVE'
FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'OPS';

-- Customer Support Positions
INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
SELECT 'Support Representative', 'SUPPORT_REP', DEPARTMENT_ID, 2, 40000, 55000, 'Customer support representative', 'ACTIVE'
FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'SUPPORT';

INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS)
SELECT 'Support Manager', 'SUPPORT_MGR', DEPARTMENT_ID, 4, 65000, 90000, 'Customer support manager', 'ACTIVE'
FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'SUPPORT';

-- Switch to Sales Schema for territories
USE SCHEMA SALES_SCHEMA<% schema_suffix %>;

-- Populate Sales Territories
INSERT INTO SALES_TERRITORIES (TERRITORY_NAME, TERRITORY_CODE, REGION, COUNTRY) VALUES
('Northeast US', 'NE_US', 'North America', 'USA'),
('Southeast US', 'SE_US', 'North America', 'USA'),
('Midwest US', 'MW_US', 'North America', 'USA'),
('Southwest US', 'SW_US', 'North America', 'USA'),
('Northwest US', 'NW_US', 'North America', 'USA'),
('West Coast US', 'WC_US', 'North America', 'USA'),
('Canada East', 'CA_EAST', 'North America', 'Canada'),
('Canada West', 'CA_WEST', 'North America', 'Canada'),
('International', 'INTL', 'International', 'Various');