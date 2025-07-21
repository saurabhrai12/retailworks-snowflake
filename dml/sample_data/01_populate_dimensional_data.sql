-- =====================================================
-- Sample Data Population - Dimensional Tables
-- Description: Populates date dimension, customer segments, and categories
-- Environment: Dev
-- =====================================================

USE DATABASE <% database_name %>;
USE SCHEMA ANALYTICS_SCHEMA<% schema_suffix %>;

-- Populate Date Dimension (2023-2025)
INSERT INTO DATE_DIM (
    DATE_ACTUAL, EPOCH, DAY_SUFFIX, DAY_NAME, DAY_OF_WEEK, DAY_OF_MONTH, DAY_OF_QUARTER, DAY_OF_YEAR,
    WEEK_OF_MONTH, WEEK_OF_YEAR, WEEK_OF_YEAR_ISO, MONTH_ACTUAL, MONTH_NAME, MONTH_NAME_ABBREVIATED,
    QUARTER_ACTUAL, QUARTER_NAME, YEAR_ACTUAL, FIRST_DAY_OF_WEEK, LAST_DAY_OF_WEEK, FIRST_DAY_OF_MONTH,
    LAST_DAY_OF_MONTH, FIRST_DAY_OF_QUARTER, LAST_DAY_OF_QUARTER, FIRST_DAY_OF_YEAR, LAST_DAY_OF_YEAR,
    IS_WEEKEND, IS_WEEKDAY, IS_HOLIDAY, HOLIDAY_NAME
)
WITH date_range AS (
    SELECT DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY 1) - 1, '2023-01-01'::DATE) AS date_actual
    FROM TABLE(GENERATOR(ROWCOUNT => 1095)) -- 3 years
),
date_details AS (
    SELECT 
        date_actual,
        EXTRACT(EPOCH FROM date_actual) AS epoch,
        CASE 
            WHEN EXTRACT(DAY FROM date_actual) IN (1, 21, 31) THEN 'st'
            WHEN EXTRACT(DAY FROM date_actual) IN (2, 22) THEN 'nd'
            WHEN EXTRACT(DAY FROM date_actual) IN (3, 23) THEN 'rd'
            ELSE 'th'
        END AS day_suffix,
        DAYNAME(date_actual) AS day_name,
        EXTRACT(DOW FROM date_actual) + 1 AS day_of_week,
        EXTRACT(DAY FROM date_actual) AS day_of_month,
        EXTRACT(DOY FROM date_actual) - EXTRACT(DOY FROM DATE_TRUNC('QUARTER', date_actual)) + 1 AS day_of_quarter,
        EXTRACT(DOY FROM date_actual) AS day_of_year,
        CEIL(EXTRACT(DAY FROM date_actual) / 7.0) AS week_of_month,
        EXTRACT(WEEK FROM date_actual) AS week_of_year,
        EXTRACT(WEEK FROM date_actual) AS week_of_year_iso,
        EXTRACT(MONTH FROM date_actual) AS month_actual,
        MONTHNAME(date_actual) AS month_name,
        LEFT(MONTHNAME(date_actual), 3) AS month_name_abbreviated,
        EXTRACT(QUARTER FROM date_actual) AS quarter_actual,
        'Q' || EXTRACT(QUARTER FROM date_actual) AS quarter_name,
        EXTRACT(YEAR FROM date_actual) AS year_actual,
        DATE_TRUNC('WEEK', date_actual) AS first_day_of_week,
        DATEADD(DAY, 6, DATE_TRUNC('WEEK', date_actual)) AS last_day_of_week,
        DATE_TRUNC('MONTH', date_actual) AS first_day_of_month,
        LAST_DAY(date_actual) AS last_day_of_month,
        DATE_TRUNC('QUARTER', date_actual) AS first_day_of_quarter,
        LAST_DAY(DATE_TRUNC('QUARTER', date_actual), 'QUARTER') AS last_day_of_quarter,
        DATE_TRUNC('YEAR', date_actual) AS first_day_of_year,
        LAST_DAY(DATE_TRUNC('YEAR', date_actual), 'YEAR') AS last_day_of_year,
        CASE WHEN EXTRACT(DOW FROM date_actual) IN (5, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN EXTRACT(DOW FROM date_actual) NOT IN (5, 6) THEN TRUE ELSE FALSE END AS is_weekday,
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
        END AS holiday_name
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
INSERT INTO CATEGORIES (CATEGORY_NAME, DESCRIPTION, PARENT_CATEGORY_ID) VALUES
('Laptops', 'Portable computers', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Computers')),
('Desktops', 'Desktop computers', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Computers')),
('Monitors', 'Computer monitors and displays', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Electronics')),
('Keyboards', 'Computer keyboards', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Electronics')),
('Mice', 'Computer mice and pointing devices', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Electronics')),
('Office Chairs', 'Ergonomic office seating', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Furniture')),
('Desks', 'Office desks and workstations', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Furniture')),
('Productivity Software', 'Office productivity applications', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Software')),
('Stationery', 'Pens, paper, and writing supplies', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Office Supplies'));

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
INSERT INTO POSITIONS (POSITION_TITLE, POSITION_CODE, DEPARTMENT_ID, JOB_LEVEL, MIN_SALARY, MAX_SALARY, DESCRIPTION, STATUS) VALUES
-- Sales Positions
('Sales Representative', 'SALES_REP', (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'SALES'), 2, 50000, 80000, 'Field sales representative', 'ACTIVE'),
('Senior Sales Representative', 'SR_SALES_REP', (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'SALES'), 3, 70000, 100000, 'Senior field sales representative', 'ACTIVE'),
('Sales Manager', 'SALES_MGR', (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'SALES'), 4, 90000, 130000, 'Regional sales manager', 'ACTIVE'),
('Sales Director', 'SALES_DIR', (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'SALES'), 5, 120000, 180000, 'Sales organization director', 'ACTIVE'),

-- Marketing Positions
('Marketing Specialist', 'MKT_SPEC', (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'MKT'), 2, 55000, 75000, 'Marketing campaigns specialist', 'ACTIVE'),
('Marketing Manager', 'MKT_MGR', (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'MKT'), 4, 80000, 120000, 'Marketing team manager', 'ACTIVE'),

-- Engineering Positions
('Software Engineer', 'SW_ENG', (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'ENG'), 3, 85000, 120000, 'Software development engineer', 'ACTIVE'),
('Senior Software Engineer', 'SR_SW_ENG', (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'ENG'), 4, 110000, 150000, 'Senior software development engineer', 'ACTIVE'),
('Engineering Manager', 'ENG_MGR', (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'ENG'), 5, 140000, 190000, 'Engineering team manager', 'ACTIVE'),

-- HR Positions
('HR Specialist', 'HR_SPEC', (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'HR'), 2, 50000, 70000, 'Human resources specialist', 'ACTIVE'),
('HR Manager', 'HR_MGR', (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'HR'), 4, 75000, 110000, 'Human resources manager', 'ACTIVE'),

-- Finance Positions
('Financial Analyst', 'FIN_ANALYST', (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'FIN'), 3, 65000, 90000, 'Financial analysis and reporting', 'ACTIVE'),
('Finance Manager', 'FIN_MGR', (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'FIN'), 4, 85000, 125000, 'Finance team manager', 'ACTIVE'),

-- Operations Positions
('Operations Specialist', 'OPS_SPEC', (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'OPS'), 2, 48000, 68000, 'Operations and logistics specialist', 'ACTIVE'),
('Operations Manager', 'OPS_MGR', (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'OPS'), 4, 75000, 110000, 'Operations team manager', 'ACTIVE'),

-- Customer Support Positions
('Support Representative', 'SUPPORT_REP', (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'SUPPORT'), 2, 40000, 55000, 'Customer support representative', 'ACTIVE'),
('Support Manager', 'SUPPORT_MGR', (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'SUPPORT'), 4, 65000, 90000, 'Customer support manager', 'ACTIVE');

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