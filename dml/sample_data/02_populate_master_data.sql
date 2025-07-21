-- =====================================================
-- Sample Data Population - Master Data
-- Description: Populates employees, customers, and products
-- Environment: Dev
-- =====================================================

USE DATABASE <% database_name %>;

-- Switch to HR Schema for employees
USE SCHEMA HR_SCHEMA<% schema_suffix %>;

-- Populate Employees
INSERT INTO EMPLOYEES (
    EMPLOYEE_NUMBER, FIRST_NAME, LAST_NAME, EMAIL, PHONE, MOBILE, BIRTH_DATE, GENDER, 
    MARITAL_STATUS, ADDRESS, CITY, STATE_PROVINCE, POSTAL_CODE, COUNTRY, HIRE_DATE,
    POSITION_ID, DEPARTMENT_ID, EMPLOYMENT_TYPE, STATUS, EMERGENCY_CONTACT_NAME, 
    EMERGENCY_CONTACT_PHONE, EMERGENCY_CONTACT_RELATIONSHIP
) VALUES
-- Sales Team
('EMP001', 'Alice', 'Johnson', 'alice.johnson@retailworks.com', '+1-555-2001', '+1-555-3001', '1985-03-15', 'F', 'Married', '123 Oak St', 'New York', 'NY', '10001', 'USA', '2022-01-15', 
 (SELECT POSITION_ID FROM POSITIONS WHERE POSITION_CODE = 'SALES_DIR'), (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'SALES'), 'FULL_TIME', 'ACTIVE', 'Bob Johnson', '+1-555-4001', 'Spouse'),

('EMP002', 'Robert', 'Smith', 'robert.smith@retailworks.com', '+1-555-2002', '+1-555-3002', '1990-07-22', 'M', 'Single', '456 Pine Ave', 'New York', 'NY', '10002', 'USA', '2022-03-01',
 (SELECT POSITION_ID FROM POSITIONS WHERE POSITION_CODE = 'SALES_MGR'), (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'SALES'), 'FULL_TIME', 'ACTIVE', 'Mary Smith', '+1-555-4002', 'Mother'),

('EMP003', 'Jennifer', 'Davis', 'jennifer.davis@retailworks.com', '+1-555-2003', '+1-555-3003', '1992-11-08', 'F', 'Single', '789 Elm St', 'Boston', 'MA', '02101', 'USA', '2022-06-15',
 (SELECT POSITION_ID FROM POSITIONS WHERE POSITION_CODE = 'SR_SALES_REP'), (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'SALES'), 'FULL_TIME', 'ACTIVE', 'James Davis', '+1-555-4003', 'Father'),

('EMP004', 'Michael', 'Wilson', 'michael.wilson@retailworks.com', '+1-555-2004', '+1-555-3004', '1988-05-12', 'M', 'Married', '321 Maple Dr', 'Chicago', 'IL', '60601', 'USA', '2021-09-01',
 (SELECT POSITION_ID FROM POSITIONS WHERE POSITION_CODE = 'SALES_REP'), (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'SALES'), 'FULL_TIME', 'ACTIVE', 'Lisa Wilson', '+1-555-4004', 'Spouse'),

('EMP005', 'Sarah', 'Brown', 'sarah.brown@retailworks.com', '+1-555-2005', '+1-555-3005', '1987-12-03', 'F', 'Married', '654 Cedar Ln', 'Dallas', 'TX', '75201', 'USA', '2022-02-14',
 (SELECT POSITION_ID FROM POSITIONS WHERE POSITION_CODE = 'SALES_REP'), (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'SALES'), 'FULL_TIME', 'ACTIVE', 'Tom Brown', '+1-555-4005', 'Spouse'),

-- Engineering Team
('EMP006', 'David', 'Garcia', 'david.garcia@retailworks.com', '+1-555-2006', '+1-555-3006', '1983-09-18', 'M', 'Married', '987 Redwood Ave', 'San Francisco', 'CA', '94105', 'USA', '2021-05-10',
 (SELECT POSITION_ID FROM POSITIONS WHERE POSITION_CODE = 'ENG_MGR'), (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'ENG'), 'FULL_TIME', 'ACTIVE', 'Maria Garcia', '+1-555-4006', 'Spouse'),

('EMP007', 'Emily', 'Martinez', 'emily.martinez@retailworks.com', '+1-555-2007', '+1-555-3007', '1991-04-25', 'F', 'Single', '147 Birch St', 'San Francisco', 'CA', '94106', 'USA', '2022-01-03',
 (SELECT POSITION_ID FROM POSITIONS WHERE POSITION_CODE = 'SR_SW_ENG'), (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'ENG'), 'FULL_TIME', 'ACTIVE', 'Carlos Martinez', '+1-555-4007', 'Brother'),

('EMP008', 'James', 'Anderson', 'james.anderson@retailworks.com', '+1-555-2008', '+1-555-3008', '1989-01-14', 'M', 'Single', '258 Willow Way', 'Seattle', 'WA', '98101', 'USA', '2021-11-22',
 (SELECT POSITION_ID FROM POSITIONS WHERE POSITION_CODE = 'SW_ENG'), (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'ENG'), 'FULL_TIME', 'ACTIVE', 'Helen Anderson', '+1-555-4008', 'Mother'),

-- Marketing Team
('EMP009', 'Lisa', 'Taylor', 'lisa.taylor@retailworks.com', '+1-555-2009', '+1-555-3009', '1986-08-07', 'F', 'Married', '369 Spruce Blvd', 'Los Angeles', 'CA', '90210', 'USA', '2021-12-06',
 (SELECT POSITION_ID FROM POSITIONS WHERE POSITION_CODE = 'MKT_MGR'), (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'MKT'), 'FULL_TIME', 'ACTIVE', 'Mark Taylor', '+1-555-4009', 'Spouse'),

('EMP010', 'Christopher', 'Lee', 'christopher.lee@retailworks.com', '+1-555-2010', '+1-555-3010', '1993-06-30', 'M', 'Single', '741 Palm Ave', 'Los Angeles', 'CA', '90211', 'USA', '2022-04-18',
 (SELECT POSITION_ID FROM POSITIONS WHERE POSITION_CODE = 'MKT_SPEC'), (SELECT DEPARTMENT_ID FROM DEPARTMENTS WHERE DEPARTMENT_CODE = 'MKT'), 'FULL_TIME', 'ACTIVE', 'Jennifer Lee', '+1-555-4010', 'Sister');

-- Update manager relationships
UPDATE EMPLOYEES SET MANAGER_ID = (SELECT EMPLOYEE_ID FROM EMPLOYEES WHERE EMPLOYEE_NUMBER = 'EMP002') WHERE EMPLOYEE_NUMBER IN ('EMP003', 'EMP004', 'EMP005');
UPDATE EMPLOYEES SET MANAGER_ID = (SELECT EMPLOYEE_ID FROM EMPLOYEES WHERE EMPLOYEE_NUMBER = 'EMP006') WHERE EMPLOYEE_NUMBER IN ('EMP007', 'EMP008');
UPDATE EMPLOYEES SET MANAGER_ID = (SELECT EMPLOYEE_ID FROM EMPLOYEES WHERE EMPLOYEE_NUMBER = 'EMP009') WHERE EMPLOYEE_NUMBER = 'EMP010';

-- Switch to Sales Schema for sales reps
USE SCHEMA SALES_SCHEMA<% schema_suffix %>;

-- Populate Sales Reps (linking to employees)
INSERT INTO SALES_REPS (EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, TERRITORY_ID, HIRE_DATE, COMMISSION_RATE, STATUS) 
SELECT 
    e.EMPLOYEE_ID, e.FIRST_NAME, e.LAST_NAME, e.EMAIL, e.PHONE, 
    CASE 
        WHEN e.EMPLOYEE_NUMBER = 'EMP003' THEN (SELECT TERRITORY_ID FROM SALES_TERRITORIES WHERE TERRITORY_CODE = 'NE_US')
        WHEN e.EMPLOYEE_NUMBER = 'EMP004' THEN (SELECT TERRITORY_ID FROM SALES_TERRITORIES WHERE TERRITORY_CODE = 'MW_US')
        WHEN e.EMPLOYEE_NUMBER = 'EMP005' THEN (SELECT TERRITORY_ID FROM SALES_TERRITORIES WHERE TERRITORY_CODE = 'SW_US')
        ELSE (SELECT TERRITORY_ID FROM SALES_TERRITORIES WHERE TERRITORY_CODE = 'NE_US')
    END,
    e.HIRE_DATE, 
    CASE 
        WHEN p.POSITION_CODE = 'SR_SALES_REP' THEN 0.08
        WHEN p.POSITION_CODE = 'SALES_REP' THEN 0.05
        ELSE 0.10
    END,
    'ACTIVE'
FROM <% database_name %>.HR_SCHEMA<% schema_suffix %>.EMPLOYEES e
JOIN <% database_name %>.HR_SCHEMA<% schema_suffix %>.POSITIONS p ON e.POSITION_ID = p.POSITION_ID
JOIN <% database_name %>.HR_SCHEMA<% schema_suffix %>.DEPARTMENTS d ON e.DEPARTMENT_ID = d.DEPARTMENT_ID
WHERE d.DEPARTMENT_CODE = 'SALES';

-- Switch to Customers Schema
USE SCHEMA CUSTOMERS_SCHEMA<% schema_suffix %>;

-- Populate Addresses first
INSERT INTO ADDRESSES (ADDRESS_LINE_1, ADDRESS_LINE_2, CITY, STATE_PROVINCE, POSTAL_CODE, COUNTRY, ADDRESS_TYPE, LATITUDE, LONGITUDE) VALUES
('123 Business Plaza', 'Suite 100', 'New York', 'NY', '10001', 'USA', 'BILLING', 40.7128, -74.0060),
('456 Corporate Center', NULL, 'Los Angeles', 'CA', '90210', 'USA', 'BILLING', 34.0522, -118.2437),
('789 Enterprise Way', 'Floor 5', 'Chicago', 'IL', '60601', 'USA', 'BILLING', 41.8781, -87.6298),
('321 Innovation Drive', NULL, 'Austin', 'TX', '73301', 'USA', 'BILLING', 30.2672, -97.7431),
('654 Technology Blvd', 'Building A', 'Seattle', 'WA', '98101', 'USA', 'BILLING', 47.6062, -122.3321),
('987 Startup Street', NULL, 'San Francisco', 'CA', '94105', 'USA', 'BILLING', 37.7749, -122.4194),
('147 Commerce Road', 'Unit 200', 'Boston', 'MA', '02101', 'USA', 'BILLING', 42.3601, -71.0589),
('258 Industry Lane', NULL, 'Atlanta', 'GA', '30301', 'USA', 'BILLING', 33.7490, -84.3880),
('369 Business Park', 'Suite 300', 'Denver', 'CO', '80201', 'USA', 'BILLING', 39.7392, -104.9903),
('741 Corporate Hill', NULL, 'Phoenix', 'AZ', '85001', 'USA', 'BILLING', 33.4484, -112.0740),
-- Individual addresses
('111 Residential St', 'Apt 1A', 'Miami', 'FL', '33101', 'USA', 'BILLING', 25.7617, -80.1918),
('222 Home Avenue', NULL, 'Portland', 'OR', '97201', 'USA', 'BILLING', 45.5152, -122.6784),
('333 Family Lane', 'Unit B', 'Nashville', 'TN', '37201', 'USA', 'BILLING', 36.1627, -86.7816),
('444 Personal Dr', NULL, 'Salt Lake City', 'UT', '84101', 'USA', 'BILLING', 40.7608, -111.8910),
('555 Individual Way', 'Apt 2C', 'Minneapolis', 'MN', '55401', 'USA', 'BILLING', 44.9778, -93.2650);

-- Populate Customers
INSERT INTO CUSTOMERS (
    CUSTOMER_NUMBER, CUSTOMER_TYPE, COMPANY_NAME, FIRST_NAME, LAST_NAME, TITLE, EMAIL, PHONE, MOBILE,
    BIRTH_DATE, GENDER, ANNUAL_INCOME, SEGMENT_ID, BILLING_ADDRESS_ID, SHIPPING_ADDRESS_ID,
    REGISTRATION_DATE, CREDIT_LIMIT, STATUS, MARKETING_OPT_IN
) VALUES
-- Enterprise Customers
('CUST001', 'CORPORATE', 'TechCorp Solutions', 'John', 'Anderson', 'CEO', 'john.anderson@techcorp.com', '+1-555-1001', '+1-555-2001', 
 '1975-05-15', 'M', 2500000, (SELECT SEGMENT_ID FROM CUSTOMER_SEGMENTS WHERE SEGMENT_NAME = 'Enterprise'), 1, 1, '2022-01-10', 500000, 'ACTIVE', TRUE),

('CUST002', 'CORPORATE', 'Global Enterprises Inc', 'Maria', 'Rodriguez', 'CTO', 'maria.rodriguez@globalent.com', '+1-555-1002', '+1-555-2002',
 '1978-09-22', 'F', 1800000, (SELECT SEGMENT_ID FROM CUSTOMER_SEGMENTS WHERE SEGMENT_NAME = 'Enterprise'), 2, 2, '2022-02-15', 400000, 'ACTIVE', TRUE),

('CUST003', 'CORPORATE', 'Innovation Systems LLC', 'Robert', 'Chen', 'VP Technology', 'robert.chen@innovsys.com', '+1-555-1003', '+1-555-2003',
 '1980-12-08', 'M', 1200000, (SELECT SEGMENT_ID FROM CUSTOMER_SEGMENTS WHERE SEGMENT_NAME = 'Enterprise'), 3, 3, '2021-11-20', 300000, 'ACTIVE', TRUE),

-- Corporate Customers  
('CUST004', 'CORPORATE', 'MidSize Tech Co', 'Sarah', 'Williams', 'IT Director', 'sarah.williams@midsizetech.com', '+1-555-1004', '+1-555-2004',
 '1982-03-30', 'F', 450000, (SELECT SEGMENT_ID FROM CUSTOMER_SEGMENTS WHERE SEGMENT_NAME = 'Corporate'), 4, 4, '2022-04-05', 100000, 'ACTIVE', TRUE),

('CUST005', 'CORPORATE', 'Business Solutions Ltd', 'Michael', 'Thompson', 'Operations Manager', 'michael.thompson@bizsol.com', '+1-555-1005', '+1-555-2005',
 '1979-07-18', 'M', 320000, (SELECT SEGMENT_ID FROM CUSTOMER_SEGMENTS WHERE SEGMENT_NAME = 'Corporate'), 5, 5, '2022-03-12', 75000, 'ACTIVE', TRUE),

('CUST006', 'CORPORATE', 'Digital Dynamics', 'Jennifer', 'Lee', 'IT Manager', 'jennifer.lee@digitaldyn.com', '+1-555-1006', '+1-555-2006',
 '1984-11-05', 'F', 280000, (SELECT SEGMENT_ID FROM CUSTOMER_SEGMENTS WHERE SEGMENT_NAME = 'Corporate'), 6, 6, '2021-12-08', 60000, 'ACTIVE', TRUE),

-- Small Business Customers
('CUST007', 'CORPORATE', 'StartupTech', 'David', 'Kim', 'Founder', 'david.kim@startuptech.com', '+1-555-1007', '+1-555-2007',
 '1990-02-14', 'M', 85000, (SELECT SEGMENT_ID FROM CUSTOMER_SEGMENTS WHERE SEGMENT_NAME = 'Small Business'), 7, 7, '2022-05-20', 25000, 'ACTIVE', TRUE),

('CUST008', 'CORPORATE', 'Local Business Co', 'Lisa', 'Brown', 'Owner', 'lisa.brown@localbiz.com', '+1-555-1008', '+1-555-2008',
 '1986-08-27', 'F', 65000, (SELECT SEGMENT_ID FROM CUSTOMER_SEGMENTS WHERE SEGMENT_NAME = 'Small Business'), 8, 8, '2022-06-15', 20000, 'ACTIVE', TRUE),

('CUST009', 'CORPORATE', 'Consulting Plus', 'James', 'Wilson', 'Principal', 'james.wilson@consultplus.com', '+1-555-1009', '+1-555-2009',
 '1983-04-12', 'M', 72000, (SELECT SEGMENT_ID FROM CUSTOMER_SEGMENTS WHERE SEGMENT_NAME = 'Small Business'), 9, 9, '2021-10-30', 18000, 'ACTIVE', TRUE),

('CUST010', 'CORPORATE', 'Creative Agency', 'Amanda', 'Davis', 'Creative Director', 'amanda.davis@creative.com', '+1-555-1010', '+1-555-2010',
 '1988-06-09', 'F', 58000, (SELECT SEGMENT_ID FROM CUSTOMER_SEGMENTS WHERE SEGMENT_NAME = 'Small Business'), 10, 10, '2022-07-22', 15000, 'ACTIVE', TRUE),

-- Individual Customers
('CUST011', 'INDIVIDUAL', NULL, 'Kevin', 'Martinez', 'Freelancer', 'kevin.martinez@email.com', '+1-555-1011', '+1-555-2011',
 '1992-01-25', 'M', 45000, (SELECT SEGMENT_ID FROM CUSTOMER_SEGMENTS WHERE SEGMENT_NAME = 'Individual'), 11, 11, '2022-08-14', 5000, 'ACTIVE', TRUE),

('CUST012', 'INDIVIDUAL', NULL, 'Rachel', 'Garcia', 'Consultant', 'rachel.garcia@email.com', '+1-555-1012', '+1-555-2012',
 '1989-10-18', 'F', 52000, (SELECT SEGMENT_ID FROM CUSTOMER_SEGMENTS WHERE SEGMENT_NAME = 'Individual'), 12, 12, '2022-09-05', 7500, 'ACTIVE', TRUE),

('CUST013', 'INDIVIDUAL', NULL, 'Daniel', 'Taylor', 'Designer', 'daniel.taylor@email.com', '+1-555-1013', '+1-555-2013',
 '1991-12-03', 'M', 38000, (SELECT SEGMENT_ID FROM CUSTOMER_SEGMENTS WHERE SEGMENT_NAME = 'Individual'), 13, 13, '2021-11-12', 4000, 'ACTIVE', FALSE),

('CUST014', 'INDIVIDUAL', NULL, 'Nicole', 'White', 'Developer', 'nicole.white@email.com', '+1-555-1014', '+1-555-2014',
 '1987-05-29', 'F', 67000, (SELECT SEGMENT_ID FROM CUSTOMER_SEGMENTS WHERE SEGMENT_NAME = 'Individual'), 14, 14, '2022-10-07', 8000, 'ACTIVE', TRUE),

('CUST015', 'INDIVIDUAL', NULL, 'Andrew', 'Johnson', 'Writer', 'andrew.johnson@email.com', '+1-555-1015', '+1-555-2015',
 '1985-09-16', 'M', 42000, (SELECT SEGMENT_ID FROM CUSTOMER_SEGMENTS WHERE SEGMENT_NAME = 'Individual'), 15, 15, '2022-01-28', 3500, 'ACTIVE', TRUE);

-- Link customers to addresses
INSERT INTO CUSTOMER_ADDRESSES (CUSTOMER_ID, ADDRESS_ID, ADDRESS_TYPE, IS_DEFAULT) 
SELECT CUSTOMER_ID, BILLING_ADDRESS_ID, 'BILLING', TRUE FROM CUSTOMERS;

INSERT INTO CUSTOMER_ADDRESSES (CUSTOMER_ID, ADDRESS_ID, ADDRESS_TYPE, IS_DEFAULT) 
SELECT CUSTOMER_ID, SHIPPING_ADDRESS_ID, 'SHIPPING', TRUE FROM CUSTOMERS
WHERE SHIPPING_ADDRESS_ID != BILLING_ADDRESS_ID;

-- Switch to Products Schema
USE SCHEMA PRODUCTS_SCHEMA<% schema_suffix %>;

-- Populate Products
INSERT INTO PRODUCTS (
    PRODUCT_NAME, PRODUCT_NUMBER, CATEGORY_ID, SUPPLIER_ID, DESCRIPTION, COLOR, SIZE, WEIGHT,
    UNIT_PRICE, COST, LIST_PRICE, DISCONTINUED, REORDER_LEVEL, UNITS_IN_STOCK, PRODUCT_LINE, CLASS, STYLE
) VALUES
-- Laptops
('Business Laptop Pro 15"', 'PROD001', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Laptops'), 
 (SELECT SUPPLIER_ID FROM SUPPLIERS WHERE SUPPLIER_NAME = 'TechSource Inc.'), 
 'High-performance business laptop with 15" display', 'Black', '15"', 4.5, 1299.99, 950.00, 1399.99, FALSE, 25, 150, 'Professional', 'Premium', 'Modern'),

('Ultrabook Slim 13"', 'PROD002', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Laptops'),
 (SELECT SUPPLIER_ID FROM SUPPLIERS WHERE SUPPLIER_NAME = 'Global Electronics'),
 'Ultra-portable laptop for professionals', 'Silver', '13"', 2.8, 899.99, 680.00, 999.99, FALSE, 30, 200, 'Professional', 'Standard', 'Sleek'),

('Gaming Laptop Elite', 'PROD003', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Laptops'),
 (SELECT SUPPLIER_ID FROM SUPPLIERS WHERE SUPPLIER_NAME = 'TechSource Inc.'),
 'High-end gaming laptop with advanced graphics', 'Black', '17"', 6.2, 1899.99, 1400.00, 2099.99, FALSE, 15, 75, 'Gaming', 'Premium', 'Aggressive'),

-- Desktops
('Workstation Pro Tower', 'PROD004', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Desktops'),
 (SELECT SUPPLIER_ID FROM SUPPLIERS WHERE SUPPLIER_NAME = 'TechSource Inc.'),
 'Professional workstation for demanding applications', 'Black', 'Full Tower', 15.5, 1599.99, 1200.00, 1799.99, FALSE, 20, 100, 'Professional', 'Premium', 'Professional'),

('Compact Desktop PC', 'PROD005', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Desktops'),
 (SELECT SUPPLIER_ID FROM SUPPLIERS WHERE SUPPLIER_NAME = 'Global Electronics'),
 'Space-saving desktop for office use', 'Black', 'Mini', 8.2, 699.99, 520.00, 799.99, FALSE, 35, 180, 'Office', 'Standard', 'Compact'),

-- Monitors
('4K Professional Monitor 27"', 'PROD006', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Monitors'),
 (SELECT SUPPLIER_ID FROM SUPPLIERS WHERE SUPPLIER_NAME = 'Global Electronics'),
 'High-resolution 4K monitor for professional work', 'Black', '27"', 12.1, 599.99, 430.00, 699.99, FALSE, 25, 120, 'Professional', 'Premium', 'Modern'),

('Ultra-wide Monitor 34"', 'PROD007', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Monitors'),
 (SELECT SUPPLIER_ID FROM SUPPLIERS WHERE SUPPLIER_NAME = 'TechSource Inc.'),
 'Ultra-wide curved monitor for multitasking', 'Black', '34"', 16.8, 799.99, 590.00, 899.99, FALSE, 20, 85, 'Professional', 'Premium', 'Curved'),

-- Keyboards and Mice
('Mechanical Keyboard Pro', 'PROD008', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Keyboards'),
 (SELECT SUPPLIER_ID FROM SUPPLIERS WHERE SUPPLIER_NAME = 'Global Electronics'),
 'Premium mechanical keyboard for professionals', 'Black', 'Full Size', 2.2, 149.99, 95.00, 179.99, FALSE, 50, 300, 'Professional', 'Premium', 'Mechanical'),

('Wireless Mouse Elite', 'PROD009', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Mice'),
 (SELECT SUPPLIER_ID FROM SUPPLIERS WHERE SUPPLIER_NAME = 'Global Electronics'),
 'High-precision wireless mouse', 'Black', 'Standard', 0.3, 79.99, 45.00, 99.99, FALSE, 100, 500, 'Professional', 'Standard', 'Ergonomic'),

-- Office Furniture
('Executive Office Chair', 'PROD010', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Office Chairs'),
 (SELECT SUPPLIER_ID FROM SUPPLIERS WHERE SUPPLIER_NAME = 'Furniture World'),
 'Ergonomic executive chair with lumbar support', 'Black', 'Standard', 45.0, 449.99, 280.00, 549.99, FALSE, 15, 60, 'Executive', 'Premium', 'Executive'),

('Adjustable Desk 60"', 'PROD011', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Desks'),
 (SELECT SUPPLIER_ID FROM SUPPLIERS WHERE SUPPLIER_NAME = 'Furniture World'),
 'Height-adjustable standing desk', 'White', '60"', 85.5, 699.99, 450.00, 799.99, FALSE, 10, 40, 'Office', 'Premium', 'Modern'),

-- Software
('Office Suite Professional', 'PROD012', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Productivity Software'),
 (SELECT SUPPLIER_ID FROM SUPPLIERS WHERE SUPPLIER_NAME = 'Software Paradise'),
 'Complete office productivity suite license', NULL, 'Digital', 0.0, 299.99, 180.00, 349.99, FALSE, 100, 1000, 'Software', 'Standard', 'Digital'),

('Project Management Pro', 'PROD013', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Productivity Software'),
 (SELECT SUPPLIER_ID FROM SUPPLIERS WHERE SUPPLIER_NAME = 'Software Paradise'),
 'Advanced project management software license', NULL, 'Digital', 0.0, 199.99, 120.00, 249.99, FALSE, 75, 500, 'Software', 'Premium', 'Digital'),

-- Office Supplies
('Premium Notebook Set', 'PROD014', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Stationery'),
 (SELECT SUPPLIER_ID FROM SUPPLIERS WHERE SUPPLIER_NAME = 'Office Solutions Ltd'),
 'Set of 5 premium lined notebooks', 'Blue', 'A4', 1.2, 24.99, 12.00, 29.99, FALSE, 200, 800, 'Office', 'Standard', 'Classic'),

('Executive Pen Collection', 'PROD015', (SELECT CATEGORY_ID FROM CATEGORIES WHERE CATEGORY_NAME = 'Stationery'),
 (SELECT SUPPLIER_ID FROM SUPPLIERS WHERE SUPPLIER_NAME = 'Office Solutions Ltd'),
 'Luxury pen set for executives', 'Black', 'Standard', 0.5, 149.99, 75.00, 199.99, FALSE, 50, 150, 'Executive', 'Premium', 'Luxury');

-- Populate Inventory for all products
INSERT INTO INVENTORY (
    PRODUCT_ID, LOCATION_CODE, QUANTITY_ON_HAND, QUANTITY_AVAILABLE, QUANTITY_ALLOCATED, 
    REORDER_POINT, MAX_STOCK_LEVEL, LAST_COUNT_DATE, AVERAGE_COST, LAST_COST
)
SELECT 
    PRODUCT_ID, 
    'MAIN' as LOCATION_CODE,
    UNITS_IN_STOCK as QUANTITY_ON_HAND,
    UNITS_IN_STOCK as QUANTITY_AVAILABLE,
    0 as QUANTITY_ALLOCATED,
    REORDER_LEVEL,
    REORDER_LEVEL * 5 as MAX_STOCK_LEVEL,
    CURRENT_DATE() as LAST_COUNT_DATE,
    COST as AVERAGE_COST,
    COST as LAST_COST
FROM PRODUCTS;