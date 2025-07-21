-- =====================================================
-- Sample Data Population - Transactional Data
-- Description: Populates orders, payroll, and analytics data
-- Environment: Dev
-- =====================================================

USE DATABASE <% database_name %>;

-- Switch to Sales Schema for orders
USE SCHEMA SALES_SCHEMA<% schema_suffix %>;

-- Generate sequence numbers for order generation
CREATE OR REPLACE SEQUENCE SEQ_ORDER_NUMBER START = 1000;

-- Populate Orders (past 12 months)
INSERT INTO ORDERS (
    ORDER_NUMBER, CUSTOMER_ID, SALES_REP_ID, ORDER_DATE, REQUIRED_DATE, SHIPPED_DATE,
    SHIP_VIA, FREIGHT, SHIP_NAME, SHIP_ADDRESS, SHIP_CITY, SHIP_REGION, SHIP_POSTAL_CODE, SHIP_COUNTRY,
    SUBTOTAL, TAX_AMOUNT, TOTAL_AMOUNT, STATUS
)
WITH order_data AS (
    SELECT 
        'ORD' || LPAD(ROW_NUMBER() OVER (ORDER BY RANDOM()), 6, '0') as order_number,
        c.CUSTOMER_ID,
        sr.SALES_REP_ID,
        DATEADD(DAY, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) as order_date,
        a.ADDRESS_LINE_1 as ship_address,
        a.CITY as ship_city,
        a.STATE_PROVINCE as ship_region,
        a.POSTAL_CODE as ship_postal_code,
        a.COUNTRY as ship_country,
        CASE c.CUSTOMER_TYPE
            WHEN 'CORPORATE' THEN c.COMPANY_NAME
            ELSE c.FIRST_NAME || ' ' || c.LAST_NAME
        END as ship_name
    FROM <% database_name %>.CUSTOMERS_SCHEMA<% schema_suffix %>.CUSTOMERS c
    JOIN <% database_name %>.CUSTOMERS_SCHEMA<% schema_suffix %>.ADDRESSES a ON c.SHIPPING_ADDRESS_ID = a.ADDRESS_ID
    CROSS JOIN <% database_name %>.SALES_SCHEMA<% schema_suffix %>.SALES_REPS sr
    CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 5)) -- Generate 5 orders per customer-sales rep combination
)
SELECT 
    order_number,
    CUSTOMER_ID,
    SALES_REP_ID,
    order_date,
    DATEADD(DAY, UNIFORM(7, 21, RANDOM()), order_date) as required_date,
    CASE 
        WHEN UNIFORM(1, 10, RANDOM()) <= 8 THEN DATEADD(DAY, UNIFORM(1, 14, RANDOM()), order_date)
        ELSE NULL -- 20% not yet shipped
    END as shipped_date,
    CASE UNIFORM(1, 3, RANDOM())
        WHEN 1 THEN 'FedEx'
        WHEN 2 THEN 'UPS'
        ELSE 'DHL'
    END as ship_via,
    UNIFORM(10, 100, RANDOM()) as freight,
    ship_name,
    ship_address,
    ship_city,
    ship_region,
    ship_postal_code,
    ship_country,
    0 as subtotal, -- Will be updated after order items
    0 as tax_amount, -- Will be calculated
    0 as total_amount, -- Will be calculated
    CASE 
        WHEN shipped_date IS NOT NULL THEN 'SHIPPED'
        WHEN order_date <= DATEADD(DAY, -7, CURRENT_DATE()) THEN 'PROCESSING'
        ELSE 'PENDING'
    END as status
LIMIT 500; -- Limit to 500 orders for sample data

-- Populate Order Items
INSERT INTO ORDER_ITEMS (ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, DISCOUNT, LINE_TOTAL)
WITH order_items_data AS (
    SELECT 
        o.ORDER_ID,
        p.PRODUCT_ID,
        CASE 
            WHEN cs.SEGMENT_NAME = 'Enterprise' THEN UNIFORM(20, 100, RANDOM())
            WHEN cs.SEGMENT_NAME = 'Corporate' THEN UNIFORM(5, 50, RANDOM())
            WHEN cs.SEGMENT_NAME = 'Small Business' THEN UNIFORM(2, 20, RANDOM())
            ELSE UNIFORM(1, 5, RANDOM())
        END as quantity,
        p.UNIT_PRICE,
        CASE 
            WHEN cs.SEGMENT_NAME = 'Enterprise' THEN cs.DISCOUNT_RATE + UNIFORM(0, 5, RANDOM()) / 100.0
            WHEN cs.SEGMENT_NAME = 'Corporate' THEN cs.DISCOUNT_RATE + UNIFORM(0, 3, RANDOM()) / 100.0
            ELSE cs.DISCOUNT_RATE
        END as discount_rate
    FROM ORDERS o
    JOIN <% database_name %>.CUSTOMERS_SCHEMA<% schema_suffix %>.CUSTOMERS c ON o.CUSTOMER_ID = c.CUSTOMER_ID
    JOIN <% database_name %>.CUSTOMERS_SCHEMA<% schema_suffix %>.CUSTOMER_SEGMENTS cs ON c.SEGMENT_ID = cs.SEGMENT_ID
    CROSS JOIN <% database_name %>.PRODUCTS_SCHEMA<% schema_suffix %>.PRODUCTS p
    WHERE UNIFORM(1, 100, RANDOM()) <= 
        CASE 
            WHEN cs.SEGMENT_NAME = 'Enterprise' THEN 40 -- 40% chance of ordering each product
            WHEN cs.SEGMENT_NAME = 'Corporate' THEN 25   -- 25% chance
            WHEN cs.SEGMENT_NAME = 'Small Business' THEN 15 -- 15% chance
            ELSE 8 -- 8% chance for individuals
        END
)
SELECT 
    ORDER_ID,
    PRODUCT_ID,
    quantity,
    UNIT_PRICE,
    discount_rate,
    ROUND(quantity * UNIT_PRICE * (1 - discount_rate), 2) as line_total
FROM order_items_data;

-- Update Order totals based on order items
UPDATE ORDERS 
SET 
    SUBTOTAL = (
        SELECT COALESCE(SUM(LINE_TOTAL), 0)
        FROM ORDER_ITEMS oi 
        WHERE oi.ORDER_ID = ORDERS.ORDER_ID
    ),
    TAX_AMOUNT = (
        SELECT COALESCE(SUM(LINE_TOTAL), 0) * 0.08
        FROM ORDER_ITEMS oi 
        WHERE oi.ORDER_ID = ORDERS.ORDER_ID
    ),
    TOTAL_AMOUNT = (
        SELECT COALESCE(SUM(LINE_TOTAL), 0) * 1.08
        FROM ORDER_ITEMS oi 
        WHERE oi.ORDER_ID = ORDERS.ORDER_ID
    );

-- Update customer totals
UPDATE <% database_name %>.CUSTOMERS_SCHEMA<% schema_suffix %>.CUSTOMERS 
SET 
    TOTAL_ORDERS = (
        SELECT COUNT(*)
        FROM ORDERS o 
        WHERE o.CUSTOMER_ID = CUSTOMERS.CUSTOMER_ID
    ),
    TOTAL_SPENT = (
        SELECT COALESCE(SUM(TOTAL_AMOUNT), 0)
        FROM ORDERS o 
        WHERE o.CUSTOMER_ID = CUSTOMERS.CUSTOMER_ID
    ),
    LAST_ORDER_DATE = (
        SELECT MAX(ORDER_DATE)
        FROM ORDERS o 
        WHERE o.CUSTOMER_ID = CUSTOMERS.CUSTOMER_ID
    );

-- Switch to HR Schema for payroll
USE SCHEMA HR_SCHEMA<% schema_suffix %>;

-- Populate Payroll (past 12 months, bi-weekly)
INSERT INTO PAYROLL (
    EMPLOYEE_ID, PAY_PERIOD_START, PAY_PERIOD_END, PAY_DATE, BASE_SALARY, OVERTIME_HOURS, OVERTIME_RATE,
    OVERTIME_PAY, BONUS, COMMISSION, GROSS_PAY, FEDERAL_TAX, STATE_TAX, SOCIAL_SECURITY, MEDICARE,
    HEALTH_INSURANCE, RETIREMENT_401K, OTHER_DEDUCTIONS, TOTAL_DEDUCTIONS, NET_PAY
)
WITH payroll_periods AS (
    SELECT 
        e.EMPLOYEE_ID,
        e.FIRST_NAME || ' ' || e.LAST_NAME as full_name,
        p.MIN_SALARY + (p.MAX_SALARY - p.MIN_SALARY) * UNIFORM(0.3, 0.8, RANDOM()) as annual_salary,
        DATEADD(DAY, (ROW_NUMBER() OVER (ORDER BY 1) - 1) * 14, DATE_TRUNC('YEAR', CURRENT_DATE())) as period_start
    FROM EMPLOYEES e
    JOIN POSITIONS p ON e.POSITION_ID = p.POSITION_ID
    CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 26)) -- 26 bi-weekly periods
    WHERE e.STATUS = 'ACTIVE'
),
payroll_calc AS (
    SELECT 
        *,
        period_start as pay_period_start,
        DATEADD(DAY, 13, period_start) as pay_period_end,
        DATEADD(DAY, 16, period_start) as pay_date,
        ROUND(annual_salary / 26, 2) as bi_weekly_salary,
        UNIFORM(0, 8, RANDOM()) as overtime_hours,
        ROUND((annual_salary / 26 / 80) * 1.5, 2) as overtime_rate,
        CASE 
            WHEN MONTH(period_start) IN (3, 6, 9, 12) THEN UNIFORM(500, 2000, RANDOM())
            ELSE 0
        END as bonus
    FROM payroll_periods
    WHERE period_start <= CURRENT_DATE()
),
payroll_final AS (
    SELECT 
        *,
        overtime_hours * overtime_rate as overtime_pay,
        bi_weekly_salary + (overtime_hours * overtime_rate) + bonus as gross_pay_calc
    FROM payroll_calc
),
payroll_taxes AS (
    SELECT 
        *,
        ROUND(gross_pay_calc * 0.22, 2) as federal_tax,
        ROUND(gross_pay_calc * 0.05, 2) as state_tax,
        ROUND(gross_pay_calc * 0.062, 2) as social_security,
        ROUND(gross_pay_calc * 0.0145, 2) as medicare,
        350 as health_insurance,
        ROUND(gross_pay_calc * 0.06, 2) as retirement_401k,
        50 as other_deductions
    FROM payroll_final
)
SELECT 
    EMPLOYEE_ID,
    pay_period_start,
    pay_period_end,
    pay_date,
    bi_weekly_salary,
    overtime_hours,
    overtime_rate,
    overtime_pay,
    bonus,
    0 as commission, -- Will update for sales reps separately
    gross_pay_calc as gross_pay,
    federal_tax,
    state_tax,
    social_security,
    medicare,
    health_insurance,
    retirement_401k,
    other_deductions,
    federal_tax + state_tax + social_security + medicare + health_insurance + retirement_401k + other_deductions as total_deductions,
    gross_pay_calc - (federal_tax + state_tax + social_security + medicare + health_insurance + retirement_401k + other_deductions) as net_pay
FROM payroll_taxes;

-- Update commission for sales employees
UPDATE PAYROLL 
SET 
    COMMISSION = (
        SELECT COALESCE(SUM(o.TOTAL_AMOUNT * sr.COMMISSION_RATE), 0)
        FROM <% database_name %>.SALES_SCHEMA<% schema_suffix %>.ORDERS o
        JOIN <% database_name %>.SALES_SCHEMA<% schema_suffix %>.SALES_REPS sr ON o.SALES_REP_ID = sr.SALES_REP_ID
        WHERE sr.EMPLOYEE_ID = PAYROLL.EMPLOYEE_ID
        AND o.ORDER_DATE BETWEEN PAYROLL.PAY_PERIOD_START AND PAYROLL.PAY_PERIOD_END
        AND o.STATUS = 'SHIPPED'
    ),
    GROSS_PAY = BASE_SALARY + OVERTIME_PAY + BONUS + (
        SELECT COALESCE(SUM(o.TOTAL_AMOUNT * sr.COMMISSION_RATE), 0)
        FROM <% database_name %>.SALES_SCHEMA<% schema_suffix %>.ORDERS o
        JOIN <% database_name %>.SALES_SCHEMA<% schema_suffix %>.SALES_REPS sr ON o.SALES_REP_ID = sr.SALES_REP_ID
        WHERE sr.EMPLOYEE_ID = PAYROLL.EMPLOYEE_ID
        AND o.ORDER_DATE BETWEEN PAYROLL.PAY_PERIOD_START AND PAYROLL.PAY_PERIOD_END
        AND o.STATUS = 'SHIPPED'
    )
WHERE EMPLOYEE_ID IN (
    SELECT EMPLOYEE_ID FROM <% database_name %>.SALES_SCHEMA<% schema_suffix %>.SALES_REPS
);

-- Recalculate net pay after commission updates
UPDATE PAYROLL 
SET NET_PAY = GROSS_PAY - TOTAL_DEDUCTIONS
WHERE EMPLOYEE_ID IN (
    SELECT EMPLOYEE_ID FROM <% database_name %>.SALES_SCHEMA<% schema_suffix %>.SALES_REPS
);

-- Populate Employee Performance Reviews (annual)
INSERT INTO EMPLOYEE_PERFORMANCE (
    EMPLOYEE_ID, REVIEW_PERIOD_START, REVIEW_PERIOD_END, REVIEWER_ID, PERFORMANCE_SCORE,
    GOALS_MET, TOTAL_GOALS, STRENGTHS, AREAS_FOR_IMPROVEMENT, DEVELOPMENT_PLAN,
    SALARY_INCREASE_PERCENT, PROMOTION_ELIGIBLE
)
SELECT 
    e.EMPLOYEE_ID,
    DATE_TRUNC('YEAR', DATEADD(YEAR, -1, CURRENT_DATE())) as review_period_start,
    DATE_TRUNC('YEAR', CURRENT_DATE()) - 1 as review_period_end,
    e.MANAGER_ID,
    ROUND(UNIFORM(2.5, 4.5, RANDOM()), 2) as performance_score,
    UNIFORM(3, 8, RANDOM()) as goals_met,
    8 as total_goals,
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN 'Strong technical skills, excellent communication'
        WHEN 2 THEN 'Leadership abilities, team collaboration'
        WHEN 3 THEN 'Problem-solving skills, initiative'
        WHEN 4 THEN 'Customer focus, reliability'
        ELSE 'Innovation, adaptability'
    END as strengths,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'Time management, project planning'
        WHEN 2 THEN 'Documentation, process improvement'
        WHEN 3 THEN 'Cross-functional collaboration'
        ELSE 'Strategic thinking, delegation'
    END as areas_for_improvement,
    'Focus on professional development courses and mentoring opportunities' as development_plan,
    CASE 
        WHEN UNIFORM(2.5, 4.5, RANDOM()) >= 3.5 THEN UNIFORM(3, 8, RANDOM())
        ELSE UNIFORM(0, 3, RANDOM())
    END as salary_increase_percent,
    CASE 
        WHEN UNIFORM(2.5, 4.5, RANDOM()) >= 4.0 THEN TRUE
        ELSE FALSE
    END as promotion_eligible
FROM EMPLOYEES e
WHERE e.STATUS = 'ACTIVE' 
AND e.HIRE_DATE <= DATEADD(YEAR, -1, CURRENT_DATE())
AND e.MANAGER_ID IS NOT NULL;

-- Switch to Analytics Schema for dimensional data
USE SCHEMA ANALYTICS_SCHEMA<% schema_suffix %>;

-- Populate Customer Dimension
INSERT INTO CUSTOMER_DIM (
    CUSTOMER_KEY, CUSTOMER_ID, CUSTOMER_NUMBER, CUSTOMER_TYPE, COMPANY_NAME, FIRST_NAME, LAST_NAME,
    EMAIL, PHONE, SEGMENT_NAME, CITY, STATE_PROVINCE, COUNTRY, REGISTRATION_DATE, STATUS,
    ANNUAL_INCOME, CREDIT_LIMIT, TOTAL_ORDERS, TOTAL_SPENT, EFFECTIVE_DATE, EXPIRY_DATE, IS_CURRENT
)
SELECT 
    CUSTOMER_ID as customer_key,
    c.CUSTOMER_ID,
    c.CUSTOMER_NUMBER,
    c.CUSTOMER_TYPE,
    c.COMPANY_NAME,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.EMAIL,
    c.PHONE,
    cs.SEGMENT_NAME,
    a.CITY,
    a.STATE_PROVINCE,
    a.COUNTRY,
    c.REGISTRATION_DATE,
    c.STATUS,
    c.ANNUAL_INCOME,
    c.CREDIT_LIMIT,
    c.TOTAL_ORDERS,
    c.TOTAL_SPENT,
    c.CREATED_DATE as effective_date,
    '9999-12-31'::DATE as expiry_date,
    TRUE as is_current
FROM <% database_name %>.CUSTOMERS_SCHEMA<% schema_suffix %>.CUSTOMERS c
JOIN <% database_name %>.CUSTOMERS_SCHEMA<% schema_suffix %>.CUSTOMER_SEGMENTS cs ON c.SEGMENT_ID = cs.SEGMENT_ID
JOIN <% database_name %>.CUSTOMERS_SCHEMA<% schema_suffix %>.ADDRESSES a ON c.BILLING_ADDRESS_ID = a.ADDRESS_ID;

-- Populate Product Dimension
INSERT INTO PRODUCT_DIM (
    PRODUCT_KEY, PRODUCT_ID, PRODUCT_NUMBER, PRODUCT_NAME, CATEGORY_NAME, SUPPLIER_NAME,
    DESCRIPTION, COLOR, SIZE, WEIGHT, UNIT_PRICE, COST, LIST_PRICE, PRODUCT_LINE, CLASS, STYLE,
    DISCONTINUED, EFFECTIVE_DATE, EXPIRY_DATE, IS_CURRENT
)
SELECT 
    PRODUCT_ID as product_key,
    p.PRODUCT_ID,
    p.PRODUCT_NUMBER,
    p.PRODUCT_NAME,
    c.CATEGORY_NAME,
    s.SUPPLIER_NAME,
    p.DESCRIPTION,
    p.COLOR,
    p.SIZE,
    p.WEIGHT,
    p.UNIT_PRICE,
    p.COST,
    p.LIST_PRICE,
    p.PRODUCT_LINE,
    p.CLASS,
    p.STYLE,
    p.DISCONTINUED,
    p.CREATED_DATE as effective_date,
    '9999-12-31'::DATE as expiry_date,
    TRUE as is_current
FROM <% database_name %>.PRODUCTS_SCHEMA<% schema_suffix %>.PRODUCTS p
JOIN <% database_name %>.PRODUCTS_SCHEMA<% schema_suffix %>.CATEGORIES c ON p.CATEGORY_ID = c.CATEGORY_ID
JOIN <% database_name %>.PRODUCTS_SCHEMA<% schema_suffix %>.SUPPLIERS s ON p.SUPPLIER_ID = s.SUPPLIER_ID;

-- Populate Sales Rep Dimension
INSERT INTO SALES_REP_DIM (
    SALES_REP_KEY, SALES_REP_ID, EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE,
    TERRITORY_NAME, HIRE_DATE, COMMISSION_RATE, STATUS, EFFECTIVE_DATE, EXPIRY_DATE, IS_CURRENT
)
SELECT 
    SALES_REP_ID as sales_rep_key,
    sr.SALES_REP_ID,
    sr.EMPLOYEE_ID,
    sr.FIRST_NAME,
    sr.LAST_NAME,
    sr.EMAIL,
    sr.PHONE,
    st.TERRITORY_NAME,
    sr.HIRE_DATE,
    sr.COMMISSION_RATE,
    sr.STATUS,
    sr.CREATED_DATE as effective_date,
    '9999-12-31'::DATE as expiry_date,
    TRUE as is_current
FROM <% database_name %>.SALES_SCHEMA<% schema_suffix %>.SALES_REPS sr
JOIN <% database_name %>.SALES_SCHEMA<% schema_suffix %>.SALES_TERRITORIES st ON sr.TERRITORY_ID = st.TERRITORY_ID;

-- Populate Sales Fact Table
INSERT INTO SALES_FACT (
    ORDER_DATE_KEY, CUSTOMER_KEY, PRODUCT_KEY, SALES_REP_KEY, ORDER_ID, ORDER_ITEM_ID,
    QUANTITY, UNIT_PRICE, DISCOUNT_AMOUNT, LINE_TOTAL, TAX_AMOUNT, FREIGHT_AMOUNT, NET_AMOUNT
)
SELECT 
    d.DATE_KEY as order_date_key,
    cd.CUSTOMER_KEY,
    pd.PRODUCT_KEY,
    srd.SALES_REP_KEY,
    o.ORDER_ID,
    oi.ORDER_ITEM_ID,
    oi.QUANTITY,
    oi.UNIT_PRICE,
    oi.QUANTITY * oi.UNIT_PRICE * oi.DISCOUNT as discount_amount,
    oi.LINE_TOTAL,
    (oi.LINE_TOTAL / o.SUBTOTAL) * o.TAX_AMOUNT as tax_amount,
    (oi.LINE_TOTAL / o.SUBTOTAL) * o.FREIGHT as freight_amount,
    oi.LINE_TOTAL + ((oi.LINE_TOTAL / o.SUBTOTAL) * o.TAX_AMOUNT) + ((oi.LINE_TOTAL / o.SUBTOTAL) * o.FREIGHT) as net_amount
FROM <% database_name %>.SALES_SCHEMA<% schema_suffix %>.ORDERS o
JOIN <% database_name %>.SALES_SCHEMA<% schema_suffix %>.ORDER_ITEMS oi ON o.ORDER_ID = oi.ORDER_ID
JOIN DATE_DIM d ON o.ORDER_DATE = d.DATE_ACTUAL
JOIN CUSTOMER_DIM cd ON o.CUSTOMER_ID = cd.CUSTOMER_ID
JOIN PRODUCT_DIM pd ON oi.PRODUCT_ID = pd.PRODUCT_ID
JOIN SALES_REP_DIM srd ON o.SALES_REP_ID = srd.SALES_REP_ID
WHERE o.STATUS = 'SHIPPED';

-- Populate Customer LTV Fact (calculated monthly)
INSERT INTO CUSTOMER_LTV_FACT (
    CALCULATION_DATE_KEY, CUSTOMER_KEY, TOTAL_ORDERS, TOTAL_REVENUE, AVERAGE_ORDER_VALUE,
    MONTHS_ACTIVE, PREDICTED_LTV, CUSTOMER_SCORE, RISK_SEGMENT
)
WITH customer_metrics AS (
    SELECT 
        cd.CUSTOMER_KEY,
        COUNT(DISTINCT sf.ORDER_ID) as total_orders,
        SUM(sf.NET_AMOUNT) as total_revenue,
        AVG(sf.NET_AMOUNT) as avg_order_value,
        DATEDIFF(MONTH, cd.REGISTRATION_DATE, CURRENT_DATE()) as months_active
    FROM CUSTOMER_DIM cd
    LEFT JOIN SALES_FACT sf ON cd.CUSTOMER_KEY = sf.CUSTOMER_KEY
    GROUP BY cd.CUSTOMER_KEY, cd.REGISTRATION_DATE
)
SELECT 
    (SELECT DATE_KEY FROM DATE_DIM WHERE DATE_ACTUAL = CURRENT_DATE()) as calculation_date_key,
    cm.CUSTOMER_KEY,
    cm.total_orders,
    COALESCE(cm.total_revenue, 0) as total_revenue,
    COALESCE(cm.avg_order_value, 0) as average_order_value,
    cm.months_active,
    COALESCE(cm.total_revenue, 0) * GREATEST(cm.months_active, 1) * 1.2 as predicted_ltv,
    CASE 
        WHEN cm.total_revenue > 50000 THEN 'A'
        WHEN cm.total_revenue > 20000 THEN 'B'
        WHEN cm.total_revenue > 5000 THEN 'C'
        ELSE 'D'
    END as customer_score,
    CASE 
        WHEN cm.months_active > 12 AND cm.total_orders = 0 THEN 'High Risk'
        WHEN cm.months_active > 6 AND cm.total_orders <= 1 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as risk_segment
FROM customer_metrics cm;

-- Populate Product Performance Fact (monthly aggregation)
INSERT INTO PRODUCT_PERFORMANCE_FACT (
    DATE_KEY, PRODUCT_KEY, UNITS_SOLD, REVENUE, PROFIT_MARGIN, INVENTORY_TURNS,
    RETURN_RATE, CUSTOMER_SATISFACTION, PERFORMANCE_SCORE
)
WITH monthly_dates AS (
    SELECT DISTINCT 
        DATE_KEY,
        DATE_ACTUAL,
        YEAR_ACTUAL,
        MONTH_ACTUAL
    FROM DATE_DIM 
    WHERE DATE_ACTUAL = LAST_DAY(DATE_ACTUAL)
    AND DATE_ACTUAL <= CURRENT_DATE()
    AND DATE_ACTUAL >= DATEADD(MONTH, -12, CURRENT_DATE())
),
product_metrics AS (
    SELECT 
        md.DATE_KEY,
        pd.PRODUCT_KEY,
        COALESCE(SUM(sf.QUANTITY), 0) as units_sold,
        COALESCE(SUM(sf.LINE_TOTAL), 0) as revenue,
        COALESCE(AVG((sf.UNIT_PRICE - pd.COST) / NULLIF(sf.UNIT_PRICE, 0)), 0) as profit_margin,
        UNIFORM(1, 12, RANDOM()) as inventory_turns,
        UNIFORM(0.01, 0.05, RANDOM()) as return_rate,
        UNIFORM(3.5, 5.0, RANDOM()) as customer_satisfaction
    FROM monthly_dates md
    CROSS JOIN PRODUCT_DIM pd
    LEFT JOIN SALES_FACT sf ON pd.PRODUCT_KEY = sf.PRODUCT_KEY 
        AND sf.ORDER_DATE_KEY BETWEEN 
            (SELECT MIN(DATE_KEY) FROM DATE_DIM WHERE YEAR_ACTUAL = md.YEAR_ACTUAL AND MONTH_ACTUAL = md.MONTH_ACTUAL)
            AND md.DATE_KEY
    GROUP BY md.DATE_KEY, pd.PRODUCT_KEY
)
SELECT 
    DATE_KEY,
    PRODUCT_KEY,
    units_sold,
    revenue,
    profit_margin,
    inventory_turns,
    return_rate,
    customer_satisfaction,
    CASE 
        WHEN revenue > 10000 AND profit_margin > 0.3 THEN 'A'
        WHEN revenue > 5000 AND profit_margin > 0.2 THEN 'B'
        WHEN revenue > 1000 AND profit_margin > 0.1 THEN 'C'
        ELSE 'D'
    END as performance_score
FROM product_metrics;

-- Update inventory based on sales
UPDATE <% database_name %>.PRODUCTS_SCHEMA<% schema_suffix %>.INVENTORY 
SET 
    QUANTITY_ON_HAND = GREATEST(QUANTITY_ON_HAND - COALESCE((
        SELECT SUM(oi.QUANTITY)
        FROM <% database_name %>.SALES_SCHEMA<% schema_suffix %>.ORDER_ITEMS oi
        JOIN <% database_name %>.SALES_SCHEMA<% schema_suffix %>.ORDERS o ON oi.ORDER_ID = o.ORDER_ID
        WHERE oi.PRODUCT_ID = INVENTORY.PRODUCT_ID
        AND o.STATUS = 'SHIPPED'
    ), 0), 0),
    QUANTITY_AVAILABLE = GREATEST(QUANTITY_AVAILABLE - COALESCE((
        SELECT SUM(oi.QUANTITY)
        FROM <% database_name %>.SALES_SCHEMA<% schema_suffix %>.ORDER_ITEMS oi
        JOIN <% database_name %>.SALES_SCHEMA<% schema_suffix %>.ORDERS o ON oi.ORDER_ID = o.ORDER_ID
        WHERE oi.PRODUCT_ID = INVENTORY.PRODUCT_ID
        AND o.STATUS = 'SHIPPED'
    ), 0), 0),
    LAST_COUNT_DATE = CURRENT_DATE();

-- Create a few data quality issues for demonstration
USE SCHEMA STAGING_SCHEMA<% schema_suffix %>;

INSERT INTO DATA_QUALITY_ISSUES (
    TABLE_NAME, COLUMN_NAME, ISSUE_TYPE, ISSUE_DESCRIPTION, RECORD_IDENTIFIER, 
    RAW_VALUE, SUGGESTED_VALUE, SEVERITY, STATUS
) VALUES
('CUSTOMERS', 'EMAIL', 'INVALID_FORMAT', 'Email address format is invalid', 'CUST013', 
 'daniel.taylor@email', 'daniel.taylor@email.com', 'MEDIUM', 'OPEN'),
('PRODUCTS', 'WEIGHT', 'NULL_VALUE', 'Weight value is missing for physical product', 'PROD008', 
 NULL, '2.2', 'LOW', 'OPEN'),
('ORDERS', 'REQUIRED_DATE', 'BUSINESS_RULE', 'Required date is before order date', 'ORD001234', 
 '2023-01-15', '2023-01-25', 'HIGH', 'RESOLVED'),
('INVENTORY', 'QUANTITY_ON_HAND', 'NEGATIVE_VALUE', 'Quantity on hand cannot be negative', 'PROD015-MAIN', 
 '-5', '0', 'HIGH', 'OPEN');

-- Log ETL processes
INSERT INTO ETL_PROCESS_LOG (
    PROCESS_NAME, START_TIME, END_TIME, STATUS, RECORDS_PROCESSED, RECORDS_INSERTED, 
    RECORDS_UPDATED, RECORDS_REJECTED, ERROR_MESSAGE
) VALUES
('CUSTOMER_DATA_LOAD', DATEADD(HOUR, -2, CURRENT_TIMESTAMP()), DATEADD(HOUR, -1, CURRENT_TIMESTAMP()), 
 'SUCCESS', 15, 15, 0, 0, NULL),
('PRODUCT_DATA_LOAD', DATEADD(HOUR, -3, CURRENT_TIMESTAMP()), DATEADD(HOUR, -2, CURRENT_TIMESTAMP()), 
 'SUCCESS', 15, 15, 0, 0, NULL),
('ORDER_DATA_LOAD', DATEADD(HOUR, -1, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 
 'SUCCESS', 500, 500, 0, 0, NULL),
('INVENTORY_UPDATE', DATEADD(MINUTE, -30, CURRENT_TIMESTAMP()), DATEADD(MINUTE, -15, CURRENT_TIMESTAMP()), 
 'SUCCESS', 15, 0, 15, 0, NULL);

-- Create data lineage records
INSERT INTO DATA_LINEAGE (
    SOURCE_TABLE, SOURCE_COLUMN, TARGET_TABLE, TARGET_COLUMN, TRANSFORMATION_RULE, PROCESS_NAME
) VALUES
('STG_CUSTOMERS_RAW', 'CUSTOMER_NUMBER', 'CUSTOMERS', 'CUSTOMER_NUMBER', 'DIRECT_COPY', 'CUSTOMER_ETL'),
('STG_CUSTOMERS_RAW', 'ANNUAL_INCOME', 'CUSTOMERS', 'ANNUAL_INCOME', 'CAST_TO_DECIMAL', 'CUSTOMER_ETL'),
('CUSTOMERS', 'CUSTOMER_ID', 'CUSTOMER_DIM', 'CUSTOMER_ID', 'DIRECT_COPY', 'DIM_LOAD'),
('ORDERS', 'ORDER_DATE', 'SALES_FACT', 'ORDER_DATE_KEY', 'DATE_LOOKUP', 'FACT_LOAD'),
('ORDER_ITEMS', 'QUANTITY', 'SALES_FACT', 'QUANTITY', 'DIRECT_COPY', 'FACT_LOAD');