-- =====================================================
-- Sales Analytics Views
-- Description: Business intelligence views for sales reporting
-- Version: 1.0
-- Date: 2025-07-19
-- =====================================================

USE SCHEMA <% database_name %>.ANALYTICS_SCHEMA<% schema_suffix %>;

-- Monthly Sales Summary View
CREATE OR ALTER VIEW VW_MONTHLY_SALES_SUMMARY AS
SELECT 
    d.YEAR_NUMBER,
    d.MONTH_NUMBER,
    d.MONTH_NAME,
    COUNT(DISTINCT sf.ORDER_ID) AS total_orders,
    SUM(sf.QUANTITY) AS total_units_sold,
    SUM(sf.LINE_TOTAL) AS total_revenue,
    SUM(sf.COST * sf.QUANTITY) AS total_cost,
    SUM(sf.PROFIT) AS total_profit,
    ROUND(AVG(sf.LINE_TOTAL), 2) AS avg_order_value,
    ROUND((SUM(sf.PROFIT) / NULLIF(SUM(sf.LINE_TOTAL), 0)) * 100, 2) AS profit_margin_percent
FROM SALES_FACT sf
JOIN DATE_DIM d ON sf.ORDER_DATE_KEY = d.DATE_KEY
GROUP BY d.YEAR_NUMBER, d.MONTH_NUMBER, d.MONTH_NAME
ORDER BY d.YEAR_NUMBER, d.MONTH_NUMBER;

-- Quarterly Sales Performance View
CREATE OR ALTER VIEW VW_QUARTERLY_SALES_PERFORMANCE AS
SELECT 
    d.YEAR_NUMBER,
    d.QUARTER_NUMBER,
    d.QUARTER_NAME,
    COUNT(DISTINCT sf.ORDER_ID) AS total_orders,
    COUNT(DISTINCT sf.CUSTOMER_KEY) AS unique_customers,
    SUM(sf.LINE_TOTAL) AS total_revenue,
    SUM(sf.PROFIT) AS total_profit,
    ROUND(AVG(sf.LINE_TOTAL), 2) AS avg_order_value,
    ROUND(SUM(sf.LINE_TOTAL) / NULLIF(COUNT(DISTINCT sf.CUSTOMER_KEY), 0), 2) AS revenue_per_customer
FROM SALES_FACT sf
JOIN DATE_DIM d ON sf.ORDER_DATE_KEY = d.DATE_KEY
GROUP BY d.YEAR_NUMBER, d.QUARTER_NUMBER, d.QUARTER_NAME
ORDER BY d.YEAR_NUMBER, d.QUARTER_NUMBER;

-- Sales Rep Performance View
CREATE OR ALTER VIEW VW_SALES_REP_PERFORMANCE AS
SELECT 
    sr.SALES_REP_NAME,
    sr.TERRITORY_NAME,
    sr.REGION,
    COUNT(DISTINCT sf.ORDER_ID) AS total_orders,
    COUNT(DISTINCT sf.CUSTOMER_KEY) AS unique_customers,
    SUM(sf.LINE_TOTAL) AS total_revenue,
    SUM(sf.PROFIT) AS total_profit,
    ROUND(AVG(sf.LINE_TOTAL), 2) AS avg_order_value,
    ROUND(SUM(sf.LINE_TOTAL) / NULLIF(COUNT(DISTINCT sf.ORDER_ID), 0), 2) AS avg_order_size,
    ROUND((SUM(sf.PROFIT) / NULLIF(SUM(sf.LINE_TOTAL), 0)) * 100, 2) AS profit_margin_percent
FROM SALES_FACT sf
JOIN SALES_REP_DIM sr ON sf.SALES_REP_KEY = sr.SALES_REP_KEY
WHERE sr.IS_CURRENT = TRUE
GROUP BY sr.SALES_REP_NAME, sr.TERRITORY_NAME, sr.REGION
ORDER BY total_revenue DESC;

-- Territory Sales Analysis View
CREATE OR ALTER VIEW VW_TERRITORY_SALES_ANALYSIS AS
SELECT 
    sr.TERRITORY_NAME,
    sr.REGION,
    sr.COUNTRY,
    COUNT(DISTINCT sr.SALES_REP_KEY) AS active_sales_reps,
    COUNT(DISTINCT sf.ORDER_ID) AS total_orders,
    COUNT(DISTINCT sf.CUSTOMER_KEY) AS unique_customers,
    SUM(sf.LINE_TOTAL) AS total_revenue,
    SUM(sf.PROFIT) AS total_profit,
    ROUND(SUM(sf.LINE_TOTAL) / NULLIF(COUNT(DISTINCT sr.SALES_REP_KEY), 0), 2) AS revenue_per_rep,
    ROUND(SUM(sf.LINE_TOTAL) / NULLIF(COUNT(DISTINCT sf.CUSTOMER_KEY), 0), 2) AS revenue_per_customer
FROM SALES_FACT sf
JOIN SALES_REP_DIM sr ON sf.SALES_REP_KEY = sr.SALES_REP_KEY
WHERE sr.IS_CURRENT = TRUE
GROUP BY sr.TERRITORY_NAME, sr.REGION, sr.COUNTRY
ORDER BY total_revenue DESC;

-- Top Products by Revenue View
CREATE OR ALTER VIEW VW_TOP_PRODUCTS_BY_REVENUE AS
SELECT 
    p.PRODUCT_NAME,
    p.CATEGORY_NAME,
    p.SUPPLIER_NAME,
    p.PRODUCT_LINE,
    SUM(sf.QUANTITY) AS total_units_sold,
    SUM(sf.LINE_TOTAL) AS total_revenue,
    SUM(sf.PROFIT) AS total_profit,
    ROUND(AVG(sf.UNIT_PRICE), 2) AS avg_selling_price,
    COUNT(DISTINCT sf.ORDER_ID) AS orders_count,
    ROUND((SUM(sf.PROFIT) / NULLIF(SUM(sf.LINE_TOTAL), 0)) * 100, 2) AS profit_margin_percent
FROM SALES_FACT sf
JOIN PRODUCT_DIM p ON sf.PRODUCT_KEY = p.PRODUCT_KEY
WHERE p.IS_CURRENT = TRUE
GROUP BY p.PRODUCT_NAME, p.CATEGORY_NAME, p.SUPPLIER_NAME, p.PRODUCT_LINE
ORDER BY total_revenue DESC;

-- Customer Sales Summary View
CREATE OR ALTER VIEW VW_CUSTOMER_SALES_SUMMARY AS
SELECT 
    c.CUSTOMER_NAME,
    c.CUSTOMER_TYPE,
    c.SEGMENT_NAME,
    c.BILLING_CITY,
    c.BILLING_COUNTRY,
    COUNT(DISTINCT sf.ORDER_ID) AS total_orders,
    SUM(sf.QUANTITY) AS total_units_purchased,
    SUM(sf.LINE_TOTAL) AS total_spent,
    ROUND(AVG(sf.LINE_TOTAL), 2) AS avg_order_value,
    MIN(d.DATE_ACTUAL) AS first_order_date,
    MAX(d.DATE_ACTUAL) AS last_order_date,
    DATEDIFF(DAY, MIN(d.DATE_ACTUAL), MAX(d.DATE_ACTUAL)) AS customer_lifespan_days
FROM SALES_FACT sf
JOIN CUSTOMER_DIM c ON sf.CUSTOMER_KEY = c.CUSTOMER_KEY
JOIN DATE_DIM d ON sf.ORDER_DATE_KEY = d.DATE_KEY
WHERE c.IS_CURRENT = TRUE
GROUP BY c.CUSTOMER_NAME, c.CUSTOMER_TYPE, c.SEGMENT_NAME, c.BILLING_CITY, c.BILLING_COUNTRY
ORDER BY total_spent DESC;

-- Sales Trend Analysis View
CREATE OR ALTER VIEW VW_SALES_TREND_ANALYSIS AS
SELECT 
    d.DATE_ACTUAL,
    d.DAY_OF_WEEK_NAME,
    d.MONTH_NAME,
    d.QUARTER_NAME,
    d.YEAR_NUMBER,
    d.IS_WEEKEND,
    d.IS_HOLIDAY,
    COUNT(DISTINCT sf.ORDER_ID) AS daily_orders,
    SUM(sf.LINE_TOTAL) AS daily_revenue,
    SUM(sf.PROFIT) AS daily_profit,
    ROUND(AVG(sf.LINE_TOTAL), 2) AS avg_order_value
FROM SALES_FACT sf
JOIN DATE_DIM d ON sf.ORDER_DATE_KEY = d.DATE_KEY
GROUP BY d.DATE_ACTUAL, d.DAY_OF_WEEK_NAME, d.MONTH_NAME, d.QUARTER_NAME, 
         d.YEAR_NUMBER, d.IS_WEEKEND, d.IS_HOLIDAY
ORDER BY d.DATE_ACTUAL;

-- Category Performance View
CREATE OR ALTER VIEW VW_CATEGORY_PERFORMANCE AS
SELECT 
    p.CATEGORY_NAME,
    COUNT(DISTINCT p.PRODUCT_KEY) AS products_in_category,
    COUNT(DISTINCT sf.ORDER_ID) AS total_orders,
    SUM(sf.QUANTITY) AS total_units_sold,
    SUM(sf.LINE_TOTAL) AS total_revenue,
    SUM(sf.PROFIT) AS total_profit,
    ROUND(AVG(sf.UNIT_PRICE), 2) AS avg_unit_price,
    ROUND((SUM(sf.PROFIT) / NULLIF(SUM(sf.LINE_TOTAL), 0)) * 100, 2) AS profit_margin_percent,
    ROUND(SUM(sf.LINE_TOTAL) / NULLIF(COUNT(DISTINCT p.PRODUCT_KEY), 0), 2) AS revenue_per_product
FROM SALES_FACT sf
JOIN PRODUCT_DIM p ON sf.PRODUCT_KEY = p.PRODUCT_KEY
WHERE p.IS_CURRENT = TRUE
GROUP BY p.CATEGORY_NAME
ORDER BY total_revenue DESC;

-- Executive KPI Dashboard View
CREATE OR ALTER VIEW VW_EXECUTIVE_KPI_DASHBOARD AS
SELECT 
    'Current Month' AS period_type,
    d.YEAR_NUMBER || '-' || LPAD(d.MONTH_NUMBER, 2, '0') AS period_name,
    COUNT(DISTINCT sf.ORDER_ID) AS total_orders,
    COUNT(DISTINCT sf.CUSTOMER_KEY) AS unique_customers,
    SUM(sf.LINE_TOTAL) AS total_revenue,
    SUM(sf.PROFIT) AS total_profit,
    ROUND(AVG(sf.LINE_TOTAL), 2) AS avg_order_value,
    ROUND((SUM(sf.PROFIT) / NULLIF(SUM(sf.LINE_TOTAL), 0)) * 100, 2) AS profit_margin_percent,
    COUNT(DISTINCT sf.PRODUCT_KEY) AS products_sold
FROM SALES_FACT sf
JOIN DATE_DIM d ON sf.ORDER_DATE_KEY = d.DATE_KEY
WHERE d.YEAR_NUMBER = YEAR(CURRENT_DATE()) 
  AND d.MONTH_NUMBER = MONTH(CURRENT_DATE())
GROUP BY d.YEAR_NUMBER, d.MONTH_NUMBER

UNION ALL

SELECT 
    'Current Quarter' AS period_type,
    d.YEAR_NUMBER || '-' || d.QUARTER_NAME AS period_name,
    COUNT(DISTINCT sf.ORDER_ID) AS total_orders,
    COUNT(DISTINCT sf.CUSTOMER_KEY) AS unique_customers,
    SUM(sf.LINE_TOTAL) AS total_revenue,
    SUM(sf.PROFIT) AS total_profit,
    ROUND(AVG(sf.LINE_TOTAL), 2) AS avg_order_value,
    ROUND((SUM(sf.PROFIT) / NULLIF(SUM(sf.LINE_TOTAL), 0)) * 100, 2) AS profit_margin_percent,
    COUNT(DISTINCT sf.PRODUCT_KEY) AS products_sold
FROM SALES_FACT sf
JOIN DATE_DIM d ON sf.ORDER_DATE_KEY = d.DATE_KEY
WHERE d.YEAR_NUMBER = YEAR(CURRENT_DATE()) 
  AND d.QUARTER_NUMBER = QUARTER(CURRENT_DATE())
GROUP BY d.YEAR_NUMBER, d.QUARTER_NUMBER, d.QUARTER_NAME

UNION ALL

SELECT 
    'Year to Date' AS period_type,
    d.YEAR_NUMBER::STRING AS period_name,
    COUNT(DISTINCT sf.ORDER_ID) AS total_orders,
    COUNT(DISTINCT sf.CUSTOMER_KEY) AS unique_customers,
    SUM(sf.LINE_TOTAL) AS total_revenue,
    SUM(sf.PROFIT) AS total_profit,
    ROUND(AVG(sf.LINE_TOTAL), 2) AS avg_order_value,
    ROUND((SUM(sf.PROFIT) / NULLIF(SUM(sf.LINE_TOTAL), 0)) * 100, 2) AS profit_margin_percent,
    COUNT(DISTINCT sf.PRODUCT_KEY) AS products_sold
FROM SALES_FACT sf
JOIN DATE_DIM d ON sf.ORDER_DATE_KEY = d.DATE_KEY
WHERE d.YEAR_NUMBER = YEAR(CURRENT_DATE())
GROUP BY d.YEAR_NUMBER;

-- Note: Role-based access control (RBAC) should be managed separately
-- by your Snowflake administrator after deployment