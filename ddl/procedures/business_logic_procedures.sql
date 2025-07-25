-- =====================================================
-- Business Logic Stored Procedures
-- Description: Core business logic procedures for RetailWorks
-- Version: 1.0
-- Date: 2025-07-19
-- =====================================================

USE SCHEMA <% database_name %>.SALES_SCHEMA<% schema_suffix %>;

-- Order Processing Procedure
CREATE OR REPLACE PROCEDURE SP_PROCESS_ORDER(
    P_CUSTOMER_ID NUMBER,
    P_SALES_REP_ID NUMBER,
    P_SHIP_ADDRESS VARCHAR,
    P_SHIP_CITY VARCHAR,
    P_SHIP_COUNTRY VARCHAR,
    P_ORDER_ITEMS VARIANT
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_order_id NUMBER;
    v_order_number VARCHAR(20);
    v_subtotal DECIMAL(12,2) := 0;
    v_tax_rate DECIMAL(5,4) := 0.08; -- 8% tax rate
    v_tax_amount DECIMAL(12,2);
    v_total_amount DECIMAL(12,2);
    v_item VARIANT;
    v_product_id NUMBER;
    v_quantity NUMBER;
    v_unit_price DECIMAL(10,2);
    v_discount DECIMAL(5,4);
    v_line_total DECIMAL(12,2);
    v_result STRING;
BEGIN
    -- Generate order number
    v_order_number := 'ORD' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS') || LPAD(SEQ_ORDER_NUMBER.NEXTVAL, 4, '0');
    
    -- Insert order header
    INSERT INTO ORDERS (
        ORDER_NUMBER, CUSTOMER_ID, SALES_REP_ID, ORDER_DATE,
        SHIP_ADDRESS, SHIP_CITY, SHIP_COUNTRY, STATUS
    ) VALUES (
        v_order_number, P_CUSTOMER_ID, P_SALES_REP_ID, CURRENT_DATE(),
        P_SHIP_ADDRESS, P_SHIP_CITY, P_SHIP_COUNTRY, 'PENDING'
    );
    
    -- Get the generated order ID
    v_order_id := (SELECT ORDER_ID FROM ORDERS WHERE ORDER_NUMBER = v_order_number);
    
    -- Process each order item
    FOR i IN 0 TO (ARRAY_SIZE(P_ORDER_ITEMS) - 1) DO
        v_item := GET(P_ORDER_ITEMS, i);
        v_product_id := GET(v_item, 'product_id')::NUMBER;
        v_quantity := GET(v_item, 'quantity')::NUMBER;
        v_unit_price := GET(v_item, 'unit_price')::DECIMAL(10,2);
        v_discount := COALESCE(GET(v_item, 'discount')::DECIMAL(5,4), 0);
        
        -- Calculate line total
        v_line_total := v_quantity * v_unit_price * (1 - v_discount);
        v_subtotal := v_subtotal + v_line_total;
        
        -- Insert order item
        INSERT INTO ORDER_ITEMS (
            ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, DISCOUNT
        ) VALUES (
            v_order_id, v_product_id, v_quantity, v_unit_price, v_discount
        );
        
        -- Update product inventory
        UPDATE <% database_name %>.PRODUCTS_SCHEMA<% schema_suffix %>.INVENTORY 
        SET QUANTITY_AVAILABLE = QUANTITY_AVAILABLE - v_quantity,
            QUANTITY_ALLOCATED = QUANTITY_ALLOCATED + v_quantity
        WHERE PRODUCT_ID = v_product_id;
        
    END FOR;
    
    -- Calculate tax and total
    v_tax_amount := v_subtotal * v_tax_rate;
    v_total_amount := v_subtotal + v_tax_amount;
    
    -- Update order totals
    UPDATE ORDERS 
    SET SUBTOTAL = v_subtotal,
        TAX_AMOUNT = v_tax_amount,
        TOTAL_AMOUNT = v_total_amount,
        MODIFIED_DATE = CURRENT_TIMESTAMP()
    WHERE ORDER_ID = v_order_id;
    
    v_result := 'Order processed successfully. Order ID: ' || v_order_id || ', Order Number: ' || v_order_number;
    RETURN v_result;
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error processing order: ' || SQLERRM;
END;
$$;

-- Customer Onboarding Procedure
CREATE OR REPLACE PROCEDURE SP_ONBOARD_CUSTOMER(
    P_CUSTOMER_TYPE VARCHAR,
    P_COMPANY_NAME VARCHAR,
    P_FIRST_NAME VARCHAR,
    P_LAST_NAME VARCHAR,
    P_EMAIL VARCHAR,
    P_PHONE VARCHAR,
    P_ADDRESS_LINE_1 VARCHAR,
    P_CITY VARCHAR,
    P_COUNTRY VARCHAR,
    P_ANNUAL_INCOME DECIMAL
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_customer_id NUMBER;
    v_customer_number VARCHAR(20);
    v_address_id NUMBER;
    v_segment_id NUMBER;
    v_result STRING;
BEGIN
    -- Generate customer number
    v_customer_number := 'C' || LPAD(SEQ_CUSTOMER_NUMBER.NEXTVAL, 8, '0');
    
    -- Insert address first
    INSERT INTO <% database_name %>.CUSTOMERS_SCHEMA<% schema_suffix %>.ADDRESSES (
        ADDRESS_LINE_1, CITY, COUNTRY, ADDRESS_TYPE
    ) VALUES (
        P_ADDRESS_LINE_1, P_CITY, P_COUNTRY, 'BILLING'
    );
    
    v_address_id := (SELECT MAX(ADDRESS_ID) FROM <% database_name %>.CUSTOMERS_SCHEMA<% schema_suffix %>.ADDRESSES);
    
    -- Determine customer segment based on annual income
    v_segment_id := CASE 
        WHEN P_ANNUAL_INCOME >= 1000000 THEN 1 -- Enterprise
        WHEN P_ANNUAL_INCOME >= 100000 THEN 2 -- Corporate
        WHEN P_ANNUAL_INCOME >= 10000 THEN 3 -- Small Business
        ELSE 4 -- Individual
    END;
    
    -- Insert customer
    INSERT INTO <% database_name %>.CUSTOMERS_SCHEMA<% schema_suffix %>.CUSTOMERS (
        CUSTOMER_NUMBER, CUSTOMER_TYPE, COMPANY_NAME, FIRST_NAME, LAST_NAME,
        EMAIL, PHONE, ANNUAL_INCOME, SEGMENT_ID, BILLING_ADDRESS_ID,
        SHIPPING_ADDRESS_ID, REGISTRATION_DATE, STATUS
    ) VALUES (
        v_customer_number, P_CUSTOMER_TYPE, P_COMPANY_NAME, P_FIRST_NAME, P_LAST_NAME,
        P_EMAIL, P_PHONE, P_ANNUAL_INCOME, v_segment_id, v_address_id,
        v_address_id, CURRENT_DATE(), 'ACTIVE'
    );
    
    v_customer_id := (SELECT MAX(CUSTOMER_ID) FROM <% database_name %>.CUSTOMERS_SCHEMA<% schema_suffix %>.CUSTOMERS);
    
    v_result := 'Customer onboarded successfully. Customer ID: ' || v_customer_id || ', Customer Number: ' || v_customer_number;
    RETURN v_result;
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error onboarding customer: ' || SQLERRM;
END;
$$;

-- Inventory Management Procedure
CREATE OR REPLACE PROCEDURE SP_UPDATE_INVENTORY(
    P_PRODUCT_ID FLOAT,
    P_LOCATION_CODE STRING,
    P_QUANTITY_RECEIVED FLOAT,
    P_TRANSACTION_TYPE STRING -- 'RECEIPT', 'ADJUSTMENT', 'RETURN'
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {
    // Get current inventory
    var stmt = snowflake.createStatement({
        sqlText: `SELECT QUANTITY_ON_HAND, QUANTITY_ALLOCATED, REORDER_POINT 
                  FROM <% database_name %>.PRODUCTS_SCHEMA<% schema_suffix %>.INVENTORY
                  WHERE PRODUCT_ID = ? AND LOCATION_CODE = ?`,
        binds: [P_PRODUCT_ID, P_LOCATION_CODE]
    });
    var result = stmt.execute();
    
    if (!result.next()) {
        return 'Error: Product not found in inventory';
    }
    
    var currentQuantity = result.getColumnValue(1);
    var quantityAllocated = result.getColumnValue(2);
    var reorderPoint = result.getColumnValue(3);
    var newQuantity;
    
    // Calculate new quantity based on transaction type
    if (P_TRANSACTION_TYPE === 'RECEIPT') {
        newQuantity = currentQuantity + P_QUANTITY_RECEIVED;
    } else if (P_TRANSACTION_TYPE === 'ADJUSTMENT') {
        newQuantity = P_QUANTITY_RECEIVED; // Direct adjustment
    } else if (P_TRANSACTION_TYPE === 'RETURN') {
        newQuantity = currentQuantity + P_QUANTITY_RECEIVED;
    } else {
        return 'Invalid transaction type: ' + P_TRANSACTION_TYPE;
    }
    
    // Update inventory
    var updateStmt = snowflake.createStatement({
        sqlText: `UPDATE <% database_name %>.PRODUCTS_SCHEMA<% schema_suffix %>.INVENTORY
                  SET QUANTITY_ON_HAND = ?,
                      QUANTITY_AVAILABLE = ? - QUANTITY_ALLOCATED,
                      LAST_RECEIVED_DATE = CASE WHEN ? IN ('RECEIPT', 'RETURN') 
                                               THEN CURRENT_DATE() 
                                               ELSE LAST_RECEIVED_DATE END,
                      MODIFIED_DATE = CURRENT_TIMESTAMP()
                  WHERE PRODUCT_ID = ? AND LOCATION_CODE = ?`,
        binds: [newQuantity, newQuantity, P_TRANSACTION_TYPE, P_PRODUCT_ID, P_LOCATION_CODE]
    });
    updateStmt.execute();
    
    // Check for reorder point and return result
    if (newQuantity <= reorderPoint) {
        return 'Inventory updated. WARNING: Stock level below reorder point for Product ID: ' + P_PRODUCT_ID;
    } else {
        return 'Inventory updated successfully for Product ID: ' + P_PRODUCT_ID + ', New quantity: ' + newQuantity;
    }
    
} catch (err) {
    return 'Error updating inventory: ' + err.message;
}
$$;

-- Sales Commission Calculation Procedure
USE SCHEMA <% database_name %>.HR_SCHEMA<% schema_suffix %>;

CREATE OR REPLACE PROCEDURE SP_CALCULATE_SALES_COMMISSION(
    P_EMPLOYEE_ID NUMBER,
    P_PAY_PERIOD_START DATE,
    P_PAY_PERIOD_END DATE
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_total_sales DECIMAL(15,2);
    v_commission_rate DECIMAL(5,4);
    v_commission_amount DECIMAL(12,2);
    v_result STRING;
BEGIN
    -- Get commission rate for the sales rep
    SELECT COMMISSION_RATE INTO v_commission_rate
    FROM <% database_name %>.SALES_SCHEMA<% schema_suffix %>.SALES_REPS
    WHERE EMPLOYEE_ID = P_EMPLOYEE_ID;
    
    -- Calculate total sales for the period
    SELECT COALESCE(SUM(o.TOTAL_AMOUNT), 0) INTO v_total_sales
    FROM <% database_name %>.SALES_SCHEMA<% schema_suffix %>.ORDERS o
    JOIN <% database_name %>.SALES_SCHEMA<% schema_suffix %>.SALES_REPS sr ON o.SALES_REP_ID = sr.SALES_REP_ID
    WHERE sr.EMPLOYEE_ID = P_EMPLOYEE_ID
      AND o.ORDER_DATE BETWEEN P_PAY_PERIOD_START AND P_PAY_PERIOD_END
      AND o.STATUS = 'COMPLETED';
    
    -- Calculate commission
    v_commission_amount := v_total_sales * v_commission_rate;
    
    -- Update payroll record (assuming it exists)
    UPDATE PAYROLL
    SET COMMISSION = v_commission_amount,
        GROSS_PAY = BASE_SALARY + OVERTIME_PAY + BONUS + v_commission_amount,
        NET_PAY = GROSS_PAY - TOTAL_DEDUCTIONS,
        MODIFIED_DATE = CURRENT_TIMESTAMP()
    WHERE EMPLOYEE_ID = P_EMPLOYEE_ID
      AND PAY_PERIOD_START = P_PAY_PERIOD_START
      AND PAY_PERIOD_END = P_PAY_PERIOD_END;
    
    v_result := 'Commission calculated: $' || v_commission_amount || ' based on sales of $' || v_total_sales;
    RETURN v_result;
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error calculating commission: ' || SQLERRM;
END;
$$;

-- Data Quality Validation Procedure
USE SCHEMA <% database_name %>.STAGING_SCHEMA<% schema_suffix %>;

CREATE OR REPLACE PROCEDURE SP_VALIDATE_DATA_QUALITY()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_issues_found NUMBER := 0;
    v_result STRING;
BEGIN
    -- Clear previous issues
    DELETE FROM DATA_QUALITY_ISSUES WHERE STATUS = 'RESOLVED';
    
    -- Check for duplicate customers
    INSERT INTO DATA_QUALITY_ISSUES (TABLE_NAME, ISSUE_TYPE, ISSUE_DESCRIPTION, SEVERITY)
    SELECT 'CUSTOMERS', 'DUPLICATE', 'Duplicate email addresses found', 'HIGH'
    FROM (
        SELECT EMAIL, COUNT(*) as cnt
        FROM <% database_name %>.CUSTOMERS_SCHEMA<% schema_suffix %>.CUSTOMERS
        GROUP BY EMAIL
        HAVING COUNT(*) > 1
    ) WHERE cnt > 0;
    
    -- Check for products without categories
    INSERT INTO DATA_QUALITY_ISSUES (TABLE_NAME, COLUMN_NAME, ISSUE_TYPE, ISSUE_DESCRIPTION, SEVERITY)
    SELECT 'PRODUCTS', 'CATEGORY_ID', 'MISSING_VALUE', 'Products without valid category', 'MEDIUM'
    FROM <% database_name %>.PRODUCTS_SCHEMA<% schema_suffix %>.PRODUCTS p
    LEFT JOIN <% database_name %>.PRODUCTS_SCHEMA<% schema_suffix %>.CATEGORIES c ON p.CATEGORY_ID = c.CATEGORY_ID
    WHERE c.CATEGORY_ID IS NULL;
    
    -- Check for negative inventory
    INSERT INTO DATA_QUALITY_ISSUES (TABLE_NAME, COLUMN_NAME, ISSUE_TYPE, ISSUE_DESCRIPTION, SEVERITY)
    SELECT 'INVENTORY', 'QUANTITY_ON_HAND', 'INVALID_VALUE', 'Negative inventory quantities', 'HIGH'
    FROM <% database_name %>.PRODUCTS_SCHEMA<% schema_suffix %>.INVENTORY
    WHERE QUANTITY_ON_HAND < 0;
    
    -- Check for orders without items
    INSERT INTO DATA_QUALITY_ISSUES (TABLE_NAME, ISSUE_TYPE, ISSUE_DESCRIPTION, SEVERITY)
    SELECT 'ORDERS', 'MISSING_ITEMS', 'Orders without order items', 'HIGH'
    FROM <% database_name %>.SALES_SCHEMA<% schema_suffix %>.ORDERS o
    LEFT JOIN <% database_name %>.SALES_SCHEMA<% schema_suffix %>.ORDER_ITEMS oi ON o.ORDER_ID = oi.ORDER_ID
    WHERE oi.ORDER_ID IS NULL;
    
    -- Get count of issues found
    SELECT COUNT(*) INTO v_issues_found FROM DATA_QUALITY_ISSUES WHERE STATUS = 'OPEN';
    
    v_result := 'Data quality validation completed. Issues found: ' || v_issues_found;
    RETURN v_result;
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error during data quality validation: ' || SQLERRM;
END;
$$;

-- Create sequences for generating numbers
USE SCHEMA <% database_name %>.SALES_SCHEMA<% schema_suffix %>;
CREATE SEQUENCE IF NOT EXISTS SEQ_ORDER_NUMBER START = 1000 INCREMENT = 1;

USE SCHEMA <% database_name %>.CUSTOMERS_SCHEMA<% schema_suffix %>;
CREATE SEQUENCE IF NOT EXISTS SEQ_CUSTOMER_NUMBER START = 100000 INCREMENT = 1;