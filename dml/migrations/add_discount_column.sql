-- =====================================================
-- Migration: Add Discount Column to Suppliers Table
-- Description: Adds discount column and populates existing records with 10% default
-- Version: 1.1
-- Date: 2025-07-21
-- =====================================================

USE DATABASE <% database_name %>;
USE SCHEMA PRODUCTS_SCHEMA<% schema_suffix %>;

-- Step 1: Update existing suppliers with 10% default discount
-- This will only affect records that don't already have a discount value
UPDATE SUPPLIERS 
SET DISCOUNT = 10.00,
    MODIFIED_DATE = CURRENT_TIMESTAMP()
WHERE DISCOUNT IS NULL;

-- Step 2: Verify the update
SELECT 
    'Updated Records' as OPERATION,
    COUNT(*) as RECORD_COUNT,
    AVG(DISCOUNT) as AVERAGE_DISCOUNT,
    MIN(DISCOUNT) as MIN_DISCOUNT,
    MAX(DISCOUNT) as MAX_DISCOUNT
FROM SUPPLIERS
WHERE DISCOUNT IS NOT NULL;

-- Step 3: Show sample of updated data
SELECT 
    SUPPLIER_ID,
    SUPPLIER_NAME,
    DISCOUNT,
    STATUS,
    MODIFIED_DATE
FROM SUPPLIERS
LIMIT 10;

-- Migration completed successfully