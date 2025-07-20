-- =====================================================
-- RetailWorks Database Creation Script
-- Description: Creates the main database and schemas
-- Version: 1.0
-- Date: 2025-07-19
-- =====================================================

-- Create main database
CREATE DATABASE IF NOT EXISTS <% database_name %>
COMMENT = 'RetailWorks Enterprise Data Platform - Main Database';

-- Use the database
USE DATABASE <% database_name %>;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS SALES_SCHEMA<% schema_suffix %>
COMMENT = 'Sales data including orders, territories, and representatives';

CREATE SCHEMA IF NOT EXISTS PRODUCTS_SCHEMA<% schema_suffix %>
COMMENT = 'Product catalog, categories, suppliers, and inventory';

CREATE SCHEMA IF NOT EXISTS CUSTOMERS_SCHEMA<% schema_suffix %>
COMMENT = 'Customer information, addresses, and segmentation';

CREATE SCHEMA IF NOT EXISTS HR_SCHEMA<% schema_suffix %>
COMMENT = 'Human resources data including employees, departments, and payroll';

CREATE SCHEMA IF NOT EXISTS ANALYTICS_SCHEMA<% schema_suffix %>
COMMENT = 'Data warehouse dimensional model for analytics';

CREATE SCHEMA IF NOT EXISTS STAGING_SCHEMA<% schema_suffix %>
COMMENT = 'Staging area for data loading and ETL processes';

-- Set default schema
USE SCHEMA ANALYTICS_SCHEMA<% schema_suffix %>;