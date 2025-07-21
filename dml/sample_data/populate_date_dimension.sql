-- =====================================================
-- Date Dimension Population Script
-- Description: Populates the DATE_DIM table with 10 years of data
-- Version: 1.0
-- Date: 2025-07-19
-- =====================================================

USE SCHEMA <% database_name %>.ANALYTICS_SCHEMA<% schema_suffix %>;

-- Create a procedure to populate date dimension
CREATE OR REPLACE PROCEDURE SP_POPULATE_DATE_DIMENSION(START_DATE DATE, END_DATE DATE)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    current_date DATE;
    date_key NUMBER(8,0);
    day_of_week NUMBER(1,0);
    day_of_week_name VARCHAR(10);
    day_of_month NUMBER(2,0);
    day_of_year NUMBER(3,0);
    week_of_year NUMBER(2,0);
    month_number NUMBER(2,0);
    month_name VARCHAR(10);
    month_abbr VARCHAR(3);
    quarter_number NUMBER(1,0);
    quarter_name VARCHAR(2);
    year_number NUMBER(4,0);
    is_weekend BOOLEAN;
    fiscal_year NUMBER(4,0);
    fiscal_quarter NUMBER(1,0);
    fiscal_month NUMBER(2,0);
    season VARCHAR(10);
    records_inserted NUMBER := 0;
BEGIN
    -- Clear existing data
    DELETE FROM DATE_DIM WHERE DATE_ACTUAL BETWEEN START_DATE AND END_DATE;
    
    -- Set current date to start date
    current_date := START_DATE;
    
    -- Loop through each date
    WHILE (current_date <= END_DATE) DO
        -- Calculate date components
        date_key := YEAR(current_date) * 10000 + MONTH(current_date) * 100 + DAY(current_date);
        day_of_week := DAYOFWEEK(current_date);
        day_of_week_name := DAYNAME(current_date);
        day_of_month := DAY(current_date);
        day_of_year := DAYOFYEAR(current_date);
        week_of_year := WEEKOFYEAR(current_date);
        month_number := MONTH(current_date);
        month_name := MONTHNAME(current_date);
        month_abbr := LEFT(MONTHNAME(current_date), 3);
        quarter_number := QUARTER(current_date);
        quarter_name := 'Q' || quarter_number;
        year_number := YEAR(current_date);
        is_weekend := (day_of_week = 1 OR day_of_week = 7); -- Sunday = 1, Saturday = 7
        
        -- Calculate fiscal year (assuming fiscal year starts in July)
        IF (month_number >= 7) THEN
            fiscal_year := year_number + 1;
        ELSE
            fiscal_year := year_number;
        END IF;
        
        -- Calculate fiscal quarter
        CASE 
            WHEN month_number IN (7, 8, 9) THEN fiscal_quarter := 1;
            WHEN month_number IN (10, 11, 12) THEN fiscal_quarter := 2;
            WHEN month_number IN (1, 2, 3) THEN fiscal_quarter := 3;
            WHEN month_number IN (4, 5, 6) THEN fiscal_quarter := 4;
        END CASE;
        
        -- Calculate fiscal month
        CASE 
            WHEN month_number = 7 THEN fiscal_month := 1;
            WHEN month_number = 8 THEN fiscal_month := 2;
            WHEN month_number = 9 THEN fiscal_month := 3;
            WHEN month_number = 10 THEN fiscal_month := 4;
            WHEN month_number = 11 THEN fiscal_month := 5;
            WHEN month_number = 12 THEN fiscal_month := 6;
            WHEN month_number = 1 THEN fiscal_month := 7;
            WHEN month_number = 2 THEN fiscal_month := 8;
            WHEN month_number = 3 THEN fiscal_month := 9;
            WHEN month_number = 4 THEN fiscal_month := 10;
            WHEN month_number = 5 THEN fiscal_month := 11;
            WHEN month_number = 6 THEN fiscal_month := 12;
        END CASE;
        
        -- Determine season
        CASE 
            WHEN month_number IN (12, 1, 2) THEN season := 'Winter';
            WHEN month_number IN (3, 4, 5) THEN season := 'Spring';
            WHEN month_number IN (6, 7, 8) THEN season := 'Summer';
            WHEN month_number IN (9, 10, 11) THEN season := 'Fall';
        END CASE;
        
        -- Insert the record
        INSERT INTO DATE_DIM (
            DATE_KEY, DATE_ACTUAL, DAY_OF_WEEK, DAY_OF_WEEK_NAME, 
            DAY_OF_MONTH, DAY_OF_YEAR, WEEK_OF_YEAR, MONTH_NUMBER, 
            MONTH_NAME, MONTH_ABBR, QUARTER_NUMBER, QUARTER_NAME, 
            YEAR_NUMBER, IS_WEEKEND, IS_HOLIDAY, FISCAL_YEAR, 
            FISCAL_QUARTER, FISCAL_MONTH, SEASON
        ) VALUES (
            date_key, current_date, day_of_week, day_of_week_name,
            day_of_month, day_of_year, week_of_year, month_number,
            month_name, month_abbr, quarter_number, quarter_name,
            year_number, is_weekend, FALSE, fiscal_year,
            fiscal_quarter, fiscal_month, season
        );
        
        records_inserted := records_inserted + 1;
        
        -- Move to next date
        current_date := DATEADD(DAY, 1, current_date);
    END WHILE;
    
    RETURN 'Date dimension populated successfully. Records inserted: ' || records_inserted;
END;
$$;

-- Populate date dimension with 10 years of data (2020-2029)
CALL SP_POPULATE_DATE_DIMENSION('2020-01-01', '2029-12-31');

-- Update holidays (US holidays)
UPDATE DATE_DIM SET IS_HOLIDAY = TRUE, HOLIDAY_NAME = 'New Year''s Day' 
WHERE MONTH_NUMBER = 1 AND DAY_OF_MONTH = 1;

UPDATE DATE_DIM SET IS_HOLIDAY = TRUE, HOLIDAY_NAME = 'Independence Day' 
WHERE MONTH_NUMBER = 7 AND DAY_OF_MONTH = 4;

UPDATE DATE_DIM SET IS_HOLIDAY = TRUE, HOLIDAY_NAME = 'Christmas Day' 
WHERE MONTH_NUMBER = 12 AND DAY_OF_MONTH = 25;

-- Thanksgiving (4th Thursday of November)
UPDATE DATE_DIM SET IS_HOLIDAY = TRUE, HOLIDAY_NAME = 'Thanksgiving' 
WHERE MONTH_NUMBER = 11 
  AND DAY_OF_WEEK = 5 -- Thursday
  AND DAY_OF_MONTH BETWEEN 22 AND 28;

-- Memorial Day (Last Monday of May)
UPDATE DATE_DIM SET IS_HOLIDAY = TRUE, HOLIDAY_NAME = 'Memorial Day'
WHERE DATE_KEY IN (
    SELECT DATE_KEY 
    FROM DATE_DIM 
    WHERE MONTH_NUMBER = 5 
      AND DAY_OF_WEEK = 2 -- Monday
      AND DAY_OF_MONTH > 24
);

-- Labor Day (First Monday of September)
UPDATE DATE_DIM SET IS_HOLIDAY = TRUE, HOLIDAY_NAME = 'Labor Day'
WHERE DATE_KEY IN (
    SELECT DATE_KEY 
    FROM DATE_DIM 
    WHERE MONTH_NUMBER = 9 
      AND DAY_OF_WEEK = 2 -- Monday
      AND DAY_OF_MONTH <= 7
);

-- Verify the population
SELECT 
    COUNT(*) as total_records,
    MIN(DATE_ACTUAL) as start_date,
    MAX(DATE_ACTUAL) as end_date,
    COUNT(CASE WHEN IS_HOLIDAY THEN 1 END) as holiday_count
FROM DATE_DIM;