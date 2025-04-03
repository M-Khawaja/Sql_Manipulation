-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- Goal: Clean and prepare customer data for clustering.
-- Author: Maleeha Khawaja
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-- Set the date format to DD-MM-YYYY
SET datestyle TO DMY;

-- Drop the table if it exists
DROP TABLE IF EXISTS marketing_campaign;

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- Step 1: Create the table.
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

CREATE TABLE marketing_campaign (
  ID INTEGER,
  Year_Birth INTEGER,
  Education TEXT,
  Marital_Status TEXT,
  Income INTEGER,
  Kidhome INTEGER,
  Teenhome INTEGER,
  Dt_Customer DATE,
  Recency INTEGER,
  MntWines INTEGER,
  MntFruits INTEGER,
  MntMeatProducts INTEGER,
  MntFishProducts INTEGER,
  MntSweetProducts INTEGER,
  MntGoldProds INTEGER,
  NumDealsPurchases INTEGER,
  NumWebPurchases INTEGER,
  NumCatalogPurchases INTEGER,
  NumStorePurchases INTEGER,
  NumWebVisitsMonth INTEGER,
  AcceptedCmp3 INTEGER,
  AcceptedCmp4 INTEGER,
  AcceptedCmp5 INTEGER,
  AcceptedCmp1 INTEGER,
  AcceptedCmp2 INTEGER,
  Complain INTEGER,
  Z_CostContact INTEGER,
  Z_Revenue INTEGER,
  Response INTEGER
);

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- Step 2: Import the data.
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\copy marketing_campaign FROM 'marketing_campaign.csv' WITH (FORMAT csv, HEADER true);

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- Step 3: Preview the data.
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-- Previewing the first 5 rows of the data
SELECT * 
FROM marketing_campaign 
LIMIT 5;

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- Step 4: Check for duplicates.                            
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

SELECT ID, COUNT(*) 
FROM marketing_campaign 
GROUP BY ID 
HAVING COUNT(*) > 1;

-- This returned zero rows which means there are no duplicate rows.

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- Step 5: Feature engineering.
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-- Let's add an age column to the table. This is an important factor in the profile of a customer.

ALTER TABLE marketing_campaign ADD COLUMN Age INT;

UPDATE marketing_campaign
SET Age = EXTRACT(YEAR FROM CURRENT_DATE) - Year_Birth;

-- We can also combine the Kidhome and Teenhome columns into one and call it Total_Children.

ALTER TABLE marketing_campaign ADD COLUMN Total_Children INT;

UPDATE marketing_campaign
SET Total_Children = Kidhome + Teenhome;

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- Step 6: Clean the data.
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- Step 6.1: Remove irrelevant columns.
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-- Since we're interested in data related to customer demographics and data related to customer spending habits,
-- we'll remove irrelevant columns from the data.

-- We'll create a backup of the table before removing the columns.

CREATE TABLE marketing_campaign_backup AS
SELECT * FROM marketing_campaign;

-- Now we can remove the columns.

ALTER TABLE marketing_campaign
DROP COLUMN Z_CostContact,
DROP COLUMN Z_Revenue,
DROP COLUMN Complain,
DROP COLUMN Dt_Customer,
DROP COLUMN Kidhome,
DROP COLUMN Teenhome,
DROP COLUMN Response,
DROP COLUMN ID,
DROP COLUMN Year_Birth;

-- Let's confirm that these columns have been removed from the table.

SELECT * 
FROM marketing_campaign
LIMIT 5;

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- Step 6.2: Handle inconsistent entries.
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-- We want to check for inconsistent entries in the Education and Marital_Status columns.

SELECT DISTINCT Education
FROM marketing_campaign;

SELECT DISTINCT Marital_Status
FROM marketing_campaign;

-- There are some unorthodox entries in the Marital_Status column (e.g. Absurd, YOLO).
-- Let's check how many rows of the table are affected.

SELECT Marital_Status, COUNT(*) 
FROM marketing_campaign
GROUP BY Marital_Status
ORDER BY COUNT(*) DESC;

-- There are two entries each with a Marital_Status of 'Absurd' and 'YOLO'.
-- We will remove these rows from the table and we will merge the 'Alone' and 'Single' entries into one.

DELETE FROM marketing_campaign
WHERE Marital_Status = 'Absurd' OR Marital_Status = 'YOLO';

UPDATE marketing_campaign
SET Marital_Status = 'Single'
WHERE Marital_Status = 'Alone';

-- Let's check that our table has correctly updated.

SELECT Marital_Status, COUNT(*)
FROM marketing_campaign
GROUP BY Marital_Status
ORDER BY COUNT(*) DESC;

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- Step 7: Handle missing values.
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-- We want to check for any null entries in our table.
-- Let's generate this query dynamically.

SELECT 
    'SELECT ' || 
    string_agg(
        'SUM(CASE WHEN "' || column_name || '" IS NULL THEN 1 ELSE 0 END) AS Null_' || column_name, 
        ', ' || E'\n    '
    ) 
    || ' FROM marketing_campaign;' 
AS sql_query
FROM information_schema.columns
WHERE table_name = 'marketing_campaign';


-- Now we can run this query.

 SELECT SUM(CASE WHEN "total_children" IS NULL THEN 1 ELSE 0 END) AS Null_total_children,           +
     SUM(CASE WHEN "acceptedcmp2" IS NULL THEN 1 ELSE 0 END) AS Null_acceptedcmp2,                  +
     SUM(CASE WHEN "age" IS NULL THEN 1 ELSE 0 END) AS Null_age,                                    +
     SUM(CASE WHEN "income" IS NULL THEN 1 ELSE 0 END) AS Null_income,                              +
     SUM(CASE WHEN "recency" IS NULL THEN 1 ELSE 0 END) AS Null_recency,                            +
     SUM(CASE WHEN "mntwines" IS NULL THEN 1 ELSE 0 END) AS Null_mntwines,                          +
     SUM(CASE WHEN "mntfruits" IS NULL THEN 1 ELSE 0 END) AS Null_mntfruits,                        +
     SUM(CASE WHEN "mntmeatproducts" IS NULL THEN 1 ELSE 0 END) AS Null_mntmeatproducts,            +
     SUM(CASE WHEN "mntfishproducts" IS NULL THEN 1 ELSE 0 END) AS Null_mntfishproducts,            +
     SUM(CASE WHEN "mntsweetproducts" IS NULL THEN 1 ELSE 0 END) AS Null_mntsweetproducts,          +
     SUM(CASE WHEN "mntgoldprods" IS NULL THEN 1 ELSE 0 END) AS Null_mntgoldprods,                  +
     SUM(CASE WHEN "numdealspurchases" IS NULL THEN 1 ELSE 0 END) AS Null_numdealspurchases,        +
     SUM(CASE WHEN "numwebpurchases" IS NULL THEN 1 ELSE 0 END) AS Null_numwebpurchases,            +
     SUM(CASE WHEN "numcatalogpurchases" IS NULL THEN 1 ELSE 0 END) AS Null_numcatalogpurchases,    +
     SUM(CASE WHEN "numstorepurchases" IS NULL THEN 1 ELSE 0 END) AS Null_numstorepurchases,        +
     SUM(CASE WHEN "numwebvisitsmonth" IS NULL THEN 1 ELSE 0 END) AS Null_numwebvisitsmonth,        +
     SUM(CASE WHEN "acceptedcmp3" IS NULL THEN 1 ELSE 0 END) AS Null_acceptedcmp3,                  +
     SUM(CASE WHEN "acceptedcmp4" IS NULL THEN 1 ELSE 0 END) AS Null_acceptedcmp4,                  +
     SUM(CASE WHEN "acceptedcmp5" IS NULL THEN 1 ELSE 0 END) AS Null_acceptedcmp5,                  +
     SUM(CASE WHEN "acceptedcmp1" IS NULL THEN 1 ELSE 0 END) AS Null_acceptedcmp1,                  +
     SUM(CASE WHEN "marital_status" IS NULL THEN 1 ELSE 0 END) AS Null_marital_status,              +
     SUM(CASE WHEN "education" IS NULL THEN 1 ELSE 0 END) AS Null_education 
FROM marketing_campaign;

-- It looks like there are 24 null entries in the Income column.
-- Let's confirm this.

SELECT COUNT(*) FILTER (WHERE Income IS NULL) AS Null_Income
FROM marketing_campaign;

-- Since the Income column will form an integral part of our analysis later on,
-- we will drop the rows which have a null entry in the income column.

DELETE FROM marketing_campaign
WHERE Income IS NULL;

-- Let's confirm these rows have been removed from the table.

SELECT COUNT(*) 
 FILTER (WHERE Income IS NULL) AS Null_Income
FROM marketing_campaign;

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- Step 8: Check for outliers.
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-- Let's check for outliers in the Age, Income and Total_Children columns.

-- Check for extremely high income or ages

SELECT 
  MIN(Age) AS "Min_Age", MAX(Age) AS "Max_Age", 
  MIN(Income) AS "Min_Income", MAX(Income) AS "Max_Income",
  MIN(Total_Children) AS "Min_Children", MAX(Total_Children) AS "Max_Children"
FROM marketing_campaign;

-- There seem to be some outliers in the data. 
-- For example, the maximum age is 132.
-- Let's compute the 95th and 99th percentile for the age column.

SELECT 
  percentile_disc(0.95) WITHIN GROUP (ORDER BY Age) AS p95_Age,
  percentile_disc(0.99) WITHIN GROUP (ORDER BY Age) AS p99_Age
FROM marketing_campaign;

-- 95% of the customers are 75 years or younger, and 99% of the customers are 80 years or younger.
-- We'll update the Age column with a cap to reduce the impact of outliers on future clustering.

UPDATE marketing_campaign
 SET Age = 80
 Where Age > 80;

-- Now let's look at the Income column.

SELECT
  percentile_disc(0.95) WITHIN GROUP (ORDER BY Income) AS p95_Income,
  percentile_disc(0.99) WITHIN GROUP (ORDER BY Income) AS p99_Income
FROM marketing_campaign;

-- 95% of the customers have an income of 84169 or less, and 99% of the customers have an income of 94472 or less.
-- We'll also update the Income column with a cap.

UPDATE marketing_campaign
 SET Income = 94472
 WHERE Income > 94472;

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- Step 9: Export the data.
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-- The data is now ready for the next step so we can export the table to a new csv file.

COPY (SELECT * FROM marketing_campaign) TO '/Users/MaleehaKhawaja/Documents/Codecademy/Portfolio/sql/cleaned_marketing_campaign.csv' WITH CSV HEADER;
 

