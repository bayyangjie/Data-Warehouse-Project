/*
=====================================================================================
Quality Checks
=====================================================================================
Script Purpose:
This script performs various quality checks for data consistency, accuracy and 
standardization across the 'Silver' schemas. It includes checks for:
- Null or Duplicate primary keys
- Unwanted spaces in string fields
- Data Standardization and consistency
- Invalid date ranges and orders
- Data consistency between related fields

Usage Notes:
- Run the checks after inserting data into the Silver Layer
- Discrepancies found during the checks must be resolved
=====================================================================================
*/

-- ==================================================================================
-- Checking 'silver.crm_cust_info'
-- ==================================================================================
-- Check for NULLs or Duplicates in Primary Key
SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 or cst_id IS NULL  /* 'or cst_id IS NULL' is in case 'cst_id' happens to have only one NULL record, it should also be considered */


--- Check and Remove unwanted spaces in the columns with string values (e.g cst_firstname, cst_lastname, cst_marital_status, cst_gndr) to ensure data consistency and uniformity
/* if the original value is not equal to the value after trimming, it means there are spaces in the original value */
SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)


--- Data Standardization & Consistency. Checking the data levels in columns with low cardinalty.
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info


-- ==================================================================================
-- Checking 'silver.crm_prd_info'
-- ==================================================================================
-- Check for NULLs or Duplicates in Primary Key
SELECT
    prd_id,
    COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


--- Check for unwanted spaces in string values (prd_nm, prd_line, prd_key, cat_id)
/* if the original value is not equal to the value after trimming, it means there are spaces in the original value */
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)


--- Check for NULLs or negative numbers
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

--- Data Standardization & Consistency. Checking the data levels in columns with low cardinalty.
SELECT DISTINCT prd_line
FROM silver.crm_prd_info


-- Check for invalid date orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt


-- ==================================================================================
-- Checking 'silver.crm_sales_details'
-- ==================================================================================
-- Check for invalid dates
SELECT NULLIF(sls_due_dt,0) sls_due_dt   -- Replacing entries with '0' with 'NULL' 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 -- Returns the newly replaced NULL values instead of '0's and returns any negative values unchanged
    OR LEN(sls_due_dt) != 8 
    OR sls_due_dt > 20500101 
    OR sls_due_dt < 19000101;
    

-- Check for Invalid Dates (Order Date > Shipping/Due Dates)
/* check returns clean with no invalid date orders */
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;


-- Check data consistency between Sales, Quantity and Price after the transformations were applied previously
/* check shows that calculations tally between the sales, quantity and price variables and the values format are consistent (i.e non NULLs, non-zeros) */

SELECT DISTINCT
    sls_sales,
    sls_quantity, 
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price    
    OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL   
    OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0   
ORDER BY sls_sales, sls_quantity, sls_price;


-- ==================================================================================
-- Checking 'silver.erp_cust_az12'
-- ==================================================================================
-- Identify Out-of-Range Dates
/* the check just returns the list of very old age customers , but does not have customers with age older than the present date which is correct */
SELECT bdate
FROM silver.erp_CUST_AZ12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()


----- Data Standardization and Consistency -----
SELECT DISTINCT gen
FROM silver.erp_CUST_AZ12;


-- ==================================================================================
-- Checking 'silver.erp_loc_a101'
-- ==================================================================================
--- Data Standardization & Consistency
/* Check shows that the distinct country strings follow a consistent naming format */
SELECT DISTINCT CNTRY
FROM silver.erp_LOC_A101
ORDER BY CNTRY;


-- ==================================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ==================================================================================
-- Check for unwanted spaces in string columns
/* check verifies that there are no unwanted spaces in the inserted data in the SILVER layer */
SELECT *
FROM silver.erp_PX_CAT_G1V2
WHERE CAT != TRIM(CAT) OR SUBCAT != TRIM(SUBCAT) OR MAINTENANCE != TRIM(MAINTENANCE)


-- Data Standardization & Consistency
/* Check verifies that the inserted distinct data for the low cardinality string columns in the SILVER layer is valid and clean */
SELECT DISTINCT MAINTENANCE
FROM silver.erp_PX_CAT_G1V2;
