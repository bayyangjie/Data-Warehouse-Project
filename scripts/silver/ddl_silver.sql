						------------ Compiling all queries for inserting all 6 tables from BRONZE layer into SILVER layer ------------
		/* this query is just a combination of all the INSERT queries from the individual tables into 1 single query including a TRUNCATE to prevent duplicated FULL LOADs for each query run */


/* need to have a TRUNCATE function in place so that the table is not inserted twice each time the query is ran , then perform FULL LOAD inserting */


-- Run this for executing the stored procedure.
-- If there are changes done in the stored procedure, rerun the stored procedure query segment first before executing the below statement
EXEC silver.load_silver 

CREATE OR ALTER PROCEDURE silver.load_silver AS   -- naming procedure for creating a stored procedure is 'load_<layer> 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;  -- Declaring variables
	BEGIN TRY
		SET @batch_start_time = GETDATE();   -- Setting start time of the entire FULL LOAD procedure of the tables

		PRINT '============================================';
		PRINT 'Loading the Silver Layer';
		PRINT '============================================';

	
		PRINT '--------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------';


		-- Loading silver.crm_cust_info
		SET @start_time = GETDATE();		-- Setting the start time of loading of the 'crm_cust_info' table into SILVER layer
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)

		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,

			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'	 /* UPPER() converts all 'F' values to upper casings in case small cap values appear. TRIM() ensures all spaces are removed if exist */
				 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'   /* UPPER() converts all 'M' values to upper casings in case small cap values appear. TRIM() ensures all spaces are removed if exist */
				 ELSE 'n/a'
			END cst_marital_status,

			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'  /* UPPER() converts all 'F' values to upper casings in case small cap values appear. TRIM() ensures all spaces are removed if exist */
				 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'    /* UPPER() converts all 'M' values to upper casings in case small cap values appear. TRIM() ensures all spaces are removed if exist */
				 ELSE 'n/a'
			END cst_gndr,

			cst_create_date
		FROM 
		(
			SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t
		WHERE flag_last = 1;
		SET @end_time = GETDATE();		 -- Setting the end time after loading the 'crm_cust_info' table into SILVER layer
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds'   -- CAST() is used so that the DATEDIFF() output is recognized as a STRING as well for the concatenation
		PRINT '>> ------------------------------------------';



		-- Loading silver.crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,      
			cat_id,       
			prd_key,     
			prd_nm,	     
			prd_cost,    
			prd_line,	
			prd_start_dt,
			prd_end_dt
		)
		SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,  -- Extract category ID while ensuring format is the same as the common primary key in "erp_PX_CAT_G1V2" erp table
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,			-- Extract product key
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))	   
			WHEN 'M' THEN 'Mountain'  
			WHEN 'R'THEN 'Road'
			WHEN 'S'THEN 'Other sales'
			WHEN 'T'THEN 'Touring'
			ELSE 'n/a'		-- NULL values in 'prd_line' column are replaced with 'n/a' 
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt  -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();		 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds'
		PRINT '>> ------------------------------------------';



		-- Loading silver.crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
		sls_ord_num,	
		sls_prd_key,		
		sls_cust_id,		
		sls_order_dt,	
		sls_ship_dt,		
		sls_due_dt,		
		sls_sales,		
		sls_quantity,	
		sls_price		
		)
		SELECT
		sls_ord_num,

		sls_prd_key,

		sls_cust_id,

		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL   -- Handling invalid data 
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)    -- Converting integer to date data type. SQL doesn't allow direct casting of integer to date data type, so convert to VARCHAR first then to DATE datatype
		END AS sls_order_dt,

		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL  -- apply same CASE WHEN conditions to sls_ship_date as well just in case the same errors occur in future on new records for the shipping date
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,

		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL 
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,

		CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price) -- Handling missing (NULL) and invalid SALES data (<=0) and deriving it from existing variables 
				THEN sls_quantity * ABS(sls_price)  -- ignore the existing sales values from the source system and recalculate it instead to get the correct figures
			ELSE sls_sales
		END AS sls_sales,

		sls_quantity,

		CASE WHEN sls_price <= 0  OR sls_price IS NULL    -- Handling missing (NULL) and invalid (<=0) PRICE data and deriving it from existing variables. This is performed after the CASE WHEN transformations for "sls_sales" is done so that the correct "sls_sales" is used to calculate the sls_price
				THEN sls_sales / NULLIF(sls_quantity, 0)  -- in the event if new records are input as '0' then replace with 'NULL'
			ELSE sls_price     -- if sls_price is not equal or less than 0 or is not NULL, then everything is fine and the existing value is retained
		END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();		 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds'
		PRINT '>> ------------------------------------------';



		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';



		-- Loading silver.erp_CUST_AZ12
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_CUST_AZ12';
		TRUNCATE TABLE silver.erp_CUST_AZ12;
		PRINT '>> Inserting Data Into: silver.erp_CUST_AZ12';
		INSERT INTO silver.erp_CUST_AZ12(
		cid,
		bdate,
		gen
		)
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))  -- Remove 'NAS' prefix part if presnet in the string values
			ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL  -- Set illogical birth dates (that are in the future) to NULL
			ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN  ('F' , 'FEMALE') THEN 'Female' -- UPPER() and TRIM() are used only during the CHECKING process to standardize the format of the existing values before the check. E.g 'fEMALE' is not the same as 'femalE', so the value are standardized first
			 WHEN UPPER(TRIM(gen)) IN  ('M' , 'MALE') THEN 'Male'
			 ELSE 'n/a'
		END gen
		FROM bronze.erp_CUST_AZ12;
		SET @end_time = GETDATE();		 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds'
		PRINT '>> ------------------------------------------';



		-- Loading silver.erp_LOC_A101
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_LOC_A101';
		TRUNCATE TABLE silver.erp_LOC_A101;
		PRINT '>> Inserting Data Into: silver.erp_LOC_A101';
		INSERT INTO silver.erp_LOC_A101(
			cid,
			cntry)
		SELECT 
		REPLACE(cid, '-', '') AS cid,  -- need to provide an alias name at the end of REPLACE(). With or Without 'AS' works the same but with 'AS' improves readability
		CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'   -- 'LIKE' clause is not allowed here because we are not doing pattern matching (i.e using wildcards like '%'), so ''= is used
			 WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'  -- Only need to specify 'CASE' once if referencing the same variable
			 WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
			 ELSE TRIM(CNTRY)
		END AS cntry
		FROM bronze.erp_LOC_A101;
		SET @end_time = GETDATE();		 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds'
		PRINT '>> ------------------------------------------';



		-- Loading silver.erp_PX_CAT_G1V2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_PX_CAT_G1V2';
		TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
		PRINT '>> Inserting Data Into: silver.erp_PX_CAT_G1V2';
		INSERT INTO silver.erp_PX_CAT_G1V2 (
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE)
		SELECT
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		FROM bronze.erp_PX_CAT_G1V2;
		SET @end_time = GETDATE();		 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds'
		PRINT '>> ------------------------------------------';



		SET @batch_end_time = GETDATE();
		PRINT '============================================'
		PRINT 'Loading Silver Layer is Completed';
		PRINT '>>  - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '============================================';

	END TRY
	BEGIN CATCH  -- ONCE an error is caught when the script starts running the execution immediately jumps to the CATCH block sequence below. Even if multiple errors exist in the TRY block, only the FIRST ERROR is handled in the CATCH block */
		PRINT '============================================'
		PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR); /* Cast the error number as NVARCHAR since String + Int would not be possible for the '+' sign to work */
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '============================================'
	END CATCH
END;    -- End of the stored procedure execution
