/*
=====================================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=====================================================================================================

Script Purpose:
	This stored procedure loads the raw data into the 'bronze' schema from external CSV files.
	It performs the following actions:
	- Truncates the bronze tables before loading data
	- Uses the 'BULK INSERT' command to load data from the raw csv files to the bronze tables.


Parameters:
	This stored procedure does not accept any parameters or return any values.


Usage example:
	EXEC bronze.load_bronze
=====================================================================================================
*/


USE DataWarehouse;     /* to indicate to use the database 'DataWarehouse' */
GO

----- Creating a stored procedure to store sql scripts that are frequently used -----
/* bronze. because the script is belonging to the bronze layer */
/* naming procedure for stored procedure is load_<layer> */

CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN /* start of the sql script to be stored */
	DECLARE @start_time DATETIME , @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;   /* 'batch_start_time' refers to the very beginning of the entire stored procedure */
	BEGIN TRY   
		SET @batch_start_time = GETDATE();     /* this is the very first step of the stored procedure would then be to get the start date and time information of the batch sequence */
	  
		PRINT '============================================';
		PRINT 'Loading the Bronze Layer';
		PRINT '============================================';

	
		PRINT '--------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------';


		SET @start_time = GETDATE();   /* start time of generating this table */
		PRINT '>> Truncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info; /* this is FULL LOAD to avoid creating duplicated versions of the table when the data is reloaded. Contents from the table is emptied out then reload the data from scratch */
	
		PRINT '>> Inserting Data Into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\yangj\OneDrive\Desktop\Personal project\SQL\Data Warehouse Project\datasets\cust_info.csv'
		WITH (
			FIRSTROW = 2, /* do not need the first row because we have already specified the structure when creating the tables using 'CREATE TABLE' */
			FIELDTERMINATOR = ',', /* refers to the delimiter that separates the values */
			TABLOCK
		);
		SET @end_time = GETDATE();    /* end time of generating this table */
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'   /* cast again to allow the concatenation '+'  to work  */
		PRINT '>> ------------------------------------------';



		SET @start_time = GETDATE();   /* start time of generating this table */
		PRINT '>> Truncating Table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info; /* this is FULL LOAD to avoid creating duplicated versions of the table when the data is reloaded. Contents from the table is emptied out then reload the data from scratch */


		PRINT '>> Inserting Data Into: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\yangj\OneDrive\Desktop\Personal project\SQL\Data Warehouse Project\datasets\prd_info.csv'
		WITH (
			FIRSTROW = 2, /* do not need the first row because we have already specified the structure when creating the tables using 'CREATE TABLE' */
			FIELDTERMINATOR = ',', /* refers to the delimiter that separates the values */
			TABLOCK
		);
		SET @end_time = GETDATE();    /* end time of generating this table */
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'   /* cast again to allow the concatenation '+'  to work  */
		PRINT '>> ------------------------------------------';



		SET @start_time = GETDATE();   /* start time of generating this table */
		/* Using TRUNCATE then BULK INSERT avoids creating duplicated versions of the table when the data is reloaded. Contents from the table is emptied out then reload the data from scratch */
		/* TRUNCATE TABLE removes all the existing data from the table, ensuring that there is no leftover data from previous loads */
		PRINT '>> Truncating Table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details; 

	
		PRINT '>> Inserting Data Into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details /* BULK INSERT loads new data into the table without worrying about duplicates or conflicts */
		FROM 'C:\Users\yangj\OneDrive\Desktop\Personal project\SQL\Data Warehouse Project\datasets\sales_details.csv'
		WITH (
			FIRSTROW = 2, /* do not need the first row because we have already specified the structure when creating the tables using 'CREATE TABLE' */
			FIELDTERMINATOR = ',', /* refers to the delimiter that separates the values */
			TABLOCK
		);
		SET @end_time = GETDATE();    /* end time of generating this table */
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'   /* cast again to allow the concatenation '+'  to work  */
		PRINT '>> ------------------------------------------';




		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();   /* start time of generating this table */
		/* Using TRUNCATE then BULK INSERT avoids creating duplicated versions of the table when the data is reloaded. Contents from the table is emptied out then reload the data from scratch */
		/* TRUNCATE TABLE removes all the existing data from the table, ensuring that there is no leftover data from previous loads */
		PRINT '>> Truncating Table: bronze.erp_CUST_AZ12'
		TRUNCATE TABLE bronze.erp_CUST_AZ12; 


		PRINT '>> Inserting Data Into: bronze.erp_CUST_AZ12'
		BULK INSERT bronze.erp_CUST_AZ12 /* BULK INSERT loads new data into the table without worrying about duplicates or conflicts */
		FROM 'C:\Users\yangj\OneDrive\Desktop\Personal project\SQL\Data Warehouse Project\datasets\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, /* do not need the first row because we have already specified the structure when creating the tables using 'CREATE TABLE' */
			FIELDTERMINATOR = ',', /* refers to the delimiter that separates the values */
			TABLOCK
		);
		SET @end_time = GETDATE();    /* end time of generating this table */
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'   /* cast again to allow the concatenation '+'  to work  */
		PRINT '>> ------------------------------------------';


		SET @start_time = GETDATE();   /* start time of generating this table */
		/* Using TRUNCATE then BULK INSERT avoids creating duplicated versions of the table when the data is reloaded. Contents from the table is emptied out then reload the data from scratch */
		/* TRUNCATE TABLE removes all the existing data from the table, ensuring that there is no leftover data from previous loads */
		PRINT '>> Truncating Table: bronze.erp_LOC_A101'
		TRUNCATE TABLE bronze.erp_LOC_A101; 


		PRINT '>> Inserting Data Into: bronze.erp_LOC_A101'
		BULK INSERT bronze.erp_LOC_A101 /* BULK INSERT loads new data into the table without worrying about duplicates or conflicts */
		FROM 'C:\Users\yangj\OneDrive\Desktop\Personal project\SQL\Data Warehouse Project\datasets\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, /* do not need the first row because we have already specified the structure when creating the tables using 'CREATE TABLE' */
			FIELDTERMINATOR = ',', /* refers to the delimiter that separates the values */
			TABLOCK
		);
		SET @end_time = GETDATE();    /* end time of generating this table */
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'   /* cast again to allow the concatenation '+'  to work  */
		PRINT '>> ------------------------------------------';



		SET @start_time = GETDATE();   /* start time of generating this table */
		/* Using TRUNCATE then BULK INSERT avoids creating duplicated versions of the table when the data is reloaded. Contents from the table is emptied out then reload the data from scratch */
		/* TRUNCATE TABLE removes all the existing data from the table, ensuring that there is no leftover data from previous loads */
		PRINT '>> Truncating Table: bronze.erp_PX_CAT_G1V2'
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2; 


		PRINT '>> Inserting Data Into: bronze.erp_PX_CAT_G1V2'
		BULK INSERT bronze.erp_PX_CAT_G1V2 /* BULK INSERT loads new data into the table without worrying about duplicates or conflicts */
		FROM 'C:\Users\yangj\OneDrive\Desktop\Personal project\SQL\Data Warehouse Project\datasets\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, /* do not need the first row because we have already specified the structure when creating the tables using 'CREATE TABLE' */
			FIELDTERMINATOR = ',', /* refers to the delimiter that separates the values */
			TABLOCK
		);
		SET @end_time = GETDATE();    /* end time of generating this table */
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'   /* cast again to allow the concatenation '+'  to work  */
		PRINT '>> ------------------------------------------';

		SET @batch_end_time = GETDATE();      /* this is the very last step of the stored procedure to get the end date and time of the batch sequence */
		PRINT '============================================'
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '>>   - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '============================================';

	END TRY
	BEGIN CATCH /* ONCE an error is caught when the script starts to run then execution immediately jumps to the CATCH block. Even if multiple errors exist in the TRY block, only the FIRST ERROR is handled in the CATCH block */
		PRINT '============================================'
		PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR); /* Cast the error number as NVARCHAR since String + Int would not be possible for the '+' sign to work */
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '============================================'
	END CATCH
END /* end of the stored sql script */

