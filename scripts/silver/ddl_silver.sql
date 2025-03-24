/* 
=====================================================================================================
DDL: Script: Create Silver Tables
=====================================================================================================

Script Purpose:
	This script creates tables in the 'silver' schema, dropping existing tables if they already exist. 
	Run this script to re-define the DDL structure of 'silver' Tables

=====================================================================================================
*/


/* Drop the specified table if it is existing then go and create a new/updated table from scratch. This logic here is necessary to prevent the error returning that table is existing */
IF OBJECT_ID ('silver.crm_cust_info' , 'U') IS NOT NULL  /* 'U' refers to a user-defined table in this case */
	DROP TABLE silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info ( /* the table "crm_cust_info" will reside in the silver schema denoted by "silver.") */
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()     /* Additional column of information in the SILVER LAYER tables to indicate when the data is added */
)

/* Drop the specified table if it is existing then go and create a new/updated table from scratch. This logic here is necessary to prevent the error returning that table is existing */
IF OBJECT_ID ('silver.crm_prd_info' , 'U') IS NOT NULL  /* 'U' refers to a user-defined table in this case */
	DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
prd_id       INT,
cat_id       NVARCHAR(50),  /* This is a new column created during the data cleaning phase of this table in the silver schema. Generated from splitting of the original 'prd_key' column */
prd_key      NVARCHAR(50),
prd_nm	     NVARCHAR(50),
prd_cost     INT,
prd_line	 NVARCHAR(50),
prd_start_dt DATE,			/* prd_start_dt & prd_start_dt date types were changed from DATETIME to DATE to aligned with the DATE data type during the cleaning stage of the table 'silver.crm_prd_info' */
prd_end_dt   DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()     /* Additional column of information to indicate when the data is added */
);


/* Drop the specified table if it is existing then go and create a new/updated table from scratch. This logic here is necessary to prevent the error returning that table is existing */
IF OBJECT_ID ('silver.crm_sales_details' , 'U') IS NOT NULL  /* 'U' refers to a user-defined table in this case */
	DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
sls_ord_num		NVARCHAR(50),
sls_prd_key		NVARCHAR(50),
sls_cust_id		INT,
sls_order_dt	DATE,  -- the date variables need to be converted from INT to DATE after the transformations performe
sls_ship_dt		DATE,   
sls_due_dt		DATE,  
sls_sales		INT,
sls_quantity	INT,
sls_price		INT,
dwh_create_date DATETIME2 DEFAULT GETDATE()     /* Additional column of information to indicate when the data is added */
);


/* Drop the specified table if it is existing then go and create a new/updated table from scratch.This logic here is necessary to prevent the error returning that table is existing */
IF OBJECT_ID ('silver.erp_CUST_AZ12' , 'U') IS NOT NULL  /* 'U' refers to a user-defined table in this case */
	DROP TABLE silver.erp_CUST_AZ12;

CREATE TABLE silver.erp_CUST_AZ12 (
CID		NVARCHAR(50),
BDATE	DATE,
GEN		NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()     /* Additional column of information to indicate when the data is added */
);

/* Drop the specified table if it is existing then go and create a new/updated table from scratch.This logic here is necessary to prevent the error returning that table is existing */
IF OBJECT_ID ('silver.erp_LOC_A101' , 'U') IS NOT NULL  /* 'U' refers to a user-defined table in this case */
	DROP TABLE silver.erp_LOC_A101;

CREATE TABLE silver.erp_LOC_A101 (
CID		NVARCHAR(50),
CNTRY	NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()     /* Additional column of information to indicate when the data is added */
);

/* Drop the specified table if it is existing then go and create a new/updated table from scratch.This logic here is necessary to prevent the error returning that table is existing */
IF OBJECT_ID ('silver.erp_PX_CAT_G1V2' , 'U') IS NOT NULL  /* 'U' refers to a user-defined table in this case */
	DROP TABLE silver.erp_PX_CAT_G1V2;

CREATE TABLE silver.erp_PX_CAT_G1V2 (
ID			NVARCHAR(50),
CAT			NVARCHAR(50),
SUBCAT		NVARCHAR(50),
MAINTENANCE NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()     /* Additional column of information to indicate when the data is added */
);
