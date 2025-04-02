/*
================================================================================================
DDL Script: Create Gold Views
================================================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Three different views are created in the GOLD layer to represent customers, products and sales.
    
    During the creation of each view, transformations are performed and with data combination from 
    the Silver layer to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
================================================================================================
*/


-- ================================================================================================
-- Create Dimension: gold.dim_customers
-- ================================================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT      
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,   <!-- The surrogate keys just assigns incrementing serial numbers to the rows even if there are duplicated rows. Cnt use the surrogate key in the 'ON' clause as it's not considered an existing surrogate key -->
	ci.cst_id AS customer_id, 
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr   -- CRM table (alias 'ci') is the Master for gender info
		 ELSE COALESCE(ca.gen, 'n/a')    -- if ca_gen has a value, then use ca.gen. but if ca.gen is NULL then fill as 'n/a' instead
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_CUST_AZ12 ca 
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_LOC_A101 la
ON ci.cst_key = la.cid;
