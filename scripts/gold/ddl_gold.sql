/*
================================================================================================
DDL Script: Creating Views in Gold Layer
================================================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    The views created in the GOLD layer represent the customers, products and sales.

    The CUSTOMERS and PRODUCTS dimension tables VIEWs are created through the table JOINs 
    from the silver layer, and then stored in the GOLD layer.
    The SALES fact table VIEW is created through joining the PRODUCTS and CUSTOMERS dimension tables
    from the GOLD layer.

Usage:
    - These views can be queried directly for analytics and reporting.
================================================================================================
*/


-- ================================================================================================
-- Create Dimension: gold.dim_customers
-- ================================================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;        -- Drop view if exists then recreate it agian with CREATE VIEW. This prevents data duplication when script is ran more than once.
GO

CREATE VIEW gold.dim_customers AS
SELECT      
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,   -- The surrogate keys just assign incrementing serial numbers to the rows even if there are duplicated rows. Cnt use the surrogate key in the 'ON' clause as it's not considered an existing surrogate key
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


-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,    -- The surrogate keys just assigns incrementing serial numbers to the rows even if there are duplicated rows. Cannot use in this same query in 'ON' statement as it is not considered existing surrogate key.
	/* 
	How ROW_NUMBER() statement works:
	MUST have either a ORDER BY or PARTITION BY to tell SQL how the number is going to be done. 
	Order by start date then for products with same start date, sort those by product key 
	*/
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost, 
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_PX_CAT_G1V2 pc
ON pn.cat_id = pc.id       -- 'cat_id' was created by splitting out from the 'prd_key' column during the transformation step
WHERE prd_end_dt IS NULL;  -- to filter out historical data and only return current ongoing data for each . can also exclude 'end_dt' column in this case


-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO
