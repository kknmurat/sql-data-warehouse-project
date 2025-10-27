
/*
======================================================================
DDL Script : Create Silver Tables 
======================================================================
Script Purpose:
  This script creates tables in the 'silver' schema.
  Run this script to define the DDL structure of 'silver' Tables.
======================================================================
*/

CREATE TABLE silver.crm_cust_info (
	cst_id int4 NULL,
	cst_key varchar NULL,
	cst_firstname varchar NULL,
	cst_lastname varchar NULL,
	cst_material_status varchar NULL,
	cst_gndr varchar NULL,
	cst_create_date date null,
	dwh_create_date timestamp default current_timestamp
);

-- silver.crm_prd_info definition

-- Drop table

-- DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
	prd_id int4 NULL,
	cat_id varchar NULL,
	prd_key varchar NULL,
	prd_nm varchar NULL,
	prd_cost int4 NULL,
	prd_line varchar NULL,
	prd_start_dt date NULL,
	prd_end_dt date NULL,
	dwh_create_date timestamp DEFAULT CURRENT_TIMESTAMP NULL
);

-- silver.crm_sales_details definition

-- Drop table

 --DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
	sls_ord_num varchar NULL,
	sls_prd_key varchar NULL,
	sls_cust_id int4 NULL,
	sls_order_dt date NULL,
	sls_ship_dt date NULL,
	sls_due_dt date NULL,
	sls_sales int4 NULL,
	sls_quantity int4 NULL,
	sls_price int4 null,
	dwh_create_date timestamp default current_timestamp

);

-- silver.erp_cust_az12 definition

-- Drop table

-- DROP TABLE silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12 (
	cid varchar NULL,
	bdate date NULL,
	gen varchar null,
	dwh_create_date timestamp default current_timestamp

);


-- silver.erp_loc_a101 definition

-- Drop table

-- DROP TABLE silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101 (
	cid varchar NULL,
	cntry varchar null,
	dwh_create_date timestamp default current_timestamp
);


-- silver.erp_px_cat_g1v2 definition

-- Drop table

-- DROP TABLE silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2 (
	id varchar NULL,
	cat varchar NULL,
	subcat varchar NULL,
	maintenance varchar null,
	dwh_create_date timestamp default current_timestamp
);
