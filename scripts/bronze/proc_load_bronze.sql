/*
======================================================================
Stored Procedur: Load Bronze Layer (Source -> Bronze)
======================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses the 'copy' command to load data from csv files to bronze tables.

Parameters: 
  None.
This stored procedure does not accept any parameters or return any values.

Using example:
  call bronze.load_bronze();
=======================================================================

*/
create or replace procedure bronze.load_bronze()
language plpgsql
as $$
declare 
start_time TIMESTAMP;
end_time TIMESTAMP;
BEGIN

start_time:=clock_timestamp();
RAISE NOTICE '===============================';
raise notice 'Started at:%',start_time;
RAISE NOTICE '===============================';


truncate table bronze.crm_cust_info;
copy crm_cust_info 
from '/home/murat/datawarehousing project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
delimiter ','
csv header;
RAISE NOTICE 'Loaded table: %', 'crm_cust_info';
RAISE NOTICE '===============================';

truncate table bronze.crm_prd_info;
copy crm_prd_info 
from '/home/murat/datawarehousing project/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
delimiter ','
csv header;
RAISE NOTICE 'Loaded table: %', 'crm_prd_info';
RAISE NOTICE '===============================';

truncate table bronze.crm_sales_details;
copy crm_sales_details
from '/home/murat/datawarehousing project/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
delimiter ','
csv header;
RAISE NOTICE 'Loaded table: %', 'rm_sales_details';
RAISE NOTICE '===============================';

truncate table bronze.erp_cust_az12;
copy erp_cust_az12
from '/home/murat/datawarehousing project/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
delimiter ','
csv header;
RAISE NOTICE 'Loaded table: %', 'erp_cust_az12';
RAISE NOTICE '===============================';

truncate table bronze.erp_loc_a101;
copy erp_loc_a101
from '/home/murat/datawarehousing project/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
delimiter ','
csv header;
RAISE NOTICE 'Loaded table: %', 'erp_loc_a101';
RAISE NOTICE '===============================';


truncate table bronze.erp_px_cat_g1v2;
copy erp_px_cat_g1v2
from '/home/murat/datawarehousing project/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
delimiter ','
csv header;
RAISE NOTICE 'Loaded table: %', 'erp_px_cat_g1v2';
RAISE NOTICE '===============================';
end_time:=clock_timestamp();
raise notice 'Done at: %',end_time;
RAISE NOTICE '===============================';

raise notice 'Duration: %',end_time-start_time;

end;
$$;


call bronze.load_bronze();





