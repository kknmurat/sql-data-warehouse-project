/*
==============================================================================================

Stored Proceedure : Load Silver Layer (Bronze -> Silver)

Script Purpose: 

These stored procedures performs the ETL process to populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
-Truncates Silver tables.
-Inserts transformed and cleansed data from Bronze into Silver tables.

==============================================================================================

*/

-- DROP PROCEDURE silver.load_erp_cust_az12();

CREATE OR REPLACE PROCEDURE silver.load_erp_cust_az12()
 LANGUAGE plpgsql
AS $procedure$
declare
start_time timestamp;
end_time timestamp;

begin
	
start_time:= clock_timestamp();

RAISE NOTICE '===============================';
raise notice 'Started at:%',start_time;
RAISE NOTICE '===============================';


truncate silver.erp_cust_az12;
insert into silver.erp_cust_az12 
select 
case when cid like 'NAS%' then substring(cid,4,length(cid))
else cid  end as cid,
case when bdate > CURRENT_DATE then null
else bdate end as bdate,
case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
	 when upper(trim(gen)) in ('M','MALE') then 'Male'
	 else 'n/a' end
	 as gen
from bronze.erp_cust_az12;

end_time:=clock_timestamp();
raise notice 'Done at: %',end_time;
RAISE NOTICE '===============================';

raise notice 'Duration: %',end_time-start_time;
end;
$procedure$
;

==============================================================================================

-- DROP PROCEDURE silver.load_erp_loc_a101();

CREATE OR REPLACE PROCEDURE silver.load_erp_loc_a101()
 LANGUAGE plpgsql
AS $procedure$
declare 
start_time timestamp;
end_time timestamp;

begin
start_time:=clock_timestamp();
RAISE NOTICE '===============================';
raise notice 'Started at:%',start_time;
RAISE NOTICE '===============================';

truncate silver.erp_loc_a101;
insert into silver.erp_loc_a101 
select 
replace(cid,'-','')as cid,
case when trim(cntry) = 'DE' then 'Germany'
	 when trim(cntry) in ('US','USA') then 'United States'
	 when trim(cntry) = '' or cntry is null then 'n/a'
	 else trim(cntry) end
	 as cntry
from bronze.erp_loc_a101 ela ;
RAISE NOTICE 'Loaded table: %', 'silver.erp_loc_a101';
RAISE NOTICE '===============================';
end_time:=clock_timestamp();
raise notice 'Done at: %',end_time;
RAISE NOTICE '===============================';

raise notice 'Duration: %',end_time-start_time;
end;
$procedure$
;
==============================================================================================

-- DROP PROCEDURE silver.load_erp_px_cat_g1v2();

CREATE OR REPLACE PROCEDURE silver.load_erp_px_cat_g1v2()
 LANGUAGE plpgsql
AS $procedure$
declare 
start_time TIMESTAMP;
end_time TIMESTAMP;
BEGIN
truncate silver.erp_px_cat_g1v2;
insert into silver.erp_px_cat_g1v2 
select
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2;
RAISE NOTICE 'Loaded table: %', 'silver.erp_px_cat_g1v2';
RAISE NOTICE '===============================';
end_time:=clock_timestamp();
raise notice 'Done at: %',end_time;
RAISE NOTICE '===============================';

raise notice 'Duration: %',end_time-start_time;

end;
$procedure$
;
==============================================================================================
-- DROP PROCEDURE silver.load_to_silver_crm_cust_info();

CREATE OR REPLACE PROCEDURE silver.load_to_silver_crm_cust_info()
 LANGUAGE plpgsql
AS $procedure$
declare 
start_time TIMESTAMP;
end_time TIMESTAMP;

begin
	
start_time:=clock_timestamp();
raise NOTICE '===============================';
raise notice 'Started at:%',start_time;
raise NOTICE '===============================';

truncate silver.crm_cust_info;
insert into silver.crm_cust_info 
select 
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case when  upper(trim(cst_marital_status)) = 'S' then 'Single'
	 when  upper(trim(cst_marital_status)) = 'M' then 'Married'
	 else 'n/a'
	 end as cst_marital_status,
case when upper(trim(cst_gndr)) ='F' then 'Female'
	 when upper(trim(cst_gndr)) = 'M' then 'Male'
else 'n/a'
end as cst_gndr,
cst_create_date
from bronze.crm_cust_info cci ;

raise NOTICE 'Loaded table: %', 'silver.crm_cust_info';
raise NOTICE '===============================';


end_time:=clock_timestamp();
raise notice 'Done at: %',end_time;
RAISE NOTICE '===============================';

raise notice 'Duration: %',end_time-start_time;

end;
$procedure$
;
==============================================================================================

-- DROP PROCEDURE silver.load_to_silver_crm_prd_info();

CREATE OR REPLACE PROCEDURE silver.load_to_silver_crm_prd_info()
 LANGUAGE plpgsql
AS $procedure$
declare 
start_time TIMESTAMP;
end_time TIMESTAMP;

begin

truncate silver.crm_prd_info;
insert into silver.crm_prd_info 
select 
prd_id,
replace(substring(prd_key,1,5),'-','_') as cat_id,
substring(prd_key,7,length(prd_key)) as prd_key,
prd_nm,
coalesce(prd_cost,0) as prd_cost,
case 
	 when upper(trim(prd_line))='M' then 'Mountain'
	 when upper(trim(prd_line))='R'  then 'Road'
     when upper(trim(prd_line))='S'  then 'Other Sales'
	 when upper(trim(prd_line))='T'  then 'Touring'
	 else 'n/a'
end as prd_line ,
prd_start_dt,
lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt
from bronze.crm_prd_info;

raise NOTICE 'Loaded table: %', 'silver.crm_prd_info';
raise NOTICE '===============================';


end_time:=clock_timestamp();
raise notice 'Done at: %',end_time;
RAISE NOTICE '===============================';

raise notice 'Duration: %',end_time-start_time;

end;
$procedure$
;
==============================================================================================
-- DROP PROCEDURE silver.load_to_silver_crm_sales_details();

CREATE OR REPLACE PROCEDURE silver.load_to_silver_crm_sales_details()
 LANGUAGE plpgsql
AS $procedure$
declare 
start_time TIMESTAMP;
end_time TIMESTAMP;

begin

truncate silver.crm_sales_details;
insert into silver.crm_sales_details 
SELECT 
sls_ord_num, 
sls_prd_key,
sls_cust_id, 
case when sls_order_dt <=0 or length(cast(sls_order_dt as varchar)) != 8 then NULL
else cast(cast(sls_order_dt as varchar ) as date)
end as sls_order_dt, 
case when sls_order_dt <=0 or length(cast(sls_ship_dt as varchar)) != 8 then NULL
else cast(cast(sls_ship_dt as varchar ) as date)
end as sls_ship_dt, 
case when sls_due_dt <=0 or length(cast(sls_due_dt as varchar)) != 8 then NULL
else cast(cast(sls_due_dt as varchar ) as date)
end as sls_due_dt, 

case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
	then sls_quantity * abs(sls_price) 
else sls_sales end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price <= 0
			then sls_sales / nullif(sls_quantity,0)
			else sls_price end as  sls_price
FROM bronze.crm_sales_details;

raise NOTICE 'Loaded table: %', 'silver.crm_sales_details';
raise NOTICE '===============================';


end_time:=clock_timestamp();
raise notice 'Done at: %',end_time;
RAISE NOTICE '===============================';

raise notice 'Duration: %',end_time-start_time;

end;
$procedure$
;

