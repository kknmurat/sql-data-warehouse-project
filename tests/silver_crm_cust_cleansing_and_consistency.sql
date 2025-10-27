--Check For Nulls Or Duplicates in Primary Key

select 
cst_id,
count(*)
from silver.crm_cust_info
group by cst_id
having count(*)> 1 or cst_id is null;

--Check for unwanted spaces

select cst_key
	from silver.crm_cust_info cci 
	where cst_key != trim(cst_key);

--Check for NULLs or Negative Numbers

select prd_cost
	fom silver.crm_prd_info
	where prd_cost <0 or prd_cost is null;

--Data Standardization & Consistency

select distinct cst_marital_status 
from silver.crm_cust_info cci ;

--Check for invalid Date Orders 

select * from bronze.crm_prd_info
where prd_end_dt < prd_start_dt
