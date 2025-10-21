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

--Data Standardization & Consistency

select distinct cst_marital_status 
from silver.crm_cust_info cci ;

