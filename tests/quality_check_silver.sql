/*
==========================================================================
Quality Checks
==========================================================================
Script Purpose:
This script performs various quality checks for data consistency, accuracy,
and standardization across the 'silver' schema. It includes checks for:

- Null or duplicate primary keys.
- Unwanted spaces in string fields.
- Data standardization and consistency.
- Invalid date ranges and orders.
- Data consistency between related fields.

Usage Notes:
- Run these checks after data loading Silver Layer.
- Investigate and resolve any discrepancies found during the checks.

===========================================================================
*/
============================================================================
-- Checking 'silver.crm_cust_info'
============================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Checks for NULL or duplicate in the primary key
-- Expectation : No result

============================================================================
-- Checking 'silver.crm_prd_info'
============================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
  
SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

--CHECK FOR UNWANTED SPACES
-- EXPECTATION : NO RESULTS

SELECT
prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

--CHECK FOR NULLS OR NEGATIVE NUMBER

SELECT
prd_cost FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL


-- data standardization

SELECT DISTINCT prd_line 
FROM silver.crm_prd_info

--check for invaid date orders
SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM silver.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

============================================================================
-- Checking 'silver.crm_sales_details'
============================================================================

--check for invalid dates

SELECT
NULLIF(sls_due_dt,0) sls_order_dt
FROM silver.crm_sales_details
WHERE sls_due_dt < = 0 
OR LEN(sls_due_dt) != 8 
OR sls_due_dt > 20500101 
OR  sls_due_dt < 19000101

-- check invalid order date

SELECT
*
FROM silver.crm_sales_details
WHERE  sls_order_dt >  sls_due_dt OR sls_order_dt >  sls_ship_dt


--check data consistency : between  sales, quantity and price

-- sales = quantity * price
-- values must not be zero, null, or negative

SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE
	WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
CASE 
	WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF (sls_quantity,0) 
	ELSE sls_price
END AS sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

============================================================================
-- Checking 'silver.erp_cust_az12'
============================================================================

--Identify out of range dates

SELECT DISTINCT -- 
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

--DATA STANDARDIZATION
SELECT DISTINCT 
gen,
CASE 
	WHEN UPPER(TRIM(gen)) IN ('F','FEMALE')  THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M','MALE')  THEN 'Male'
	ELSE 'n/a'
END AS gen 
FROM silver.erp_cust_az12

SELECT
*
FROM silver.erp_cust_az12

-- Data standarization & consistency
SELECT DISTINCT
cntry

FROM silver.erp_loc_a101

============================================================================
-- Checking 'silver.erp_px_cat_g1v2'
============================================================================

-- Check for unwanted spaces
SELECT
*
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data Standardization & consistency
SELECT DISTINCT 
maintenance
FROM silver.erp_px_cat_g1v2

SELECT
*
FROM silver.erp_px_cat_g1v2
