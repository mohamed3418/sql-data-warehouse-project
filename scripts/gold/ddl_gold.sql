
/****** Script for creating Gold layer in the data warehouse as a star schema  ******/
/*
This script builds the Gold Layer views for the data warehouse.

It creates dim_customers and dim_products with enriched, cleaned attributes.

It defines fact_sales to store sales transactions linked to products and customers.

Together, they form a star schema for BI reporting and analytics.

*/
Create view gold.dim_customers as 
SELECT 
	row_number() over (order by ci.cst_id) as customer_key 
	,ci.cst_id customer_id
     , ci.[cst_key] customer_number
      ,ci.[cst_firstname] first_name
      ,ci.[cst_lastname] last_name
      ,ci.[cst_marital_status] marital_status
      ,case when ci.[cst_gndr]	!=  'n/a' then ci.[cst_gndr] 
			else coalesce( ca.gen ,'n/a') end as gender
      ,ci.[cst_create_date] create_date
      ,ca.bdate  birth_date
	  ,cl.cntry coutnry
  FROM [DataWarehouse].[silver].[crm_cust_info] ci
  left join [silver].[erp_cust_az12] ca on ci.cst_key = ca.cid
  left join [silver].[erp_loc_a101] cl on ca.cid = cl.cid
  --------------------------------------------------------------------------------------------------------

  create view gold.dim_products as 
SELECT 
		row_NUMBER() OVER (ORDER BY [prd_start_dt] ,[prd_key]) product_key
	  ,pn.[prd_id] product_id
      ,pn.[prd_key] product_number
      ,pn.[prd_nm] product_name
	  ,pn.[cat_id] category_id
	  ,pc.cat category
      ,pc.subcat subcategory
	  ,pc.maintenance 
	  ,pn.[prd_cost] cost 
      ,pn.[prd_line] product_line
      ,pn.[prd_start_dt] as start_date
  FROM [DataWarehouse].[silver].[crm_prd_info] pn 
  left join [silver].[erp_px_cat_g1v2] pc on pn.cat_id = pc.id
  where prd_end_dt is null  --filter out historical data 

  --------------------------------------------------------------------------------------------------
  create view gold.fact_sales  as 
SELECT 
		sd.sls_ord_num order_number
	   ,pr.product_key
	   ,cu.customer_key
      ,sd.[sls_order_dt] order_date
      ,sd.[sls_ship_dt] ship_date 
      ,sd.[sls_due_dt] due_date
      ,sd.[sls_sales] sales_amount
      ,sd.[sls_quantity] quantity
      ,sd.[sls_price] price 
	  FROM [DataWarehouse].[silver].[crm_sales_details] sd
  left join [DataWarehouse].[gold].[dim_products] pr on sd.sls_prd_key = pr.product_number 
  left join [gold].[dim_customers] cu on cu.customer_id = sd.sls_cust_id



