CREATE TABLE [dp700_e014].[dim_product_scd2] (

	[surrogate_key] int NULL, 
	[product_id] int NULL, 
	[product_name] varchar(100) NULL, 
	[price] decimal(10,2) NULL, 
	[record_start_date] date NULL, 
	[record_end_date] date NULL, 
	[is_current] bit NULL
);