-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "51049431-5363-4b5d-8ff9-56e5ed5f8b73",
-- META       "default_lakehouse_name": "LH_VistasMaterializadas",
-- META       "default_lakehouse_workspace_id": "a6b99220-13b0-4034-a6dd-9aa70312e882",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "51049431-5363-4b5d-8ff9-56e5ed5f8b73"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- # Create materialized lake views 
-- 1. Use this notebook to create materialized lake views. 
-- 2. Select **Run all** to run the notebook. 
-- 3. When the notebook run is completed, return to your lakehouse and refresh your materialized lake views graph. 


-- CELL ********************

-- Welcome to your new notebook 
-- Type here in the cell editor to add code! 
  CREATE MATERIALIZED LAKE VIEW mlv_Sales AS select SalesOrderNumber, SalesOrderLineNumber, OrderDate, CustomerName, Quantity, UnitPrice, TaxAmount from dbo.sales

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
