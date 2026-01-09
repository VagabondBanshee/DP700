-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "b36afab0-55a3-823c-4e05-4b937067efdc",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "b36afab0-55a3-823c-4e05-4b937067efdc",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- Welcome to your new notebook
-- Type here in the cell editor to add code!


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT TOP (100) [CityKey],
			[WWICityID],
			[City],
			[StateProvince],
			[Country],
			[Continent],
			[SalesTerritory],
			[Region],
			[Subregion],
			[Location],
			[LatestRecordedPopulation],
			[ValidFrom],
			[ValidTo],
			[LineageKey]
FROM [WH_WWI].[dbo].[dimension_city]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
