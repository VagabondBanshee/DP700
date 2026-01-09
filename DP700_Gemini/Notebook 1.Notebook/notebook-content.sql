-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "cae5a797-4721-4e14-8595-e560dc71eaa7",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "cae5a797-4721-4e14-8595-e560dc71eaa7",
-- META           "type": "MountedWarehouse"
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

SELECT TOP (100) [CustomerID],
			[NameStyle],
			[Title],
			[FirstName],
			[MiddleName],
			[LastName],
			[PasswordSalt],
			[rowguid],
			[ModifiedDate],
			[Suffix],
			[CompanyName],
			[SalesPerson],
			[EmailAddress],
			[Phone],
			[PasswordHash]
FROM [SQL_MeetUp].[SalesLT].[Customer]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
