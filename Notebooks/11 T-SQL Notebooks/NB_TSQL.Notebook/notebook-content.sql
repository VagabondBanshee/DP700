-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "afd37c1d-d4c8-a3f7-482f-4fb8457b028e",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "afd37c1d-d4c8-a3f7-482f-4fb8457b028e",
-- META           "type": "Datawarehouse"
-- META         },
-- META         {
-- META           "id": "02138ad0-440e-4850-9a64-54a45d218bbc",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- # # Markdown cell example here!

-- CELL ********************

SELECT TOP (100) [FILM],
			[YEAR],
			[RATING]
FROM WH_TSQL_Notebooks.Movies.movies


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT TOP (100) [Poster_Link],
			[Series_Title],
			[Released_Year],
			[Certificate],
			[Runtime],
			[Genre],
			[IMDB_Rating],
			[Overview],
			[Meta_score],
			[Director],
			[Star1],
			[Star2],
			[Star3],
			[Star4],
			[No_of_Votes],
			[Gross]
FROM [LK_SparkSQL].[IMDB].[imdb_top_1000]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************


SELECT TOP (100) [FILM],
			[YEAR],
			[RATING]
FROM WH_TSQL_Notebooks.Movies.movies

SELECT TOP (100) [Poster_Link],
			[Series_Title],
			[Released_Year],
			[Certificate],
			[Runtime],
			[Genre],
			[IMDB_Rating],
			[Overview],
			[Meta_score],
			[Director],
			[Star1],
			[Star2],
			[Star3],
			[Star4],
			[No_of_Votes],
			[Gross]
FROM [LK_SparkSQL].[IMDB].[imdb_top_1000]



-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT
[FILM],
[YEAR],
[RATING],
'WH_TSQL_Notebooks.Movies.Movies' AS '[DATA_SOURCE]'
FROM WH_TSQL_Notebooks.Movies.movies
UNION ALL
SELECT
[Series_Title] AS 'FILM',
[Released_Year] AS 'YEAR',
[IMDB_Rating] AS 'RATING',
'LK_SparkSQL.IMDB.imdb_top_1' AS '[DATA_SOURCE]'
FROM LK_SparkSQL.IMDB.imdb_top_1000
WHERE Series_Title like 'The Lord of the Rings%'


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT TOP (100) [Poster_Link],
			[Series_Title],
			[Released_Year],
			[Certificate],
			[Runtime],
			[Genre],
			[IMDB_Rating],
			[Overview],
			[Meta_score],
			[Director],
			[Star1],
			[Star2],
			[Star3],
			[Star4],
			[No_of_Votes],
			[Gross]
FROM [LK_SparkSQL].[IMDB].[imdb_top_1000]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
