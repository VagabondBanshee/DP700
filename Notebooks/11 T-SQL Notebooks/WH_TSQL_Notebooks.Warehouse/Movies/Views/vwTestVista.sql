-- Auto Generated (Do not modify) FC9019FF01A72DFBEBB8DD0B94D64E4115EBAF4B37AC3559033820E43A9AEC26
CREATE VIEW [Movies].[vwTestVista] AS (SELECT
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
WHERE Series_Title like 'The Lord of the Rings%')