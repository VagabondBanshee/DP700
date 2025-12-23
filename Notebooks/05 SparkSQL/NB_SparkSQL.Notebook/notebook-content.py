# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bf29f10f-4da4-414b-99c2-95d10aa6e060",
# META       "default_lakehouse_name": "LK_SparkSQL",
# META       "default_lakehouse_workspace_id": "d0423244-dbb2-439d-a787-3fb5d6a66f24",
# META       "known_lakehouses": [
# META         {
# META           "id": "bf29f10f-4da4-414b-99c2-95d10aa6e060"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
spark.sql("CREATE SCHEMA IF NOT EXISTS IMDB")
df_movies = spark.read.format("csv").option("header","true").option("quote", "\"").option("escape", "\"").load("Files/IMDB/imdb_top_1000.csv")
df_movies.write.mode("overwrite").format("delta").saveAsTable("IMDB.imdb_top_1000")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Convierte la siguiente celda en una celda de Spark SQL y utiliza lenguiaje SQL para consultar

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC Series_Title AS title,
# MAGIC CAST(IMDB_Rating AS DOUBLE) AS rating
# MAGIC FROM IMDB.imdb_top_1000
# MAGIC WHERE CAST(IMDB_Rating AS DOUBLE) >= 9.0
# MAGIC ORDER BY rating DESC

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Hace la misma consulta pero con SPArk.sql y cfea un dataframe

# CELL ********************

df_1 = spark.sql("SELECT Series_Title AS title, CAST(IMDB_Rating AS DOUBLE) AS rating FROM IMDB.imdb_top_1000 WHERE CAST(IMDB_Rating AS DOUBLE) >= 9.0 ORDER BY rating DESC")
display(df_1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Ahora hace la misma consulta pero de una forma "leible"

# CELL ********************

df_2 = spark.sql('''
SELECT
Series_Title AS title,
CAST(IMDB_Rating AS DOUBLE) AS rating
FROM IMDB.imdb_top_1000
WHERE CAST(IMDB_Rating AS DOUBLE) >= 9.0
ORDER BY rating DESC
''')
display(df_2)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Un dataframe no se puede leer desde Spark SQL, pero se puede crear una vista temportal a partir del dataframe y esta si se puede consutlar con el SQL

# CELL ********************

df_2 = spark.sql('''
SELECT
Series_Title AS title,
CAST(IMDB_Rating AS DOUBLE) AS rating
FROM IMDB.imdb_top_1000
WHERE CAST(IMDB_Rating AS DOUBLE) >= 9.0
ORDER BY rating DESC
''')
df_2.createOrReplaceTempView("df_2_view")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM df_2_view

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# La vista creada dura mientras dura la sesion o si se elimina

# CELL ********************

spark.catalog.dropTempView("df_2_view")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
