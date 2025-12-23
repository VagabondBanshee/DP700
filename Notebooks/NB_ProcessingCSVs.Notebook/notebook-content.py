# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d671eebb-88ba-4689-b76d-d214cb43f170",
# META       "default_lakehouse_name": "LH_NotebooksProcessingCSVs",
# META       "default_lakehouse_workspace_id": "d0423244-dbb2-439d-a787-3fb5d6a66f24",
# META       "known_lakehouses": [
# META         {
# META           "id": "d671eebb-88ba-4689-b76d-d214cb43f170"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_movies_1 = spark.read.format("csv").load("Files/Movies/movies_1.csv")
display(df_movies_1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_movies_1 = spark.read.format("csv").option("header","true").load("Files/Movies/movies_1.csv")
display(df_movies_1)
df_movies_1.write.mode("overwrite").format("delta").saveAsTable("Movies.movies_1")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Indicando el delimitador al leer de un CSV

# CELL ********************

df_movies_2 = spark.read.format("csv").option("delimiter", ";").load("Files/Movies/movies_2.csv")
display(df_movies_2)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Crear Schema e indicarselo en la lectura del fichero

# CELL ********************


from pyspark.sql.types import *
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

movies_2_schema = '''
{
	"fields": [
		{
			"metadata": {},
			"name": "FILM",
			"nullable": true,
			"type": "string"
		},
		{
			"metadata": {},
			"name": "YEAR",
			"nullable": true,
			"type": "string"
		},
		{
			"metadata": {},
			"name": "RATING",
			"nullable": true,
			"type": "string"
		}
	],
	"type": "struct"
}
'''
movies_2_schema_st = StructType.fromJson(json.loads(movies_2_schema))
print(movies_2_schema_st)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_movies_2 = spark.read.format("csv").option("delimiter", ";").schema(movies_2_schema_st).load("Files/Movies/movies_2.csv")
display(df_movies_2)
df_movies_2.write.mode("overwrite").format("delta").saveAsTable("Movies.movies_2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Como generar el esquema a  partir de un DataFrame

# CELL ********************

df_movies_2.schema.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
