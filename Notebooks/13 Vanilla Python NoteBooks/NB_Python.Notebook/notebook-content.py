# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "814c3255-276e-42a2-aed1-facb3a993d3d",
# META       "default_lakehouse_name": "LH_Python_NB",
# META       "default_lakehouse_workspace_id": "d0423244-dbb2-439d-a787-3fb5d6a66f24",
# META       "known_lakehouses": [
# META         {
# META           "id": "814c3255-276e-42a2-aed1-facb3a993d3d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Cuaderno en PYTHON para ver como realizar las operaciones de leer y escribir una tabla delta sin Spark
# #
# # Es más econbomuico

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

import pandas as pd
lakehouse_path = "abfss://DP700_Notebooks@onelake.dfs.fabric.microsoft.com/LH_Python_NB.Lakehouse/Files"

# file name in folder
file_name = "movies.csv"

df_movies_csv = pd.read_csv(f"{lakehouse_path}/{file_name}", sep=',')



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# display pandas dataframe
display(df_movies_csv)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Mismo codifgo pero utilizando la ruta API

# CELL ********************

# library imports
import pandas as pd

# default file api path to lakehouse folder
file_path = "/lakehouse/default/Files"

# file name in folder
file_name = "movies.csv"

# read csv file using pandas to dataframe
df_movies_csv = pd.read_csv(f"{file_path}/{file_name}", sep=',')

# display pandas dataframe
display(df_movies_csv)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Ahora un ejemplo de crear una tabla delta

# CELL ********************

# library imports
import pandas as pd
import deltalake

# abfss file path to lakehouse file folder
file_path = "abfss://DP700_Notebooks@onelake.dfs.fabric.microsoft.com/LH_Python_NB.Lakehouse/Files"

# file name in folder
file_name = "movies.csv"

# read csv file using pandas to dataframe
df_movies_csv = pd.read_csv(f"{file_path}/{file_name}", sep=',')

# abfss file path to lakehouse table folder
table_path = "abfss://DP700_Notebooks@onelake.dfs.fabric.microsoft.com/LH_Python_NB.Lakehouse/Tables/Python"

# delta table name
table_name = "movies"

# storage options
storage_options = {"bearer_token": notebookutils.credentials.getToken("storage"), "use_fabric_endpoint": "true"}

# write data to delta table
deltalake.write_deltalake(f"{table_path}/{table_name}", df_movies_csv, mode='overwrite', schema_mode='overwrite', storage_options=storage_options)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Ahora como leer de una tabla delta

# CELL ********************

# read delta table to pandas dataframe
df_movies_delta = deltalake.DeltaTable(f"{table_path}/{table_name}", storage_options=storage_options).to_pandas()

# display data
display(df_movies_delta)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # DUCKDB 
# ## DuckDB es, en términos sencillos, el "SQLite para analítica". Es una base de datos relacional orientada a columnas que se ejecuta in-process (dentro de tu aplicación o notebook) sin necesidad de un servidor externo.

# CELL ********************

# library imports
import pandas as pd
import deltalake
import duckdb

# abfss file path to lakehouse table folder
table_path = "abfss://DP700_Notebooks@onelake.dfs.fabric.microsoft.com/LH_Python_NB.Lakehouse/Tables/Python"

# delta table name
table_name = "movies"

# storage options
storage_options = {"bearer_token": notebookutils.credentials.getToken("storage"), "use_fabric_endpoint": "true"}

# read delta table to pandas dataframe
df_movies_delta = deltalake.DeltaTable(f"{table_path}/{table_name}", storage_options=storage_options).to_pandas()

# query data using duckdb
df_movies_delta_query = duckdb.sql("select *, 'abc' as new_col from df_movies_delta").df()

# diplay data
display(df_movies_delta_query)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import duckdb

# use duckdb delta scan to query delta table directly
display(duckdb.sql("select *, 1 as new_col from delta_scan('/lakehouse/default/Tables/Python/movies')").df())



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
