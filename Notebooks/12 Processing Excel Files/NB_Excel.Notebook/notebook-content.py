# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d87f4947-dd7f-4e1e-aa47-e3fa60786a4f",
# META       "default_lakehouse_name": "LH_Excel",
# META       "default_lakehouse_workspace_id": "d0423244-dbb2-439d-a787-3fb5d6a66f24",
# META       "known_lakehouses": [
# META         {
# META           "id": "d87f4947-dd7f-4e1e-aa47-e3fa60786a4f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import pandas as pd

lakehouse_Path = "abfss://DP700_Notebooks@onelake.dfs.fabric.microsoft.com/LH_Excel.Lakehouse/Files/Excel"

df_Excel = pd.read_excel(f"{lakehouse_Path}/file_1.xlsx")

print(type(df_Excel))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_Excel)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Spark = spark.createDataFrame(df_Excel)

print(type(df_Spark))
display(df_Spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a schema if not exists and write the spark df to lakehouse table
spark.sql("CREATE SCHEMA IF NOT EXISTS EXCEL")
df_Spark.write.mode("overwrite").format("delta").saveAsTable("EXCEL.file_1")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_read = spark.sql("Select * from LH_Excel.EXCEL.file_1")
display (df_read)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Acceso directo a los archivos Delta
path = "abfss://DP700_Notebooks@onelake.dfs.fabric.microsoft.com/LH_Excel.Lakehouse/Tables/Excel/file_1"
df = spark.read.format("delta").load(path)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sintaxis profesional
df2 = spark.read.table("LH_Excel.EXCEL.file_1")
display(df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Ahora lectura de un fichero excel con dos hojas, lapriemra no tiene datos solouna frase y la segunda, tuiene un texto en la primera fila, luego varias filas en blanco, despues lso datos y despues otras filas en blanco y un ultimo texto.
# # Con los parametros de la función read_excel se hacen alguans transformqaciones

# CELL ********************

# Import pandas library
import pandas as pd

# Lakehouse file path to a variable
lh_file_path = "abfss://DP700_Notebooks@onelake.dfs.fabric.microsoft.com/LH_Excel.Lakehouse/Files/Excel"
print(lh_file_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read excel to a pandas df
df_pd = pd.read_excel(f"{lh_file_path}/file_2.xlsx",\
        sheet_name="data_2",\
        header=None,\
        skiprows=3,\
        skipfooter=2,\
        names=["date","col1","col2","col3"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Conver to a spark df
df_spark = spark.createDataFrame(df_pd)

# Display the spark df
display(df_spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS EXCEL")
df_spark.write.mode("overwrite").format("delta").saveAsTable("EXCEL.file_2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read data from the lakehouse table
df_table = spark.sql("SELECT * FROM LH_Excel.Excel.file_2")
display(df_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
