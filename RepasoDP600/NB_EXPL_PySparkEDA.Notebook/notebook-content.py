# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4f4e70ed-18ff-4a99-baf6-a51ff5d09109",
# META       "default_lakehouse_name": "LH_GOLD_Repaso",
# META       "default_lakehouse_workspace_id": "d62a3a18-b622-4f96-af59-07c7c3b40eed",
# META       "known_lakehouses": [
# META         {
# META           "id": "4f4e70ed-18ff-4a99-baf6-a51ff5d09109"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# ## Bibliotecas Python para EDA (Exploratory Data Analysis)

# CELL ********************

import sweetviz as sv

df = spark.table('predicciones_temperaturas_todas').toPandas()

report = sv.analyze(df)
report.show_notebook()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
