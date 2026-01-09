# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7380ee72-9d05-4fda-80fe-bbe2636504b8",
# META       "default_lakehouse_name": "LH_ShorCut",
# META       "default_lakehouse_workspace_id": "cda65b5e-066b-49b9-8dd8-f5adbf875fd3",
# META       "known_lakehouses": [
# META         {
# META           "id": "7380ee72-9d05-4fda-80fe-bbe2636504b8"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
df = spark.sql(
"SELECT * FROM LH_ShorCut.dbo_1.dimension_city"

)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
