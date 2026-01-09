# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ded3efd6-fb5d-4116-8a13-b624e22cc67f",
# META       "default_lakehouse_name": "LH_DP700_Alex",
# META       "default_lakehouse_workspace_id": "96ade54f-a71f-442d-ada7-eca1dfaf62cd",
# META       "known_lakehouses": [
# META         {
# META           "id": "ded3efd6-fb5d-4116-8a13-b624e22cc67f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # DP-700 Examp Prep Episode 009: Notebooks
# 


# MARKDOWN ********************

# # 🔹 Parameters

# PARAMETERS CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

dataset= "movies"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #  🔹 Reading data from a file in Lakehouse

# CELL ********************

df = spark.read.format("csv").option("header","true").load(f"Files/dp700_e009/{dataset}.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🔹 Displaying data

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🔹 Using Spark SQL 

# CELL ********************

df.createOrReplaceTempView("df_view")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC *
# MAGIC ,now() AS ts
# MAGIC  FROM df_view


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ts = spark.sql('''
    SELECT
    *
    ,now() AS ts
    FROM df_view
''')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🔹 Writing data to a table in Lakehouse

# CELL ********************

spark.sql('''
CREATE SCHEMA IF NOT EXISTS dp700_e009
''')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ts.write.mode("overwrite").format("delta").saveAsTable(f"dp700_e009.{dataset}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🔹 Reading data from a table in Lakehouse

# CELL ********************

df_table = spark.sql(f"SELECT * FROM lh_dp700_Alex.dp700_e009.{dataset}")
display(df_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🔹 Using NotebooUtils to set an exit value for the notebook

# CELL ********************

notebookutils.notebook.exit(f"Dataset {dataset} was processed ok!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
