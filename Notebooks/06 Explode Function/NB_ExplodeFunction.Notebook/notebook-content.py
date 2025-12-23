# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
df_1 = spark.sql('''
SELECT 1 AS id, array(10, 20, 30) AS values UNION ALL
SELECT 2 AS id, array(40, 50) AS values UNION ALL
SELECT 3 AS id, array(60, 70, 80, 90) AS values;
''')
df_1.createOrReplaceTempView("df_1_view")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM df_1_view

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 
# MAGIC id
# MAGIC ,EXPLODE(values) AS value
# MAGIC FROM df_1_view


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_2 = spark.sql('''
SELECT 1 AS id, 'John' AS name, array(array(10, 20), array(30, 40)) AS nested_values UNION ALL
SELECT 2 AS id, 'Jane' AS name, array(array(50, 60), array(70)) AS nested_values UNION ALL
SELECT 3 AS id, 'Mike' AS name, array(array(80), array(90, 100)) AS nested_values;
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

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC id
# MAGIC ,name, EXPLODE(nested_values) AS values
# MAGIC FROM df_2_view;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC id
# MAGIC ,name
# MAGIC ,EXPLODE(values) AS value
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC   id
# MAGIC   ,name
# MAGIC   ,EXPLODE(nested_values) AS values
# MAGIC   FROM df_2_view
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F

# Sample data with arrays
data = [
    (1, [10, 20, 30]),
    (2, [40, 50]),
    (3, [60, 70, 80])
]

# Create a DataFrame
df = spark.createDataFrame(data, ["id", "values"])

# Display the original DataFrame
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Use explode to flatten the array
exploded_df = df.select("id", F.explode("values").alias("value"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Display the exploded DataFrame
display(exploded_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
