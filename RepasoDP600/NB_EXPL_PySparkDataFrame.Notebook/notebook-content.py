# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "23c414e9-32db-42aa-b901-82045153e0b5",
# META       "default_lakehouse_name": "Lakehouse",
# META       "default_lakehouse_workspace_id": "ac5ff783-1d70-4566-bd6f-fecddcb57ce8",
# META       "known_lakehouses": [
# META         {
# META           "id": "23c414e9-32db-42aa-b901-82045153e0b5"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Explorando datos con un DataFrame Spark

# CELL ********************

from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import seaborn as sns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table('AW_Customer')
display(df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Análisis general

# Número de filas y columnas
num_rows = df.count()
num_cols = len(df.columns)
print("Número de filas:", num_rows)
print("Número de columnas:", num_cols)

# Valores nulos por columna
null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
print("\nValores nulos por columna:")
null_counts.show()

# Porcentaje de nulos por columna
null_percentages = df.select([
    (F.count(F.when(F.col(c).isNull(), c)) / num_rows * 100).alias(c)
    for c in df.columns
])
print("Porcentaje de nulos por columna:")
null_percentages.show()

# Tipos de datos
print("\nTipos de datos por columna:")
print(df.dtypes)

# Valores únicos por columna
print("\nValores únicos por columna:")
for c in df.columns:
    print(f"{c}: {df.select(c).distinct().count()}")

# Estadísticas básicas numéricas y de fecha
print("\nDescripción estadística:")
df.describe().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Agrupaciones

df.groupBy("Title").count().orderBy(F.desc("count")).show(truncate=False)
df.groupBy("SalesPerson").count().orderBy(F.desc("count")).show(truncate=False)
print("Compañías únicas:", df.select("CompanyName").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Conversión de fecha y análisis por año

df = df.withColumn("ModifiedDate", F.to_date("ModifiedDate"))
df.select(F.min("ModifiedDate"), F.max("ModifiedDate")).show()

df.groupBy(F.year("ModifiedDate").alias("Year")).count().orderBy("Year").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Duplicados exactos
duplicates = df.groupBy(df.columns).count().filter("count > 1")
print("Filas duplicadas exactas:")
duplicates.show()

# Duplicados por nombre + empresa
dup_names = df.groupBy("FirstName", "LastName", "CompanyName").count().filter("count > 1")
print("Duplicados por nombre y compañía:")
dup_names.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Longitud del email
df = df.withColumn("EmailLength", F.length("EmailAddress"))
df.select(F.min("EmailLength"), F.max("EmailLength"), F.avg("EmailLength")).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
