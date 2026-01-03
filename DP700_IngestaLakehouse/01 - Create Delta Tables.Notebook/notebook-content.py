# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "293be51c-14c1-4c07-a4fb-9f6414847571",
# META       "default_lakehouse_name": "LH_WWI",
# META       "default_lakehouse_workspace_id": "7d3c6aa0-cca0-460b-9335-8894c5a189c8",
# META       "known_lakehouses": [
# META         {
# META           "id": "293be51c-14c1-4c07-a4fb-9f6414847571"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Spark session configuration
# This cell sets Spark session settings to enable _Verti-Parquet_ and _Optimize on Write_. More details about _Verti-Parquet_ and _Optimize on Write_ in tutorial document.

# CELL ********************

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Fact - Sale
# 
# This cell reads raw data from the _Files_ section of the lakehouse, adds additional columns for different date parts and the same information is being used to create partitioned fact delta table.

# CELL ********************

from pyspark.sql.functions import col, year, month, quarter

# Nombre de la tabla (sin la ruta 'Tables/')
table_name = 'fact_sale'

df = spark.read.format("parquet").load('Files/wwi-raw-data/full/fact_sale_1y_full')

# Transformaciones
df = df.withColumn('Year', year(col("InvoiceDateKey")))
df = df.withColumn('Quarter', quarter(col("InvoiceDateKey")))
df = df.withColumn('Month', month(col("InvoiceDateKey")))

# ESCRITURA CORRECTA
# Nota: No indicamos "Tables/...", solo el nombre de la tabla.
df.write.mode("overwrite") \
    .format("delta") \
    .partitionBy("Year", "Quarter") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dimensions
# This cell creates a function to read raw data from the _Files_ section of the lakehouse for the table name passed as a parameter. Next, it creates a list of dimension tables. Finally, it has a _for loop_ to loop through the list of tables and call above function with each table name as parameter to read data for that specific table and create delta table.

# CELL ********************

from pyspark.sql.types import *

# Definimos la lista de tablas primero
full_tables = [
    'dimension_city',
    'dimension_customer',
    'dimension_date',
    'dimension_employee',
    'dimension_stock_item'
]

def loadFullDataFromSource(table_name):
    print(f"Procesando tabla: {table_name}...")
    
    # 1. LIMPIEZA PREVIA (Arquitectura robusta)
    # Borramos la tabla del metastore para evitar conflictos de esquema antiguos
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    
    # Borramos físicamente los archivos por si quedaron residuos huérfanos (doble seguridad)
    mssparkutils.fs.rm(f"Tables/{table_name}", True)

    # 2. LECTURA
    # Usamos f-strings para que sea más limpio que concatenar con +
    df = spark.read.format("parquet").load(f'Files/wwi-raw-data/full/{table_name}')

    # 3. TRANSFORMACIÓN
    # Tu lógica es correcta, pero .drop es más legible y eficiente si la columna existe
    if "Photo" in df.columns:
        df = df.drop("Photo")
    
    # 4. ESCRITURA PROFESIONAL
    # Usamos saveAsTable para registrarla como Managed Table en el Lakehouse
    df.write.mode("overwrite") \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
        
    print(f"Tabla {table_name} creada correctamente.")

# Ejecución del bucle
for table in full_tables:
    loadFullDataFromSource(table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
