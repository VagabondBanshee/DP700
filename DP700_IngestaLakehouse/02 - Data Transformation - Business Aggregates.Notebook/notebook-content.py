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

# #### Approach #1 - sale_by_date_city
# In this cell, you are creating three different Spark dataframes, each referencing an existing delta table.

# CELL ********************

df_fact_sale = spark.read.table("dbo.fact_sale") 
df_dimension_date = spark.read.table("dbo.dimension_date")
df_dimension_city = spark.read.table("dbo.dimension_city")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# In this cell, you are joining these tables using the dataframes created earlier, doing group by to generate aggregation, renaming few of the columns and finally writing it as delta table in the _Tables_ section of the lakehouse.

# CELL ********************

from pyspark.sql import functions as f
import notebookutils.mssparkutils.fs as fs

# ---------------------------------------------------------
# 1. DEFINICIÓN DE TRANSFORMACIONES
# ---------------------------------------------------------
# Optimizamos la agregación para hacerla en un solo paso sin múltiples withColumnRenamed
sale_by_date_city = df_fact_sale.alias("sale") \
    .join(df_dimension_date.alias("date"), df_fact_sale.InvoiceDateKey == df_dimension_date.Date, "inner") \
    .join(df_dimension_city.alias("city"), df_fact_sale.CityKey == df_dimension_city.CityKey, "inner") \
    .groupBy(
        "date.Date", 
        "date.CalendarMonthLabel", 
        "date.Day", 
        "date.ShortMonth", 
        "date.CalendarYear", 
        "city.City", 
        "city.StateProvince", 
        "city.SalesTerritory"
    ) \
    .agg(
        f.sum("sale.TotalExcludingTax").alias("SumOfTotalExcludingTax"),
        f.sum("sale.TaxAmount").alias("SumOfTaxAmount"),
        f.sum("sale.TotalIncludingTax").alias("SumOfTotalIncludingTax"),
        f.sum("sale.Profit").alias("SumOfProfit")
    ) \
    .orderBy("date.Date", "city.StateProvince", "city.City")

# ---------------------------------------------------------
# 2. CONFIGURACIÓN Y LIMPIEZA DE TABLA (SOLUCIÓN DEL ERROR)
# ---------------------------------------------------------
table_name = "aggregate_sale_by_date_city"
table_path = f"Tables/{table_name}"

print(f"Iniciando proceso para: {table_name}")

# A. Borramos del Metastore (Catálogo)
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# B. Borramos archivos físicos SOLO si existen (Evita el error PathNotFound)
try:
    if fs.exists(table_path):
        fs.rm(table_path, True)
        print("Archivos residuales eliminados correctamente.")
    else:
        print("No se encontraron archivos previos, continuando limpieza.")
except Exception as e:
    print(f"Aviso no bloqueante durante limpieza: {e}")

# ---------------------------------------------------------
# 3. ESCRITURA PROFESIONAL (Lakehouse Managed Table)
# ---------------------------------------------------------
sale_by_date_city.write.mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"ÉXITO: Tabla '{table_name}' creada y registrada en el Lakehouse.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. VERIFICACIÓN DE TABLAS EXISTENTES
print("--- Tablas disponibles en el catálogo ---")
# Listamos las tablas para ver si se llaman 'fact_sale' o 'dbo.fact_sale'
tables = spark.catalog.listTables()
for t in tables:
    print(f"Nombre: {t.name} | Base de datos: {t.database} | Tipo: {t.tableType}")

print("\n--- PRUEBA DE EJECUCIÓN DE LA CONSULTA ---")

# 2. INTENTO DE EJECUCIÓN (Sin crear vista, solo para probar el SELECT)
# He quitado los 'dbo.' preventivamente, pero si fallara, el error nos dirá por qué.
sql_debug = """
SELECT
    DD.Date, DD.CalendarMonthLabel
    , DD.Day, DD.ShortMonth AS Month, CalendarYear AS Year
    ,DE.PreferredName, DE.Employee
    ,SUM(FS.TotalExcludingTax) AS SumOfTotalExcludingTax
    ,SUM(FS.TaxAmount) AS SumOfTaxAmount
    ,SUM(FS.TotalIncludingTax) AS SumOfTotalIncludingTax
    ,SUM(Profit) AS SumOfProfit 
FROM fact_sale FS
INNER JOIN dimension_date DD ON FS.InvoiceDateKey = DD.Date
INNER JOIN dimension_Employee DE ON FS.SalespersonKey = DE.EmployeeKey
GROUP BY DD.Date, DD.CalendarMonthLabel, DD.Day, DD.ShortMonth, DD.CalendarYear, DE.PreferredName, DE.Employee
ORDER BY DD.Date ASC, DE.PreferredName ASC, DE.Employee ASC
"""

try:
    # Intentamos ejecutar y mostrar solo 5 filas para verificar
    df_debug = spark.sql(sql_debug)
    print("¡ÉXITO! La consulta funciona correctamente (sin 'dbo.').")
    df_debug.show(5)
except Exception as e:
    print("ERROR AL EJECUTAR LA CONSULTA:")
    print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Approach #2 - sale_by_date_employee
# In this cell, you are creating a temporary Spark view by joining 3 tables, doing group by to generate aggregation, renaming few of the columns. 

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sale_by_date_employee
# MAGIC AS
# MAGIC SELECT
# MAGIC 	DD.Date, DD.CalendarMonthLabel
# MAGIC     , DD.Day, DD.ShortMonth Month, CalendarYear Year
# MAGIC 	,DE.PreferredName, DE.Employee
# MAGIC 	,SUM(FS.TotalExcludingTax) SumOfTotalExcludingTax
# MAGIC 	,SUM(FS.TaxAmount) SumOfTaxAmount
# MAGIC 	,SUM(FS.TotalIncludingTax) SumOfTotalIncludingTax
# MAGIC 	,SUM(Profit) SumOfProfit 
# MAGIC FROM fact_sale FS
# MAGIC INNER JOIN dimension_date DD ON FS.InvoiceDateKey = DD.Date
# MAGIC INNER JOIN dimension_Employee DE ON FS.SalespersonKey = DE.EmployeeKey
# MAGIC GROUP BY DD.Date, DD.CalendarMonthLabel, DD.Day, DD.ShortMonth, DD.CalendarYear, DE.PreferredName, DE.Employee
# MAGIC ORDER BY DD.Date ASC, DE.PreferredName ASC, DE.Employee ASC

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# In this cell, you are reading from the temporary Spark view created in the previous cell and and finally writing it as delta table in the _Tables_ section of the lakehouse.

# CELL ********************

sale_by_date_employee = spark.sql("SELECT * FROM sale_by_date_employee")
sale_by_date_employee.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("aggregate_sale_by_date_employee")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
