# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "20348c4c-2778-4764-acf3-e36cdfa1a4ef",
# META       "default_lakehouse_name": "LH_Bronce",
# META       "default_lakehouse_workspace_id": "1550361c-4683-4904-ad71-99a2e75aa22c",
# META       "known_lakehouses": [
# META         {
# META           "id": "20348c4c-2778-4764-acf3-e36cdfa1a4ef"
# META         },
# META         {
# META           "id": "d9d0f3c6-83c1-4961-842d-11febfc6aebf"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# =====================================================================
# EJERCICIO 1: ETL de Bronce a Plata (Método OneLake)
# =====================================================================
# Objetivo: 
# 1. Leer datos crudos (Parquet) desde LH_Bronce.
# 2. Limpiar y transformar los datos.
# 3. Escribir el resultado en LH_Plata usando la ruta absoluta de OneLake.
# =====================================================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1: Leer datos crudos (Bronce)
# ==================================
# La ruta a la carpeta 'Files' del Lakehouse actual es RELATIVA
# (sin la barra '/' al principio).

# ¡Este era el error!
ruta_lectura_bronce = "Files/yellow_tripdata_2023-01.parquet"

df_bronze = spark.read.format("parquet").load(ruta_lectura_bronce)

print("Datos de Bronce cargados en 'df_bronze'.")
display(df_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 2: Transformar datos (Plata)
# ==================================
# (Este código es idéntico, es correcto)

from pyspark.sql.functions import col, to_timestamp

df_silver = df_bronze.select(
    to_timestamp(col("tpep_pickup_datetime")).alias("pickup_datetime"),
    to_timestamp(col("tpep_dropoff_datetime")).alias("dropoff_datetime"),
    col("passenger_count").cast("int").alias("passenger_count"),
    col("trip_distance").cast("double").alias("trip_distance"),
    col("PULocationID").alias("pickup_location_id"),
    col("DOLocationID").alias("dropoff_location_id"),
    col("total_amount").cast("double").alias("total_amount"),
    col("payment_type").alias("payment_type")
)

df_silver = df_silver.filter(
    (col("passenger_count") > 0) & 
    (col("trip_distance") > 0)
)

print("Datos transformados en 'df_silver'.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 3: Escribir DataFrame en LH_Plata (CORREGIDO OTRA VEZ)
# ==========================================================
#
# NUEVA HIPÓTESIS: La carpeta "/dbo/" es virtual y no se puede escribir
# en ella directamente.
#
# SOLUCIÓN: Escribiremos la tabla en la "raíz" del Lakehouse de Plata.
# El Lakehouse debería detectarla y registrarla en su catálogo.

path_escritura_final = "/LH_PlataSE.Lakehouse/TaxisNY_Plata" # ¡Hemos quitado /dbo/!

print(f"Escribiendo datos en la ruta OneLake (sin /dbo/): {path_escritura_final}")

# Escribimos directamente en esa ruta
df_silver.write.format("delta").mode("overwrite").save(path_escritura_final)

print("---")
print("¡ÉXITO! (Esperado) Escritura completada.")
print("Por favor, ve a la UI de LH_Plata y ACTUALIZA.")
print("La tabla debería aparecer (quizás en la raíz, o quizás registrada dentro de 'dbo').")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
