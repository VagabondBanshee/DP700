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
# META         },
# META         {
# META           "id": "14e976fa-975a-4901-af55-6da567f2bfd6"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Para exolotar un modelo de datos directamente como fuente de datos de ml
#Importamos la libreria SEMPY Semantic Python=

%load_ext sempy

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Exploremos los modelos
import sempy.fabric as fab

nsemantic = fab.list_datasets()
display(nsemantic)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Alamacenamos el nombre del modelo semantico para trabajar con el
my_sm = "sm_TemperaturasEstacionPorDia"
#listamos sus tablas
tablas = fab.list_tables(my_sm)


columnas     = fab.list_columns(my_sm)
columnas

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_hechos = fab.read_table(my_sm,"fttemperaturas_estacionpordia")
display(df_hechos)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#enriquecemos la tabla de hechos con datos de la dimesnion estaciones meterologicas
from pyspark.sql.functions import col, to_date, lit, broadcast
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df_hechos_fab = fab.read_table(my_sm, "fttemperaturas_estacionpordia")
df_hechos = spark.createDataFrame(df_hechos_fab)

df_hechos = df_hechos.withColumn("fecha", to_date(col("fecha"), "yyyy-MM-dd"))

# Filtrar con between y descartando fechas nulas
df_filtrado = df_hechos.filter(
    col("fecha").isNotNull() &
    col("fecha").between(to_date(lit("1978-01-01")), to_date(lit("1993-12-31")))
)

df_estaciones_fab = fab.read_table(my_sm, "dimEstacionesMeterologicas")
df_estaciones = spark.createDataFrame(df_estaciones_fab)

df_join = df_filtrado.join(broadcast(df_estaciones), on="CodigoNacional", how="inner")

# Selección final de columnas (evita duplicados)
cols_out = [
    "fecha","Tmaxima", "Tminima",
    df_filtrado["CodigoNacional"].alias("CodigoNacional"),
    "NombreEstacion", "Latitud", "Longitud"
]

df_out = df_join.select(*cols_out)

display(df_out)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#SAlvar fadatframe como tabla de entrenamiento
df_out.write.mode("overwrite").option("overwriteSchema","true").saveAsTable("LH_GOLD_Repaso.dbo.ml_train_temperaturas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
