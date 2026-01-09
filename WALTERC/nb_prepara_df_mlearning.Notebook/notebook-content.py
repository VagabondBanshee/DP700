# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2927b450-8642-4028-8efa-d319658828d9",
# META       "default_lakehouse_name": "LH_GOLD_WalterC_nmd",
# META       "default_lakehouse_workspace_id": "3fc34cb6-8e3b-45a4-b6b3-4b99a6e3b75e",
# META       "known_lakehouses": [
# META         {
# META           "id": "2927b450-8642-4028-8efa-d319658828d9"
# META         },
# META         {
# META           "id": "e2cbbeb9-20a1-42de-884b-599d190f9e21"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Para explotar un modelo semantico directamente como fuentes de datos para Machine Learning utilizaremos SEMPY
# Semantic Python => Semantic Link

#Primnero se isntala
%pip install semantic-link
#Carga la extension Sempy
%load_ext sempy

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Explorar los modelos semanticos del area de trabajo

#Primero se importa la libreria
import sempy.fabric as fab

#listamos lso datasets
mSemantics = fab.list_datasets()
mSemantics

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Establecemos en una variable el nombre del modelo semantico con el que trabajar
my_sm = "sm_temperaturas_dia"

#m ostramos las tablñas de mi modelo semantico
tablas = fab.list_tables(my_sm)
tablas
#Muestra las columans de cada tabla
columnas = fab.list_columns(my_sm)
columnas

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

# CELL ********************

#Vista previa de la tabla de hechos
df_Hechos = fab.read_table(my_sm,"ft_temperaturas_estacion_dia")
display(df_Hechos)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Enriquecemos l tabla hechos con datos de estaciones
from pyspark.sql.functions import col, to_date, lit, broadcast
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df_hechos_fab = fab.read_table(my_sm, "ft_temperaturas_estacion_dia")
df_hechos = spark.createDataFrame(df_hechos_fab) # Se convierte en DataFrame de Spark, el anterior era de Fabric y tiene menos caracteristicas

df_hechos = df_hechos.withColumn("fecha", to_date(col("fecha"), "yyyy-MM-dd"))

# Filtrar con between y descartando fechas nulas
df_filtrado = df_hechos.filter(
    col("fecha").isNotNull() &
    col("fecha").between(to_date(lit("1978-01-01")), to_date(lit("1993-12-31")))
)

df_estaciones_fab = fab.read_table(my_sm, "dimEstacionesMeterologicas")
df_estaciones = spark.createDataFrame(df_estaciones_fab) # Se convierte en DataFrame de Spark

# Se hace un join de las tablas

df_join = df_filtrado.join(
    df_estaciones,
    df_filtrado["codigoNacional"] == df_estaciones["CodigoNacional"],
    how="inner"
)

# Selección final de columnas (evita duplicados)
cols_out = [
    "fecha", "TMAXIMA", "TMINIMA",
    df_filtrado["CodigoNacional"].alias("CodigoNacional"),
    # agrega aquí las columnas que te interesen de la dimensión:
    "nombreEstacion", "latitud", "longitud"
]

df_out = df_join.select(*cols_out)

display(df_out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Guardamos los datos como informacion de entrenamiento de ML

df_out.write.mode("overwrite").saveAsTable("LH_GOLD_WalterC_nmd.ml_train_temperaturas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
