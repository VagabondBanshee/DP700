# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e2cbbeb9-20a1-42de-884b-599d190f9e21",
# META       "default_lakehouse_name": "LH_SILVER_WalterC_nmd",
# META       "default_lakehouse_workspace_id": "3fc34cb6-8e3b-45a4-b6b3-4b99a6e3b75e",
# META       "known_lakehouses": [
# META         {
# META           "id": "e2cbbeb9-20a1-42de-884b-599d190f9e21"
# META         },
# META         {
# META           "id": "0d168bb8-a587-43b6-b1f5-5c4d9d49b980"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Contruir una Dimensión de Tiempo

# Importar Librerias y funciones a utiliar

from pyspark.sql.functions import col, date_format, to_date

#Cargar DAtos
dfTempMinima = spark.table("LH_BRONZE_WAlterC_nmd.temperatura_minima")

#seelccionamos unicamente el campo time y elimimamos duplciados
dfTempMinima_Time = dfTempMinima.select("time")

#agregar columans solo fecha y solo hora
df_th = (
    dfTempMinima_Time .withColumn("fecha",to_date(col("time")))
                      .withColumn("hora", date_format(col("time"),"HH:mm:ss"))
)
df_th.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Agregar más columnas al dataframe df_th

#Se importan nuevas funciones
from pyspark.sql.functions import year, month, dayofmonth, dayofyear, quarter, dayofweek

dimTiempo = (
    df_th .withColumn("anno", year(col("fecha")))
          .withColumn("mes", month(col("fecha")))
          .withColumn("trimestre", quarter(col("fecha")))
            .withColumn("nom_trimestre", quarter(col("fecha")))
          .withColumn("dia", dayofmonth(col("fecha")))
          .withColumn("nombre_dia", date_format(col("fecha"),"EEEE"))
          .withColumn("dia_anno", dayofyear(col("fecha")))
          .withColumn("dia_semana", dayofweek(col("fecha")))

)
dimTiempo.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Se materialiaz la dimension tiempo en el lakehouse Silver
dimTiempo.write.mode("overwrite").saveAsTable("LH_SILVER_WalterC_nmd.dimTiempo_fecha_hora")

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
