# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "14e976fa-975a-4901-af55-6da567f2bfd6",
# META       "default_lakehouse_name": "LH_Silver_Repaso",
# META       "default_lakehouse_workspace_id": "d62a3a18-b622-4f96-af59-07c7c3b40eed",
# META       "known_lakehouses": [
# META         {
# META           "id": "14e976fa-975a-4901-af55-6da567f2bfd6"
# META         },
# META         {
# META           "id": "9e6698f7-a598-4fc0-b823-d10424ba4a7d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Construir la diemnsion de tiempo apartir de la tempertura

#seimportrn las librerias

from pyspark.sql.functions import col, to_date, date_format

df = spark.table("LH_REPASO.tmaxima_dmc")

#seleccionamos la columna time yslo vaores unicos
df_time = df.select("time").distinct()

#agrergamos columnas solo de fecha y solo de hora

df_th = (
        df_time.withColumn("fecha",to_date(col("time")))
                .withColumn("hora", date_format(col("time"),"HH:mm:ss"))
)



#Agregamos nuevas funciones
from pyspark.sql.functions import year, dayofmonth, dayofweek, dayofyear, month
df_calendar= (
        df_th.withColumn("anno",year(col("fecha")))
                .withColumn("mes", month(col("fecha")))
                .withColumn("diaSemana", dayofweek(col("fecha")))
                .withColumn("diaMes", dayofmonth(col("fecha")))
                .withColumn("diaAño", dayofyear(col("fecha")))


)

df_calendar.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
