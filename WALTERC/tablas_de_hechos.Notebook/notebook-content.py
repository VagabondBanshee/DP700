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

# EXPLORAMOS LOS DATOS PARA CREAR

#temperatura maxima
tmax = spark.sql(
   """
   SELECT date(time) as fecha, TxPM,codigoNacional FROM LH_BRONZE_WAlterC_nmd.temperatura_maxima
   """
)

tmax.show(10)

#temperatura minima
tmin = spark.sql(
    """
   SELECT date(time) as fecha, TnAM,codigoNacional FROM LH_BRONZE_WAlterC_nmd.temperatura_minima
   """
)

tmin.show(10)

temperaturas = spark.sql("""

 SELECT date(tMAX.time) as fecha, tMAX.TxPM as TMAXIMA,tMIN.TnAM as TMINIMA,cast(tMAX.codigoNacional as int) as codigoNacional FROM LH_BRONZE_WAlterC_nmd.temperatura_maxima tMAX
 join   LH_BRONZE_WAlterC_nmd.temperatura_minima tMIN
 on tMAX.CodigoNacional = tMIN.codigoNacional
 and date(tMAX.time) = date(tMIN.time)
""")
temperaturas.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Guardo la tabla de Hechos en el LH_Silver

temperaturas.write.mode("overwrite").option("overwriteSchema","true").saveAsTable("LH_SILVER_WalterC_nmd.ft_Temperaturas_estacion_dia")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
