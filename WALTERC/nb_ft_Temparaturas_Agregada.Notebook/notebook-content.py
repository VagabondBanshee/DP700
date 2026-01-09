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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Se crea la tabla agrgada por mes

mensual = spark.sql(
    """
    select year(fecha) AS Anno
    ,month(fecha) as Mes
    ,codigoNacional
    , avg(TMAXIMA) as p_TMaxima
    , avg(TMINIMA) as p_TMINIMA
    from LH_SILVER_WalterC_nmd.ft_temperaturas_estacion_dia
    group by year(fecha), month(fecha), codigoNAcional
    """
)
mensual.show(5)

#Guardamos como tabla de Hechos
mensual.write.mode("overwrite").saveAsTable("LH_SILVER_WalterC_nmd.ft_temperaturas_estacion_mes")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
