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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Traigotemperatura para agbregar

mensual = spark.sql(
    """
    select CodigoNacional ,month(fecha) as mes,
        year(fecha) as anno,
        avg(Tmaxima) as media_tmaxima,
        avg(Tminima) as medida_tminima
         from fttemperaturas_estacionpordia
         group by CodigoNacional,year(fecha), month(fecha)
    """
)

mensual.write.mode("overwrite").saveAsTable("LH_SILVER_Repaso.ft_Temperaturas_estacion_mes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
