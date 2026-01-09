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

# Exploramos lso datos

#trmpratura maxima
tmax = spark.sql(
    """
    SELECT date(time) as fecha, TxPM, CodigoNacional from LH_BRONCE_REPASO.tmaxima_dmc
    """

)

#temperatura minima
tmin = spark.sql(
    """
    SELECT date(time) as fecha, TnAM, CodigoNacional from LH_BRONCE_REPASO.tminima_dmc
    """

)
temperaturas = spark.sql(
    """
     SELECT date(A.time) as fecha, A.TxPM as Tmaxima, B.TnAM  as Tminima, cast(A.CodigoNacional as int) as CodigoNacional
     from LH_BRONCE_REPASO.tmaxima_dmc A
     left JOIN   LH_BRONCE_REPASO.tminima_dmc B
     ON date(A.time) = date(B.time)
     and A.CodigoNacional = B.CodigoNacional
    """


)
temperaturas.write.mode("overwrite").option("overwriteSchema","true").saveAsTable("LH_Silver_Repaso.ftTemperaturas_EstacionporDia")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
