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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# modelo predictivo de temperaturas basado en datos historicos
#importamos librerias

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year

df=spark.read.table("LH_GOLD_Repaso.dbo.ml_train_temperaturas")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_convertido = (

    df.withColumn("anno",year(col("fecha")))
)
display(df_convertido)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#paso 2 añadimos los lags
from pyspark.sql.functions import lag
from pyspark.sql.window import Window
windows_spec = Window.partitionBy("NombreEstacion").orderBy("fecha")

for col_name in ["Tmaxima","Tminima"]:
    df_convertido =(
        df_convertido.withColumn(f"{col_name}_lag1", lag(col_name,1).over(windows_spec))
        .withColumn(f"{col_name}_lag2", lag(col_name,2).over(windows_spec))
    )
#limpiamos los nulos
df_ml =  df_convertido.dropna()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Paso Vector Assembler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

features = [
    "Tmaxima_lag1", "Tmaxima_lag2",
    "Tminima_lag1", "Tminima_lag2"
]

asseembler = VectorAssembler(inputCols=features, outputCol="features")
df_ml = asseembler.transform(df_ml)

lr_Tmax = LinearRegression(featuresCol= "features", labelCol="Tmaxima")
lr_Tmin = LinearRegression(featuresCol= "features", labelCol="Tminima")
model_tmax = lr_Tmax.fit(df_ml)
model_tmin = lr_Tmin.fit(df_ml)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Ahora crearemos datos ficticios y aplicaremos el modelo a esos dtos ficticios

# CELL ********************

from pyspark.sql.functions import sequence, date_add, current_date, explode, col, expr

# --- ¿Qué se intenta hacer aquí? ---
# Se necesita una lista de todas las fechas futuras para las cuales vamos a generar una predicción.
# El plan es:
# 1. Obtener la fecha actual como punto de partida.
# 2. Generar una secuencia de fechas desde mañana (hoy + 1 día) hasta 730 días en el futuro.
# 3. Convertir esa secuencia (que es un array) en filas individuales, una por cada fecha.
# 4. El resultado final, `fechas_futuras`, será un DataFrame de Spark con una única columna "fecha".

fechas_futuras = (
    # Empezamos con un DataFrame que solo tiene la fecha de hoy.
    spark.sql("SELECT current_date() AS today")
    .select(
        # `explode` es una función que toma un array y crea una nueva fila por cada elemento del array.
        explode(
            # `sequence` crea un array de fechas. Le damos una fecha de inicio, una de fin y un intervalo.
            sequence(
                # Inicio: Mañana.
                date_add(col("today"), 1),
                # Fin: Hoy más 730 días.
                date_add(col("today"), 730),
                # Intervalo: Avanzar de 1 en 1 día.
                expr("interval 1 day")
            )
        # Le damos el nombre "fecha" a la nueva columna que contiene las fechas individuales.
        ).alias("fecha")
    )
)

# Mostramos las primeras 5 fechas para verificar que todo está correcto y su esquema.
print("Fechas futuras generadas:")
fechas_futuras.show(5)
fechas_futuras.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import sequence, date_add, current_date, explode, col, expr
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, StringType
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import col

# --- ¿Qué se intenta hacer aquí? ---
# El objetivo es predecir la temperatura máxima y mínima para cada estación y cada fecha futura.
# El proceso es secuencial: para predecir el día 3, necesitamos el resultado del día 2, etc.
# Por eso, seguimos estos pasos:
# 1. Traer a la memoria del driver las listas de estaciones y fechas futuras (son datos pequeños, así que es eficiente).
# 2. Recorrer cada estación una por una.
# 3. Para cada estación, obtener las dos últimas mediciones reales. Serán la "semilla" para empezar a predecir.
# 4. Recorrer cada fecha futura y:
#    a. Crear un "vector de características" con los datos de los dos días anteriores.
#    b. Usar los modelos `model_tmax` y `model_tmin` para predecir.
#    c. Guardar la predicción.
#    d. Actualizar los datos de "los dos días anteriores" con la nueva predicción para que sirva de entrada en la siguiente iteración.

# 1. Traemos las listas de estaciones y fechas a Python para poder iterar sobre ellas.
# Usamos `.collect()` que convierte un DataFrame de Spark en una lista de objetos Row en el driver.
estaciones_lista = [row.NombreEstacion for row in df.select("NombreEstacion").distinct().collect()]
fechas_futuras_lista = [row.fecha for row in fechas_futuras.collect()]

# Creamos una lista vacía de Python para ir guardando los resultados.
filas_pred = []

# 2. Bucle principal que recorre cada estación.
print(f"Iniciando predicciones para {len(estaciones_lista)} estaciones...")
for estacion in estaciones_lista:
    
    # 3. Obtenemos las 2 últimas observaciones reales para esta estación.
    last_obs_pd = (
        df.filter(col("NombreEstacion") == estacion)
          .orderBy(col("fecha").desc())
          .limit(2)
          .toPandas() # .toPandas() aquí es correcto porque solo traemos 2 filas.
    )
    
    if len(last_obs_pd) < 2:
        print(f"Saltando estación '{estacion}' por no tener suficientes datos.")
        continue
    
    # Convertimos las filas a diccionarios para que sea más fácil de manejar.
    # lag1 = día más reciente, lag2 = día anterior al más reciente.
    lag1 = last_obs_pd.iloc[0].to_dict()
    lag2 = last_obs_pd.iloc[1].to_dict()

    # 4. Bucle anidado que recorre las fechas futuras para esta estación.
    for fecha_futura in fechas_futuras_lista:
        # a. Creamos el vector de características que necesita el modelo.
        features_vector = Vectors.dense([
            float(lag1["Tmaxima"]), float(lag2["Tmaxima"]),
            float(lag1["Tminima"]), float(lag2["Tminima"])
        ])
        
        # Creamos un DataFrame de Spark de una sola fila para poder usar el método `.transform()` del modelo.
        df_features = spark.createDataFrame([(features_vector,)], ["features"])
        
        # b. Realizamos las predicciones. `.collect()[0]` extrae el valor de la predicción.
        pred_tmax = model_tmax.transform(df_features).collect()[0].prediction
        pred_tmin = model_tmin.transform(df_features).collect()[0].prediction
        
        # c. Guardamos el resultado (fecha, estación, predicciones) en nuestra lista de Python.
        filas_pred.append((fecha_futura, estacion, pred_tmax, pred_tmin))
        
        # d. Actualizamos los lags para la siguiente iteración.
        # El día de "ayer" (lag1) se convierte ahora en el de "anteayer" (lag2).
        lag2 = lag1.copy()
        # La predicción que acabamos de hacer se convierte en el nuevo "ayer" (lag1).
        lag1 = {
            "Tminima": pred_tmin,
            "Tmaxima": pred_tmax,
            # Añadimos otros campos por si fueran necesarios, aunque no se usan como features.
            "NombreEstacion": estacion,
            "fecha": fecha_futura
        }

print("Proceso de predicción iterativa completado.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, DateType, DoubleType, StringType

# --- ¿Qué se intenta hacer aquí? ---
# Ya hemos calculado todas las predicciones y las tenemos en una lista de Python (`filas_pred`).
# Ahora queremos:
# 1. Definir la estructura (esquema) que tendrá nuestra tabla final.
# 2. Convertir la lista de resultados de Python a un DataFrame de Spark, que es distribuido y escalable.
# 3. Guardar este DataFrame como una tabla en el catálogo de Spark para poder consultarla fácilmente con SQL o en otros notebooks.

# 1. Definimos el esquema. Es como crear la estructura de una tabla SQL antes de meterle datos.
# Le decimos el nombre y el tipo de dato de cada columna.
schema_pred = StructType([
    StructField("fecha", DateType(), True),
    StructField("nombreEstacion", StringType(), True),
    StructField("pred_tmax", DoubleType(), True),
    StructField("pred_tmin", DoubleType(), True)
])

# 2. Creamos el DataFrame de Spark a partir de la lista de Python y el esquema que definimos.
df_predicciones = spark.createDataFrame(filas_pred, schema=schema_pred)

# 3. Guardamos el DataFrame como una tabla gestionada por Spark.
#    .write              -> Inicia el proceso de escritura.
#    .mode("overwrite")  -> Si la tabla "predicciones_temperaturas_todas" ya existe, la borrará y la reemplazará por completo.
#    .saveAsTable(...)   -> Guarda el contenido como una tabla permanente en el metastore de Spark/Hive.
df_predicciones.write.mode("overwrite").saveAsTable("predicciones_temperaturas_todas")

print("¡Éxito! Predicciones para todas las estaciones completadas y guardadas en la tabla 'predicciones_temperaturas_todas'.")

# Opcional: Mostramos una muestra del resultado final.
df_predicciones.show()


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
