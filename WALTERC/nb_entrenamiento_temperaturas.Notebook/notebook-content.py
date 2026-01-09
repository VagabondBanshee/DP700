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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Modelo Predictivo de temraturabasado en datos historicos
#Importar Librerias y cargar datos
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year

#leer lso datos a entrenar
df = spark.read.table("ml_train_temperaturas")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Se enriquece el df
df_Enriquecido =(
    df.withColumn("anno", year(col("fecha")))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Importamos las librerías necesarias de PySpark.
# Window nos permite definir una "ventana" de datos sobre la que realizar cálculos.
# lag es una función que nos permite acceder a valores de filas anteriores.
from pyspark.sql.window import Window
from pyspark.sql.functions import lag

# --- Creación de la especificación de la ventana ---
# Aquí definimos cómo se agruparán y ordenarán los datos para los cálculos.
# Es como decir: "para cada estación meteorológica, ordena los datos por fecha".
# - partitionBy("nombreEstacion"): Agrupa los datos por cada estación. Los cálculos se reiniciarán para cada estación diferente.
# - orderBy("fecha"): Dentro de cada grupo (cada estación), ordena los registros cronológicamente por fecha.
# Esta ventana es crucial para asegurarnos de que los lags (valores de días anteriores) se calculen correctamente para cada estación por separado.
windw_spec = Window.partitionBy("nombreEstacion").orderBy("fecha")

# --- Bucle para crear las variables "lag" ---
# Vamos a crear lags para las columnas de temperatura máxima y mínima.
# El bucle 'for' nos permite aplicar la misma lógica a ambas columnas sin repetir código.
for col_name in ["TMAXIMA", "TMINIMA"]:
    # 'df_Enriquecido' es el DataFrame que estamos modificando.
    # El método .withColumn() añade una nueva columna al DataFrame.
    df_Enriquecido = (
        df_Enriquecido.withColumn(
            # Creamos la primera columna de lag, por ejemplo "TMAXIMA_lag1".
            # Esta columna contendrá el valor de "TMAXIMA" del día anterior.
            # lag(col_name, 1): Toma el valor de la columna 'col_name' de la fila anterior (offset de 1).
            # .over(windw_spec): Aplica esta función de lag sobre la ventana que definimos antes.
            f"{col_name}_lag1", lag(col_name, 1).over(windw_spec)
        )
        .withColumn(
            # Creamos la segunda columna de lag, por ejemplo "TMAXIMA_lag2".
            # Esta columna contendrá el valor de "TMAXIMA" de hace dos días.
            # lag(col_name, 2): Toma el valor de la columna 'col_name' de dos filas atrás (offset de 2).
            f"{col_name}_lag2", lag(col_name, 2).over(windw_spec)
        )
    )

# --- Limpieza de Nulos ---
# La función lag() no puede calcular el valor para los primeros registros de cada estación.
# Por ejemplo, el primer día no tiene un "día anterior", y el segundo día no tiene un "hace dos días".
# Esto genera valores nulos (null) en las nuevas columnas para esas primeras filas.
# Los modelos de machine learning generalmente no pueden trabajar con valores nulos.
# .dropna() elimina todas las filas que contengan al menos un valor nulo.
# El resultado es un DataFrame limpio, listo para ser usado en el entrenamiento del modelo.
df_ml = df_Enriquecido.dropna()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Importación de las librerías necesarias ---
# VectorAssembler es una herramienta para agrupar varias columnas en una sola columna de tipo vector.
# LinearRegression es el algoritmo de machine learning que vamos a usar: Regresión Lineal.
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# --- Definición de las variables predictoras (Features) ---
# Creamos una lista con los nombres de las columnas que usaremos para hacer las predicciones.
# Estas son las columnas 'lag' que creamos en el paso anterior.
# En machine learning, a estas variables de entrada se les llama "features" o "características".
features = [
    "TMAXIMA_lag1", "TMAXIMA_lag2",
    "TMINIMA_lag1", "TMINIMA_lag2"
]

# --- Ensamblador de Vectores ---
# Los modelos de Spark ML esperan que todas las variables de entrada (features) estén en una única columna.
# VectorAssembler se encarga de esto: toma las columnas de la lista 'features' y las une en un vector.
# - inputCols: Las columnas que queremos combinar.
# - outputCol: El nombre de la nueva columna que contendrá el vector. Le llamamos "features" por convención.
assembler = VectorAssembler(inputCols=features, outputCol="features")

# Aplicamos el ensamblador a nuestro DataFrame.
# Esto añade la nueva columna "features" al DataFrame `df_ml`.
df_ml = assembler.transform(df_ml)

# --- Configuración y Entrenamiento de los Modelos ---

# --- 1. Modelo para predecir la Temperatura Máxima (TMAXIMA) ---

# Creamos una instancia del modelo de Regresión Lineal.
# - featuresCol="features": Le decimos al modelo que la columna de entrada es la que creamos con el Assembler.
# - labelCol="TMAXIMA": Le indicamos cuál es la columna que queremos predecir (la "etiqueta" o "label").
lr_tmax = LinearRegression(featuresCol="features", labelCol="TMAXIMA")

# Entrenamos el modelo.
# El método .fit() es el que realiza el aprendizaje. El modelo analiza los datos en `df_ml`
# y encuentra la mejor fórmula matemática para predecir "TMAXIMA" a partir de la columna "features".
# El resultado es un modelo ya entrenado, listo para hacer predicciones.
model_tmax = lr_tmax.fit(df_ml)

# --- 2. Modelo para predecir la Temperatura Mínima (TMINIMA) ---

# Hacemos exactamente lo mismo, pero esta vez para predecir la temperatura mínima.
# Creamos otra instancia del modelo.
lr_tmin = LinearRegression(featuresCol="features", labelCol="TMINIMA")

# Entrenamos este segundo modelo. Aprenderá a predecir "TMINIMA" usando los mismos 'features' de antes.
model_tmin = lr_tmin.fit(df_ml)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###Ahora crearemos datos ficticios y aplicaremos el modelo a esos datos

# CELL ********************

# Crear fechas absurdas a partir de hoy
from pyspark.sql.functions import sequence, col, date_add, current_date, explode,expr

fechas_futuras = (
    spark.sql("SELECT current_date()  AS today")
    .select(
        explode(
            sequence(
                date_add(col("today"),1),
                date_add(col("today"),730),
                expr("interval 1 day"))).alias("fecha")).toPandas()["fecha"]

)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Paso 4: Predecir Temperaturas para fechas Futuras

from pyspark.sql.types import StructType, StructField, DateType, DoubleType, StringType
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import col # <-- Es buena práctica importar 'col'
import pandas as pd # <-- Necesario para el .copy()

# Defino el esquema de la tabla sobre la que aplicar el modelo
schema_pred = StructType([
    StructField("fecha", DateType(), True),
    StructField("nombreEstacion", StringType(), True),
    StructField("predTmax", DoubleType(), True),
    StructField("predTmin", DoubleType(), True) # <-- Corregí el nombre de la columna a predTmin para consistencia
])

# La lista contendrá tuplas con los resultados
filas_pred = [] 
estaciones = df.select("nombreEstacion").distinct().rdd.flatMap(lambda x: x).collect()

# Suponiendo que tienes una lista de fechas futuras ya definida
# Ejemplo:
from datetime import date, timedelta
fechas_futuras = [date.today() + timedelta(days=i) for i in range(1, 8)] # Predicción para los próximos 7 días


# --- INICIO DEL BLOQUE CORREGIDO ---

for estacion in estaciones:
    # Todo el siguiente bloque debe estar indentado para que se ejecute para CADA estación
    last_obs = (
        df.filter(col("nombreEstacion") == estacion)
        .orderBy(col("fecha").desc())
        .limit(2)
        .toPandas()
    )

    # Esta comprobación ahora está DENTRO del bucle de estaciones
    if len(last_obs) < 2:
        print(f"Saltando estación '{estacion}' por falta de datos.")
        continue  # Saltar al siguiente 'estacion' en el bucle

    # Aseguramos que los datos estén en el orden correcto (el más reciente primero)
    lag1_pd = last_obs.iloc[0] # El dato más reciente (ayer)
    lag2_pd = last_obs.iloc[1] # El segundo dato más reciente (anteayer)

    # Bucle anidado para predecir cada fecha futura
    for future_date in fechas_futuras:
        # Creamos el vector de características con los valores de los lags
        features_vector = Vectors.dense([
            float(lag1_pd["TMAXIMA"]), float(lag2_pd["TMAXIMA"]),
            float(lag1_pd["TMINIMA"]), float(lag2_pd["TMINIMA"])
        ])
        
        # Creamos un DataFrame temporal de Spark para hacer la predicción
        df_features = spark.createDataFrame([(features_vector,)], ["features"])

        # Hacemos las predicciones usando los modelos CORRECTOS
        pred_tmax = model_tmax.transform(df_features).collect()[0].prediction
        pred_tmin = model_tmin.transform(df_features).collect()[0].prediction # <-- CORREGIDO: Usar model_tmin y [0]

        # Añadimos la predicción a nuestra lista de resultados
        filas_pred.append((future_date, estacion, pred_tmax, pred_tmin)) # <-- CORREGIDO: Usar future_date

        # --- Actualización para la siguiente predicción (predicción en cadena) ---
        # El valor de "ayer" se convierte en el de "anteayer"
        lag2_pd = lag1_pd.copy()
        
        # El nuevo valor de "ayer" es la predicción que acabamos de hacer.
        # Creamos un nuevo objeto (Series de Pandas) para lag1_pd
        # <-- CORREGIDO: Usar claves en MAYÚSCULAS para consistencia
        lag1_pd = pd.Series({
            "TMINIMA": pred_tmin,
            "TMAXIMA": pred_tmax
        })

# Al final, creamos el DataFrame con todas las predicciones
df_predicciones = spark.createDataFrame(filas_pred, schema=schema_pred)
df_predicciones.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


#Paso 6 Guardar Predicciones
df_pred = spark.createDataFrame(filas_pred, schema = schema_pred)
df_pred.write.mode("overwrite").saveAsTable("predicciones_temperaturas_todas")
print("Predicciones para todas las  estaciones completadas y guardadas en predicciones_temperaturas_todas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
