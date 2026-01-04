# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c01dd8f4-03d1-46bf-b5e9-040f59966ca3",
# META       "default_lakehouse_name": "LH_CompilacionModelo",
# META       "default_lakehouse_workspace_id": "f98e8a5b-6f39-49f5-b226-1a6e01ae59fc",
# META       "known_lakehouses": [
# META         {
# META           "id": "c01dd8f4-03d1-46bf-b5e9-040f59966ca3"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Part 4: Score the trained model


# MARKDOWN ********************

# 
# Microsoft Fabric allows you to operationalize machine learning models with a scalable function called PREDICT, which supports batch scoring in any compute engine. You can generate batch predictions directly from a Microsoft Fabric notebook or from a given model's item page. Learn about [PREDICT](https://aka.ms/fabric-predict).  
# 
# To generate batch predictions on our test dataset, you'll use version 1 of the trained churn model. You'll load the test dataset into a spark DataFrame and create an MLFlowTransformer object to generate batch predictions. You can then invoke the PREDICT function using one of following three ways: 
# 
# - Using the Transformer API from SynapseML
# - Using the Spark SQL API
# - Using PySpark user-defined function (UDF)
# 
# ## Prerequisites
# 
# - Complete [Part 3: Train and register machine learning models](https://learn.microsoft.com/fabric/data-science/tutorial-data-science-train-models).
# - Attach the same lakehouse you used in Part 3 to this notebook.

# CELL ********************

%pip install scikit-learn==1.6.1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load the test data
# 
# Load the test data that you saved in Part 3.

# CELL ********************

# CORRECTO (Leemos del catálogo directamente)
df_test = spark.table("df_test")
display(df_test)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### PREDICT with the Transformer API
# 
# To use the Transformer API from SynapseML, you'll need to first create an MLFlowTransformer object.
# 
# ### Instantiate MLFlowTransformer object
# 
# The MLFlowTransformer object is a wrapper around the MLFlow model that you registered in Part 3. It allows you to generate batch predictions on a given DataFrame. To instantiate the MLFlowTransformer object, you'll need to provide the following parameters:
# 
# - The columns from the test DataFrame that you need as input to the model (in this case, you would need all of them).
# - A name for the new output column (in this case, predictions).
# - The correct model name and model version to generate the predictions (in this case, `lgbm_sm` and version 1).

# CELL ********************

import mlflow
from pyspark.sql.functions import struct, col
from pyspark.sql.types import StringType

# 1. Configuración URI
model_name = 'lgbm_sm'
model_version = 1
model_uri = f"models:/{model_name}/{model_version}"

# 2. SOLUCIÓN FINAL: Usar "string"
# Esto fuerza a que la salida sea texto plano, evitando el error de conversión de tipos.
predict_udf = mlflow.pyfunc.spark_udf(
    spark, 
    model_uri=model_uri, 
    result_type="string"  # <--- Hacemos caso a la sugerencia del error
)

# 3. Predecir
# Creamos una columna temporal 'prediction_raw' para ver qué nos devuelve exactamente
df_predictions = df_test.withColumn("prediction_raw", predict_udf(struct(*df_test.columns)))

# 4. Mostrar el resultado CRUDO
display(df_predictions.select("prediction_raw").limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Now that you have the MLFlowTransformer object, you can use it to generate batch predictions.

# CELL ********************

# import pandas

# predictions = model.transform(df_test)
# display(predictions)

# 1. Asignamos nuestro resultado manual a la variable que espera el tutorial
predictions = df_predictions

# 2. Mostramos el resultado
display(predictions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### PREDICT with the Spark SQL API

# CELL ********************

from pyspark.ml.feature import SQLTransformer 

# Substitute "model_name", "model_version", and "features" below with values for your own model name, model version, and feature columns
model_name = 'lgbm_sm'
model_version = 1
features = df_test.columns

sqlt = SQLTransformer().setStatement( 
    f"SELECT PREDICT('{model_name}/{model_version}', {','.join(features)}) as predictions FROM __THIS__")

# Substitute "X_test" below with your own test dataset
display(sqlt.transform(df_test))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### PREDICT with a user-defined function (UDF)

# CELL ********************

from pyspark.sql.functions import col, pandas_udf, udf, lit

# Substitute "model" and "features" below with values for your own model name and feature columns
my_udf = model.to_udf()
features = df_test.columns

display(df_test.withColumn("predictions", my_udf(*[col(f) for f in features])))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write model prediction results to the lakehouse
# 
# Once you have generated batch predictions, write the model prediction results back to the lakehouse.  

# CELL ********************

from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

table_name = "df_test_with_predictions_v1"

# 1. PASO CRÍTICO: Convertir (Castear) explícitamente a Double
# Si la columna se llama 'prediction_raw' o 'prediction', asegúrate de usar el nombre correcto.
# El .cast("double") es lo que habilitará la función AVERAGE en Power BI.
predictions_fixed = predictions.withColumn(
    "prediction", 
    col("prediction_raw").cast(DoubleType())
)

# 2. Guardar sobrescribiendo la tabla anterior
predictions_fixed.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"Tabla guardada con tipos corregidos: {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Next step
# 
# Use these predictions you just saved to [create a report in Power BI](https://learn.microsoft.com/fabric/data-science/tutorial-data-science-create-report).
