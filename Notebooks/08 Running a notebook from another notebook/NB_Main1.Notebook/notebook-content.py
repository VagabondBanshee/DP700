# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run nb_fabric_de_series_028_sub_1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Definir parámetros si los necesitas: ESTA ES OTRA FORMA DE H_AERLO SEGUN GEMINI
params = {"param_1": "Valor desde notebook principal"}

# Ejecutar el notebook y capturar la salida
try:
    exit_message = mssparkutils.notebook.run("nb_fabric_de_series_028_sub_1", 3600, params)
    print(f"Resultado del hijo: {exit_message}")
except Exception as e:
    print(f"Error en la ejecución: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_fabric_de_series_028_sub_1 {"param_1": "new param_1 value!"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Con el metodo de ejecucion con el comando magico %run, los dos notebooks comparten el mismo contexto de ejecución.
# ## Se suele utilizar por ejemplo cuando sd tienen funciones de finidas y separan en otro notebook para tener mas limpio el  primero.
# ## Si el notebook al que se le llama tiene parametro de salida, utiliza la funcion exit, la ejecución del primer notebook se para.
