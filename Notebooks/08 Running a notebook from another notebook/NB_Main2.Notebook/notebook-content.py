# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # otra forma de llamar a un notebook desde otro notebook
# ## notebookutils.run


# CELL ********************


notebookutils.notebook.run("nb_fabric_de_series_028_sub_1", 90, {"param_1": "some param_1 value here!"})


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # con esta manera de llamar no se puede acceder a fubnciones del otro notebook porque se ejecutan en diferntens contextos.
# # Ademas la ejecución de la función exit en el notebook llamado no para la ejecución del notebook principal por la misma razón.

# CELL ********************

print("ABC")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

exit_value = notebookutils.notebook.run("nb_fabric_de_series_028_sub_1", 90, {"param_1": "some param_1 value here!"})
print(exit_value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #PAra ejecutar v sarios notebooks se utiliza notebookutils.notebook.runMultiple

# CELL ********************

exit_values = notebookutils.notebook.runMultiple(["nb_fabric_de_series_028_sub_1", "nb_fabric_de_series_028_sub_2"])
print(exit_values)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(exit_values["1"]["exitVal"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Ejecución de un DAG

# CELL ********************

DAG = {
    "activities": [
        {
            "name": "nb_fabric_de_series_028_sub_1", # activity name, must be unique
            "path": "nb_fabric_de_series_028_sub_1", # notebook path
            "timeoutPerCellInSeconds": 90, # max timeout for each cell, default to 90 seconds
            "args": {"param_1": "this is param_1 value!"}, # notebook parameters
            "retry": 1,
            "retryIntervalInSeconds": 10
        },
        {
            "name": "nb_fabric_de_series_028_sub_2",
            "path": "nb_fabric_de_series_028_sub_2",
            "args": {"param_1": "this is param_1 value!"}
        },
        {
            "name": "nb_fabric_de_series_028_sub_3",
            "path": "nb_fabric_de_series_028_sub_3",
            "timeoutPerCellInSeconds": 90,
            "args": {"param_1": "this is param_1 value!"},
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "dependencies": ["nb_fabric_de_series_028_sub_1","nb_fabric_de_series_028_sub_2"] # list of activity names that this activity depends on
        }
    ],
    "timeoutInSeconds": 300, # max timeout for the entire DAG, default to 12 hours
    "concurrency": 3 # max number of notebooks to run concurrently, default to 50
}
exit_values_dag = notebookutils.notebook.runMultiple(DAG, {"displayDAGViaGraphviz": True})
print(exit_values_dag)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(exit_values_dag["nb_fabric_de_series_028_sub_3"]["exitVal"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
