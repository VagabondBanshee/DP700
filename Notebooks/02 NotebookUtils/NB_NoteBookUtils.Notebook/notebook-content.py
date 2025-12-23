# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c8ee8c7e-796a-4866-9afc-b45738c01263",
# META       "default_lakehouse_name": "LH_NotebookExample",
# META       "default_lakehouse_workspace_id": "d0423244-dbb2-439d-a787-3fb5d6a66f24",
# META       "known_lakehouses": [
# META         {
# META           "id": "c8ee8c7e-796a-4866-9afc-b45738c01263"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_notebookutils_demo_main
# 
# # # NotebookUtils Example Notebook

# CELL ********************



# In[3]:


notebookutils.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Seccion 1
# ## notebookutils.fs

# MARKDOWN ********************


# CELL ********************

notebookutils.fs.ls("Files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.mkdirs('Files/Copy')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.mkdirs('Files/Movies')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.ls("Files/Animals")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.cp('Files/Animals/animals.csv','Files/Copy/animals_copied.csv',False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Seccion 2
# ## notebookutils.notebook`

# CELL ********************


notebookutils.notebook.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.exit("some string value")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

exit_value = notebookutils.notebook.run("nb_notebookutils_demo_sub_1", 60, {"param1": "Hello World!"})
print(exit_value)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.runMultiple(["nb_notebookutils_demo_sub_1", "nb_notebookutils_demo_sub_2"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DAG = {
    "activities": [
        {
            "name": "nb_notebookutils_demo_sub_1", # activity name, must be unique
            "path": "nb_notebookutils_demo_sub_1", # notebook path
            "timeoutPerCellInSeconds": 90, # max timeout for each cell, default to 90 seconds
            "retry": 1, # retry amount
            "retryIntervalInSeconds": 10, # retry interval in seconds
            "args": {"param1": "Hello World!"} # notebook parameters
        },
        {
            "name": "nb_notebookutils_demo_sub_2",
            "path": "nb_notebookutils_demo_sub_2"
        },
        {
            "name": "nb_notebookutils_demo_sub_3",
            "path": "nb_notebookutils_demo_sub_3",
            "timeoutPerCellInSeconds": 120,
            "dependencies": ["nb_notebookutils_demo_sub_2"] # list of activity names that this activity depends on
        }
    ],
    "timeoutInSeconds": 43200, # max timeout for the entire pipeline, default to 12 hours
    "concurrency": 50 # max number of notebooks to run concurrently, default to 50, 0 means unlimited
}
notebookutils.notebook.runMultiple(DAG, {"displayDAGViaGraphviz": True})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# notebookutils.notebook.help("list")
notebookutils.notebook.list()   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebook_content =  {
	"cells": [
		{
			"cell_type": "code",
			"source": [
				"print(\"abc\")"
			],
			"outputs": [],
			"execution_count": None,
			"metadata": {
				"microsoft": {
					"language": "python",
					"language_group": "synapse_pyspark"
				}
			},
			"id": "abe16427-3af0-41bb-b1fd-7d05fb9e8185"
		},
		{
			"cell_type": "code",
			"source": [
				"notebookutils.notebook.exit(\"exit value!\")"
			],
			"outputs": [],
			"execution_count": None,
			"metadata": {
				"jupyter": {
					"source_hidden": False,
					"outputs_hidden": False
				},
				"nteract": {
					"transient": {
						"deleting": False
					}
				},
				"microsoft": {
					"language": "python",
					"language_group": "synapse_pyspark"
				}
			},
			"id": "667e03d1-86b8-403a-baa2-989c98642d31"
		}
	],
	"metadata": {
		"kernel_info": {
			"name": "synapse_pyspark"
		},
		"kernelspec": {
			"name": "synapse_pyspark",
			"display_name": "synapse_pyspark"
		},
		"language_info": {
			"name": "python"
		},
		"microsoft": {
			"language": "python",
			"language_group": "synapse_pyspark",
			"ms_spell_check": {
				"ms_spell_check_language": "en"
			}
		},
		"widgets": {},
		"nteract": {
			"version": "nteract-front-end@1.0.0"
		},
		"spark_compute": {
			"compute_id": "/trident/default"
		},
		"dependencies": {}
	},
	"nbformat": 4,
	"nbformat_minor": 5
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.create("nb_notebookutils_demo_create_test_123", "random description", notebook_content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.delete("nb_notebookutils_demo_create_test_123")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Seccion 3
# # # Lakehouse

# CELL ********************

notebookutils.lakehouse.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.lakehouse.get("LH_NotebookExample")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.lakehouse.getWithProperties("LH_NotebookExample")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.runtime.con

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
