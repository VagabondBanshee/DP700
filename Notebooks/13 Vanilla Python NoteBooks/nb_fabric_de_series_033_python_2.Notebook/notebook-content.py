# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

#!/usr/bin/env python
# coding: utf-8

# ## nb_fabric_de_series_033_python_2
# 
# New notebook

# In[1]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install pandas


# In[2]:


from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .getOrCreate()

# Verify the Spark session is working
print(spark.version)

spark.sql("select 1 as b union select 2 as b")


# In[4]:


notebookutils.help()


# In[5]:


notebookutils.notebook.run("nb_fabric_de_series_033_python_3", 90, {"param_1": "new value for param_1", "param_2": 999})


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
