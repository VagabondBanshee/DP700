# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

#!/usr/bin/env python
# coding: utf-8

# ## nb_masterclass_example
# 
# New notebook

# In[2]:


# Create sample data
data = [
    (1, "Alice", 29, "New York"),
    (2, "Bob", 35, "Los Angeles"),
    (3, "Cathy", 24, "Chicago"),
    (4, "David", 42, "San Francisco"),
    (5, "Eva", 31, "Seattle")
]

# Define schema (column names)
columns = ["ID", "Name", "Age", "City"]

# Create DataFrame
df = spark.createDataFrame(data, schema=columns)
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sh
# MAGIC ls

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_masterclass_example_2


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
