# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# PARAMETERS CELL ********************

#!/usr/bin/env python
# coding: utf-8

# ## nb_fabric_de_series_028_sub_1
# 
# New notebook

# In[ ]:


param_1 = "default param_1 value"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************




# In[ ]:


def print_this(some_value):
    print(f"This is: {str(some_value)}")


# In[ ]:


print_this(param_1)


# In[ ]:


notebookutils.runtime.context


# In[ ]:


notebookutils.notebook.exit(f"Exit value of {notebookutils.runtime.context['currentNotebookName']}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
