# Fabric notebook source


# CELL ********************

#!/usr/bin/env python
# coding: utf-8

# ## nb_fabric_de_series_033_python_3
# 
# New notebook

# In[ ]:


param_1 = "default value for param_1"
param_2 = 0


# In[ ]:


print(f"param_1 value is: {param_1}")
print(f"param_2 value is: {param_2}")


# In[ ]:


notebook_exit_value = f"my notebook exit value is param_2 value: {param_2}"
notebookutils.notebook.exit(notebook_exit_value)

