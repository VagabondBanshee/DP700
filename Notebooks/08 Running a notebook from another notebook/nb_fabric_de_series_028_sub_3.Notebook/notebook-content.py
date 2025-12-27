# Fabric notebook source


# CELL ********************

#!/usr/bin/env python
# coding: utf-8

# ## nb_fabric_de_series_028_sub_3
# 
# New notebook

# In[ ]:


param_1 = "yyy"


# In[ ]:


print(param_1)


# In[ ]:


notebookutils.runtime.context


# In[ ]:


notebookutils.notebook.exit(f"Exit value of {notebookutils.runtime.context['currentNotebookName']}")

