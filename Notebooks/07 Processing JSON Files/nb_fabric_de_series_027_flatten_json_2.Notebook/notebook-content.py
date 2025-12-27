# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fc61a033-a607-4645-b218-ac175a9fa9d9",
# META       "default_lakehouse_name": "LH_ProcessingJSONFiles",
# META       "default_lakehouse_workspace_id": "d0423244-dbb2-439d-a787-3fb5d6a66f24",
# META       "known_lakehouses": [
# META         {
# META           "id": "fc61a033-a607-4645-b218-ac175a9fa9d9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#!/usr/bin/env python
# coding: utf-8

# ## nb_fabric_de_series_027_flatten_json_2
# 
# New notebook

# ## raw file to data frame

# In[2]:


df_zoo_raw = spark.read.option("multiline", "true").json("Files/ProcessingJSONFiles/zoo.json")
df_zoo_raw.createOrReplaceTempView("df_zoo_raw_view")


# ## create schema

# In[3]:


spark.sql("CREATE SCHEMA IF NOT EXISTS Animals_ZOO")


# ## zoo table

# In[4]:


df_zoo = spark.sql("""
select
a.zoo.zoo_id as zoo_id
,a.zoo.country as country
,a.zoo.city as city
from (
    select explode(zoo) as zoo
    from df_zoo_raw_view
) as a
""")
df_zoo.write.mode("overwrite").format("delta").saveAsTable("Animals_ZOO.zoo")


# ## zoo_animals table

# In[5]:


df_zoo_animals = spark.sql("""
select
b.zoo_id as zoo_id
,b.animals.animal_id as animal_id
,b.animals.name as name
,b.animals.species as species
,b.animals.characteristics.diet as characteristics_diet
,b.animals.characteristics.lifespan.average_years as characteristics_lifespan_average_years
,b.animals.characteristics.lifespan.in_wild as characteristics_lifespan_in_wild
,b.animals.characteristics.lifespan.in_captivity as characteristics_lifespan_in_captivity
,b.animals.characteristics.weight_kg.male as characteristics_weight_kg_male
,b.animals.characteristics.weight_kg.female as characteristics_weight_kg_female
from (
    select
    a.zoo.zoo_id
    ,explode(a.zoo.animals) as animals
        from (
            select explode(zoo) as zoo
            from df_zoo_raw_view
        ) as a
) as b
""")
df_zoo_animals.write.mode("overwrite").format("delta").saveAsTable("Animals_ZOO.zoo_animals")


# ## zoo_animals_characteristics_habitat table

# In[6]:


df_zoo_animals_characteristics_habitat = spark.sql("""
select
b.animals.animal_id as animal_id
,explode(b.animals.characteristics.habitat) as characteristics_habitat
from (
    select
    a.zoo.zoo_id
    ,explode(a.zoo.animals) as animals
        from (
            select explode(zoo) as zoo
            from df_zoo_raw_view
        ) as a
) as b
""")
df_zoo_animals_characteristics_habitat.write.mode("overwrite").format("delta").saveAsTable("Animals_ZOO.zoo_animals_characteristics_habitat")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
