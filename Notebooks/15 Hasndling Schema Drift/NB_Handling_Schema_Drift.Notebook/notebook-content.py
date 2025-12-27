# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8764de32-a242-496c-9cbc-3abbd118cf47",
# META       "default_lakehouse_name": "LH_HandlingSchemaDrift",
# META       "default_lakehouse_workspace_id": "d0423244-dbb2-439d-a787-3fb5d6a66f24",
# META       "known_lakehouses": [
# META         {
# META           "id": "8764de32-a242-496c-9cbc-3abbd118cf47"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Preparations
source_path_prefix = "Files/Handling/source"
spark.sql("CREATE SCHEMA IF NOT EXISTS Handling")
spark.sql("DROP TABLE IF EXISTS Handling.person")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reading person_1.json
df_person_1_raw = spark.read.option("multiline", "true").json(f"{source_path_prefix}/person_1.json")
display(df_person_1_raw)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reading person_1.json and writing the data to a table
df_person_1_raw = spark.read.option("multiline", "true").json(f"{source_path_prefix}/person_1.json")
df_person_1_raw.write.mode("overwrite").saveAsTable("Handling.person")
df_person_table = spark.read.table("Handling.person")
display(df_person_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reading person_2.json
df_person_2_raw = spark.read.option("multiline", "true").json(f"{source_path_prefix}/person_2.json")
display(df_person_2_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reading person_2.json and appending the data to a same table with person_1.json data
df_person_2_raw = spark.read.option("multiline", "true").json(f"{source_path_prefix}/person_2.json")
df_person_2_raw.write.mode("append").saveAsTable("fabric_de_series_035.person")
df_person_table = spark.read.table("fabric_de_series_035.person")
display(df_person_table)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # En este caso se corrige el error de schema con la opción .option("mergeSchema", "true")

# CELL ********************

# Reading person_2.json and appending the data to a same table with person_1.json data with mergeSchema option

df_person_2_raw = spark.read.option("multiline", "true").json(f"{source_path_prefix}/person_2.json")
df_person_2_raw.write.mode("append").option("mergeSchema", "true").saveAsTable("Handling.person")
df_person_table = spark.read.table("Handling.person")
display(df_person_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reading person_3.json
df_person_3_raw = spark.read.option("multiline", "true").json(f"{source_path_prefix}/person_3.json")
display(df_person_3_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Aunque el fichero trae menos columnas no da error porque puede hacer un merge automatico de lso schemas, no hace falta indicarle la opcion de schema, merge

# CELL ********************

# Reading person_3.json and appending the data to a same table with person_1.json and person_2.json data
df_person_3_raw = spark.read.option("multiline", "true").json(f"{source_path_prefix}/person_3.json")
df_person_3_raw.write.mode("append").saveAsTable("Handling.person")
df_person_table = spark.read.table("Handling.person")
display(df_person_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reading person_*.json using a wildcard
df_person_wc_raw = spark.read.option("multiline", "true").json(f"{source_path_prefix}/person_*.json")
display(df_person_wc_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reading person_*.json using a wildcard and printing the file schema
df_person_wc_raw = spark.read.option("multiline", "true").json(f"{source_path_prefix}/person_*.json")
df_person_wc_raw.schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #En este codigo se fuerxsa el schema al leer el fichero

# CELL ********************

from pyspark.sql.types import *
schema = StructType([
StructField('age', LongType(), True),
StructField('gender', StringType(), True),
StructField('id', LongType(), True),
StructField('name', StringType(), True)
])
df_person_1_raw = spark.read.option("multiline", "true").schema(schema).json(f"{source_path_prefix}/person_1.json")
display(df_person_1_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enforcing the schema while reading person_1.json file
from pyspark.sql.types import *
schema = StructType([
#StructField('age', LongType(), True),
StructField('gender', StringType(), True),
StructField('id', LongType(), True),
StructField('name', StringType(), True)
])
df_person_1_raw = spark.read.option("multiline", "true").schema(schema).json(f"{source_path_prefix}/person_1.json")
display(df_person_1_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Schema as json
df_person_wc_raw = spark.read.option("multiline", "true").json(f"{source_path_prefix}/person_*.json")
df_person_wc_raw.schema.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Using the json schema
import json
schema_json = '''
{
	"fields": [
		{
			"metadata": {},
			"name": "age",
			"nullable": true,
			"type": "long"
		},
		{
			"metadata": {},
			"name": "gender",
			"nullable": true,
			"type": "string"
		},
		{
			"metadata": {},
			"name": "id",
			"nullable": true,
			"type": "long"
		},
		{
			"metadata": {},
			"name": "name",
			"nullable": true,
			"type": "string"
		}
	],
	"type": "struct"
}
'''
schema = StructType.fromJson(json.loads(schema_json))
print(schema)
df_person_1_raw = spark.read.option("multiline", "true").schema(schema).json(f"{source_path_prefix}/person_1.json")
display(df_person_1_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS Handling.person")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reading the configuration file
with open("/lakehouse/default/Files/Handling/config/person_config.json", "r") as file:
    config_list = json.load(file)
print(config_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reading configuration file
with open("/lakehouse/default/Files/Handling/config/person_config.json", "r") as file:
    config_list = json.load(file)
print(config_list)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write data based on configuration to a lakehouse table
i = 1
len_config_list = len(config_list)
for config in config_list:
    print(f"PROCESSING FILE:\t{str(i)}/{str(len_config_list)}")
    print(f"SOURCE FILE NAME:\t{config['src_file']}")
    print(f"DESTINATION TABLE:\t{config['dst_table']}")
    print(f"USING WRITE MODE:\t{config['write_mode']}")
    df_raw = spark.read.option("multiline", "true").schema(StructType.fromJson(config['schema'])).json(f"{source_path_prefix}/{config['src_file']}")
    df_raw.write.mode(config['write_mode']).option("mergeSchema", "true").saveAsTable(f"Handling.{config['dst_table']}")
    print("*************************************")
    i += 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_person_table = spark.read.table("Handling.person")
display(df_person_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
