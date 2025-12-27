# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7badc3ed-060d-400c-a7d7-7665c1c5f6fe",
# META       "default_lakehouse_name": "LH_MetaDataDriven_NB",
# META       "default_lakehouse_workspace_id": "d0423244-dbb2-439d-a787-3fb5d6a66f24",
# META       "known_lakehouses": [
# META         {
# META           "id": "7badc3ed-060d-400c-a7d7-7665c1c5f6fe"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Setup
import json
path_prefix_file_api = "/lakehouse/default/Files/MetaDataDriven"
path_prefix_spark = "Files/MetaDataDriven"
config_file_name = "config.json"
spark.sql("CREATE SCHEMA IF NOT EXISTS MetaDataDriven")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mover una subcarpeta de origen a destino
source_path = "Files/Source"
destination_path = "Files/MetaDataDriven"

mssparkutils.fs.mv(source_path, destination_path, create_path=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reading configuration file
with open(f"{path_prefix_file_api}/Config/{config_file_name}", "r") as file:
    config_list = json.load(file)
print(config_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Print configs
i = 1
for config in config_list:
    print(f"**CONFIG {str(i)}**")
    print(f"src_file:\t{config['src_file']}")
    print(f"dst_table:\t{config['dst_table']}")
    print(f"write_mode:\t{config['write_mode']}")
    print("*************************************")
    i += 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read and display data
i = 1
len_config_list = len(config_list)
for config in config_list:
    print(f"PROCESSING FILE:\t{str(i)}/{str(len_config_list)}")
    print(f"SOURCE FILE NAME:\t{config['src_file']}")
    print(f"DESTINATION TABLE:\t{config['dst_table']}")
    print(f"USING WRITE MODE:\t{config['write_mode']}")
    df_csv = spark.read.format("csv").option("header","true").load(f"{path_prefix_spark}/Source/{config['src_file']}")
    display(df_csv)
    print("*************************************")
    i += 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write data to lakehouse table
i = 1
len_config_list = len(config_list)
for config in config_list:
    print(f"PROCESSING FILE:\t{str(i)}/{str(len_config_list)}")
    print(f"SOURCE FILE NAME:\t{config['src_file']}")
    print(f"DESTINATION TABLE:\t{config['dst_table']}")
    print(f"USING WRITE MODE:\t{config['write_mode']}")
    df_raw = spark.read.format("csv").option("header","true").load(f"{path_prefix_spark}/Source/{config['src_file']}")
    df_raw.write.mode(config['write_mode']).saveAsTable(f"MetaDataDriven.{config['dst_table']}")
    print("*************************************")
    i += 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Logic to functions for a cleaner and modular code
def read_notebook_config(path):
    with open(path, "r") as file:
        config_list = json.load(file)
    return config_list

def write_csv_file_to_lh_table(config):
    print(f"SOURCE FILE NAME:\t{config['src_file']}")
    print(f"DESTINATION TABLE:\t{config['dst_table']}")
    print(f"USING WRITE MODE:\t{config['write_mode']}")
    df_raw = spark.read.format("csv").option("header","true").load(f"{path_prefix_spark}/Source/{config['src_file']}")
    df_raw.write.mode(config['write_mode']).saveAsTable(f"MetaDataDriven.{config['dst_table']}")
    print("*************************************")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function execution
config_path = f"{path_prefix_file_api}/Config/{config_file_name}"
config_list = read_notebook_config(config_path)

i = 1
len_config_list = len(config_list)
for config in config_list:
    print(f"PROCESSING FILE:\t{str(i)}/{str(len_config_list)}")
    write_csv_file_to_lh_table(config)
    i += 1


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Aqui el mismo ejemplo pero haciendolo en paralelo

# CELL ********************

# Functions for parallel processing using concurrent.futures library
from concurrent.futures import ThreadPoolExecutor, as_completed

def read_notebook_config(path):
    with open(path, "r") as file:
        config_list = json.load(file)
    return config_list

def write_csv_file_to_lh_table_parallel(config):
    try:
        df_raw = spark.read.format("csv").option("header","true").load(f"{path_prefix_spark}/Source/{config['src_file']}")
        df_raw.write.mode(config['write_mode']).saveAsTable(f"MetaDataDriven.{config['dst_table']}")
        return f"PROCESSING OK FOR FILE:\t{config['src_file']}"
    except Exception as e:
        raise ValueError(f"AN ERROR OCCURRED WHILE PROCESSING FILE: {config['src_file']}: {e}")

def handle_errors(futures):
    failed = []
    for future in as_completed(futures):
        config = futures[future]
        try:
            result = future.result()
            print(result)
        except Exception as e:
            failed.append(e)

    if failed:
        print("THESE FAILED WITH THESE ERROR MESSAGES:")
        for fail in failed:
            print(fail)
        raise ValueError(f"{len(failed)} error(s) occurred while processing")
    else:
        print("NO FAILURES DURING PROCESSING")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

config_file_name = "config.json"
#config_file_name = "config_fail.json"
config_path = f"{path_prefix_file_api}/Config/{config_file_name}"
config_list = read_notebook_config(config_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run processing in parallel and handle errors
with ThreadPoolExecutor(max_workers=2) as executor:
    futures = {executor.submit(write_csv_file_to_lh_table_parallel, config): config for config in config_list}
handle_errors(futures)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
