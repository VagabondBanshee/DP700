# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d804dbd9-9611-49f9-9fd9-e034e209b6de",
# META       "default_lakehouse_name": "LH_Pokemon",
# META       "default_lakehouse_workspace_id": "d0423244-dbb2-439d-a787-3fb5d6a66f24",
# META       "known_lakehouses": [
# META         {
# META           "id": "d804dbd9-9611-49f9-9fd9-e034e209b6de"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
response = requests.get("https://pokeapi.co/api/v2/pokemon")
print(type(response))
print(response)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# sele añade la llamada a la función json() en el objeto response
import requests
response = requests.get("https://pokeapi.co/api/v2/pokemon")
print(type(response.json()))
print(response.json())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Se indica el offset y el limit en la llamada a la api para que traiga todos
import requests
response = requests.get("https://pokeapi.co/api/v2/pokemon?offset=0&limit=1302")
print(response.json())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import os
import json
response = requests.get("https://pokeapi.co/api/v2/pokemon?offset=0&limit=1302")
data = response.json()

lakehouse_path = "/lakehouse/default/Files/Pokemon"

# Ensure the folder exists by creating the directory (automatically handles missing directories)
os.makedirs(lakehouse_path, exist_ok=True)

with open(f"{lakehouse_path}/pokemon.json", "w") as f:
        f.write(json.dumps(data["results"]))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json("Files/Pokemon/pokemon.json")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Ahora el mismo ejercicio pero recorriendo la api pagina a pagina

# CELL ********************

import requests
import os
import json
from datetime import datetime

# Get the current timestamp in the yyyyMMddThhmmssZ format
current_timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')

lakehouse_base_path = "/lakehouse/default/Files/Pokemon"

base_url = "https://pokeapi.co/api/v2/"
endpoint = "pokemon"
url = base_url + endpoint

# Start with the initial URL
next_url = url

i = 0

# Folder path in the lakehouse
root_folder = f"{lakehouse_base_path}/{endpoint}/{current_timestamp}/"
# Ensure the folder exists by creating the directory (automatically handles missing directories)
os.makedirs(root_folder, exist_ok=True)


# Loop until 'next' is None (no more pages)
while next_url:
    print(f"Endpoint {endpoint} API call index: {i}")
    
    # Make the API call
    response = requests.get(next_url)
    data = response.json()

    # Write results to Lakehouse Files
    with open(f"{lakehouse_base_path}/{endpoint}/{current_timestamp}/{endpoint}_{i}.json", "w") as f:
        f.write(json.dumps(data["results"]))
    
    # Update the URL to the next page
    next_url = data['next']
    
    i += 1

print(f"All {endpoint} pages processed!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Ahora el mismo ejemplo pero recorriendo dos endpoints diferentes de la API

# CELL ********************

import requests
import os
import json
from datetime import datetime


# Get the current timestamp in the yyyyMMddThhmmssZ format
current_timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')

lakehouse_base_path = "/lakehouse/default/Files/fabric_de_series_029"

base_url = "https://pokeapi.co/api/v2/"
endpoint_list = ["pokemon","berry"]

for endpoint in endpoint_list:
    url = base_url + endpoint

    # Start with the initial URL
    next_url = url

    i = 0

    # Folder path in the lakehouse
    root_folder = f"{lakehouse_base_path}/{endpoint}/{current_timestamp}/"
    # Ensure the folder exists by creating the directory (automatically handles missing directories)
    os.makedirs(root_folder, exist_ok=True)


    # Loop until 'next' is None (no more pages)
    while next_url:
        print(f"Endpoint {endpoint} API call index: {i}")
        
        # Make the API call
        response = requests.get(next_url)
        data = response.json()

        # Write results to Lakehouse Files
        with open(f"{lakehouse_base_path}/{endpoint}/{current_timestamp}/{endpoint}_{i}.json", "w") as f:
            f.write(json.dumps(data["results"]))
        
        # Update the URL to the next page
        next_url = data['next']
        
        i += 1

    print(f"All {endpoint} pages processed!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json(f"Files/fabric_de_series_029/berry/{current_timestamp}/berry_*.json")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
