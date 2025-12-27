# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "93a4d83e-e6f1-4087-bd77-a31a61978e97",
# META       "default_lakehouse_name": "LH_ParallelAPI_Calls",
# META       "default_lakehouse_workspace_id": "d0423244-dbb2-439d-a787-3fb5d6a66f24",
# META       "known_lakehouses": [
# META         {
# META           "id": "93a4d83e-e6f1-4087-bd77-a31a61978e97"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

def testing(some_value):
    print(f"processing {some_value}")

testing("abc")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time

def testing(some_value):
    time.sleep(1.5)
    print(f"processing {some_value}")

some_value_list = ["a","b","c","d","e","f"]

for some_value in some_value_list:
    testing(some_value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from concurrent.futures import ThreadPoolExecutor
import time

def parallel_testing(some_value):
    time.sleep(1.5)
    print(f"processing {some_value}")


some_value_list = ["a","b","c","d","e","f"]

with ThreadPoolExecutor(max_workers=2) as executor:
    futures = {executor.submit(parallel_testing, some_value): some_value for some_value in some_value_list}



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Es muy importante tratar los errores cuando se ejecutan cosas en paralelo, ali gual que saber cuanta CPU se utiliza para saber cuantos max_workers sepueden utilziar.
# 
# # En este ejemplo se lanza un error pero la ejecución en paralelo no lo tiene en cuetna

# CELL ********************

from concurrent.futures import ThreadPoolExecutor
import time

def parallel_testing(some_value):
    time.sleep(1.5)
    print(f"processing {some_value}")
    if some_value == "d":
        raise ValueError(f"An error occurred while processing '{some_value}'")

some_value_list = ["a", "b", "c", "d", "e", "f"]

with ThreadPoolExecutor(max_workers=2) as executor:
    futures = {executor.submit(parallel_testing, some_value): some_value for some_value in some_value_list}



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # El siguiente código trata al final de la ejecución el resultado para ver si ha habido errores

# CELL ********************


from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def parallel_testing(some_value):
    time.sleep(1.5)
    print(f"processing {some_value}")
    if some_value == "d":
        raise ValueError(f"an error occurred while processing '{some_value}'")
    return f"completed {some_value}"

some_value_list = ["a", "b", "c", "d", "e", "f"]

with ThreadPoolExecutor(max_workers=2) as executor:
    futures = {executor.submit(parallel_testing, some_value): some_value for some_value in some_value_list}

print("*******************")
print("RESULT:")
print("*******************")
failed = []
for future in as_completed(futures):
    some_value = futures[future]
    try:
        result = future.result()
    except Exception as e:
        fail = f"{some_value}: {e}"
        failed.append(fail)

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

# MARKDOWN ********************

# # El siguiente código ejecuta la llamada a la api de pokemosn en paralelo

# CELL ********************


import requests
import os
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Get the current timestamp in the yyyyMMddThhmmssZ format
current_timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
lakehouse_base_path = "/lakehouse/default/Files/fabric_de_series_030"
base_url = "https://pokeapi.co/api/v2/"
endpoint_list = ["pokemon", "berry"]

# Function to process each endpoint
def process_endpoint(endpoint):
    url = base_url + endpoint
    next_url = url
    i = 0

    # Folder path in the lakehouse
    root_folder = f"{lakehouse_base_path}/{endpoint}/{current_timestamp}/"
    os.makedirs(root_folder, exist_ok=True)

    while next_url:
        print(f"Endpoint {endpoint} API call index: {i}")
        
        try:
            # Make the API call
            response = requests.get(next_url)
            response.raise_for_status()  # Raise an error for bad status codes
            data = response.json()

            # Write results to Lakehouse Files
            with open(f"{root_folder}/{endpoint}_{i}.json", "w") as f:
                f.write(json.dumps(data["results"]))

            # Update the URL to the next page
            next_url = data.get('next')
            i += 1

        except Exception as e:
            raise ValueError(f"An error occurred while processing {endpoint} at index {i}: {e}")

    return f"All {endpoint} pages processed!"

# Run endpoints in parallel and handle errors
with ThreadPoolExecutor(max_workers=len(endpoint_list)) as executor:
    futures = {executor.submit(process_endpoint, endpoint): endpoint for endpoint in endpoint_list}

print("*******************")
print("RESULT:")
print("*******************")
failed = []
for future in as_completed(futures):
    endpoint = futures[future]
    try:
        result = future.result()
        print(result)
    except Exception as e:
        fail = f"{endpoint}: {e}"
        failed.append(fail)

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
