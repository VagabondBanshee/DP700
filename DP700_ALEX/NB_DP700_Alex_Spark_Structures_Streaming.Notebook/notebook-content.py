# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "86c71145-06ab-4fe1-857b-e8b2653d05c1",
# META       "default_lakehouse_name": "LH_DP700_Alex",
# META       "default_lakehouse_workspace_id": "de9b50f8-e09a-4339-95c9-a8dccb86fb19",
# META       "known_lakehouses": [
# META         {
# META           "id": "86c71145-06ab-4fe1-857b-e8b2653d05c1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import json
import os
import uuid
import random
import time
from datetime import datetime

# Settings
output_folder = "/lakehouse/default/Files/dp700_e011/source"
schema_name   = "dp700_e011"
num_files     = 10
wait_seconds  = 5

# Make sure that source folder and destination schema exist
os.makedirs(output_folder, exist_ok=True)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


# In[4]:


for i in range(num_files):
    now = datetime.utcnow()
    timestamp_str = now.strftime("%Y%m%dT%H%M%SZ")

    # Simulate one temperature reading
    record = {
        "id": str(uuid.uuid4()),
        "temperature": round(random.uniform(18.0, 30.0), 2),
        "timestamp": now.isoformat()
    }

    # Build file path with name like temperature_20250404T101010Z.json
    filename = f"temperature_{timestamp_str}.json"
    filepath = os.path.join(output_folder, filename)

    # Write the JSON file
    with open(filepath, "w") as f:
        json.dump(record, f)

    print(f"✅ [{i+1}/{num_files}] Wrote: {filename}")
    time.sleep(wait_seconds)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import os
import json
import pyspark.sql.functions as F
import time

source_path = "Files/dp700_e011/source"
checkpoint_path = "Files/dp700_e011/checkpoint"
schema_name = "dp700_e011"
table_name = "temperature_stream"

# Schema for incoming JSON data
file_schema = StructType() \
    .add("id", StringType()) \
    .add("temperature", DoubleType()) \
    .add("timestamp", TimestampType())


# In[3]:


# Read streaming data to unbounded table/dataframe
raw_stream_df = spark.readStream \
    .schema(file_schema) \
    .option("maxFilesPerTrigger", 1) \
    .json(source_path)

# Example transformation that adds a processed_timestamp column to the data
transformed_stream_df = raw_stream_df \
    .withColumn("processed_timestamp", \
    F.current_timestamp())

# Stream data to a delta table
deltastream = transformed_stream_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .start(f"Tables/{schema_name}/{table_name}")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[4]:


raw_stream_df.isStreaming





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[5]:


deltastream.isActive




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[8]:


deltastream.status




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[9]:


deltastream.lastProgress




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[11]:


while deltastream.isActive:
    print("✅ Stream is running...")
    print("📊 Last progress:", deltastream.lastProgress)
    time.sleep(5)

print("❌ Stream has stopped.")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[10]:


deltastream.stop()


# In[12]:


df = spark.sql("SELECT * FROM lh_dp700.dp700_e011.temperature_stream")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
