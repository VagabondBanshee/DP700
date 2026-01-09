# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9e6698f7-a598-4fc0-b823-d10424ba4a7d",
# META       "default_lakehouse_name": "LH_BRONCE_REPASO",
# META       "default_lakehouse_workspace_id": "d62a3a18-b622-4f96-af59-07c7c3b40eed",
# META       "known_lakehouses": [
# META         {
# META           "id": "9e6698f7-a598-4fc0-b823-d10424ba4a7d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Explorando datos con un DataFrame Pandas

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/MinisterioCiencia/Datos-CambioClimatico/refs/heads/main/output/tmaxima_dmc/1950/1950_tmaxima_dmc.csv")
# df now is a Spark DataFrame containing CSV data from "Files/MinisterioCiencia/Datos-CambioClimatico/refs/heads/main/output/tmaxima_dmc/1950/1950_tmaxima_dmc.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table('AW_Customer').toPandas()
display(df.head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Análisis general

num_rows, num_cols = df.shape
null_counts = df.isna().sum()
null_percentages = (df.isna().mean() * 100).round(2)
column_types = df.dtypes
unique_counts = df.nunique()
description = df.describe()

print("Número de filas:", num_rows)
print("Número de columnas:", num_cols)

print("\nValores nulos por columna:\n", null_counts)
print("\nPorcentaje de valores nulos por columna:\n", null_percentages)
print("\nTipos de datos por columna:\n", column_types)
print("\nCantidad de valores únicos por columna:\n", unique_counts)
print("\nDescripción estadística general:\n", description)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Agrupaciones y conteos

customers_per_title = df.groupby("Title").size().reset_index(name="total").sort_values("total", ascending=False)
print("\nCantidad de clientes por título (Title):\n", customers_per_title)

customers_per_salesperson = df.groupby("SalesPerson").size().reset_index(name="total").sort_values("total", ascending=False)
print("\nCantidad de clientes por vendedor (SalesPerson):\n", customers_per_salesperson)

companies_count = df["CompanyName"].nunique()
print("\nCantidad de compañías únicas:", companies_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fechas

df["ModifiedDate"] = pd.to_datetime(df["ModifiedDate"])
date_range = (df["ModifiedDate"].min(), df["ModifiedDate"].max())
print("\nRango de fechas de modificación:", date_range)

modifications_per_year = df["ModifiedDate"].dt.year.value_counts().sort_index()
print("\nCantidad de modificaciones por año:\n", modifications_per_year)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calidad de datos

exact_duplicates = df[df.duplicated()]
print("\nFilas duplicadas exactas:\n", exact_duplicates)

duplicates_by_name = df[df.duplicated(subset=["FirstName", "LastName", "CompanyName"])]
print("\nDuplicados por nombre y compañía:\n", duplicates_by_name)

df["Email_length"] = df["EmailAddress"].str.len()
email_length_stats = df["Email_length"].describe()
print("\nEstadísticas de longitud de direcciones de email:\n", email_length_stats)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Valores únicos

unique_values_summary = df.apply(pd.Series.nunique).sort_values()
print("\nCantidad de valores únicos por columna (ordenados):\n", unique_values_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gráficos: Estilo general

sns.set(style="whitegrid")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gráfico de barras: cantidad de clientes por título

customers_per_title = df["Title"].value_counts().reset_index()
customers_per_title.columns = ["Title", "Count"]

plt.figure(figsize=(6,4))
sns.barplot(x="Count", y="Title", data=customers_per_title, palette="Blues_d")
plt.title("Cantidad de clientes por título")
plt.xlabel("Cantidad")
plt.ylabel("Título")
plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Histograma: longitud de emails
df["Email_length"] = df["EmailAddress"].str.len()

plt.figure(figsize=(6,4))
sns.histplot(df["Email_length"], bins=20, kde=True)
plt.title("Distribución de longitud de direcciones de email")
plt.xlabel("Longitud del email")
plt.ylabel("Frecuencia")
plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
