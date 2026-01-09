# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2bdfd233-a573-48cb-aad6-c166124a3877",
# META       "default_lakehouse_name": "lh_AdnFabric",
# META       "default_lakehouse_workspace_id": "29e4bc80-cccb-43e5-ad04-7883dbc53dab",
# META       "known_lakehouses": [
# META         {
# META           "id": "2bdfd233-a573-48cb-aad6-c166124a3877"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Descargar datos desde una API REST hacia un archivo en un Lakehouse
# 
# 1. **API REST utilizada: ExchangeRate**  
#    - Se utilizará la API **ExchangeRate** ([https://www.exchangerate-api.com/](https://www.exchangerate-api.com/)), que permite obtener tasas de cambio de divisas en **formato JSON**.
# 
# 2. **Descargar los datos en formato JSON**  
#    - Se obtiene la respuesta de la API y se guarda en un archivo JSON dentro de una carpeta del **Lakehouse**.
# 
# 3. **Configurar el Lakehouse como origen por defecto**  
#    - En el panel izquierdo, selecciona el **Lakehouse** y configúralo como destino predeterminado.
# 
# 4. **Verificar que la carpeta de destino exista en el Lakehouse**  
#    - La carpeta donde se guardará el archivo debe existir previamente en el **Lakehouse**.
# 
# 5. **Garantizar un nombre de archivo único con un UUID**  
#    - Para evitar sobrescribir archivos anteriores, se genera un nombre de archivo único usando un **UUID**.
#    
# ---

# PARAMETERS CELL ********************

API_KEY = '5a4d5e4754c26b636f66a950' # En un proyecto real utilizar Azure Key Vault
CARPETA_DESTINO = 'divisas'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Librerías de Python
import uuid
import requests  
from notebookutils import fs

# URL de la API REST
api_url = f'https://v6.exchangerate-api.com/v6/{API_KEY}/latest/eur'

# Ruta al archivo destino
ruta_fichero_json = f'file:/lakehouse/default/Files/{CARPETA_DESTINO}/datos_{uuid.uuid4()}.json' # Genera un ficehro diferente cada vez

# Llamada a la API ExchangeRate-API 
r = requests.get(api_url)

# Aborta la ejecución si la respuesta de la API es incorrecta
r.raise_for_status()

# Guarda los datos en el archivo JSON
fs.put(ruta_fichero_json, r.text)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
