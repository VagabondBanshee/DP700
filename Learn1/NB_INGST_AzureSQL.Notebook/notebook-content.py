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

# ### Descargar datos desde Azure SQL hacia una tabla en un Lakehouse
# 
# 1. **Configurar el Lakehouse como origen por defecto**  
#    - En el panel izquierdo, selecciona el **Lakehouse** y configúralo como destino predeterminado.
# 
# 2. **Conectarse a Azure SQL y cargar los datos en un DataFrame Spark**  
#    - Se utiliza el conector JDBC para leer los datos desde Azure SQL y almacenarlos en un **DataFrame de Spark**.
# 
# 3. **Creación automática de la tabla en el Lakehouse**  
#    - Si la tabla de destino no existe, **se creará automáticamente** en la primera carga.
# 
# 4. **Sobrescritura de datos en la tabla de destino**  
#    - Cada vez que se ejecuta el proceso, **los datos existentes en la tabla de destino se sobrescriben**.
# 
# 5. **Leer los datos desde el Lakehouse en un DataFrame Spark y visualizarlos**  
#    - Después de cargar los datos en la tabla del Lakehouse, se pueden leer nuevamente en un **DataFrame Spark** para su análisis y verificación.
# 
# ---

# PARAMETERS CELL ********************

# Conexión al origen Azure SQL
# En un proyecto real utilizar Azure Key Vault
SQL_SERVIDOR = "sqlservercentralpublic.database.windows.net"
SQL_BASE_DATOS = "AdventureWorks"
SQL_USUARIO = "sqlfamily"
SQL_CONTRASENA = "sqlf@m1ly"
SQL_TABLA = "SalesLT.SalesOrderDetail"

# Nombre de la tabla destino en el lakehouse
TABLA_DESTINO = "SalesOrderDetail"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creando la cadena de conexión JDBC
jdbc_url = f"jdbc:sqlserver://{SQL_SERVIDOR}:1433;databaseName={SQL_BASE_DATOS};encrypt=true;trustServerCertificate=false;loginTimeout=30"

# Propiedades de conexión
connection_properties = {
    "user": SQL_USUARIO,
    "password": SQL_CONTRASENA,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Leer los datos de la tabla desde Azure SQL y guardarlos en un DataFrame de Spark
df = spark.read.jdbc(url=jdbc_url, table=SQL_TABLA, properties=connection_properties)

# Copiar los datos hacia la taba Delta destino en el lakehouse
df.write.format("delta").mode("overwrite").saveAsTable(TABLA_DESTINO)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Leer los datos de la tabla Delta y guardarlos en un DataFrame Spark
df = spark.table(TABLA_DESTINO)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
