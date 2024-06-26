# Importación de las librerías necesarias para la migración de datos de MongoDB a PostgreSQL:
import pymongo # Librería para la conexión a MongoDB
import psycopg2 # Librería para la conexión a PostgreSQL
from psycopg2 import sql   # Librería para la creación de consultas SQL

# Configuración de la conexión a MongoDB Atlas:
mongo_client = pymongo.MongoClient('mongodb+srv://python_user:HmOzPoPd9EC4QOmc@cluster0.tqhnt3m.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0') # Link de conexión a MongoDB Atlas
mongo_database = mongo_client['FinalProject'] # Nombre de la base de datos
mongo_collection = mongo_database['companies'] # Nombre de la colección

# Configuración de la conexión a PostgreSQL:
pg_conn = psycopg2.connect(
    dbname="ztgofwko", # Nombre de la base de datos
    user="ztgofwko",   # Usuario
    password="WmZMhsabs9kZpDKx-I17WGvNu15ddZpo", # Contraseña
    host="bubble.db.elephantsql.com", # Host
    port="5432"
)
pg_cursor = pg_conn.cursor() # Crear cursor (Objeto que permite interactuar con la base de datos) para ejecutar consultas

# Crear la tabla 'companies' en PostgreSQL con la misma estructura que en MongoDB:
create_table_query = """
CREATE TABLE IF NOT EXISTS companies (
    _id INT PRIMARY KEY,
    company TEXT,
    domain TEXT,
    staff INT,
    products INT,
    address TEXT
);
"""
pg_cursor.execute(create_table_query) # Ejecutar la consulta para crear la tabla
pg_conn.commit() # Confirmar los cambios

print("Tabla 'companies' creada correctamente en PostgreSQL.") # Mensaje de confirmación

# Leectura de datos de MongoDB:
documents = mongo_collection.find() # Obtener todos los documentos de la colección 'companies'

# Insertar datos en PostgreSQL:
for doc in documents: # Iteramos sobre los documentos obtenidos de MongoDB para insertarlos en PostgreSQL
    insert_query = sql.SQL("INSERT INTO companies (_id, company, domain, staff, products, address) VALUES (%s, %s, %s, %s, %s, %s)") 
    values = ( 
        int(doc.get('_id')),
        doc.get('company'),
        doc.get('domain'),
        doc.get('staff'),
        doc.get('products'),
        doc.get('address')
    )
    pg_cursor.execute(insert_query, values) # Ejecutar la consulta de inserción

print("Datos insertados correctamente en PostgreSQL.")  # Mensaje de confirmación

# Confirmar cambios y cerrar conexiones:
pg_conn.commit() # Confirmar los cambios
pg_cursor.close() # Cerrar cursor
pg_conn.close() # Cerrar conexión a PostgreSQL
mongo_client.close() # Cerrar conexión a MongoDB

print("Proceso de migración completado.") # Mensaje de confirmación


