import logging
import psycopg2
import os

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_HOST = "192.168.1.49"
DB_NAME = "criptodb"
DB_USER = "admincar"
DB_PASSWORD = "1234car"
DB_PORT = "5432"
SSL_MODE = 'require' 

try:
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        #sslmode=SSL_MODE, 
    )
    cur = conn.cursor()
    print(f"Intentando conectar a PostgreSQL en {DB_HOST}:{DB_PORT}...")
    
except psycopg2.Error as e:
    print(f"Error al conectar o interactuar con la base de datos: {e}")

# Cargar el secreto desde la variable de entorno
# El nombre 'MI_SECRETO_EN_PYTHON' debe coincidir con el que definiste en el archivo .yml
api_key = os.environ.get('BINANCE_API_KEY')
api_secret = os.environ.get('BINANCE_API_SECRET')