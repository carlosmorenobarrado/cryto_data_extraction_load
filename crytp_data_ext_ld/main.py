import asyncio
import logging
import json # Importar la librería json
from datetime import datetime, timezone
import psycopg2
import os


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
# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

