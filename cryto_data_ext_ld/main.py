import logging
import psycopg2
import os
from binance import Client
import pandas as pd
from sqlalchemy import create_engine


# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_HOST = "192.168.1.49"
DB_NAME = "criptodb"
DB_USER = "admincar"
DB_PASSWORD = "1234car"
DB_PORT = "5432"
SSL_MODE = 'require' 

# Crear el "motor" de SQLAlchemy para conectar con la base de datos
try:
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)
    logging.info(f"Conexión a PostgreSQL establecida exitosamente con SQLAlchemy en {DB_HOST}.")
except Exception as e:
    logging.error(f"Error al crear el motor de SQLAlchemy: {e}")
    exit() # Salimos si no podemos conectar


#Keys de binance
api_key = os.environ.get('API_KEY')
api_secret = os.environ.get('API_SECRET')


client = Client(
    api_key=api_key, 
    api_secret=api_secret, 
    testnet = False
    )

columns = [
    "Open_time",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Close_time",
    "Quote_asset_volume",
    "Number_of_trades",
    "Taker_buy_base_asset_volume",
    "Taker_buy_quote_asset_volume",
    "Ignore"
]

delta = 70  #Numero de minutos previos a extraer

try:
    logging.info("Se inicia el proceso de captura de datos.")

    # --- Procesar BTC ---
    # 1. Obtener datos de Binance
    klines_btc = client.get_historical_klines("BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, f"{delta*2} minutes ago UTC")
    df_btc = pd.DataFrame(klines_btc, columns=columns)
    df_btc[['Open_time', 'Close_time']] = df_btc[['Open_time', 'Close_time']].apply(pd.to_datetime, unit='ms')
    # 2. Consultar las filas que ya existen en la base de datos para evitar duplicados
    existing_times_btc = pd.read_sql('SELECT DISTINCT "Open_time" FROM crypto.raw_btc_usdt_1m', engine)
    
    # 3. Filtrar el DataFrame para quedarnos solo con las filas nuevas
    df_btc_new = df_btc[~df_btc['Open_time'].isin(existing_times_btc['Open_time'])]

    # 4. Insertar solo las filas nuevas en la base de datos usando to_sql()
    if not df_btc_new.empty:
        df_btc_new.to_sql(
            name='raw_btc_usdt_1m', # Nombre de la tabla
            con=engine,              # El motor de conexión
            schema='crypto',         # Esquema de la tabla
            if_exists='append',      # 'append' para añadir, no sobreescribir
            index=False              # No queremos insertar el índice del DataFrame
        )
        logging.info(f"Insertadas {len(df_btc_new)} nuevas filas para BTCUSDT.")
    else:
        logging.info("No hay filas nuevas para insertar para BTCUSDT.")


    # --- Procesar ETH (repetimos la misma lógica) ---
    klines_eth = client.get_historical_klines("ETHUSDT", Client.KLINE_INTERVAL_1MINUTE, f"{delta*2} minutes ago UTC")
    df_eth = pd.DataFrame(klines_eth, columns=columns)
    df_eth[['Open_time', 'Close_time']] = df_eth[['Open_time', 'Close_time']].apply(pd.to_datetime, unit='ms')

    existing_times_eth = pd.read_sql('SELECT DISTINCT "Open_time" FROM crypto.raw_eth_usdt_1m', engine)
    df_eth_new = df_eth[~df_eth['Open_time'].isin(existing_times_eth['Open_time'])]

    if not df_eth_new.empty:
        df_eth_new.to_sql('raw_eth_usdt_1m', con=engine, schema='crypto', if_exists='append', index=False)
        logging.info(f"Insertadas {len(df_eth_new)} nuevas filas para ETHUSDT.")
    else:
        logging.info("No hay filas nuevas para insertar para ETHUSDT.")


except Exception as e:
    logging.error(f"Parece que hay un error durante el procesamiento: {e}")

finally:
    logging.info("Finalizado el proceso.")