import logging
import psycopg2
import os
from binance import Client
import pandas as pd

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_HOST = "192.168.1.49"
DB_NAME = "criptodb"
DB_USER = "admincar"
DB_PASSWORD = "1234car"
DB_PORT = "5432"
SSL_MODE = 'require' 

try:
    conexion = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        #sslmode=SSL_MODE, 
    )
    conn = conexion.cursor()
    print(f"Intentando conectar a PostgreSQL en {DB_HOST}:{DB_PORT}...")
    
except psycopg2.Error as e:
    print(f"Error al conectar o interactuar con la base de datos: {e}")


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
    "Number of trades",
    "Taker_buy_base_asset_volume",
    "Taker_buy_quote_asset_volume",
    "Ignore"
]

delta = 70  #Numero de minutos previos a extraer

try:
    logging.info("Se inicia el proceso de captura de datos por minuto")

    prices_last_delta_m = client.get_historical_klines("BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, f"{delta*2} minutes ago UTC")
    df_btc = pd.DataFrame(prices_last_delta_m, columns=columns)
    for col in ["Open_time", "Close_time"]:
        df_btc[col] = pd.to_datetime(df_btc[col], unit='ms')
    conn.execute("INSERT INTO main.raw_btc_usdt_1m SELECT * FROM df_btc where Open_time not in (select distinct Open_time from raw_btc_usdt_1m)")
    prices_last_delta_m = client.get_historical_klines("ETHUSDT", Client.KLINE_INTERVAL_1MINUTE, f"{delta*2} minutes ago UTC")
    df_eth = pd.DataFrame(prices_last_delta_m, columns=columns)
    for col in ["Open_time", "Close_time"]:
        df_eth[col] = pd.to_datetime(df_eth[col], unit='ms')
    conn.execute("INSERT INTO main.raw_eth_usdt_1m SELECT * FROM df_eth where Open_time not in (select distinct Open_time from raw_eth_usdt_1m)")

except Exception as e:
    logging.error(f"Parece que hay un error: {e}")

finally:
    logging.info("Finalizado el proceso")