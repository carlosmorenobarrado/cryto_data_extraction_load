import logging
import psycopg2

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



#cur.execute(''' 
#CREATE TABLE crypto.acciones_realizadas (Open_time TIMESTAMP, Decision VARCHAR);
#''')
#cur.execute('''
#CREATE TABLE crypto.balance (FechaHora VARCHAR, bitcoin FLOAT, ethereum FLOAT, usdt FLOAT);
#''')
#cur.execute('''
#CREATE TABLE crypto.decision(FechaHora VARCHAR, v_btc FLOAT, v_eth FLOAT, c_btc FLOAT, c_eth FLOAT);
#''')
cur.execute('''
CREATE TABLE crypto.raw_btc_usdt_1m (Open_time TIMESTAMP, Open FLOAT, High FLOAT, Low FLOAT, "Close" FLOAT, Volume FLOAT, Close_time TIMESTAMP, Quote_asset_volume FLOAT, Number_of_trades INTEGER, Taker_buy_base_asset_volume FLOAT, Taker_buy_quote_asset_volume FLOAT, "Ignore" INTEGER);
''')
cur.execute(''' 
CREATE TABLE crypto.raw_eth_usdt_1m (Open_time TIMESTAMP, Open FLOAT, High FLOAT, Low FLOAT, "Close" FLOAT, Volume FLOAT, Close_time TIMESTAMP, Quote_asset_volume FLOAT, Number_of_trades INTEGER, Taker_buy_base_asset_volume FLOAT, Taker_buy_quote_asset_volume FLOAT, "Ignore" INTEGER);
''')

cur.close()