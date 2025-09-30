import logging
import os
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
from sqlalchemy import create_engine, text
from binance import Client
import requests
# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- DB ---
DB_HOST = "192.168.1.47"
DB_NAME = "criptodb"
DB_USER = "admincar"
DB_PASSWORD = "1234car"
DB_PORT = "5432"

# --- Binance keys (env) ---
api_key = os.environ.get('BINANCE_API_KEY', '')
api_secret = os.environ.get('BINANCE_API_SECRET', '')

# --- Conexión SQLAlchemy ---
db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)
logging.info(f"Conexión a PostgreSQL OK en {DB_HOST}")

# --- Cliente Binance ---
client = Client(api_key=api_key, api_secret=api_secret, testnet=False)

# --- Config ---
delta = 70  # minutos previos para OHLCV
columns = [
    "Open_time","Open","High","Low","Close","Volume","Close_time",
    "Quote_asset_volume","Number_of_trades","Taker_buy_base_asset_volume",
    "Taker_buy_quote_asset_volume","Ignore"
]

# ========= utilidades DB =========
def ensure_tables():
    with engine.begin() as conn:
        conn.execute(text('CREATE SCHEMA IF NOT EXISTS crypto'))

        # OHLCV: se asume que ya existen (como en tu código original)

        # Order book snapshot 1m (spot)
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS crypto.orderbook_snapshot_1m (
          ts timestamptz NOT NULL,
          symbol text NOT NULL,
          best_bid numeric NOT NULL,
          best_ask numeric NOT NULL,
          spread numeric NOT NULL,
          spread_bps numeric NOT NULL,
          mid numeric NOT NULL,
          bid_qty_top5 numeric NOT NULL,
          ask_qty_top5 numeric NOT NULL,
          obi5 numeric NOT NULL,
          -- nuevas columnas para la "doble foto" y deltas:
          bid_qty_top5_2 numeric,
          ask_qty_top5_2 numeric,
          bid_qty_top5_delta numeric,
          ask_qty_top5_delta numeric,
          PRIMARY KEY (ts, symbol)
        );
        """))
        # Por si la tabla existía de antes sin las nuevas columnas:
        conn.execute(text("ALTER TABLE crypto.orderbook_snapshot_1m ADD COLUMN IF NOT EXISTS bid_qty_top5_2 numeric"))
        conn.execute(text("ALTER TABLE crypto.orderbook_snapshot_1m ADD COLUMN IF NOT EXISTS ask_qty_top5_2 numeric"))
        conn.execute(text("ALTER TABLE crypto.orderbook_snapshot_1m ADD COLUMN IF NOT EXISTS bid_qty_top5_delta numeric"))
        conn.execute(text("ALTER TABLE crypto.orderbook_snapshot_1m ADD COLUMN IF NOT EXISTS ask_qty_top5_delta numeric"))

        # Perp metrics 1m
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS crypto.perp_metrics_1m (
          ts timestamptz NOT NULL,
          symbol text NOT NULL,
          spot_price numeric NOT NULL,
          mark_price numeric NOT NULL,
          basis_rel numeric NOT NULL,
          funding_rate numeric,
          next_funding_time timestamptz,
          open_interest numeric,
          PRIMARY KEY (ts, symbol)
        );
        """))

        # Liquidations 1m agregadas
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS crypto.liquidations_1m (
          ts_window_start timestamptz NOT NULL,
          ts_window_end   timestamptz NOT NULL,
          symbol          text NOT NULL,
          count_liqs      integer NOT NULL,
          qty_liqs        numeric NOT NULL,
          side_buy_qty    numeric NOT NULL, -- shorts liquidados (compras forzadas)
          side_sell_qty   numeric NOT NULL, -- longs liquidados (ventas forzadas)
          PRIMARY KEY (ts_window_start, symbol)
        );
        """))

        # Agg trades 1m (resumen intraminuto sin streaming)
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS crypto.agg_trades_1m (
          ts_window_start timestamptz NOT NULL,
          ts_window_end   timestamptz NOT NULL,
          symbol          text NOT NULL,
          buy_vol         numeric NOT NULL,
          sell_vol        numeric NOT NULL,
          total_vol       numeric NOT NULL,
          vwap_tick       numeric,       -- sum(p*q)/sum(q)
          rv_tick         numeric,       -- std de log-returns de ticks en la ventana
          trades_count    integer NOT NULL,
          PRIMARY KEY (ts_window_start, symbol)
        );
        """))

def upsert_ohlcv(df, table_name: str):
    if df.empty:
        logging.info(f"[{table_name}] vacío.")
        return
    with engine.begin() as conn:
        existing_times = pd.read_sql(f'SELECT DISTINCT "Open_time" FROM crypto.{table_name}', conn)
        df_new = df[~df['Open_time'].isin(existing_times['Open_time'])]
        if not df_new.empty:
            df_new.to_sql(name=table_name, con=conn, schema='crypto', if_exists='append', index=False)
            logging.info(f"[{table_name}] +{len(df_new)} filas")
        else:
            logging.info(f"[{table_name}] sin nuevas filas.")

# ========= Order Book (doble snapshot) =========
def _ob_top5(symbol):
    ob = client.get_order_book(symbol=symbol, limit=5)
    bids = [(float(p), float(q)) for p, q in ob.get("bids", [])]
    asks = [(float(p), float(q)) for p, q in ob.get("asks", [])]
    if not bids or not asks:
        raise ValueError("Order book vacío")
    best_bid, _ = bids[0]
    best_ask, _ = asks[0]
    spread = best_ask - best_bid
    mid = (best_ask + best_bid) / 2
    spread_bps = (spread / mid) * 1e4 if mid else 0.0
    bid_qty_top5 = sum(q for _, q in bids)
    ask_qty_top5 = sum(q for _, q in asks)
    obi5 = (bid_qty_top5 - ask_qty_top5) / (bid_qty_top5 + ask_qty_top5) if (bid_qty_top5 + ask_qty_top5) > 0 else 0.0
    return dict(best_bid=best_bid, best_ask=best_ask, spread=spread, mid=mid,
                spread_bps=spread_bps, bid_qty_top5=bid_qty_top5, ask_qty_top5=ask_qty_top5, obi5=obi5)

def store_orderbook_snapshot_double(symbols, pause_seconds=1.5):
    ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    rows = []
    for sym in symbols:
        try:
            m1 = _ob_top5(sym)
            time.sleep(pause_seconds)  # segunda foto cerca en el tiempo (sin websockets)
            m2 = _ob_top5(sym)
            rows.append({
                "ts": ts, "symbol": sym,
                **m1,
                "bid_qty_top5_2": m2["bid_qty_top5"],
                "ask_qty_top5_2": m2["ask_qty_top5"],
                "bid_qty_top5_delta": m2["bid_qty_top5"] - m1["bid_qty_top5"],
                "ask_qty_top5_delta": m2["ask_qty_top5"] - m1["ask_qty_top5"],
            })
        except Exception as e:
            logging.warning(f"OB doble snapshot fallo {sym}: {e}")

    if rows:
        df = pd.DataFrame(rows)
        with engine.begin() as conn:
            tmp_tbl = "_tmp_ob"
            df.to_sql(tmp_tbl, con=conn, schema='crypto', if_exists='replace', index=False)
            conn.execute(text(f"""
            INSERT INTO crypto.orderbook_snapshot_1m
            (ts, symbol, best_bid, best_ask, spread, spread_bps, mid,
             bid_qty_top5, ask_qty_top5, obi5,
             bid_qty_top5_2, ask_qty_top5_2, bid_qty_top5_delta, ask_qty_top5_delta)
            SELECT ts, symbol, best_bid, best_ask, spread, spread_bps, mid,
                   bid_qty_top5, ask_qty_top5, obi5,
                   bid_qty_top5_2, ask_qty_top5_2, bid_qty_top5_delta, ask_qty_top5_delta
            FROM crypto.{tmp_tbl}
            ON CONFLICT (ts, symbol) DO NOTHING;
            """))
            conn.execute(text(f"DROP TABLE crypto.{tmp_tbl}"))
        logging.info(f"[orderbook_snapshot_1m] +{len(rows)} filas (doble foto)")

# ========= Perp metrics (BTCUSDT) =========
def store_perp_metrics(symbol="BTCUSDT"):
    ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    try:
        spot = float(client.get_symbol_ticker(symbol=symbol)['price'])
        mark = client.futures_mark_price(symbol=symbol)
        mark_price = float(mark['markPrice'])
        basis_rel = (mark_price / spot) - 1.0

        funding_rate = float(mark.get('lastFundingRate')) if mark.get('lastFundingRate') is not None else None
        nft_ms = mark.get('nextFundingTime')
        next_funding_time = datetime.fromtimestamp(nft_ms/1000, tz=timezone.utc) if nft_ms else None

        oi_now = float(client.futures_open_interest(symbol=symbol)['openInterest'])

        row = {
            "ts": ts, "symbol": symbol, "spot_price": spot, "mark_price": mark_price,
            "basis_rel": basis_rel, "funding_rate": funding_rate,
            "next_funding_time": next_funding_time, "open_interest": oi_now
        }
        df = pd.DataFrame([row])
        with engine.begin() as conn:
            df.to_sql("_tmp_perp", con=conn, schema='crypto', if_exists='replace', index=False)
            conn.execute(text("""
            INSERT INTO crypto.perp_metrics_1m
            (ts, symbol, spot_price, mark_price, basis_rel, funding_rate, next_funding_time, open_interest)
            SELECT ts, symbol, spot_price, mark_price, basis_rel, funding_rate, next_funding_time, open_interest
            FROM crypto._tmp_perp
            ON CONFLICT (ts, symbol) DO NOTHING;
            """))
            conn.execute(text("DROP TABLE crypto._tmp_perp"))
        logging.info("[perp_metrics_1m] +1 fila")
    except Exception as e:
        logging.warning(f"Perp metrics fallo {symbol}: {e}")

# ========= Liquidaciones (último minuto, BTCUSDT) =========

def _parse_liq_qty(o):
    # Diferentes posibles nombres según versión
    for k in ("qty", "q", "executedQty", "origQty"):
        v = o.get(k)
        if v not in (None, ""):
            try:
                return float(v)
            except:
                pass
    # Algunos payloads anidan en 'o'
    if isinstance(o.get("o"), dict):
        for k in ("q", "qty", "executedQty", "origQty"):
            v = o["o"].get(k)
            if v not in (None, ""):
                try:
                    return float(v)
                except:
                    pass
    return 0.0

def _parse_liq_side(o):
    # 'side' plano o en 'o' anidado
    side = o.get("side")
    if not side and isinstance(o.get("o"), dict):
        side = o["o"].get("S") or o["o"].get("side")
    return side  # 'BUY' o 'SELL'

def _parse_liq_time_ms(o):
    t = o.get("time") or o.get("T")
    if t is None and isinstance(o.get("o"), dict):
        t = o["o"].get("T") or o["o"].get("time")
    return int(t) if t is not None else None

def store_liquidations_last_min(symbol="BTCUSDT"):
    """
    Liquidaciones último minuto vía endpoint público:
    - Plan A: /fapi/v1/allForceOrders con startTime/endTime.
    - Plan B: si 400, pedir sin tiempos (limit) y filtrar localmente por ventana.
    """
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_ms = int((now - timedelta(minutes=1)).timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)

    base_url = "https://fapi.binance.com/fapi/v1/allForceOrders"
    params = {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000}

    try:
        resp = requests.get(base_url, params=params, timeout=10)
        if resp.status_code == 400:
            # Fallback: sin tiempos, luego filtramos localmente
            fb_params = {"symbol": symbol, "limit": 1000}
            fb = requests.get(base_url, params=fb_params, timeout=10)
            try:
                fb.raise_for_status()
            except Exception as e2:
                # Log del cuerpo para diagnosticar
                raise RuntimeError(f"allForceOrders fallback 400->err: {e2}; body={fb.text[:300]}")
            liqs = fb.json() or []
            # Filtra a la ventana [start_ms, end_ms)
            liqs = [o for o in liqs if (t := _parse_liq_time_ms(o)) is not None and start_ms <= t < end_ms]
        else:
            resp.raise_for_status()
            liqs = resp.json() or []

        count_liqs = len(liqs)
        qty_total = 0.0
        side_buy_qty = 0.0  # BUY = short liquidado (compra forzada)
        side_sell_qty = 0.0 # SELL = long  liquidado (venta forzada)

        for o in liqs:
            qty = _parse_liq_qty(o)
            side = _parse_liq_side(o)
            qty_total += qty
            if side == "BUY":
                side_buy_qty += qty
            elif side == "SELL":
                side_sell_qty += qty

        row = {
            "ts_window_start": datetime.fromtimestamp(start_ms/1000, tz=timezone.utc),
            "ts_window_end":   datetime.fromtimestamp(end_ms/1000, tz=timezone.utc),
            "symbol": symbol,
            "count_liqs": count_liqs,
            "qty_liqs": qty_total,
            "side_buy_qty": side_buy_qty,
            "side_sell_qty": side_sell_qty
        }
        df = pd.DataFrame([row])
        with engine.begin() as conn:
            df.to_sql("_tmp_liq", con=conn, schema='crypto', if_exists='replace', index=False)
            conn.execute(text("""
            INSERT INTO crypto.liquidations_1m
            (ts_window_start, ts_window_end, symbol, count_liqs, qty_liqs, side_buy_qty, side_sell_qty)
            SELECT ts_window_start, ts_window_end, symbol, count_liqs, qty_liqs, side_buy_qty, side_sell_qty
            FROM crypto._tmp_liq
            ON CONFLICT (ts_window_start, symbol) DO NOTHING;
            """))
            conn.execute(text("DROP TABLE crypto._tmp_liq"))
        logging.info("[liquidations_1m] +1 fila (endpoint público allForceOrders, con fallback)")
    except Exception as e:
        logging.warning(f"Liquidaciones fallo {symbol} (público allForceOrders): {e}")


# ========= Agg trades (último minuto, REST) =========
def store_agg_trades_last_min(symbol="BTCUSDT"):
    """
    Resume los aggTrades del último minuto:
    - buy_vol: volumen donde agresor = BUY (m == False)
    - sell_vol: volumen donde agresor = SELL (m == True)
    - vwap_tick: sum(p*q)/sum(q)
    - rv_tick: std de log-returns de precios tick a tick dentro de la ventana
    """
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_ms = int((now - timedelta(minutes=1)).timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)

    try:
        trades = client.get_aggregate_trades(symbol=symbol, startTime=start_ms, endTime=end_ms)
        if not trades:
            logging.info(f"[agg_trades_1m] sin trades para {symbol} en el último minuto")
            return

        prices = []
        qtys = []
        buy_vol = 0.0
        sell_vol = 0.0
        for t in trades:
            p = float(t['p'])
            q = float(t['q'])
            prices.append(p)
            qtys.append(q)
            is_buyer_maker = t['m']  # True => agresor SELL
            if is_buyer_maker:
                sell_vol += q
            else:
                buy_vol += q

        total_vol = buy_vol + sell_vol
        vwap_tick = (sum(p*q for p, q in zip(prices, qtys)) / sum(qtys)) if qtys else None

        # realized volatility sobre ticks (std log-returns). Si <=1 precio, rv=None
        rv_tick = None
        if len(prices) > 1:
            import math
            rets = []
            for i in range(1, len(prices)):
                if prices[i-1] > 0:
                    rets.append(math.log(prices[i] / prices[i-1]))
            if rets:
                # desviación estándar muestral
                mean_r = sum(rets) / len(rets)
                var_r = sum((r - mean_r)**2 for r in rets) / (len(rets)-1) if len(rets) > 1 else 0.0
                rv_tick = var_r**0.5

        row = {
            "ts_window_start": datetime.fromtimestamp(start_ms/1000, tz=timezone.utc),
            "ts_window_end":   datetime.fromtimestamp(end_ms/1000, tz=timezone.utc),
            "symbol": symbol,
            "buy_vol": buy_vol,
            "sell_vol": sell_vol,
            "total_vol": total_vol,
            "vwap_tick": vwap_tick,
            "rv_tick": rv_tick,
            "trades_count": len(trades)
        }
        df = pd.DataFrame([row])
        with engine.begin() as conn:
            df.to_sql("_tmp_agg", con=conn, schema='crypto', if_exists='replace', index=False)
            conn.execute(text("""
            INSERT INTO crypto.agg_trades_1m
            (ts_window_start, ts_window_end, symbol, buy_vol, sell_vol, total_vol, vwap_tick, rv_tick, trades_count)
            SELECT ts_window_start, ts_window_end, symbol, buy_vol, sell_vol, total_vol, vwap_tick, rv_tick, trades_count
            FROM crypto._tmp_agg
            ON CONFLICT (ts_window_start, symbol) DO NOTHING;
            """))
            conn.execute(text("DROP TABLE crypto._tmp_agg"))
        logging.info("[agg_trades_1m] +1 fila")
    except Exception as e:
        logging.warning(f"Agg trades fallo {symbol}: {e}")

# ========= OHLCV (tu bloque original) =========
def run_ohlcv_batch():
    try:
        logging.info("OHLCV batch…")
        # BTC 1m
        k = client.get_historical_klines("BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, f"{delta*2} minutes ago UTC")
        df = pd.DataFrame(k, columns=columns)
        df[['Open_time','Close_time']] = df[['Open_time','Close_time']].apply(pd.to_datetime, unit='ms')
        upsert_ohlcv(df, 'raw_btc_usdt_1m')

        # BTC 5m
        k = client.get_historical_klines("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, f"{delta*2} minutes ago UTC")
        df = pd.DataFrame(k, columns=columns)
        df[['Open_time','Close_time']] = df[['Open_time','Close_time']].apply(pd.to_datetime, unit='ms')
        upsert_ohlcv(df, 'raw_btc_usdt_5m')

        # BTC 15m
        k = client.get_historical_klines("BTCUSDT", Client.KLINE_INTERVAL_15MINUTE, f"{delta*2} minutes ago UTC")
        df = pd.DataFrame(k, columns=columns)
        df[['Open_time','Close_time']] = df[['Open_time','Close_time']].apply(pd.to_datetime, unit='ms')
        upsert_ohlcv(df, 'raw_btc_usdt_15m')

        # ETH 1m
        k = client.get_historical_klines("ETHUSDT", Client.KLINE_INTERVAL_1MINUTE, f"{delta*2} minutes ago UTC")
        df = pd.DataFrame(k, columns=columns)
        df[['Open_time','Close_time']] = df[['Open_time','Close_time']].apply(pd.to_datetime, unit='ms')
        upsert_ohlcv(df, 'raw_eth_usdt_1m')

        # ETH 5m
        k = client.get_historical_klines("ETHUSUT".replace("USUT", "USDT"), Client.KLINE_INTERVAL_5MINUTE, f"{delta*2} minutes ago UTC")  # safe replace por typo común
        df = pd.DataFrame(k, columns=columns)
        df[['Open_time','Close_time']] = df[['Open_time','Close_time']].apply(pd.to_datetime, unit='ms')
        upsert_ohlcv(df, 'raw_eth_usdt_5m')

        # ETH 15m
        k = client.get_historical_klines("ETHUSDT", Client.KLINE_INTERVAL_15MINUTE, f"{delta*2} minutes ago UTC")
        df = pd.DataFrame(k, columns=columns)
        df[['Open_time','Close_time']] = df[['Open_time','Close_time']].apply(pd.to_datetime, unit='ms')
        upsert_ohlcv(df, 'raw_eth_usdt_15m')

        logging.info("OHLCV listo.")
    except Exception as e:
        logging.error(f"Error en OHLCV: {e}")

# ========= MAIN =========
if __name__ == "__main__":
    ensure_tables()

    # 1) Tus OHLCV como hasta ahora
    run_ohlcv_batch()

    # 2) AggTrades del último minuto (intraminuto agregado, sin streaming)
    store_agg_trades_last_min(symbol="BTCUSDT")

    # 3) Doble snapshot de Order Book spot por minuto
    store_orderbook_snapshot_double(symbols=["BTCUSDT", "ETHUSDT"], pause_seconds=1.5)

    # 4) Métricas de Perpetuos (BTCUSDT)
    store_perp_metrics(symbol="BTCUSDT")

    # 5) Liquidaciones agregadas (BTCUSDT, último minuto)
    store_liquidations_last_min(symbol="BTCUSDT")

    logging.info("Ejecución batch finalizada.")
