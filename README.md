# cryto_data_extraction_load

## Proceso BATCH

- Batch (cada minuto vía CronJob):
- OHLCV (BTC/ETH, 1m/5m/15m).
- AggTrades agregados por minuto.
- Orderbook snapshots (doble foto cada minuto).
- Perp metrics (funding, basis, OI, mark price).

## Esquema de Base de Datos: crypto

### 1. OHLCV (raw_btc_usdt_1m, raw_btc_usdt_5m, raw_btc_usdt_15m)

Velas de precios y volumen.

Columnas:

- Open_time → inicio de la vela (timestamptz).
- Open, High, Low, Close → precios OHLC (numeric).
- Volume → volumen negociado en la base asset (BTC/ETH).
- Close_time → fin de la vela (timestamptz).
- Quote_asset_volume → volumen en la quote asset (ej. USDT).
- Number_of_trades → cantidad de trades en esa vela.
- Taker_buy_base_asset_volume → volumen comprado por takers (en BTC/ETH).
- Taker_buy_quote_asset_volume → volumen comprado por takers (en USDT).
- Ignore → campo sin uso (de Binance).

### 2. AggTrades (agg_trades_1m)

Trades agregados por ventana de 1 minuto.

Columnas:

- ts_window_start / ts_window_end → ventana temporal (timestamptz).
- symbol → par (ej. BTCUSDT).
- buy_vol → volumen ejecutado por compradores agresivos (takers BUY).
- sell_vol → volumen ejecutado por vendedores agresivos (takers SELL).
- total_vol → volumen total (buy_vol + sell_vol).
- trades_count → número de trades en la ventana.

### 3. Orderbook (orderbook_snapshot_1m)

Snapshot del orderbook con doble foto (inicio y final de la ventana de 1m).

Columnas:

- ts_window_start / ts_window_end → ventana temporal.
- symbol → par.
- snapshot_side → start o end (marca si es la foto inicial o final).
- best_bid_price → mejor precio de compra.
- best_bid_qty → cantidad disponible al mejor bid.
- best_ask_price → mejor precio de venta.
- best_ask_qty → cantidad disponible al mejor ask.
- spread → diferencia absoluta entre ask y bid.
- mid_price → precio medio entre bid y ask.
- imbalance → desequilibrio relativo:
(best_bid_qty−best_ask_qty​) / (best_bid_qty−best_ask_qty)

### 4. Perp metrics (perp_metrics_1m)

Indicadores de contratos perpetuos.

Columnas:

- ts → timestamp de captura.
- symbol → par.
- mark_price → precio mark de futuros perpetuos.
- index_price → precio spot de referencia.
- funding_rate → tasa de funding en ese momento.
- next_funding_time → timestamp del próximo funding (si aplica).
- oi → open interest (posición abierta total).
- basis → diferencia relativa entre mark y spot:
index_price/mark_price−1

### 5. Liquidaciones (Streaming) (liquidations_stream)

Eventos de liquidaciones en tiempo real vía WS.

Columnas:

- event_time → timestamp del evento (timestamptz).
- symbol → par.
- side → BUY (short liquidado) o SELL (long liquidado).
- order_type → tipo de orden (LIMIT, MARKET…).
- time_in_force → política de ejecución (IOC, GTC…).
- price → precio nominal de la orden liquidada.
- avg_price → precio medio de ejecución.
- last_filled_qty → cantidad liquidada en la última ejecución.
- cum_filled_qty → cantidad acumulada liquidada.
- orig_qty → cantidad original de la orden liquidada.
- trade_time → timestamp exacto de la ejecución.
- order_id → identificador de la orden (si lo proporciona Binance).


## Resumen

Ya tenemos un data lake estructurado con:

- Velas (OHLCV).
- Microestructura (trades, orderbook).
- Datos de derivados (perps).
- Eventos de riesgo (liquidaciones).