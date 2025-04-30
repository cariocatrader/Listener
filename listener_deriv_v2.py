import asyncio
import websockets
import json
import sqlite3
import time
import logging
from collections import defaultdict

# ======================
# ⚡ CONFIGURAÇÕES
# ======================
APP_ID = "72037"  # Seu app_id oficial
TOKEN = "a1-xRY5Wg0UzhBaR8jftPFNF3kYvkavb"  # Seu token oficial
WEBSOCKET_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"

# Ativos monitorados
forex_symbols = [
    "frxEURUSD", "frxUSDJPY", "frxGBPUSD", "frxAUDUSD", "frxUSDCHF",
    "frxUSDCAD", "frxNZDUSD", "frxEURJPY", "frxGBPJPY", "frxEURGBP"
]
volatility_symbols = [
    "R_10", "R_25", "R_50", "R_75", "R_100"
]
wanted_symbols = forex_symbols + volatility_symbols

# Banco de dados
conn = sqlite3.connect('shared.db', check_same_thread=False)
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS candles (
    symbol TEXT,
    epoch INTEGER,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    PRIMARY KEY (symbol, epoch)
)
''')
conn.commit()

# Log
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

ticks_data = defaultdict(list)

def save_candle(symbol, candle):
    try:
        cursor.execute('''
        INSERT OR IGNORE INTO candles (symbol, epoch, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (symbol, candle['epoch'], candle['open'], candle['high'], candle['low'], candle['close'], candle['volume']))
        conn.commit()
        logging.info(f"✅ Candle salvo | {symbol} | {candle['epoch']} | Close: {candle['close']}")
    except Exception as e:
        logging.error(f"Erro ao salvar candle: {e}")

def build_candle(symbol):
    ticks = ticks_data[symbol]
    if not ticks:
        return None

    ticks.sort(key=lambda x: x['epoch'])
    open_price = ticks[0]['quote']
    close_price = ticks[-1]['quote']
    high_price = max(tick['quote'] for tick in ticks)
    low_price = min(tick['quote'] for tick in ticks)
    volume = len(ticks)
    epoch_minute = ticks[0]['epoch'] - (ticks[0]['epoch'] % 60)

    candle = {
        "symbol": symbol,
        "epoch": epoch_minute,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "volume": volume
    }
    return candle

async def connect_and_listen():
    while True:
        try:
            async with websockets.connect(WEBSOCKET_URL, ping_interval=None) as websocket:
                await websocket.send(json.dumps({"authorize": TOKEN}))
                auth_response = await websocket.recv()
                auth_data = json.loads(auth_response)

                if auth_data.get('msg_type') != 'authorize':
                    logging.error("❌ Erro na autenticação")
                    continue
                else:
                    logging.info("🔑 Autenticado com sucesso")

                # Assinar TICKS corretos
                for symbol in wanted_symbols:
                    subscribe_ticks = {
                        "ticks": symbol,
                        "subscribe": 1
                    }
                    await websocket.send(json.dumps(subscribe_ticks))
                    logging.info(f"🛎 Assinado para receber TICKS de {symbol}")
                    await asyncio.sleep(0.1)

                current_minute = int(time.time() // 60)

                # Escutando WebSocket
                while True:
                    response = await websocket.recv()
                    data = json.loads(response)

                    # DEBUG: Printar cada tick recebido!
                    if data.get('msg_type') == 'tick':
                        tick = data['tick']
                        symbol = tick['symbol']
                        quote = tick['quote']
                        epoch = tick['epoch']

                        logging.info(f"📈 Tick recebido | {symbol} | Preço: {quote} | Horário: {epoch}")

                        ticks_data[symbol].append(tick)

                    # Gerar candle a cada novo minuto
                    new_minute = int(time.time() // 60)
                    if new_minute != current_minute:
                        for symbol in wanted_symbols:
                            candle = build_candle(symbol)
                            if candle:
                                save_candle(symbol, candle)
                        ticks_data.clear()
                        current_minute = new_minute

        except websockets.exceptions.ConnectionClosed as e:
            logging.warning(f"⚡ Conexão WebSocket fechada: {e}. Tentando reconectar...")
            time.sleep(5)
        except Exception as e:
            logging.error(f"Erro inesperado: {e}")
            time.sleep(5)

def iniciar_listener():
    logging.info("🔄 Listener iniciado via função externa")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(connect_and_listen())


if __name__ == "__main__":
    logging.info("🚀 Iniciando listener de candles V6 DEBUG (monitorando ticks)")
    asyncio.run(connect_and_listen())
