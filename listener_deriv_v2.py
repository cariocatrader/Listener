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
APP_ID = "72037"
TOKEN = "a1-xRY5Wg0UzhBaR8jftPFNF3kYvkavb"
WEBSOCKET_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
RECONNECT_DELAY = 5  # segundos entre tentativas de reconexão
HEARTBEAT_INTERVAL = 30  # enviar ping a cada 30 segundos

# Ativos monitorados
forex_symbols = [
    "frxEURUSD", "frxUSDJPY", "frxGBPUSD", "frxAUDUSD", "frxUSDCHF",
    "frxUSDCAD", "frxNZDUSD", "frxEURJPY", "frxGBPJPY", "frxEURGBP"
]
volatility_symbols = ["R_10", "R_25", "R_50", "R_75", "R_100"]
wanted_symbols = forex_symbols + volatility_symbols

# Banco de dados
conn = sqlite3.connect('shared.db', check_same_thread=False, timeout=10)
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
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("listener.log"),
        logging.StreamHandler()
    ]
)

ticks_data = defaultdict(list)
last_heartbeat = time.time()

def save_candle(symbol, candle):
    try:
        cursor.execute('''
        INSERT OR IGNORE INTO candles (symbol, epoch, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (symbol, candle['epoch'], candle['open'], candle['high'], 
              candle['low'], candle['close'], candle['volume']))
        conn.commit()
        logging.info(f"✅ Candle salvo | {symbol} | {candle['epoch']} | Close: {candle['close']}")
    except sqlite3.Error as e:
        logging.error(f"Erro SQL ao salvar candle: {e}")
        # Tentar reconectar ao banco de dados
        try:
            conn.close()
            conn = sqlite3.connect('shared.db', check_same_thread=False, timeout=10)
            cursor = conn.cursor()
        except Exception as db_e:
            logging.error(f"Erro ao reconectar ao banco de dados: {db_e}")

def build_candle(symbol):
    ticks = ticks_data.get(symbol, [])
    if not ticks:
        return None

    ticks.sort(key=lambda x: x['epoch'])
    open_price = ticks[0]['quote']
    close_price = ticks[-1]['quote']
    high_price = max(tick['quote'] for tick in ticks)
    low_price = min(tick['quote'] for tick in ticks)
    volume = len(ticks)
    epoch_minute = ticks[0]['epoch'] - (ticks[0]['epoch'] % 60)

    return {
        "symbol": symbol,
        "epoch": epoch_minute,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "volume": volume
    }

async def send_heartbeat(websocket):
    global last_heartbeat
    try:
        await websocket.send(json.dumps({"ping": 1}))
        last_heartbeat = time.time()
        logging.debug("❤️ Heartbeat enviado")
    except Exception as e:
        logging.warning(f"Falha ao enviar heartbeat: {e}")
        raise

async def subscribe_symbols(websocket):
    for symbol in wanted_symbols:
        try:
            await websocket.send(json.dumps({
                "ticks": symbol,
                "subscribe": 1
            }))
            logging.info(f"🛎 Assinado para receber TICKS de {symbol}")
            await asyncio.sleep(0.1)  # Pequeno delay entre assinaturas
        except Exception as e:
            logging.error(f"Erro ao assinar {symbol}: {e}")
            raise

async def handle_ticks(websocket):
    global last_heartbeat
    current_minute = int(time.time() // 60)
    
    while True:
        try:
            # Verificar se precisa enviar heartbeat
            if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                await send_heartbeat(websocket)

            # Receber dados com timeout para evitar bloqueio
            try:
                response = await asyncio.wait_for(websocket.recv(), timeout=HEARTBEAT_INTERVAL)
            except asyncio.TimeoutError:
                await send_heartbeat(websocket)
                continue

            data = json.loads(response)

            # Processar heartbeat response
            if data.get("msg_type") == "ping":
                logging.debug("❤️ Heartbeat response recebido")
                continue

            if data.get('msg_type') == 'tick':
                tick = data['tick']
                symbol = tick['symbol']
                ticks_data[symbol].append(tick)
                logging.debug(f"📈 Tick recebido | {symbol} | Preço: {tick['quote']} | Horário: {tick['epoch']}")

            # Verificar se mudou o minuto
            new_minute = int(time.time() // 60)
            if new_minute != current_minute:
                for symbol in wanted_symbols:
                    candle = build_candle(symbol)
                    if candle:
                        save_candle(symbol, candle)
                ticks_data.clear()
                current_minute = new_minute

        except websockets.exceptions.ConnectionClosed as e:
            logging.warning(f"⚡ Conexão WebSocket fechada: {e}")
            raise
        except Exception as e:
            logging.error(f"Erro inesperado ao processar ticks: {e}")
            raise

async def connect_and_listen():
    while True:
        try:
            logging.info("🔗 Conectando ao WebSocket...")
            async with websockets.connect(
                WEBSOCKET_URL,
                ping_interval=None,
                close_timeout=1,
                max_queue=1024
            ) as websocket:
                # Autenticar
                await websocket.send(json.dumps({"authorize": TOKEN}))
                auth_response = await websocket.recv()
                auth_data = json.loads(auth_response)

                if auth_data.get('msg_type') != 'authorize':
                    logging.error("❌ Erro na autenticação")
                    raise ConnectionError("Falha na autenticação")

                logging.info("🔑 Autenticado com sucesso")
                
                # Assinar símbolos
                await subscribe_symbols(websocket)
                
                # Iniciar loop principal
                await handle_ticks(websocket)

        except (websockets.exceptions.ConnectionClosed, ConnectionError) as e:
            logging.warning(f"⚡ Conexão perdida: {e}. Reconectando em {RECONNECT_DELAY} segundos...")
            await asyncio.sleep(RECONNECT_DELAY)
        except Exception as e:
            logging.error(f"Erro crítico: {e}. Reconectando em {RECONNECT_DELAY} segundos...")
            await asyncio.sleep(RECONNECT_DELAY)

async def main():
    while True:
        try:
            await connect_and_listen()
        except Exception as e:
            logging.error(f"Erro no loop principal: {e}. Reiniciando...")
            await asyncio.sleep(RECONNECT_DELAY)

if __name__ == "__main__":
    logging.info("🚀 Iniciando listener de candles V7 (com reconexão robusta)")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("👋 Listener encerrado pelo usuário")
    finally:
        conn.close()
