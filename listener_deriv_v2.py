import asyncio
import websockets
import json
import time
import logging
import requests
from collections import defaultdict

APP_ID = "72037"
TOKEN = "a1-xRY5Wg0UzhBaR8jftPFNF3kYvkavb"
WEBSOCKET_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
WEB_SERVICE_URL = "https://server-p2qr.onrender.com/salvar_candle"

forex_symbols = [
    "frxEURUSD", "frxGBPUSD", "frxUSDJPY", "frxAUDUSD", "frxUSDCAD",
    "frxUSDCHF", "frxNZDUSD", "frxEURJPY", "frxGBPJPY", "frxAUDJPY", "frxEURGBP"
]

volatility_symbols = ["R_10", "R_25", "R_50", "R_75", "R_100"]
wanted_symbols = forex_symbols + volatility_symbols

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

ticks_data = defaultdict(list)

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
    return {
        "symbol": symbol,
        "epoch": epoch_minute,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "volume": volume
    }

def enviar_para_webservice(candle):
    try:
        response = requests.post(WEB_SERVICE_URL, json=candle)
        if response.status_code == 200:
            logging.info(f"✅ Candle salvo | {candle['symbol']} | {candle['epoch']} | Close: {candle['close']}")
        else:
            logging.warning(f"⚠️ Falha ao salvar candle: {response.status_code}")
    except Exception as e:
        logging.error(f"Erro ao enviar candle para WebService: {e}")

async def connect_and_listen():
    while True:
        try:
            async with websockets.connect(WEBSOCKET_URL, ping_interval=30) as websocket:
                await websocket.send(json.dumps({"authorize": TOKEN}))
                auth_data = json.loads(await websocket.recv())

                if auth_data.get('msg_type') != 'authorize':
                    logging.error("❌ Erro na autenticação")
                    await asyncio.sleep(5)
                    continue

                logging.info("🔑 Autenticado com sucesso")

                for symbol in wanted_symbols:
                    await websocket.send(json.dumps({"ticks": symbol, "subscribe": 1}))
                    logging.info(f"🛎 Subscrito: {symbol}")
                    await asyncio.sleep(0.1)

                current_minute = int(time.time() // 60)

                while True:
                    try:
                        message = await websocket.recv()
                        data = json.loads(message)

                        if data.get('msg_type') == 'tick' and 'tick' in data:
                            tick = data['tick']
                            symbol = tick['symbol']
                            ticks_data[symbol].append(tick)

                        new_minute = int(time.time() // 60)
                        if new_minute != current_minute:
                            for symbol in wanted_symbols:
                                candle = build_candle(symbol)
                                if candle:
                                    enviar_para_webservice(candle)
                            ticks_data.clear()
                            current_minute = new_minute

                    except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK) as e:
                        logging.warning(f"🔌 Conexão encerrada no loop interno: {e}")
                        break  # Sair do loop interno para reconectar

                    except Exception as e:
                        logging.error(f"❗ Erro interno no loop de ticks: {e}")
                        await asyncio.sleep(1)

        except Exception as e:
            logging.error(f"🔥 Erro de conexão externa: {e}")
            await asyncio.sleep(5)

def iniciar_listener():
    logging.info("🚀 Iniciando listener com loop infinito")
    asyncio.run(connect_and_listen())

if __name__ == "__main__":
    iniciar_listener()
