import asyncio
import websockets
import json
import sqlite3
import time
import logging
from collections import defaultdict
import requests

# Configurações
TOKEN = "seu_token_deriv"
APP_ID = "72037"
WEBSOCKET_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
SYMBOLS = ["R_10", "R_25", "R_50", "R_75", "R_100", "frxEURUSD", "frxUSDJPY"]

ticks_data = defaultdict(list)

def build_candle(symbol, ticks):
    ticks.sort(key=lambda x: x['epoch'])
    open_ = ticks[0]['quote']
    close = ticks[-1]['quote']
    high = max(t['quote'] for t in ticks)
    low = min(t['quote'] for t in ticks)
    epoch = ticks[0]['epoch'] - (ticks[0]['epoch'] % 60)
    return {
        "symbol": symbol,
        "epoch": epoch,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": len(ticks)
    }

async def connect_and_listen():
    while True:
        try:
            async with websockets.connect(WEBSOCKET_URL) as ws:
                await ws.send(json.dumps({"authorize": TOKEN}))
                auth = await ws.recv()
                print("✅ Autenticado!")

                for s in SYMBOLS:
                    await ws.send(json.dumps({"ticks": s, "subscribe": 1}))
                    await asyncio.sleep(0.1)

                current_minute = int(time.time() // 60)

                while True:
                    data = json.loads(await ws.recv())
                    if data.get("msg_type") == "tick":
                        tick = data["tick"]
                        symbol = tick["symbol"]
                        ticks_data[symbol].append(tick)

                        new_minute = int(time.time() // 60)
                        if new_minute != current_minute:
                            for symbol, ticks in ticks_data.items():
                                candle = build_candle(symbol, ticks)
                                try:
                                    requests.post("https://server-p2qr.onrender.com/salvar_candle", json=candle)
                                    print(f"🔥 Candle salvo: {symbol} - {candle['epoch']}")
                                except Exception as e:
                                    print(f"Erro ao enviar candle: {e}")
                            ticks_data.clear()
                            current_minute = new_minute

        except Exception as e:
            print(f"Erro na conexão WebSocket: {e}")
            time.sleep(5)

def iniciar_listener():
    print("🚀 Iniciando listener via função")
    asyncio.run(connect_and_listen())
