import asyncio
import websockets
import json
import time
import requests
from collections import defaultdict

APP_ID = "72037"
TOKEN = "a1-xRY5Wg0UzhBaR8jftPFNF3kYvkavb"
WEBSOCKET_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
REMOTE_DB_URL = "https://server-p2qr.onrender.com/salvar_candle"

forex_symbols = [
    "frxEURUSD", "frxUSDJPY", "frxGBPUSD", "frxAUDUSD", "frxUSDCHF",
    "frxUSDCAD", "frxNZDUSD", "frxEURJPY", "frxGBPJPY", "frxEURGBP"
]
volatility_symbols = ["R_10", "R_25", "R_50", "R_75", "R_100"]
wanted_symbols = forex_symbols + volatility_symbols

ticks_data = defaultdict(list)

def save_candle(symbol, candle):
    try:
        data = {
            "symbol": symbol,
            "epoch": candle["epoch"],
            "open": candle["open"],
            "high": candle["high"],
            "low": candle["low"],
            "close": candle["close"],
            "volume": candle["volume"]
        }
        response = requests.post(REMOTE_DB_URL, json=data)
        if response.status_code == 200 and response.json().get("success"):
            print(f"✅ Candle enviado | {symbol} | {candle['epoch']} | Close: {candle['close']}")
        else:
            print(f"⚠️ Erro ao enviar candle: {response.text}")
    except Exception as e:
        print(f"❌ Erro ao tentar enviar candle: {e}")

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

async def connect_and_listen():
    while True:
        try:
            async with websockets.connect(WEBSOCKET_URL, ping_interval=None) as ws:
                await ws.send(json.dumps({"authorize": TOKEN}))
                auth_response = await ws.recv()
                if json.loads(auth_response).get("msg_type") != "authorize":
                    print("❌ Erro na autenticação")
                    continue
                print("🔐 Autenticado com sucesso")

                for symbol in wanted_symbols:
                    await ws.send(json.dumps({"ticks": symbol, "subscribe": 1}))
                    await asyncio.sleep(0.05)

                current_minute = int(time.time() // 60)

                while True:
                    data = json.loads(await ws.recv())
                    if data.get("msg_type") == "tick":
                        tick = data["tick"]
                        symbol = tick["symbol"]
                        ticks_data[symbol].append(tick)

                    new_minute = int(time.time() // 60)
                    if new_minute != current_minute:
                        for symbol in wanted_symbols:
                            candle = build_candle(symbol)
                            if candle:
                                save_candle(symbol, candle)
                        ticks_data.clear()
                        current_minute = new_minute

        except Exception as e:
            print(f"⚠️ Erro: {e}")
            await asyncio.sleep(5)

def iniciar_listener():
    print("🚀 Iniciando listener com POST remoto")
    asyncio.run(connect_and_listen())
