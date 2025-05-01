import asyncio
import websockets
import json
import time
import requests
from collections import defaultdict

# Configurações
APP_ID = "72037"
TOKEN = "a1-xRY5Wg0UzhBaR8jftPFNF3kYvkavb"
WEBSOCKET_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
WEBSERVICE_URL = "https://server-p2qr.onrender.com/salvar_candle"

forex_symbols = [
    "frxEURUSD", "frxUSDJPY", "frxGBPUSD", "frxAUDUSD", "frxUSDCHF",
    "frxUSDCAD", "frxNZDUSD", "frxEURJPY", "frxGBPJPY", "frxEURGBP"
]
volatility_symbols = ["R_10", "R_25", "R_50", "R_75", "R_100"]
wanted_symbols = forex_symbols + volatility_symbols

ticks_data = defaultdict(list)

def build_candle(symbol):
    ticks = ticks_data[symbol]
    if not ticks:
        return None

    ticks.sort(key=lambda x: x['epoch'])
    open_price = ticks[0]['quote']
    close_price = ticks[-1]['quote']
    high_price = max(t['quote'] for t in ticks)
    low_price = min(t['quote'] for t in ticks)
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

def salvar_candle_ws(candle):
    try:
        r = requests.post(WEBSERVICE_URL, json=candle, timeout=5)
        if r.status_code == 200:
            print(f"[✅] Candle enviado: {candle['symbol']} | {candle['epoch']} | Close: {candle['close']}")
        else:
            print(f"[⚠️] Falha ao enviar candle ({r.status_code})")
    except Exception as e:
        print(f"[ERRO] Exceção ao enviar candle: {e}")

async def conectar_deriv():
    while True:
        try:
            async with websockets.connect(WEBSOCKET_URL, ping_interval=None) as ws:
                await ws.send(json.dumps({"authorize": TOKEN}))
                res = await ws.recv()
                if 'authorize' not in res:
                    print("[ERRO] Autenticação falhou.")
                    continue

                print("[🔑] Autenticado com sucesso.")

                # Inscreve nos símbolos
                for symbol in wanted_symbols:
                    await ws.send(json.dumps({"ticks": symbol, "subscribe": 1}))
                    print(f"[🛎️] Subscrito em {symbol}")
                    await asyncio.sleep(0.1)

                minuto_atual = int(time.time() // 60)

                while True:
                    data = await ws.recv()
                    msg = json.loads(data)

                    if msg.get("msg_type") == "tick":
                        tick = msg["tick"]
                        symbol = tick["symbol"]
                        ticks_data[symbol].append(tick)

                    novo_minuto = int(time.time() // 60)
                    if novo_minuto != minuto_atual:
                        for symbol in wanted_symbols:
                            candle = build_candle(symbol)
                            if candle:
                                salvar_candle_ws(candle)
                        ticks_data.clear()
                        minuto_atual = novo_minuto

        except Exception as e:
            print(f"[ERRO] WebSocket desconectado. Reconectando... {e}")
            await asyncio.sleep(5)

def iniciar_listener():
    print("🚀 Iniciando listener com envio para WebService")
    asyncio.run(conectar_deriv())

if __name__ == "__main__":
    iniciar_listener()
