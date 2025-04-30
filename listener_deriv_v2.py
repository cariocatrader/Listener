import asyncio
import websockets
import json
import sqlite3
import time
import logging
from collections import defaultdict

# ======================
# ⚙️ CONFIGURATIONS
# ======================
APP_ID = "72037"
TOKEN = "a1-xRY5Wg0UzhBaR8jftPFNF3kYvkavb"
WEBSOCKET_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
RECONNECT_DELAY = 5  # seconds between connection attempts
HEARTBEAT_INTERVAL = 30  # send ping every 30 seconds
DB_TIMEOUT = 10  # database connection timeout

# Tracked assets
FOREX_SYMBOLS = [
    "frxEURUSD", "frxUSDJPY", "frxGBPUSD", "frxAUDUSD", "frxUSDCHF",
    "frxUSDCAD", "frxNZDUSD", "frxEURJPY", "frxGBPJPY", "frxEURGBP"
]
VOLATILITY_SYMBOLS = ["R_10", "R_25", "R_50", "R_75", "R_100"]
ALL_SYMBOLS = FOREX_SYMBOLS + VOLATILITY_SYMBOLS

# ======================
# 📦 INITIALIZATION
# ======================
def init_db():
    """Initialize database connection"""
    conn = sqlite3.connect('shared.db', check_same_thread=False, timeout=DB_TIMEOUT)
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
    return conn, cursor

# Initialize database
db_conn, db_cursor = init_db()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("listener.log"),
        logging.StreamHandler()
    ]
)

# Global variables
ticks_data = defaultdict(list)
last_heartbeat = time.time()

# ======================
# 🛠️ UTILITY FUNCTIONS
# ======================
def save_candle(symbol, candle):
    """Save candle to database"""
    try:
        db_cursor.execute('''
        INSERT OR IGNORE INTO candles (symbol, epoch, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (symbol, candle['epoch'], candle['open'], candle['high'], 
              candle['low'], candle['close'], candle['volume']))
        db_conn.commit()
        logging.info(f"✅ Candle saved | {symbol} | {candle['epoch']} | Close: {candle['close']}")
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        handle_db_error()

def handle_db_error():
    """Handle database connection errors"""
    global db_conn, db_cursor
    try:
        db_conn.close()
        db_conn, db_cursor = init_db()
        logging.info("Database connection reestablished")
    except Exception as e:
        logging.error(f"Failed to reconnect to database: {e}")

def build_candle(symbol):
    """Build candle from ticks data"""
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

# ======================
# 🌐 WEBSOCKET FUNCTIONS
# ======================
async def send_heartbeat(websocket):
    """Send ping to keep connection alive"""
    global last_heartbeat
    try:
        await websocket.send(json.dumps({"ping": 1}))
        last_heartbeat = time.time()
        logging.debug("❤️ Heartbeat sent")
    except Exception as e:
        logging.warning(f"Heartbeat failed: {e}")
        raise

async def subscribe_to_symbols(websocket):
    """Subscribe to all symbols"""
    for symbol in ALL_SYMBOLS:
        try:
            await websocket.send(json.dumps({
                "ticks": symbol,
                "subscribe": 1
            }))
            logging.info(f"🔔 Subscribed to {symbol}")
            await asyncio.sleep(0.1)  # Small delay between subscriptions
        except Exception as e:
            logging.error(f"Subscription error for {symbol}: {e}")
            raise

async def handle_messages(websocket):
    """Process incoming WebSocket messages"""
    global last_heartbeat
    current_minute = int(time.time() // 60)
    
    while True:
        try:
            # Check if heartbeat is needed
            if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                await send_heartbeat(websocket)

            # Receive data with timeout
            try:
                response = await asyncio.wait_for(websocket.recv(), timeout=HEARTBEAT_INTERVAL)
            except asyncio.TimeoutError:
                await send_heartbeat(websocket)
                continue

            data = json.loads(response)

            # Handle ping responses
            if data.get("msg_type") == "ping":
                logging.debug("❤️ Heartbeat response received")
                continue

            # Process tick data
            if data.get('msg_type') == 'tick':
                tick = data['tick']
                symbol = tick['symbol']
                ticks_data[symbol].append(tick)
                logging.debug(f"📊 Tick received | {symbol} | Price: {tick['quote']}")

            # Build candles at minute change
            new_minute = int(time.time() // 60)
            if new_minute != current_minute:
                for symbol in ALL_SYMBOLS:
                    candle = build_candle(symbol)
                    if candle:
                        save_candle(symbol, candle)
                ticks_data.clear()
                current_minute = new_minute

        except websockets.exceptions.ConnectionClosed as e:
            logging.warning(f"🔌 Connection closed: {e}")
            raise
        except Exception as e:
            logging.error(f"Message handling error: {e}")
            raise

async def maintain_connection():
    """Main WebSocket connection loop"""
    while True:
        try:
            logging.info("🔗 Connecting to WebSocket...")
            async with websockets.connect(
                WEBSOCKET_URL,
                ping_interval=None,
                close_timeout=1,
                max_queue=1024
            ) as websocket:
                # Authenticate
                await websocket.send(json.dumps({"authorize": TOKEN}))
                auth_response = await websocket.recv()
                auth_data = json.loads(auth_response)

                if auth_data.get('msg_type') != 'authorize':
                    logging.error("❌ Authentication failed")
                    raise ConnectionError("Authentication failed")

                logging.info("🔑 Successfully authenticated")
                
                # Subscribe to symbols
                await subscribe_to_symbols(websocket)
                
                # Start message processing
                await handle_messages(websocket)

        except (websockets.exceptions.ConnectionClosed, ConnectionError) as e:
            logging.warning(f"⚡ Connection lost: {e}. Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)
        except Exception as e:
            logging.error(f"⚠️ Critical error: {e}. Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)

# ======================
# 🚀 MAIN FUNCTIONS
# ======================
async def main_async():
    """Main async loop"""
    while True:
        try:
            await maintain_connection()
        except Exception as e:
            logging.error(f"Main loop error: {e}. Restarting...")
            await asyncio.sleep(RECONNECT_DELAY)

def iniciar_listener():
    """Thread-safe entry point"""
    logging.info("🚀 Starting candle listener V8 (thread-safe)")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main_async())
    except KeyboardInterrupt:
        logging.info("👋 Listener stopped by user")
    finally:
        loop.close()
        db_conn.close()

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logging.info("👋 Listener stopped by user")
    finally:
        db_conn.close()
