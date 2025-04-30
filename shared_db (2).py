import sqlite3
import logging
import os
from pathlib import Path
from threading import Lock

class SharedDB:
    _instance = None
    _lock = Lock()

    def __new__(cls, db_path='deriv_candles.db'):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self, db_path='deriv_candles.db'):
        if getattr(self, '_initialized', False):
            return

        self._initialized = True
        self.db_path = os.path.abspath(db_path)
        self._setup_db()

    def _setup_db(self):
        """Configura o banco de dados e cria tabelas se não existirem"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        
        with self.conn:
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS candles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                dt_utc TEXT NOT NULL,
                dt_brasil TEXT NOT NULL,
                is_volatil BOOLEAN NOT NULL DEFAULT 0,
                UNIQUE(symbol, timeframe, timestamp)
            )
            """)
            self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_symbol_timeframe 
            ON candles(symbol, timeframe)
            """)

    def insert_candle(self, symbol: str, timeframe: str, timestamp: int,
                     open: float, high: float, low: float, close: float,
                     dt_utc: str, dt_brasil: str, is_volatil: bool = False) -> bool:
        """Insere ou atualiza um candle no banco de dados"""
        try:
            with self.conn:
                self.conn.execute("""
                INSERT INTO candles 
                (symbol, timeframe, timestamp, open, high, low, close, dt_utc, dt_brasil, is_volatil)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, timeframe, timestamp) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close
                """, (symbol, timeframe, timestamp, open, high, low, close, dt_utc, dt_brasil, is_volatil))
            return True
        except Exception as e:
            logging.error(f"Erro ao inserir candle: {e}")
            return False

    def close(self):
        """Fecha a conexão com o banco de dados"""
        if hasattr(self, 'conn'):
            self.conn.close()

# Cria a instância global do banco de dados
db = SharedDB()
