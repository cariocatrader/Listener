from flask import Flask, jsonify
import sqlite3

app = Flask(__name__)

def get_connection():
    return sqlite3.connect("shared.db")

@app.route("/")
def home():
    return "✅ API do Shared DB está online."

@app.route("/candles/<symbol>")
def get_candles(symbol):
    try:
        con = get_connection()
        cur = con.cursor()
        cur.execute("""
            SELECT epoch, open, high, low, close, volume
            FROM candles
            WHERE symbol = ?
            ORDER BY epoch DESC LIMIT 10
        """, (symbol,))
        rows = cur.fetchall()
        con.close()

        candles = [
            {
                "epoch": r[0],
                "open": r[1],
                "high": r[2],
                "low": r[3],
                "close": r[4],
                "volume": r[5]
            } for r in rows
        ]
        return jsonify(candles)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
