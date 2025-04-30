from flask import Flask, jsonify, request
import sqlite3

app = Flask(__name__)

@app.route("/")
def index():
    return "API Online - Carioca Shared DB"

@app.route("/candles")
def get_candle():
    symbol = request.args.get("symbol")
    timestamp = request.args.get("timestamp")

    if not symbol or not timestamp:
        return jsonify({"error": "Parâmetros ausentes: symbol e timestamp são obrigatórios."})

    try:
        con = sqlite3.connect("shared.db")
        cur = con.cursor()
        cur.execute("""
            SELECT * FROM candles
            WHERE symbol = ? AND epoch <= ?
            ORDER BY epoch DESC LIMIT 1
        """, (symbol, int(timestamp)))
        row = cur.fetchone()
        con.close()

        if row:
            return jsonify({
                "symbol": row[0],
                "epoch": row[1],
                "open": row[2],
                "high": row[3],
                "low": row[4],
                "close": row[5],
                "volume": row[6]
            })
        else:
            return jsonify({"error": "Candle não encontrado."})

    except Exception as e:
        return jsonify({"error": str(e)})

def iniciar_api():
    app.run(host="0.0.0.0", port=10000)
