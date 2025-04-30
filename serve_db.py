from flask import Flask, jsonify, request
import sqlite3
import os

app = Flask(__name__)

@app.route("/")
def home():
    return jsonify({"status": "API online - Carioca IA"})

@app.route("/candles", methods=["GET"])
def get_candle():
    symbol = request.args.get("symbol")
    timestamp = request.args.get("timestamp", type=int)

    if not symbol or not timestamp:
        return jsonify({"error": "Parâmetros obrigatórios: symbol e timestamp"}), 400

    try:
        con = sqlite3.connect("shared.db")
        cur = con.cursor()
        cur.execute("""
            SELECT symbol, epoch, open, high, low, close, volume 
            FROM candles 
            WHERE symbol = ? AND epoch = ?
            LIMIT 1
        """, (symbol, timestamp))
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
            return jsonify({"error": "Candle não encontrado"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 🔧 ESTA FUNÇÃO ESTAVA FALTANDO!
def run_app():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
