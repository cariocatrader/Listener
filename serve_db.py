from flask import Flask, request, jsonify
import sqlite3

app = Flask(__name__)
DB_PATH = "shared.db"

@app.route("/get_candle", methods=["GET"])
def get_candle():
    try:
        paridade = request.args.get("paridade")
        timeframe = int(request.args.get("timeframe", 60))
        timestamp = int(request.args.get("timestamp"))

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        cur.execute("""
            SELECT symbol, epoch, open, high, low, close, volume
            FROM candles
            WHERE symbol = ?
            AND epoch <= ?
            ORDER BY epoch DESC
            LIMIT 1
        """, (paridade, timestamp))

        row = cur.fetchone()
        conn.close()

        if row:
            candle = {
                "symbol": row[0],
                "epoch": row[1],
                "open": row[2],
                "high": row[3],
                "low": row[4],
                "close": row[5],
                "volume": row[6]
            }
            return jsonify({"success": True, "candle": candle})
        else:
            return jsonify({"success": False, "error": "Candle não encontrado"})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/", methods=["GET"])
def home():
    return "Servidor de dados online", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
