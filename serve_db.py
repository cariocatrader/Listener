from flask import Flask, request, jsonify
import sqlite3

app = Flask(__name__)

@app.route("/")
def home():
    return "✅ WebService da Carioca IA está ativo!"

@app.route("/get_candle", methods=["GET"])
def get_candle():
    paridade = request.args.get("paridade")
    timestamp = request.args.get("timestamp")

    if not paridade or not timestamp:
        return jsonify({"success": False, "error": "Parâmetros 'paridade' e 'timestamp' são obrigatórios."}), 400

    try:
        timestamp = int(timestamp)
        con = sqlite3.connect("shared.db")
        cur = con.cursor()
        cur.execute("""
            SELECT symbol, epoch, open, high, low, close, volume
            FROM candles
            WHERE symbol = ?
            AND epoch <= ?
            ORDER BY epoch DESC
            LIMIT 1
        """, (paridade, timestamp))
        row = cur.fetchone()
        con.close()

        if row:
            return jsonify({
                "success": True,
                "candle": {
                    "symbol": row[0],
                    "epoch": row[1],
                    "open": row[2],
                    "high": row[3],
                    "low": row[4],
                    "close": row[5],
                    "volume": row[6]
                }
            })
        else:
            return jsonify({"success": False, "error": "Candle não encontrado"}), 404

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
