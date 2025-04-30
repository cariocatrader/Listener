from flask import Flask, request, jsonify
import sqlite3

app = Flask(__name__)

DB_PATH = "shared.db"  # Certifique-se de que shared.db está sendo atualizado corretamente

@app.route("/")
def index():
    return "API de candles online."

@app.route("/candles")
def get_candles():
    symbol = request.args.get("symbol")
    limit = request.args.get("limit", default=20, type=int)

    if not symbol:
        return jsonify({"error": "Símbolo não fornecido"}), 400

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT symbol, epoch, open, close
            FROM candles
            WHERE symbol = ?
            ORDER BY epoch DESC
            LIMIT ?
        """, (symbol, limit))

        rows = cursor.fetchall()
        conn.close()

        candles = []
        for row in rows:
            candles.append({
                "symbol": row[0],
                "epoch": row[1],
                "open": row[2],
                "close": row[3]
            })

        candles.reverse()  # Deixar os candles do mais antigo para o mais recente
        return jsonify(candles)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Se quiser rodar localmente
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
