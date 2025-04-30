from flask import Flask, jsonify
import sqlite3

app = Flask(__name__)

@app.route('/')
def home():
    return "✅ Web Service do shared.db está ativo!"

@app.route('/ultimo_candle/<string:symbol>')
def ultimo_candle(symbol):
    try:
        con = sqlite3.connect("shared.db")
        cur = con.cursor()
        cur.execute("""
            SELECT symbol, epoch, open, close
            FROM candles
            WHERE symbol = ?
            ORDER BY epoch DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        con.close()

        if row:
            return jsonify({
                "symbol": row[0],
                "epoch": row[1],
                "open": row[2],
                "close": row[3]
            })
        else:
            return jsonify({"error": "Nenhum dado encontrado"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=10000)
