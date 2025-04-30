from flask import Flask, jsonify
import sqlite3

app = Flask(__name__)

@app.route("/")
def home():
    return "🔗 API ativa - Banco compartilhado."

@app.route("/paridades")
def listar_paridades():
    try:
        con = sqlite3.connect("shared.db")
        cur = con.cursor()
        cur.execute("SELECT DISTINCT symbol FROM candles ORDER BY symbol ASC")
        dados = [row[0] for row in cur.fetchall()]
        con.close()
        return jsonify({"paridades": dados})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/ultimos/<symbol>")
def ultimos_candles(symbol):
    try:
        con = sqlite3.connect("shared.db")
        cur = con.cursor()
        cur.execute("SELECT * FROM candles WHERE symbol=? ORDER BY epoch DESC LIMIT 5", (symbol,))
        candles = cur.fetchall()
        con.close()
        return jsonify({"symbol": symbol, "candles": candles})
    except Exception as e:
        return jsonify({"error": str(e)})
