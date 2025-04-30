import threading
import listener_deriv_v2
from serve_db import app

def iniciar():
    # Iniciar o listener de candles em uma thread separada
    t1 = threading.Thread(target=listener_deriv_v2.iniciar_listener, daemon=True)
    t1.start()
    return app  # retorna o objeto Flask para o Gunicorn servir

app = iniciar()
