import threading
from serve_db import app  # Flask app
import listener_deriv_v2  # Módulo que contém iniciar_listener()

if __name__ == '__main__':
    # Iniciar listener em thread separada
    t1 = threading.Thread(target=listener_deriv_v2.iniciar_listener, daemon=True)
    t1.start()

    # Iniciar Flask
    app.run(host="0.0.0.0", port=10000)
