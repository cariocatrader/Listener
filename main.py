import threading
from serve_db import app
import listener_deriv_v2

if __name__ == '__main__':
    t1 = threading.Thread(target=listener_deriv_v2.iniciar_listener, daemon=True)
    t1.start()

    app.run(host="0.0.0.0", port=10000)
