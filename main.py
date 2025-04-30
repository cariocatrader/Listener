from serve_db import app
from threading import Thread
import listener_deriv_v2

def start_listener():
    listener_deriv_v2.iniciar_listener()

# Inicia o listener em thread separada
Thread(target=start_listener, daemon=True).start()
