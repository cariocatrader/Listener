import threading
from listener_deriv_v2 import iniciar_listener

if __name__ == "__main__":
    t1 = threading.Thread(target=iniciar_listener, daemon=True)
    t1.start()

    # Mantém o processo principal vivo
    while True:
        pass
