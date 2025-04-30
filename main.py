import threading
import listener_deriv
import serve_db

# Inicia o listener da Deriv
t1 = threading.Thread(target=listener_deriv.iniciar_listener)
t1.start()

# Inicia o servidor Flask
t2 = threading.Thread(target=serve_db.run_app)
t2.start()
