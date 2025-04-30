import threading
import serve_db
import listener_deriv_v2  # ou o nome correto do seu listener

if __name__ == "__main__":
    t1 = threading.Thread(target=serve_db.run_app)  # função que roda o Flask
    t2 = threading.Thread(target=listener_deriv_v2.start_listener)  # função que inicia o listener
    t1.start()
    t2.start()
    t1.join()
    t2.join()
