import http.server
import socketserver
import os

PORT = int(os.environ.get("PORT", 10000))  # Render define automaticamente a porta

Handler = http.server.SimpleHTTPRequestHandler

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print(f"Servindo shared.db em http://0.0.0.0:{PORT}")
    httpd.serve_forever()
