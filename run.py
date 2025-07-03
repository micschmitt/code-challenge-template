import os
import socket
import sys
from app import create_app
from app.commands import register_commands

app = create_app()
register_commands(app)

def find_free_port(start_port=5000):
    for port in range(start_port, start_port + 100):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(('127.0.0.1', port))
            sock.close()
            return port
        except OSError:
            continue
    return None

if __name__ == '__main__':
    if len(sys.argv) > 1:
        pass  # CLI mode
    else:
        port = find_free_port(5000)
        if port is None:
            print("Could not find a free port")
            exit(1)
        
        print(f"Starting server on http://127.0.0.1:{port}")
        print(f"API Documentation: http://127.0.0.1:{port}/docs/")
        print(f"Weather Data API: http://127.0.0.1:{port}/weather")
        print(f"Weather Stats API: http://127.0.0.1:{port}/weather/stats")
        
        app.run(debug=False, host='127.0.0.1', port=port)
