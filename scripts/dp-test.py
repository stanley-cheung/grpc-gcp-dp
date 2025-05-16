import socket

try:
    sock = socket.create_connection(("www.google.com", 80), timeout=5)
    print("successful")
    sock.close()
except Exception as e:
    print(f"connection failed: {e}")
