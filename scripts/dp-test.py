import socket

try:
    sock = socket.create_connection(("2001:4860:8040:0826:0000:00ac:b953:bbae", 22), timeout=10)
    print("successful")
    sock.close()
except Exception as e:
    print(f"connection failed: {e}")
