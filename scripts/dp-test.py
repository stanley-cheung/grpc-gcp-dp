import socket

try:
    sock = socket.create_connection(("2001:4860:8040:0826:0000:00ad:0b0d:8a5a", 22), timeout=6)
    print("successful")
    sock.close()
except Exception as e:
    print(f"connection failed: {e}")
