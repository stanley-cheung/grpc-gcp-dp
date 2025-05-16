import socket

try:
    sock = socket.create_connection(("2001:4860:8040:0826:0000:00ac:b96d:04df", 14507), timeout=5)
    print("successful")
    sock.close()
except Exception as e:
    print(f"connection failed: {e}")
