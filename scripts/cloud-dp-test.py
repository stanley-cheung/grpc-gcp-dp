import socket

try:
    address = ("2001:4860:8040:0826:0000:00ad:18e9:dc62", 26021)
    sock = socket.create_connection(address, timeout=5)
    sock.close()
    print("successful")
except Exception as e:
    print(f"connection failed: {address}")
