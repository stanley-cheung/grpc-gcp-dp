import socket

address = ("2001:4860:8040:0826:0000:00ac:efb0:4ed9", 14778)

for i in range(0, 10):
    try:
        print(f"trying {address}")
        sock = socket.create_connection(address, timeout=5)
        print("successful")
        sock.close()
    except Exception as e:
        print(f"connection failed: {e}")
