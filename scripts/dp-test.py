import socket

addresses = [
    "2001:4860:8040:0826:0000:00ac:b958:8958",
    "2001:4860:8040:0826:0000:00ac:ffaa:e78b",
    "2001:4860:8040:0826:0000:00ac:e853:9134",
    "2001:4860:8040:0826:0000:00ad:0625:a92d",
    "2001:4860:8040:0826:0000:00ad:0437:1d09",
    "2001:4860:8040:0826:0000:00ac:b959:90ab",
    "2001:4860:8040:0826:0000:00ac:f78f:f705",
    "2001:4860:8040:0826:0000:00ac:b959:9345",
    "2001:4860:8040:0826:0000:00ac:b959:938b",
]

for address in addresses:
    try:
        print(f"trying {address}")
        sock = socket.create_connection((address, 22), timeout=5)
        print("successful")
        sock.close()
    except Exception as e:
        print(f"connection failed: {e}")
