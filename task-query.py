import socket
addresses = [
    ('google.com', 80)]
for address in addresses:
    try:
        sock = socket.create_connection(address, timeout=5)
        print(f"Successful connecting to {address}")
    except Exception as e:
        print(f"Connection failed: {address}")
