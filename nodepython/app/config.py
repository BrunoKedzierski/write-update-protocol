import os

NODE_ID = 2
NODE_IP = "192.168.1.7"  # IP maszyny z Pythonem (macOS)
HTTP_PORT = 8082
UDP_PORT = 4444

# Adresy IP i URL rówieśników (Java i C#)
PEERS = {
    1: {"ip": "192.168.1.40", "url": "http://192.168.1.40:8081"},  # Node 1: C#
    3: {"ip": "192.168.1.7", "url": "http://192.168.1.7:8083"}   # Node 3: Java
}

# Stan globalny węzła (używamy prostej zmiennej, zmienianej w locie)
CURRENT_LEADER = 3  # Domyślny lider na starcie
NODE_STATE = "NORMAL"  # Modyfikowane na "ELECTION" w trakcie wyborów