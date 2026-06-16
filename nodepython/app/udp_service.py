import socket
import json
import time
import threading
import logging
from app import config

logger = logging.getLogger(__name__)
LAST_HEARTBEATS = {}  # Słownik przechowujący: {node_id: timestamp}

def udp_sender_thread():
    """Wysyła pakiety UDP Heartbeat co 2 sekundy do rówieśników."""
    # Tworzymy niskopoziomowe gniazdo UDP
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    while True:
        if config.NODE_STATE != "ELECTION":
            payload = {
                "nodeId": config.NODE_ID,
                "status": "ALIVE",
                "timestamp": int(time.time() * 1000)
            }
            message = json.dumps(payload).encode('utf-8')
            
            for peer_id, peer_info in config.PEERS.items():
                try:
                    sock.sendto(message, (peer_info["ip"], config.UDP_PORT))
                except Exception as e:
                    logger.debug(f"Nie udało się wysłać UDP do Węzła {peer_id}: {e}")
                    
        time.sleep(2)

def udp_receiver_thread(start_election_callback):
    """Nasłuchuje pakietów UDP i sprawdza timeouty rówieśników."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((config.NODE_IP, config.UDP_PORT))
    sock.settimeout(1.0) # Zapobiega wiecznemu blokowaniu wątku
    
    # Wątek monitorujący timeouty (wewnątrz odbiornika)
    def timeout_checker():
        while True:
            current_time = time.time() * 1000
            # Jeśli aktualny lider nie przysłał sygnału przez 5 sekund -> Wybory!
            leader_last_contact = LAST_HEARTBEATS.get(config.CURRENT_LEADER)
            if leader_last_contact and (current_time - leader_last_contact) > 5000:
                if config.NODE_STATE != "ELECTION":
                    logger.warning(f"Wykryto timeout Lidera (Węzeł {config.CURRENT_LEADER}). Uruchamianie Bully...")
                    LAST_HEARTBEATS.clear()
                    threading.Thread(target=start_election_callback).start()
            time.sleep(2)
            
    threading.Thread(target=timeout_checker, daemon=True).start()

    while True:
        try:
            data, addr = sock.recvfrom(1024)
            payload = json.loads(data.decode('utf-8'))
            sender_id = payload["nodeId"]
            LAST_HEARTBEATS[sender_id] = time.time() * 1000
        except socket.timeout:
            continue
        except Exception as e:
            logger.error(f"Błąd odbiornika UDP: {e}")