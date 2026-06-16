import httpx
import logging
import asyncio
import threading
from app import config, database

logger = logging.getLogger(__name__)

def start_election():
    """Inicjuje algorytm Bully w osobnym pętli asynchronicznej."""
    asyncio.run(start_election_async())

async def start_election_async():
    if config.NODE_STATE == "ELECTION":
        return
    config.NODE_STATE = "ELECTION"
    logger.info(f"Węzeł {config.NODE_ID} rozpoczyna wybory lidera...")

    # Filtrujemy węzły o WYŻSZYM ID niż nasze (Dla Pythona [ID:2] wyższy jest tylko Węzeł 3 [Java])
    higher_nodes = {k: v for k, v in config.PEERS.items() if k > config.NODE_ID}
    
    if not higher_nodes:
        await announce_victory()
        return

    received_answer = False
    async with httpx.AsyncClient() as client:
        for peer_id, peer_info in higher_nodes.items():
            try:
                # Wysyłamy żądanie do Javy
                response = await client.post(
                    f"{peer_info['url']}/election", 
                    json={"senderNodeId": config.NODE_ID, "type": "ELECTION"},
                    timeout=1.0
                )
                if response.status_code == 200 and response.json() is True:
                    received_answer = True
            except Exception:
                logger.debug(f"Węzeł {peer_id} (wyższy) nie odpowiedział.")

    if not received_answer:
        logger.info("Żaden wyższy węzeł nie odpowiedział. Węzeł 2 (Python) wygrywa wybory!")
        await announce_victory()
    else:
        logger.info("Wyższy węzeł przejął wybory. Oczekiwanie na komunikat COORDINATOR.")
        config.NODE_STATE = "NORMAL"

async def announce_victory():
    config.CURRENT_LEADER = config.NODE_ID
    config.NODE_STATE = "NORMAL"
    logger.info(f"Węzeł {config.NODE_ID} ogłasza się NOWYM LIDEREM.")
    
    # Gdyby Python został liderem, odpytałby Javę lub .NET o ich cache:
    # (Logika analogiczna do Javy - odtwarzanie DirectoryManager z pustego katalogu)
    
    async with httpx.AsyncClient() as client:
        for peer_id, peer_info in config.PEERS.items():
            try:
                await client.post(
                    f"{peer_info['url']}/election",
                    json={"senderNodeId": config.NODE_ID, "type": "COORDINATOR"},
                    timeout=1.0
                )
            except Exception:
                pass