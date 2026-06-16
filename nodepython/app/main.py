import logging
from fastapi import FastAPI, BackgroundTasks, status, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn
import httpx
import threading

from app import config, database, udp_service, election

# Konfiguracja logowania konsoli
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(threadName)s] %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Cache Coherence Node 2 - Python")

# Klasy Pydantic do walidacji przychodzących obiektów JSON (Kontrakt Heterogeniczny)
class CacheUpdateRequest(BaseModel):
    senderNodeId: int
    variableName: str
    newValue: str
    timestamp: int

class ElectionMessage(BaseModel):
    senderNodeId: int
    type: str

@app.on_event("startup")
def startup_event():
    """Uruchamia procesy UDP w tle podczas startu serwera FastAPI."""
    threading.Thread(target=udp_service.udp_sender_thread, daemon=True, name="UdpSender").start()
    threading.Thread(target=udp_service.udp_receiver_thread, args=(election.start_election,), daemon=True, name="UdpReceiver").start()
    logger.info("Serwisy UDP dla Węzła 2 (Python) zostały pomyślnie uruchomione.")

@app.post("/force-update")
async def handle_force_update(request: CacheUpdateRequest):
    """Endpoint wywoływany przez Lidera (np. Javę) w celu wymuszenia aktualizacji danych."""
    logger.info(f"Otrzymano Write-Update od Lidera dla: {request.variableName} = {request.newValue}")
    database.local_cache.put(request.variableName, request.newValue)
    return {"status": "SUCCESS"}

@app.get("/reconstruct-directory")
async def handle_reconstruct_directory():
    """Zwraca zrzut całego lokalnego cache na żądanie nowego lidera."""
    logger.info("Lider zażądał zrzutu pamięci podręcznej do rekonstrukcji katalogu.")
    return database.local_cache.get_all()

@app.post("/election")
async def handle_election(message: ElectionMessage, background_tasks: BackgroundTasks):
    """Obsługuje zapytania dotyczące algorytmu wyboru lidera Bully."""
    logger.info(f"Bully: Otrzymano komunikat {message.type} od Węzła {message.senderNodeId}")
    
    if message.type == "ELECTION":
        # Jeśli pisze do nas węzeł o NIŻSZYM ID (np. Node 1 - .NET), odpowiadamy True i sami odpalamy wybory
        if config.NODE_ID > message.senderNodeId:
            logger.info(f"Węzeł {message.senderNodeId} ma niższy priorytet. Odpowiadam TRUE.")
            background_tasks.add_task(election.start_election)
            return True
        return False
        
    elif message.type == "COORDINATOR":
        logger.info(f"Uznano nowego lidera w sieci: Węzeł {message.senderNodeId}")
        config.CURRENT_LEADER = message.senderNodeId
        config.NODE_STATE = "NORMAL"
        return {"status": "ACK"}

@app.get("/cache/{key}")
async def get_cache_value(key: str):
    """Pozwala sprawdzić stan lokalnego cache w Pythonie za pomocą przeglądarki."""
    value = database.local_cache.get(key)
    if value is not None:
        return {"key": key, "value": value}
    return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"error": "Key not found"})

@app.post("/update-request")
async def handle_user_update_request(key: str = Query(...), value: str = Query(...)):
    """
    Endpoint wywoływany przez użytkownika w celu zapisu/modyfikacji danych.
    Działa w pełnym trybie lidera (Home Node), jeśli Python wygrał wybory Bully.
    """
    logger.info(f"Otrzymano lokalne żądanie zapisu: {key} = {value}")

    # 1. SPRAWDZENIE CZY JESTEŚMY LIDEREM
    if config.CURRENT_LEADER == config.NODE_ID:
        logger.info("Węzeł 2 (Python) działa jako LIDER. Zapisuję i replikuję dane...")

        # Zapis do własnego, bezpiecznego cache
        database.local_cache.put(key, value)

        # Przygotowanie paczki danych do replikacji (kontrakt JSON)
        replication_payload = {
            "senderNodeId": config.NODE_ID,
            "variableName": key,
            "newValue": value,
            "timestamp": int(time.time() * 1000)
        }

        # Asynchroniczne rozsyłanie (Write-Update Broadcast) do rówieśników
        async with httpx.AsyncClient() as client:
            for peer_id, peer_info in config.PEERS.items():
                try:
                    logger.info(f"Replikacja: Wysyłam aktualizację do Węzła {peer_id} na {peer_info['url']}/force-update")
                    # Wysyłamy żądanie w tło, nie blokując wątku głównego na długo (timeout 1s)
                    await client.post(
                        f"{peer_info['url']}/force-update",
                        json=replication_payload,
                        timeout=1.0
                    )
                except Exception as e:
                    logger.warning(f"Nie udało się zreplikować danych do Węzła {peer_id}: {e}")

        return {"status": "SUCCESS", "message": "Zmienna zaktualizowana i rozreplikowana przez Lidera (Python)."}

    # 2. JEŚLI NIE JESTEŚMY LIDEREM -> DELEGACJA/PRZEKIEROWANIE
    else:
        leader_id = config.CURRENT_LEADER
        leader_url = config.PEERS.get(leader_id, {}).get("url")
        target_url = f"{leader_url}/update-request"

        logger.info(f"Węzeł 2 działa jako Proxy. Przekazuję żądanie w tle do lidera na: {target_url}")

        async with httpx.AsyncClient() as client:
            try:
                # Przekazujemy zapytanie do Javy
                response = await client.post(target_url, params={"key": key, "value": value}, timeout=2.0)

                # POPRAWKA: Zamiast response.json(), czytamy bezpieczny tekst z Javy
                return JSONResponse(
                    status_code=response.status_code,
                    content={
                        "status": "SUCCESS_VIA_PROXY",
                        "leader_response": response.text  # bezpieczne przypisanie tekstu
                    }
                )
            except Exception as e:
                return JSONResponse(
                    status_code=500,
                    content={"error": f"Błąd komunikacji Proxy z Liderem: {e}"}
                )

if __name__ == "__main__":
    # Uruchomienie serwera aplikacji na porcie 8082
    uvicorn.run(app, host=config.NODE_IP, port=config.HTTP_PORT)