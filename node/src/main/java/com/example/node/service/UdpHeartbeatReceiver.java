package com.example.node.service;

import com.example.node.model.SystemNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serwis odpowiedzialny za odbieranie i analizę pakietów UDP Heartbeat.
 * Monitoruje stan zdrowia klastra P2P i wykrywa awarie innych węzłów.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class UdpHeartbeatReceiver {

    private final SystemNode systemNode;
    private final BullyElectionService electionService;
    private final ObjectMapper objectMapper; // Do bezpiecznego parsowania JSON z innych środowisk

    // Bezpieczna mapa przechowująca: [ID_Węzła -> Znacznik_Czasu_Ostatniego_Heartbeatu]
    private final Map<Integer, Long> lastHeartbeats = new ConcurrentHashMap<>();

    // Maksymalny czas braku odpowiedzi (np. 5 sekund) przed uznaniem węzła za martwy
    private static final long TIMEOUT_MS = 5000;

    /**
     * Metoda wywoływana przez UdpConfig po odebraniu pakietu UDP.
     * Przetwarza wiadomość tekstową pochodzącą z dowolnego środowiska (Java, C#, Python).
     */
    public void processHeartbeat(String payload) {
        try {
            int senderNodeId;

            // Obsługa elastyczności (zarzut 6 prowadzącego - heterogeniczność)
            // Sprawdzamy, czy przyszedł JSON, czy prosty tekst
            if (payload.trim().startsWith("{")) {
                JsonNode jsonNode = objectMapper.readTree(payload);
                senderNodeId = jsonNode.get("nodeId").asInt();
            } else {
                // Założenie, że tekst to np. "HEARTBEAT_FROM_NODE_1"
                senderNodeId = Integer.parseInt(payload.replaceAll("[^0-9]", ""));
            }

            // Aktualizacja czasu ostatniego kontaktu w bezpiecznej dla wątków mapie
            lastHeartbeats.put(senderNodeId, System.currentTimeMillis());
            log.debug("Odrzymano UDP Heartbeat od Węzła {}", senderNodeId);

        } catch (Exception e) {
            log.error("Błąd podczas parsowania pakietu UDP Heartbeat: {}. Surowa treść: {}", e.getMessage(), payload);
        }
    }

    /**
     * Weryfikator działający cyklicznie w tle (np. co 2 sekundy).
     * Sprawdza, czy któryś z aktywnych węzłów (w szczególności Lider) nie uległ awarii.
     */
    @Scheduled(fixedRate = 2000)
    public void checkNodeTimeouts() {
        long currentTime = System.currentTimeMillis();

        // Pobieramy z SystemNode aktualną listę znanych nam rówieśników (peers) z konfiguracji
        for (Integer peerId : systemNode.getPeers().keySet()) {
            Long lastContact = lastHeartbeats.get(peerId);

            // Jeśli kiedykolwiek mieliśmy kontakt z tym węzłem, sprawdzamy timeout
            if (lastContact != null && (currentTime - lastContact) > TIMEOUT_MS) {
                log.warn("Wykryto timeout dla Węzła {}! Brak odpowiedzi przez {} ms.", peerId, (currentTime - lastContact));

                // Usunięcie z mapy, aby nie triggerować błędu w nieskończoność
                lastHeartbeats.remove(peerId);

                // Wywołanie logiki obsługi awarii węzła
                handleNodeFailure(peerId);
            }
        }
    }

    /**
     * Reakcja na awarię sieciową lub crash procesu innego węzła.
     */
    private void handleNodeFailure(int failedNodeId) {
        // Jeśli z systemu zniknął aktualny Lider, musimy natychmiast ogłosić nowe wybory!
        if (systemNode.getLeaderId() == failedNodeId) {
            log.warn("Zgłoszono awarię Lidera (Węzeł {}). Uruchamianie algorytmu Bully...", failedNodeId);
            electionService.startElection(); // Przejście do stanu Election
        } else {
            // Awaria zwykłego węzła (Followera) - Lider uaktualnia swój katalog (obecność), jeśli to konieczne
            log.info("Węzeł {} (Follower) jest nieaktywny. System kontynuuje pracę.", failedNodeId);
        }
    }
}