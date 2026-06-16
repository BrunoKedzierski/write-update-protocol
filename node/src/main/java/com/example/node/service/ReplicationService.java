package com.example.node.service;

import com.example.node.dto.CacheUpdateRequest;
import com.example.node.model.SystemNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Serwis odpowiedzialny za replikację danych w modelu Write-Update.
 * Kiedy zmienna zostanie zmodyfikowana, serwis ten asynchronicznie rozsyła
 * nową wartość do pozostałych węzłów za pomocą WebClienta (HTTP REST).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ReplicationService {

    private final SystemNode systemNode;
    private final WebClient.Builder webClientBuilder;

    /**
     * Rozgłasza informację o aktualizacji zmiennej (Write-Update Broadcast)
     * do wszystkich pozostałych aktywnych węzłów w klastrze P2P.
     *
     * @param variableName nazwa modyfikowanej zmiennej (klucz)
     * @param newValue nowa wartość zmiennej
     */
    public void broadcastUpdate(String variableName, String newValue) {
        log.info("Inicjowanie asynchronicznego broadcastu Write-Update dla zmiennej: {} = {}", variableName, newValue);

        // Tworzymy ujednolicony kontrakt danych JSON dla środowisk heterogenicznych (C#, Python)
        CacheUpdateRequest updateRequest = new CacheUpdateRequest(
                systemNode.getNodeId(),
                variableName,
                newValue,
                System.currentTimeMillis()
        );

        // Iterujemy po mapie rówieśników sieciowych (Peers) i wysyłamy żądania równolegle
        Flux.fromIterable(systemNode.getPeers().entrySet())
                .flatMap(entry -> {
                    int peerId = entry.getKey();
                    String peerUrl = entry.getValue();

                    log.debug("Wysyłanie aktualizacji do Węzła {} na adres {}", peerId, peerUrl);

                    // Konfiguracja i wykonanie asynchronicznego żądania POST za pomocą WebClienta
                    WebClient webClient = webClientBuilder.baseUrl(peerUrl).build();

                    return webClient.post()
                            .uri("/force-update") // Punkt końcowy wymagany u rówieśników
                            .bodyValue(updateRequest)
                            .retrieve()
                            .toBodilessEntity() // Interesuje nas tylko kod statusu HTTP (np. 200 OK)
                            .timeout(Duration.ofMillis(1000)) // Maksymalnie 1 sekunda oczekiwania na połączenie sieciowe
                            .doOnSuccess(response -> log.info("Węzeł {} pomyślnie zaktualizował pamięć podręczną.", peerId))
                            .doOnError(error -> log.warn("Nie udało się zaktualizować Węzła {} (Węzeł może być offline lub obciążony): {}", peerId, error.getMessage()))
                            .onErrorResume(e -> Mono.empty()); // Przechwytujemy błąd, aby awaria jednego węzła nie przerwała pętli dla innych
                })
                .subscribe(); // KLUCZOWE: Uruchamia asynchroniczny proces w tle. Metoda główna natychmiast wraca do działania.
    }
}