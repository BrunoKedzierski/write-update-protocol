package com.example.node.service;

import com.example.node.model.SystemNode;
import com.example.node.model.LocalCache;
import com.example.node.model.DirectoryManager;
import com.example.node.dto.ElectionMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Serwis realizujący algorytm wyboru lidera (Bully Algorithm)
 * oraz procedurę odzyskiwania stanu po awarii (State Recovery).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BullyElectionService {

    private final SystemNode systemNode;
    private final LocalCache localCache;
    private final DirectoryManager directoryManager; // Komponent aktywowany po wygranych wyborach
    private final WebClient.Builder webClientBuilder;

    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);

    /**
     * Inicjuje procedurę wyborów (wywoływane po wykryciu timeoutu Lidera lub przy starcie węzła/Recovery).
     */
    public void startElection() {
        // Zabezpieczenie przed wielokrotnym uruchomieniem procedury w tym samym czasie
        if (!electionInProgress.compareAndSet(false, true)) {
            log.info("Wybory są już w toku. Ignorowanie ponownego wywołania.");
            return;
        }

        systemNode.setState("ELECTION");
        log.info("Węzeł {} rozpoczyna procedurę wyborów lidera (Algorytm Bully)...", systemNode.getNodeId());

        // Filtrujemy węzły, które mają WYŻSZE ID niż nasze
        Map<Integer, String> higherNodes = systemNode.getPeers().entrySet().stream()
                .filter(entry -> entry.getKey() > systemNode.getNodeId())
                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (higherNodes.isEmpty()) {
            // Jesteśmy węzłem o najwyższym ID w sieci (Węzeł 3 zazwyczaj tak ma)!
            // Automatycznie wygrywamy wybory.
            log.info("Brak węzłów o wyższym ID. Węzeł {} ogłasza się nowym Liderem.", systemNode.getNodeId());
            announceVictory();
        } else {
            // Wysyłamy komunikat ELECTION do węzłów o wyższym ID
            AtomicBoolean receivedAnswer = new AtomicBoolean(false);

            Flux.fromIterable(higherNodes.entrySet())
                    .flatMap(entry -> sendElectionMessage(entry.getValue(), entry.getKey()))
                    .timeout(Duration.ofMillis(1500)) // Agresywny timeout sieciowy, aby nie blokować systemu
                    .doOnNext(answer -> {
                        if (Boolean.TRUE.equals(answer)) {
                            receivedAnswer.set(true);
                        }
                    })
                    .onErrorResume(e -> Mono.empty()) // Ignorujemy błędy połączenia (węzeł wyłączony)
                    .then()
                    .doOnTerminate(() -> {
                        if (!receivedAnswer.get()) {
                            // Żaden wyższy węzeł nie odpowiedział w wyznaczonym czasie – wygrywamy!
                            log.info("Żaden węzeł o wyższym ID nie odpowiedział. Węzeł {} wygrywa wybory.", systemNode.getNodeId());
                            announceVictory();
                        } else {
                            // Ktoś wyższy odpowiedział, więc przejmuje proces wyborów. Czekamy na jego ogłoszenie (COORDINATOR)
                            log.info("Wyższy węzeł odpowiedział. Ustępowanie miejsca w wyborach i oczekiwanie na nowego lidera.");
                            electionInProgress.set(false);
                        }
                    })
                    .subscribe();
        }
    }

    /**
     * Wysyła komunikat typu ELECTION do konkretnego węzła.
     */
    private Mono<Boolean> sendElectionMessage(String nodeUrl, int targetNodeId) {
        WebClient client = webClientBuilder.baseUrl(nodeUrl).build();
        return client.post()
                .uri("/election")
                .bodyValue(new ElectionMessage(systemNode.getNodeId(), "ELECTION"))
                .retrieve()
                .bodyToMono(Boolean.class)
                .doOnError(err -> log.debug("Węzeł {} nie odpowiedział na komunikat wyborczy (prawdopodobnie offline).", targetNodeId))
                .onErrorReturn(false);
    }

    /**
     * Ogłoszenie zwycięstwa i wysłanie komunikatu COORDINATOR do wszystkich pozostałych węzłów.
     */
    private void announceVictory() {
        systemNode.updateLeader(systemNode.getNodeId());

        // KROK NAPRAWCZY (Zarzut 1, 2 i 5 prowadzącego):
        // Zanim zaczniemy w pełni zarządzać siecią, musimy odtworzyć dane katalogu globalnego.
        reconstructGlobalDirectoryFromPeers();

        // Rozsyłanie wiadomości COORDINATOR do rówieśników (asynchronicznie, nieblokująco)
        Flux.fromIterable(systemNode.getPeers().entrySet())
                .flatMap(entry -> {
                    WebClient client = webClientBuilder.baseUrl(entry.getValue()).build();
                    return client.post()
                            .uri("/election")
                            .bodyValue(new ElectionMessage(systemNode.getNodeId(), "COORDINATOR"))
                            .retrieve()
                            .toBodilessEntity()
                            .onErrorResume(e -> Mono.empty());
                })
                .subscribe();

        electionInProgress.set(false);
        log.info("Węzeł {} pomyślnie rozesłał status nowego Lidera (COORDINATOR) i zakończył wybory.", systemNode.getNodeId());
    }

    /**
     * Procedura rekonstrukcji globalnego katalogu (Directory Manager) na nowo wybranym liderze.
     * Pobiera kopie lokalnych pamięci podręcznych od wszystkich aktywnych węzłów (C# i Python).
     */
    private void reconstructGlobalDirectoryFromPeers() {
        log.info("Uruchamianie procedury rekonstrukcji stanu katalogu z aktywnych węzłów...");

        // Czyszczenie starego, niepewnego stanu katalogu lokalnego
        directoryManager.clearDirectory();

        Flux.fromIterable(systemNode.getPeers().entrySet())
                .flatMap(entry -> {
                    WebClient client = webClientBuilder.baseUrl(entry.getValue()).build();
                    return client.get()
                            .uri("/reconstruct-directory") // Specjalny endpoint dodany u rówieśników
                            .retrieve()
                            .bodyToMono(Map.class) // Odbieramy mapę [Nazwa_Zmiennej -> Wartość] z lokalnego cache węzła
                            .map(cacheMap -> Map.entry(entry.getKey(), cacheMap))
                            .onErrorResume(e -> Mono.empty());
                })
                .doOnNext(entry -> {
                    int peerId = entry.getKey();
                    Map<String, String> peerCache = entry.getValue();

                    // Rejestrujemy dane w DirectoryManagerze nowego lidera
                    peerCache.forEach((variableName, value) -> {
                        directoryManager.registerVariablePresence(variableName, peerId);
                        directoryManager.updateMainMemoryValue(variableName, value);
                    });
                    log.info("Pomyślnie zsynchronizowano i odtworzono stan dla zasobów od Węzła {}.", peerId);
                })
                .then()
                .doOnTerminate(() -> {
                    // Dodatkowo, Lider dorzuca do katalogu i pamięci głównej to, co sam ma w pamięci podręcznej
                    localCache.getAll().forEach((varName, value) -> {
                        directoryManager.registerVariablePresence(varName, systemNode.getNodeId());
                        directoryManager.updateMainMemoryValue(varName, value);
                    });
                    log.info("Rekonstrukcja katalogu zakończona sukcesem. Stan obecności i wartości odtworzony.");
                })
                .subscribe();
    }
}
