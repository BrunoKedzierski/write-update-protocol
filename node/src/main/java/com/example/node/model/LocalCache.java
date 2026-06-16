package com.example.node.model;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Klasa reprezentująca lokalną pamięć podręczną węzła (Local Cache).
 * * Odpowiada za bezpieczne przechowywanie par [Nazwa_Zmiennej -> Wartość] w pamięci RAM.
 * * Wykorzystuje rygorystyczne mechanizmy współbieżności w celu wyeliminowania
 * wyścigów (Race Conditions) zgłoszonych w zarzucie nr 4 prowadzącego.
 */
@Slf4j
@Component
public class LocalCache {

    // Bezpieczna mapa przechowująca lokalne kopie zmiennych klastra
    private final Map<String, String> cacheStorage = new ConcurrentHashMap<>();

    // Mapa przechowująca dedykowane blokady dla każdej zmiennej osobno (Granular Locking)
    private final Map<String, ReentrantLock> variableLocks = new ConcurrentHashMap<>();

    /**
     * Pobiera wartość zmiennej z lokalnego cache (Operacja odczytu - nieblokująca).
     * Zgodnie z protokołami cache coherence, lokalny odczyt z prawidłowego stanu jest natychmiastowy.
     *
     * @param variableName nazwa zmiennej (klucz)
     * @return wartość zmiennej lub null, jeśli nie istnieje w lokalnym cache
     */
    public String get(String variableName) {
        return cacheStorage.get(variableName);
    }

    /**
     * Zapisuje lub aktualizuje wartość w lokalnej pamięci podręcznej.
     * Metoda zabezpieczona blokadą Lock dla konkretnej zmiennej, aby zapobiec konfliktom
     * jednoczesnego zapisu lokalnego i sieciowego (Write-Update od innego węzła).
     *
     * @param variableName nazwa zmiennej
     * @param value nowa wartość do zapisania
     */
    public void put(String variableName, String value) {
        // Pobieramy istniejący lock dla danej zmiennej lub tworzymy nowy, jeśli to jej pierwszy zapis
        ReentrantLock lock = variableLocks.computeIfAbsent(variableName, k -> new ReentrantLock());

        lock.lock(); // Blokujemy krytyczną sekcję zapisu dla tej konkretnej zmiennej
        try {
            cacheStorage.put(variableName, value);
            log.info("Lokalny Cache: Zaktualizowano stan zmiennej '{}' = '{}'", variableName, value);
        } finally {
            lock.unlock(); // Obowiązkowe zwolnienie blokady w bloku finally
        }
    }

    /**
     * Usuwa zmienną z lokalnego cache (np. w przypadku inwalidacji).
     */
    public void remove(String variableName) {
        ReentrantLock lock = variableLocks.get(variableName);
        if (lock != null) {
            lock.lock();
            try {
                cacheStorage.remove(variableName);
                variableLocks.remove(variableName);
                log.info("Lokalny Cache: Usunięto zmienną '{}' z pamięci podręcznej.", variableName);
            } finally {
                lock.unlock();
            }
        } else {
            cacheStorage.remove(variableName);
        }
    }

    /**
     * Zwraca niemodyfikowalny widok całego lokalnego cache.
     * Wykorzystywane przez BullyElectionService podczas procedury rekonstrukcji katalogu,
     * kiedy nowy Lider prosi o zrzut lokalnej pamięci (Directory Manager Recovery).
     */
    public Map<String, String> getAll() {
        return Collections.unmodifiableMap(cacheStorage);
    }

    /**
     * Czyszczenie całego cache (np. podczas pełnego resetu węzła).
     */
    public void clear() {
        cacheStorage.clear();
        variableLocks.clear();
        log.info("Lokalny Cache został całkowicie wyczyszczony.");
    }
}