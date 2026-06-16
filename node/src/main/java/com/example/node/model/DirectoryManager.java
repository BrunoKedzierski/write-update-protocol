package com.example.node.model;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Komponent zarządzający globalnym katalogiem spójności (Directory Manager).
 * Klasa ta jest w pełni operacyjna wyłącznie na aktywnym Liderze (Home Node).
 * * Odpowiada za:
 * 1. presenceList - śledzenie, które węzły posiadają kopię danej zmiennej.
 * 2. mainMemory  - przechowywanie nadrzędnych, najbardziej aktualnych wartości zmiennych.
 */
@Slf4j
@Component
public class DirectoryManager {

    // Globalna mapa obecności: [Nazwa_Zmiennej -> Set z ID węzłów, które ją posiadają]
    private final Map<String, Set<Integer>> presenceList = new ConcurrentHashMap<>();

    // Główna pamięć wirtualna klastra: [Nazwa_Zmiennej -> Aktualna_Wartość]
    private final Map<String, String> mainMemory = new ConcurrentHashMap<>();

    // Blokada Read/Write chroniąca całościową spójność struktur przed wyścigami (Race Conditions)
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    /**
     * Rejestruje informację, że dany węzeł posiada kopię określonej zmiennej w swoim cache.
     * Metoda kluczowa dla odtwarzania stanu katalogu po awarii poprzedniego lidera.
     */
    public void registerVariablePresence(String variableName, int nodeId) {
        rwLock.writeLock().lock();
        try {
            presenceList.computeIfAbsent(variableName, k -> new HashSet<>()).add(nodeId);
            log.debug("Katalog: Zarejestrowano obecność zmiennej '{}' na Węźle {}.", variableName, nodeId);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Aktualizuje wartość w pamięci głównej klastra (Main Memory).
     */
    public void updateMainMemoryValue(String variableName, String value) {
        rwLock.writeLock().lock();
        try {
            mainMemory.put(variableName, value);
            log.debug("Katalog: Zaktualizowano pamięć główną dla '{}' = {}.", variableName, value);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Pobiera zestaw identyfikatorów węzłów, które posiadają kopię danej zmiennej.
     * Wykorzystywane przez Lidera do określenia adresatów wiadomości typu Write-Update (Broadcast).
     *
     * @return Set zawierający ID węzłów lub pusty zestaw, jeśli nikt nie ma zmiennej.
     */
    public Set<Integer> getOwnersOf(String variableName) {
        rwLock.readLock().lock();
        try {
            Set<Integer> owners = presenceList.get(variableName);
            return owners != null ? new HashSet<>(owners) : new HashSet<>();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Pobiera aktualną wartość zmiennej zapisaną w pamięci głównej klastra.
     */
    public String getValueFromMainMemory(String variableName) {
        rwLock.readLock().lock();
        try {
            return mainMemory.get(variableName);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Czyści całkowicie stan katalogu głównego.
     * Metoda wywoływana obowiązkowo przez BullyElectionService przed przystąpieniem
     * do odpytywania rówieśników w celu eliminacji problemu "Pustego Katalogu".
     */
    public void clearDirectory() {
        rwLock.writeLock().lock();
        try {
            presenceList.clear();
            mainMemory.clear();
            log.info("Katalog globalny został zresetowany i wyczyszczony w celu rekonstrukcji stanu.");
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Usuwa węzeł z listy obecności dla wszystkich zmiennych.
     * Przydatne, gdy wykryjemy trwały timeout (awarię) jakiegoś followera.
     */
    public void removeNodeFromPresence(int failedNodeId) {
        rwLock.writeLock().lock();
        try {
            presenceList.forEach((varName, nodesSet) -> {
                if (nodesSet.remove(failedNodeId)) {
                    log.info("Katalog: Usunięto zmarły Węzeł {} z listy współdzielenia zmiennej '{}'.", failedNodeId, varName);
                }
            });
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}