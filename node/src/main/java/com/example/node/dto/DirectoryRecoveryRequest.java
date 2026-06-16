package com.example.node.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data Transfer Object (DTO) używany podczas procedury rekonstrukcji katalogu globalnego.
 * * Klasa ta definiuje strukturę zapytania wysyłanego przez nowo wybranego Lidera
 * (lub węzeł w stanie Recovery) do pozostałych węzłów (C# i Python) w celu
 * zebrania informacji o stanie ich lokalnych pamięci podręcznych (Local Cache).
 * * Bezpośrednio rozwiązuje zarzut nr 1 i 5 prowadzącego dotyczące "Pustego Katalogu".
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DirectoryRecoveryRequest {

    /**
     * Identyfikator nowego lidera, który żąda zrzutu danych (np. 3 dla Javy).
     * Pozwala węzłowi odbierającemu zweryfikować, czy żądanie pochodzi od
     * uprawnionego koordynatora sieci.
     */
    private int requesterNodeId;

    /**
     * Unikalny token lub identyfikator sesji odzyskiwania stanu.
     * Pomaga w synchronizacji asynchronicznych komunikatów i zapobiega
     * ponownemu przetwarzaniu przestarzałych żądań rekonstrukcji.
     */
    private String recoverySessionId;

    /**
     * Znacznik czasu (timestamp) utworzenia żądania.
     * Służy do weryfikacji aktualności żądania w środowisku rozproszonym.
     */
    private long timestamp;
}
