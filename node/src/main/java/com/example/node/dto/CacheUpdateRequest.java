package com.example.node.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data Transfer Object (DTO) reprezentujący strukturę wiadomości aktualizacji cache.
 * Klasa mapuje się automatycznie na format JSON dla zachowania kontraktu heterogenicznego.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CacheUpdateRequest {
    private int senderNodeId;       // Kto wysłał żądanie aktualizacji
    private String variableName;    // Identyfikator zmiennej (klucz)
    private String newValue;        // Nowa wartość przekazywana w sieci
    private long timestamp;         // Znacznik czasu zapobiegający konfliktom chronologicznym
}