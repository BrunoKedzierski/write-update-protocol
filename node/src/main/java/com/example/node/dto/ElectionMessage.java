package com.example.node.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data Transfer Object (DTO) reprezentujący komunikat w algorytmie Bully.
 * Klasa definiuje ujednolicony kontrakt wymiany danych JSON pomiędzy
 * środowiskami Java (Windows), C# (Linux) oraz Python (macOS).
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ElectionMessage {

    /**
     * Identyfikator węzła, który nadaje dany komunikat (np. 1, 2 lub 3).
     * W heterogenicznym środowisku przesyłany jako standardowy prymityw int (32-bit),
     * co zapobiega problemom z interpretacją rozmiaru typów numerycznych.
     */
    private int senderNodeId;

    /**
     * Typ komunikatu algorytmu Bully.
     * Dozwolone i oczekiwane wartości zgodne z protokołem to:
     * - "ELECTION"    : Inicjalizacja wyborów (wysyłana do węzłów o wyższym ID).
     * - "ANSWER"      : Odpowiedź "OK" od wyższego węzła (przerywa ogłaszanie się liderem przez nadawcę).
     * - "COORDINATOR" : Ogłoszenie zwycięstwa i objęcie roli Lidera przez nadawcę.
     */
    private String type;
}
