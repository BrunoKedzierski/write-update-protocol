package com.example.node.service;

import com.example.node.model.SystemNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * Serwis odpowiedzialny za cykliczne wysyłanie sygnałów życiowych (Heartbeat)
 * przez bezpołączeniowy protokół UDP do pozostałych węzłów w sieci.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class UdpHeartbeatSender {

    private final SystemNode systemNode;
    private final ObjectMapper objectMapper; // Do generowania ujednoliconego formatu JSON

    // Port UDP, na którym nasłuchują inne węzły (wstrzykiwany z application.properties)
    @Value("${udp.port:4444}")
    private int udpPort;

    // Adresy IP pozostałych maszyn zdefiniowane w pliku properties
    @Value("${peers.node1.ip:localhost}")
    private String node1Ip;

    @Value("${peers.node2.ip:localhost}")
    private String node2Ip;

    /**
     * Metoda uruchamiana automatycznie w osobnym wątku co 2000 milisekund (2 sekundy).
     * Wysyła asynchroniczne pakiety UDP w trybie "odpal i zapomnij" (fire-and-forget).
     */
    @Scheduled(fixedRate = 2000)
    public void sendHeartbeats() {
        // Pomijamy wysyłanie, jeśli węzeł jest w trakcie procedury wyborów (ELECTION)
        if ("ELECTION".equals(systemNode.getState())) {
            return;
        }

        try {
            // Zabezpieczenie kontraktu danych (Zarzut 6): Tworzymy zwięzły obiekt JSON
            ObjectNode heartbeatJson = objectMapper.createObjectNode();
            heartbeatJson.put("nodeId", systemNode.getNodeId());
            heartbeatJson.put("status", "ALIVE");
            heartbeatJson.put("timestamp", System.currentTimeMillis());

            String jsonPayload = heartbeatJson.toString();
            byte[] buffer = jsonPayload.getBytes();

            // Otwieramy gniazdo UDP (DatagramSocket automatycznie zamknie się dzięki try-with-resources)
            try (DatagramSocket socket = new DatagramSocket()) {

                // 1. Wysyłka do Węzła 1 (C# / Linux)
                if (systemNode.getNodeId() != 1) {
                    sendUdpPacket(socket, buffer, node1Ip);
                }

                // 2. Wysyłka do Węzła 2 (Python / macOS)
                if (systemNode.getNodeId() != 2) {
                    sendUdpPacket(socket, buffer, node2Ip);
                }
            }

        } catch (Exception e) {
            log.error("Krytyczny błąd podczas przygotowywania pakietu UDP Heartbeat: {}", e.getMessage());
        }
    }

    /**
     * Pomocnicza metoda wysyłająca surowy pakiet danych pod wskazany adres IP.
     */
    private void sendUdpPacket(DatagramSocket socket, byte[] buffer, String targetIp) {
        try {
            InetAddress address = InetAddress.getByName(targetIp);
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, address, udpPort);
            socket.send(packet);
            log.debug("Wysłano UDP Heartbeat do adresu {}:{}", targetIp, udpPort);
        } catch (Exception e) {
            // Logujemy jako debug/warn, ponieważ w systemie rozproszonym padnięty rówieśnik to norma, nie krytyczny błąd aplikacji
            log.warn("Nie udało się wysłać pakietu UDP do {} (Węzeł może być offline): {}", targetIp, e.getMessage());
        }
    }
}
