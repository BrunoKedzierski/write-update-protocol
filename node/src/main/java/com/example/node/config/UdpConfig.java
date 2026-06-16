package com.example.node.config;

import com.example.node.service.UdpHeartbeatReceiver;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.ip.udp.UnicastReceivingChannelAdapter;

/**
 * Klasa konfiguracyjna odpowiedzialna za uruchomienie serwera UDP.
 * Serwer ten nasłuchuje asynchronicznych sygnałów życiowych (Heartbeat)
 * wysyłanych przez pozostałe węzły w sieci P2P.
 */
@Configuration
public class UdpConfig {

    // Port UDP wstrzykiwany z pliku application.properties (np. udp.port=4444)
    @Value("${udp.port:4444}")
    private int udpPort;

    /**
     * Konfiguracja adaptera wejściowego UDP (Inbound Channel Adapter).
     * Adapter otwiera gniazdo (Socket) na określonym porcie i nasłuchuje pakietów.
     */
    @Bean
    public UnicastReceivingChannelAdapter udpInboundAdapter() {
        UnicastReceivingChannelAdapter adapter = new UnicastReceivingChannelAdapter(udpPort);
        // Opcjonalnie: ustawienie rozmiaru bufora (w bajtach) adekwatnie do małych pakietów Heartbeat
        adapter.setReceiveBufferSize(1024);
        return adapter;
    }

    /**
     * Definicja potoku przetwarzania (Integration Flow) dla odebranych pakietów UDP.
     * Pobiera surowy pakiet, konwertuje go na String i przekazuje do dedykowanego serwisu.
     *
     * @param udpInboundAdapter adapter odbierający pakiety UDP
     * @param heartbeatReceiver serwis przetwarzający logikę odebranego sygnału życiowego
     */
    @Bean
    public IntegrationFlow udpHeartbeatFlow(UnicastReceivingChannelAdapter udpInboundAdapter,
                                            UdpHeartbeatReceiver heartbeatReceiver) {
        return IntegrationFlow.from(udpInboundAdapter)
                // Konwersja payloadu z byte[] na String (wsparcie dla czytelnego formatu komunikatów)
                .transform(byte[].class, String::new)
                // Przekazanie przetworzonej wiadomości tekstowej do metody w serwisie odbiorczym
                .handle(String.class, (payload, headers) -> {
                    heartbeatReceiver.processHeartbeat(payload);
                    return null; // Strumień kończy się tutaj (fire-and-forget)
                })
                .get();
    }
}
