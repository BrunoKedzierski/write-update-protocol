package com.example.node.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.util.concurrent.TimeUnit;

/**
 * Klasa konfiguracyjna dla WebClienta.
 * Tworzy fabrykę budującą (WebClient.Builder), która pozwala na generowanie
 * asynchronicznych i nieblokujących żądań HTTP REST do innych węzłów.
 */
@Configuration
public class WebClientConfig {

    /**
     * Rejestruje Bean dostarczający WebClient.Builder z rygorystycznie
     * skonfigurowanymi limitami czasu (Timeouts).
     * * Wykorzystujemy wzorzec Buildera, ponieważ adresy bazowe (Base URL)
     * będą dynamicznie podstawiane w zależności od tego, do którego węzła
     * (C# czy Python) wysyłamy w danym momencie zapytanie.
     */
    @Bean
    public WebClient.Builder webClientBuilder() {
        // Konfiguracja niskopoziomowego klienta HttpClient z biblioteki Reactor Netty
        HttpClient httpClient = HttpClient.create()
                // Maksymalny czas na ustanowienie fizycznego połączenia TCP (1 sekunda)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
                // Konfiguracja czasu reakcji na poziomie potoku sieciowego (Pipeline)
                .doOnConnected(conn -> conn
                        // Maksymalny czas oczekiwania na nadejście kolejnego pakietu danych z sieci (1 sekunda)
                        .addHandlerLast(new ReadTimeoutHandler(1000, TimeUnit.MILLISECONDS))
                        // Maksymalny czas na wysłanie danych przez nasze gniazdo sieciowe (1 sekunda)
                        .addHandlerLast(new WriteTimeoutHandler(1000, TimeUnit.MILLISECONDS)));

        // Zwracamy skonfigurowany builder, opakowany w konektor Reactor Netty
        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient));
    }
}