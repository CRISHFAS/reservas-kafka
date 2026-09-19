package com.portfolio.reservas.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws")
                // "*" ahora para test-ws.html en el mismo origen y para cuando
                // en la Fase 7 el frontend React corra en un puerto distinto (3000).
                // Restringir a orígenes específicos si esto pasa a producción real.
                .setAllowedOriginPatterns("*")
                .withSockJS();
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        // Broker simple en memoria: alcanza para esta demo/portfolio, pero no
        // escala con múltiples instancias del backend (cada una tendría su
        // propio set de sockets conectados). Para eso se necesitaría un broker
        // externo tipo RabbitMQ con soporte STOMP.
        registry.enableSimpleBroker("/topic");
        registry.setApplicationDestinationPrefixes("/app");
    }
}
