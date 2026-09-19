package com.portfolio.reservas.service;

import com.portfolio.reservas.event.SeatReservationResultEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Publica el resultado de un intento de reserva (HELD, REJECTED, EXPIRED, ...)
 * tanto a Kafka (fuente de verdad del historial de eventos) como al WebSocket
 * (notificación en vivo al frontend). Extraído en la Fase 6 desde
 * ReservationConsumerService para que el nuevo SeatExpirationScheduler pueda
 * reutilizar exactamente la misma lógica de publicación sin duplicarla.
 */
@Component
public class ReservationEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(ReservationEventPublisher.class);
    private static final String RESULTS_TOPIC = "seat-reservation-events";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final SimpMessagingTemplate messagingTemplate;

    public ReservationEventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                                      SimpMessagingTemplate messagingTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.messagingTemplate = messagingTemplate;
    }

    public void publish(UUID showId, UUID seatId, String userId, String result, String reason) {
        SeatReservationResultEvent resultEvent = SeatReservationResultEvent.of(
                showId, seatId, userId, result, reason);

        kafkaTemplate.send(RESULTS_TOPIC, showId.toString(), resultEvent)
                .whenComplete((sendResult, ex) -> {
                    if (ex != null) {
                        log.error("Error publicando resultado showId={} seatId={} result={}",
                                showId, seatId, result, ex);
                    } else {
                        log.info("Resultado publicado: showId={} seatId={} result={} partition={} offset={}",
                                showId, seatId, result,
                                sendResult.getRecordMetadata().partition(),
                                sendResult.getRecordMetadata().offset());
                    }
                });

        String destination = "/topic/shows/" + showId;
        messagingTemplate.convertAndSend(destination, resultEvent);
        log.info("Resultado enviado por WebSocket a destination={}", destination);
    }
}
