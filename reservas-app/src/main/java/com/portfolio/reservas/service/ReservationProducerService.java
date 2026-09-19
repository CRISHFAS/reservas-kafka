package com.portfolio.reservas.service;

import com.portfolio.reservas.config.KafkaTopicConfig;
import com.portfolio.reservas.event.SeatReservationRequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class ReservationProducerService {

    private static final Logger log = LoggerFactory.getLogger(ReservationProducerService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public ReservationProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void requestSeatHold(UUID showId, UUID seatId, String userId) {
        SeatReservationRequestEvent event = SeatReservationRequestEvent.of(showId, seatId, userId);

        String key = showId.toString();

        log.info("Publicando solicitud de reserva: eventId={}, showId={}, seatId={}, userId={}",
                event.eventId(), showId, seatId, userId);

        kafkaTemplate.send(KafkaTopicConfig.REQUESTS_TOPIC, key, event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Error publicando evento de reserva", ex);
                    } else {
                        log.info("Evento publicado en partición {} offset {}",
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
