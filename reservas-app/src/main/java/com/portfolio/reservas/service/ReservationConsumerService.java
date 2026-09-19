package com.portfolio.reservas.service;

import com.portfolio.reservas.domain.ProcessedEvent;
import com.portfolio.reservas.domain.Reservation;
import com.portfolio.reservas.domain.Seat;
import com.portfolio.reservas.domain.enums.ReservationStatus;
import com.portfolio.reservas.domain.enums.SeatStatus;
import com.portfolio.reservas.event.SeatReservationRequestEvent;
import com.portfolio.reservas.repository.ProcessedEventRepository;
import com.portfolio.reservas.repository.ReservationRepository;
import com.portfolio.reservas.repository.SeatRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

@Service
public class ReservationConsumerService {

    private static final Logger log = LoggerFactory.getLogger(ReservationConsumerService.class);
    private static final int HOLD_SECONDS = 300; // 5 minutos de reserva provisoria

    private final SeatRepository seatRepository;
    private final ReservationRepository reservationRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final ReservationEventPublisher eventPublisher;

    public ReservationConsumerService(SeatRepository seatRepository,
                                       ReservationRepository reservationRepository,
                                       ProcessedEventRepository processedEventRepository,
                                       ReservationEventPublisher eventPublisher) {
        this.seatRepository = seatRepository;
        this.reservationRepository = reservationRepository;
        this.processedEventRepository = processedEventRepository;
        this.eventPublisher = eventPublisher;
    }

    @KafkaListener(topics = "seat-reservation-requests", groupId = "reservas-consumer-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void handleReservationRequest(SeatReservationRequestEvent event) {
        log.info("Procesando evento eventId={} showId={} seatId={} userId={}",
                event.eventId(), event.showId(), event.seatId(), event.userId());

        // Idempotencia: chequeo unificado contra processed_events, cubre los 3
        // caminos (HELD, REJECTED, y el que termina en el DLT) por igual —
        // ya no depende de que se haya persistido una Reservation.
        if (processedEventRepository.existsById(event.eventId())) {
            log.info("Evento eventId={} ya procesado anteriormente, se omite (idempotencia)", event.eventId());
            return;
        }

        Optional<Seat> seatOpt = seatRepository.findById(event.seatId());
        if (seatOpt.isEmpty()) {
            // seatId inexistente: no es un problema transitorio, es un dato inválido.
            // El registro de idempotencia para este camino NO se hace acá (ver
            // KafkaConsumerConfig.kafkaErrorHandler): si se guardara antes de cada
            // throw, el propio chequeo de existsById detendría los reintentos
            // internos del FixedBackOff, y el mensaje nunca llegaría a recuperarse
            // de verdad hacia el DLT (bug real encontrado y corregido en la Fase 5).
            // El recoverer se encarga de marcarlo como procesado una sola vez,
            // cuando el mensaje realmente se recupera tras agotar los reintentos.
            throw new IllegalArgumentException("Seat no encontrado: " + event.seatId());
        }

        Seat seat = seatOpt.get();

        // Regla agregada en la Fase 8 tras encontrar el bug: sin este chequeo,
        // nada impedia que un mismo usuario acumulara N asientos HELD
        // simultaneos para el mismo show (confirmado visualmente: la UI ni
        // siquiera avisaba, y el segundo hold en adelante quedaba huerfano
        // porque el frontend solo rastrea un asiento propio a la vez).
        List<Reservation> otrosHoldsDelUsuario = reservationRepository
                .findByShowIdAndUserIdAndStatus(event.showId(), event.userId(), ReservationStatus.HELD);
        boolean tieneOtroHoldActivo = otrosHoldsDelUsuario.stream()
                .anyMatch(r -> !r.getSeatId().equals(event.seatId())
                        && r.getExpiresAt() != null
                        && r.getExpiresAt().isAfter(LocalDateTime.now()));
        if (tieneOtroHoldActivo) {
            processedEventRepository.save(new ProcessedEvent(event.eventId()));
            eventPublisher.publish(event.showId(), event.seatId(), event.userId(),
                    "REJECTED", "Ya tenes otro asiento reservado para esta funcion. Confirmalo o espera a que venza antes de elegir otro.");
            return;
        }

        if (seat.getStatus() != SeatStatus.AVAILABLE) {
            processedEventRepository.save(new ProcessedEvent(event.eventId()));
            eventPublisher.publish(event.showId(), event.seatId(), event.userId(),
                    "REJECTED", "Asiento no disponible (estado actual: " + seat.getStatus() + ")");
            return;
        }

        try {
            LocalDateTime heldUntil = LocalDateTime.ofInstant(
                    event.timestamp().plusSeconds(HOLD_SECONDS), ZoneOffset.UTC);
            seat.hold(event.userId(), heldUntil);
            seatRepository.save(seat);

            Reservation reservation = new Reservation(event.showId(), event.seatId(), event.userId());
            reservation.setEventId(event.eventId());
            reservation.setStatus(com.portfolio.reservas.domain.enums.ReservationStatus.HELD);
            reservation.setExpiresAt(heldUntil);
            reservationRepository.save(reservation);
            processedEventRepository.save(new ProcessedEvent(event.eventId()));

            log.info("Asiento seatId={} HELD por userId={} hasta {}", event.seatId(), event.userId(), heldUntil);
            eventPublisher.publish(event.showId(), event.seatId(), event.userId(), "HELD", null);

        } catch (OptimisticLockingFailureException e) {
            // Otro consumer/request ganó la carrera por este asiento entre el chequeo
            // de disponibilidad y el save(). El particionamiento por showId hace esto
            // raro (mismo showId siempre va a la misma partición/consumer), pero puede
            // pasar si hay otro origen de escritura sobre el mismo Seat.
            processedEventRepository.save(new ProcessedEvent(event.eventId()));
            log.warn("Conflicto de optimistic locking en seatId={}, asiento tomado por otro proceso", event.seatId());
            eventPublisher.publish(event.showId(), event.seatId(), event.userId(),
                    "REJECTED", "Conflicto de concurrencia: asiento tomado por otro proceso");
        }
    }
}
