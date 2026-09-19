package com.portfolio.reservas.service;

import com.portfolio.reservas.domain.Reservation;
import com.portfolio.reservas.domain.Seat;
import com.portfolio.reservas.domain.enums.ReservationStatus;
import com.portfolio.reservas.domain.enums.SeatStatus;
import com.portfolio.reservas.repository.ReservationRepository;
import com.portfolio.reservas.repository.SeatRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Confirma una reserva HELD, completando la máquina de estados que estaba
 * prevista desde el diseño original (AVAILABLE -> HELD -> CONFIRMED) pero
 * nunca se había implementado hasta la Fase 7 (mejoras de UX). A diferencia
 * de la solicitud inicial de hold (que pasa por Kafka porque varias personas
 * pueden competir por el mismo asiento), la confirmación es una operación
 * síncrona y directa: solo la persona que ya tiene el hold puede confirmarlo,
 * no hay contienda real con otros usuarios por esta acción puntual — el
 * único conflicto posible es contra el propio SeatExpirationScheduler si el
 * hold vence justo en el medio, cubierto abajo con el chequeo de heldUntil
 * y el manejo de OptimisticLockingFailureException.
 */
@Service
public class ReservationConfirmationService {

    private static final Logger log = LoggerFactory.getLogger(ReservationConfirmationService.class);

    private final SeatRepository seatRepository;
    private final ReservationRepository reservationRepository;
    private final ReservationEventPublisher eventPublisher;

    public ReservationConfirmationService(SeatRepository seatRepository,
                                           ReservationRepository reservationRepository,
                                           ReservationEventPublisher eventPublisher) {
        this.seatRepository = seatRepository;
        this.reservationRepository = reservationRepository;
        this.eventPublisher = eventPublisher;
    }

    public ConfirmationResult confirm(UUID showId, UUID seatId, String userId) {
        Optional<Seat> seatOpt = seatRepository.findById(seatId);
        if (seatOpt.isEmpty()) {
            return ConfirmationResult.notFound("Asiento no encontrado.");
        }

        Seat seat = seatOpt.get();

        if (seat.getStatus() != SeatStatus.HELD) {
            return ConfirmationResult.conflict(
                    "Este asiento ya no está a la espera de confirmación (estado actual: " + seat.getStatus() + ").");
        }

        if (!userId.equals(seat.getHeldBy())) {
            return ConfirmationResult.conflict("Este asiento está reservado por otra persona.");
        }

        if (seat.getHeldUntil() != null && seat.getHeldUntil().isBefore(LocalDateTime.now())) {
            return ConfirmationResult.conflict("El tiempo para confirmar esta reserva ya venció.");
        }

        try {
            seat.confirm();
            seatRepository.save(seat);
        } catch (OptimisticLockingFailureException e) {
            log.warn("Conflicto de optimistic locking al confirmar seatId={}: probablemente expiró justo ahora", seatId);
            return ConfirmationResult.conflict("El tiempo para confirmar esta reserva venció justo ahora. Probá reservar de nuevo.");
        }

        List<Reservation> heldReservations = reservationRepository.findBySeatIdAndStatus(seatId, ReservationStatus.HELD);
        for (Reservation reservation : heldReservations) {
            reservation.setStatus(ReservationStatus.CONFIRMED);
            reservationRepository.save(reservation);
        }

        log.info("Asiento seatId={} CONFIRMADO por userId={}", seatId, userId);
        eventPublisher.publish(showId, seatId, userId, "CONFIRMED", null);

        return ConfirmationResult.ok("¡Reserva confirmada! Que disfrutes la función.");
    }
}
