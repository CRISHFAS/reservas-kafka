package com.portfolio.reservas.service;
import com.portfolio.reservas.domain.Reservation;
import com.portfolio.reservas.domain.Seat;
import com.portfolio.reservas.domain.enums.ReservationStatus;
import com.portfolio.reservas.domain.enums.SeatStatus;
import com.portfolio.reservas.repository.ReservationRepository;
import com.portfolio.reservas.repository.SeatRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.util.List;
/**
 * Libera periódicamente los asientos que quedaron en HELD más allá de su
 * heldUntil sin haber sido confirmados. Usa fixedDelay (no fixedRate): así
 * cada ciclo espera a que termine el anterior antes de programar el
 * siguiente, evitando que dos ejecuciones se pisen entre sí sobre los mismos
 * datos si el procesamiento de un ciclo tardara más de lo esperado (ver
 * discusión de diseño en el handoff de la Fase 6).
 *
 * A diferencia del consumer de Kafka, esto no corre en un contexto de
 * redelivery (un mismo Seat vencido no puede "reprocesarse" dos veces salvo
 * que corran 2+ instancias del backend en simultáneo, que no es el caso
 * actual del proyecto). Por eso no se registra marcador en processed_events
 * para este camino; el optimistic locking (@Version) en Seat ya protegería
 * ese escenario si llegara a darse.
 *
 * findBySeatIdAndStatus devuelve una LISTA (no un Optional/único resultado):
 * no hay ninguna restricción de base de datos que garantice como máximo una
 * Reservation en HELD por Seat, y en la práctica llegaron a acumularse
 * duplicados huérfanos (ej. por intervenciones manuales directas sobre la
 * tabla seats durante pruebas, sin pasar por el flujo normal). Iterar y
 * expirar TODAS las que encuentre evita que un dato inconsistente rompa el
 * scheduler completo con NonUniqueResultException (bug real encontrado y
 * corregido en la Fase 7).
 */
@Component
public class SeatExpirationScheduler {
    private static final Logger log = LoggerFactory.getLogger(SeatExpirationScheduler.class);
    private final SeatRepository seatRepository;
    private final ReservationRepository reservationRepository;
    private final ReservationEventPublisher eventPublisher;
    public SeatExpirationScheduler(SeatRepository seatRepository,
                                    ReservationRepository reservationRepository,
                                    ReservationEventPublisher eventPublisher) {
        this.seatRepository = seatRepository;
        this.reservationRepository = reservationRepository;
        this.eventPublisher = eventPublisher;
    }
    @Scheduled(fixedDelay = 30000)
    public void releaseExpiredHolds() {
        List<Seat> expiredSeats = seatRepository.findByStatusAndHeldUntilBefore(
                SeatStatus.HELD, LocalDateTime.now());
        if (expiredSeats.isEmpty()) {
            return;
        }
        log.info("Encontrados {} asiento(s) HELD vencido(s), liberando...", expiredSeats.size());
        for (Seat seat : expiredSeats) {
            String userId = seat.getHeldBy();
            seat.release();
            seatRepository.save(seat);
            List<Reservation> heldReservations = reservationRepository.findBySeatIdAndStatus(
                    seat.getId(), ReservationStatus.HELD);
            if (heldReservations.size() > 1) {
                log.warn("Se encontraron {} reservas HELD para seatId={} (se esperaba 0 o 1); "
                                + "se expiran todas para no dejar datos huérfanos.",
                        heldReservations.size(), seat.getId());
            }
            for (Reservation reservation : heldReservations) {
                reservation.setStatus(ReservationStatus.EXPIRED);
                reservationRepository.save(reservation);
            }
            log.info("Asiento seatId={} liberado por expiración (estaba HELD por userId={})",
                    seat.getId(), userId);
            eventPublisher.publish(seat.getShowId(), seat.getId(), userId,
                    "EXPIRED", "Reserva expirada: no se confirmó dentro del tiempo límite");
        }
    }
}
