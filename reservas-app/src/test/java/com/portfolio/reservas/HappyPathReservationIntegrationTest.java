package com.portfolio.reservas;

import com.portfolio.reservas.controller.dto.ReservationRequestDto;
import com.portfolio.reservas.domain.Reservation;
import com.portfolio.reservas.domain.Seat;
import com.portfolio.reservas.domain.Show;
import com.portfolio.reservas.domain.enums.ReservationStatus;
import com.portfolio.reservas.domain.enums.SeatStatus;
import com.portfolio.reservas.repository.ReservationRepository;
import com.portfolio.reservas.repository.SeatRepository;
import com.portfolio.reservas.repository.ShowRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test de integración de la Fase 8, complementario al de concurrencia:
 * cubre el "camino feliz" sin contención — un único usuario reserva un
 * asiento y lo confirma, sin que nadie más compita por él. Verifica la
 * máquina de estados completa AVAILABLE -> HELD -> CONFIRMED de punta a
 * punta contra Kafka y Postgres reales (Testcontainers), no mocks.
 */
@AutoConfigureTestRestTemplate
class HappyPathReservationIntegrationTest extends AbstractIntegrationTest {

    private static final String USER_ID = "usuario-feliz";

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ShowRepository showRepository;

    @Autowired
    private SeatRepository seatRepository;

    @Autowired
    private ReservationRepository reservationRepository;

    private UUID showId;
    private UUID seatId;

    @BeforeEach
    void setUp() {
        Show show = showRepository.save(
                new Show("Función de prueba - camino feliz", "Sala 1", LocalDateTime.now().plusDays(1), 1));
        showId = show.getId();

        Seat seat = seatRepository.save(new Seat(showId, "A", 1));
        seatId = seat.getId();
    }

    @Test
    void unUsuarioSinContiendaPuedeReservarYConfirmarSuAsiento() throws InterruptedException {
        // Paso 1: pedir el hold (asíncrono vía Kafka)
        ReservationRequestDto request = new ReservationRequestDto(showId, seatId, USER_ID);
        ResponseEntity<String> holdResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/reservations", request, String.class);
        assertEquals(HttpStatus.ACCEPTED, holdResponse.getStatusCode());

        // Paso 2: esperar a que el consumer procese el evento y deje el asiento HELD
        Seat heldSeat = esperarHastaQueElAsientoDejeDeEstarAvailable();
        assertEquals(SeatStatus.HELD, heldSeat.getStatus(),
                "Sin contienda, el único usuario debería lograr el HELD sin problema");
        assertEquals(USER_ID, heldSeat.getHeldBy());

        List<Reservation> reservasHeld = reservationRepository.findBySeatIdAndStatus(seatId, ReservationStatus.HELD);
        assertEquals(1, reservasHeld.size());
        assertEquals(USER_ID, reservasHeld.get(0).getUserId());

        // Paso 3: confirmar la reserva (síncrono, no pasa por Kafka)
        ResponseEntity<String> confirmResponse = restTemplate.exchange(
                "http://localhost:" + port + "/api/reservations/confirm",
                HttpMethod.PUT,
                new HttpEntity<>(request),
                String.class);
        assertEquals(HttpStatus.OK, confirmResponse.getStatusCode());

        // Paso 4: verificar el estado final CONFIRMED, tanto en Seat como en Reservation
        Seat confirmedSeat = seatRepository.findById(seatId).orElseThrow();
        assertEquals(SeatStatus.CONFIRMED, confirmedSeat.getStatus());
        assertNull(confirmedSeat.getHeldUntil(), "confirm() debería limpiar heldUntil");

        List<Reservation> reservasConfirmed =
                reservationRepository.findBySeatIdAndStatus(seatId, ReservationStatus.CONFIRMED);
        assertEquals(1, reservasConfirmed.size());
        assertEquals(USER_ID, reservasConfirmed.get(0).getUserId());
    }

    private Seat esperarHastaQueElAsientoDejeDeEstarAvailable() throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(15);
        while (Instant.now().isBefore(deadline)) {
            Seat seat = seatRepository.findById(seatId).orElseThrow();
            if (seat.getStatus() != SeatStatus.AVAILABLE) {
                Thread.sleep(300);
                return seatRepository.findById(seatId).orElseThrow();
            }
            Thread.sleep(200);
        }
        fail("El asiento seatId=" + seatId + " siguió AVAILABLE después de 15s.");
        return null;
    }
}
