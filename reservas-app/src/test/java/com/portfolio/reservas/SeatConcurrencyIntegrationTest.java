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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test de integración de la Fase 8: automatiza lo que hasta ahora se hacía
 * a mano con `curl & / wait` para verificar la condición de carrera al
 * reservar un asiento. Dispara N requests HTTP concurrentes de distintos
 * usuarios pidiendo el MISMO asiento del MISMO show, y verifica que
 * exactamente uno gane (quede HELD) y el resto pierda.
 *
 * Nota de diseño: como ReservationProducerService particiona el evento de
 * Kafka por showId (key = showId.toString()), todas las requests de este
 * test caen en la misma partición y son procesadas en orden por un único
 * consumer thread. Esto ya evita la mayoría de las condiciones de carrera
 * reales a nivel de fila en Postgres; el optimistic locking (@Version en
 * Seat) queda como defensa en profundidad para el caso límite de otro
 * origen de escritura concurrente sobre el mismo asiento. Este test valida
 * el comportamiento observable end-to-end (HTTP -> Kafka -> consumer ->
 * Postgres) tal como lo vería un usuario real, no la lógica en aislamiento.
 */
@AutoConfigureTestRestTemplate
class SeatConcurrencyIntegrationTest extends AbstractIntegrationTest {

    private static final int CONCURRENT_USERS = 10;

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
                new Show("Función de prueba - Fase 8", "Sala 1", LocalDateTime.now().plusDays(1), 1));
        showId = show.getId();

        Seat seat = seatRepository.save(new Seat(showId, "A", 1));
        seatId = seat.getId();
    }

    @Test
    void soloUnoDeVariosUsuariosConcurrentesLograReservarElMismoAsiento() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_USERS);
        CountDownLatch readyLatch = new CountDownLatch(CONCURRENT_USERS);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(CONCURRENT_USERS);
        List<ResponseEntity<String>> responses = new ArrayList<>();

        for (int i = 0; i < CONCURRENT_USERS; i++) {
            String userId = "usuario-" + i;
            executor.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();
                    ReservationRequestDto request = new ReservationRequestDto(showId, seatId, userId);
                    ResponseEntity<String> response = restTemplate.postForEntity(
                            "http://localhost:" + port + "/api/reservations", request, String.class);
                    synchronized (responses) {
                        responses.add(response);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await(5, TimeUnit.SECONDS);
        startLatch.countDown();
        boolean allRequestsCompleted = doneLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertTrue(allRequestsCompleted, "Todas las requests HTTP deberían completar en 10s");
        assertEquals(CONCURRENT_USERS, responses.size());
        responses.forEach(r -> assertEquals(HttpStatus.ACCEPTED, r.getStatusCode(),
                "Cada POST /api/reservations solo encola en Kafka, siempre debería responder 202"));

        Seat finalSeat = esperarHastaQueElAsientoDejeDeEstarAvailable();

        assertEquals(SeatStatus.HELD, finalSeat.getStatus(),
                "Exactamente un usuario debería haber logrado el HELD");
        assertNotNull(finalSeat.getHeldBy());

        List<Reservation> reservasHeld = reservationRepository.findBySeatIdAndStatus(seatId, ReservationStatus.HELD);
        assertEquals(1, reservasHeld.size(),
                "Debería existir exactamente una Reservation en estado HELD para este asiento");
        assertEquals(finalSeat.getHeldBy(), reservasHeld.get(0).getUserId(),
                "El userId de la Reservation ganadora debe coincidir con heldBy del asiento");
    }

    private Seat esperarHastaQueElAsientoDejeDeEstarAvailable() throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(15);
        while (Instant.now().isBefore(deadline)) {
            Seat seat = seatRepository.findById(seatId).orElseThrow();
            if (seat.getStatus() != SeatStatus.AVAILABLE) {
                Thread.sleep(500);
                return seatRepository.findById(seatId).orElseThrow();
            }
            Thread.sleep(200);
        }
        fail("El asiento seatId=" + seatId + " siguió AVAILABLE después de 15s: el consumer de Kafka "
                + "no procesó ninguna de las " + CONCURRENT_USERS + " requests a tiempo.");
        return null;
    }
}
