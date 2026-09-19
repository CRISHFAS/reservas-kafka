package com.portfolio.reservas;

import com.portfolio.reservas.controller.dto.ReservationRequestDto;
import com.portfolio.reservas.domain.Seat;
import com.portfolio.reservas.domain.Show;
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

import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test de integración de la Fase 8 (pendiente cerrado al final de la fase):
 * cubre las 3 ramas de conflicto de ReservationConfirmationService.confirm()
 * que NO se habían probado todavía (solo el camino exitoso, en
 * HappyPathReservationIntegrationTest): asiento inexistente, estado
 * incorrecto, usuario incorrecto, y hold ya vencido.
 *
 * A diferencia de los tests de hold (que pasan por Kafka, asíncronos),
 * confirm() es una operación síncrona vía PUT /api/reservations/confirm —
 * no hace falta esperar nada ni leer tópicos, el resultado viene directo
 * en la respuesta HTTP.
 */
@AutoConfigureTestRestTemplate
class ReservationConfirmationConflictsIntegrationTest extends AbstractIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ShowRepository showRepository;

    @Autowired
    private SeatRepository seatRepository;

    private UUID showId;

    @BeforeEach
    void setUp() {
        Show show = showRepository.save(
                new Show("Función de prueba - conflictos de confirm", "Sala 1", LocalDateTime.now().plusDays(1), 4));
        showId = show.getId();
    }

    @Test
    void confirmarUnAsientoInexistenteDevuelveNotFound() {
        UUID seatIdInexistente = UUID.randomUUID();
        ResponseEntity<String> response = confirmar(seatIdInexistente, "cualquier-usuario");

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertTrue(response.getBody().toLowerCase().contains("no encontrado"),
                "El mensaje debería indicar que el asiento no existe, fue: " + response.getBody());
    }

    @Test
    void confirmarUnAsientoQueNoEstaHeldDevuelveConflict() {
        Seat asientoDisponible = seatRepository.save(new Seat(showId, "A", 1));

        ResponseEntity<String> response = confirmar(asientoDisponible.getId(), "cualquier-usuario");

        assertEquals(HttpStatus.CONFLICT, response.getStatusCode());
        assertTrue(response.getBody().contains("no está a la espera de confirmación"),
                "El mensaje debería indicar que el asiento no está a la espera de confirmación, fue: "
                        + response.getBody());
    }

    @Test
    void confirmarConUnUsuarioDistintoAlQueTieneElHoldDevuelveConflict() {
        Seat asiento = new Seat(showId, "A", 2);
        asiento.hold("usuario-dueño", LocalDateTime.now().plusMinutes(5));
        asiento = seatRepository.save(asiento);

        ResponseEntity<String> response = confirmar(asiento.getId(), "usuario-impostor");

        assertEquals(HttpStatus.CONFLICT, response.getStatusCode());
        assertTrue(response.getBody().contains("reservado por otra persona"),
                "El mensaje debería indicar que está reservado por otra persona, fue: " + response.getBody());
    }

    @Test
    void confirmarUnHoldYaVencidoDevuelveConflict() {
        Seat asiento = new Seat(showId, "A", 3);
        asiento.hold("usuario-lento", LocalDateTime.now().minusMinutes(1));
        asiento = seatRepository.save(asiento);

        ResponseEntity<String> response = confirmar(asiento.getId(), "usuario-lento");

        assertEquals(HttpStatus.CONFLICT, response.getStatusCode());
        assertTrue(response.getBody().toLowerCase().contains("venció"),
                "El mensaje debería indicar que el tiempo para confirmar venció, fue: " + response.getBody());
    }

    private ResponseEntity<String> confirmar(UUID seatId, String userId) {
        ReservationRequestDto request = new ReservationRequestDto(showId, seatId, userId);
        return restTemplate.exchange(
                "http://localhost:" + port + "/api/reservations/confirm",
                HttpMethod.PUT,
                new HttpEntity<>(request),
                String.class);
    }
}
