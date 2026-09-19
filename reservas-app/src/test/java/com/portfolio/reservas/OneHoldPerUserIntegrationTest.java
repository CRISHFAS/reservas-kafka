package com.portfolio.reservas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.portfolio.reservas.controller.dto.ReservationRequestDto;
import com.portfolio.reservas.domain.Reservation;
import com.portfolio.reservas.domain.Seat;
import com.portfolio.reservas.domain.Show;
import com.portfolio.reservas.domain.enums.ReservationStatus;
import com.portfolio.reservas.domain.enums.SeatStatus;
import com.portfolio.reservas.repository.ReservationRepository;
import com.portfolio.reservas.repository.SeatRepository;
import com.portfolio.reservas.repository.ShowRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test de integración de la Fase 8 (pendiente cerrado al final de la fase):
 * verifica la regla agregada tras el bug real encontrado navegando la app
 * manualmente — un mismo usuario no puede tener más de un asiento HELD
 * activo para el mismo show. Ver ReservationConsumerService y el handoff
 * para el contexto completo del hallazgo.
 *
 * A diferencia de SeatConcurrencyIntegrationTest (que verifica la pelea
 * entre USUARIOS DISTINTOS por el MISMO asiento), este test verifica la
 * restricción sobre el MISMO usuario pidiendo DOS asientos DISTINTOS del
 * mismo show, en secuencia (no hay condición de carrera real acá, es una
 * regla de negocio determinística).
 */
@AutoConfigureTestRestTemplate
class OneHoldPerUserIntegrationTest extends AbstractIntegrationTest {

    private static final String EVENTS_TOPIC = "seat-reservation-events";
    private static final String USER_ID = "usuario-un-solo-hold";

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
    private UUID primerAsientoId;
    private UUID segundoAsientoId;

    @BeforeEach
    void setUp() {
        Show show = showRepository.save(
                new Show("Función de prueba - un hold por usuario", "Sala 1", LocalDateTime.now().plusDays(1), 2));
        showId = show.getId();

        primerAsientoId = seatRepository.save(new Seat(showId, "A", 1)).getId();
        segundoAsientoId = seatRepository.save(new Seat(showId, "A", 2)).getId();
    }

    @Test
    void unUsuarioNoPuedeTenerDosAsientosHeldSimultaneosEnElMismoShow() throws InterruptedException {
        pedirAsiento(primerAsientoId);
        Seat primerAsiento = esperarHastaQueElAsientoDejeDeEstarAvailable(primerAsientoId);
        assertEquals(SeatStatus.HELD, primerAsiento.getStatus());
        assertEquals(USER_ID, primerAsiento.getHeldBy());

        pedirAsiento(segundoAsientoId);
        JsonNode eventoRechazo = leerResultadoDelSegundoAsiento();

        assertEquals("REJECTED", eventoRechazo.get("result").asText());
        assertTrue(eventoRechazo.get("reason").asText().contains("Ya tenes otro asiento reservado"),
                "El motivo del rechazo debería mencionar que ya tiene otro asiento reservado, fue: "
                        + eventoRechazo.get("reason").asText());

        Seat segundoAsiento = seatRepository.findById(segundoAsientoId).orElseThrow();
        assertEquals(SeatStatus.AVAILABLE, segundoAsiento.getStatus());

        List<Reservation> reservasSegundoAsiento =
                reservationRepository.findBySeatIdAndStatus(segundoAsientoId, ReservationStatus.HELD);
        assertTrue(reservasSegundoAsiento.isEmpty(),
                "No debería existir ninguna Reservation HELD para el segundo asiento");

        Seat primerAsientoFinal = seatRepository.findById(primerAsientoId).orElseThrow();
        assertEquals(SeatStatus.HELD, primerAsientoFinal.getStatus());
        assertEquals(USER_ID, primerAsientoFinal.getHeldBy());
    }

    private void pedirAsiento(UUID seatId) {
        ReservationRequestDto request = new ReservationRequestDto(showId, seatId, USER_ID);
        ResponseEntity<String> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/reservations", request, String.class);
        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
    }

    private Seat esperarHastaQueElAsientoDejeDeEstarAvailable(UUID seatId) throws InterruptedException {
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

    private JsonNode leerResultadoDelSegundoAsiento() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "one-hold-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        ObjectMapper mapper = new ObjectMapper();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(EVENTS_TOPIC));

            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JsonNode node = mapper.readTree(record.value());
                        if (node.has("seatId") && segundoAsientoId.toString().equals(node.get("seatId").asText())) {
                            return node;
                        }
                    } catch (Exception ignored) {
                        // mensaje no parseable como el JSON esperado, se ignora
                    }
                }
            }
        }

        fail("No se encontró en " + EVENTS_TOPIC + " ningún resultado para seatId=" + segundoAsientoId + " después de 15s");
        return null; // inalcanzable, fail() lanza AssertionError
    }
}
