package com.portfolio.reservas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.portfolio.reservas.domain.Reservation;
import com.portfolio.reservas.domain.Seat;
import com.portfolio.reservas.domain.Show;
import com.portfolio.reservas.domain.enums.ReservationStatus;
import com.portfolio.reservas.domain.enums.SeatStatus;
import com.portfolio.reservas.repository.ReservationRepository;
import com.portfolio.reservas.repository.SeatRepository;
import com.portfolio.reservas.repository.ShowRepository;
import com.portfolio.reservas.service.SeatExpirationScheduler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test de integración de la Fase 8 (pendiente cerrado al final de la fase):
 * verifica SeatExpirationScheduler, el job que libera asientos HELD cuyo
 * heldUntil ya venció sin confirmarse.
 *
 * En vez de esperar los 30s reales del fixedDelay, se invoca el método del
 * scheduler directamente (bean real, autowireado) — sigue siendo una
 * prueba de integración legítima porque toca Postgres y Kafka reales, solo
 * que no dependemos del reloj del @Scheduled para disparar el ciclo.
 *
 * Se seedean dos asientos a propósito: uno con heldUntil en el PASADO (debe
 * liberarse) y uno con heldUntil en el FUTURO (debe quedar intacto), para
 * confirmar que el scheduler solo toca lo que realmente venció.
 */
class SeatExpirationSchedulerIntegrationTest extends AbstractIntegrationTest {

    private static final String EVENTS_TOPIC = "seat-reservation-events";

    @Autowired
    private SeatExpirationScheduler scheduler;

    @Autowired
    private ShowRepository showRepository;

    @Autowired
    private SeatRepository seatRepository;

    @Autowired
    private ReservationRepository reservationRepository;

    private UUID showId;
    private UUID asientoVencidoId;
    private UUID asientoVigenteId;

    @BeforeEach
    void setUp() {
        Show show = showRepository.save(
                new Show("Función de prueba - expiración", "Sala 1", LocalDateTime.now().plusDays(1), 2));
        showId = show.getId();

        Seat asientoVencido = new Seat(showId, "A", 1);
        LocalDateTime heldUntilVencido = LocalDateTime.now().minusMinutes(1);
        asientoVencido.hold("usuario-vencido", heldUntilVencido);
        asientoVencido = seatRepository.save(asientoVencido);
        asientoVencidoId = asientoVencido.getId();

        Reservation reservaVencida = new Reservation(showId, asientoVencidoId, "usuario-vencido");
        reservaVencida.setStatus(ReservationStatus.HELD);
        reservaVencida.setExpiresAt(heldUntilVencido);
        reservationRepository.save(reservaVencida);

        Seat asientoVigente = new Seat(showId, "A", 2);
        LocalDateTime heldUntilVigente = LocalDateTime.now().plusMinutes(10);
        asientoVigente.hold("usuario-vigente", heldUntilVigente);
        asientoVigente = seatRepository.save(asientoVigente);
        asientoVigenteId = asientoVigente.getId();

        Reservation reservaVigente = new Reservation(showId, asientoVigenteId, "usuario-vigente");
        reservaVigente.setStatus(ReservationStatus.HELD);
        reservaVigente.setExpiresAt(heldUntilVigente);
        reservationRepository.save(reservaVigente);
    }

    @Test
    void soloLiberaLosAsientosCuyoHoldRealmenteVencio() {
        scheduler.releaseExpiredHolds();

        Seat asientoVencido = seatRepository.findById(asientoVencidoId).orElseThrow();
        assertEquals(SeatStatus.AVAILABLE, asientoVencido.getStatus());
        assertNull(asientoVencido.getHeldBy());
        assertNull(asientoVencido.getHeldUntil());

        List<Reservation> reservasVencidas =
                reservationRepository.findBySeatIdAndStatus(asientoVencidoId, ReservationStatus.EXPIRED);
        assertEquals(1, reservasVencidas.size());

        Seat asientoVigente = seatRepository.findById(asientoVigenteId).orElseThrow();
        assertEquals(SeatStatus.HELD, asientoVigente.getStatus(),
                "El asiento con heldUntil en el futuro no debería tocarse");
        assertEquals("usuario-vigente", asientoVigente.getHeldBy());

        List<Reservation> reservasVigentesAunHeld =
                reservationRepository.findBySeatIdAndStatus(asientoVigenteId, ReservationStatus.HELD);
        assertEquals(1, reservasVigentesAunHeld.size());

        JsonNode eventoExpirado = leerResultadoExpiradoDe(asientoVencidoId);
        assertEquals("EXPIRED", eventoExpirado.get("result").asText());
        assertTrue(eventoExpirado.get("reason").asText().toLowerCase().contains("expir"),
                "El motivo debería mencionar la expiración, fue: " + eventoExpirado.get("reason").asText());
    }

    private JsonNode leerResultadoExpiradoDe(UUID seatId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "expiration-test-" + UUID.randomUUID());
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
                        if (node.has("seatId") && seatId.toString().equals(node.get("seatId").asText())
                                && "EXPIRED".equals(node.get("result").asText())) {
                            return node;
                        }
                    } catch (Exception ignored) {
                        // mensaje no parseable como el JSON esperado, se ignora
                    }
                }
            }
        }

        fail("No se encontró en " + EVENTS_TOPIC + " ningún resultado EXPIRED para seatId=" + seatId + " después de 15s");
        return null; // inalcanzable, fail() lanza AssertionError
    }
}
