package com.portfolio.reservas;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.portfolio.reservas.domain.Reservation;
import com.portfolio.reservas.domain.Seat;
import com.portfolio.reservas.domain.Show;
import com.portfolio.reservas.domain.enums.ReservationStatus;
import com.portfolio.reservas.domain.enums.SeatStatus;
import com.portfolio.reservas.event.SeatReservationRequestEvent;
import com.portfolio.reservas.repository.ReservationRepository;
import com.portfolio.reservas.repository.SeatRepository;
import com.portfolio.reservas.repository.ShowRepository;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test de integración de la Fase 8 (último pendiente cerrado al final de la
 * fase): verifica la idempotencia real del consumer — el mismo eventId
 * entregado dos veces (redelivery genuino de Kafka, no un evento nuevo) no
 * debería duplicar la Reservation ni reprocesar el hold.
 *
 * A diferencia de los demás tests, acá NO se usa el endpoint HTTP (que
 * genera un eventId aleatorio en cada llamada) — se publica el evento
 * directamente al tópico con un producer crudo, dos veces con el MISMO
 * eventId, simulando una redelivery real de Kafka (reintento del cliente,
 * rebalanceo antes del commit de offset, etc.).
 *
 * Detalle técnico: el consumer usa JsonDeserializer<Object> (sin tipo fijo),
 * que depende del header "__TypeId__" que Spring agrega automáticamente al
 * serializar con JsonSerializer. Como acá se publica con un producer crudo
 * (StringSerializer), ese header se agrega a mano — si no, el mensaje
 * fallaría al deserializar y terminaría en el DLT en vez de probar lo que
 * este test necesita probar.
 */
class RequestIdempotencyIntegrationTest extends AbstractIntegrationTest {

    private static final String REQUESTS_TOPIC = "seat-reservation-requests";
    private static final String TYPE_ID_HEADER = "__TypeId__";
    private static final String EVENT_CLASS_NAME = "com.portfolio.reservas.event.SeatReservationRequestEvent";

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
                new Show("Función de prueba - idempotencia", "Sala 1", LocalDateTime.now().plusDays(1), 1));
        showId = show.getId();
        seatId = seatRepository.save(new Seat(showId, "A", 1)).getId();
    }

    @Test
    void elMismoEventIdEntregadoDosVecesNoDuplicaLaReserva() throws Exception {
        UUID eventId = UUID.randomUUID();
        String userId = "usuario-idempotencia";

        SeatReservationRequestEvent evento = new SeatReservationRequestEvent(
                eventId, showId, seatId, userId, "REQUEST_HOLD", Instant.now());

        publicarEnKafka(evento);
        Seat asientoTrasElPrimero = esperarHastaQueElAsientoDejeDeEstarAvailable();
        assertEquals(SeatStatus.HELD, asientoTrasElPrimero.getStatus());
        assertEquals(userId, asientoTrasElPrimero.getHeldBy());

        List<Reservation> reservasTrasElPrimero =
                reservationRepository.findBySeatIdAndStatus(seatId, ReservationStatus.HELD);
        assertEquals(1, reservasTrasElPrimero.size());

        publicarEnKafka(evento);
        Thread.sleep(4000);

        List<Reservation> reservasTrasElSegundo =
                reservationRepository.findBySeatIdAndStatus(seatId, ReservationStatus.HELD);
        assertEquals(1, reservasTrasElSegundo.size(),
                "La redelivery del mismo eventId no debería crear una segunda Reservation");

        Seat asientoFinal = seatRepository.findById(seatId).orElseThrow();
        assertEquals(SeatStatus.HELD, asientoFinal.getStatus());
        assertEquals(userId, asientoFinal.getHeldBy());
    }

    private void publicarEnKafka(SeatReservationRequestEvent evento) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        String json = mapper.writeValueAsString(evento);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        RecordHeader typeIdHeader = new RecordHeader(
                TYPE_ID_HEADER, EVENT_CLASS_NAME.getBytes(StandardCharsets.UTF_8));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    REQUESTS_TOPIC, null, showId.toString(), json, List.of(typeIdHeader));
            producer.send(record).get();
        }
    }

    private Seat esperarHastaQueElAsientoDejeDeEstarAvailable() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            Seat seat = seatRepository.findById(seatId).orElseThrow();
            if (seat.getStatus() != SeatStatus.AVAILABLE) {
                Thread.sleep(300);
                return seatRepository.findById(seatId).orElseThrow();
            }
            Thread.sleep(200);
        }
        throw new AssertionError("El asiento seatId=" + seatId + " siguió AVAILABLE después de 15s.");
    }
}
