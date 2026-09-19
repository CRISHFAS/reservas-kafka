package com.portfolio.reservas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.portfolio.reservas.controller.dto.ReservationRequestDto;
import com.portfolio.reservas.repository.ProcessedEventRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test de integración de la Fase 8: cubre el camino de ERROR, complementario
 * a los otros dos tests que solo cubren caminos exitosos. Verifica qué pasa
 * cuando el consumer recibe un evento para un seatId que NO existe en la
 * base — el mecanismo de reintentos (FixedBackOff: 3 intentos, 1s entre
 * cada uno, ver KafkaConsumerConfig) y, tras agotarlos, la recuperación
 * hacia el Dead Letter Topic junto con el registro de idempotencia.
 *
 * A diferencia de los otros tests, acá no hay endpoint HTTP que exponga el
 * DLT, así que se lee directamente con un KafkaConsumer crudo apuntando al
 * broker efímero de Testcontainers (mismo bootstrapServers que usa el
 * contexto de Spring, expuesto por el campo `kafka` heredado de
 * AbstractIntegrationTest).
 */
@AutoConfigureTestRestTemplate
class DeadLetterTopicIntegrationTest extends AbstractIntegrationTest {

    private static final String DLT_TOPIC = "seat-reservation-dlt";

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Test
    void unSeatIdInexistenteTerminaEnElDeadLetterTopicTrasAgotarReintentos() {
        UUID showId = UUID.randomUUID();
        UUID seatIdInexistente = UUID.randomUUID(); // nunca se persistió ningún Seat con este id
        String userId = "usuario-dlt";

        ReservationRequestDto request = new ReservationRequestDto(showId, seatIdInexistente, userId);
        ResponseEntity<String> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/reservations", request, String.class);
        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode(),
                "El POST solo encola en Kafka, siempre responde 202 aunque el seatId no exista");

        // El FixedBackOff son 3 reintentos con 1s de espera entre cada uno,
        // así que el mensaje tarda ~3s en agotar los reintentos y recién
        // ahí recuperarse hacia el DLT. Damos margen generoso porque además
        // hay que esperar a que el KafkaConsumer del test haga join al
        // grupo consumer y arranque a hacer poll.
        JsonNode mensajeDlt = leerMensajeDelDltConSeatId(seatIdInexistente);

        assertEquals(seatIdInexistente.toString(), mensajeDlt.get("seatId").asText());
        assertEquals(userId, mensajeDlt.get("userId").asText());

        UUID eventId = UUID.fromString(mensajeDlt.get("eventId").asText());
        assertTrue(processedEventRepository.existsById(eventId),
                "El recoverer del DLT debería haber marcado el evento como procesado en processed_events");
    }

    private JsonNode leerMensajeDelDltConSeatId(UUID seatIdBuscado) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        ObjectMapper mapper = new ObjectMapper();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(DLT_TOPIC));

            long deadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JsonNode node = mapper.readTree(record.value());
                        if (node.has("seatId") && seatIdBuscado.toString().equals(node.get("seatId").asText())) {
                            return node;
                        }
                    } catch (Exception ignored) {
                        // mensaje no parseable como el JSON esperado, se ignora
                    }
                }
            }
        }

        fail("No se encontró en el DLT ningún mensaje para seatId=" + seatIdBuscado + " después de 20s");
        return null; // inalcanzable, fail() lanza AssertionError
    }
}
