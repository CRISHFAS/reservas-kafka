package com.portfolio.reservas;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Clase base para los tests de integración de la Fase 8: levanta Kafka y
 * Postgres reales (efímeros, vía Testcontainers) en lugar de mocks, para
 * validar el comportamiento real del sistema end-to-end, incluyendo el
 * particionamiento de Kafka por showId y el optimistic locking de JPA
 * sobre la entidad Seat.
 *
 * Nota sobre versiones (mezcla deliberada, no descuido): Testcontainers
 * 2.x reorganizó las clases de contenedor bajo nuevos paquetes
 * (org.testcontainers.kafka.*, org.testcontainers.postgresql.*), dejando
 * las de org.testcontainers.containers.* como deprecadas pero funcionales.
 * Postgres acá usa deliberadamente la clase deprecada porque
 * JdbcContainerConnectionDetailsFactory (el bridge de @ServiceConnection
 * para bases JDBC) sigue soportando esa jerarquía sin problema. Kafka, en
 * cambio, tuvo que migrarse a ConfluentKafkaContainer (paquete nuevo)
 * porque KafkaContainerConnectionDetailsFactory en Spring Boot 4.1 solo
 * reconoce por tipo los contenedores de org.testcontainers.kafka.* /
 * org.testcontainers.redpanda.RedpandaContainer — con la clase deprecada,
 * @ServiceConnection fallaba con ConnectionDetailsNotFoundException sin
 * importar qué "name" se le pusiera (bug real encontrado en esta fase).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class AbstractIntegrationTest {

    // Patrón "singleton container": arrancados a mano una sola vez para
    // todo el proceso de Maven, NO gestionados por la extensión de JUnit
    // (@Testcontainers/@Container). Con @Container, JUnit para los
    // contenedores en el afterAll de CADA clase de test; como esta clase
    // base se comparte entre SeatConcurrencyIntegrationTest y
    // HappyPathReservationIntegrationTest, eso rompía la segunda clase en
    // ejecutarse (Postgres ya apagado por el afterAll de la primera).
    // Con este patrón, Testcontainers los limpia solo al final de todo
    // el proceso vía Ryuk, no entre clases (bug real encontrado en esta fase).
    @ServiceConnection
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @ServiceConnection
    static ConfluentKafkaContainer kafka =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"));

    static {
        postgres.start();
        kafka.start();
    }
}
