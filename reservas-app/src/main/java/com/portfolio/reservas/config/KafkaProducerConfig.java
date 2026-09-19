package com.portfolio.reservas.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ObjectMapper kafkaObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory(ObjectMapper kafkaObjectMapper,
                                                             KafkaConnectionDetails kafkaConnectionDetails) {
        Map<String, Object> configProps = new HashMap<>();
        // Se usa KafkaConnectionDetails (en vez de @Value("${spring.kafka.bootstrap-servers}"))
        // porque este proyecto construye sus propios beans de Kafka a mano, sin pasar por
        // la autoconfiguración estándar de Spring Boot. @ServiceConnection de Testcontainers
        // solo sobreescribe la propiedad para código que consulta KafkaConnectionDetails, NO
        // el valor crudo de application.yml — leerlo directo con @Value hacía que los tests
        // de integración terminaran hablando con el Kafka real y persistente en vez del
        // efímero de Testcontainers (bug real encontrado y corregido en la Fase 8).
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                String.join(",", kafkaConnectionDetails.getBootstrapServers()));
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // Explícito para evitar que ambos ProducerFactory (este y el dltProducerFactory
        // de KafkaConsumerConfig) terminen con el mismo clientId auto-asignado
        // (reservas-app-producer-1), lo que generaba un warning inofensivo de mbean
        // duplicado en los logs (encontrado en el debugging de la Fase 8).
        configProps.put(ProducerConfig.CLIENT_ID_CONFIG, "reservas-app-producer");

        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(configProps);
        factory.setValueSerializer(new JsonSerializer<>(kafkaObjectMapper));
        return factory;
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
