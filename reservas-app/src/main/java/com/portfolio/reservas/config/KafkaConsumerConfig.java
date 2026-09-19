package com.portfolio.reservas.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.portfolio.reservas.domain.ProcessedEvent;
import com.portfolio.reservas.event.SeatReservationRequestEvent;
import com.portfolio.reservas.repository.ProcessedEventRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);
    private static final String DLT_TOPIC = "seat-reservation-dlt";

    @Bean
    public ConsumerFactory<String, Object> consumerFactory(ObjectMapper kafkaObjectMapper,
                                                             KafkaConnectionDetails kafkaConnectionDetails) {
        JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>(kafkaObjectMapper);
        jsonDeserializer.addTrustedPackages("com.portfolio.reservas.event");

        ErrorHandlingDeserializer<Object> errorHandlingDeserializer =
                new ErrorHandlingDeserializer<>(jsonDeserializer);

        Map<String, Object> props = new HashMap<>();
        // Ver comentario equivalente en KafkaProducerConfig sobre por qué se usa
        // KafkaConnectionDetails en vez de @Value("${spring.kafka.bootstrap-servers}").
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                String.join(",", kafkaConnectionDetails.getBootstrapServers()));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "reservas-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                errorHandlingDeserializer
        );
    }

    // ProducerFactory/KafkaTemplate dedicado solo para el Dead Letter Topic.
    // No reutiliza el KafkaTemplate<String, Object> del producer porque
    // DeadLetterPublishingRecoverer necesita KafkaOperations<Object, Object>.
    @Bean
    public ProducerFactory<Object, Object> dltProducerFactory(ObjectMapper kafkaObjectMapper,
                                                                KafkaConnectionDetails kafkaConnectionDetails) {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                String.join(",", kafkaConnectionDetails.getBootstrapServers()));
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // Explícito para evitar que este ProducerFactory y el de KafkaProducerConfig
        // terminen con el mismo clientId auto-asignado (reservas-app-producer-1),
        // lo que generaba un warning inofensivo de mbean duplicado en los logs
        // (encontrado en el debugging de la Fase 8).
        configProps.put(ProducerConfig.CLIENT_ID_CONFIG, "reservas-app-dlt-producer");

        DefaultKafkaProducerFactory<Object, Object> factory = new DefaultKafkaProducerFactory<>(configProps);
        factory.setValueSerializer(new JsonSerializer<>(kafkaObjectMapper));
        return factory;
    }

    @Bean
    public KafkaTemplate<Object, Object> dltKafkaTemplate(ProducerFactory<Object, Object> dltProducerFactory) {
        return new KafkaTemplate<>(dltProducerFactory);
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> dltKafkaTemplate,
                                                  ProcessedEventRepository processedEventRepository) {
        // Siempre partición 0: seat-reservation-dlt tiene una sola partición.
        DeadLetterPublishingRecoverer dltRecoverer = new DeadLetterPublishingRecoverer(
                dltKafkaTemplate,
                (record, exception) -> new TopicPartition(DLT_TOPIC, 0)
        );

        // Envolvemos el recoverer real: además de publicar al DLT, marcamos el
        // evento como procesado en processed_events. CLAVE: este código se
        // ejecuta UNA SOLA VEZ, cuando el DefaultErrorHandler ya agotó los
        // reintentos del FixedBackOff y decide recuperar el mensaje - no en
        // cada intento individual. Si el registro de idempotencia se guardara
        // dentro del listener antes de cada throw (como se hizo en un primer
        // intento), el propio findExistsById detendría los reintentos
        // internos del backoff, y el mensaje nunca llegaría a recuperarse
        // de verdad (bug real encontrado y corregido en la Fase 5).
        ConsumerRecordRecoverer recoverer = (record, exception) -> {
            log.warn("Reintentos agotados, recuperando hacia DLT: topic={} partition={} offset={} causa={}",
                    record.topic(), record.partition(), record.offset(), exception.getMessage());
            try {
                dltRecoverer.accept(record, exception);
                log.warn("Publicado en DLT OK: topic={} partition={} offset={}",
                        record.topic(), record.partition(), record.offset());
            } catch (Exception e) {
                log.error("FALLÓ la publicación en el DLT para topic={} partition={} offset={}",
                        record.topic(), record.partition(), record.offset(), e);
                throw e;
            }
            Object value = record.value();
            if (value instanceof SeatReservationRequestEvent event) {
                processedEventRepository.save(new ProcessedEvent(event.eventId()));
                log.warn("processed_events actualizado para eventId={}", event.eventId());
            }
        };

        FixedBackOff backOff = new FixedBackOff(1000L, 3L); // 3 intentos, 1s entre cada uno

        return new DefaultErrorHandler(recoverer, backOff);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            DefaultErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }
}
