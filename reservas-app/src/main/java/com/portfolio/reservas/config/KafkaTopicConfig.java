package com.portfolio.reservas.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String REQUESTS_TOPIC = "seat-reservation-requests";
    public static final String EVENTS_TOPIC = "seat-reservation-events";
    public static final String DLT_TOPIC = "seat-reservation-dlt";

    @Bean
    public NewTopic seatReservationRequestsTopic() {
        return TopicBuilder.name(REQUESTS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic seatReservationEventsTopic() {
        return TopicBuilder.name(EVENTS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic seatReservationDltTopic() {
        return TopicBuilder.name(DLT_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
