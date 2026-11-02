package io.github.kartikkumarmahindrakar.kafka.streams.spring.integration.config;


import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kafkaStreamsConfig() {
        Map<String, Object> props = Map.of(
                APPLICATION_ID_CONFIG, "example-app",
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                CACHE_MAX_BYTES_BUFFERING_CONFIG, "0"
        );

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    NewTopic inputTopic() {
        return TopicBuilder
                .name("output-topic")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
