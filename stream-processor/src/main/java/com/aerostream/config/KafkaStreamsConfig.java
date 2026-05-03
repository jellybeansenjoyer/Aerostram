package com.aerostream.config;

import com.aerostream.avro.EnrichedTelemetryEvent;
import com.aerostream.avro.TelemetryEvent;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.Map;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.streams.application-id}")
    private String applicationId;

    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfig() {
        Map<String, Object> props = Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG,           applicationId,
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers,
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,  Serdes.String().getClass().getName(),
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName(),
            StreamsConfig.PROCESSING_GUARANTEE_CONFIG,     StreamsConfig.EXACTLY_ONCE_V2,
            StreamsConfig.NUM_STREAM_THREADS_CONFIG,       2,
            StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,       100,
            "schema.registry.url",                         schemaRegistryUrl
        );
        log.info("Kafka Streams configured: appId={}, bootstrap={}", applicationId, bootstrapServers);
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public SpecificAvroSerde<TelemetryEvent> telemetrySerde() {
        SpecificAvroSerde<TelemetryEvent> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of("schema.registry.url", schemaRegistryUrl), false);
        return serde;
    }

    @Bean
    public SpecificAvroSerde<EnrichedTelemetryEvent> enrichedSerde() {
        SpecificAvroSerde<EnrichedTelemetryEvent> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of("schema.registry.url", schemaRegistryUrl), false);
        return serde;
    }

    @Bean
    public GenericAvroSerde genericSerde() {
        GenericAvroSerde serde = new GenericAvroSerde();
        serde.configure(Map.of("schema.registry.url", schemaRegistryUrl), false);
        return serde;
    }
}
