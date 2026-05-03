package com.aerostream.topology;

import com.aerostream.avro.EnrichedTelemetryEvent;
import com.aerostream.avro.TelemetryEvent;
import com.aerostream.enrichment.EnrichmentMapper;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EnrichmentTopology {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentTopology.class);

    @Value("${kafka.topics.raw-telemetry}")
    private String rawTelemetryTopic;

    @Value("${kafka.topics.circuit-metadata}")
    private String circuitMetadataTopic;

    @Value("${kafka.topics.driver-profiles}")
    private String driverProfilesTopic;

    @Value("${kafka.topics.enriched-telemetry}")
    private String enrichedTelemetryTopic;

    /**
     * Bean name must differ from the configuration class bean ({@code enrichmentTopology}
     * is reserved for the {@link EnrichmentTopology} {@code @Configuration} itself).
     */
    @Bean
    public KStream<String, EnrichedTelemetryEvent> enrichedTelemetryKStream(
            StreamsBuilder builder,
            SpecificAvroSerde<TelemetryEvent> telemetrySerde,
            SpecificAvroSerde<EnrichedTelemetryEvent> enrichedSerde,
            GenericAvroSerde genericSerde) {

        // ── Source: raw telemetry events, keyed by car_id ─────────────────────
        KStream<String, TelemetryEvent> rawStream = builder.stream(
            rawTelemetryTopic,
            Consumed.with(Serdes.String(), telemetrySerde)
        );

        // ── GlobalKTable 1: circuit reference data (from Debezium CDC) ─────────
        // Key: circuit_id String (e.g. "MONZA")
        // Replicated to ALL stream thread state stores — no co-partitioning needed
        GlobalKTable<String, GenericRecord> circuitTable = builder.globalTable(
            circuitMetadataTopic,
            Consumed.with(Serdes.String(), genericSerde)
        );

        // ── GlobalKTable 2: driver reference data (from Debezium CDC) ──────────
        // Key: driver_id String (e.g. "DRV_01")
        GlobalKTable<String, GenericRecord> driverTable = builder.globalTable(
            driverProfilesTopic,
            Consumed.with(Serdes.String(), genericSerde)
        );

        // ── Join 1: enrich with circuit data ───────────────────────────────────
        // Key mapper: extract "MONZA" from session_id "RACE_2024_MONZA_R1"
        // leftJoin: events pass through even when circuit GlobalKTable hasn't loaded yet
        KStream<String, EnrichedTelemetryEvent> withCircuit = rawStream.leftJoin(
            circuitTable,
            (carId, event) -> extractCircuitId(event.getSessionId().toString()),
            EnrichmentMapper::withCircuit
        );

        // ── Join 2: enrich with driver data ────────────────────────────────────
        // Key mapper: driver_id is already a clean String in the event
        // leftJoin: events with unknown drivers still pass through (enriched=false for driver)
        KStream<String, EnrichedTelemetryEvent> fullyEnriched = withCircuit.leftJoin(
            driverTable,
            (carId, enrichedEvent) -> enrichedEvent.getDriverId().toString(),
            EnrichmentMapper::withDriver
        );

        // ── Sink: write enriched events to enriched-telemetry ─────────────────
        fullyEnriched.to(
            enrichedTelemetryTopic,
            Produced.with(Serdes.String(), enrichedSerde)
        );

        log.info("Enrichment topology wired: {} → [{}, {}] → {}",
            rawTelemetryTopic, circuitMetadataTopic, driverProfilesTopic, enrichedTelemetryTopic);

        return fullyEnriched;
    }

    /**
     * Extracts the circuit ID from the session_id format used by Phase 2 simulator.
     * "RACE_2024_MONZA_R1"      → "MONZA"
     * "RACE_2024_SILVERSTONE_R1" → "SILVERSTONE"
     * Parts: [RACE, 2024, MONZA, R1] → index 2
     */
    public static String extractCircuitId(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) return null;
        String[] parts = sessionId.split("_");
        return parts.length >= 3 ? parts[2] : null;
    }
}
