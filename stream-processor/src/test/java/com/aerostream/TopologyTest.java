package com.aerostream;

import com.aerostream.avro.EnrichedTelemetryEvent;
import com.aerostream.avro.TelemetryEvent;
import com.aerostream.avro.TireCompound;
import com.aerostream.topology.EnrichmentTopology;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the enrichment topology using TopologyTestDriver — no running Kafka cluster required.
 * Uses MockSchemaRegistry to simulate schema registration.
 */
class TopologyTest {

    private static final String MOCK_REGISTRY_SCOPE = "topology-test";
    private static final String SCHEMA_REGISTRY_URL  = "mock://" + MOCK_REGISTRY_SCOPE;

    private TopologyTestDriver driver;
    private TestInputTopic<String, TelemetryEvent>   rawInput;
    private TestInputTopic<String, GenericRecord>    circuitInput;
    private TestInputTopic<String, GenericRecord>    driverInput;
    private TestOutputTopic<String, EnrichedTelemetryEvent> enrichedOutput;

    private SpecificAvroSerde<TelemetryEvent>       telemetrySerde;
    private SpecificAvroSerde<EnrichedTelemetryEvent> enrichedSerde;
    private GenericAvroSerde                         genericSerde;

    @BeforeEach
    void setUp() {
        Map<String, String> serdeConfig = Map.of(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL
        );

        telemetrySerde = new SpecificAvroSerde<>();
        telemetrySerde.configure(serdeConfig, false);

        enrichedSerde = new SpecificAvroSerde<>();
        enrichedSerde.configure(serdeConfig, false);

        genericSerde = new GenericAvroSerde();
        genericSerde.configure(serdeConfig, false);

        // Build topology using the same class under test
        StreamsBuilder builder = new StreamsBuilder();
        EnrichmentTopology topology = new EnrichmentTopology();
        // Inject @Value fields via reflection or direct assignment for test
        setField(topology, "rawTelemetryTopic",     "raw-telemetry");
        setField(topology, "circuitMetadataTopic",  "circuit-metadata");
        setField(topology, "driverProfilesTopic",   "driver-profiles");
        setField(topology, "enrichedTelemetryTopic","enriched-telemetry");

        topology.enrichedTelemetryKStream(builder, telemetrySerde, enrichedSerde, genericSerde);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,  "test-enrichment");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);

        driver = new TopologyTestDriver(builder.build(), props);

        rawInput = driver.createInputTopic("raw-telemetry",
            Serdes.String().serializer(), telemetrySerde.serializer());
        circuitInput = driver.createInputTopic("circuit-metadata",
            Serdes.String().serializer(), genericSerde.serializer());
        driverInput = driver.createInputTopic("driver-profiles",
            Serdes.String().serializer(), genericSerde.serializer());
        enrichedOutput = driver.createOutputTopic("enriched-telemetry",
            Serdes.String().deserializer(), enrichedSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        MockSchemaRegistry.dropScope(MOCK_REGISTRY_SCOPE);
    }

    @Test
    void enrichesEventWithCircuitAndDriverData() {
        // Pipe circuit reference data into GlobalKTable
        circuitInput.pipeInput("MONZA", buildCircuitRecord("Autodromo Nazionale Monza", "Italy", 5.793, 53, 2, 22.0));

        // Pipe driver reference data into GlobalKTable
        driverInput.pipeInput("DRV_01", buildDriverRecord("Max Verstappen", "VER", "REDBULL", "Dutch"));

        // Pipe one telemetry event
        rawInput.pipeInput("CAR_01", buildTelemetryEvent("CAR_01", "DRV_01", "RACE_2024_MONZA_R1"));

        // Assert enriched output
        assertThat(enrichedOutput.isEmpty()).isFalse();
        EnrichedTelemetryEvent result = enrichedOutput.readValue();

        assertThat(result.getCarId().toString()).isEqualTo("CAR_01");
        assertThat(result.getCircuitName().toString()).isEqualTo("Autodromo Nazionale Monza");
        assertThat(result.getCircuitCountry().toString()).isEqualTo("Italy");
        assertThat(result.getDriverFullName().toString()).isEqualTo("Max Verstappen");
        assertThat(result.getDriverAbbrev().toString()).isEqualTo("VER");
        assertThat(result.getDriverTeam().toString()).isEqualTo("REDBULL");
        assertThat(result.getEnriched()).isTrue();
    }

    @Test
    void passesEventThroughWhenCircuitNotInGlobalKTable() {
        // No circuit data in GlobalKTable
        driverInput.pipeInput("DRV_01", buildDriverRecord("Max Verstappen", "VER", "REDBULL", "Dutch"));
        rawInput.pipeInput("CAR_01", buildTelemetryEvent("CAR_01", "DRV_01", "RACE_2024_MONZA_R1"));

        assertThat(enrichedOutput.isEmpty()).isFalse();
        EnrichedTelemetryEvent result = enrichedOutput.readValue();

        // Event passes through (leftJoin) but circuit fields are null
        assertThat(result.getCarId().toString()).isEqualTo("CAR_01");
        assertThat(result.getCircuitName()).isNull();
        assertThat(result.getDriverFullName().toString()).isEqualTo("Max Verstappen");
    }

    @Test
    void circuitIdExtractionFromSessionId() {
        assertThat(EnrichmentTopology.extractCircuitId("RACE_2024_MONZA_R1")).isEqualTo("MONZA");
        assertThat(EnrichmentTopology.extractCircuitId("RACE_2024_SILVERSTONE_R1")).isEqualTo("SILVERSTONE");
        assertThat(EnrichmentTopology.extractCircuitId(null)).isNull();
        assertThat(EnrichmentTopology.extractCircuitId("")).isNull();
        assertThat(EnrichmentTopology.extractCircuitId("SHORT")).isNull();
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private TelemetryEvent buildTelemetryEvent(String carId, String driverId, String sessionId) {
        return TelemetryEvent.newBuilder()
            .setCarId(carId).setDriverId(driverId).setSessionId(sessionId)
            .setTimestampMs(System.currentTimeMillis()).setLap(1).setSector(1)
            .setSpeedKph(250.0).setRpm(12000).setGear(6)
            .setThrottlePct(90.0).setBrakePct(0.0).setDrsActive(false)
            .setTireCompound(TireCompound.MEDIUM)
            .setTireTempFl(95.0).setTireTempFr(95.0).setTireTempRl(95.0).setTireTempRr(95.0)
            .setTireWearFl(10.0).setTireWearFr(10.0).setTireWearRl(10.0).setTireWearRr(10.0)
            .setFuelLoadKg(95.0).setEngineTempC(100.0).setErsDeployPct(60.0)
            .setGForceLat(2.5).setGForceLon(1.2)
            .build();
    }

    private GenericRecord buildCircuitRecord(String name, String country,
                                              double lengthKm, int numLaps, int drsZones, double pitLoss) {
        Schema schema = buildSimpleSchema("CircuitRecord",
            "circuit_name", "country", "length_km", "num_laps", "drs_zones", "pit_loss_time_s");
        GenericRecord r = new GenericData.Record(schema);
        r.put("circuit_name", name);
        r.put("country", country);
        r.put("length_km", lengthKm);
        r.put("num_laps", numLaps);
        r.put("drs_zones", drsZones);
        r.put("pit_loss_time_s", pitLoss);
        return r;
    }

    private GenericRecord buildDriverRecord(String fullName, String abbrev, String teamId, String nationality) {
        Schema schema = buildSimpleSchema("DriverRecord", "full_name", "abbreviated_name", "team_id", "nationality");
        GenericRecord r = new GenericData.Record(schema);
        r.put("full_name", fullName);
        r.put("abbreviated_name", abbrev);
        r.put("team_id", teamId);
        r.put("nationality", nationality);
        return r;
    }

    private Schema buildSimpleSchema(String name, String... fields) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"type\":\"record\",\"name\":\"").append(name).append("\",\"fields\":[");
        for (int i = 0; i < fields.length; i++) {
            sb.append("{\"name\":\"").append(fields[i]).append("\",\"type\":[\"null\",\"string\",\"double\",\"int\"],\"default\":null}");
            if (i < fields.length - 1) sb.append(",");
        }
        sb.append("]}");
        return new Schema.Parser().parse(sb.toString());
    }

    private void setField(Object obj, String fieldName, String value) {
        try {
            var field = obj.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(obj, value);
        } catch (Exception e) {
            throw new RuntimeException("Could not set field " + fieldName, e);
        }
    }
}
