package com.aerostream.schema;

import com.aerostream.avro.TelemetryEvent;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates the TelemetryEvent Avro schema structure without requiring a running Schema Registry.
 * The schema is read from the generated class, which is produced by the avro-maven-plugin.
 */
class SchemaRegistrationTest {

    @Test
    void telemetryEventSchemaIsValid() {
        Schema schema = TelemetryEvent.getClassSchema();
        assertThat(schema.getName()).isEqualTo("TelemetryEvent");
        assertThat(schema.getNamespace()).isEqualTo("com.aerostream.avro");
        assertThat(schema.getFields()).hasSize(26);
    }

    @Test
    void requiredFieldsArePresent() {
        Schema schema = TelemetryEvent.getClassSchema();
        assertThat(schema.getField("car_id")).isNotNull();
        assertThat(schema.getField("driver_id")).isNotNull();
        assertThat(schema.getField("session_id")).isNotNull();
        assertThat(schema.getField("timestamp_ms")).isNotNull();
        assertThat(schema.getField("tire_wear_fl")).isNotNull();
        assertThat(schema.getField("tire_wear_fr")).isNotNull();
        assertThat(schema.getField("tire_wear_rl")).isNotNull();
        assertThat(schema.getField("tire_wear_rr")).isNotNull();
        assertThat(schema.getField("g_force_lat")).isNotNull();
        assertThat(schema.getField("g_force_lon")).isNotNull();
        assertThat(schema.getField("ers_deploy_pct")).isNotNull();
    }

    @Test
    void tireCompoundEnumHasCorrectSymbols() {
        Schema schema = TelemetryEvent.getClassSchema();
        Schema tireCompoundSchema = schema.getField("tire_compound").schema();
        assertThat(tireCompoundSchema.getType()).isEqualTo(Schema.Type.ENUM);
        assertThat(tireCompoundSchema.getEnumSymbols())
            .containsExactly("SOFT", "MEDIUM", "HARD", "INTER", "WET");
    }
}
