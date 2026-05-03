package com.aerostream.kafka;

import com.aerostream.simulation.TelemetrySimulator;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Routes failed producer records to the dlq-telemetry topic.
 *
 * Uses a separate KafkaTemplate<String, byte[]> — no Avro serialization.
 * RETRIES=0 and ACKS=1 to ensure DLQ failures never block or retry.
 *
 * Each DLQ record carries diagnostic headers:
 *   X-Error-Message, X-Original-Topic, X-Error-Timestamp, X-Exception-Class, X-Original-Car-Id
 */
@Component
@Slf4j
public class DlqPublisher {

    private static final String DLQ_TOPIC = "dlq-telemetry";
    private static final int    HEADER_MAX_BYTES = 500;

    private final KafkaTemplate<String, byte[]> dlqTemplate;
    private final Counter dlqCounter;

    public DlqPublisher(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                        MeterRegistry meterRegistry,
                        @Lazy TelemetrySimulator simulator) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        // DLQ producer is non-idempotent — failures here are logged, not re-routed
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        this.dlqTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));

        this.dlqCounter = Counter.builder("telemetry.events.dlq")
            .description("Total events routed to dead letter queue")
            .tag("topic", DLQ_TOPIC)
            .register(meterRegistry);

        // Wire back into simulator so it can track DLQ count
        simulator.setDlqPublisher(this);
    }

    /**
     * Called when a full TelemetryEvent record fails publish.
     */
    public void send(ProducerRecord<String, ?> original, Exception cause) {
        String carId = original.key() != null ? original.key() : "UNKNOWN";
        log.warn("Routing to DLQ: car={}, cause={}", carId, cause.getMessage());

        byte[] valueBytes = original.value() != null
            ? original.value().toString().getBytes(StandardCharsets.UTF_8)
            : "null-value".getBytes(StandardCharsets.UTF_8);

        publishToDlq(carId, original.topic(), valueBytes, cause);
    }

    /**
     * Called when the raw car ID is known but no full record is available (e.g. async failure).
     */
    public void sendRaw(String carId, String originalTopic, Throwable cause) {
        log.warn("DLQ (raw): car={}, cause={}", carId, cause.getMessage());
        byte[] valueBytes = ("async-failure:" + cause.getMessage()).getBytes(StandardCharsets.UTF_8);
        publishToDlq(carId, originalTopic, valueBytes, cause);
    }

    private void publishToDlq(String carId, String originalTopic, byte[] valueBytes, Throwable cause) {
        ProducerRecord<String, byte[]> dlqRecord = new ProducerRecord<>(DLQ_TOPIC, carId, valueBytes);
        dlqRecord.headers()
            .add("X-Error-Message",   truncate(cause.getMessage()).getBytes(StandardCharsets.UTF_8))
            .add("X-Original-Topic",  originalTopic.getBytes(StandardCharsets.UTF_8))
            .add("X-Error-Timestamp", String.valueOf(Instant.now().toEpochMilli()).getBytes(StandardCharsets.UTF_8))
            .add("X-Exception-Class", cause.getClass().getName().getBytes(StandardCharsets.UTF_8))
            .add("X-Original-Car-Id", carId.getBytes(StandardCharsets.UTF_8));

        dlqTemplate.send(dlqRecord).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to write to DLQ for car={}: {}", carId, ex.getMessage());
            } else {
                dlqCounter.increment();
            }
        });
    }

    private String truncate(String s) {
        if (s == null) return "null";
        return s.length() > HEADER_MAX_BYTES ? s.substring(0, HEADER_MAX_BYTES) + "..." : s;
    }
}
