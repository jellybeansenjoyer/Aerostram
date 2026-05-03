package com.aerostream.testing;

import com.aerostream.avro.TelemetryEvent;
import com.aerostream.avro.TireCompound;
import com.aerostream.kafka.DlqPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Generates deliberately malformed TelemetryEvent records to exercise the DLQ path.
 * Negative timestamps, impossible RPM, > 100% throttle, > 100% tire wear, etc.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class PoisonPillGenerator {

    private final KafkaTemplate<String, TelemetryEvent> kafkaTemplate;
    private final DlqPublisher dlqPublisher;

    private static final String RAW_TELEMETRY = "raw-telemetry";

    public int generateAndSend(int count) {
        int sent = 0;
        for (int i = 0; i < count; i++) {
            String poisonKey = "POISON_" + i;
            try {
                TelemetryEvent poison = createPoisonPill(i);
                ProducerRecord<String, TelemetryEvent> record =
                    new ProducerRecord<>(RAW_TELEMETRY, poisonKey, poison);
                kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);
                // If publish somehow succeeds, still track it
                log.info("Poison pill {} published (schema allows extreme values)", i);
                sent++;
            } catch (Exception e) {
                // Expected — malformed records trigger serialization / validation errors
                dlqPublisher.send(
                    new ProducerRecord<>(RAW_TELEMETRY, poisonKey, (TelemetryEvent) null),
                    new RuntimeException("Synthetic poison pill #" + i + ": " + e.getMessage())
                );
                log.info("Poison pill {} routed to DLQ: {}", i, e.getMessage());
                sent++;
            }
        }
        return sent;
    }

    private TelemetryEvent createPoisonPill(int index) {
        return TelemetryEvent.newBuilder()
            .setCarId("POISON_" + index)
            .setDriverId("DRV_INVALID")
            .setSessionId("INVALID_SESSION")
            .setTimestampMs(-1L)           // invalid negative timestamp
            .setLap(-999)                  // impossible lap number
            .setSector(99)                 // invalid sector (only 1-3 valid)
            .setSpeedKph(9999.99)          // impossible speed
            .setRpm(-1)                    // invalid RPM
            .setGear(0)
            .setThrottlePct(200.0)         // over 100% throttle
            .setBrakePct(-50.0)            // negative braking
            .setDrsActive(false)
            .setTireCompound(TireCompound.SOFT)
            .setTireTempFl(500.0)          // impossibly hot tire
            .setTireTempFr(500.0)
            .setTireTempRl(500.0)
            .setTireTempRr(500.0)
            .setTireWearFl(999.0)          // over 100% wear
            .setTireWearFr(999.0)
            .setTireWearRl(999.0)
            .setTireWearRr(999.0)
            .setFuelLoadKg(-10.0)          // negative fuel load
            .setEngineTempC(999.0)
            .setErsDeployPct(200.0)
            .setGForceLat(50.0)            // impossible g-force
            .setGForceLon(50.0)
            .build();
    }
}
