package com.aerostream.simulation;

import com.aerostream.avro.TelemetryEvent;
import com.aerostream.avro.TireCompound;
import com.aerostream.model.CarState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Slf4j
public class TelemetrySimulator {

    private final PhysicsEngine physicsEngine;
    private final KafkaTemplate<String, TelemetryEvent> kafkaTemplate;
    private final SimulatorProperties props;
    private final Counter eventsPublishedCounter;
    private final AtomicLong activeCarsGauge = new AtomicLong(0);

    @Value("${kafka.topics.raw-telemetry:raw-telemetry}")
    private String rawTelemetryTopic;

    private final List<CarState> cars = new ArrayList<>();
    private ScheduledExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong totalPublished = new AtomicLong(0);
    private final AtomicLong totalDlqRouted = new AtomicLong(0);

    // Injected in PROD-5 to avoid circular dependency at construction time
    private volatile com.aerostream.kafka.DlqPublisher dlqPublisher;

    public TelemetrySimulator(PhysicsEngine physicsEngine,
                              KafkaTemplate<String, TelemetryEvent> kafkaTemplate,
                              SimulatorProperties props,
                              MeterRegistry meterRegistry) {
        this.physicsEngine = physicsEngine;
        this.kafkaTemplate = kafkaTemplate;
        this.props = props;

        this.eventsPublishedCounter = Counter.builder("telemetry.events.published")
            .description("Total telemetry events successfully published to Kafka")
            .tag("topic", "raw-telemetry")
            .register(meterRegistry);

        Gauge.builder("telemetry.active.cars", activeCarsGauge, AtomicLong::get)
            .description("Number of cars currently being simulated")
            .register(meterRegistry);
    }

    public void setDlqPublisher(com.aerostream.kafka.DlqPublisher dlqPublisher) {
        this.dlqPublisher = dlqPublisher;
    }

    public void incrementDlqCount() {
        totalDlqRouted.incrementAndGet();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (props.isEnabled()) {
            log.info("Auto-starting simulator (simulator.enabled=true)");
            start();
        }
    }

    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            initCars();
            activeCarsGauge.set(cars.size());

            // Compute delay between ticks: 1_000_000 µs / (events/sec / cars)
            long tickIntervalMicros = Math.max(1, 1_000_000L / Math.max(1, props.getEventsPerSecond() / cars.size()));
            executor = Executors.newScheduledThreadPool(4);
            executor.scheduleAtFixedRate(this::tickAll, 0, tickIntervalMicros, TimeUnit.MICROSECONDS);

            log.info("Simulator started: {} cars, target {}/sec, tick interval {}µs",
                cars.size(), props.getEventsPerSecond(), tickIntervalMicros);
        } else {
            log.warn("Simulator already running — ignoring start request");
        }
    }

    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            executor.shutdownNow();
            cars.clear();
            activeCarsGauge.set(0);
            log.info("Simulator stopped. Total published: {}, DLQ routed: {}",
                totalPublished.get(), totalDlqRouted.get());
        }
    }

    private void tickAll() {
        for (CarState car : cars) {
            physicsEngine.tick(car);
            TelemetryEvent event = TelemetryEventMapper.toAvro(car);

            kafkaTemplate.send(rawTelemetryTopic, car.getCarId(), event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        eventsPublishedCounter.increment();
                        totalPublished.incrementAndGet();
                    } else {
                        log.error("Publish failed for {}: {}", car.getCarId(), ex.getMessage());
                        if (dlqPublisher != null) {
                            dlqPublisher.sendRaw(car.getCarId(), rawTelemetryTopic, ex);
                            totalDlqRouted.incrementAndGet();
                        }
                    }
                });
        }
    }

    private void initCars() {
        cars.clear();
        TireCompound[] compounds = {TireCompound.SOFT, TireCompound.MEDIUM, TireCompound.HARD};
        for (int i = 1; i <= props.getNumCars(); i++) {
            cars.add(CarState.builder()
                .carId(String.format("CAR_%02d", i))
                .driverId(String.format("DRV_%02d", i))
                .sessionId(props.getSessionId())
                .tireCompound(compounds[(i - 1) % 3])
                .speedKph(200 + i * 3.0)
                .rpm(12000)
                .gear(5)
                .throttlePct(90)
                .brakePct(0)
                .drsActive(false)
                .tireTempFl(80.0).tireTempFr(80.0)
                .tireTempRl(80.0).tireTempRr(80.0)
                .tireWearFl(0).tireWearFr(0)
                .tireWearRl(0).tireWearRr(0)
                .fuelLoadKg(110.0)
                .engineTempC(90.0)
                .ersDeployPct(50.0)
                .gForceLat(0).gForceLon(0)
                .lap(1).sector(1)
                .trackPosition(i / (double) props.getNumCars())  // staggered start positions
                .onBrakingZone(false)
                .build());
        }
    }

    public SimulatorStatus getStatus() {
        return SimulatorStatus.builder()
            .running(running.get())
            .activeCarCount(cars.size())
            .totalPublished(totalPublished.get())
            .totalDlqRouted(totalDlqRouted.get())
            .eventsPerSecond(props.getEventsPerSecond())
            .build();
    }
}
