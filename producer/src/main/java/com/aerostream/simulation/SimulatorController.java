package com.aerostream.simulation;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/simulator")
public class SimulatorController {

    private final TelemetrySimulator simulator;

    public SimulatorController(TelemetrySimulator simulator) {
        this.simulator = simulator;
    }

    @PostMapping("/start")
    public ResponseEntity<String> start() {
        simulator.start();
        return ResponseEntity.ok("Simulator started");
    }

    @PostMapping("/stop")
    public ResponseEntity<String> stop() {
        simulator.stop();
        return ResponseEntity.ok("Simulator stopped");
    }

    /**
     * Runtime rate change (BE-SIM-1) — JSON body (preferred for dashboards / proxies).
     */
    @PostMapping(value = "/rate", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> setRateJson(@RequestBody SetRateRequest body) {
        return applyRate(body.eventsPerSecond());
    }

    /**
     * Same as JSON variant — query param for curl and backward compatibility.
     */
    @PostMapping(value = "/rate", params = "eventsPerSecond")
    public ResponseEntity<?> setRateQuery(@RequestParam("eventsPerSecond") int eventsPerSecond) {
        return applyRate(eventsPerSecond);
    }

    private ResponseEntity<?> applyRate(int eventsPerSecond) {
        try {
            simulator.setTargetEventsPerSecond(eventsPerSecond);
            return ResponseEntity.ok(simulator.getStatus());
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body(Map.of("error", ex.getMessage()));
        }
    }

    @GetMapping("/status")
    public ResponseEntity<SimulatorStatus> status() {
        return ResponseEntity.ok(simulator.getStatus());
    }
}
