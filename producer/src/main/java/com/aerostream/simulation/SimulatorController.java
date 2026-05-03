package com.aerostream.simulation;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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

    @GetMapping("/status")
    public ResponseEntity<SimulatorStatus> status() {
        return ResponseEntity.ok(simulator.getStatus());
    }
}
