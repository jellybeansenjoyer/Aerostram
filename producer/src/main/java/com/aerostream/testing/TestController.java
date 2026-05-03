package com.aerostream.testing;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;

/**
 * Test-only endpoints for exercising the DLQ and error handling paths.
 * Excluded from production profile via @Profile("!production").
 */
@RestController
@RequestMapping("/api/test")
@Profile("!production")
@RequiredArgsConstructor
public class TestController {

    private final PoisonPillGenerator generator;

    /**
     * Send N deliberately malformed events to trigger DLQ routing.
     * Usage: POST /api/test/poison-pill?count=5
     */
    @PostMapping("/poison-pill")
    public ResponseEntity<Map<String, Object>> poisonPill(
            @RequestParam(defaultValue = "5") int count) {
        int sent = generator.generateAndSend(count);
        return ResponseEntity.ok(Map.of(
            "requested", count,
            "sent", sent,
            "dlqTopic", "dlq-telemetry",
            "timestamp", Instant.now().toString()
        ));
    }
}
