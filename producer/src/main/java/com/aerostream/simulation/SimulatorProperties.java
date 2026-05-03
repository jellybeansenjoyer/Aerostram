package com.aerostream.simulation;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "simulator")
public class SimulatorProperties {

    /** Target events per second across all cars. */
    private int eventsPerSecond = 500;

    /** Number of simulated cars (default 20). */
    private int numCars = 20;

    /** Race session identifier embedded in every event. */
    private String sessionId = "RACE_2024_MONZA_R1";

    /** If true, simulator starts automatically on ApplicationReadyEvent. */
    private boolean enabled = false;
}
