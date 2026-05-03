package com.aerostream.simulation;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SimulatorStatus {

    private boolean running;
    private int activeCarCount;
    private long totalPublished;
    private long totalDlqRouted;
    private int eventsPerSecond;
    private double currentThroughput;
}
