package com.aerostream.simulation;

/**
 * JSON body for {@code POST /api/simulator/rate} (BE-SIM-1).
 */
public record SetRateRequest(int eventsPerSecond) {}
