package com.aerostream.model;

import com.aerostream.avro.TireCompound;
import lombok.Builder;
import lombok.Data;

/**
 * Mutable physics state for a single simulated F1 car.
 * One CarState instance per car, mutated by PhysicsEngine.tick() each simulation cycle.
 */
@Data
@Builder
public class CarState {

    // Identity
    private final String carId;       // "CAR_01" through "CAR_20"
    private final String driverId;    // "DRV_01" through "DRV_20"
    private final String sessionId;

    // Compound
    private TireCompound tireCompound;

    // Drivetrain state
    private double speedKph;
    private int rpm;
    private int gear;
    private double throttlePct;
    private double brakePct;
    private boolean drsActive;

    // Tire state — temperature in Celsius, wear in 0-100%
    private double tireTempFl;
    private double tireTempFr;
    private double tireTempRl;
    private double tireTempRr;
    private double tireWearFl;
    private double tireWearFr;
    private double tireWearRl;
    private double tireWearRr;

    // Vehicle state
    private double fuelLoadKg;
    private double engineTempC;
    private double ersDeployPct;
    private double gForceLat;
    private double gForceLon;

    // Race progress
    private int lap;
    private int sector;

    // Internal simulation state — not published to Kafka
    private double trackPosition;   // 0.0 → 1.0 around the circuit
    private boolean onBrakingZone;
}
