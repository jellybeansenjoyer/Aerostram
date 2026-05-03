package com.aerostream.simulation;

import com.aerostream.avro.TireCompound;
import com.aerostream.model.CarState;
import org.springframework.stereotype.Component;

import java.util.Random;

/**
 * Advances a car's physics state by one simulation tick (~50ms of virtual race time).
 *
 * Models: speed curves with braking zones, tire heat-up/cool-down, tire wear by compound,
 * fuel burn, engine temperature, g-forces, DRS activation, lap/sector progression.
 */
@Component
public class PhysicsEngine {

    private static final double FUEL_BURN_PER_TICK = 0.000028;  // ~2 kg/lap at 70 ticks/lap
    private static final double TIRE_WEAR_PER_TICK = 0.0004;
    private static final double TIRE_HEAT_RATE     = 0.3;
    private static final double TIRE_COOL_RATE     = 0.1;
    private static final double OPTIMAL_TIRE_TEMP  = 95.0;
    private static final double CIRCUIT_LENGTH_KM  = 5.0;
    private static final double TICK_SECONDS       = 0.05;      // 50 ms per tick

    private final Random random = new Random();

    public void tick(CarState car) {
        advanceTrackPosition(car);
        updateSpeed(car);
        updateRpm(car);
        updateTireTemperatures(car);
        updateTireWear(car);
        updateFuel(car);
        updateEngineTemp(car);
        updateGForces(car);
        updateDrs(car);
        updateErs(car);
        advanceLapSector(car);
    }

    private void advanceTrackPosition(CarState car) {
        double distancePerTick = car.getSpeedKph() / 3600.0 * TICK_SECONDS;
        car.setTrackPosition((car.getTrackPosition() + distancePerTick / CIRCUIT_LENGTH_KM) % 1.0);
    }

    private void updateSpeed(CarState car) {
        boolean inBrakingZone = isInBrakingZone(car.getTrackPosition());
        if (inBrakingZone) {
            car.setSpeedKph(Math.max(80, car.getSpeedKph() - 15 + random.nextGaussian() * 2));
            car.setThrottlePct(0);
            car.setBrakePct(80 + random.nextDouble() * 20);
        } else {
            car.setSpeedKph(Math.min(335, car.getSpeedKph() + 8 + random.nextGaussian() * 1.5));
            car.setThrottlePct(90 + random.nextDouble() * 10);
            car.setBrakePct(0);
        }
        car.setOnBrakingZone(inBrakingZone);
    }

    private boolean isInBrakingZone(double pos) {
        return (pos > 0.18 && pos < 0.22)
            || (pos > 0.43 && pos < 0.47)
            || (pos > 0.68 && pos < 0.72)
            || (pos > 0.83 && pos < 0.87);
    }

    private void updateRpm(CarState car) {
        int targetRpm = (int) (car.getSpeedKph() * 50 + random.nextGaussian() * 200);
        car.setRpm(Math.max(800, Math.min(18000, targetRpm)));
        car.setGear(Math.min(8, Math.max(1, (int) (car.getSpeedKph() / 45) + 1)));
    }

    private void updateTireTemperatures(CarState car) {
        double speedFactor = car.getSpeedKph() / 300.0;
        double heatInput  = speedFactor * TIRE_HEAT_RATE + random.nextGaussian() * 0.1;
        // Cooling rate proportional to temperature delta from optimal
        double coolInput  = TIRE_COOL_RATE * (car.getTireTempFl() - OPTIMAL_TIRE_TEMP) / OPTIMAL_TIRE_TEMP;
        double delta      = heatInput - coolInput;

        car.setTireTempFl(clamp(car.getTireTempFl() + delta,        60, 130));
        car.setTireTempFr(clamp(car.getTireTempFr() + delta * 1.05, 60, 130));  // front-right slightly hotter
        car.setTireTempRl(clamp(car.getTireTempRl() + delta * 0.9,  60, 130));
        car.setTireTempRr(clamp(car.getTireTempRr() + delta * 0.95, 60, 130));
    }

    private void updateTireWear(CarState car) {
        double compoundMultiplier = switch (car.getTireCompound()) {
            case SOFT  -> 1.4;
            case HARD  -> 0.7;
            case INTER -> 0.9;
            case WET   -> 0.6;
            default    -> 1.0;
        };
        double wearRate = TIRE_WEAR_PER_TICK * (car.getSpeedKph() / 200.0) * compoundMultiplier;
        car.setTireWearFl(Math.min(100, car.getTireWearFl() + wearRate + random.nextDouble() * 0.0001));
        car.setTireWearFr(Math.min(100, car.getTireWearFr() + wearRate));
        car.setTireWearRl(Math.min(100, car.getTireWearRl() + wearRate * 0.85));
        car.setTireWearRr(Math.min(100, car.getTireWearRr() + wearRate * 0.85));
    }

    private void updateFuel(CarState car) {
        car.setFuelLoadKg(Math.max(0, car.getFuelLoadKg() - FUEL_BURN_PER_TICK));
    }

    private void updateEngineTemp(CarState car) {
        double target = 90 + car.getSpeedKph() * 0.05;
        car.setEngineTempC(car.getEngineTempC() + (target - car.getEngineTempC()) * 0.01
                           + random.nextGaussian() * 0.2);
    }

    private void updateGForces(CarState car) {
        if (car.isOnBrakingZone()) {
            car.setGForceLat(0.2 + random.nextGaussian() * 0.1);
            car.setGForceLon(-4.5 + random.nextGaussian() * 0.3);
        } else {
            car.setGForceLat(2.5 + random.nextGaussian() * 0.5);
            car.setGForceLon(1.2 + random.nextGaussian() * 0.2);
        }
    }

    private void updateDrs(CarState car) {
        car.setDrsActive(car.getSpeedKph() > 280 && !car.isOnBrakingZone());
    }

    private void updateErs(CarState car) {
        // ERS deployment high under acceleration, off in braking zones
        double target = car.isOnBrakingZone() ? 0 : 60 + random.nextDouble() * 40;
        car.setErsDeployPct(clamp(car.getErsDeployPct() + (target - car.getErsDeployPct()) * 0.1, 0, 100));
    }

    private void advanceLapSector(CarState car) {
        int newSector = Math.min(3, (int) (car.getTrackPosition() * 3) + 1);
        car.setSector(newSector);
        // New lap when crossing start/finish (position wraps through ~0)
        if (car.getTrackPosition() < 0.01 && newSector == 1 && car.getLap() > 0) {
            car.setLap(car.getLap() + 1);
        }
    }

    private double clamp(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }
}
