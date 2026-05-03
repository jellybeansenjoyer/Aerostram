package com.aerostream.simulation;

import com.aerostream.avro.TelemetryEvent;
import com.aerostream.model.CarState;

/**
 * Maps a CarState (mutable simulation state) to an immutable Avro TelemetryEvent.
 * Called once per car per simulation tick before publishing to Kafka.
 */
public final class TelemetryEventMapper {

    private TelemetryEventMapper() {}

    public static TelemetryEvent toAvro(CarState car) {
        return TelemetryEvent.newBuilder()
            .setCarId(car.getCarId())
            .setDriverId(car.getDriverId())
            .setSessionId(car.getSessionId())
            .setTimestampMs(System.currentTimeMillis())
            .setLap(car.getLap())
            .setSector(car.getSector())
            .setSpeedKph(round2(car.getSpeedKph()))
            .setRpm(car.getRpm())
            .setGear(car.getGear())
            .setThrottlePct(round2(car.getThrottlePct()))
            .setBrakePct(round2(car.getBrakePct()))
            .setDrsActive(car.isDrsActive())
            .setTireCompound(car.getTireCompound())
            .setTireTempFl(round2(car.getTireTempFl()))
            .setTireTempFr(round2(car.getTireTempFr()))
            .setTireTempRl(round2(car.getTireTempRl()))
            .setTireTempRr(round2(car.getTireTempRr()))
            .setTireWearFl(round2(car.getTireWearFl()))
            .setTireWearFr(round2(car.getTireWearFr()))
            .setTireWearRl(round2(car.getTireWearRl()))
            .setTireWearRr(round2(car.getTireWearRr()))
            .setFuelLoadKg(round2(car.getFuelLoadKg()))
            .setEngineTempC(round2(car.getEngineTempC()))
            .setErsDeployPct(round2(car.getErsDeployPct()))
            .setGForceLat(round2(car.getGForceLat()))
            .setGForceLon(round2(car.getGForceLon()))
            .build();
    }

    private static double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
