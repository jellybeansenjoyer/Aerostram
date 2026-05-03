package com.aerostream.enrichment;

import com.aerostream.avro.EnrichedTelemetryEvent;
import com.aerostream.avro.TelemetryEvent;
import com.aerostream.avro.TireCompound;
import org.apache.avro.generic.GenericRecord;

/**
 * Pure static mappers — no Spring dependencies.
 *
 * withCircuit(TelemetryEvent, GenericRecord) — copies all 26 TelemetryEvent fields,
 *   then adds circuit fields if circuit != null (leftJoin may pass null).
 *
 * withDriver(EnrichedTelemetryEvent, GenericRecord) — adds driver fields to the
 *   already circuit-enriched event. If driver is null, returns event unchanged.
 */
public final class EnrichmentMapper {

    private EnrichmentMapper() {}

    public static EnrichedTelemetryEvent withCircuit(TelemetryEvent event, GenericRecord circuit) {
        EnrichedTelemetryEvent.Builder builder = EnrichedTelemetryEvent.newBuilder()
            .setCarId(event.getCarId())
            .setDriverId(event.getDriverId())
            .setSessionId(event.getSessionId())
            .setTimestampMs(event.getTimestampMs())
            .setLap(event.getLap())
            .setSector(event.getSector())
            .setSpeedKph(event.getSpeedKph())
            .setRpm(event.getRpm())
            .setGear(event.getGear())
            .setThrottlePct(event.getThrottlePct())
            .setBrakePct(event.getBrakePct())
            .setDrsActive(event.getDrsActive())
            .setTireCompound(TireCompound.valueOf(event.getTireCompound().name()))
            .setTireTempFl(event.getTireTempFl())
            .setTireTempFr(event.getTireTempFr())
            .setTireTempRl(event.getTireTempRl())
            .setTireTempRr(event.getTireTempRr())
            .setTireWearFl(event.getTireWearFl())
            .setTireWearFr(event.getTireWearFr())
            .setTireWearRl(event.getTireWearRl())
            .setTireWearRr(event.getTireWearRr())
            .setFuelLoadKg(event.getFuelLoadKg())
            .setEngineTempC(event.getEngineTempC())
            .setErsDeployPct(event.getErsDeployPct())
            .setGForceLat(event.getGForceLat())
            .setGForceLon(event.getGForceLon())
            .setEnrichmentTsMs(System.currentTimeMillis())
            .setEnriched(circuit != null);

        if (circuit != null) {
            builder.setCircuitName(str(circuit, "circuit_name"))
                   .setCircuitCountry(str(circuit, "country"))
                   .setCircuitLengthKm(dbl(circuit, "length_km"))
                   .setNumLaps(integer(circuit, "num_laps"))
                   .setDrsZones(integer(circuit, "drs_zones"))
                   .setPitLossTimeS(dbl(circuit, "pit_loss_time_s"));
        }

        return builder.build();
    }

    public static EnrichedTelemetryEvent withDriver(EnrichedTelemetryEvent event, GenericRecord driver) {
        if (driver == null) return event;

        return EnrichedTelemetryEvent.newBuilder(event)
            .setDriverFullName(str(driver, "full_name"))
            .setDriverAbbrev(str(driver, "abbreviated_name"))
            .setDriverTeam(str(driver, "team_id"))
            .setDriverNationality(str(driver, "nationality"))
            .setEnriched(true)
            .build();
    }

    private static String str(GenericRecord r, String field) {
        Object v = r.get(field);
        return v != null ? v.toString() : null;
    }

    private static Double dbl(GenericRecord r, String field) {
        Object v = r.get(field);
        return v instanceof Number n ? n.doubleValue() : null;
    }

    private static Integer integer(GenericRecord r, String field) {
        Object v = r.get(field);
        return v instanceof Number n ? n.intValue() : null;
    }
}
