-- Phase 4 — Hopping windows (30s window, 10s advance) ≈ overlapping sliding analysis.
-- Pace + tire summaries per car_id → Kafka topic stream-aggregates (JSON values).

CREATE TABLE AGGREGATE_METRICS WITH (
  KAFKA_TOPIC='stream-aggregates',
  VALUE_FORMAT='JSON',
  PARTITIONS=10,
  REPLICAS=3
) AS
SELECT
  car_id,
  WINDOWSTART AS window_start_ms,
  WINDOWEND AS window_end_ms,
  AVG(speed_kph) AS avg_speed_kph,
  MAX(speed_kph) AS max_speed_kph,
  AVG((tire_temp_fl + tire_temp_fr + tire_temp_rl + tire_temp_rr) / 4.0) AS avg_tire_temp_c,
  AVG((tire_wear_fl + tire_wear_fr + tire_wear_rl + tire_wear_rr) / 4.0) AS avg_tire_wear_pct,
  COUNT(*) AS event_count
FROM ENRICHED_EVENTS
WINDOW HOPPING (SIZE 30 SECONDS, ADVANCE BY 10 SECONDS)
WHERE enriched = true
GROUP BY car_id
EMIT CHANGES;
