import type { PitPredictionRow } from "@/lib/api/types";

/**
 * Placeholder data when the dashboard BFF is offline (FE-10).
 * Shape matches `PitPrediction` / BFF JSON (no Kafka metadata).
 */
export const MOCK_PIT_PREDICTION_ROWS: PitPredictionRow[] = [
  {
    car_id: "mock-01",
    timestamp_ms: 1_700_000_000_000,
    pit_probability: 0.84,
    recommend_pit: true,
    model_version: "mock-rf-1.0.0",
  },
  {
    car_id: "mock-02",
    timestamp_ms: 1_700_000_030_000,
    pit_probability: 0.22,
    recommend_pit: false,
    model_version: "mock-rf-1.0.0",
  },
  {
    car_id: "mock-03",
    timestamp_ms: 1_700_000_060_000,
    pit_probability: 0.91,
    recommend_pit: true,
    model_version: "mock-rf-1.0.0",
  },
  {
    car_id: "mock-04",
    timestamp_ms: 1_700_000_090_000,
    pit_probability: 0.41,
    recommend_pit: false,
    model_version: "mock-rf-1.0.0",
  },
];
