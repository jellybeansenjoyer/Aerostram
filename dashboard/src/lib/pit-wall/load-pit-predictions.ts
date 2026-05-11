import { getPitPredictionsRecent } from "@/lib/api/client";
import type { PitPredictionRow } from "@/lib/api/types";

import { MOCK_PIT_PREDICTION_ROWS } from "./mock-pit-rows";

export type PitWallLoadResult = {
  mode: "live" | "mock";
  topic: string;
  items: PitPredictionRow[];
};

/**
 * Load pit predictions from the FE-9 BFF; on any failure, return deterministic mock rows (FE-10).
 */
export async function loadPitPredictionsForWall(
  base: string,
  limit: number,
): Promise<PitWallLoadResult> {
  const lim = Math.max(1, limit);
  try {
    const res = await getPitPredictionsRecent(base, lim);
    return { mode: "live", topic: res.topic, items: res.items };
  } catch {
    return {
      mode: "mock",
      topic: "pit-predictions",
      items: MOCK_PIT_PREDICTION_ROWS.slice(0, lim),
    };
  }
}
