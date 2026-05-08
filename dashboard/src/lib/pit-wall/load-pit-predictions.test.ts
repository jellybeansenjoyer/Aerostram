import { beforeEach, describe, expect, it, vi } from "vitest";

import { getPitPredictionsRecent } from "@/lib/api/client";

import { loadPitPredictionsForWall } from "./load-pit-predictions";
import { MOCK_PIT_PREDICTION_ROWS } from "./mock-pit-rows";

vi.mock("@/lib/api/client", () => ({
  getPitPredictionsRecent: vi.fn(),
}));

describe("loadPitPredictionsForWall", () => {
  beforeEach(() => {
    vi.mocked(getPitPredictionsRecent).mockReset();
  });

  it("returns live BFF data when the request succeeds", async () => {
    vi.mocked(getPitPredictionsRecent).mockResolvedValue({
      topic: "pit-predictions",
      limit: 5,
      items: [{ car_id: "live-1", pit_probability: 0.5, recommend_pit: false }],
    });

    const r = await loadPitPredictionsForWall("/svc/bff", 5);

    expect(r.mode).toBe("live");
    expect(r.topic).toBe("pit-predictions");
    expect(r.items).toHaveLength(1);
    expect(r.items[0]?.car_id).toBe("live-1");
  });

  it("falls back to mock rows when the BFF is unreachable", async () => {
    vi.mocked(getPitPredictionsRecent).mockRejectedValue(new Error("ECONNREFUSED"));

    const r = await loadPitPredictionsForWall("/svc/bff", 3);

    expect(r.mode).toBe("mock");
    expect(r.items).toHaveLength(3);
    expect(r.items).toEqual(MOCK_PIT_PREDICTION_ROWS.slice(0, 3));
  });
});
