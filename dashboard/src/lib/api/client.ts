import type {
  ConnectConnectorStatusResponse,
  ConnectConnectorsResponse,
  KsqlInfoResponse,
  MlHealthResponse,
  MlReadyResponse,
  PitPredictionsRecentResponse,
  RegistryLatestVersionResponse,
  RegistrySubjectsResponse,
  SimulatorStatus,
  StreamAggregatesRecentResponse,
} from "@/lib/api/types";

/** Spring Boot Actuator health JSON (subset). */
export type ActuatorHealth = {
  status: string;
  components?: Record<
    string,
    {
      status?: string;
      details?: Record<string, unknown>;
    }
  >;
};

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
    public readonly body?: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

/**
 * Fetch JSON from same-origin paths (use `/svc/...` + Vite proxy in dev).
 */
export async function getJson<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: {
      Accept: "application/json",
      ...init?.headers,
    },
  });
  const text = await res.text();
  if (!res.ok) {
    throw new ApiError(res.status, res.statusText || "Request failed", text);
  }
  if (!text) return {} as T;
  try {
    return JSON.parse(text) as T;
  } catch {
    throw new ApiError(res.status, "Invalid JSON response", text);
  }
}

/** Empty string → same-origin `/svc/bff` (Vite proxy in dev). */
export function resolveBffBase(bffUrl: string): string {
  const t = bffUrl.trim();
  if (t) return t.replace(/\/$/, "");
  return "/svc/bff";
}

export async function getPitPredictionsRecent(
  base: string,
  limit?: number,
): Promise<PitPredictionsRecentResponse> {
  const q = limit != null && limit > 0 ? `?limit=${limit}` : "";
  return getJson<PitPredictionsRecentResponse>(`${base}/api/v1/pit-predictions/recent${q}`);
}

export async function getStreamAggregatesRecent(
  base: string,
  limit?: number,
): Promise<StreamAggregatesRecentResponse> {
  const q = limit != null && limit > 0 ? `?limit=${limit}` : "";
  return getJson<StreamAggregatesRecentResponse>(`${base}/api/v1/stream-aggregates/recent${q}`);
}

export async function getRegistrySubjects(): Promise<RegistrySubjectsResponse> {
  return getJson<RegistrySubjectsResponse>("/svc/registry/subjects");
}

export async function getRegistryLatestVersion(
  subject: string,
): Promise<RegistryLatestVersionResponse> {
  const enc = encodeURIComponent(subject);
  return getJson<RegistryLatestVersionResponse>(`/svc/registry/subjects/${enc}/versions/latest`);
}

export async function getConnectConnectors(): Promise<ConnectConnectorsResponse> {
  return getJson<ConnectConnectorsResponse>("/svc/connect/connectors");
}

export async function getKsqlInfo(): Promise<KsqlInfoResponse> {
  return getJson<KsqlInfoResponse>("/svc/ksql/info");
}

export async function getMlHealth(): Promise<MlHealthResponse> {
  return getJson<MlHealthResponse>("/svc/ml/health");
}

/** ML readiness may return 503 JSON — does not throw on non-OK status. */
export async function fetchMlReady(): Promise<{ httpOk: boolean; data: MlReadyResponse }> {
  const res = await fetch("/svc/ml/ready", {
    headers: { Accept: "application/json" },
  });
  const text = await res.text();
  let data: MlReadyResponse = {};
  if (text) {
    try {
      data = JSON.parse(text) as MlReadyResponse;
    } catch {
      /* ignore malformed body */
    }
  }
  return { httpOk: res.ok, data };
}

export async function fetchMlOverview(): Promise<{
  health: MlHealthResponse;
  readyOk: boolean;
  readyBody: MlReadyResponse;
}> {
  const health = await getMlHealth();
  const { httpOk, data } = await fetchMlReady();
  return { health, readyOk: httpOk, readyBody: data };
}

export async function postSimulatorStart(): Promise<void> {
  await postSimulatorAction("start");
}

export async function postSimulatorStop(): Promise<void> {
  await postSimulatorAction("stop");
}

/** BE-SIM-1 — runtime EPS without container restart (JSON body — reliable through proxies) */
export async function postSimulatorRate(eventsPerSecond: number): Promise<SimulatorStatus> {
  const body = JSON.stringify({
    eventsPerSecond: Math.max(1, Math.round(eventsPerSecond)),
  });
  const res = await fetch(`/svc/producer/api/simulator/rate`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body,
  });
  const text = await res.text();
  if (!res.ok) {
    throw new ApiError(res.status, res.statusText || "Rate update failed", text);
  }
  if (!text) {
    throw new ApiError(res.status, "Empty response from simulator rate", text);
  }
  try {
    return JSON.parse(text) as SimulatorStatus;
  } catch {
    throw new ApiError(res.status, "Invalid JSON from simulator rate", text);
  }
}

async function postSimulatorAction(action: "start" | "stop"): Promise<void> {
  const res = await fetch(`/svc/producer/api/simulator/${action}`, {
    method: "POST",
  });
  const text = await res.text();
  if (!res.ok) {
    throw new ApiError(res.status, res.statusText || "POST failed", text);
  }
}

export async function getSimulatorStatus(): Promise<SimulatorStatus> {
  return getJson<SimulatorStatus>("/svc/producer/api/simulator/status");
}

export async function getConnectConnectorStatus(
  name: string,
): Promise<ConnectConnectorStatusResponse> {
  const enc = encodeURIComponent(name);
  return getJson<ConnectConnectorStatusResponse>(`/svc/connect/connectors/${enc}/status`);
}

/** Connector names plus each `/status` payload (for `/cdc` dashboard). */
export async function fetchConnectDashboard(): Promise<
  { name: string; status: ConnectConnectorStatusResponse }[]
> {
  const names = await getConnectConnectors();
  return Promise.all(
    names.map(async (name) => ({
      name,
      status: await getConnectConnectorStatus(name),
    })),
  );
}

/**
 * ksqlDB REST `POST /ksql` — same JSON shape as `infra/scripts/deploy-ksql-queries.sh`.
 */
export async function postKsqlStatement(ksql: string): Promise<unknown> {
  const res = await fetch("/svc/ksql/ksql", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify({
      ksql,
      streamsProperties: {
        "ksql.streams.auto.offset.reset": "earliest",
      },
    }),
  });
  const text = await res.text();
  if (!res.ok) {
    throw new ApiError(res.status, res.statusText || "ksql request failed", text);
  }
  if (!text) return [];
  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw new ApiError(res.status, "Invalid JSON from ksqlDB", text);
  }
}

export async function getMlMetricsText(): Promise<string> {
  const res = await fetch("/svc/ml/metrics", {
    headers: { Accept: "text/plain" },
  });
  const text = await res.text();
  if (!res.ok) {
    throw new ApiError(res.status, res.statusText || "metrics failed", text);
  }
  return text;
}
