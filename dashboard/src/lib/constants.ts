export const DASHBOARD_VERSION = "0.1.0";

/** Default TanStack Query refetch for health endpoints (ms). */
export const HEALTH_REFETCH_MS = 10_000;

/** Operations overview subsystem cards (ms). */
export const OVERVIEW_REFETCH_MS = 30_000;

/** Pipeline simulator status when tab is visible (ms). */
export const SIMULATOR_STATUS_REFETCH_MS = 2_000;

/** Schema Registry subject list on /schemas (ms). */
export const SCHEMAS_SUBJECTS_REFETCH_MS = 45_000;

/** Kafka Connect /cdc dashboard (ms). */
export const CONNECT_DASHBOARD_REFETCH_MS = 20_000;

/** ML `/metrics` polling on /ml (ms). */
export const ML_METRICS_REFETCH_MS = 5_000;

/** ksqlDB `/info` on /analytics (ms). */
export const KSQL_INFO_REFETCH_MS = 60_000;

/** Dashboard BFF topic previews — Pit Wall & stream-aggregates (ms). */
export const BFF_TOPIC_PREVIEW_REFETCH_MS = 8_000;
