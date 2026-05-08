/** Schema Registry `GET /subjects/{subject}/versions/latest` */
export type RegistryLatestVersionResponse = {
  subject: string;
  version: number;
  id: number;
  schemaType?: string;
  /** Stringified Avro/JSON/Protobuf schema (format depends on schemaType). */
  schema: string;
  references?: unknown;
};

/** Schema Registry `GET /subjects` — array of subject names. */
export type RegistrySubjectsResponse = string[];

/** Kafka Connect `GET /connectors` — connector names. */
export type ConnectConnectorsResponse = string[];

/** Kafka Connect `GET /connectors/{name}/status`. */
export type ConnectConnectorStatusResponse = {
  name?: string;
  connector?: { state?: string; worker_id?: string; trace?: string };
  tasks?: Array<{ id?: number; state?: string; worker_id?: string; trace?: string }>;
};

/** Parsed row from `SHOW QUERIES` REST JSON (best-effort). */
export type KsqlQueryRow = {
  id: string;
  queryString: string;
  statusSummary?: string;
};

/** ksqlDB `GET /info` — Confluent server metadata. */
export type KsqlInfoResponse = {
  KsqlServerInfo?: {
    version?: string;
    ksqlServiceId?: string;
    kafkaClusterId?: string;
  };
};

export type MlHealthResponse = {
  status?: string;
};

export type MlReadyResponse = {
  status?: string;
};

export type SimulatorStatus = {
  running: boolean;
  activeCarCount: number;
  totalPublished: number;
  totalDlqRouted?: number;
  eventsPerSecond?: number;
  currentThroughput?: number;
};

/** Dashboard BFF — recent pit predictions (Avro → JSON). */
export type PitPredictionRow = {
  car_id?: string;
  timestamp_ms?: number;
  pit_probability?: number;
  recommend_pit?: boolean;
  model_version?: string;
  _kafka_partition?: number;
  _kafka_offset?: number;
};

export type PitPredictionsRecentResponse = {
  topic: string;
  limit: number;
  items: PitPredictionRow[];
};

/** Dashboard BFF — ksql `stream-aggregates` JSON snapshots. */
export type StreamAggregateRow = {
  car_id?: string;
  window_start_ms?: number;
  window_end_ms?: number;
  avg_speed_kph?: number;
  max_speed_kph?: number;
  avg_tire_temp_c?: number;
  avg_tire_wear_pct?: number;
  event_count?: number;
  _kafka_partition?: number;
  _kafka_offset?: number;
};

export type StreamAggregatesRecentResponse = {
  topic: string;
  limit: number;
  items: StreamAggregateRow[];
};
