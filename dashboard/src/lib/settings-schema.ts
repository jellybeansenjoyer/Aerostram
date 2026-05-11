import { z } from "zod";

const url = (label: string) =>
  z.string().url({ message: `${label} must be a valid http(s) URL` });

/** Persisted service base URLs — matches README defaults & frontend-dashboard-plan §3 */
export const servicesSettingsSchema = z.object({
  producerUrl: url("Producer"),
  streamProcessorUrl: url("Stream processor"),
  schemaRegistryUrl: url("Schema Registry"),
  kafkaConnectUrl: url("Kafka Connect"),
  ksqlUrl: url("ksqlDB"),
  mlConsumerUrl: url("ML consumer"),
  prometheusUrl: url("Prometheus"),
  kafkaUiUrl: url("Kafka UI"),
  grafanaUrl: url("Grafana"),
  bffUrl: z.union([z.literal(""), url("BFF")]),
});

export type ServicesSettings = z.infer<typeof servicesSettingsSchema>;

export const defaultServicesSettings: ServicesSettings = {
  producerUrl: "http://localhost:8090",
  streamProcessorUrl: "http://localhost:8091",
  schemaRegistryUrl: "http://localhost:8081",
  kafkaConnectUrl: "http://localhost:8083",
  ksqlUrl: "http://localhost:8088",
  mlConsumerUrl: "http://localhost:8099",
  prometheusUrl: "http://localhost:9090",
  kafkaUiUrl: "http://localhost:8080",
  grafanaUrl: "http://localhost:3000",
  bffUrl: "",
};

export const STORAGE_KEY = "aerostream.settings.v1";

export function parseStoredSettings(raw: string | null): ServicesSettings {
  if (!raw) return defaultServicesSettings;
  try {
    const json = JSON.parse(raw) as unknown;
    return servicesSettingsSchema.parse(json);
  } catch {
    return defaultServicesSettings;
  }
}

export function validateSettings(
  partial: Partial<ServicesSettings>,
): { success: true; data: ServicesSettings } | { success: false; errors: Record<string, string> } {
  const merged = { ...defaultServicesSettings, ...partial };
  const result = servicesSettingsSchema.safeParse(merged);
  if (result.success) return { success: true, data: result.data };
  const errors: Record<string, string> = {};
  for (const issue of result.error.issues) {
    const key = issue.path[0];
    if (typeof key === "string") errors[key] = issue.message;
  }
  return { success: false, errors };
}

/** Validate full merged object (e.g. after editing multiple fields). */
export function validateMerged(
  merged: ServicesSettings,
):
  | { success: true; data: ServicesSettings }
  | { success: false; errors: Record<string, string> } {
  const result = servicesSettingsSchema.safeParse(merged);
  if (result.success) return { success: true, data: result.data };
  const errors: Record<string, string> = {};
  for (const issue of result.error.issues) {
    const key = issue.path[0];
    if (typeof key === "string") errors[key] = issue.message;
  }
  return { success: false, errors };
}
