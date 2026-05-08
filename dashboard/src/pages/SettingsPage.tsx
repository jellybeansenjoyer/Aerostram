import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { useServices } from "@/context/ServicesContext";
import type { ServicesSettings } from "@/lib/settings-schema";
import { defaultServicesSettings } from "@/lib/settings-schema";

const FIELDS: { key: keyof ServicesSettings; label: string; hint?: string }[] = [
  { key: "producerUrl", label: "Producer base URL" },
  { key: "streamProcessorUrl", label: "Stream processor URL" },
  { key: "schemaRegistryUrl", label: "Schema Registry URL" },
  { key: "kafkaConnectUrl", label: "Kafka Connect URL" },
  { key: "ksqlUrl", label: "ksqlDB URL" },
  { key: "mlConsumerUrl", label: "ML consumer URL" },
  { key: "prometheusUrl", label: "Prometheus URL" },
  { key: "kafkaUiUrl", label: "Kafka UI URL" },
  { key: "grafanaUrl", label: "Grafana URL" },
  {
    key: "bffUrl",
    label: "BFF base URL (optional)",
    hint: "Leave empty to use same-origin /svc/bff (Vite proxies to localhost:8089). Or set http://localhost:8089.",
  },
];

export function SettingsPage() {
  const { settings, commitSettings, resetToDefaults, fieldErrors, clearFieldErrors } =
    useServices();
  const [draft, setDraft] = useState<ServicesSettings>(settings);

  useEffect(() => {
    setDraft(settings);
  }, [settings]);

  const onSave = () => {
    commitSettings(draft);
  };

  const onReset = () => {
    resetToDefaults();
    setDraft(defaultServicesSettings);
  };

  return (
    <Card className="max-w-2xl">
      <CardHeader>
        <CardTitle>Service URLs</CardTitle>
        <CardDescription>
          Stored in <code className="rounded bg-muted px-1 font-mono text-xs">localStorage</code>{" "}
          (<span className="font-mono text-xs">aerostream.settings.v1</span>). Use valid{" "}
          <code className="font-mono text-xs">http://</code> or{" "}
          <code className="font-mono text-xs">https://</code> URLs.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {FIELDS.map(({ key, label, hint }) => (
          <div key={key} className="space-y-1.5">
            <label htmlFor={key} className="text-sm font-medium">
              {label}
            </label>
            <Input
              id={key}
              value={draft[key]}
              onChange={(e) => {
                clearFieldErrors();
                setDraft((d) => ({ ...d, [key]: e.target.value }));
              }}
              aria-invalid={Boolean(fieldErrors[key])}
              className={fieldErrors[key] ? "border-red-500 focus-visible:ring-red-500" : undefined}
              placeholder={
                key === "bffUrl"
                  ? "Optional — leave empty"
                  : String(defaultServicesSettings[key])
              }
              autoComplete="off"
            />
            {fieldErrors[key] ? (
              <p className="text-xs text-red-600 dark:text-red-400">{fieldErrors[key]}</p>
            ) : hint ? (
              <p className="text-xs text-muted-foreground">{hint}</p>
            ) : null}
          </div>
        ))}
      </CardContent>
      <CardFooter className="flex flex-wrap gap-2 border-t border-border pt-6">
        <Button type="button" onClick={onSave}>
          Save
        </Button>
        <Button type="button" variant="outline" onClick={onReset}>
          Reset to defaults
        </Button>
      </CardFooter>
    </Card>
  );
}
