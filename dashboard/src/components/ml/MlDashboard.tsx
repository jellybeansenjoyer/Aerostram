import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import {
  fetchMlReady,
  getMlHealth,
  getMlMetricsText,
} from "@/lib/api/client";
import { ML_METRICS_REFETCH_MS } from "@/lib/constants";
import { useErrorToast } from "@/hooks/use-error-toast";
import { parsePrometheusCounters } from "@/lib/prometheus";
import { Sparkline } from "@/components/ml/Sparkline";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

const HISTORY_LEN = 72;

const COUNTER_DEFS = [
  { key: "ml_consumer_messages_consumed_total", label: "Messages consumed" },
  { key: "ml_consumer_predictions_emitted_total", label: "Predictions emitted" },
  { key: "ml_consumer_errors_total", label: "Errors" },
] as const;

type History = { msg: number[]; pred: number[]; err: number[] };

const emptyHistory = (): History => ({ msg: [], pred: [], err: [] });

function formatInt(n: number) {
  return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
}

export function MlDashboard() {
  const [hist, setHist] = useState<History>(emptyHistory);

  const healthQ = useQuery({
    queryKey: ["ml", "page", "health"],
    queryFn: getMlHealth,
    refetchInterval: ML_METRICS_REFETCH_MS,
  });
  const readyQ = useQuery({
    queryKey: ["ml", "page", "ready"],
    queryFn: () => fetchMlReady(),
    refetchInterval: ML_METRICS_REFETCH_MS,
  });
  const metricsQ = useQuery({
    queryKey: ["ml", "page", "metrics"],
    queryFn: getMlMetricsText,
    refetchInterval: ML_METRICS_REFETCH_MS,
  });

  useErrorToast(healthQ.isError, "ML /health", healthQ.error);
  useErrorToast(readyQ.isError, "ML /ready", readyQ.error);
  useErrorToast(metricsQ.isError, "ML /metrics", metricsQ.error);

  useEffect(() => {
    if (!metricsQ.data) return;
    const c = parsePrometheusCounters(metricsQ.data, "ml_consumer");
    setHist((h) => ({
      msg: [...h.msg, c.ml_consumer_messages_consumed_total ?? 0].slice(-HISTORY_LEN),
      pred: [...h.pred, c.ml_consumer_predictions_emitted_total ?? 0].slice(-HISTORY_LEN),
      err: [...h.err, c.ml_consumer_errors_total ?? 0].slice(-HISTORY_LEN),
    }));
  }, [metricsQ.data]);

  const lastCounters = metricsQ.data
    ? parsePrometheusCounters(metricsQ.data, "ml_consumer")
    : null;

  const historyFor = (i: 0 | 1 | 2) => (i === 0 ? hist.msg : i === 1 ? hist.pred : hist.err);

  return (
    <div className="space-y-6">
      <div className="grid gap-4 sm:grid-cols-2">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Health</CardTitle>
            <CardDescription>GET /svc/ml/health</CardDescription>
          </CardHeader>
          <CardContent>
            {healthQ.isPending ? (
              <div className="h-8 w-32 animate-pulse rounded bg-muted" />
            ) : healthQ.isError ? (
              <p className="text-sm text-red-600 dark:text-red-400">Unavailable</p>
            ) : (
              <p
                className={cn(
                  "font-mono text-lg font-semibold",
                  healthQ.data?.status?.toLowerCase() === "healthy"
                    ? "text-emerald-700 dark:text-emerald-400"
                    : "text-amber-700 dark:text-amber-400",
                )}
              >
                {healthQ.data?.status ?? "—"}
              </p>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Ready</CardTitle>
            <CardDescription>GET /svc/ml/ready (503 until Kafka consumer assigned)</CardDescription>
          </CardHeader>
          <CardContent>
            {readyQ.isPending ? (
              <div className="h-8 w-32 animate-pulse rounded bg-muted" />
            ) : readyQ.isError ? (
              <p className="text-sm text-red-600 dark:text-red-400">Unavailable</p>
            ) : (
              <p
                className={cn(
                  "font-mono text-lg font-semibold",
                  readyQ.data?.httpOk ? "text-emerald-700 dark:text-emerald-400" : "text-amber-700 dark:text-amber-400",
                )}
              >
                {readyQ.data?.httpOk ? "ready" : readyQ.data?.data.status ?? "not_ready"}
              </p>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="text-sm text-muted-foreground">
          Prometheus text from <code className="font-mono text-xs">/svc/ml/metrics</code> — refreshed every{" "}
          {ML_METRICS_REFETCH_MS / 1000}s.
        </p>
        <Button type="button" variant="outline" size="sm" onClick={() => setHist(emptyHistory())}>
          Reset sparklines
        </Button>
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        {COUNTER_DEFS.map((def, i) => {
          const v = lastCounters?.[def.key];
          return (
            <Card key={def.key}>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">{def.label}</CardTitle>
                <CardDescription className="font-mono text-xs">{def.key}</CardDescription>
              </CardHeader>
              <CardContent className="space-y-3">
                <p className="font-display text-3xl font-semibold tabular-nums">
                  {v !== undefined ? formatInt(v) : metricsQ.isPending ? "…" : "—"}
                </p>
                <Sparkline values={historyFor(i as 0 | 1 | 2)} />
              </CardContent>
            </Card>
          );
        })}
      </div>
    </div>
  );
}
