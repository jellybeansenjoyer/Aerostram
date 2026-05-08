import { useQuery } from "@tanstack/react-query";
import { RefreshCw } from "lucide-react";
import {
  ApiError,
  getStreamAggregatesRecent,
  resolveBffBase,
} from "@/lib/api/client";
import { BFF_TOPIC_PREVIEW_REFETCH_MS } from "@/lib/constants";
import { useServices } from "@/context/ServicesContext";
import { useErrorToast } from "@/hooks/use-error-toast";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

const PREVIEW_LIMIT = 40;

function fmtNum(n: number | undefined, digits = 1) {
  if (n == null || Number.isNaN(n)) return "—";
  return n.toLocaleString(undefined, { maximumFractionDigits: digits });
}

export function StreamAggregatesPreview() {
  const { settings } = useServices();
  const base = resolveBffBase(settings.bffUrl);

  const q = useQuery({
    queryKey: ["bff", "stream-aggregates", base, PREVIEW_LIMIT],
    queryFn: () => getStreamAggregatesRecent(base, PREVIEW_LIMIT),
    refetchInterval: BFF_TOPIC_PREVIEW_REFETCH_MS,
  });

  useErrorToast(q.isError, "BFF stream aggregates", q.error);

  const errBody = q.error instanceof ApiError ? q.error.body : undefined;

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-3">
        <Button
          type="button"
          variant="default"
          size="sm"
          className="gap-2"
          disabled={q.isFetching}
          onClick={() => q.refetch()}
        >
          <RefreshCw className={cn("h-4 w-4", q.isFetching && "animate-spin")} />
          Refresh
        </Button>
        <span className="text-xs text-muted-foreground">
          Topic <code className="rounded bg-muted px-1 font-mono text-[11px]">{q.data?.topic ?? "stream-aggregates"}</code>{" "}
          via <code className="rounded bg-muted px-1 font-mono text-[11px]">{base}</code>
        </span>
      </div>

      {q.isError ? (
        <Card className="border-amber-500/40 bg-amber-500/5">
          <CardHeader>
            <CardTitle className="text-base text-amber-950 dark:text-amber-100">Could not load aggregates</CardTitle>
            <CardDescription className="text-amber-900/90 dark:text-amber-200/90">
              Start <span className="font-mono text-xs">dashboard-bff</span> and ensure Phase 4 ksql emits to{" "}
              <span className="font-mono text-xs">stream-aggregates</span>.
            </CardDescription>
          </CardHeader>
          {errBody ? (
            <CardContent>
              <pre className="max-h-40 overflow-auto rounded-md border border-border bg-muted/40 p-3 font-mono text-xs">
                {errBody.slice(0, 2000)}
              </pre>
            </CardContent>
          ) : null}
        </Card>
      ) : null}

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Stream aggregates preview</CardTitle>
          <CardDescription>
            JSON rows from the ksql hopping-window sink (<span className="font-mono text-xs">AGGREGATE_METRICS</span>) —
            tail snapshot, not a full history API.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {q.isPending ? (
            <div className="space-y-2">
              <div className="h-10 animate-pulse rounded bg-muted" />
              <div className="h-10 animate-pulse rounded bg-muted/80" />
            </div>
          ) : q.data?.items.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No rows — deploy ksql queries (
              <code className="font-mono text-xs">infra/scripts/deploy-ksql-queries.sh</code>) and confirm enriched telemetry
              is flowing.
            </p>
          ) : (
            <div className="overflow-x-auto rounded-md border border-border">
              <table className="w-full text-left text-sm">
                <thead className="border-b border-border bg-muted/40">
                  <tr>
                    <th className="px-3 py-2 font-medium">Car</th>
                    <th className="px-3 py-2 font-medium">Window start</th>
                    <th className="px-3 py-2 font-medium">Window end</th>
                    <th className="px-3 py-2 font-medium">Avg speed</th>
                    <th className="px-3 py-2 font-medium">Max speed</th>
                    <th className="px-3 py-2 font-medium">Events</th>
                    <th className="px-3 py-2 font-medium">Kafka</th>
                  </tr>
                </thead>
                <tbody>
                  {q.data?.items.map((row, i) => (
                    <tr
                      key={`${row._kafka_offset ?? i}-${row.car_id ?? ""}-${row.window_start_ms ?? ""}`}
                      className="border-b border-border last:border-0"
                    >
                      <td className="px-3 py-2 font-mono text-xs">{row.car_id ?? "—"}</td>
                      <td className="whitespace-nowrap px-3 py-2 font-mono text-[11px] text-muted-foreground">
                        {row.window_start_ms ?? "—"}
                      </td>
                      <td className="whitespace-nowrap px-3 py-2 font-mono text-[11px] text-muted-foreground">
                        {row.window_end_ms ?? "—"}
                      </td>
                      <td className="px-3 py-2 font-mono text-xs">{fmtNum(row.avg_speed_kph)}</td>
                      <td className="px-3 py-2 font-mono text-xs">{fmtNum(row.max_speed_kph)}</td>
                      <td className="px-3 py-2 font-mono text-xs">{fmtNum(row.event_count, 0)}</td>
                      <td className="whitespace-nowrap px-3 py-2 font-mono text-[11px] text-muted-foreground">
                        p{row._kafka_partition ?? "—"}@{row._kafka_offset ?? "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
