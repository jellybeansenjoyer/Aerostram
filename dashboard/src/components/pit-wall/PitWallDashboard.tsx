import { useQuery } from "@tanstack/react-query";
import { RefreshCw } from "lucide-react";
import { PitWallDataGrid } from "@/components/pit-wall/PitWallDataGrid";
import { resolveBffBase } from "@/lib/api/client";
import { BFF_TOPIC_PREVIEW_REFETCH_MS } from "@/lib/constants";
import { loadPitPredictionsForWall } from "@/lib/pit-wall/load-pit-predictions";
import { useServices } from "@/context/ServicesContext";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

const PREVIEW_LIMIT = 40;

export function PitWallDashboard() {
  const { settings } = useServices();
  const base = resolveBffBase(settings.bffUrl);

  const q = useQuery({
    queryKey: ["bff", "pit-predictions", base, PREVIEW_LIMIT],
    queryFn: () => loadPitPredictionsForWall(base, PREVIEW_LIMIT),
    refetchInterval: BFF_TOPIC_PREVIEW_REFETCH_MS,
  });

  const mode = q.data?.mode;
  const topicLabel = q.data?.topic ?? "pit-predictions";

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
          Topic{" "}
          <code className="rounded bg-muted px-1 font-mono text-[11px]">{topicLabel}</code> via{" "}
          <code className="rounded bg-muted px-1 font-mono text-[11px]">{base}</code>
          {mode === "live" ? (
            <span className="ml-2 rounded bg-emerald-500/15 px-1.5 py-0.5 text-emerald-800 dark:text-emerald-200">
              live
            </span>
          ) : null}
          {mode === "mock" ? (
            <span className="ml-2 rounded bg-sky-500/15 px-1.5 py-0.5 text-sky-900 dark:text-sky-100">
              demo data
            </span>
          ) : null}
        </span>
      </div>

      {mode === "mock" ? (
        <p className="rounded-md border border-sky-500/30 bg-sky-500/10 px-3 py-2 text-sm text-sky-950 dark:text-sky-100">
          BFF unavailable — showing placeholder rows. Start{" "}
          <code className="rounded bg-muted px-1 font-mono text-xs">dashboard-bff</code> on port 8089 or set the BFF URL
          in Settings to load live Kafka previews.
        </p>
      ) : null}

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Pit predictions wall</CardTitle>
          <CardDescription>
            TanStack Table — columns match <span className="font-mono text-xs">PitPrediction</span>. Rows with{" "}
            <span className="font-mono text-xs">recommend_pit</span> are highlighted.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {q.isPending ? (
            <div className="space-y-2">
              <div className="h-10 animate-pulse rounded bg-muted" />
              <div className="h-10 animate-pulse rounded bg-muted/80" />
            </div>
          ) : (
            <PitWallDataGrid
              rows={q.data?.items ?? []}
              emptyLabel={
                mode === "live"
                  ? "No rows returned — ensure ml-consumer is publishing to pit-predictions."
                  : "No demo rows (unexpected)."
              }
            />
          )}
        </CardContent>
      </Card>
    </div>
  );
}
