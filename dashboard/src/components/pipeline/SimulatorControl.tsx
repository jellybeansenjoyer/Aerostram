import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ChevronDown } from "lucide-react";
import { useEffect, useId, useState } from "react";
import { toast } from "sonner";
import {
  ApiError,
  getSimulatorStatus,
  postSimulatorRate,
  postSimulatorStart,
  postSimulatorStop,
} from "@/lib/api/client";
import type { SimulatorStatus } from "@/lib/api/types";
import { SIMULATOR_STATUS_REFETCH_MS } from "@/lib/constants";
import { useTabVisible } from "@/hooks/use-tab-visible";
import { StackEmptyHint } from "@/components/layout/StackEmptyHint";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

const EPS_MIN = 50;
const EPS_MAX = 100_000;
const EPS_STEP = 50;

function toastApiError(e: unknown, fallback: string) {
  if (e instanceof ApiError) toast.error(`${fallback}: ${e.status} ${e.message}`);
  else if (e instanceof Error) toast.error(`${fallback}: ${e.message}`);
  else toast.error(fallback);
}

export function SimulatorControl() {
  const queryClient = useQueryClient();
  const tabVisible = useTabVisible();
  const sliderId = useId();

  const statusQuery = useQuery({
    queryKey: ["simulator", "status"],
    queryFn: getSimulatorStatus,
    refetchInterval: tabVisible ? SIMULATOR_STATUS_REFETCH_MS : false,
  });

  /** True after user moves the slider — blocks status polling from overwriting the draft. Cleared on Apply / Start / Stop success. */
  const [epsDirty, setEpsDirty] = useState(false);
  const [epsDraft, setEpsDraft] = useState(EPS_MIN);

  useEffect(() => {
    const v = statusQuery.data?.eventsPerSecond;
    if (v == null || epsDirty) return;
    setEpsDraft(Math.min(EPS_MAX, Math.max(EPS_MIN, v)));
  }, [statusQuery.data?.eventsPerSecond, epsDirty]);

  const invalidateStatus = () =>
    queryClient.invalidateQueries({ queryKey: ["simulator", "status"] });

  const startMut = useMutation({
    mutationFn: postSimulatorStart,
    onSuccess: async () => {
      toast.success("Simulator started");
      setEpsDirty(false);
      await invalidateStatus();
    },
    onError: (e) => toastApiError(e, "Start failed"),
  });

  const stopMut = useMutation({
    mutationFn: postSimulatorStop,
    onSuccess: async () => {
      toast.success("Simulator stopped");
      setEpsDirty(false);
      await invalidateStatus();
    },
    onError: (e) => toastApiError(e, "Stop failed"),
  });

  const rateMut = useMutation({
    mutationFn: postSimulatorRate,
    onSuccess: async (next) => {
      const v = next.eventsPerSecond;
      if (v != null) {
        setEpsDraft(Math.min(EPS_MAX, Math.max(EPS_MIN, v)));
      }
      setEpsDirty(false);
      toast.success("Simulator rate updated");
      await invalidateStatus();
    },
    onError: (e) => toastApiError(e, "Rate update failed"),
  });

  const busy = startMut.isPending || stopMut.isPending || rateMut.isPending;
  const data = statusQuery.data;

  return (
    <div className="space-y-6">
      <div
        role="region"
        aria-labelledby={`${sliderId}-heading`}
        className="rounded-lg border border-border bg-card px-4 py-4 shadow-sm"
      >
        <h2 id={`${sliderId}-heading`} className="text-sm font-semibold text-foreground">
          Events per second (runtime)
        </h2>
        <p className="mt-1 text-sm leading-relaxed text-muted-foreground">
          Adjust target throughput via{" "}
          <code className="rounded bg-muted px-1 font-mono text-xs">POST /api/simulator/rate</code> with a JSON body{" "}
          <code className="font-mono text-[11px]">{`{ "eventsPerSecond": N }`}</code>. Applies immediately while the simulator
          is running; otherwise the next start uses this target. Rebuild the <span className="font-mono text-xs">producer</span>{" "}
          image after pulling BE-SIM-1 if the endpoint returns 404.
        </p>
        <div className="mt-4 flex flex-col gap-4 sm:flex-row sm:items-center">
          <div className="flex min-w-0 flex-1 flex-col gap-2">
            <label htmlFor={sliderId} className="text-xs font-medium text-muted-foreground">
              Target events per second ({EPS_MIN}–{EPS_MAX.toLocaleString()})
            </label>
            <input
              id={sliderId}
              type="range"
              min={EPS_MIN}
              max={EPS_MAX}
              step={EPS_STEP}
              value={epsDraft}
              disabled={busy}
              aria-valuemin={EPS_MIN}
              aria-valuemax={EPS_MAX}
              aria-valuenow={epsDraft}
              onChange={(e) => {
                setEpsDirty(true);
                setEpsDraft(Number(e.target.value));
              }}
              className="h-2 w-full cursor-pointer accent-accent disabled:opacity-50"
            />
            <span className="font-mono text-sm tabular-nums text-foreground" aria-live="polite">
              {epsDraft.toLocaleString()} events/sec
            </span>
          </div>
          <Button
            type="button"
            variant="secondary"
            className="shrink-0 self-start sm:self-center"
            disabled={
              busy ||
              (!epsDirty && data?.eventsPerSecond != null && epsDraft === data.eventsPerSecond)
            }
            onClick={() => rateMut.mutate(epsDraft)}
          >
            {rateMut.isPending ? "Applying…" : "Apply rate"}
          </Button>
        </div>
      </div>

      <StackEmptyHint />

      <Card>
        <CardHeader>
          <CardTitle>Telemetry simulator</CardTitle>
          <CardDescription>
            Control the Phase 2 producer simulator via REST (polled every{" "}
            {SIMULATOR_STATUS_REFETCH_MS / 1000}s while this tab is visible).
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="flex flex-wrap gap-3">
            <Button
              type="button"
              onClick={() => startMut.mutate()}
              disabled={busy}
              className="min-w-[7rem]"
            >
              {startMut.isPending ? "Starting…" : "Start"}
            </Button>
            <Button
              type="button"
              variant="secondary"
              onClick={() => stopMut.mutate()}
              disabled={busy}
              className="min-w-[7rem]"
            >
              {stopMut.isPending ? "Stopping…" : "Stop"}
            </Button>
          </div>

          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <Metric label="Running" value={data?.running === undefined ? "…" : data.running ? "yes" : "no"} />
            <Metric
              label="Events / sec (configured)"
              value={data?.eventsPerSecond != null ? String(data.eventsPerSecond) : "—"}
            />
            <Metric
              label="Total published"
              value={data?.totalPublished != null ? formatInt(data.totalPublished) : "—"}
            />
            <Metric
              label="Active cars"
              value={data?.activeCarCount != null ? String(data.activeCarCount) : "—"}
            />
          </div>

          <details className="group rounded-lg border border-border bg-muted/30">
            <summary
              className={cn(
                "flex cursor-pointer list-none items-center gap-2 px-4 py-3 text-sm font-medium",
                "marker:content-none [&::-webkit-details-marker]:hidden",
                "rounded-lg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-background",
              )}
            >
              <ChevronDown className="h-4 w-4 shrink-0 transition-transform group-open:rotate-180" />
              Raw status JSON
            </summary>
            <div className="border-t border-border px-4 py-3">
              <StatusJsonPreview status={data} loading={statusQuery.isPending} error={statusQuery.error} />
            </div>
          </details>
        </CardContent>
      </Card>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-border bg-card px-3 py-2 shadow-sm">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className="font-mono text-lg font-semibold tabular-nums">{value}</p>
    </div>
  );
}

function formatInt(n: number) {
  return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
}

function StatusJsonPreview({
  status,
  loading,
  error,
}: {
  status: SimulatorStatus | undefined;
  loading: boolean;
  error: unknown;
}) {
  if (loading && !status) {
    return (
      <div
        className="h-24 animate-pulse rounded bg-muted"
        aria-busy="true"
        aria-label="Loading simulator status"
      />
    );
  }
  if (error) {
    const msg =
      error instanceof ApiError
        ? `${error.status} ${error.message}`
        : error instanceof Error
          ? error.message
          : "Failed to load status";
    return (
      <p className="font-mono text-xs text-red-700 dark:text-red-300" role="alert">
        {msg}
      </p>
    );
  }
  return (
    <pre className="max-h-72 overflow-auto rounded-md bg-background p-3 font-mono text-xs leading-relaxed text-foreground shadow-inner">
      {JSON.stringify(status ?? {}, null, 2)}
    </pre>
  );
}
