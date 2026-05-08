import { useQuery } from "@tanstack/react-query";
import { Pause, Play } from "lucide-react";
import { useState } from "react";
import type { ActuatorHealth } from "@/lib/api/client";
import { getJson } from "@/lib/api/client";
import { HEALTH_REFETCH_MS } from "@/lib/constants";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

function StatusBadge({
  label,
  ok,
  detail,
}: {
  label: string;
  ok: boolean | undefined;
  detail?: string;
}) {
  const tone =
    ok === undefined
      ? "bg-muted text-muted-foreground"
      : ok
        ? "bg-emerald-500/15 text-emerald-700 dark:text-emerald-400"
        : "bg-red-500/15 text-red-700 dark:text-red-400";
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div
          role="status"
          aria-label={`${label}: ${ok === undefined ? "checking" : ok ? "healthy" : "unhealthy"}`}
          className={cn(
            "flex cursor-default items-center gap-2 rounded-md px-3 py-2 font-mono text-xs font-medium",
            tone,
          )}
        >
          <span className="text-muted-foreground">{label}</span>
          <span>{ok === undefined ? "…" : ok ? "UP" : "DOWN"}</span>
        </div>
      </TooltipTrigger>
      {detail ? (
        <TooltipContent side="bottom" className="max-w-xs font-mono text-xs">
          {detail}
        </TooltipContent>
      ) : null}
    </Tooltip>
  );
}

export function HealthStrip() {
  const [paused, setPaused] = useState(false);
  const interval = paused ? false : HEALTH_REFETCH_MS;

  const producer = useQuery({
    queryKey: ["health", "producer"],
    queryFn: () => getJson<ActuatorHealth>("/svc/producer/actuator/health"),
    refetchInterval: interval,
  });

  const processor = useQuery({
    queryKey: ["health", "processor"],
    queryFn: () => getJson<ActuatorHealth>("/svc/processor/actuator/health"),
    refetchInterval: interval,
  });

  const producerOk =
    producer.data?.status === "UP"
      ? true
      : producer.isError
        ? false
        : undefined;
  const processorOk =
    processor.data?.status === "UP"
      ? true
      : processor.isError
        ? false
        : undefined;

  const prodDetail = producer.error
    ? producer.error instanceof Error
      ? producer.error.message
      : String(producer.error)
    : producer.data?.status;
  const procDetail = processor.error
    ? processor.error instanceof Error
      ? processor.error.message
      : String(processor.error)
    : processor.data?.status;

  return (
    <div className="flex flex-wrap items-center gap-3 rounded-lg border border-border bg-card p-4 shadow-sm">
      <div className="flex flex-wrap items-center gap-2">
        <StatusBadge label="Producer" ok={producerOk} detail={prodDetail} />
        <StatusBadge label="Stream processor" ok={processorOk} detail={procDetail} />
      </div>
      <Button
        type="button"
        variant="outline"
        size="sm"
        className="ml-auto gap-1"
        onClick={() => setPaused((p) => !p)}
        aria-pressed={paused}
      >
        {paused ? (
          <>
            <Play className="h-4 w-4" /> Resume polling
          </>
        ) : (
          <>
            <Pause className="h-4 w-4" /> Pause polling
          </>
        )}
      </Button>
      <span className="text-xs text-muted-foreground">
        Refetch every {HEALTH_REFETCH_MS / 1000}s · paths{" "}
        <code className="rounded bg-muted px-1">/svc/producer</code>,{" "}
        <code className="rounded bg-muted px-1">/svc/processor</code>
      </span>
    </div>
  );
}
