import { useQuery } from "@tanstack/react-query";
import { ExternalLink } from "lucide-react";
import { fetchConnectDashboard } from "@/lib/api/client";
import { CONNECT_DASHBOARD_REFETCH_MS } from "@/lib/constants";
import { useErrorToast } from "@/hooks/use-error-toast";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

function stateTone(state: string | undefined): "green" | "yellow" | "red" | "muted" {
  const s = (state ?? "").toUpperCase();
  if (s === "RUNNING") return "green";
  if (s === "FAILED") return "red";
  if (s === "PAUSED" || s === "UNASSIGNED" || s === "STOPPED") return "yellow";
  return "muted";
}

function StateBadge({ label }: { label: string }) {
  const tone = stateTone(label);
  const cls =
    tone === "green"
      ? "bg-emerald-500/15 text-emerald-800 dark:text-emerald-300"
      : tone === "red"
        ? "bg-red-500/15 text-red-800 dark:text-red-300"
        : tone === "yellow"
          ? "bg-amber-500/15 text-amber-900 dark:text-amber-200"
          : "bg-muted text-muted-foreground";
  return (
    <span className={cn("rounded-md px-2 py-0.5 font-mono text-xs font-semibold uppercase", cls)}>
      {label || "—"}
    </span>
  );
}

export function ConnectDashboard() {
  const q = useQuery({
    queryKey: ["connect", "dashboard"],
    queryFn: fetchConnectDashboard,
    refetchInterval: CONNECT_DASHBOARD_REFETCH_MS,
  });
  useErrorToast(q.isError, "Kafka Connect", q.error);

  return (
    <div className="space-y-6">
      {q.isPending ? (
        <div className="grid gap-4 md:grid-cols-2">
          {[1, 2].map((i) => (
            <div key={i} className="h-48 animate-pulse rounded-lg border border-border bg-muted/40" />
          ))}
        </div>
      ) : q.isError ? (
        <p className="text-sm text-red-600 dark:text-red-400">Could not load connectors.</p>
      ) : !q.data?.length ? (
        <Card className="border-dashed">
          <CardHeader>
            <CardTitle>No connectors</CardTitle>
            <CardDescription>
              Deploy Debezium connectors (e.g. Phase 3) — list stays empty until Connect returns names from{" "}
              <code className="font-mono text-xs">GET /connectors</code>.
            </CardDescription>
          </CardHeader>
        </Card>
      ) : (
        <div className="grid gap-4 lg:grid-cols-2">
          {q.data.map(({ name, status }) => {
            const cState = status.connector?.state;
            const tasks = status.tasks ?? [];
            const allRunning =
              cState?.toUpperCase() === "RUNNING" &&
              tasks.length > 0 &&
              tasks.every((t) => (t.state ?? "").toUpperCase() === "RUNNING");
            return (
              <Card
                key={name}
                className={cn(
                  "border-l-4",
                  allRunning ? "border-l-emerald-500" : "border-l-amber-500",
                )}
              >
                <CardHeader className="space-y-1 pb-2">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <CardTitle className="break-all font-mono text-base">{name}</CardTitle>
                    <StateBadge label={cState ?? ""} />
                  </div>
                  <CardDescription>
                    Worker:{" "}
                    <span className="font-mono text-xs">{status.connector?.worker_id ?? "—"}</span>
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="overflow-x-auto rounded-md border border-border">
                    <table className="w-full text-left text-sm">
                      <thead className="border-b border-border bg-muted/40">
                        <tr>
                          <th className="px-3 py-2 font-medium">Task</th>
                          <th className="px-3 py-2 font-medium">State</th>
                          <th className="hidden px-3 py-2 font-medium sm:table-cell">Worker</th>
                        </tr>
                      </thead>
                      <tbody>
                        {tasks.length === 0 ? (
                          <tr>
                            <td colSpan={3} className="px-3 py-3 text-muted-foreground">
                              No tasks reported — validate-cluster expects each connector to expose tasks.
                            </td>
                          </tr>
                        ) : (
                          tasks.map((t) => (
                            <tr key={`${name}-${t.id}`} className="border-b border-border last:border-0">
                              <td className="px-3 py-2 font-mono text-xs">{t.id ?? "—"}</td>
                              <td className="px-3 py-2">
                                <StateBadge label={t.state ?? ""} />
                              </td>
                              <td className="hidden px-3 py-2 font-mono text-xs text-muted-foreground sm:table-cell">
                                {t.worker_id ?? "—"}
                              </td>
                            </tr>
                          ))
                        )}
                      </tbody>
                    </table>
                  </div>
                  {status.connector?.trace ? (
                    <p className="text-xs text-muted-foreground">{status.connector.trace}</p>
                  ) : null}
                </CardContent>
              </Card>
            );
          })}
        </div>
      )}

      <footer className="text-xs text-muted-foreground">
        REST:{" "}
        <code className="rounded bg-muted px-1 font-mono">GET /svc/connect/connectors</code>,{" "}
        <code className="rounded bg-muted px-1 font-mono">…/connectors/&#123;name&#125;/status</code>
        . Same checks as{" "}
        <code className="rounded bg-muted px-1 font-mono">validate-cluster.sh</code> (connector + tasks{" "}
        <strong className="font-semibold text-foreground">RUNNING</strong>). Docs:{" "}
        <a
          href="https://docs.confluent.io/platform/current/connect/references/restapi.html"
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 text-accent underline-offset-4 hover:underline"
        >
          Kafka Connect REST API
          <ExternalLink className="h-3 w-3" />
        </a>
        .
      </footer>
    </div>
  );
}
