import { useQuery } from "@tanstack/react-query";
import { ExternalLink } from "lucide-react";
import {
  fetchMlOverview,
  getConnectConnectors,
  getKsqlInfo,
  getRegistrySubjects,
} from "@/lib/api/client";
import { OVERVIEW_REFETCH_MS } from "@/lib/constants";
import { useServices } from "@/context/ServicesContext";
import { useErrorToast } from "@/hooks/use-error-toast";
import {
  OverviewCardShell,
  OverviewCardSkeleton,
  type OverviewTier,
} from "@/components/overview/OverviewCardShell";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

function RegistrySubjectsCard() {
  const q = useQuery({
    queryKey: ["overview", "registry", "subjects"],
    queryFn: getRegistrySubjects,
    refetchInterval: OVERVIEW_REFETCH_MS,
  });
  useErrorToast(q.isError, "Schema Registry", q.error);

  const tier: OverviewTier = q.isPending
    ? "loading"
    : q.isError
      ? "red"
      : (q.data?.length ?? 0) === 0
        ? "yellow"
        : "green";

  return (
    <OverviewCardShell
      title="Schema Registry"
      description="Registered Avro subjects"
      tier={tier}
      footer={
        q.isError ? (
          <span className="text-red-600 dark:text-red-400">Unreachable — check Registry container and proxy.</span>
        ) : (
          <span className="text-muted-foreground">GET /svc/registry/subjects</span>
        )
      }
    >
      {q.isPending ? (
        <OverviewCardSkeleton />
      ) : q.isError ? (
        <p className="text-sm text-red-600 dark:text-red-400">Could not load subjects.</p>
      ) : (
        <div>
          <p className="font-display text-3xl font-semibold tabular-nums">{q.data?.length ?? 0}</p>
          <p className="mt-1 text-xs text-muted-foreground">
            {(q.data?.length ?? 0) === 0
              ? "No subjects yet — register schemas or start the stack."
              : "subject(s) registered"}
          </p>
        </div>
      )}
    </OverviewCardShell>
  );
}

function KafkaConnectCard() {
  const q = useQuery({
    queryKey: ["overview", "connect", "connectors"],
    queryFn: getConnectConnectors,
    refetchInterval: OVERVIEW_REFETCH_MS,
  });
  useErrorToast(q.isError, "Kafka Connect", q.error);

  const tier: OverviewTier = q.isPending ? "loading" : q.isError ? "red" : "green";

  return (
    <OverviewCardShell
      title="Kafka Connect"
      description="REST API reachability"
      tier={tier}
      footer={
        <span className="text-muted-foreground">GET /svc/connect/connectors</span>
      }
    >
      {q.isPending ? (
        <OverviewCardSkeleton />
      ) : q.isError ? (
        <p className="text-sm text-red-600 dark:text-red-400">Connect cluster not reachable.</p>
      ) : (
        <div>
          <p className="font-display text-3xl font-semibold tabular-nums">{q.data?.length ?? 0}</p>
          <p className="mt-1 text-xs text-muted-foreground">connector(s) deployed</p>
        </div>
      )}
    </OverviewCardShell>
  );
}

function KsqlInfoCard() {
  const q = useQuery({
    queryKey: ["overview", "ksql", "info"],
    queryFn: getKsqlInfo,
    refetchInterval: OVERVIEW_REFETCH_MS,
  });
  useErrorToast(q.isError, "ksqlDB", q.error);

  const version = q.data?.KsqlServerInfo?.version;
  const tier: OverviewTier = q.isPending
    ? "loading"
    : q.isError
      ? "red"
      : version
        ? "green"
        : "yellow";

  return (
    <OverviewCardShell
      title="ksqlDB"
      description="Server metadata"
      tier={tier}
      footer={<span className="text-muted-foreground">GET /svc/ksql/info</span>}
    >
      {q.isPending ? (
        <OverviewCardSkeleton />
      ) : q.isError ? (
        <p className="text-sm text-red-600 dark:text-red-400">Could not load ksqlDB info.</p>
      ) : (
        <dl className="space-y-1 text-sm">
          <div className="flex justify-between gap-4">
            <dt className="text-muted-foreground">Version</dt>
            <dd className="font-mono text-xs">{version ?? "unknown"}</dd>
          </div>
          {q.data?.KsqlServerInfo?.ksqlServiceId ? (
            <div className="flex justify-between gap-4">
              <dt className="text-muted-foreground">Service ID</dt>
              <dd className="max-w-[12rem] truncate font-mono text-xs">
                {q.data.KsqlServerInfo.ksqlServiceId}
              </dd>
            </div>
          ) : null}
        </dl>
      )}
    </OverviewCardShell>
  );
}

function MlConsumerCard() {
  const q = useQuery({
    queryKey: ["overview", "ml", "health-ready"],
    queryFn: fetchMlOverview,
    refetchInterval: OVERVIEW_REFETCH_MS,
  });
  useErrorToast(q.isError, "ML consumer", q.error);

  let tier: OverviewTier = "loading";
  if (q.isPending) tier = "loading";
  else if (q.isError) tier = "red";
  else if (q.data) {
    const h = q.data.health.status?.toLowerCase();
    if (h !== "healthy") tier = "red";
    else if (!q.data.readyOk) tier = "yellow";
    else tier = "green";
  }

  return (
    <OverviewCardShell
      title="ML consumer"
      description="Health and Kafka readiness"
      tier={tier}
      footer={
        <span className="text-muted-foreground">GET /svc/ml/health · /svc/ml/ready</span>
      }
    >
      {q.isPending ? (
        <OverviewCardSkeleton />
      ) : q.isError ? (
        <p className="text-sm text-red-600 dark:text-red-400">ML service not reachable.</p>
      ) : q.data ? (
        <dl className="space-y-2 text-sm">
          <div className="flex justify-between gap-4">
            <dt className="text-muted-foreground">Health</dt>
            <dd
              className={cn(
                "font-medium",
                q.data.health.status?.toLowerCase() === "healthy"
                  ? "text-emerald-700 dark:text-emerald-400"
                  : "text-red-600 dark:text-red-400",
              )}
            >
              {q.data.health.status ?? "—"}
            </dd>
          </div>
          <div className="flex justify-between gap-4">
            <dt className="text-muted-foreground">Ready</dt>
            <dd
              className={cn(
                "font-medium",
                q.data.readyOk ? "text-emerald-700 dark:text-emerald-400" : "text-amber-700 dark:text-amber-400",
              )}
            >
              {q.data.readyOk ? "ready" : q.data.readyBody.status ?? "not_ready"}
            </dd>
          </div>
        </dl>
      ) : null}
    </OverviewCardShell>
  );
}

function ExternalToolsCard() {
  const { settings } = useServices();

  const links = [
    { label: "Kafka UI", href: settings.kafkaUiUrl },
    { label: "Grafana", href: settings.grafanaUrl },
    { label: "Prometheus", href: settings.prometheusUrl },
  ] as const;

  return (
    <Card className="shadow-sm">
      <CardHeader className="pb-3">
        <CardTitle className="text-base">External tools</CardTitle>
        <CardDescription>URLs from Settings — open in a new tab</CardDescription>
      </CardHeader>
      <CardContent className="flex flex-wrap gap-2 pt-0">
        {links.map(({ label, href }) => (
          <Button key={label} variant="outline" size="sm" asChild className="gap-1.5">
            <a href={href} target="_blank" rel="noopener noreferrer">
              <ExternalLink className="h-3.5 w-3.5" />
              {label}
            </a>
          </Button>
        ))}
      </CardContent>
    </Card>
  );
}

export function OverviewMetricsGrid() {
  return (
    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
      <RegistrySubjectsCard />
      <KafkaConnectCard />
      <KsqlInfoCard />
      <MlConsumerCard />
      <ExternalToolsCard />
    </div>
  );
}
