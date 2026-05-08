import { useQuery } from "@tanstack/react-query";
import { ExternalLink, RefreshCw } from "lucide-react";
import { useMemo } from "react";
import { ApiError, getKsqlInfo, postKsqlStatement } from "@/lib/api/client";
import { KSQL_INFO_REFETCH_MS } from "@/lib/constants";
import { useErrorToast } from "@/hooks/use-error-toast";
import { parseKsqlQueriesResponse } from "@/lib/ksql-queries";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

const KSQL_REST_DOCS = "https://docs.confluent.io/platform/current/ksql/develop/developer-guide/ksql-rest-api.html";

export function KsqlAnalytics() {
  const infoQ = useQuery({
    queryKey: ["ksql", "info", "page"],
    queryFn: getKsqlInfo,
    refetchInterval: KSQL_INFO_REFETCH_MS,
  });
  const queriesQ = useQuery({
    queryKey: ["ksql", "show-queries", "page"],
    queryFn: () => postKsqlStatement("SHOW QUERIES;"),
    staleTime: 30_000,
  });

  useErrorToast(infoQ.isError, "ksqlDB /info", infoQ.error);
  useErrorToast(queriesQ.isError, "ksqlDB SHOW QUERIES", queriesQ.error);

  const rows = useMemo(
    () => (queriesQ.data ? parseKsqlQueriesResponse(queriesQ.data) : []),
    [queriesQ.data],
  );

  const errBody = queriesQ.error instanceof ApiError ? queriesQ.error.body : undefined;

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center gap-3">
        <Button
          type="button"
          variant="default"
          size="sm"
          className="gap-2"
          disabled={queriesQ.isFetching}
          onClick={() => queriesQ.refetch()}
        >
          <RefreshCw className={cn("h-4 w-4", queriesQ.isFetching && "animate-spin")} />
          Refresh queries
        </Button>
        <span className="text-xs text-muted-foreground">
          POST <code className="rounded bg-muted px-1 font-mono">/svc/ksql/ksql</code> with{" "}
          <code className="font-mono text-[11px]">SHOW QUERIES;</code>
        </span>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Server info</CardTitle>
            <CardDescription>GET /svc/ksql/info</CardDescription>
          </CardHeader>
          <CardContent>
            {infoQ.isPending ? (
              <div className="space-y-2">
                <div className="h-4 w-40 animate-pulse rounded bg-muted" />
                <div className="h-4 w-56 animate-pulse rounded bg-muted/80" />
              </div>
            ) : infoQ.isError ? (
              <p className="text-sm text-red-600 dark:text-red-400">Could not load /info.</p>
            ) : (
              <dl className="space-y-2 text-sm">
                <div className="flex justify-between gap-4">
                  <dt className="text-muted-foreground">Version</dt>
                  <dd className="font-mono text-xs">{infoQ.data?.KsqlServerInfo?.version ?? "—"}</dd>
                </div>
                <div className="flex justify-between gap-4">
                  <dt className="text-muted-foreground">Service ID</dt>
                  <dd className="max-w-[14rem] truncate font-mono text-xs">
                    {infoQ.data?.KsqlServerInfo?.ksqlServiceId ?? "—"}
                  </dd>
                </div>
              </dl>
            )}
          </CardContent>
        </Card>

        <Card className="border-dashed">
          <CardHeader>
            <CardTitle className="text-base">REST reference</CardTitle>
            <CardDescription>
              ksqlDB HTTP API (Confluent Platform) — request format matches deploy scripts.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <a
              href={KSQL_REST_DOCS}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1 text-sm text-accent underline-offset-4 hover:underline"
            >
              ksqlDB REST API docs
              <ExternalLink className="h-3.5 w-3.5" />
            </a>
          </CardContent>
        </Card>
      </div>

      {queriesQ.isError ? (
        <Card className="border-amber-500/40 bg-amber-500/5">
          <CardHeader>
            <CardTitle className="text-base text-amber-950 dark:text-amber-100">
              Could not run SHOW QUERIES from the browser
            </CardTitle>
            <CardDescription className="text-amber-900/90 dark:text-amber-200/90">
              If the proxy blocks POST or preflight fails, run statements from the ksqlDB CLI against port{" "}
              <code className="font-mono text-xs">8088</code>, or use the dashboard BFF for whitelisted read APIs (pit
              predictions and aggregates are wired in FE-9).
            </CardDescription>
          </CardHeader>
          {errBody ? (
            <CardContent>
              <pre className="max-h-48 overflow-auto rounded-md border border-border bg-muted/40 p-3 font-mono text-xs">
                {errBody.slice(0, 4000)}
              </pre>
            </CardContent>
          ) : null}
        </Card>
      ) : null}

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Persistent queries</CardTitle>
          <CardDescription>
            Parsed from <code className="font-mono text-xs">SHOW QUERIES</code> response — query IDs visible when
            ksqlDB returns rows.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {queriesQ.isPending ? (
            <div className="space-y-2">
              <div className="h-10 animate-pulse rounded bg-muted" />
              <div className="h-10 animate-pulse rounded bg-muted/80" />
            </div>
          ) : queriesQ.isError ? null : rows.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No queries parsed — deploy Phase 4 SQL (
              <code className="font-mono text-xs">infra/scripts/deploy-ksql-queries.sh</code>) or click Refresh.
            </p>
          ) : (
            <div className="overflow-x-auto rounded-md border border-border">
              <table className="w-full text-left text-sm">
                <thead className="border-b border-border bg-muted/40">
                  <tr>
                    <th className="px-3 py-2 font-medium">Query ID</th>
                    <th className="px-3 py-2 font-medium">Status</th>
                    <th className="px-3 py-2 font-medium">Query</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r) => (
                    <tr key={r.id} className="border-b border-border last:border-0">
                      <td className="max-w-[12rem] px-3 py-2 font-mono text-xs">{r.id}</td>
                      <td className="whitespace-nowrap px-3 py-2 font-mono text-xs text-muted-foreground">
                        {r.statusSummary ?? "—"}
                      </td>
                      <td className="px-3 py-2 font-mono text-xs leading-relaxed">{r.queryString}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      <footer className="text-xs text-muted-foreground">
        Read-only UI — arbitrary ksql is not exposed. POST errors may indicate proxy limits; use CLI or run statements
        server-side.
      </footer>
    </div>
  );
}
