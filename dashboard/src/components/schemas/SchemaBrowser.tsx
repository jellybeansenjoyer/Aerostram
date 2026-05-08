import { useQuery } from "@tanstack/react-query";
import { Search } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { JsonTree } from "@/components/schemas/JsonTree";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { getRegistryLatestVersion, getRegistrySubjects } from "@/lib/api/client";
import { SCHEMAS_SUBJECTS_REFETCH_MS } from "@/lib/constants";
import { useErrorToast } from "@/hooks/use-error-toast";
import { cn } from "@/lib/utils";

function parseSchemaBody(raw: string) {
  try {
    return { ok: true as const, parsed: JSON.parse(raw) as unknown };
  } catch {
    return { ok: false as const, raw };
  }
}

export function SchemaBrowser() {
  const [searchParams, setSearchParams] = useSearchParams();
  const selected = searchParams.get("subject") ?? "";
  const [filter, setFilter] = useState("");

  const subjectsQuery = useQuery({
    queryKey: ["registry", "subjects"],
    queryFn: getRegistrySubjects,
    refetchInterval: SCHEMAS_SUBJECTS_REFETCH_MS,
  });
  useErrorToast(subjectsQuery.isError, "Schema Registry (subjects)", subjectsQuery.error);

  useEffect(() => {
    if (subjectsQuery.isSuccess && selected && subjectsQuery.data && !subjectsQuery.data.includes(selected)) {
      setSearchParams({}, { replace: true });
    }
  }, [subjectsQuery.isSuccess, subjectsQuery.data, selected, setSearchParams]);

  const filtered = useMemo(() => {
    const list = subjectsQuery.data ?? [];
    const q = filter.trim().toLowerCase();
    if (!q) return list;
    return list.filter((s) => s.toLowerCase().includes(q));
  }, [subjectsQuery.data, filter]);

  const setSelected = (subject: string) => {
    if (subject) setSearchParams({ subject }, { replace: true });
    else setSearchParams({}, { replace: true });
  };

  return (
    <div className="flex flex-col gap-4 lg:flex-row lg:items-stretch">
      <aside className="flex w-full shrink-0 flex-col rounded-lg border border-border bg-card lg:w-72">
        <div className="border-b border-border p-3">
          <label className="sr-only" htmlFor="subject-search">
            Search subjects
          </label>
          <div className="relative">
            <Search className="pointer-events-none absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
            <Input
              id="subject-search"
              className="pl-9"
              placeholder="Search subjects…"
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              autoComplete="off"
            />
          </div>
        </div>
        <div className="max-h-[min(60vh,520px)] flex-1 overflow-y-auto p-2">
          {subjectsQuery.isPending ? (
            <div className="space-y-2 p-1">
              {[1, 2, 3, 4, 5].map((i) => (
                <div key={i} className="h-9 animate-pulse rounded-md bg-muted" />
              ))}
            </div>
          ) : subjectsQuery.isError ? (
            <p className="p-2 text-sm text-red-600 dark:text-red-400">Could not load subjects.</p>
          ) : filtered.length === 0 ? (
            <p className="p-2 text-sm text-muted-foreground">
              {filter.trim() ? "No subjects match this filter." : "No subjects registered."}
            </p>
          ) : (
            <ul className="space-y-1">
              {filtered.map((name) => (
                <li key={name}>
                  <button
                    type="button"
                    onClick={() => setSelected(name)}
                    className={cn(
                      "w-full rounded-md px-3 py-2 text-left text-sm transition-colors",
                      "hover:bg-muted",
                      selected === name &&
                        "border-l-2 border-accent bg-muted/80 font-medium text-foreground",
                    )}
                    aria-current={selected === name ? "true" : undefined}
                  >
                    <span className="break-all font-mono text-xs">{name}</span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      </aside>

      <div className="min-w-0 flex-1">
        <SchemaLatestPanel subject={selected} />
      </div>
    </div>
  );
}

function SchemaLatestPanel({ subject }: { subject: string }) {
  const q = useQuery({
    queryKey: ["registry", "latest", subject],
    queryFn: () => getRegistryLatestVersion(subject),
    enabled: Boolean(subject),
  });
  useErrorToast(q.isError, `Schema (${subject})`, q.error);

  if (!subject) {
    return (
      <Card className="border-dashed">
        <CardHeader>
          <CardTitle>Select a subject</CardTitle>
          <CardDescription>
            Choose a subject on the left to load the latest registered schema. Deep link example:{" "}
            <code className="rounded bg-muted px-1 font-mono text-xs">
              /schemas?subject=EnrichedTelemetryEvent
            </code>
          </CardDescription>
        </CardHeader>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader className="space-y-1">
        <CardTitle className="break-all font-mono text-base">{subject}</CardTitle>
        <CardDescription>GET /svc/registry/subjects/…/versions/latest</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {q.isPending ? (
          <div className="space-y-3">
            <div className="h-4 w-48 animate-pulse rounded bg-muted" />
            <div className="h-40 animate-pulse rounded-md bg-muted/80" />
          </div>
        ) : q.isError ? (
          <p className="text-sm text-red-600 dark:text-red-400">Failed to load latest schema version.</p>
        ) : q.data ? (
          <>
            <dl className="grid gap-3 text-sm sm:grid-cols-3">
              <div>
                <dt className="text-muted-foreground">Version</dt>
                <dd className="font-mono text-xs font-semibold">{q.data.version}</dd>
              </div>
              <div>
                <dt className="text-muted-foreground">Schema id</dt>
                <dd className="font-mono text-xs font-semibold">{q.data.id}</dd>
              </div>
              <div>
                <dt className="text-muted-foreground">Type</dt>
                <dd className="font-mono text-xs font-semibold">{q.data.schemaType ?? "—"}</dd>
              </div>
            </dl>
            <div>
              <p className="mb-2 text-xs font-medium text-muted-foreground">Schema body</p>
              {(() => {
                const parsed = parseSchemaBody(q.data.schema);
                if (parsed.ok) {
                  return <JsonTree value={parsed.parsed} />;
                }
                return (
                  <pre className="max-h-[min(70vh,780px)] overflow-auto whitespace-pre-wrap rounded-md border border-border bg-muted/25 p-3 font-mono text-xs leading-relaxed">
                    {parsed.raw}
                  </pre>
                );
              })()}
            </div>
          </>
        ) : null}
      </CardContent>
    </Card>
  );
}
