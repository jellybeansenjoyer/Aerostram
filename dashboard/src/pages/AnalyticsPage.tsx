import { KsqlAnalytics } from "@/components/analytics/KsqlAnalytics";
import { StreamAggregatesPreview } from "@/components/analytics/StreamAggregatesPreview";

export function AnalyticsPage() {
  return (
    <div className="space-y-6">
      <div>
        <h2 className="font-display text-xl font-semibold tracking-tight">ksqlDB analytics</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Server metadata and read-only <code className="font-mono text-xs">SHOW QUERIES</code> via{" "}
          <code className="rounded bg-muted px-1 font-mono text-xs">/svc/ksql</code>, plus a bounded Kafka preview of{" "}
          <code className="font-mono text-xs">stream-aggregates</code> via the dashboard BFF.
        </p>
      </div>
      <StreamAggregatesPreview />
      <KsqlAnalytics />
    </div>
  );
}
