import { ConnectDashboard } from "@/components/connect/ConnectDashboard";

export function CdcPage() {
  return (
    <div className="space-y-6">
      <div>
        <h2 className="font-display text-xl font-semibold tracking-tight">CDC &amp; Kafka Connect</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Connector and task states from <code className="rounded bg-muted px-1 font-mono text-xs">/svc/connect</code>.
        </p>
      </div>
      <ConnectDashboard />
    </div>
  );
}
