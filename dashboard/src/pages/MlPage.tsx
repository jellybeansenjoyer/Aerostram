import { MlDashboard } from "@/components/ml/MlDashboard";

export function MlPage() {
  return (
    <div className="space-y-6">
      <div>
        <h2 className="font-display text-xl font-semibold tracking-tight">ML consumer</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Phase 5 FastAPI service — health, readiness, and Prometheus counters from{" "}
          <code className="rounded bg-muted px-1 font-mono text-xs">/svc/ml</code>.
        </p>
      </div>
      <MlDashboard />
    </div>
  );
}
