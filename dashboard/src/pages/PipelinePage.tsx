import { SimulatorControl } from "@/components/pipeline/SimulatorControl";

export function PipelinePage() {
  return (
    <div className="space-y-6">
      <header>
        <h2 className="font-display text-xl font-semibold tracking-tight">Pipeline</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Telemetry simulator rate (BE-SIM-1) and start/stop controls for the Phase 2 producer.
        </p>
      </header>
      <SimulatorControl />
    </div>
  );
}
