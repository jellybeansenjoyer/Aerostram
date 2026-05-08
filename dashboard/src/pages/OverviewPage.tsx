import { HealthStrip } from "@/components/health/HealthStrip";
import { OverviewMetricsGrid } from "@/components/overview/OverviewMetricsGrid";

export function OverviewPage() {
  return (
    <div className="space-y-6">
      <HealthStrip />
      <OverviewMetricsGrid />
    </div>
  );
}
