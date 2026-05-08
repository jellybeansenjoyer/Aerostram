import { PitWallDashboard } from "@/components/pit-wall/PitWallDashboard";

export function PitWallPage() {
  return (
    <div className="space-y-6">
      <div>
        <h2 className="font-display text-xl font-semibold tracking-tight">Pit wall</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          FE-10 pit predictions wall: TanStack Table wired to the BFF (
          <code className="rounded bg-muted px-1 font-mono text-xs">GET /svc/bff/api/v1/pit-predictions/recent</code>
          ), with demo rows when the BFF is offline.
        </p>
      </div>
      <PitWallDashboard />
    </div>
  );
}
