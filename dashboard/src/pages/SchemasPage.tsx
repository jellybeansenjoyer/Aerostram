import { SchemaBrowser } from "@/components/schemas/SchemaBrowser";

export function SchemasPage() {
  return (
    <div className="space-y-6">
      <div>
        <h2 className="font-display text-xl font-semibold tracking-tight">Schema Registry</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Browse subjects via the dev proxy at{" "}
          <code className="rounded bg-muted px-1 font-mono text-xs">/svc/registry</code> (same-origin).
        </p>
      </div>
      <SchemaBrowser />
    </div>
  );
}
