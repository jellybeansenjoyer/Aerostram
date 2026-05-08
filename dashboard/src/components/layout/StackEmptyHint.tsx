/** FE-11 — reusable empty-state hint for local development */
export function StackEmptyHint() {
  return (
    <div
      role="note"
      aria-label="How to start the AeroStream stack locally"
      className="rounded-md border border-border bg-muted/50 px-4 py-3 text-sm"
    >
      <p className="font-medium text-foreground">Start the stack with Docker Compose</p>
      <p className="mt-1.5 leading-relaxed text-muted-foreground">
        From the repository root, copy env and run:{" "}
        <code className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs text-foreground">
          cp .env.example .env && docker compose up -d
        </code>
      </p>
    </div>
  );
}
