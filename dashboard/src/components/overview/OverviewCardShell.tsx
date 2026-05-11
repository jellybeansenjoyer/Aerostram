import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

export type OverviewTier = "loading" | "green" | "yellow" | "red";

const tierBorder: Record<Exclude<OverviewTier, "loading">, string> = {
  green: "border-l-emerald-500",
  yellow: "border-l-amber-500",
  red: "border-l-red-500",
};

const tierDot: Record<Exclude<OverviewTier, "loading">, string> = {
  green: "bg-emerald-500",
  yellow: "bg-amber-500",
  red: "bg-red-500",
};

export function OverviewCardShell({
  title,
  description,
  tier,
  children,
  footer,
}: {
  title: string;
  description: string;
  tier: OverviewTier;
  children: ReactNode;
  footer?: ReactNode;
}) {
  const isLoading = tier === "loading";

  return (
    <section
      className={cn(
        "flex flex-col rounded-lg border border-border bg-card text-card-foreground shadow-sm transition-colors",
        !isLoading && ["border-l-4", tierBorder[tier]],
      )}
      aria-busy={isLoading}
    >
      <header className="flex items-start gap-3 border-b border-border px-5 py-4">
        <span className="mt-1.5 flex h-2.5 w-2.5 shrink-0 rounded-full" aria-hidden>
          {isLoading ? (
            <span className="block h-2.5 w-2.5 animate-pulse rounded-full bg-muted-foreground/40" />
          ) : (
            <span className={cn("block h-2.5 w-2.5 rounded-full shadow-sm", tierDot[tier])} />
          )}
        </span>
        <div className="min-w-0 flex-1 space-y-1">
          <h2 className="font-semibold leading-none tracking-tight">{title}</h2>
          <p className="text-sm text-muted-foreground">{description}</p>
        </div>
      </header>
      <div className="flex flex-1 flex-col px-5 py-4">{children}</div>
      {footer ? <footer className="border-t border-border px-5 py-3 text-xs">{footer}</footer> : null}
    </section>
  );
}

export function OverviewCardSkeleton() {
  return (
    <div className="space-y-3">
      <div className="h-4 w-24 animate-pulse rounded bg-muted" />
      <div className="h-3 w-full max-w-[180px] animate-pulse rounded bg-muted/80" />
      <div className="h-3 w-full max-w-[140px] animate-pulse rounded bg-muted/60" />
    </div>
  );
}
