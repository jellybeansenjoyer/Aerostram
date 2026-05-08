import { cn } from "@/lib/utils";

type SparklineProps = {
  values: number[];
  className?: string;
};

/** Minimal in-memory sparkline (normalized to viewBox). */
export function Sparkline({ values, className }: SparklineProps) {
  if (values.length < 2) {
    return (
      <div
        className={cn(
          "flex h-14 items-center rounded-md border border-dashed border-border bg-muted/30 px-2 text-xs text-muted-foreground",
          className,
        )}
      >
        Collecting samples…
      </div>
    );
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const pts = values
    .map((v, i) => {
      const x = (i / (values.length - 1)) * 100;
      const y = 100 - ((v - min) / span) * 100;
      return `${x},${y}`;
    })
    .join(" ");

  return (
    <svg
      viewBox="0 0 100 100"
      className={cn("h-14 w-full text-accent", className)}
      preserveAspectRatio="none"
      aria-hidden
    >
      <polyline
        fill="none"
        stroke="currentColor"
        strokeWidth="3"
        vectorEffect="non-scaling-stroke"
        points={pts}
      />
    </svg>
  );
}