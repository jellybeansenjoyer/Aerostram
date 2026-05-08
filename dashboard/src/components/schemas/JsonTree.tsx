import { ChevronDown, ChevronRight } from "lucide-react";
import { useState } from "react";
import { cn } from "@/lib/utils";

function Primitive({ value }: { value: unknown }) {
  if (value === null) return <span className="text-muted-foreground">null</span>;
  if (typeof value === "boolean") {
    return <span className="text-amber-700 dark:text-amber-400">{String(value)}</span>;
  }
  if (typeof value === "number") {
    return <span className="text-emerald-700 dark:text-emerald-400 tabular-nums">{value}</span>;
  }
  if (typeof value === "string") {
    return (
      <span className="break-all text-rose-800 dark:text-rose-300">
        &quot;{value}&quot;
      </span>
    );
  }
  return <span className="text-muted-foreground">{String(value)}</span>;
}

function TreeNode({
  fieldName,
  value,
  depth,
  defaultExpandedDepth,
  nameIsIndex,
}: {
  fieldName: string;
  value: unknown;
  depth: number;
  defaultExpandedDepth: number;
  nameIsIndex: boolean;
}) {
  const pad = depth * 14;
  const expandable = value !== null && typeof value === "object";
  const [open, setOpen] = useState(depth < defaultExpandedDepth);

  if (!expandable) {
    return (
      <div className="break-all font-mono text-xs leading-relaxed" style={{ paddingLeft: pad }}>
        {fieldName !== "" ? (
          <>
            {nameIsIndex ? (
              <span className="text-orange-700 dark:text-orange-400">{fieldName}</span>
            ) : (
              <span className="text-sky-700 dark:text-sky-400">&quot;{fieldName}&quot;</span>
            )}
            <span className="text-muted-foreground">: </span>
          </>
        ) : null}
        <Primitive value={value} />
      </div>
    );
  }

  const isArr = Array.isArray(value);
  const entries: readonly [string, unknown][] = isArr
    ? (value as unknown[]).map((v, i) => [String(i), v] as const)
    : (Object.entries(value as Record<string, unknown>).sort(([a], [b]) =>
        a.localeCompare(b),
      ) as [string, unknown][]);

  const summary = isArr ? `Array(${entries.length})` : `Object(${entries.length})`;

  return (
    <div className="font-mono text-xs leading-relaxed">
      <div
        className={cn(
          "flex cursor-pointer items-start gap-1 rounded-sm break-all hover:bg-muted/60",
          "outline-none focus-visible:ring-2 focus-visible:ring-accent",
        )}
        style={{ paddingLeft: pad }}
        role="button"
        tabIndex={0}
        aria-expanded={open}
        onClick={() => setOpen((o) => !o)}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            setOpen((o) => !o);
          }
        }}
      >
        <span className="mt-0.5 shrink-0 text-muted-foreground" aria-hidden>
          {open ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
        </span>
        {fieldName !== "" ? (
          <>
            {nameIsIndex ? (
              <span className="text-orange-700 dark:text-orange-400">{fieldName}</span>
            ) : (
              <span className="text-sky-700 dark:text-sky-400">&quot;{fieldName}&quot;</span>
            )}
            <span className="text-muted-foreground">: </span>
          </>
        ) : null}
        <span className="text-muted-foreground">{summary}</span>
      </div>
      {open
        ? entries.map(([k, v], idx) => (
            <TreeNode
              key={`${depth}-${k}-${idx}`}
              fieldName={k}
              value={v}
              depth={depth + 1}
              defaultExpandedDepth={defaultExpandedDepth}
              nameIsIndex={isArr}
            />
          ))
        : null}
    </div>
  );
}

/** Expandable JSON tree for parsed Registry schema payloads (design tokens only). */
export function JsonTree({ value }: { value: unknown }) {
  return (
    <div className="max-h-[min(70vh,780px)] overflow-auto rounded-md border border-border bg-muted/25 p-3 shadow-inner">
      <TreeNode
        fieldName=""
        value={value}
        depth={0}
        defaultExpandedDepth={3}
        nameIsIndex={false}
      />
    </div>
  );
}
