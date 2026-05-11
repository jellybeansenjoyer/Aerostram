import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from "@tanstack/react-table";
import { useMemo } from "react";

import type { PitPredictionRow } from "@/lib/api/types";
import { cn } from "@/lib/utils";

const columnHelper = createColumnHelper<PitPredictionRow>();

function fmtProb(n: number | undefined) {
  if (n == null || Number.isNaN(n)) return "—";
  return `${(n * 100).toFixed(1)}%`;
}

function fmtTs(ms: number | undefined) {
  if (ms == null) return "—";
  try {
    return new Date(ms).toISOString();
  } catch {
    return String(ms);
  }
}

export type PitWallDataGridProps = {
  rows: PitPredictionRow[];
  emptyLabel: string;
};

export function PitWallDataGrid({ rows, emptyLabel }: PitWallDataGridProps) {
  const columns = useMemo(
    () => [
      columnHelper.accessor("car_id", {
        header: "Car",
        cell: (info) => <span className="font-mono text-xs">{info.getValue() ?? "—"}</span>,
      }),
      columnHelper.accessor("timestamp_ms", {
        header: "Timestamp",
        cell: (info) => (
          <span className="whitespace-nowrap font-mono text-[11px] text-muted-foreground">
            {fmtTs(info.getValue())}
          </span>
        ),
      }),
      columnHelper.accessor("pit_probability", {
        header: "Probability",
        cell: (info) => <span className="font-mono text-xs">{fmtProb(info.getValue())}</span>,
      }),
      columnHelper.accessor("recommend_pit", {
        header: "Recommend pit",
        cell: (info) => {
          const v = info.getValue();
          return (
            <span className="font-mono text-xs">
              {v === true ? "yes" : v === false ? "no" : "—"}
            </span>
          );
        },
      }),
      columnHelper.accessor("model_version", {
        header: "Model version",
        cell: (info) => (
          <span className="max-w-[10rem] truncate font-mono text-xs text-muted-foreground">
            {info.getValue() ?? "—"}
          </span>
        ),
      }),
    ],
    [],
  );

  const table = useReactTable({
    data: rows,
    columns,
    getCoreRowModel: getCoreRowModel(),
    getRowId: (row, index) =>
      `${row.car_id ?? "row"}-${row.timestamp_ms ?? ""}-${row._kafka_offset ?? index}`,
  });

  if (rows.length === 0) {
    return <p className="text-sm text-muted-foreground">{emptyLabel}</p>;
  }

  return (
    <div className="overflow-x-auto rounded-md border border-border">
      <table className="w-full text-left text-sm">
        <thead className="border-b border-border bg-muted/40">
          {table.getHeaderGroups().map((hg) => (
            <tr key={hg.id}>
              {hg.headers.map((h) => (
                <th key={h.id} className="px-3 py-2 font-medium" scope="col">
                  {h.isPlaceholder ? null : flexRender(h.column.columnDef.header, h.getContext())}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {table.getRowModel().rows.map((row) => (
            <tr
              key={row.id}
              className={cn(
                "border-b border-border last:border-0 transition-colors",
                row.original.recommend_pit === true &&
                  "bg-amber-500/15 dark:bg-amber-500/10 [&>td:first-child]:border-l-4 [&>td:first-child]:border-l-amber-500",
              )}
            >
              {row.getVisibleCells().map((cell) => (
                <td key={cell.id} className="px-3 py-2 align-middle">
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
