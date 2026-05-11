import type { KsqlQueryRow } from "@/lib/api/types";

/** Walk ksqlDB `/ksql` JSON and collect persistent query rows (best-effort across versions). */
export function parseKsqlQueriesResponse(payload: unknown): KsqlQueryRow[] {
  const out: KsqlQueryRow[] = [];
  const seen = new Set<string>();

  function statusSummary(sc: unknown): string | undefined {
    if (sc === null || sc === undefined) return undefined;
    if (typeof sc === "string") return sc;
    if (typeof sc === "object" && !Array.isArray(sc)) {
      return Object.entries(sc as Record<string, unknown>)
        .map(([k, v]) => `${k}:${String(v)}`)
        .join(" ");
    }
    return String(sc);
  }

  function maybePush(o: Record<string, unknown>) {
    const id = o.id;
    const qs = o.queryString;
    if (id === undefined || typeof qs !== "string") return;
    const sid = String(id);
    if (seen.has(sid)) return;
    seen.add(sid);
    out.push({
      id: sid,
      queryString: qs,
      statusSummary: statusSummary(o.statusCount ?? o.status ?? o.state),
    });
  }

  function visit(node: unknown): void {
    if (node === null || node === undefined) return;
    if (Array.isArray(node)) {
      for (const item of node) {
        if (item && typeof item === "object") {
          const o = item as Record<string, unknown>;
          if (Array.isArray(o.queries)) {
            for (const q of o.queries) {
              if (q && typeof q === "object") maybePush(q as Record<string, unknown>);
            }
          }
        }
        visit(item);
      }
      return;
    }
    if (typeof node !== "object") return;
    const o = node as Record<string, unknown>;
    if (typeof o.queryString === "string" && o.id !== undefined) maybePush(o);
    for (const v of Object.values(o)) visit(v);
  }

  visit(payload);
  return out;
}
