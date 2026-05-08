/** Parse Prometheus exposition format lines like `metric_name{labels} value`. */
export function parsePrometheusCounters(
  text: string,
  /** Metric names must start with this substring (e.g. `ml_consumer`). */
  namePrefix: string,
): Record<string, number> {
  const out: Record<string, number> = {};
  for (const line of text.split("\n")) {
    const t = line.trim();
    if (!t || t.startsWith("#")) continue;
    const space = t.lastIndexOf(" ");
    if (space <= 0) continue;
    const namePart = t.slice(0, space).trim();
    const valPart = t.slice(space + 1).trim();
    const baseName = namePart.includes("{") ? namePart.slice(0, namePart.indexOf("{")) : namePart;
    if (!baseName.startsWith(namePrefix)) continue;
    const val = Number.parseFloat(valPart);
    if (!Number.isFinite(val)) continue;
    out[baseName] = val;
  }
  return out;
}
