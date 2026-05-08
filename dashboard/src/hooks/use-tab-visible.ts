import { useEffect, useState } from "react";

/** True when the browser tab is visible (`visibilityState === "visible"`). */
export function useTabVisible() {
  const [visible, setVisible] = useState(() =>
    typeof document !== "undefined" ? document.visibilityState === "visible" : true,
  );

  useEffect(() => {
    const onVis = () => setVisible(document.visibilityState === "visible");
    document.addEventListener("visibilitychange", onVis);
    return () => document.removeEventListener("visibilitychange", onVis);
  }, []);

  return visible;
}
