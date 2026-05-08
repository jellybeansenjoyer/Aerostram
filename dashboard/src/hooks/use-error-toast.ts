import { useEffect, useRef } from "react";
import { toast } from "sonner";
import { ApiError } from "@/lib/api/client";

/** Fire a single toast when a query transitions into error (until it succeeds again). */
export function useErrorToast(isError: boolean, label: string, error: unknown) {
  const shownRef = useRef(false);
  useEffect(() => {
    if (isError && !shownRef.current) {
      let detail = "Request failed";
      if (error instanceof ApiError) detail = `${error.status} ${error.message}`;
      else if (error instanceof Error) detail = error.message;
      toast.error(`${label}: ${detail}`);
      shownRef.current = true;
    }
    if (!isError) shownRef.current = false;
  }, [isError, label, error]);
}
