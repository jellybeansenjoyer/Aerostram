import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import {
  STORAGE_KEY,
  defaultServicesSettings,
  parseStoredSettings,
  validateMerged,
  type ServicesSettings,
} from "@/lib/settings-schema";

type ServicesContextValue = {
  settings: ServicesSettings;
  /** Partial update — validated; on failure sets field errors */
  updateSettings: (partial: Partial<ServicesSettings>) => boolean;
  /** Replace entire validated snapshot (e.g. Settings form Save) */
  commitSettings: (next: ServicesSettings) => boolean;
  resetToDefaults: () => void;
  fieldErrors: Record<string, string>;
  clearFieldErrors: () => void;
};

const ServicesContext = createContext<ServicesContextValue | null>(null);

export function ServicesProvider({ children }: { children: ReactNode }) {
  const [settings, setSettings] = useState<ServicesSettings>(() => {
    if (typeof window === "undefined") return defaultServicesSettings;
    return parseStoredSettings(localStorage.getItem(STORAGE_KEY));
  });
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});

  const persist = useCallback((next: ServicesSettings) => {
    setSettings(next);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
  }, []);

  const updateSettings = useCallback(
    (partial: Partial<ServicesSettings>) => {
      const merged = { ...settings, ...partial };
      const result = validateMerged(merged);
      if (!result.success) {
        setFieldErrors(result.errors);
        return false;
      }
      setFieldErrors({});
      persist(result.data);
      return true;
    },
    [settings, persist],
  );

  const commitSettings = useCallback(
    (next: ServicesSettings) => {
      const result = validateMerged(next);
      if (!result.success) {
        setFieldErrors(result.errors);
        return false;
      }
      setFieldErrors({});
      persist(result.data);
      return true;
    },
    [persist],
  );

  const resetToDefaults = useCallback(() => {
    setFieldErrors({});
    persist(defaultServicesSettings);
  }, [persist]);

  const clearFieldErrors = useCallback(() => setFieldErrors({}), []);

  const value = useMemo(
    () => ({
      settings,
      updateSettings,
      commitSettings,
      resetToDefaults,
      fieldErrors,
      clearFieldErrors,
    }),
    [
      settings,
      updateSettings,
      commitSettings,
      resetToDefaults,
      fieldErrors,
      clearFieldErrors,
    ],
  );

  return (
    <ServicesContext.Provider value={value}>{children}</ServicesContext.Provider>
  );
}

export function useServices(): ServicesContextValue {
  const ctx = useContext(ServicesContext);
  if (!ctx) throw new Error("useServices must be used within ServicesProvider");
  return ctx;
}
