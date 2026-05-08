import {
  Activity,
  ChevronLeft,
  ChevronRight,
  Database,
  Gauge,
  LayoutDashboard,
  LineChart,
  Radio,
  Settings,
  Workflow,
  Zap,
} from "lucide-react";
import { useState } from "react";
import { NavLink, Outlet } from "react-router-dom";
import { RouteErrorBoundary } from "@/components/RouteErrorBoundary";
import { ThemeToggle } from "@/components/theme/ThemeToggle";
import { Button } from "@/components/ui/button";
import { DASHBOARD_VERSION } from "@/lib/constants";
import { cn } from "@/lib/utils";

const NAV = [
  { to: "/", label: "Overview", icon: LayoutDashboard },
  { to: "/pipeline", label: "Pipeline", icon: Workflow },
  { to: "/kafka", label: "Kafka", icon: Radio },
  { to: "/schemas", label: "Schemas", icon: Database },
  { to: "/cdc", label: "CDC", icon: Zap },
  { to: "/analytics", label: "Analytics", icon: LineChart },
  { to: "/ml", label: "ML", icon: Activity },
  { to: "/pit-wall", label: "Pit Wall", icon: Gauge },
  { to: "/settings", label: "Settings", icon: Settings },
] as const;

export function AppShell() {
  const [collapsed, setCollapsed] = useState(false);

  return (
    <div className="flex min-h-screen bg-background text-foreground">
      <a
        href="#main-content"
        className="fixed left-4 top-4 z-[100] -translate-y-14 rounded-md bg-accent px-4 py-2 text-sm font-medium text-accent-foreground opacity-0 shadow-md transition-[opacity,transform] duration-200 focus-visible:translate-y-0 focus-visible:opacity-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-background"
      >
        Skip to main content
      </a>
      <aside
        aria-label="Primary navigation"
        className={cn(
          "sticky top-0 flex h-screen flex-col border-r border-border bg-sidebar text-sidebar-foreground transition-[width] duration-200",
          collapsed ? "w-[72px]" : "w-56",
        )}
      >
        <div className="flex items-center justify-between gap-2 border-b border-border px-3 py-4">
          {!collapsed ? (
            <span className="font-display text-xs font-semibold uppercase tracking-widest text-accent">
              AeroStream
            </span>
          ) : (
            <span className="mx-auto font-display text-xs font-bold text-accent">A</span>
          )}
          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="h-8 w-8 shrink-0"
            onClick={() => setCollapsed((c) => !c)}
            aria-expanded={!collapsed}
            aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          >
            {collapsed ? <ChevronRight className="h-4 w-4" /> : <ChevronLeft className="h-4 w-4" />}
          </Button>
        </div>
        <nav className="flex flex-1 flex-col gap-1 p-2">
          {NAV.map(({ to, label, icon: Icon }) => (
            <NavLink
              key={to}
              to={to}
              end={to === "/"}
              title={collapsed ? label : undefined}
              className={({ isActive }) =>
                cn(
                  "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
                  "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-background",
                  isActive
                    ? "bg-muted text-foreground"
                    : "text-muted-foreground hover:bg-muted/60 hover:text-foreground",
                  collapsed && "justify-center px-2",
                )
              }
            >
              <Icon className="h-4 w-4 shrink-0" />
              {!collapsed ? <span>{label}</span> : null}
            </NavLink>
          ))}
        </nav>
      </aside>

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="sticky top-0 z-40 flex flex-wrap items-center justify-between gap-4 border-b border-border bg-background/95 px-6 py-4 backdrop-blur supports-[backdrop-filter]:bg-background/80">
          <div>
            <h1 className="font-display text-lg font-semibold tracking-tight">AeroStream Control</h1>
            <p className="text-xs text-muted-foreground">Operations dashboard · local stack</p>
          </div>
          <ThemeToggle />
        </header>

        <main id="main-content" className="flex-1 px-6 py-6" tabIndex={-1}>
          <RouteErrorBoundary>
            <Outlet />
          </RouteErrorBoundary>
        </main>

        <footer className="border-t border-border px-6 py-4 text-xs text-muted-foreground">
          AeroStream dashboard v{DASHBOARD_VERSION} · ports & proxy documented in{" "}
          <code className="rounded bg-muted px-1 py-0.5 font-mono text-[11px]">dashboard/README.md</code>
        </footer>
      </div>
    </div>
  );
}
