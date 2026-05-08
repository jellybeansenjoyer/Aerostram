import { Component, type ErrorInfo, type ReactNode } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

type Props = { children: ReactNode };
type State = { error: Error | null };

/** FE-11 — catches render errors in routed page content */
export class RouteErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("Route render error:", error, info.componentStack);
  }

  render() {
    if (this.state.error) {
      return (
        <Card className="max-w-xl border-red-500/35 bg-red-500/5">
          <CardHeader>
            <CardTitle id="route-error-title">This view crashed</CardTitle>
            <CardDescription>
              Reload the page or restart services. If backends are down, start the stack with{" "}
              <code className="rounded bg-muted px-1 font-mono text-xs">docker compose up -d</code>.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <pre
              className="max-h-36 overflow-auto rounded-md border border-border bg-muted/60 p-3 font-mono text-xs text-red-700 dark:text-red-300"
              aria-labelledby="route-error-title"
            >
              {this.state.error.message}
            </pre>
            <Button type="button" variant="secondary" onClick={() => this.setState({ error: null })}>
              Try again
            </Button>
          </CardContent>
        </Card>
      );
    }
    return this.props.children;
  }
}
