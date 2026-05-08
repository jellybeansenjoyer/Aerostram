import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ThemeProvider } from "next-themes";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import App from "@/App";
import { AppToaster } from "@/components/AppToaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { ServicesProvider } from "@/context/ServicesContext";
import "./index.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <ThemeProvider attribute="class" defaultTheme="system" enableSystem storageKey="aerostream.theme">
        <TooltipProvider delayDuration={200}>
          <ServicesProvider>
            <BrowserRouter>
              <App />
            </BrowserRouter>
          </ServicesProvider>
        </TooltipProvider>
        <AppToaster />
      </ThemeProvider>
    </QueryClientProvider>
  </StrictMode>,
);
