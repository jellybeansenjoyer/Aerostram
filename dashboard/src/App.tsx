import { Navigate, Route, Routes } from "react-router-dom";
import { AnalyticsPage } from "@/pages/AnalyticsPage";
import { CdcPage } from "@/pages/CdcPage";
import { MlPage } from "@/pages/MlPage";
import { AppShell } from "@/layout/AppShell";
import { OverviewPage } from "@/pages/OverviewPage";
import { PipelinePage } from "@/pages/PipelinePage";
import { PitWallPage } from "@/pages/PitWallPage";
import { PlaceholderPage } from "@/pages/PlaceholderPage";
import { SchemasPage } from "@/pages/SchemasPage";
import { SettingsPage } from "@/pages/SettingsPage";

export default function App() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route index element={<OverviewPage />} />
        <Route path="pipeline" element={<PipelinePage />} />
        <Route
          path="kafka"
          element={
            <PlaceholderPage title="Kafka & topics" description="Topics inventory and Kafka UI links." />
          }
        />
        <Route path="schemas" element={<SchemasPage />} />
        <Route path="cdc" element={<CdcPage />} />
        <Route path="analytics" element={<AnalyticsPage />} />
        <Route path="ml" element={<MlPage />} />
        <Route path="pit-wall" element={<PitWallPage />} />
        <Route path="settings" element={<SettingsPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  );
}
