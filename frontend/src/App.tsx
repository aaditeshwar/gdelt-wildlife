import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import DashboardPage from "./DashboardPage";
import MapPage from "./MapPage";

export default function App() {
  const raw = import.meta.env.BASE_URL || "/";
  const basename = raw.replace(/\/+$/, "") || "/";

  return (
    <BrowserRouter basename={basename}>
      <Routes>
        <Route path="/" element={<MapPage />} />
        <Route path="/dashboard" element={<DashboardPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </BrowserRouter>
  );
}
