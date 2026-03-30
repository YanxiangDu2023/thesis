import { BrowserRouter, Route, Routes } from "react-router-dom";
import RedirectIfAuthenticated from "./components/auth/RedirectIfAuthenticated";
import RequireAuth from "./components/auth/RequireAuth";
import AppLayout from "./components/layout/AppLayout";
import { AuthProvider } from "./context/AuthContext";
import HomePage from "./pages/HomePage";
import PipelineViewerPage from "./pages/PipelineViewerPage";
import MatrixSubmissionPage from "./pages/MatrixSubmissionPage";
import OthUploadPage from "./pages/OthUploadPage";
import CrpUploadPage from "./pages/CrpUploadPage";
import UploadResultPage from "./pages/UploadResultPage";
import LayerDetailPage from "./pages/LayerDetailPage";
import AuthPage from "./pages/AuthPage";

function App() {
  return (
    <AuthProvider>
      <BrowserRouter>
        <Routes>
          <Route
            path="/auth"
            element={
              <RedirectIfAuthenticated>
                <AuthPage />
              </RedirectIfAuthenticated>
            }
          />
          <Route
            element={
              <RequireAuth>
                <AppLayout />
              </RequireAuth>
            }
          >
            <Route path="/" element={<HomePage />} />
            <Route path="/matrix" element={<MatrixSubmissionPage />} />
            <Route path="/pipeline" element={<PipelineViewerPage />} />
            <Route path="/upload/oth" element={<OthUploadPage />} />
            <Route path="/upload/crp" element={<CrpUploadPage />} />
            <Route path="/layers/:layerCode" element={<LayerDetailPage />} />
            <Route path="/uploads/:uploadRunId/result" element={<UploadResultPage />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </AuthProvider>
  );
}

export default App;
