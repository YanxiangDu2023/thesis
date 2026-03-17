import { BrowserRouter, Routes, Route } from "react-router-dom";
import Navbar from "./components/layout/Navbar";
import HomePage from "./pages/HomePage";
import PipelineViewerPage from "./pages/PipelineViewerPage";
import MatrixSubmissionPage from "./pages/MatrixSubmissionPage";
import OthUploadPage from "./pages/OthUploadPage";
import CrpUploadPage from "./pages/CrpUploadPage";
import UploadResultPage from "./pages/UploadResultPage";

function App() {
  return (
 
    <BrowserRouter>
      <div className="app-container">
        <Navbar /> 
        <main className="main-content">
          <Routes>
            <Route path="/" element={<HomePage />} />
            <Route path="/matrix" element={<MatrixSubmissionPage />} />
            <Route path="/pipeline" element={<PipelineViewerPage />} />
            <Route path="/upload/oth" element={<OthUploadPage />} />
            <Route path="/upload/crp" element={<CrpUploadPage />} />
            <Route path="/uploads/:uploadRunId/result" element={<UploadResultPage />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}

export default App;
