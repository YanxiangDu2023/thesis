import { useEffect, useState, type FormEvent } from "react";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import RedirectIfAuthenticated from "./components/auth/RedirectIfAuthenticated";
import RequireAuth from "./components/auth/RequireAuth";
import AppLayout from "./components/layout/AppLayout";
import {
  API_BASE_URL,
  clearStoredSitePassword,
  getStoredSitePassword,
  persistSitePassword,
} from "./api/client";
import { AuthProvider } from "./context/AuthContext";
import HomePage from "./pages/HomePage";
import PipelineViewerPage from "./pages/PipelineViewerPage";
import MatrixSubmissionPage from "./pages/MatrixSubmissionPage";
import OthUploadPage from "./pages/OthUploadPage";
import CrpUploadPage from "./pages/CrpUploadPage";
import UploadResultPage from "./pages/UploadResultPage";
import LayerDetailPage from "./pages/LayerDetailPage";
import AuthPage from "./pages/AuthPage";
import TotalMarketCalculationPage from "./pages/TotalMarketCalculationPage";

function readBoolEnv(value: string | undefined, fallback: boolean): boolean {
  if (value === undefined) {
    return fallback;
  }

  return ["1", "true", "yes", "on"].includes(value.trim().toLowerCase());
}

const PASSWORD_GATE_ENABLED = readBoolEnv(import.meta.env.VITE_PASSWORD_GATE_ENABLED, false);

function App() {
  const [sitePassword, setSitePassword] = useState("");
  const [sitePasswordError, setSitePasswordError] = useState("");
  const [isUnlocking, setIsUnlocking] = useState(false);
  const [isCheckingStoredPassword, setIsCheckingStoredPassword] = useState(PASSWORD_GATE_ENABLED);
  const [isUnlocked, setIsUnlocked] = useState(!PASSWORD_GATE_ENABLED);

  useEffect(() => {
    if (!PASSWORD_GATE_ENABLED) {
      setIsCheckingStoredPassword(false);
      setIsUnlocked(true);
      return;
    }

    const storedPassword = getStoredSitePassword();
    if (!storedPassword) {
      setIsCheckingStoredPassword(false);
      setIsUnlocked(false);
      return;
    }

    let isActive = true;

    async function validateStoredPassword(password: string) {
      try {
        const response = await fetch(`${API_BASE_URL}/`, {
          headers: {
            "X-Site-Password": password,
          },
        });

        if (!isActive) {
          return;
        }

        if (response.ok) {
          setIsUnlocked(true);
          setSitePasswordError("");
        } else {
          clearStoredSitePassword();
          setIsUnlocked(false);
          setSitePasswordError("The saved site password expired. Please enter it again.");
        }
      } catch {
        if (!isActive) {
          return;
        }
        setIsUnlocked(false);
        setSitePasswordError("Unable to verify the site password right now. Please try again.");
      } finally {
        if (isActive) {
          setIsCheckingStoredPassword(false);
        }
      }
    }

    void validateStoredPassword(storedPassword);

    return () => {
      isActive = false;
    };
  }, []);

  async function handleUnlock(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmed = sitePassword.trim();
    if (!trimmed) {
      setSitePasswordError("Please enter the access password.");
      return;
    }

    setIsUnlocking(true);
    setSitePasswordError("");

    try {
      const response = await fetch(`${API_BASE_URL}/`, {
        headers: {
          "X-Site-Password": trimmed,
        },
      });

      if (!response.ok) {
        setSitePasswordError("Access password is incorrect.");
        return;
      }

      persistSitePassword(trimmed);
      setIsUnlocked(true);
      setSitePassword("");
    } catch {
      setSitePasswordError("Unable to reach the backend. Please try again.");
    } finally {
      setIsUnlocking(false);
    }
  }

  if (isCheckingStoredPassword) {
    return (
      <div className="site-gate-shell">
        <div className="site-gate-card">
          <p className="section-tag">Protected Access</p>
          <h1 className="section-title">Checking Site Access</h1>
          <p className="section-description">Verifying the saved site password with the backend.</p>
        </div>
      </div>
    );
  }

  if (!isUnlocked) {
    return (
      <div className="site-gate-shell">
        <form className="site-gate-card" onSubmit={handleUnlock}>
          <p className="section-tag">Protected Access</p>
          <h1 className="section-title">Enter Site Password</h1>
          <p className="section-description">
            This test environment is temporarily protected by a shared password.
          </p>
          <input
            className="site-gate-input"
            type="password"
            value={sitePassword}
            onChange={(event) => setSitePassword(event.target.value)}
            placeholder="Site password"
            autoComplete="current-password"
          />
          {sitePasswordError ? <p className="site-gate-error">{sitePasswordError}</p> : null}
          <button className="site-gate-button" type="submit" disabled={isUnlocking}>
            {isUnlocking ? "Checking..." : "Unlock"}
          </button>
        </form>
      </div>
    );
  }

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
            <Route path="/total-market-calculation" element={<TotalMarketCalculationPage />} />
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
