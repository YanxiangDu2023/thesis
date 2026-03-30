import type { ReactNode } from "react";
import { Navigate, useLocation } from "react-router-dom";
import { useAuth } from "../../context/AuthContext";

export default function RequireAuth({ children }: { children: ReactNode }) {
  const { isAuthenticated, isLoading, isBypassed } = useAuth();
  const location = useLocation();

  if (isBypassed) {
    return <>{children}</>;
  }

  if (isLoading) {
    return (
      <div className="auth-loading-shell">
        <div className="auth-loading-card">
          <p className="section-tag">Session</p>
          <h1 className="section-title">Loading your workspace</h1>
          <p className="section-description">
            We are restoring your sign-in status before opening the app.
          </p>
        </div>
      </div>
    );
  }

  if (!isAuthenticated) {
    return <Navigate to="/auth" replace state={{ from: location.pathname }} />;
  }

  return <>{children}</>;
}
