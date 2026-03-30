import type { ReactNode } from "react";
import { Navigate } from "react-router-dom";
import { useAuth } from "../../context/AuthContext";

export default function RedirectIfAuthenticated({ children }: { children: ReactNode }) {
  const { isAuthenticated, isLoading, isBypassed } = useAuth();

  if (isLoading) {
    return (
      <div className="auth-loading-shell">
        <div className="auth-loading-card">
          <p className="section-tag">Session</p>
          <h1 className="section-title">Loading your workspace</h1>
          <p className="section-description">
            We are checking whether you already have an active session.
          </p>
        </div>
      </div>
    );
  }

  if (isAuthenticated) {
    return <Navigate to="/" replace />;
  }

  if (isBypassed) {
    return <>{children}</>;
  }

  return <>{children}</>;
}
