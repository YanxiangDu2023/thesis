import { NavLink } from "react-router-dom";
import { useAuth } from "../../context/AuthContext";

function Navbar() {
  const { user, token, logout, isBypassed, isAuthenticated } = useAuth();

  const userName = user?.full_name || (isBypassed ? "Development Mode" : "User");
  const userDetail = isAuthenticated
    ? user?.email || ""
    : isBypassed
      ? "Login optional during local development"
      : "";

  return (
    <header className="navbar">
      <div className="navbar__brand">
        <div className="navbar__logo">
          <img
            src="/volvo_construction_equipment_logo.jpg"
            alt="Volvo logo"
            className="brand-image-logo brand-image-logo--navbar"
          />
        </div>
        <div>
          <h1 className="navbar__title">TMC Process Visualizer</h1>
          <p className="navbar__subtitle">Volvo CE Thesis Prototype</p>
        </div>
      </div>

      <nav className="navbar__nav">
        <NavLink
          to="/"
          className={({ isActive }: { isActive: boolean }) =>
            isActive ? "nav-link nav-link--active" : "nav-link"
          }
        >
          Home
        </NavLink>

        <NavLink
          to="/pipeline"
          className={({ isActive }: { isActive: boolean }) =>
            isActive ? "nav-link nav-link--active" : "nav-link"
          }
        >
          Pipeline
        </NavLink>

        <div className="navbar__session">
          <div className="navbar__user-chip">
            <span className="navbar__user-name">{userName}</span>
            <span className="navbar__user-email">{userDetail}</span>
          </div>
          {token ? (
            <button type="button" className="nav-action" onClick={() => void logout()}>
              Sign out
            </button>
          ) : null}
        </div>
      </nav>
    </header>
  );
}

export default Navbar;
