import { NavLink } from "react-router-dom";

function Navbar() {
  return (
    <header className="navbar">
      <div className="navbar__brand">
        <div className="navbar__logo">TMC</div>
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
      </nav>
    </header>
  );
}

export default Navbar;