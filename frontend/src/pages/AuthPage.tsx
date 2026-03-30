import { useState, type FormEvent } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { useAuth } from "../context/AuthContext";

type AuthMode = "login" | "register";

type LocationState = {
  from?: string;
};

export default function AuthPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const { login, register, isBypassed } = useAuth();
  const [mode, setMode] = useState<AuthMode>("login");
  const [fullName, setFullName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  const from = (location.state as LocationState | null)?.from || "/";
  const passwordHint = "At least 10 characters, 1 uppercase, 1 special character";

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError("");
    setIsSubmitting(true);

    try {
      if (mode === "register") {
        await register({
          full_name: fullName.trim(),
          email: email.trim(),
          password,
        });
      } else {
        await login({
          email: email.trim(),
          password,
        });
      }

      navigate(from, { replace: true });
    } catch (submitError) {
      const message =
        submitError instanceof Error ? submitError.message : "Unable to authenticate";
      setError(message);
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <div className="auth-shell">
      <section className="auth-brand-panel">
        <div className="auth-brand-panel__content">
          <p className="auth-brand-panel__eyebrow">Volvo CE thesis prototype</p>

          <div className="volvo-mark" aria-hidden="true">
            <div className="volvo-mark__ring" />
            <div className="volvo-mark__arrow-shaft" />
            <div className="volvo-mark__arrow-head" />
            <div className="volvo-mark__word">VOLVO</div>
          </div>

          <div className="auth-brand-panel__copy">
            <h1 className="auth-brand-panel__title">Access the TMC process workspace</h1>
            <p className="auth-brand-panel__text">
              A focused entry point for matrix submission, data uploads, and pipeline
              review with a cleaner Volvo-inspired identity.
            </p>
          </div>
        </div>
      </section>

      <section className="auth-form-panel">
        <div className="auth-form-panel__header">
          <p className="auth-form-panel__eyebrow">Authentication</p>
          <h2 className="auth-form-panel__title">
            {mode === "login" ? "Sign in" : "Register"}
          </h2>
          <p className="auth-form-panel__description">
            {mode === "login"
              ? "Use your registered email and password to enter the platform."
              : "Create a new account for secure access to the platform."}
          </p>
        </div>

        <div className="auth-mode-toggle" role="tablist" aria-label="Authentication mode">
          <button
            type="button"
            className={mode === "login" ? "auth-mode-toggle__button auth-mode-toggle__button--active" : "auth-mode-toggle__button"}
            onClick={() => {
              setMode("login");
              setError("");
            }}
          >
            Login
          </button>
          <button
            type="button"
            className={mode === "register" ? "auth-mode-toggle__button auth-mode-toggle__button--active" : "auth-mode-toggle__button"}
            onClick={() => {
              setMode("register");
              setError("");
            }}
          >
            Register
          </button>
        </div>

        <form className="auth-form" onSubmit={handleSubmit}>
          {mode === "register" ? (
            <label className="auth-field">
              <span>Full name</span>
              <input
                type="text"
                value={fullName}
                onChange={(event) => setFullName(event.target.value)}
                placeholder="Your full name"
                autoComplete="name"
                required
                minLength={2}
              />
            </label>
          ) : null}

          <label className="auth-field">
            <span>Email</span>
            <input
              type="email"
              value={email}
              onChange={(event) => setEmail(event.target.value)}
              placeholder="name@company.com"
              autoComplete="email"
              required
            />
          </label>

          <label className="auth-field">
            <span>Password</span>
            <input
              type="password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              placeholder={passwordHint}
              autoComplete={mode === "login" ? "current-password" : "new-password"}
              required
              minLength={10}
            />
            <small className="auth-field__hint">
              Minimum 10 characters, including one uppercase letter and one special character.
            </small>
          </label>

          {error ? <p className="auth-form__error">{error}</p> : null}

          <button className="btn auth-form__submit" type="submit" disabled={isSubmitting}>
            {isSubmitting
              ? mode === "login"
                ? "Signing in..."
                : "Creating account..."
              : mode === "login"
                ? "Sign in"
                : "Create account"}
          </button>

          {isBypassed ? (
            <button
              type="button"
              className="btn auth-form__skip"
              onClick={() => navigate("/", { replace: true })}
            >
              Skip and enter workspace
            </button>
          ) : null}
        </form>

        <p className="auth-form-panel__footer">
          {isBypassed
            ? "Development shortcut is enabled. You can sign in normally, or skip login and open the workspace directly."
            : mode === "login"
              ? "Only authenticated users can access uploads, reports, and matrix pages."
              : "Your account will be stored in the dedicated auth database, separate from business data."}
        </p>
      </section>
    </div>
  );
}
