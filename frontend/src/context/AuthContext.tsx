import {
  createContext,
  startTransition,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";
import {
  clearStoredAuthToken,
  getStoredAuthToken,
  persistAuthToken,
} from "../api/client";
import {
  getCurrentUser,
  login as loginRequest,
  logout as logoutRequest,
  register as registerRequest,
} from "../api/auth";
import { isAuthBypassed } from "../authMode";
import type { AuthResponse, AuthUser, LoginPayload, RegisterPayload } from "../types/auth";

type AuthContextValue = {
  user: AuthUser | null;
  token: string | null;
  isLoading: boolean;
  isAuthenticated: boolean;
  isBypassed: boolean;
  login: (payload: LoginPayload) => Promise<void>;
  register: (payload: RegisterPayload) => Promise<void>;
  logout: () => Promise<void>;
};

const AuthContext = createContext<AuthContextValue | undefined>(undefined);
const AUTH_BYPASSED = isAuthBypassed();

function applySession(auth: AuthResponse) {
  persistAuthToken(auth.token);
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [token, setToken] = useState<string | null>(() => getStoredAuthToken());
  const [user, setUser] = useState<AuthUser | null>(null);
  const [isLoading, setIsLoading] = useState(Boolean(getStoredAuthToken()));

  useEffect(() => {
    if (!token) {
      setIsLoading(false);
      return;
    }

    let isCancelled = false;
    setIsLoading(true);

    getCurrentUser()
      .then((response) => {
        if (isCancelled) {
          return;
        }

        if (AUTH_BYPASSED && response.user.id === 0) {
          clearStoredAuthToken();
          startTransition(() => {
            setToken(null);
            setUser(null);
          });
          return;
        }

        startTransition(() => {
          setUser(response.user);
        });
      })
      .catch(() => {
        if (isCancelled) {
          return;
        }

        clearStoredAuthToken();
        startTransition(() => {
          setToken(null);
          setUser(null);
        });
      })
      .finally(() => {
        if (!isCancelled) {
          setIsLoading(false);
        }
      });

    return () => {
      isCancelled = true;
    };
  }, [token]);

  async function login(payload: LoginPayload) {
    const auth = await loginRequest(payload);
    applySession(auth);
    startTransition(() => {
      setToken(auth.token);
      setUser(auth.user);
      setIsLoading(false);
    });
  }

  async function register(payload: RegisterPayload) {
    const auth = await registerRequest(payload);
    applySession(auth);
    startTransition(() => {
      setToken(auth.token);
      setUser(auth.user);
      setIsLoading(false);
    });
  }

  async function logout() {
    try {
      if (token) {
        await logoutRequest();
      }
    } finally {
      clearStoredAuthToken();
      startTransition(() => {
        setToken(null);
        setUser(null);
        setIsLoading(false);
      });
    }
  }

  return (
    <AuthContext.Provider
      value={{
        user,
        token,
        isLoading,
        isAuthenticated: Boolean(user && token && user.id !== 0),
        isBypassed: AUTH_BYPASSED,
        login,
        register,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextValue {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
}
