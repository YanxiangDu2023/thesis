import { apiFetch } from "./client";
import type {
  AuthResponse,
  CurrentUserResponse,
  LoginPayload,
  RegisterPayload,
} from "../types/auth";

async function readJson<T>(response: Response): Promise<T> {
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Authentication request failed");
  }

  return result as T;
}

export async function login(payload: LoginPayload): Promise<AuthResponse> {
  const response = await apiFetch("/auth/login", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  return readJson<AuthResponse>(response);
}

export async function register(payload: RegisterPayload): Promise<AuthResponse> {
  const response = await apiFetch("/auth/register", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  return readJson<AuthResponse>(response);
}

export async function getCurrentUser(): Promise<CurrentUserResponse> {
  const response = await apiFetch("/auth/me");
  return readJson<CurrentUserResponse>(response);
}

export async function logout(): Promise<void> {
  const response = await apiFetch("/auth/logout", {
    method: "POST",
  });

  if (!response.ok) {
    const result = await response.json();
    throw new Error(result.detail || "Logout failed");
  }
}
