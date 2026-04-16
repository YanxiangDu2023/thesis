const configuredApiBaseUrl = import.meta.env.VITE_API_BASE_URL?.trim();
export const API_BASE_URL = (
  configuredApiBaseUrl && configuredApiBaseUrl.length > 0
    ? configuredApiBaseUrl
    : "http://127.0.0.1:8001"
).replace(/\/+$/, "");
export const AUTH_TOKEN_STORAGE_KEY = "tmc_auth_token";
export const SITE_PASSWORD_STORAGE_KEY = "tmc_site_password";

export function getStoredAuthToken(): string | null {
  return window.localStorage.getItem(AUTH_TOKEN_STORAGE_KEY);
}

export function persistAuthToken(token: string): void {
  window.localStorage.setItem(AUTH_TOKEN_STORAGE_KEY, token);
}

export function clearStoredAuthToken(): void {
  window.localStorage.removeItem(AUTH_TOKEN_STORAGE_KEY);
}

export function getStoredSitePassword(): string | null {
  return window.localStorage.getItem(SITE_PASSWORD_STORAGE_KEY);
}

export function persistSitePassword(password: string): void {
  window.localStorage.setItem(SITE_PASSWORD_STORAGE_KEY, password);
}

export function clearStoredSitePassword(): void {
  window.localStorage.removeItem(SITE_PASSWORD_STORAGE_KEY);
}

export async function apiFetch(path: string, init: RequestInit = {}): Promise<Response> {
  const headers = new Headers(init.headers);
  const token = getStoredAuthToken();
  const sitePassword = getStoredSitePassword();

  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  }

  if (sitePassword) {
    headers.set("X-Site-Password", sitePassword);
  }

  return fetch(`${API_BASE_URL}${path}`, {
    ...init,
    headers,
  });
}
