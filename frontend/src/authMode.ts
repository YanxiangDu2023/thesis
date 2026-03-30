function readBoolEnv(value: string | undefined, fallback: boolean): boolean {
  if (value === undefined) {
    return fallback;
  }

  return ["1", "true", "yes", "on"].includes(value.trim().toLowerCase());
}

export function isAuthBypassed(): boolean {
  const isLocalDev = import.meta.env.DEV && (
    typeof window !== "undefined" &&
    (window.location.hostname === "localhost" ||
      window.location.hostname === "127.0.0.1")
  );

  return readBoolEnv(import.meta.env.VITE_DISABLE_AUTH, isLocalDev);
}
