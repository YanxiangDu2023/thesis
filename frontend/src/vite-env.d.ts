/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_DISABLE_AUTH?: string;
  readonly VITE_API_BASE_URL?: string;
  readonly VITE_PASSWORD_GATE_ENABLED?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
