import os


def _read_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def is_auth_bypassed() -> bool:
    return _read_bool_env("DISABLE_AUTH", True)


def get_database_url() -> str:
    return os.getenv("DATABASE_URL", "").strip()


def get_cors_allow_origins() -> list[str]:
    raw_value = os.getenv("CORS_ALLOW_ORIGINS", "*").strip()
    if raw_value == "":
        return ["*"]
    if raw_value == "*":
        return ["*"]
    return [origin.strip() for origin in raw_value.split(",") if origin.strip()]


def get_upload_root_dir() -> str:
    return os.getenv("UPLOAD_ROOT_DIR", "uploads").strip() or "uploads"
