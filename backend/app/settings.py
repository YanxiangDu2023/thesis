import os


def _read_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def is_auth_bypassed() -> bool:
    return _read_bool_env("DISABLE_AUTH", True)
