"""Shared utility helpers for TortoiseMarch."""


def safe_module_fragment(value: str) -> str:
    """Return a safe fragment for module names (ascii letters/digits/_)."""
    safe = []
    for ch in value:
        if ch.isalnum() or ch == "_":
            safe.append(ch)
        else:
            safe.append("_")
    return "".join(safe) or "migrations"
