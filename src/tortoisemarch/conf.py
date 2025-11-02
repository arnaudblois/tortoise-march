"""Configuration loader for TortoiseMarch.

Reads settings from `pyproject.toml` under `[tool.tortoisemarch]`, with optional
environment overrides. Returns a dict containing:
    - 'tortoise_orm' : dict   # Tortoise ORM config
    - 'location'     : Path   # absolute path to the migrations directory
    - 'src_folder'   : Path   # absolute path that was (optionally) added to sys.path

Environment overrides:
    TORTOISEMARCH_SRC_FOLDER   - path to prepend to sys.path
    TORTOISEMARCH_TORTOISE_ORM - dotted or colon path to a module attribute
    TORTOISEMARCH_LOCATION     - migrations directory
"""

import importlib
import os
import sys
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def _resolve_attr(spec: str) -> Any:
    """Resolve 'module.attr' or 'module:attr[.subattr...]' to a Python object.

    Examples:
        'myproj.settings.TORTOISE_ORM'
        'myproj.settings:TORTOISE_ORM'
        'pkg.mod:config.TORTOISE_ORM'

    Returns:
        The resolved object.

    Raises:
        ModuleNotFoundError, AttributeError on bad paths.

    """
    if ":" in spec:
        module_path, attr_path = spec.split(":", 1)
    else:
        module_path, attr_path = spec.rsplit(".", 1)

    module = importlib.import_module(module_path)
    obj: Any = module
    for part in attr_path.split("."):
        obj = getattr(obj, part)
    return obj


def _read_pyproject(pyproject_path: Path) -> dict:
    """Read and parse a pyproject.toml as a dict."""
    if not pyproject_path.exists():
        msg = (
            f"pyproject.toml not found at {pyproject_path!s}. "
            "Create it and add a [tool.tortoisemarch] section."
        )
        raise FileNotFoundError(msg)
    with pyproject_path.open("rb") as f:
        return tomllib.load(f)


def load_config(pyproject_path: Path | None = None) -> dict[str, Any]:
    """Load TortoiseMarch configuration.

    Order of precedence:
      1) Environment variables (if set)
      2) pyproject.toml -> [tool.tortoisemarch] table
      3) Sensible defaults (where possible)

    Args:
        pyproject_path: Optional explicit path to pyproject.toml. If omitted,
                        'pyproject.toml' in the current working directory is used.

    Returns:
        A dict with keys: 'tortoise_orm' (dict), 'location' (Path), 'src_folder' (Path).

    Raises:
        KeyError, TypeError, FileNotFoundError with clear messages when configuration
        is missing or invalid.

    """
    pyproject_path = pyproject_path or Path("pyproject.toml")
    data = _read_pyproject(pyproject_path)

    try:
        cfg = data["tool"]["tortoisemarch"]
    except KeyError as exc:
        msg = "Missing [tool.tortoisemarch] section in pyproject.toml"
        raise KeyError(msg) from exc

    # Resolve src_folder (default ".") with env override
    src_folder_str = os.environ.get(
        "TORTOISEMARCH_SRC_FOLDER",
        cfg.get("src_folder", "."),
    )
    src_folder = (pyproject_path.parent / Path(src_folder_str)).resolve()

    # Prepend to sys.path if not already present
    src_folder_str_abs = str(src_folder)
    if src_folder_str_abs not in sys.path:
        sys.path.insert(0, src_folder_str_abs)

    # Resolve tortoise_orm spec (env override or config)
    tortoise_spec = os.environ.get(
        "TORTOISEMARCH_TORTOISE_ORM",
        cfg.get("tortoise_orm"),
    )
    if not tortoise_spec:
        msg = (
            "Missing 'tortoise_orm' in [tool.tortoisemarch] and no "
            "TORTOISEMARCH_TORTOISE_ORM env var set."
        )
        raise KeyError(msg)

    try:
        tortoise_obj = _resolve_attr(tortoise_spec)
    except (ModuleNotFoundError, AttributeError) as exc:
        msg = ImportError(
            f"Could not resolve tortoise_orm spec '{tortoise_spec}'. "
            "Use 'package.module.ATTRIBUTE' or 'package.module:ATTRIBUTE[.subattr]'.",
        )
        raise ImportError(msg) from exc

    # Allow a callable that returns the dict, or a dict directly
    tortoise_orm = tortoise_obj() if callable(tortoise_obj) else tortoise_obj

    if not isinstance(tortoise_orm, Mapping):
        msg = (
            f"Resolved tortoise_orm '{tortoise_spec}' is not a mapping "
            f"(got {type(tortoise_orm).__name__})."
        )
        raise TypeError(msg)

    # Resolve migrations location (env override or config,
    # default 'tortoisemarch/migrations')
    loc_str = os.environ.get(
        "TORTOISEMARCH_LOCATION",
        cfg.get("location", "tortoisemarch/migrations"),
    )
    # Interpret relative paths relative to the pyproject directory
    location = (pyproject_path.parent / Path(loc_str)).resolve()

    return {
        "tortoise_orm": dict(tortoise_orm),  # copy to avoid accidental mutation
        "location": location,
        "src_folder": src_folder,
    }
