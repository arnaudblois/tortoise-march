"""Configuration loader for TortoiseMarch.

Reads settings from `pyproject.toml` under `[tool.tortoisemarch]` or from a
`.tortoisemarch.cfg` file, with optional environment overrides. Returns a dict
containing:
    - 'tortoise_orm'       : dict   # Tortoise ORM config
    - 'location'           : Path   # absolute path to the migrations directory
    - 'include_locations'  : list   # additional migration dirs (label + path)
    - 'src_folder'         : Path   # absolute path (optionally) added to sys.path

Environment overrides:
    TORTOISEMARCH_SRC_FOLDER   - path to prepend to sys.path
    TORTOISEMARCH_TORTOISE_ORM - dotted or colon path to a module attribute
    TORTOISEMARCH_LOCATION     - migrations directory
Notes:
    When using `.tortoisemarch.cfg`, `include_locations` must be JSON.
"""

import configparser
import importlib
import json
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


def _read_pyproject(pyproject_path: Path) -> dict | None:
    """Read and parse a pyproject.toml as a dict."""
    if not pyproject_path.exists():
        return None
    with pyproject_path.open("rb") as f:
        return tomllib.load(f)


def _read_cfg(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    parser = configparser.ConfigParser()
    parser.read(path, encoding="utf-8")
    if "tortoisemarch" not in parser:
        return None
    return dict(parser["tortoisemarch"])


def _load_raw_config(pyproject_path: Path) -> dict[str, Any]:
    """Load config from pyproject.toml or .tortoisemarch.cfg."""
    data = _read_pyproject(pyproject_path)
    cfg: dict[str, Any] | None = None
    config_root = pyproject_path.parent

    if data is not None:
        cfg = data.get("tool", {}).get("tortoisemarch")

    if cfg is None:
        cfg_path = config_root / ".tortoisemarch.cfg"
        cfg = _read_cfg(cfg_path)
        if cfg is None:
            msg = (
                "Missing [tool.tortoisemarch] section in pyproject.toml "
                "and no .tortoisemarch.cfg found."
            )
            raise KeyError(msg)
    return cfg


def _resolve_src_folder(cfg: dict[str, Any], *, base_dir: Path) -> Path:
    """Resolve and prepend src_folder to sys.path."""
    src_folder_str = os.environ.get(
        "TORTOISEMARCH_SRC_FOLDER",
        cfg.get("src_folder", "."),
    )
    src_folder = (base_dir / Path(src_folder_str)).resolve()
    src_folder_str_abs = str(src_folder)
    if src_folder_str_abs not in sys.path:
        sys.path.insert(0, src_folder_str_abs)
    return src_folder


def _resolve_tortoise_orm(cfg: dict[str, Any]) -> dict[str, Any]:
    """Resolve the tortoise_orm config object from config/env."""
    tortoise_spec = os.environ.get(
        "TORTOISEMARCH_TORTOISE_ORM",
        cfg.get("tortoise_orm"),
    )
    if not tortoise_spec:
        msg = (
            "Missing 'tortoise_orm' in config and no "
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

    tortoise_orm = tortoise_obj() if callable(tortoise_obj) else tortoise_obj
    if not isinstance(tortoise_orm, Mapping):
        msg = (
            f"Resolved tortoise_orm '{tortoise_spec}' is not a mapping "
            f"(got {type(tortoise_orm).__name__})."
        )
        raise TypeError(msg)
    return dict(tortoise_orm)


def _resolve_location(cfg: dict[str, Any], *, base_dir: Path) -> Path:
    """Resolve the migrations directory path."""
    loc_str = os.environ.get(
        "TORTOISEMARCH_LOCATION",
        cfg.get("location", "migrations"),
    )
    return (base_dir / Path(loc_str)).resolve()


def _parse_include_locations(
    cfg: dict[str, Any],
    *,
    base_dir: Path,
) -> list[dict[str, Any]]:
    """Normalize and validate include_locations entries."""
    include_raw = _normalize_include_raw(cfg.get("include_locations", []))
    include_locations: list[dict[str, Any]] = []
    seen_labels: set[str] = set()
    for entry in include_raw:
        label, path = _parse_include_entry(entry, base_dir=base_dir)
        if label in seen_labels:
            msg = f"Duplicate include_locations label: {label!r}"
            raise TypeError(msg)
        seen_labels.add(label)
        include_locations.append({"label": label, "path": path})
    return include_locations


def _normalize_include_raw(value: Any) -> list[Any]:
    """Normalize include_locations to a list."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            msg = "include_locations in .tortoisemarch.cfg must be valid JSON."
            raise TypeError(msg) from exc
    if value is None:
        value = []
    if not isinstance(value, list):
        msg = "'include_locations' must be a list in [tool.tortoisemarch]"
        raise TypeError(msg)
    return value


def _parse_include_entry(entry: Any, *, base_dir: Path) -> tuple[str, Path]:
    """Parse a single include_locations entry into (label, path)."""
    if isinstance(entry, str):
        label = Path(entry).name
        path_str = entry
    elif isinstance(entry, dict):
        label = entry.get("label")
        path_str = entry.get("path")
    else:
        msg = (
            "Each include_locations entry must be a string path or "
            "a table with 'label' and 'path'."
        )
        raise TypeError(msg)

    if not label or not isinstance(label, str):
        msg = "include_locations entries require a string 'label'."
        raise TypeError(msg)
    if not path_str or not isinstance(path_str, str):
        msg = f"include_locations[{label!r}] requires a string 'path'."
        raise TypeError(msg)
    return label, (base_dir / Path(path_str)).resolve()


def load_config(pyproject_path: Path | None = None) -> dict[str, Any]:
    """Load TortoiseMarch configuration.

    Order of precedence:
      1) Environment variables (if set)
      2) pyproject.toml -> [tool.tortoisemarch] table
      3) .tortoisemarch.cfg -> [tortoisemarch] section
      3) Sensible defaults (where possible)

    Args:
        pyproject_path: Optional explicit path to pyproject.toml. If omitted,
                        'pyproject.toml' in the current working directory is used.

    Returns:
        A dict with keys:
            - 'tortoise_orm' (dict)
            - 'location' (Path): Absolute path to this project's primary
              migrations directory. Defaults to
              `migrations` (relative to `pyproject.toml`).
            - 'include_locations' (list[dict]): Additional migrations
              directories to include, each as `{"label": str, "path": Path}`
              where `path` is absolute.
            - 'src_folder' (Path): Absolute path prepended to `sys.path`
              before resolving `tortoise_orm`. Defaults to the directory
              containing `pyproject.toml` when not configured.

    Raises:
        KeyError, TypeError, FileNotFoundError with clear messages when configuration
        is missing or invalid.

    """
    pyproject_path = pyproject_path or Path("pyproject.toml")
    cfg = _load_raw_config(pyproject_path)
    base_dir = pyproject_path.parent
    src_folder = _resolve_src_folder(cfg, base_dir=base_dir)
    tortoise_orm = _resolve_tortoise_orm(cfg)
    location = _resolve_location(cfg, base_dir=base_dir)
    include_locations = _parse_include_locations(cfg, base_dir=base_dir)

    return {
        "tortoise_orm": tortoise_orm,
        "location": location,
        "include_locations": include_locations,
        "src_folder": src_folder,
    }
