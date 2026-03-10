"""Tests for runtime configuration override resolution."""

from pathlib import Path

from tortoisemarch.conf import resolve_runtime_config


def test_resolve_runtime_config_allows_location_override(monkeypatch) -> None:
    """We allow callers to override only the migration directory."""
    config = {
        "tortoise_orm": {"connections": {"default": "postgres://config"}},
        "location": Path("config-migrations"),
        "include_locations": [{"label": "lib", "path": Path("vendor/lib")}],
    }
    monkeypatch.setattr("tortoisemarch.conf.load_config", lambda: config)

    tortoise_conf, location, include_locations = resolve_runtime_config(
        location=Path("cli-migrations"),
    )

    assert tortoise_conf == config["tortoise_orm"]
    assert location == Path("cli-migrations")
    assert include_locations == config["include_locations"]


def test_resolve_runtime_config_allows_tortoise_override(monkeypatch) -> None:
    """We allow callers to override only the Tortoise ORM config."""
    config = {
        "tortoise_orm": {"connections": {"default": "postgres://config"}},
        "location": Path("config-migrations"),
        "include_locations": [{"label": "lib", "path": Path("vendor/lib")}],
    }
    monkeypatch.setattr("tortoisemarch.conf.load_config", lambda: config)

    explicit_conf = {"connections": {"default": "sqlite://:memory:"}}
    tortoise_conf, location, include_locations = resolve_runtime_config(
        tortoise_conf=explicit_conf,
    )

    assert tortoise_conf == explicit_conf
    assert location == Path("config-migrations")
    assert include_locations == config["include_locations"]


def test_resolve_runtime_config_skips_project_config_when_fully_explicit(
    monkeypatch,
) -> None:
    """We keep fully explicit API calls self-contained."""

    def _unexpected_load() -> None:
        msg = "load_config should not run for fully explicit overrides"
        raise AssertionError(msg)

    monkeypatch.setattr("tortoisemarch.conf.load_config", _unexpected_load)

    explicit_conf = {"connections": {"default": "sqlite://:memory:"}}
    tortoise_conf, location, include_locations = resolve_runtime_config(
        tortoise_conf=explicit_conf,
        location=Path("custom-migrations"),
    )

    assert tortoise_conf == explicit_conf
    assert location == Path("custom-migrations")
    assert include_locations == []
