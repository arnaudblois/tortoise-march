"""Unit tests for the CLI parser wiring."""

import sys

import pytest

from tortoisemarch import cli
from tortoisemarch.cli import _build_parser
from tortoisemarch.exceptions import ConfigError


def test_migrate_parser_accepts_rewrite_history_flag():
    """Migrate parser should accept explicit history rewrite mode."""
    parser = _build_parser()

    args = parser.parse_args(["migrate", "--fake", "--rewrite-history"])

    assert args.command == "migrate"
    assert args.fake is True
    assert args.rewrite_history is True


def test_migrate_parser_defaults_rewrite_history_to_false():
    """History rewrite mode should stay opt-in for migration safety."""
    parser = _build_parser()

    args = parser.parse_args(["migrate"])

    assert args.command == "migrate"
    assert args.rewrite_history is False


def test_show_sql_parser_accepts_migration_prefix():
    """show-sql should accept one migration identifier argument."""
    parser = _build_parser()

    args = parser.parse_args(["show-sql", "0003"])

    assert args.command == "show-sql"
    assert args.migration == "0003"


def test_main_prints_clean_library_errors(monkeypatch, capsys):
    """The CLI should render library errors without a traceback."""

    async def fake_migrate(**_kwargs):
        msg = "config is missing"
        raise ConfigError(msg)

    monkeypatch.setattr(cli, "migrate", fake_migrate)
    monkeypatch.setattr(sys, "argv", ["tortoisemarch", "migrate"])

    with pytest.raises(SystemExit) as exc_info:
        cli.main()

    captured = capsys.readouterr()

    assert exc_info.value.code == 1
    assert "config is missing" in captured.err
    assert "Traceback" not in captured.err
