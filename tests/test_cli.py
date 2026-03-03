"""Unit tests for the CLI parser wiring."""

from tortoisemarch.cli import _build_parser


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
