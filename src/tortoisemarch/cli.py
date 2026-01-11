"""Command-line interface for TortoiseMarch.

Subcommands
-----------
- makemigrations: Generate new migration files by diffing current models
  against the last recorded project state. Can also create an empty, data-
  migration stub.

- migrate: Apply unapplied migrations in order, or display SQL without
  executing it (--sql), or mark them as applied (--fake).

Configured as a console script via:

    [tool.poetry.scripts]
    tortoisemarch = "tortoisemarch.cli:main"

So users can run `poetry run tortoisemarch ...` (or just `tortoisemarch`
when installed globally).
"""

import argparse
import asyncio
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import click

from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.makemigrations import makemigrations
from tortoisemarch.migrate import migrate


def _build_parser() -> argparse.ArgumentParser:
    """Construct the top-level argument parser (no parsing yet)."""
    parser = argparse.ArgumentParser(
        prog="tortoisemarch",
        description="Django-style schema migrations for Tortoise ORM.",
    )
    parser.add_argument(
        "-V",
        "--version",
        action="store_true",
        help="Print version and exit.",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=False,
        metavar="{makemigrations,migrate}",
    )

    # ---- makemigrations -------------------------------------------------

    makemig = subparsers.add_parser(
        "makemigrations",
        help="Generate new migration files by diffing model state.",
        description=(
            "Generate migration files based on differences between your current "
            "models and the last recorded migration state. Without flags, the "
            "location and Tortoise config are read from pyproject.toml."
        ),
    )
    makemig.add_argument(
        "--empty",
        action="store_true",
        help="Create an empty (data) migration with a RunPython stub.",
    )
    makemig.add_argument(
        "--name",
        type=str,
        help="Optional name for the migration file (used in the filename slug).",
    )
    makemig.add_argument(
        "--location",
        type=Path,
        help="Override the migrations directory (otherwise read from pyproject).",
    )

    # ---- migrate --------------------------------------------------------

    mig = subparsers.add_parser(
        "migrate",
        help="Apply unapplied migrations in order.",
        description=(
            "Apply migrations found in the migrations directory. Use --sql to "
            "print SQL without executing, or --fake to record as applied without "
            "running the operations."
        ),
    )
    mig.add_argument(
        "target",
        nargs="?",
        help=(
            "Optional migration target (e.g. 0002 or 0002_add_user). "
            "If omitted, runs all pending migrations forward. "
            "If provided, will migrate forward or backward to reach that target."
        ),
    )
    mig.add_argument(
        "--sql",
        action="store_true",
        help="Display the SQL that would run, do not execute.",
    )
    mig.add_argument(
        "--fake",
        action="store_true",
        help="Mark migrations as applied without running them.",
    )
    mig.add_argument(
        "--location",
        type=Path,
        help="Override the migrations directory (otherwise read from pyproject).",
    )
    return parser


def _parse_args(parser: argparse.ArgumentParser) -> argparse.Namespace:
    """Parse CLI args and handle top-level flags that short-circuit (e.g. --version)."""
    args = parser.parse_args()

    if args.version:
        try:
            click.echo(f"tortoisemarch {version('tortoise-march')}")
        except PackageNotFoundError:
            click.echo("tortoisemarch (version unknown)")
        raise SystemExit(0)

    if args.command is None:
        parser.print_help()
        raise SystemExit(2)

    return args


def main() -> None:
    """Console script entry point.

    Parses arguments and dispatches to the appropriate async workflow:
      - makemigrations(...)
      - migrate(...)

    Notes:
        `makemigrations`/`migrate` load Tortoise config and the migrations
        location from `pyproject.toml` if `--location` is not provided.

    """
    parser = _build_parser()
    args = _parse_args(parser)

    try:
        if args.command == "makemigrations":
            asyncio.run(
                makemigrations(
                    location=args.location,
                    empty=args.empty,
                    name=args.name,
                ),
            )
        elif args.command == "migrate":
            asyncio.run(
                migrate(
                    location=args.location,
                    sql=args.sql,
                    fake=args.fake,
                    target=args.target,
                ),
            )
        else:
            msg = f"Unknown command: {args.command!r}"
            raise SystemExit(msg)
    except InvalidMigrationError as exc:
        click.secho(str(exc), fg="red", err=True)
        raise SystemExit(1) from exc
    except KeyboardInterrupt as error:
        # Conventional exit code for SIGINT
        raise SystemExit(130) from error
