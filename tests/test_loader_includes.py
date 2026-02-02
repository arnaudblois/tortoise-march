"""Tests for loading migrations with included directories."""

from pathlib import Path

from tortoisemarch.loader import load_migration_state


def _write_migration(path: Path, model_name: str, table: str) -> None:
    path.write_text(
        "\n".join(
            [
                "from tortoisemarch.base import BaseMigration",
                "from tortoisemarch.operations import CreateModel",
                "",
                "class Migration(BaseMigration):",
                "    operations = [",
                "        CreateModel(",
                f'            name="{model_name}",',
                f'            db_table="{table}",',
                '            fields=[("id", "IntField", {"primary_key": True})],',
                "        ),",
                "    ]",
                "",
            ],
        ),
        encoding="utf-8",
    )


def test_load_migration_state_with_includes(tmp_path: Path) -> None:
    """Load state from included and main migration directories."""
    include_dir = tmp_path / "lib_migrations"
    include_dir.mkdir()
    _write_migration(include_dir / "0001_create_foo.py", "Foo", "foo")

    main_dir = tmp_path / "migrations"
    main_dir.mkdir()
    _write_migration(main_dir / "0001_create_bar.py", "Bar", "bar")

    state = load_migration_state(
        migration_dir=main_dir,
        include_dirs=[("lib", include_dir)],
    )

    assert "Foo" in state.model_states
    assert "Bar" in state.model_states
