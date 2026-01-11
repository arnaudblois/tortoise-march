"""Write migration files with sequential numbering (0001, 0002, ...).

Each filename is "<NNNN>_<slug>.py". We auto-detect the next number by scanning
the migrations directory. Files are Black-formatted if Black is available.
"""

import re
import textwrap
from datetime import UTC, datetime
from pathlib import Path

import black
import click

from tortoisemarch.exceptions import InvalidMigrationError


def _auto_name(operations: list, number: int) -> str:  # noqa: C901, PLR0912
    """Generate a default name based on the operations, like Django does."""
    if number == 1:
        return "0001_initial.py"

    parts: list[str] = []
    for op in operations[:2]:  # only first few, keep short
        cname = op.__class__.__name__
        if cname == "CreateModel":
            parts.append(f"create_{op.name.lower()}")
        elif cname == "RemoveModel":
            parts.append(f"remove_{op.name.lower()}")
        elif cname == "AddField":
            parts.append(f"add_{op.model_name.lower()}_{op.field_name.lower()}")
        elif cname == "RemoveField":
            parts.append(f"remove_{op.model_name.lower()}_{op.field_name.lower()}")
        elif cname == "AlterField":
            parts.append(f"alter_{op.model_name.lower()}_{op.field_name.lower()}")
        elif cname == "RenameField":
            parts.append(f"rename_{op.model_name.lower()}_{op.old_name.lower()}")
        elif cname == "RenameModel":
            parts.append(
                f"rename_{op.old_name.lower()}_to_{op.new_name.lower()}",
            )
        elif cname == "CreateIndex":
            cols = "_".join(op.columns) if getattr(op, "columns", None) else "index"
            parts.append(f"createindex_{op.model_name.lower()}_{cols.lower()}")
        elif cname == "RemoveIndex":
            parts.append(f"removeindex_{op.model_name.lower()}")
        elif cname == "RunPython":
            parts.append("runpython")
    if len(operations) > 2:  # noqa: PLR2004
        parts.append("and_more")
    slug = "_".join(parts) if parts else "auto"
    return f"{number:04d}_{slug}.py"


def _slugify(name: str) -> str:
    """Return a filesystem-safe slug for the migration name."""
    slug = re.sub(r"\s+", "_", name.strip())
    return re.sub(r"[^A-Za-z0-9_]+", "", slug)


def _parse_number(filename: str) -> int:
    """Parse '0001_*.py' -> 1, raise if it doesn't match."""
    m = re.match(r"^(\d{4})_.*\.py$", filename)
    if m:
        return int(m.group(1))
    msg = f"No number found in `{filename}`"
    raise InvalidMigrationError(msg)


def _next_number(migrations_dir: Path) -> int:
    """Compute the next migration number by scanning the directory."""
    numbers: list[int] = []
    for f in migrations_dir.glob("*.py"):
        if f.name == "__init__.py":
            continue
        n = _parse_number(f.name)
        numbers.append(n)
    return (max(numbers) + 1) if numbers else 1


def write_migration(
    operations: list,
    migrations_dir: str | Path,
    name: str | None = None,
    *,
    empty: bool = False,
) -> str:
    """Write a numbered migration file to `migrations_dir`.

    Args:
        operations: list of Operation instances (may be empty if `empty=True`).
        migrations_dir: Directory to write into.
        name: Optional human-readable suffix (slugified into the filename).
        empty: If True and operations is empty, scaffold a RunPython stub.

    Returns:
        The full path to the written migration file (as a string).

    """
    migrations_dir = Path(migrations_dir)
    migrations_dir.mkdir(parents=True, exist_ok=True)
    (migrations_dir / "__init__.py").touch(exist_ok=True)

    number = _next_number(migrations_dir)
    if name:
        filename = f"{number:04d}_{_slugify(name)}.py"
    elif empty and not operations and number > 1:
        filename = f"{number:04d}_data_migration.py"
    else:
        filename = _auto_name(operations=operations, number=number)
    path = migrations_dir / filename

    # Build import list dynamically from operations
    import_classes = sorted({op.__class__.__name__ for op in operations})
    include_runpython_stub = empty and not operations
    if include_runpython_stub:
        import_classes.append("RunPython")

    # Always import ClassVar for Ruff compliance
    import_lines: list[str] = [
        "from typing import ClassVar",
        "from tortoisemarch.base import BaseMigration",
    ]
    if import_classes:
        import_lines.append(
            f"from tortoisemarch.operations import {', '.join(import_classes)}",
        )
    import_block = "\n".join(import_lines)

    # Build operations block
    if include_runpython_stub:
        func_block = (
            "async def forwards(conn, schema_editor):\n"
            '\t"""Write forward data migration using the ORM."""\n'
            "\t...\n\n"
            "async def backwards(conn, schema_editor):\n"
            '\t"""Write reverse data migration if possible. Optional."""\n'
            "\t...\n\n"
        )
        ops_block = (
            "operations: ClassVar[list] = [\n"
            "# Fill in the functions above, then uncomment:\n"
            "# RunPython(forwards, reverse_func=backwards),\n"
            "]\n"
        )

    else:
        func_block = ""
        lines = [f"        {op.to_code()}," for op in operations]
        ops_block = (
            "operations: ClassVar[list] = [\n"
            + ("\n".join(lines) + "\n" if lines else "")
            + "    ]\n"
        )

    header_doc = (
        f'"""Migration {filename}.'
        f'Generated {datetime.now(UTC).isoformat(timespec="seconds")}Z '
        "by TortoiseMarch."
        '"""'
    )

    migration_block = textwrap.dedent(
        "class Migration(BaseMigration):\n"
        '\t"""Auto-generated migration."""\n'
        f"\t{ops_block}",
    )

    code = f"{header_doc}\n{import_block}\n{func_block}\n{migration_block}"
    code = black.format_str(code, mode=black.Mode())
    path.write_text(code, encoding="utf-8")
    click.echo(f"✅ Created migration {filename}")
    return str(path)
