# 🐢 Tortoise March

Readable, reliable migrations for Tortoise ORM.

Tortoise March is a Django-style, Pythonic migration system built for clarity and trust.  
It tracks model state over time, generates small, readable Python migration files (not raw SQL), and lets you add data migrations when you need them.

Documentation: https://arnaudblois.github.io/tortoise-march/

## Why Tortoise March?

Tortoise March was originally developed when Tortoise ORM relied on Aerich for
migrations and there was a need for a more Django-style workflow. Since
Tortoise ORM `1.1.5`, Tortoise ships its own migration system. Tortoise March
is still relevant if you want a migration tool that goes beyond the ORM's
built-in schema surface and models PostgreSQL-specific requirements such as
named constraints, expression-based exclusion constraints, and required
extensions like `btree_gist`.

You may still prefer Tortoise March if you want:

- a single, central migrations folder instead of more brittle per-app migration package
- small, readable, fully linted Python migration files with predictable generated operations
- first-class migration-state tracking for model-level indexes, constraints, and PostgreSQL extensions
- explicit dependency ordering for prerequisite schema requirements such as extensions before dependent constraints

Tortoise March is used in real production systems and has been battle-tested there.

---

## Features

- Tracks full model state over time and diffs it to generate migrations
- Generates readable, Python-based migration files (no raw SQL)
- Supports custom logic through data migrations
- Centralised migration folder for simplicity
- Includes full integration tests with Postgres
- Supports Postgres only for now
- Planned squashing and optional per-app mode

---

## Install

```bash
poetry add tortoise-march
```

Developing locally:

```bash
git clone https://github.com/arnaudblois/tortoise-march.git
cd tortoise-march
poetry install
```

---

## Quick start

Tortoise March relies on Tortoise’s app registry, so your models must be initialised before running commands. Make sure your models are registered with Tortoise:

```python
# e.g. myapp/__init__.py
from tortoise import Tortoise

await Tortoise.init(modules={"models": ["myapp.models"]})
```

### Configuration

You can configure TortoiseMarch via `pyproject.toml` (Poetry or any tooling that
supports it) or a `.tortoisemarch.cfg` file.

#### With Poetry (pyproject.toml)

```toml
[tool.tortoisemarch]
tortoise_orm = "myproj.settings:TORTOISE_ORM"
location = "migrations"
include_locations = [
  { label = "myapp", path = "vendor/myapp/migrations" },
]
```

#### With .tortoisemarch.cfg

If you are not using Poetry (or prefer not to use `pyproject.toml`), create a
`.tortoisemarch.cfg` in your project root:

```ini
[tortoisemarch]
tortoise_orm = myproj.settings:TORTOISE_ORM
location = migrations
src_folder = .
include_locations = [{"label": "myapp", "path": "vendor/myapp/migrations"}]
```

Note: `include_locations` must be valid JSON in `.tortoisemarch.cfg`.

Included migrations are planned **before** your project migrations, but already
applied migrations are never re-run. Their names are namespaced as
`label:NNNN_name` in the recorder and CLI, so you can `--fake` them if your
schema already includes those changes.

Generate migrations:

```bash
poetry run tortoisemarch makemigrations
```

Apply them:

```bash
poetry run tortoisemarch migrate
```

New files will appear under `migrations/` with operations like `CreateModel`, `AddField`, `RenameField`, and friends.

`makemigrations` options:

- `--empty` creates a data-migration stub with a `RunPython` placeholder.
- `--name` sets a custom filename suffix.
- `--location` overrides the migrations directory.
- `--check-only` errors out if a migration would be written and prints the filename (useful in CI).

`migrate` options:

- `tortoisemarch migrate 0002` migrates forward or backward to reach that target (number or unique prefix).
- `--sql` previews the SQL (forward or backward) without executing.
- `--fake` updates the migration recorder without running SQL (useful if you applied changes manually).
- `--rewrite-history` resets recorder history and rebuilds it from current migration files (development-only, requires `--fake`).

Inspect the SQL for one migration file without applying it:

```bash
poetry run tortoisemarch show-sql 0003
poetry run tortoisemarch show-sql 0003_add_user_indexes
```

`show-sql` resolves either a unique numeric prefix or the full migration name,
loads exactly that migration file, and renders its forward SQL without
modifying recorder history.

Migration safety:

- We store a SHA-256 checksum for each applied migration file.
- We fail fast if an applied migration file is missing or has been modified.
- We treat applied migrations as immutable history. To change behavior, add a new migration.

## RunPython Historical Models

`RunPython` can use historical models generated from migration state.

Supported callable signatures are:

- `async def forwards(): ...`
- `async def forwards(apps): ...`
- `async def forwards(conn, schema_editor): ...`
- `async def forwards(conn, schema_editor, apps): ...`

Example:

```python
from tortoisemarch.base import BaseMigration
from tortoisemarch.operations import RunPython


async def forwards(apps):
    Book = apps.get_model("Book")
    for book in await Book.all():
        book.title = book.title.upper()
        await book.save(update_fields=["title"])


class Migration(BaseMigration):
    operations = [
        RunPython(forwards),
    ]
```

Why this matters: by the time a data migration runs, your live model code may
already describe a newer schema. Tortoise March therefore builds temporary ORM
models from the migration state at that point, so data migration queries line up
with the schema being migrated.

Current scope:

- historical models are intended for schema-accurate CRUD/query work
- they are not a perfect reconstruction of every original Python model feature
- custom methods, managers, and other non-schema Python behavior are not preserved

## Constraints

Tortoise March treats model-level constraints as first-class schema objects.
That is important because constraints are not just column flags: they have
names, semantics, and dedicated DDL in Postgres.

### Background

Constraints are database-level rules that keep invalid data out even if
application code forgets to validate it.

- A unique constraint prevents duplicate values across one or more columns.
- A check constraint requires each row to satisfy a boolean SQL expression.
- An exclusion constraint prevents two rows from conflicting under a Postgres operator set.

Typical examples:

- unique: `email` must be unique, or `(tenant, slug)` must be unique together
- check: `age >= 18`, `starts_at < ends_at`
- exclusion: two bookings for the same room must not overlap in time

### Defining Constraints In Models

If Tortoise ORM exposes the constraint class directly, define it on
`Meta.constraints`.

```python
from tortoise import fields, models
from tortoise.constraints import CheckConstraint, UniqueConstraint


class Member(models.Model):
    email = fields.CharField(max_length=255)
    age = fields.IntField()
    tenant = fields.CharField(max_length=50)

    class Meta:
        constraints = (
            UniqueConstraint(
                fields=("tenant", "email"),
                name="member_tenant_email_uniq",
            ),
            CheckConstraint(
                check="age >= 18",
                name="member_age_check",
            ),
        )
```

`Meta.unique_together` is also supported. Tortoise March normalizes it into an
explicit unique constraint internally so it behaves like the newer constraint API.

```python
class Meta:
    unique_together = (("tenant", "slug"),)
```

### ExclusionConstraint

Tortoise ORM does not currently expose exclusion constraints as part of its own
model contract. Tortoise March therefore provides a TortoiseMarch-owned helper
instead of hiding that limitation behind a workaround.

Define exclusion constraints on `Meta.tortoisemarch_constraints`:

```python
from tortoise import fields, models
from tortoisemarch.constraints import ExclusionConstraint, FieldRef, RawSQL


class Booking(models.Model):
    practitioner = fields.ForeignKeyField(
        "models.Practitioner",
        related_name="bookings",
    )
    start_at = fields.DatetimeField()
    end_at = fields.DatetimeField()

    class Meta:
        tortoisemarch_constraints = (
            ExclusionConstraint(
                expressions=(
                    (FieldRef("practitioner"), "="),
                    (RawSQL("tstzrange(start_at, end_at, '[)')"), "&&"),
                ),
                name="bookings_no_overlap_per_practitioner",
                index_type="gist",
                condition="status IN ('held', 'confirmed', 'completed', 'no_show')",
            ),
        )
```

The referenced model can be any normal Tortoise model, for example:

```python
class Practitioner(models.Model):
    id = fields.IntField(primary_key=True)
```

`expressions` is a tuple of `(expression_node, operator)` pairs. We support:

- `FieldRef("field_name")` for identifier-style field/column references
- `RawSQL("...")` for verbatim SQL expressions
- plain strings as a backwards-compatible shorthand for `FieldRef(...)`

Tortoise March validates `FieldRef(...)` names against the extracted model
state, resolves logical names to physical database column names, and renders
PostgreSQL `EXCLUDE USING ...` DDL in migrations. `RawSQL(...)` is emitted
verbatim, but PostgreSQL still requires exclusion/index expressions to be
immutable. Buffered `tstzrange(...)` expressions that add or subtract
intervals from `timestamptz` values are rejected by PostgreSQL and therefore
rejected by TortoiseMarch too. If you need a buffered booking window, store
that range in a real column and reference the column with `FieldRef(...)`.

### PostgreSQL Extensions

Some PostgreSQL schema features depend on extensions being installed before the
constraint or index can be created. A common example is `btree_gist`, which is
required for GiST exclusion constraints that compare UUID values with `=`.

Declare those requirements in the same model `Meta` class with
`Meta.tortoisemarch_extensions`:

```python
from tortoise import fields, models
from tortoisemarch.constraints import ExclusionConstraint, FieldRef, RawSQL
from tortoisemarch.extensions import PostgresExtension


class Practitioner(models.Model):
    id = fields.UUIDField(primary_key=True)


class Booking(models.Model):
    id = fields.UUIDField(primary_key=True)
    practitioner = fields.ForeignKeyField(
        "models.Practitioner",
        related_name="bookings",
    )
    start_at = fields.DatetimeField()
    end_at = fields.DatetimeField()

    class Meta:
        tortoisemarch_extensions = (
            PostgresExtension("btree_gist"),
        )
        tortoisemarch_constraints = (
            ExclusionConstraint(
                expressions=(
                    (FieldRef("practitioner"), "="),
                    (RawSQL("tstzrange(start_at, end_at, '[)')"), "&&"),
                ),
                name="bookings_no_overlap_per_practitioner",
                index_type="gist",
            ),
        )
```

Tortoise March deduplicates identical extension declarations across models,
tracks them at project state level, and emits explicit `AddExtension` /
`RemoveExtension` operations in generated migrations.

When a migration adds both an extension and a dependent constraint, Tortoise
March orders the extension first so a fresh PostgreSQL database can replay the
migration without manual edits.

### What Tortoise March Does

After extraction, Tortoise March keeps constraints explicit in migration state.
It does not flatten them into generic metadata.

That lets it:

- detect pure renames and emit `RenameConstraint`
- emit `AddConstraint` and `RemoveConstraint` when semantics change
- preserve custom names when you specify them
- generate deterministic fallback names when you do not
- map field names to real DB column names before SQL generation

Examples:

- rename only the constraint name: Tortoise March emits `RenameConstraint`
- change a unique constraint column set: Tortoise March emits remove + add
- change an exclusion operator or condition: Tortoise March emits remove + add
- toggle `unique=True` on a field: Tortoise March uses named constraint DDL

Supported today on Postgres:

- model-level `UniqueConstraint`
- model-level `CheckConstraint`
- `Meta.tortoisemarch_extensions` with `PostgresExtension`
- `Meta.unique_together`
- `Meta.tortoisemarch_constraints` with `ExclusionConstraint`
- single-column `unique=True` changes through `AlterField`

Not supported yet:

- conditional unique constraints

---

## Project layout

```
src/
└── tortoisemarch/
    ├── introspect.py
    ├── loader.py
    ├── differ.py
    ├── operations.py
    ├── writer.py
    ├── makemigrations.py
    ├── migrate.py
    └── migrations/
        ├── 0001_initial.py
        └── ...
```

---

## Tests

```bash
poetry run pytest
```

Includes unit tests for operations and integration tests against Postgres.

---

## Release process

PyPI publishing is automated via GitHub Actions.

1. Update the package version in `pyproject.toml` (for example `0.1.0`).
2. Merge that version change to `main`.
3. Create and push a tag for the same version:

```bash
git tag v0.1.0
git push origin v0.1.0
```

4. In GitHub, create a Release from that tag and click **Publish release**.

Publishing happens on the `release.published` event (not on tag push alone).
The workflow verifies that the release tag matches the package version in
`pyproject.toml` (both `0.1.0` and `v0.1.0` are accepted).

---

## Contributing

Issues and pull requests are welcome.

Roadmap highlights:

- Squashing old migrations
- Optional per-app migration folders
- SQLite and MySQL support

---

## License

MIT © Arnaud Blois

---

## Philosophy

Migrations should be clear, predictable and easy to review.  
Tortoise March favours small, explicit steps so your schema evolves without surprises.
