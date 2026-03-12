# 🐢 Tortoise March

> Slow and steady wins the schema migration.

**Tortoise March** is a Django-style, Pythonic schema migration system for [Tortoise ORM](https://tortoise.github.io/), designed for clarity, reliability, and composability.

Unlike Aerich, Tortoise March tracks model state explicitly, generates readable Python migrations, and lets you reason about your schema history instead of guessing what happened. It also supports writing data migrations and will support squashing in future versions.

Documentation: https://arnaudblois.github.io/tortoise-march/

## Why Tortoise March?

Tortoise March was developed to address the need of Django-style migrations at the time Tortoise was relying on Aerich for migrations. Since Tortoise ORM `1.1.5`, Tortoise ships its own migration system. You may still prefer Tortoise March if you want:

- a single, central migrations folder instead of more brittle per-app migration package
- small, readable, fully linted Python migration files with predictable generated operations
- first-class handling of model-level indexes and constraints in migration state

Tortoise March is used in real production systems and has been battle-tested there.

---

## Features

- Tracks full model state over time and diffs it to generate migrations
- Generates readable, Python-based migration files (no raw SQL)
- Supports model-level indexes and constraints in migration state
- Supports `UniqueConstraint` and `CheckConstraint` on Postgres
- Treats `unique_together` as a real unique constraint during extraction/diffing
- Handles `AlterField(unique=...)` with named constraint DDL on Postgres
- Supports custom logic through data migrations
- Centralised migration folder for simplicity
- Includes full integration tests with Postgres
- Supports Postgres only for now
- Planned squashing and optional per-app mode

---

## Installation

```bash
poetry add tortoise-march
```

If developing locally:

```bash
git clone https://github.com/arnaudblois/tortoise-march.git
cd tortoise-march
poetry install
```

---

## Usage

Tortoise March relies on Tortoise’s app registry, so your models must be initialised before running commands. First, ensure your models are loaded:

```python
# myapp/__init__.py or wherever appropriate
from tortoise import Tortoise

await Tortoise.init(
    modules={"models": ["myapp.models"]}
)
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
`label:NNNN_name` in the migration recorder and CLI, so you can `--fake` them if
your schema already includes those changes.

### Step 1: Generate migrations

```bash
poetry run tortoisemarch makemigrations
```

This creates a file in `migrations/` with operations like `CreateModel`, `AddField`, and so on.

Tortoise March now also emits model-level schema operations such as
`CreateIndex`, `AddConstraint`, `RemoveConstraint`, and `RenameConstraint`
when your Tortoise models define indexes or constraints.

`makemigrations` options:

- `--empty` creates a data-migration stub with a `RunPython` placeholder.
- `--name` sets a custom filename suffix.
- `--location` overrides the migrations directory.
- `--check-only` errors out if a migration would be written and prints the filename (useful in CI).

### Step 2: Apply migrations

```bash
poetry run tortoisemarch migrate
```

This applies any new migration files to your database.

`migrate` options:

- `tortoisemarch migrate 0002` migrates forward or backward to reach that target (number or unique prefix).
- `--sql` previews the SQL (forward or backward) without executing.
- `--fake` updates the migration recorder without running SQL (useful if you applied changes manually).
- `--rewrite-history` resets recorder history and rebuilds it from current migration files (development-only, requires `--fake`).

Migration safety:

- We store a SHA-256 checksum for each applied migration file.
- We fail fast if an applied migration file is missing or has been modified.
- We treat applied migrations as immutable history. To change behavior, add a new migration.

### RunPython Historical Models

`RunPython` can use historical models built from migration state instead of
importing the live model modules.

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

This matters when the live models no longer match the schema at that migration
point. Tortoise March recreates temporary ORM models from the historical
migration state, so queries align with the schema being migrated.

Current caveats:

While historical models are schema-accurate for querying and saving rows, they are not a perfect reconstruction of every original Python model feature.

For instance, custom methods/managers and non-schema Python behavior are not preserved.

### Constraint support

On Postgres, Tortoise March currently supports:

- model-level `UniqueConstraint`
- model-level `CheckConstraint`
- `Meta.tortoisemarch_constraints` for TortoiseMarch-owned constraints such as `ExclusionConstraint`
- `Meta.unique_together`, normalized as a unique constraint
- single-column `unique=True` changes through `AlterField`

#### Background

Constraints are database rules that protect invariants even when application
code is wrong or bypassed.

- A unique constraint says that a value or combination of values must not appear twice.
- A check constraint says that each row must satisfy a boolean SQL expression.
- An exclusion constraint says that two rows must not conflict under a given operator set.

In practice:

- use a unique constraint for things like `email` uniqueness or `(tenant, slug)` uniqueness
- use a check constraint for things like `age >= 18` or `starts_at < ends_at`
- use an exclusion constraint for Postgres-specific "no overlap" rules such as room bookings or reservation windows

Tortoise March treats these as first-class schema objects. That matters because
they are not just implementation details of a column: they have names, can be
renamed independently, and sometimes need `ADD CONSTRAINT` / `DROP CONSTRAINT`
DDL instead of a simple column alteration.

#### Defining Constraints In Tortoise Models

When Tortoise ORM exposes a constraint object directly, define it on
`Meta.constraints`. Tortoise March will extract it, keep it in migration state,
diff it semantically, and emit model-level constraint operations.

Example:

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

Legacy `unique_together` is also supported:

```python
class Meta:
    unique_together = (("tenant", "slug"),)
```

Tortoise March normalizes that into a real unique constraint internally, so it
diffs and migrates like the explicit form above.

#### ExclusionConstraint Without ORM Support

`ExclusionConstraint` is different: Tortoise ORM does not model it yet, so we
do not pretend it is a native ORM feature. Instead, Tortoise March provides its
own helper and a namespaced Meta attribute, `Meta.tortoisemarch_constraints`.

This is deliberate:

- it keeps the unsupported surface area clearly owned by Tortoise March
- it avoids fragile workarounds around missing ORM features
- it still lets migrations round-trip the schema cleanly

Use TortoiseMarch's helper instead of Tortoise ORM's unsupported API surface:

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

`expressions` is a tuple of `(expression_node, operator)` pairs where the node is
one of:

- `FieldRef("practitioner")` for normal field/column references
- `RawSQL("tstzrange(start_at, end_at, '[)')")` for verbatim SQL expressions
- a plain string like `"room"` for backwards-compatible field refs

Tortoise March validates `FieldRef(...)` names against the extracted model
schema, resolves logical field names to physical database columns when needed,
and renders PostgreSQL `EXCLUDE USING ...` DDL during migration. `RawSQL(...)`
is emitted verbatim and is not introspected further.

#### What Tortoise March Does With Constraints

Once extracted from your model definitions, Tortoise March:

- stores constraints explicitly in migration state instead of collapsing them into generic metadata
- compares them semantically during diffing, so a pure rename becomes `RenameConstraint`
- emits `AddConstraint` and `RemoveConstraint` when the rule itself changes
- preserves custom names when you provide them
- generates deterministic fallback names when you do not
- maps logical field names to actual database column names before generating SQL

For example:

- changing only the name of a unique, check, or exclusion constraint produces a rename
- changing the columns of a unique constraint produces remove + add
- changing the operator set or condition of an exclusion constraint produces remove + add
- toggling `unique=True` on a field uses named constraint DDL rather than pretending it is just an index

This means migration history stays readable and reviewable. You can see whether
you renamed a constraint, changed its semantics, or introduced a brand new rule.

Not supported yet:

- conditional unique constraints

---

## Project Layout

```
src/
├── tortoisemarch/
│   ├── introspect.py
│   ├── loader.py
│   ├── differ.py
│   ├── operations.py
│   ├── writer.py
│   ├── makemigrations.py
│   ├── migrate.py
│   └── migrations/
│       ├── 0001_initial.py
│       └── ...
```

---

## Running Tests

```bash
poetry run pytest
```

Tests include both unit-level operations and real database integration using asyncpg.

---

## Release Process

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

## Documentation

Published docs: https://arnaudblois.github.io/tortoise-march/

Build and serve locally:

```bash
poetry run mkdocs serve
```

---

## Contributing

Feel free to open issues or pull requests. All contributions are welcome.

Current roadmap includes:

- Migration squashing
- Optional per-app migration folders
- SQLite and MySQL support

---

## License

MIT © [Arnaud Blois](https://github.com/arnaudblois)

---

## Philosophy

> Slow and steady wins the schema migration.

Migrations should be clear, predictable, and maintainable.  
Tortoise March focuses on readability and developer trust, so you can evolve your schema without stress.
