# 🐢 Tortoise March

> Slow and steady wins the schema migration.

**Tortoise March** is a Django-style, Pythonic schema migration system for [Tortoise ORM](https://tortoise.github.io/), designed for clarity, reliability, and composability.

Unlike Aerich, Tortoise March tracks model state explicitly, generates readable Python migrations, and lets you reason about your schema history instead of guessing what happened. It also supports writing data migrations and will support squashing in future versions.

Documentation: https://arnaudblois.github.io/tortoise-march/

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
│   ├── runner.py
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

## Documentation

Documentation will be available soon. You can also generate it locally using Sphinx, autodoc2, and MyST.

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
