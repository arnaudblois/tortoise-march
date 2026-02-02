# 🐢 Tortoise March

Readable, reliable migrations for Tortoise ORM.

Tortoise March is a Django-style, Pythonic migration system built for clarity and trust.  
It tracks model state over time, generates small, readable Python migration files (not raw SQL), and lets you add data migrations when you need them.

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
location = "tortoisemarch/migrations"
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
location = tortoisemarch/migrations
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

New files will appear under `tortoisemarch/migrations/` with operations like `CreateModel`, `AddField`, `RenameField`, and friends.

`makemigrations` options:

- `--empty` creates a data-migration stub with a `RunPython` placeholder.
- `--name` sets a custom filename suffix.
- `--location` overrides the migrations directory.
- `--check-only` errors out if a migration would be written and prints the filename (useful in CI).

`migrate` options:

- `tortoisemarch migrate 0002` migrates forward or backward to reach that target (number or unique prefix).
- `--sql` previews the SQL (forward or backward) without executing.
- `--fake` updates the migration recorder without running SQL (useful if you applied changes manually).

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
    ├── runner.py
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
