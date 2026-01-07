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

Generate migrations:

```bash
poetry run tortoisemarch makemigrations
```

Apply them:

```bash
poetry run tortoisemarch migrate
```

New files will appear under `tortoisemarch/migrations/` with operations like `CreateModel`, `AddField`, `RenameField`, and friends.

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
