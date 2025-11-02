# 🐢 Tortoise March

> Slow and steady wins the schema migration.

**Tortoise March** is a Django-style, Pythonic schema migration system for [Tortoise ORM](https://tortoise.github.io/), designed for clarity, reliability, and composability.

Unlike Aerich, Tortoise March tracks model state over time, generates readable Python migration files, and makes it easy to understand and manage your schema history. It also supports writing data migrations and will support squashing in future versions.

---

## Features

- Compares current models with historical state to generate migrations
- Generates Python-based migration files without raw SQL
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
git clone https://github.com/yourname/tortoise-march.git
cd tortoise-march
poetry install
```

---

## Usage

First, ensure your models are loaded:

```python
# myapp/__init__.py or wherever appropriate
from tortoise import Tortoise

await Tortoise.init(
    modules={"models": ["myapp.models"]}
)
```

### Step 1: Generate migrations

```bash
poetry run tortoisemarch makemigrations
```

This creates a file in `tortoisemarch/migrations/` with operations like `CreateModel`, `AddField`, and so on.

### Step 2: Apply migrations

```bash
poetry run tortoisemarch migrate
```

This applies any new migration files to your database.

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
