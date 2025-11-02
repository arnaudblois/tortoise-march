# 🐢 Tortoise March

Readable, reliable migrations for Tortoise ORM.

Tortoise March is a Django-style, Pythonic migration system built for clarity and trust.  
It tracks model state over time, generates small, readable Python migration files (not raw SQL), and lets you add data migrations when you need them.

---

## Features

- Diff live models against historical state to generate migrations
- Plain Python migration files you can read, review, and version
- Data migrations alongside schema changes
- Single central migrations folder for simplicity
- Integration tests against PostgreSQL
- PostgreSQL support today; squashing and other backends on the roadmap

---

## Install

```bash
poetry add tortoise-march
```

Developing locally:

```bash
git clone https://github.com/yourname/tortoise-march.git
cd tortoise-march
poetry install
```

---

## Quick start

Make sure your models are registered with Tortoise:

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
