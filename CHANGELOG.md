# Changelog

All notable changes to this project will be documented in this file.

## 0.1.0

Initial public release.

### Added

- Django-style migration workflow for Tortoise ORM via `makemigrations` and `migrate`.
- Python-based migration files with explicit operations for schema evolution.
- Deterministic model-state tracking and diffing for migration generation.
- Included migration locations with namespaced recorder entries.
- Migration integrity checks using per-file SHA-256 checksums.
- CLI support for forward/backward targets, SQL preview mode, and `--fake`.
- Development-only `--rewrite-history` flag to reset and rebuild recorded migration history from current files.
- Data migration scaffolding support through empty migration generation.
- Postgres support with unit and integration test coverage.
