# API Reference

This page documents the public surface we commit to for `0.1.x`.

## CLI

- `tortoisemarch makemigrations`
- `tortoisemarch migrate`

## Public Python API

- `tortoisemarch.base.BaseMigration`
- `tortoisemarch.ExclusionConstraint`
- `tortoisemarch.operations.*` migration operation classes
- `tortoisemarch.migrate.migrate`
- `tortoisemarch.makemigrations.makemigrations`

## Notes

- Migration files should subclass `BaseMigration` and declare `operations`.
- Operation classes are serialized into migration files, so constructor
  signatures are treated as stable in `0.1.x`.
- Internal modules (`differ`, `extractor`, `schema_editor`, etc.) are
  considered implementation details and may change between minor releases.
