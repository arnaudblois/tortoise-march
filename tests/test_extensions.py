"""Tests for PostgreSQL extension declarations."""

import pytest

from tortoisemarch.extensions import PostgresExtension, normalize_postgres_extensions


def test_postgres_extension_normalizes_and_serializes():
    """Extension declarations should be normalized for stable state and codegen."""
    extension = PostgresExtension("  BTREE_GIST  ")

    assert extension.name == "btree_gist"
    assert extension.to_dict() == {
        "type": "postgres_extension",
        "name": "btree_gist",
    }
    assert extension.deconstruct() == (
        "tortoisemarch.extensions.PostgresExtension",
        [],
        {"name": "btree_gist"},
    )
    assert repr(extension) == "PostgresExtension('btree_gist')"


def test_postgres_extension_rejects_empty_names():
    """Empty extension declarations should fail fast with a clear error."""
    with pytest.raises(ValueError, match="non-empty extension name"):
        PostgresExtension("   ")


def test_normalize_postgres_extensions_dedupes_and_sorts():
    """Project-level extension lists should be deterministic and duplicate-free."""
    normalized = normalize_postgres_extensions(
        [
            PostgresExtension("uuid-ossp"),
            PostgresExtension("btree_gist"),
            {"name": "uuid-ossp"},
        ],
        error_context="test extensions",
    )

    assert normalized == [
        PostgresExtension("btree_gist"),
        PostgresExtension("uuid-ossp"),
    ]
