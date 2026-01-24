"""Test that the guarding against adding non-nullable fields is working."""

import pytest

from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.makemigrations import (
    _validate_non_nullable_adds_and_warn_alters,
    _validate_safe_alters,
)
from tortoisemarch.operations import AddField, AlterField, CreateModel, RenameModel


def test_add_non_nullable_no_default_on_existing_table_raises():
    """Adding a non-nullable field without a default to an existing table must fail."""
    op = AddField(
        model_name="User",
        db_table="user",
        field_name="age",
        field_type="IntField",
        options={"null": False},  # no default, no db_default
    )
    with pytest.raises(InvalidMigrationError) as error:
        _validate_non_nullable_adds_and_warn_alters([op])

    assert str(error.value) == (
        "Cannot generate migration:\n"
        "You added a non-nullable field without a default "
        "(cannot backfill existing rows).\n"
        "Fix by adding a default or use this safe sequence:\n"
        "  1) Add the field as nullable.\n"
        "  2) Create a data migration to backfill it "
        "(run: tortoisemarch makemigrations --empty).\n"
        "  3) Make the field non-nullable and re-run makemigrations.\n\n"
        "Problems:\n  - User.age"
    )


def test_add_nullable_no_default_is_allowed():
    """Adding a nullable field without a default is always safe."""
    op = AddField(
        model_name="User",
        db_table="user",
        field_name="nickname",
        field_type="CharField",
        options={"null": True},
    )
    _validate_non_nullable_adds_and_warn_alters([op])  # should not raise


def test_add_non_nullable_with_literal_default_is_allowed():
    """A non-nullable field with a literal default can be safely backfilled."""
    op = AddField(
        model_name="User",
        db_table="user",
        field_name="country",
        field_type="CharField",
        options={"null": False, "default": "FR"},
    )
    _validate_non_nullable_adds_and_warn_alters([op])  # should not raise


def test_add_non_nullable_with_db_default_is_allowed():
    """A non-nullable field with a database-level default is allowed."""
    op = AddField(
        model_name="User",
        db_table="user",
        field_name="created_at",
        field_type="DateTimeField",
        options={"null": False, "db_default": "now()"},
    )
    _validate_non_nullable_adds_and_warn_alters([op])  # should not raise


def test_add_non_nullable_without_default_on_new_model_is_allowed():
    """Non-nullable fields without defaults are fine when creating a new table."""
    op = CreateModel(
        name="User",
        db_table="user",
        fields=[
            ("email", "CharField", {"null": False}),
        ],
    )
    _validate_non_nullable_adds_and_warn_alters([op])  # should not raise


def test_alter_nullable_to_non_nullable_without_default_prompts_and_cancels_by_default(
    monkeypatch,
    capsys,
):
    """Test null -> no-default non-nullable prompts and cancels by default."""
    op = AlterField(
        model_name="User",
        db_table="user",
        field_name="phone",
        old_options={"null": True},
        new_options={"null": False},
    )
    monkeypatch.setattr(
        "tortoisemarch.makemigrations._safe_input",
        lambda *_args, **_kwargs: False,
    )
    with pytest.raises(InvalidMigrationError) as e:
        _validate_non_nullable_adds_and_warn_alters([op])

    out = capsys.readouterr().out
    assert "about to" in out.lower()
    assert "User.phone" in out
    assert "cancel" in str(e.value).lower()


def test_alter_nullable_to_non_nullable_without_default_prompts_and_allows_on_yes(
    monkeypatch,
    capsys,
):
    """Test null -> no-default non-null prompts user and proceeds on yes."""
    op = AlterField(
        model_name="User",
        db_table="user",
        field_name="phone",
        old_options={"null": True},
        new_options={"null": False},
    )
    monkeypatch.setattr(
        "tortoisemarch.makemigrations._safe_input",
        lambda *_args, **_kwargs: True,
    )
    _validate_non_nullable_adds_and_warn_alters([op])  # should not raise

    out = capsys.readouterr().out
    assert "about to" in out.lower()
    assert "User.phone" in out


def test_alter_nullable_to_non_nullable_with_default_is_allowed():
    """Altering a field to non-nullable with a default is safe."""
    op = AlterField(
        model_name="User",
        db_table="user",
        field_name="phone",
        old_options={"null": True},
        new_options={"null": False, "default": ""},
    )
    _validate_non_nullable_adds_and_warn_alters([op])  # should not raise


def test_default_callable_is_not_accepted_and_should_raise():
    """Callable defaults cannot be used to backfill existing rows and must fail."""
    op = AddField(
        db_table="user",
        model_name="User",
        field_name="token",
        field_type="CharField",
        options={"null": False, "default": "python_callable"},
    )
    with pytest.raises(InvalidMigrationError):
        _validate_non_nullable_adds_and_warn_alters([op])


def test_alter_field_unsupported_type_change_raises():
    """Unsupported type changes should be rejected during makemigrations."""
    op = AlterField(
        model_name="AuditLog",
        db_table="audit_log",
        field_name="id",
        old_options={"type": "IntField"},
        new_options={"type": "UUIDField", "default": "python_callable"},
    )
    with pytest.raises(InvalidMigrationError, match="Unsupported AlterField changes"):
        _validate_safe_alters([op])


def test_alter_field_fk_reference_change_raises():
    """Changing FK references should be rejected during makemigrations."""
    op = AlterField(
        model_name="Book",
        db_table="book",
        field_name="author",
        old_options={
            "type": "ForeignKeyFieldInstance",
            "related_table": "authors",
            "to_field": "id",
            "referenced_type": "UUIDField",
        },
        new_options={
            "type": "ForeignKeyFieldInstance",
            "related_table": "writers",
            "to_field": "id",
            "referenced_type": "UUIDField",
        },
    )
    with pytest.raises(InvalidMigrationError, match="Unsupported AlterField changes"):
        _validate_safe_alters([op])


def test_alter_field_fk_reference_change_allowed_for_model_rename():
    """FK related_table updates tied to a model/table rename should be allowed."""
    rename = RenameModel(
        old_name="Author",
        new_name="Writer",
        old_db_table="author",
        new_db_table="writer",
    )
    op = AlterField(
        model_name="Book",
        db_table="book",
        field_name="author",
        old_options={
            "type": "ForeignKeyFieldInstance",
            "related_table": "author",
            "to_field": "id",
            "referenced_type": "UUIDField",
        },
        new_options={
            "type": "ForeignKeyFieldInstance",
            "related_table": "writer",
            "to_field": "id",
            "referenced_type": "UUIDField",
        },
    )
    _validate_safe_alters([rename, op])  # should not raise


def test_alter_field_allows_integer_widening():
    """Safe integer widening should be allowed during makemigrations."""
    op = AlterField(
        model_name="AuditLog",
        db_table="audit_log",
        field_name="id",
        old_options={"type": "IntField"},
        new_options={"type": "BigIntField"},
    )
    _validate_safe_alters([op])  # should not raise


def test_alter_field_allows_charfield_length_change():
    """CharField length changes should be allowed during makemigrations."""
    op = AlterField(
        model_name="User",
        db_table="user",
        field_name="email",
        old_options={"type": "CharField", "max_length": 120},
        new_options={"type": "CharField", "max_length": 200},
    )
    _validate_safe_alters([op])  # should not raise


def test_alter_field_allows_db_column_change_with_rename():
    """db_column changes paired with a rename should be allowed."""
    op = AlterField(
        model_name="Book",
        db_table="book",
        field_name="author",
        old_options={"type": "ForeignKeyFieldInstance", "db_column": "author_id"},
        new_options={"type": "ForeignKeyFieldInstance", "db_column": "writer_id"},
        new_name="writer",
    )
    _validate_safe_alters([op])  # should not raise
