"""Models used in tests."""

from tortoise import fields, models


class Book(models.Model):
    """Test model representing a book."""

    id = fields.IntField(primary_key=True)
    title = fields.CharField(max_length=100)


class Author(models.Model):
    """Test model representing an author of a book."""

    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=100)
    active = fields.BooleanField(default=True)
