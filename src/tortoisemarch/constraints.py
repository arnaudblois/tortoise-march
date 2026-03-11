"""TortoiseMarch-owned constraint helpers.

We keep these helpers separate from Tortoise ORM's own constraint classes so
projects can declare Postgres features that Tortoise does not model yet while
still letting Tortoise March diff and render them cleanly.
"""

from dataclasses import dataclass
from typing import Any

EXCLUSION_EXPRESSION_PARTS = 2


@dataclass(frozen=True)
class ExclusionConstraint:
    """Describe a Postgres exclusion constraint for `Meta.tortoisemarch_constraints`.

    Each expression is a `(field_or_column, operator)` pair. We intentionally
    keep the API field-based because the rest of Tortoise March already knows
    how to validate and normalize logical field names into physical columns.
    """

    expressions: tuple[tuple[str, str], ...]
    name: str | None = None
    index_type: str = "gist"
    condition: str | None = None

    def __post_init__(self) -> None:
        """Normalize inputs so migration state is deterministic."""
        normalized_expressions: list[tuple[str, str]] = []
        for expression in self.expressions:
            if not isinstance(expression, (tuple, list)) or (
                len(expression) != EXCLUSION_EXPRESSION_PARTS
            ):
                msg = (
                    "ExclusionConstraint expressions must be "
                    "(field_or_column, operator) pairs."
                )
                raise ValueError(msg)
            field_name = str(expression[0]).strip()
            operator = str(expression[1]).strip()
            if not field_name or not operator:
                msg = "ExclusionConstraint expressions cannot contain empty values."
                raise ValueError(msg)
            normalized_expressions.append((field_name, operator))

        if not normalized_expressions:
            msg = "ExclusionConstraint requires at least one expression."
            raise ValueError(msg)

        index_type = str(self.index_type).strip().lower()
        if not index_type:
            msg = "ExclusionConstraint requires an index_type."
            raise ValueError(msg)

        condition = self.condition.strip() if isinstance(self.condition, str) else None
        if isinstance(self.condition, str) and not condition:
            condition = None

        object.__setattr__(self, "expressions", tuple(normalized_expressions))
        object.__setattr__(self, "index_type", index_type)
        object.__setattr__(self, "condition", condition)

    def describe(self) -> dict[str, Any]:
        """Return a normalized payload for extractor compatibility."""
        return {
            "type": "ExclusionConstraint",
            "name": self.name,
            "expressions": [list(expression) for expression in self.expressions],
            "index_type": self.index_type,
            "condition": self.condition,
        }

    def deconstruct(self) -> tuple[str, list[Any], dict[str, Any]]:
        """Return a migration-friendly constructor payload."""
        return (
            "tortoisemarch.constraints.ExclusionConstraint",
            [],
            {
                "expressions": [list(expression) for expression in self.expressions],
                "name": self.name,
                "index_type": self.index_type,
                "condition": self.condition,
            },
        )
