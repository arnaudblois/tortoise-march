"""Diff two ProjectState objects and produce schema operations.

State A (from_state): result of applying existing migrations.
State B (to_state):   current models in code.

This differ compares model/field presence and field attributes
(including primary_key, lengths/precision, FK metadata, etc.) and emits
operations to move from A -> B.

Renames are *not* applied automatically. Call `suggest_renames(...)` to get
candidate (old_name -> new_name) pairs, prompt the user, then pass the user-
confirmed mapping to `diff_states(..., rename_map=...)`.

Rules:
- If a confirmed rename has no attribute changes (only the name differs),
  emit a `RenameField`.
- If it has attribute changes too, emit a single `AlterField(..., new_name=...)`
  so the SchemaEditor can perform rename + alter in one logical step.
"""

from difflib import SequenceMatcher

from tortoisemarch.model_state import ProjectState
from tortoisemarch.operations import (
    AddField,
    AlterField,
    CreateModel,
    Operation,
    RemoveField,
    RemoveModel,
    RenameField,
)

# ----------------------------- helpers ---------------------------------


def _options_with_type(fs) -> dict:
    """Return field options plus its abstract type under the key 'type'."""
    opts = dict(fs.options)
    opts.setdefault("type", fs.field_type)
    return opts


def _same_except_name(old_fs, new_fs) -> bool:
    """Check if two FieldStates are identical aside from the name."""
    return (old_fs.field_type == new_fs.field_type) and (
        old_fs.options == new_fs.options
    )


def _base_name(n: str) -> str:
    """Normalize a column/field name for similarity comparison."""
    n = n.lower()
    n = n.removesuffix("_id")
    return "".join(ch if ch.isalnum() else " " for ch in n).strip()


def _name_similarity(a: str, b: str) -> float:
    """Return a name similarity ratio in [0, 1]."""
    return SequenceMatcher(None, _base_name(a), _base_name(b)).ratio()


def score_candidate(old_name: str, old_fs, new_name: str, new_fs) -> float:
    """Score how likely (old_name -> new_name) is a rename."""
    if new_fs.field_type != old_fs.field_type:
        return -1.0

    old_opts = _options_with_type(old_fs)
    new_opts = _options_with_type(new_fs)

    # Short-circuit: very strong signals
    if old_opts.get("db_column") and old_opts["db_column"] == new_opts.get("db_column"):
        return 100.0

    score = 50.0 * _name_similarity(old_name, new_name)

    fk_types = {"ForeignKeyFieldInstance", "OneToOneFieldInstance"}
    if (
        old_fs.field_type in fk_types
        and new_fs.field_type in fk_types
        and old_opts.get("related_table") == new_opts.get("related_table")
    ):
        score += 20.0

    if old_opts.get("primary_key") == new_opts.get("primary_key"):
        score += 10.0

    score += 5.0 * sum(
        int(old_opts.get(k) == new_opts.get(k)) for k in ("null", "unique", "index")
    )

    if old_fs.field_type == "CharField":
        ol, nl = old_opts.get("max_length"), new_opts.get("max_length")
        if ol is not None and nl is not None and ol == nl:
            score += 10.0
    elif (
        old_fs.field_type == "DecimalField"
        and old_opts.get("max_digits") == new_opts.get("max_digits")
        and old_opts.get(
            "decimal_places",
        )
        == new_opts.get("decimal_places")
    ):
        score += 10.0

    # Small hints: name matches db_column on either side
    if old_opts.get("db_column") == new_name:
        score += 4.0
    if new_opts.get("db_column") == old_name:
        score += 4.0

    return score


def diff_states(
    from_state: ProjectState,
    to_state: ProjectState,
    *,
    rename_map: dict[str, dict[str, str]] | None = None,
) -> list[Operation]:
    """Compute operations to migrate from `from_state` to `to_state`."""
    ops: list[Operation] = []
    rename_map = rename_map or {}

    from_models = from_state.model_states
    to_models = to_state.model_states

    # ---- Models removed
    for model_name in from_models.keys() - to_models.keys():
        old_model = from_models[model_name]
        ops.append(RemoveModel(name=model_name, db_table=old_model.db_table))

    # ---- Models added
    ops.extend(
        CreateModel.from_model_state(to_models[model_name])
        for model_name in to_models.keys() - from_models.keys()
    )

    # ---- Models changed
    for model_name in from_models.keys() & to_models.keys():
        old_model = from_models[model_name]
        new_model = to_models[model_name]

        old_fields = old_model.field_states
        new_fields = new_model.field_states

        removed_names = set(old_fields.keys() - new_fields.keys())
        added_names = set(new_fields.keys() - old_fields.keys())

        # Apply user-confirmed renames first
        confirmed = rename_map.get(model_name, {})
        for old_name, new_name in list(confirmed.items()):
            if old_name in removed_names and new_name in added_names:
                old_fs = old_fields[old_name]
                new_fs = new_fields[new_name]

                if _same_except_name(old_fs, new_fs):
                    # Pure rename
                    ops.append(
                        RenameField(
                            model_name=model_name,
                            db_table=new_model.db_table,
                            old_name=old_name,
                            new_name=new_name,
                        ),
                    )
                else:
                    # Rename + attribute changes
                    ops.append(
                        AlterField(
                            model_name=model_name,
                            db_table=new_model.db_table,
                            field_name=old_name,  # existing column name in DB
                            old_options=_options_with_type(old_fs),
                            new_options=_options_with_type(new_fs),
                            new_name=new_name,  # perform rename + alter
                        ),
                    )
                removed_names.remove(old_name)
                added_names.remove(new_name)
            # else: silently ignore invalid pairs

        # Remaining removed -> RemoveField
        ops.extend(
            RemoveField(
                model_name=model_name,
                db_table=new_model.db_table,
                field_name=fname,
            )
            for fname in sorted(removed_names)
        )

        # Remaining added -> AddField
        for fname in sorted(added_names):
            fs = new_fields[fname]
            ops.append(
                AddField(
                    model_name=model_name,
                    db_table=new_model.db_table,
                    field_name=fs.name,
                    field_type=fs.field_type,
                    options=fs.options,
                ),
            )

        # Unchanged names but modified attributes -> AlterField
        for fname in sorted(old_fields.keys() & new_fields.keys()):
            old_fs = old_fields[fname]
            new_fs = new_fields[fname]

            old_opts = _options_with_type(old_fs)
            new_opts = _options_with_type(new_fs)

            # If either the abstract type or any option differs, we need an AlterField
            if (old_fs.field_type != new_fs.field_type) or (old_opts != new_opts):
                ops.append(
                    AlterField(
                        model_name=model_name,
                        db_table=new_model.db_table,
                        field_name=fname,
                        old_options=old_opts,
                        new_options=new_opts,
                    ),
                )

    return ops
