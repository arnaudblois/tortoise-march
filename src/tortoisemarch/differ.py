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
from enum import Enum

from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.model_state import FieldState, ModelState, ProjectState
from tortoisemarch.operations import (
    AddField,
    AlterField,
    CreateIndex,
    CreateModel,
    Operation,
    RemoveField,
    RemoveIndex,
    RemoveModel,
    RenameField,
    RenameModel,
    default_index_name,
)
from tortoisemarch.schema_filtering import FK_TYPES, is_schema_field_type

# ----------------------------- helpers ---------------------------------


def _options_with_type(fs) -> dict:
    """Return field options plus its abstract type under the key 'type'."""
    opts = dict(fs.options)
    if isinstance(opts.get("default"), Enum):
        opts["default"] = opts["default"].value
    opts.setdefault("type", fs.field_type)
    return opts


def _options_for_alter(fs) -> dict:
    """Return a complete, stable option set for AlterField diffing and rendering.

    Ensures the abstract field type is always present, and rehydrates
    essential implicit defaults (e.g. CharField max_length) so that
    ALTER statements can be generated deterministically.
    """
    opts = dict(fs.options)
    if isinstance(opts.get("default"), Enum):
        opts["default"] = opts["default"].value
    # always include abstract type for AlterField rendering/diffing
    opts["type"] = fs.field_type

    if opts.get("primary_key"):
        # Strip redundant flags for PKs (unique/index implied, null always False)
        for k in ("unique", "index", "null"):
            opts.pop(k, None)

    # For relational fields, fill in implicit defaults so missing keys
    # don't produce spurious alters when comparing to explicit options.
    if fs.field_type in FK_TYPES:
        # Default DB column name follows Tortoise conventions.
        opts["db_column"] = opts.get("db_column") or f"{fs.name}_id"
        opts["to_field"] = opts.get("to_field") or "id"
        # Tortoise defaults to CASCADE if not provided.
        opts["on_delete"] = opts.get("on_delete") or "CASCADE"
        # referenced_type may be None if unresolved; keep it stable.
        opts.setdefault("referenced_type", None)
        if fs.field_type == "OneToOneFieldInstance":
            # One-to-one implies uniqueness even if not explicitly set.
            opts["unique"] = True

    # if max_length was compacted away, rehydrate the default so comparisons
    # and SQL rendering remain deterministic
    if fs.field_type == "CharField":
        ml = opts.get("max_length")
        if ml is None:
            opts["max_length"] = 255

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


def _table_similarity(a: str, b: str) -> float:
    """Return a similarity score for DB table names in [0, 1]."""
    return _name_similarity(a, b)


def _schema_fields(ms: ModelState) -> dict:
    """Return schema-relevant FieldStates for a model."""
    return {
        k: v for k, v in ms.field_states.items() if is_schema_field_type(v.field_type)
    }


def _pk_field(ms: ModelState):
    """Return the primary key FieldState for a model, if present."""
    for fs in _schema_fields(ms).values():
        if fs.options.get("primary_key"):
            return fs
    return None


def _model_rename_score(old_ms: ModelState, new_ms: ModelState) -> float:
    """Compute a similarity score indicating if two models are likely the same."""
    old_fields = _schema_fields(old_ms)
    new_fields = _schema_fields(new_ms)

    if not old_fields or not new_fields:
        return 0.0

    # Strict matching on (normalized field key + type) for now.
    matches = 0
    for k, old_fs in old_fields.items():
        new_fs = new_fields.get(k)
        if new_fs and new_fs.field_type == old_fs.field_type:
            matches += 1

    overlap = matches / max(len(old_fields), len(new_fields))

    score = overlap

    # Strong signal: same PK type
    old_pk = _pk_field(old_ms)
    new_pk = _pk_field(new_ms)
    if old_pk and new_pk and old_pk.field_type == new_pk.field_type:
        score += 0.2

    # Mild signal: same number of schema fields
    if len(old_fields) == len(new_fields):
        score += 0.1

    # Table-name similarity helps catch renames even if some fields diverged.
    score += 0.2 * _table_similarity(old_ms.db_table, new_ms.db_table)

    # Cap at 1.0
    return min(score, 1.0)


def _implicit_field_indexes(ms: ModelState) -> set[tuple[tuple[str, ...], bool]]:
    """Return indexes implicitly created by field-level index flags."""
    implicit: set[tuple[tuple[str, ...], bool]] = set()
    for fs in ms.field_states.values():
        opts = fs.options
        if opts.get("index") and not opts.get("unique") and not opts.get("primary_key"):
            cols = _physical_index_columns(ms, (fs.name,))
            implicit.add((cols, False))
    return implicit


def _model_indexes(ms: ModelState) -> set[tuple[tuple[str, ...], bool]]:
    """Return a canonical set of (columns, unique) index definitions."""
    meta = ms.meta or {}
    indexes = set()
    for cols, unique in meta.get("indexes", []):
        indexes.add((_physical_index_columns(ms, tuple(cols)), bool(unique)))
    return indexes - _implicit_field_indexes(ms)


def _physical_index_columns(ms: ModelState, cols: tuple[str, ...]) -> tuple[str, ...]:
    """Map logical field names to physical DB column names for indexes."""
    physical: list[str] = []
    for col in cols:
        fs = ms.field_states.get(col.lower())
        if fs:
            if fs.options.get("db_column"):
                physical.append(fs.options["db_column"])
                continue
            if fs.field_type in FK_TYPES or fs.field_type == "OneToOneFieldInstance":
                physical.append(f"{fs.name}_id")
                continue
            physical.append(fs.name)
        else:
            physical.append(col)
    return tuple(physical)


def _detect_model_renames(  # noqa: C901
    removed: dict[str, ModelState],
    added: dict[str, ModelState],
    *,
    threshold: float = 0.90,
) -> list[tuple[str, str]]:
    """Return (old_name, new_name) pairs that look like model renames.

    We compute best match each way, then keep only mutual-best pairs
    above threshold.
    """
    best_for_old: dict[str, tuple[str, float]] = {}
    best_for_new: dict[str, tuple[str, float]] = {}

    for old_name, old_ms in removed.items():
        best = ("", 0.0)
        for new_name, new_ms in added.items():
            s = _model_rename_score(old_ms, new_ms)
            if s > best[1]:
                best = (new_name, s)
        if best[0]:
            best_for_old[old_name] = best

    for new_name, new_ms in added.items():
        best = ("", 0.0)
        for old_name, old_ms in removed.items():
            s = _model_rename_score(old_ms, new_ms)
            if s > best[1]:
                best = (old_name, s)
        if best[0]:
            best_for_new[new_name] = best

    pairs: list[tuple[str, str]] = []
    for old_name, (new_name, s) in best_for_old.items():
        if s < threshold:
            continue
        back = best_for_new.get(new_name)
        if back and back[0] == old_name and back[1] >= threshold:
            pairs.append((old_name, new_name))

    # Stable order for deterministic migrations
    pairs.sort(key=lambda t: (t[0].lower(), t[1].lower()))
    return pairs


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

    if (
        old_fs.field_type in FK_TYPES
        and new_fs.field_type in FK_TYPES
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


def _toposort_models_by_fk(model_states: dict[str, ModelState]) -> list[str]:
    """Return model names ordered by foreign key dependencies.

    Models are sorted so that tables referenced by foreign keys are created
    before the tables that depend on them. Cycles are detected and rejected.
    """
    # Map db_table -> model_name for models in this batch
    table_to_model = {ms.db_table.lower(): name for name, ms in model_states.items()}

    deps: dict[str, set[str]] = {name: set() for name in model_states}
    for name, ms in model_states.items():
        for fs in ms.field_states.values():
            if fs.field_type in FK_TYPES:
                rt = (fs.options.get("related_table") or "").lower()
                if rt in table_to_model:
                    deps[name].add(table_to_model[rt])

    # Kahn's algorithm for topological sorting
    ready = sorted([n for n, ds in deps.items() if not ds])
    out: list[str] = []
    while ready:
        n = ready.pop(0)
        out.append(n)
        for m in list(deps.keys()):
            if n in deps[m]:
                deps[m].remove(n)
                if not deps[m] and m not in out and m not in ready:
                    ready.append(m)
                    ready.sort()

    if len(out) != len(model_states):
        cycle = [n for n, ds in deps.items() if ds]
        msg = (
            f"Cycle detected in CreateModel dependencies: {cycle}. "
            "Either break the cycle (store FK on one side only) or "
            "implement a 2-phase FK add."
        )
        raise InvalidMigrationError(msg)

    return out


# ----------------------------- Diff States ---------------------------------
def _resolved_db_column(fs: FieldState) -> str | None:
    """Return the physical DB column name for a field, if derivable."""
    col = fs.options.get("db_column")
    if col:
        return col
    if fs.field_type in FK_TYPES:
        return f"{fs.name}_id"
    return None


def _rename_or_alter_field_op(  # noqa: PLR0913
    *,
    model_name: str,
    db_table: str,
    old_name: str,
    new_name: str,
    old_fs: FieldState,
    new_fs: FieldState,
) -> Operation:
    """Return a RenameField or AlterField op depending on attribute changes."""
    if _same_except_name(old_fs, new_fs):
        old_col = _resolved_db_column(old_fs)
        new_col = _resolved_db_column(new_fs)
        old_db_column = old_col if old_col and old_col != old_name else None
        new_db_column = new_col if new_col and new_col != new_name else None
        return RenameField(
            model_name=model_name,
            db_table=db_table,
            old_name=old_name,
            new_name=new_name,
            old_db_column=old_db_column,
            new_db_column=new_db_column,
        )
    return AlterField(
        model_name=model_name,
        db_table=db_table,
        field_name=old_name,
        old_options=_options_for_alter(old_fs),
        new_options=_options_for_alter(new_fs),
        new_name=new_name,
    )


def _diff_model_fields(
    ops: list[Operation],
    *,
    old_model: ModelState,
    new_model: ModelState,
    model_name: str,
    rename_map: dict[str, str] | None,
) -> None:
    """Append field operations to transform old_model -> new_model."""
    rename_map = rename_map or {}

    old_fields = _schema_fields(old_model)
    new_fields = _schema_fields(new_model)

    removed_names = set(old_fields.keys() - new_fields.keys())
    added_names = set(new_fields.keys() - old_fields.keys())

    # Apply user-confirmed renames first
    for old_name, new_name in list(rename_map.items()):
        if old_name in removed_names and new_name in added_names:
            old_fs = old_fields[old_name]
            new_fs = new_fields[new_name]
            ops.append(
                _rename_or_alter_field_op(
                    model_name=model_name,
                    db_table=new_model.db_table,
                    old_name=old_name,
                    new_name=new_name,
                    old_fs=old_fs,
                    new_fs=new_fs,
                ),
            )

            removed_names.remove(old_name)
            added_names.remove(new_name)
        # else: ignore invalid pairs silently

    # Remaining removed -> RemoveField
    for fname in sorted(removed_names):
        fs = old_fields[fname]
        col = _resolved_db_column(fs)
        db_column = col if col and col != fs.name else None
        ops.append(
            RemoveField(
                model_name=model_name,
                db_table=new_model.db_table,
                field_name=fname,
                db_column=db_column,
            ),
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

        old_opts = _options_for_alter(old_fs)
        new_opts = _options_for_alter(new_fs)

        if old_opts != new_opts:
            ops.append(
                AlterField(
                    model_name=model_name,
                    db_table=new_model.db_table,
                    field_name=fname,
                    old_options=old_opts,
                    new_options=new_opts,
                ),
            )


def _diff_model_indexes(
    ops: list[Operation],
    *,
    old_model: ModelState,
    new_model: ModelState,
) -> None:
    """Append index operations to transform old_model indexes -> new_model indexes."""
    old_indexes = _model_indexes(old_model)
    new_indexes = _model_indexes(new_model)

    removed = old_indexes - new_indexes
    added = new_indexes - old_indexes

    for cols, unique in sorted(added):
        ops.append(
            CreateIndex(
                model_name=new_model.name,
                db_table=new_model.db_table,
                columns=cols,
                unique=unique,
            ),
        )

    for cols, unique in sorted(removed):
        name = default_index_name(old_model.db_table, cols, unique=unique)
        ops.append(
            RemoveIndex(
                model_name=old_model.name,
                db_table=old_model.db_table,
                name=name,
                columns=cols,
                unique=unique,
            ),
        )


# ----------------------------- Diff States ---------------------------------


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

    # ---- Identify removed/added models first
    removed: dict[str, ModelState] = {
        name: from_models[name] for name in (from_models.keys() - to_models.keys())
    }
    added: dict[str, ModelState] = {
        name: to_models[name] for name in (to_models.keys() - from_models.keys())
    }

    # ---- Detect model renames and emit RenameModel first
    model_renames = _detect_model_renames(removed, added)

    for old_name, new_name in model_renames:
        old_ms = removed.pop(old_name)
        new_ms = added.pop(new_name)
        ops.append(
            RenameModel(
                old_name=old_name,
                new_name=new_name,
                old_db_table=old_ms.db_table,
                new_db_table=new_ms.db_table,
            ),
        )

    # ---- Models removed (after removing rename-pairs)
    ordered_removed_names = _toposort_models_by_fk(removed)
    for model_name in reversed(ordered_removed_names):
        old_model = removed[model_name]
        ops.append(RemoveModel(name=model_name, db_table=old_model.db_table))

    # ---- Models added (after removing rename-pairs)
    ordered_added_names = _toposort_models_by_fk(added)
    for name in ordered_added_names:
        new_model = added[name]
        ops.append(CreateModel.from_model_state(new_model))
        for cols, unique in sorted(_model_indexes(new_model)):
            ops.append(
                CreateIndex(
                    model_name=new_model.name,
                    db_table=new_model.db_table,
                    columns=cols,
                    unique=unique,
                ),
            )

    # ---- Models changed (same name in both)
    for model_name in sorted(from_models.keys() & to_models.keys()):
        _diff_model_fields(
            ops,
            old_model=from_models[model_name],
            new_model=to_models[model_name],
            model_name=model_name,
            rename_map=rename_map.get(model_name),
        )
        _diff_model_indexes(
            ops,
            old_model=from_models[model_name],
            new_model=to_models[model_name],
        )

    # ---- Renamed models may also have field changes
    for old_name, new_name in model_renames:
        old_model = from_models[old_name]
        new_model = to_models[new_name]

        # Allow user to key rename_map by either old or new model name.
        confirmed = rename_map.get(new_name) or rename_map.get(old_name) or {}
        _diff_model_fields(
            ops,
            old_model=old_model,
            new_model=new_model,
            model_name=new_name,
            rename_map=confirmed,
        )
        _diff_model_indexes(
            ops,
            old_model=old_model,
            new_model=new_model,
        )

    return ops
