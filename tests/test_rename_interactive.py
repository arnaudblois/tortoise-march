"""Test the mechanism for field renaming in makemigrations."""

import types

from tortoisemarch import makemigrations as mm


class DummyFieldState:
    """Dummy FieldState."""

    def __init__(self, field_type: str, **options):
        """Set field_type and options."""
        self.field_type = field_type
        self.options = options


class DummyModelState:
    """Dummy ModelState."""

    def __init__(self, name: str, field_states: dict[str, DummyFieldState]):
        """Set name and field states."""
        self.name = name
        self.field_states = field_states


class DummyProjectState:
    """Dummy ProjectState."""

    def __init__(self, model_states: dict[str, DummyModelState]):
        """Attach ModelStates."""
        self.model_states = model_states


def _state_for_single_model(old_fields: dict, new_fields: dict):
    """Create from_state / to_state with a single model.

    We take model 'models.Book', where old_fields / new_fields map
    field_name -> DummyFieldState.
    """
    model_key = "models.Book"
    from_state = DummyProjectState(
        {model_key: DummyModelState("Book", old_fields)},
    )
    to_state = DummyProjectState(
        {model_key: DummyModelState("Book", new_fields)},
    )
    return model_key, from_state, to_state


def test_choose_renames_accepts_best_candidate(monkeypatch):
    """Test the sequence when the user accepts the best rename suggestion.

    We should map old->new and not reuse the target for any other fields.
    """
    model_key, from_state, to_state = _state_for_single_model(
        old_fields={
            "title": DummyFieldState("CharField", max_length=200),
        },
        new_fields={
            "name": DummyFieldState("CharField", max_length=200),
        },
    )

    # Control scoring: any pair gets score 80
    def fake_score_candidate(*_, **__):
        return 80.0

    monkeypatch.setattr(mm, "score_candidate", fake_score_candidate)

    # Always accept the first suggestion
    monkeypatch.setattr(mm, "_safe_input", lambda *_, **__: True)

    # Silence echo (optional)
    monkeypatch.setattr(mm, "click", types.SimpleNamespace(echo=lambda *_, **__: None))

    rename_map = mm._choose_renames_interactive(  # noqa: SLF001
        from_state,
        to_state,
        min_score=0.0,
    )

    assert rename_map == {model_key: {"title": "name"}}


def test_choose_renames_decline_best_then_pick_from_menu(monkeypatch):
    """Test the case where the users declines the best guess.

    They should then be able to pick another candidate from the
    numbered menu.
    """
    model_key, from_state, to_state = _state_for_single_model(
        old_fields={
            "title": DummyFieldState("CharField", max_length=200),
        },
        new_fields={
            "name": DummyFieldState("CharField", max_length=200),
            "alt_title": DummyFieldState("CharField", max_length=200),
        },
    )

    # Make 'name' the best candidate and 'alt_title' second best.
    def fake_score_candidate(old_name, old_fs, new_name, new_fs):
        if new_name == "name":
            return 90.0
        if new_name == "alt_title":
            return 80.0
        return 0.0

    monkeypatch.setattr(mm, "score_candidate", fake_score_candidate)

    # First, user is asked "Accept this rename? [Y/n]" for best candidate.
    # They say "no", so we go into the numbered menu.
    monkeypatch.setattr(mm, "_safe_input", lambda *_, **__: False)

    # Then we show a menu with:
    #   1) name
    #   2) alt_title
    # We simulate user choosing option 2.
    def fake_input_int(*_, max_value: int | None = None, **__):
        # sanity check
        assert max_value == 2  # noqa: PLR2004
        return 2  # pick 'alt_title'

    monkeypatch.setattr(mm, "_input_int", fake_input_int)

    # Silence echo (optional)
    monkeypatch.setattr(mm, "click", types.SimpleNamespace(echo=lambda *_, **__: None))

    rename_map = mm._choose_renames_interactive(  # noqa: SLF001
        from_state,
        to_state,
        min_score=0.0,
    )

    assert rename_map == {model_key: {"title": "alt_title"}}


def test_choose_renames_respects_min_score_threshold(monkeypatch):
    """Test what is below min_score is ignored."""
    _, from_state, to_state = _state_for_single_model(
        old_fields={
            "title": DummyFieldState("CharField", max_length=200),
        },
        new_fields={
            "name": DummyFieldState("CharField", max_length=200),
        },
    )

    # Always return a low score
    def fake_score_candidate(*_):
        return 10.0

    monkeypatch.setattr(mm, "score_candidate", fake_score_candidate)

    # Should not even ask for input if there are no valid candidates
    called = {"safe": False}

    def fake_safe_input(_, *, default=False):
        called["safe"] = True
        return default

    monkeypatch.setattr(mm, "_safe_input", fake_safe_input)
    monkeypatch.setattr(mm, "click", types.SimpleNamespace(echo=lambda *_, **__: None))

    rename_map = mm._choose_renames_interactive(  # noqa: SLF001
        from_state,
        to_state,
        min_score=40.0,
    )

    assert not rename_map
    assert called["safe"] is False  # no prompt when there are no candidates
