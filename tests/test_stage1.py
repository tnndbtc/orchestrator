"""Tests for stage1_generate_script — §42 DraftStory → validation → StoryPrompt → Script flow.

Covers:
  - _generate_draft_story: output schema, required fields, deterministic seed, genre routing
  - _validate_draft: skip path (world-engine absent), pass path (returncode=0),
    failure path (CanonViolationError raised with details from stderr and report file),
    --canon flag presence/absence, unexpected OSError swallowed
  - _compile_story_prompt: schema correctness, fields derived from draft
  - run(): end-to-end stub fallback, StoryPrompt written after validation,
    CanonViolationError propagated, StoryPrompt NOT written on violation
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from orchestrator.registry import ArtifactRegistry
from orchestrator.stages import stage1_generate_script
from orchestrator.stages.stage1_generate_script import (
    CanonViolationError,
    _compile_story_prompt,
    _generate_draft_story,
    _validate_draft,
)


# ---------------------------------------------------------------------------
# Helpers / shared constants
# ---------------------------------------------------------------------------

_PC = {
    "id": "proj-1",
    "title": "Echoes of Tomorrow",
    "genre": "drama",
    "visual_style": "cinematic",
    "target_duration": 60,
}
_RUN_ID = "run-abc12345"

# Non-None sentinel — "world-engine is installed"
_WE_BIN = MagicMock()


def _make_we_result(returncode: int, stderr: str = "") -> MagicMock:
    m = MagicMock()
    m.returncode = returncode
    m.stderr = stderr
    return m


# ---------------------------------------------------------------------------
# _generate_draft_story
# ---------------------------------------------------------------------------

class TestGenerateDraftStory:
    """Unit tests for the DraftStory auto-generation helper."""

    def test_schema_id(self):
        draft = _generate_draft_story(_PC, _RUN_ID)
        assert draft["schema_id"] == "DraftStory"

    def test_schema_version(self):
        draft = _generate_draft_story(_PC, _RUN_ID)
        assert draft["schema_version"] == "1.0.0"

    def test_required_fields_present(self):
        draft = _generate_draft_story(_PC, _RUN_ID)
        for field in (
            "draft_id", "project_id", "title", "genre", "visual_style",
            "generation_seed", "primary_location", "characters",
            "max_scenes", "scenes",
        ):
            assert field in draft, f"Missing required field: {field!r}"

    def test_project_id_matches_config(self):
        draft = _generate_draft_story(_PC, _RUN_ID)
        assert draft["project_id"] == "proj-1"

    def test_title_matches_config(self):
        draft = _generate_draft_story(_PC, _RUN_ID)
        assert draft["title"] == "Echoes of Tomorrow"

    def test_genre_matches_config(self):
        draft = _generate_draft_story(_PC, _RUN_ID)
        assert draft["genre"] == "drama"

    def test_draft_id_encodes_project_and_run_prefix(self):
        draft = _generate_draft_story(_PC, _RUN_ID)
        assert draft["draft_id"].startswith("draft-proj-1-")
        # run_id[:8] is "run-abc1"
        assert "run-abc1" in draft["draft_id"]

    def test_max_scenes_derived_from_target_duration(self):
        pc = dict(_PC, target_duration=120)
        draft = _generate_draft_story(pc, _RUN_ID)
        assert draft["max_scenes"] == 4  # max(2, 120 // 30)

    def test_min_max_scenes_is_two(self):
        pc = dict(_PC, target_duration=10)
        draft = _generate_draft_story(pc, _RUN_ID)
        assert draft["max_scenes"] == 2  # max(2, 10 // 30) = max(2, 0) = 2

    def test_scenes_count_matches_max_scenes(self):
        draft = _generate_draft_story(_PC, _RUN_ID)
        assert len(draft["scenes"]) == draft["max_scenes"]

    def test_generation_seed_is_integer(self):
        draft = _generate_draft_story(_PC, _RUN_ID)
        assert isinstance(draft["generation_seed"], int)

    def test_generation_seed_is_deterministic(self):
        """Same inputs → same seed every time."""
        s1 = _generate_draft_story(_PC, _RUN_ID)["generation_seed"]
        s2 = _generate_draft_story(_PC, _RUN_ID)["generation_seed"]
        assert s1 == s2

    def test_different_run_ids_yield_different_seeds(self):
        """Seeds must depend on run_id so repeated runs are distinguishable."""
        s1 = _generate_draft_story(_PC, "run-aaa00000")["generation_seed"]
        s2 = _generate_draft_story(_PC, "run-bbb00000")["generation_seed"]
        assert s1 != s2

    def test_at_least_two_characters(self):
        """StoryPrompt schema requires minItems: 2 characters; draft must match."""
        draft = _generate_draft_story(_PC, _RUN_ID)
        assert len(draft["characters"]) >= 2

    def test_characters_have_id_and_role(self):
        draft = _generate_draft_story(_PC, _RUN_ID)
        for char in draft["characters"]:
            assert "id" in char, "Character missing 'id'"
            assert "role" in char, "Character missing 'role'"

    def test_drama_primary_location(self):
        draft = _generate_draft_story(_PC, _RUN_ID)
        assert draft["primary_location"] == "City apartment"

    def test_scifi_primary_location(self):
        pc = dict(_PC, genre="sci-fi")
        draft = _generate_draft_story(pc, _RUN_ID)
        assert draft["primary_location"] == "Space station"

    def test_fantasy_primary_location(self):
        pc = dict(_PC, genre="fantasy")
        draft = _generate_draft_story(pc, _RUN_ID)
        assert draft["primary_location"] == "Enchanted forest"

    def test_unknown_genre_uses_fallback_location(self):
        pc = dict(_PC, genre="experimental-art-house")
        draft = _generate_draft_story(pc, _RUN_ID)
        assert "Unspecified" in draft["primary_location"]


# ---------------------------------------------------------------------------
# _validate_draft
# ---------------------------------------------------------------------------

class TestValidateDraft:
    """Unit tests for the world-engine validation gate (§42 step 2)."""

    def test_skips_entirely_when_world_engine_absent(self, tmp_path):
        """When find_agent_bin returns None, call_agent must never be called."""
        draft_path = tmp_path / "DraftStory.json"
        draft_path.write_text("{}", encoding="utf-8")

        with patch(
            "orchestrator.stages.stage1_generate_script.find_agent_bin",
            return_value=None,
        ):
            with patch(
                "orchestrator.stages.stage1_generate_script.call_agent"
            ) as mock_call:
                _validate_draft(draft_path, tmp_path)  # must not raise

        mock_call.assert_not_called()

    def test_passes_silently_on_returncode_zero(self, tmp_path):
        """returncode=0 → _validate_draft returns without raising."""
        draft_path = tmp_path / "DraftStory.json"
        draft_path.write_text("{}", encoding="utf-8")
        ok = _make_we_result(0)

        with patch("orchestrator.stages.stage1_generate_script.find_agent_bin", return_value=_WE_BIN):
            with patch("orchestrator.stages.stage1_generate_script.call_agent", return_value=ok):
                _validate_draft(draft_path, tmp_path)  # must not raise

    def test_raises_canon_violation_error_on_nonzero(self, tmp_path):
        """Non-zero exit code → CanonViolationError is raised."""
        draft_path = tmp_path / "DraftStory.json"
        draft_path.write_text("{}", encoding="utf-8")
        bad = _make_we_result(1)

        with patch("orchestrator.stages.stage1_generate_script.find_agent_bin", return_value=_WE_BIN):
            with patch("orchestrator.stages.stage1_generate_script.call_agent", return_value=bad):
                with pytest.raises(CanonViolationError):
                    _validate_draft(draft_path, tmp_path)

    def test_error_message_includes_returncode(self, tmp_path):
        """Error message must name the exit code."""
        draft_path = tmp_path / "DraftStory.json"
        draft_path.write_text("{}", encoding="utf-8")
        bad = _make_we_result(2)

        with patch("orchestrator.stages.stage1_generate_script.find_agent_bin", return_value=_WE_BIN):
            with patch("orchestrator.stages.stage1_generate_script.call_agent", return_value=bad):
                with pytest.raises(CanonViolationError, match="2"):
                    _validate_draft(draft_path, tmp_path)

    def test_error_message_includes_stderr(self, tmp_path):
        """stderr text must appear in the CanonViolationError message."""
        draft_path = tmp_path / "DraftStory.json"
        draft_path.write_text("{}", encoding="utf-8")
        bad = _make_we_result(1, "world-engine: unknown character introduced")

        with patch("orchestrator.stages.stage1_generate_script.find_agent_bin", return_value=_WE_BIN):
            with patch("orchestrator.stages.stage1_generate_script.call_agent", return_value=bad):
                with pytest.raises(CanonViolationError, match="unknown character"):
                    _validate_draft(draft_path, tmp_path)

    def test_error_message_includes_violation_report_details(self, tmp_path):
        """Violation messages read from CanonViolationReport.json are surfaced."""
        draft_path = tmp_path / "DraftStory.json"
        draft_path.write_text("{}", encoding="utf-8")
        bad = _make_we_result(1)

        # Simulate world-engine writing its violation report
        violation_path = tmp_path / "CanonViolationReport.json"
        violation_path.write_text(
            json.dumps({"violations": [{"message": "Character Eve is not in canon"}]}),
            encoding="utf-8",
        )

        with patch("orchestrator.stages.stage1_generate_script.find_agent_bin", return_value=_WE_BIN):
            with patch("orchestrator.stages.stage1_generate_script.call_agent", return_value=bad):
                with pytest.raises(CanonViolationError, match="Character Eve"):
                    _validate_draft(draft_path, tmp_path)

    def test_passes_draft_path_to_call_agent(self, tmp_path):
        """call_agent args must include 'validate-story-draft' and '--draft'."""
        draft_path = tmp_path / "DraftStory.json"
        draft_path.write_text("{}", encoding="utf-8")
        ok = _make_we_result(0)
        captured: dict = {}

        def _side(name, args, **kwargs):
            captured["name"] = name
            captured["args"] = list(args)
            return ok

        with patch("orchestrator.stages.stage1_generate_script.find_agent_bin", return_value=_WE_BIN):
            with patch("orchestrator.stages.stage1_generate_script.call_agent", side_effect=_side):
                _validate_draft(draft_path, tmp_path)

        assert captured["name"] == "world-engine"
        assert "validate-story-draft" in captured["args"]
        assert "--draft" in captured["args"]
        draft_idx = captured["args"].index("--draft") + 1
        assert captured["args"][draft_idx] == str(draft_path)

    def test_passes_canon_flag_when_snapshot_present(self, tmp_path):
        """When CanonSnapshot.json exists alongside run_dir, --canon must be included."""
        draft_path = tmp_path / "DraftStory.json"
        draft_path.write_text("{}", encoding="utf-8")
        canon_path = tmp_path / "CanonSnapshot.json"
        canon_path.write_text("{}", encoding="utf-8")
        ok = _make_we_result(0)
        captured: dict = {}

        def _side(name, args, **kwargs):
            captured["args"] = list(args)
            return ok

        with patch("orchestrator.stages.stage1_generate_script.find_agent_bin", return_value=_WE_BIN):
            with patch("orchestrator.stages.stage1_generate_script.call_agent", side_effect=_side):
                _validate_draft(draft_path, tmp_path)

        assert "--canon" in captured["args"]
        canon_idx = captured["args"].index("--canon") + 1
        assert captured["args"][canon_idx] == str(canon_path)

    def test_no_canon_flag_when_snapshot_absent(self, tmp_path):
        """Without CanonSnapshot.json, --canon must NOT appear in call_agent args."""
        draft_path = tmp_path / "DraftStory.json"
        draft_path.write_text("{}", encoding="utf-8")
        ok = _make_we_result(0)
        captured: dict = {}

        def _side(name, args, **kwargs):
            captured["args"] = list(args)
            return ok

        with patch("orchestrator.stages.stage1_generate_script.find_agent_bin", return_value=_WE_BIN):
            with patch("orchestrator.stages.stage1_generate_script.call_agent", side_effect=_side):
                _validate_draft(draft_path, tmp_path)

        assert "--canon" not in captured["args"]

    def test_unexpected_exception_is_swallowed(self, tmp_path):
        """If call_agent raises unexpectedly (e.g. OSError), _validate_draft must not re-raise."""
        draft_path = tmp_path / "DraftStory.json"
        draft_path.write_text("{}", encoding="utf-8")

        with patch("orchestrator.stages.stage1_generate_script.find_agent_bin", return_value=_WE_BIN):
            with patch(
                "orchestrator.stages.stage1_generate_script.call_agent",
                side_effect=OSError("world-engine crashed unexpectedly"),
            ):
                _validate_draft(draft_path, tmp_path)  # must not raise


# ---------------------------------------------------------------------------
# _compile_story_prompt
# ---------------------------------------------------------------------------

class TestCompileStoryPrompt:
    """Unit tests for StoryPrompt compilation from a validated DraftStory."""

    def _draft(self, **overrides) -> dict:
        base = _generate_draft_story(_PC, _RUN_ID)
        base.update(overrides)
        return base

    def test_schema_id(self):
        sp = _compile_story_prompt(self._draft())
        assert sp["schema_id"] == "StoryPrompt"

    def test_schema_version(self):
        sp = _compile_story_prompt(self._draft())
        assert sp["schema_version"] == "1.0.0"

    def test_required_fields_present(self):
        sp = _compile_story_prompt(self._draft())
        for field in (
            "schema_id", "schema_version", "prompt_id", "episode_goal",
            "generation_seed", "series", "setting", "characters", "constraints",
        ):
            assert field in sp, f"Missing required field: {field!r}"

    def test_episode_goal_includes_genre_and_title(self):
        draft = self._draft()
        sp = _compile_story_prompt(draft)
        assert draft["genre"] in sp["episode_goal"]
        assert draft["title"] in sp["episode_goal"]

    def test_generation_seed_copied_from_draft(self):
        draft = self._draft()
        sp = _compile_story_prompt(draft)
        assert sp["generation_seed"] == draft["generation_seed"]

    def test_series_genre(self):
        draft = self._draft()
        sp = _compile_story_prompt(draft)
        assert sp["series"]["genre"] == draft["genre"]

    def test_series_title(self):
        draft = self._draft()
        sp = _compile_story_prompt(draft)
        assert sp["series"]["title"] == draft["title"]

    def test_series_tone_from_visual_style(self):
        draft = self._draft()
        sp = _compile_story_prompt(draft)
        assert sp["series"]["tone"] == draft["visual_style"]

    def test_primary_location_in_setting(self):
        draft = self._draft()
        sp = _compile_story_prompt(draft)
        assert sp["setting"]["primary_location"] == draft["primary_location"]

    def test_characters_copied_verbatim(self):
        draft = self._draft()
        sp = _compile_story_prompt(draft)
        assert sp["characters"] == draft["characters"]

    def test_max_scenes_in_constraints(self):
        draft = self._draft()
        sp = _compile_story_prompt(draft)
        assert sp["constraints"]["max_scenes"] == draft["max_scenes"]


# ---------------------------------------------------------------------------
# run() — integration
# ---------------------------------------------------------------------------

class TestRun:
    """Integration tests for stage1_generate_script.run()."""

    def _run_stub(self, tmp_path: Path) -> tuple[dict, ArtifactRegistry]:
        """Run stage1 with both external binaries absent; return (script, registry).

        world-engine is absent (find_agent_bin → None) so validation is skipped.
        writing-agent call_agent returns non-zero so the stub Script is produced.
        """
        registry = ArtifactRegistry(tmp_path)
        with patch(
            "orchestrator.stages.stage1_generate_script.find_agent_bin",
            return_value=None,
        ):
            with patch(
                "orchestrator.stages.stage1_generate_script.call_agent",
                return_value=_make_we_result(1),
            ):
                script = stage1_generate_script.run(_PC, _RUN_ID, registry)
        return script, registry

    # --- return value checks ------------------------------------------------

    def test_returns_dict(self, tmp_path):
        script, _ = self._run_stub(tmp_path)
        assert isinstance(script, dict)

    def test_script_schema_id(self, tmp_path):
        script, _ = self._run_stub(tmp_path)
        assert script["schema_id"] == "Script"

    def test_script_schema_version(self, tmp_path):
        script, _ = self._run_stub(tmp_path)
        assert script["schema_version"] == "1.0.0"

    # --- registry artifact written ------------------------------------------

    def test_script_registered_and_valid(self, tmp_path):
        _, registry = self._run_stub(tmp_path)
        assert registry.exists_and_valid("proj-1", _RUN_ID, "Script")

    # --- intermediate artifacts written (§42 flow) --------------------------

    def test_draft_story_written_to_run_dir(self, tmp_path):
        """DraftStory.json must be written before validation (step 1)."""
        _, registry = self._run_stub(tmp_path)
        draft_path = registry.run_dir("proj-1", _RUN_ID) / "DraftStory.json"
        assert draft_path.exists(), "DraftStory.json not found in run dir"
        draft = json.loads(draft_path.read_text(encoding="utf-8"))
        assert draft["schema_id"] == "DraftStory"

    def test_story_prompt_written_to_run_dir(self, tmp_path):
        """StoryPrompt.json must be written AFTER validation passes (step 3)."""
        _, registry = self._run_stub(tmp_path)
        sp_path = registry.run_dir("proj-1", _RUN_ID) / "StoryPrompt.json"
        assert sp_path.exists(), "StoryPrompt.json not found in run dir"
        sp = json.loads(sp_path.read_text(encoding="utf-8"))
        assert sp["schema_id"] == "StoryPrompt"

    # --- CanonViolationError gate -------------------------------------------

    def test_canon_violation_error_propagates(self, tmp_path):
        """When world-engine rejects the draft, CanonViolationError must propagate."""
        registry = ArtifactRegistry(tmp_path)
        bad = _make_we_result(1, "violation: character not in canon")

        with patch(
            "orchestrator.stages.stage1_generate_script.find_agent_bin",
            return_value=_WE_BIN,
        ):
            with patch(
                "orchestrator.stages.stage1_generate_script.call_agent",
                return_value=bad,
            ):
                with pytest.raises(CanonViolationError):
                    stage1_generate_script.run(_PC, _RUN_ID, registry)

    def test_story_prompt_not_written_on_canon_violation(self, tmp_path):
        """StoryPrompt.json must NOT be written when the validation gate fires."""
        registry = ArtifactRegistry(tmp_path)
        bad = _make_we_result(1, "violation")

        with patch(
            "orchestrator.stages.stage1_generate_script.find_agent_bin",
            return_value=_WE_BIN,
        ):
            with patch(
                "orchestrator.stages.stage1_generate_script.call_agent",
                return_value=bad,
            ):
                with pytest.raises(CanonViolationError):
                    stage1_generate_script.run(_PC, _RUN_ID, registry)

        sp_path = registry.run_dir("proj-1", _RUN_ID) / "StoryPrompt.json"
        assert not sp_path.exists(), (
            "StoryPrompt.json was written despite canon violation — "
            "§42 validation gate is broken"
        )

    def test_script_not_written_on_canon_violation(self, tmp_path):
        """Script.json (registry artifact) must NOT be written when validation fails."""
        registry = ArtifactRegistry(tmp_path)
        bad = _make_we_result(1, "violation")

        with patch(
            "orchestrator.stages.stage1_generate_script.find_agent_bin",
            return_value=_WE_BIN,
        ):
            with patch(
                "orchestrator.stages.stage1_generate_script.call_agent",
                return_value=bad,
            ):
                with pytest.raises(CanonViolationError):
                    stage1_generate_script.run(_PC, _RUN_ID, registry)

        assert not registry.exists_and_valid("proj-1", _RUN_ID, "Script"), (
            "Script was written to registry despite canon violation"
        )

    # --- hand-authored DraftStory.json in project dir -----------------------

    def test_existing_draft_story_is_used_instead_of_auto_generating(self, tmp_path):
        """If project dir contains DraftStory.json, it must be used (not auto-generated)."""
        # Create a fake project dir structure: tmp_path/project/project.json
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        # DraftStory.json lives alongside project.json
        hand_draft = {
            "schema_id":       "DraftStory",
            "schema_version":  "1.0.0",
            "draft_id":        "hand-authored-draft",
            "project_id":      "proj-1",
            "title":           "Hand-Authored Title",
            "genre":           "drama",
            "visual_style":    "cinematic",
            "generation_seed": 42,
            "primary_location": "Bespoke Location",
            "characters": [
                {"id": "char-a", "role": "lead"},
                {"id": "char-b", "role": "support"},
            ],
            "max_scenes": 2,
            "scenes": [{"scene_index": 1, "description": "Scene 1"}],
        }
        (project_dir / "DraftStory.json").write_text(
            json.dumps(hand_draft), encoding="utf-8"
        )
        pc = dict(_PC, _project_path=str(project_dir / "project.json"))

        registry = ArtifactRegistry(tmp_path / "registry")
        with patch(
            "orchestrator.stages.stage1_generate_script.find_agent_bin",
            return_value=None,
        ):
            with patch(
                "orchestrator.stages.stage1_generate_script.call_agent",
                return_value=_make_we_result(1),
            ):
                stage1_generate_script.run(pc, _RUN_ID, registry)

        # StoryPrompt's episode_goal must reference the hand-authored title
        sp_path = registry.run_dir("proj-1", _RUN_ID) / "StoryPrompt.json"
        sp = json.loads(sp_path.read_text(encoding="utf-8"))
        assert "Hand-Authored Title" in sp["episode_goal"]
