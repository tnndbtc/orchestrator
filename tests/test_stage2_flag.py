"""Tests for stage2_script_to_shotlist — world-engine CLI flag correctness.

Verifies that call_agent is invoked with the correct flags per the §41.4 contract:
    world-engine produce-shotlist --script <Script.json> --out <tmp.json>

The critical regression guard: old code used '--output'; the §41.4 contract
specifies '--out'.  These tests patch call_agent to capture args without
actually running world-engine.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from orchestrator.registry import ArtifactRegistry
from orchestrator.stages import stage2_script_to_shotlist as stage2


# ---------------------------------------------------------------------------
# Helpers / constants
# ---------------------------------------------------------------------------

_PC = {"id": "proj-1"}
_RUN_ID = "run-flag-test"

_MINIMAL_SCRIPT = {
    "schema_id":      "Script",
    "schema_version": "1.0.0",
    "script_id":      "script-flag-test",
    "project_id":     "proj-1",
    "title":          "Flag Test Episode",
    "scenes": [
        {
            "scene_id":    "scene-001",
            "location":    "INT. ROOM",
            "time_of_day": "DAY",
            "actions":     [],
        }
    ],
}


def _make_result(returncode: int) -> MagicMock:
    m = MagicMock()
    m.returncode = returncode
    m.stderr = ""
    return m


def _setup_registry(tmp_path: Path) -> ArtifactRegistry:
    """Write a minimal Script.json so stage2 can read it from the registry."""
    registry = ArtifactRegistry(tmp_path)
    registry.write_artifact(
        "proj-1", _RUN_ID, "Script", _MINIMAL_SCRIPT,
        parent_refs=[],
        creation_params={"stage": "stage1"},
    )
    return registry


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestWorldEngineCliFlags:
    """Verify the exact flags passed to world-engine (§41.4 CLI contract)."""

    def test_uses_out_flag_not_output(self, tmp_path):
        """call_agent must be called with '--out', not the legacy '--output'."""
        registry = _setup_registry(tmp_path)
        captured: dict = {}

        def _side(name, args, **kwargs):
            captured["name"] = name
            captured["args"] = list(args)
            return _make_result(1)  # non-zero → fall back to stub; we only care about args

        with patch("orchestrator.stages.stage2_script_to_shotlist.call_agent", side_effect=_side):
            stage2.run(_PC, _RUN_ID, registry)

        assert captured.get("name") == "world-engine", (
            "call_agent was not called with 'world-engine'"
        )
        assert "--out" in captured["args"], (
            f"Expected '--out' in call_agent args, got: {captured['args']}"
        )
        assert "--output" not in captured["args"], (
            "Legacy '--output' flag found in call_agent args; should be '--out'"
        )

    def test_uses_produce_shotlist_subcommand(self, tmp_path):
        """First positional arg to world-engine must be 'produce-shotlist'."""
        registry = _setup_registry(tmp_path)
        captured: dict = {}

        def _side(name, args, **kwargs):
            captured["args"] = list(args)
            return _make_result(1)

        with patch("orchestrator.stages.stage2_script_to_shotlist.call_agent", side_effect=_side):
            stage2.run(_PC, _RUN_ID, registry)

        assert captured["args"][0] == "produce-shotlist", (
            f"Expected 'produce-shotlist' as first arg, got: {captured['args'][0]!r}"
        )

    def test_passes_script_flag(self, tmp_path):
        """--script must appear in call_agent args and point to Script.json."""
        registry = _setup_registry(tmp_path)
        expected_script_path = registry.artifact_path("proj-1", _RUN_ID, "Script")
        captured: dict = {}

        def _side(name, args, **kwargs):
            captured["args"] = list(args)
            return _make_result(1)

        with patch("orchestrator.stages.stage2_script_to_shotlist.call_agent", side_effect=_side):
            stage2.run(_PC, _RUN_ID, registry)

        args = captured["args"]
        assert "--script" in args, "Expected '--script' in call_agent args"
        script_idx = args.index("--script") + 1
        assert args[script_idx] == str(expected_script_path), (
            f"--script value mismatch: {args[script_idx]!r} != {str(expected_script_path)!r}"
        )

    def test_stub_fallback_when_world_engine_fails(self, tmp_path):
        """When call_agent returns non-zero, stage2 falls back to the deterministic stub."""
        registry = _setup_registry(tmp_path)

        with patch(
            "orchestrator.stages.stage2_script_to_shotlist.call_agent",
            return_value=_make_result(1),
        ):
            shotlist = stage2.run(_PC, _RUN_ID, registry)

        assert shotlist["schema_id"] == "ShotList"
        assert registry.exists_and_valid("proj-1", _RUN_ID, "ShotList")

    def test_stub_fallback_produces_valid_registry_artifact(self, tmp_path):
        """Stub ShotList written to registry must pass schema validation."""
        registry = _setup_registry(tmp_path)

        with patch(
            "orchestrator.stages.stage2_script_to_shotlist.call_agent",
            return_value=_make_result(1),
        ):
            stage2.run(_PC, _RUN_ID, registry)

        # exists_and_valid internally calls validate_artifact
        assert registry.exists_and_valid("proj-1", _RUN_ID, "ShotList"), (
            "ShotList written by stub fallback failed schema validation"
        )
