"""Tests for ArtifactRegistry existence/validity checks and PipelineRunner resume logic."""

import json

import pytest

from orchestrator.pipeline import PipelineRunner, compute_run_id
from orchestrator.registry import ArtifactRegistry

# ---------------------------------------------------------------------------
# Shared project config used across all pipeline tests
# ---------------------------------------------------------------------------

PROJECT_CONFIG: dict = {
    "id": "test-project",
    "title": "Test Project",
    "genre": "test",
    "visual_style": "minimal",
    "target_duration": 30,
    "render_profiles": ["preview_local"],
    "continuity_mode": "sequential",
    "cost_policy": {"max_budget_usd": 0.0, "external_ai": "disabled"},
}

VALID_SCRIPT: dict = {
    "schema_version": "1.0.0",
    "script_id": "test-script-001",
    "project_id": "test-project",
    "title": "Test Script",
    "scenes": [
        {
            "scene_id": "scene-001",
            "location": "INT. TEST ROOM",
            "time_of_day": "DAY",
            "actions": [
                {
                    "type": "dialogue",
                    "character": "NARRATOR",
                    "text": "This is a test line.",
                }
            ],
        }
    ],
}


@pytest.fixture
def registry(tmp_path):
    return ArtifactRegistry(tmp_path)


# ---------------------------------------------------------------------------
# Registry unit tests
# ---------------------------------------------------------------------------


class TestArtifactNotFound:
    def test_artifact_not_found(self, registry):
        """exists_and_valid returns False when no file exists."""
        assert registry.exists_and_valid("proj", "run-000", "Script") is False


class TestWriteReadRoundtrip:
    def test_write_and_read_roundtrip(self, registry):
        """Data written with write_artifact is returned unchanged by read_artifact."""
        registry.write_artifact(
            "proj", "run-001", "Script", VALID_SCRIPT,
            parent_refs=[], creation_params={}
        )
        result = registry.read_artifact("proj", "run-001", "Script")
        assert result["script_id"] == VALID_SCRIPT["script_id"]
        assert result["scenes"] == VALID_SCRIPT["scenes"]


class TestExistsValidAfterWrite:
    def test_exists_valid_after_write(self, registry):
        """exists_and_valid returns True after a valid artifact is written."""
        registry.write_artifact(
            "proj", "run-002", "Script", VALID_SCRIPT,
            parent_refs=[], creation_params={}
        )
        assert registry.exists_and_valid("proj", "run-002", "Script") is True


class TestInvalidArtifactNotValid:
    def test_invalid_artifact_not_valid(self, registry):
        """exists_and_valid returns False when the file fails schema validation."""
        path = registry.artifact_path("proj", "run-003", "Script")
        path.parent.mkdir(parents=True, exist_ok=True)
        # Missing required fields — schema-invalid
        path.write_text(json.dumps({"schema_version": "1.0.0"}), encoding="utf-8")
        assert registry.exists_and_valid("proj", "run-003", "Script") is False


# ---------------------------------------------------------------------------
# PipelineRunner tests
# ---------------------------------------------------------------------------


class TestPipelineSkipsExistingStage:
    def test_pipeline_skips_existing_stage(self, tmp_path):
        """Second run (no force) skips all stages whose artifacts are already valid."""
        registry = ArtifactRegistry(tmp_path)
        run_id = compute_run_id(PROJECT_CONFIG)

        # First run — all stages execute
        runner1 = PipelineRunner(
            project_config=PROJECT_CONFIG,
            registry=registry,
            artifacts_dir=tmp_path,
            force=False,
            run_id=run_id,
        )
        summary1 = runner1.run()
        assert summary1["status"] == "completed"

        # Second run — all stages should be skipped
        runner2 = PipelineRunner(
            project_config=PROJECT_CONFIG,
            registry=registry,
            artifacts_dir=tmp_path,
            force=False,
            run_id=run_id,
        )
        summary2 = runner2.run()
        assert all(s["skipped"] for s in summary2["stages"]), (
            "All stages should be skipped on second run"
        )


class TestPipelineForceReruns:
    def test_pipeline_force_reruns(self, tmp_path):
        """force=True causes all stages to re-execute; hashes are identical (determinism)."""
        registry = ArtifactRegistry(tmp_path)
        run_id = compute_run_id(PROJECT_CONFIG)

        runner1 = PipelineRunner(
            project_config=PROJECT_CONFIG,
            registry=registry,
            artifacts_dir=tmp_path,
            force=True,
            run_id=run_id,
        )
        summary1 = runner1.run()
        assert summary1["status"] == "completed"

        runner2 = PipelineRunner(
            project_config=PROJECT_CONFIG,
            registry=registry,
            artifacts_dir=tmp_path,
            force=True,
            run_id=run_id,
        )
        summary2 = runner2.run()

        # No stage skipped
        assert all(not s["skipped"] for s in summary2["stages"]), (
            "No stage should be skipped when force=True"
        )

        # Hashes must be identical (deterministic stubs)
        for s1, s2 in zip(summary1["stages"], summary2["stages"]):
            if s1["artifact_hash"] is not None:
                assert s1["artifact_hash"] == s2["artifact_hash"], (
                    f"Hash mismatch for {s1['name']}: {s1['artifact_hash']} != {s2['artifact_hash']}"
                )


class TestFromStageReruns:
    def test_from_stage_reruns_from_n(self, tmp_path):
        """from_stage=3 skips stages 1–2, re-runs stages 3–5."""
        registry = ArtifactRegistry(tmp_path)
        run_id = compute_run_id(PROJECT_CONFIG)

        # Full first run to populate all artifacts
        runner1 = PipelineRunner(
            project_config=PROJECT_CONFIG,
            registry=registry,
            artifacts_dir=tmp_path,
            force=False,
            run_id=run_id,
        )
        summary1 = runner1.run()
        assert summary1["status"] == "completed"

        # Re-run from stage 3 with force so stages 3-5 actually execute
        runner2 = PipelineRunner(
            project_config=PROJECT_CONFIG,
            registry=registry,
            artifacts_dir=tmp_path,
            force=True,
            from_stage=3,
            run_id=run_id,
        )
        summary2 = runner2.run()
        assert summary2["status"] == "completed"

        for stage in summary2["stages"]:
            stage_num = stage["stage_num"]
            if stage_num < 3:
                assert stage["skipped"] is True, (
                    f"Stage {stage_num} should be skipped (< from_stage=3)"
                )
            else:
                assert stage["skipped"] is False, (
                    f"Stage {stage_num} should NOT be skipped (>= from_stage=3, force=True)"
                )
