"""Tests for ArtifactRegistry existence/validity checks and PipelineRunner resume logic."""

import json
from unittest.mock import patch

import pytest

from orchestrator.pipeline import PipelineRunner, compute_run_id
from orchestrator.registry import ArtifactRegistry

# ---------------------------------------------------------------------------
# Stage 5 stub — used by pipeline-level tests so they don't need the real
# video renderer binary.
# ---------------------------------------------------------------------------

_STUB_RENDER_OUTPUT = {
    "schema_version": "1.0.0",
    "output_id": "test-output-001",
    "video_uri": "file:///tmp/test/output.mp4",
    "captions_uri": "file:///tmp/test/output.srt",
    "hashes": {
        "video_sha256": "a" * 64,
        "captions_sha256": "b" * 64,
    },
}


@pytest.fixture()
def mock_stage5():
    """Patch stage5_render_preview.run to avoid needing the real video renderer."""

    def _stub(project_config, run_id, registry):
        pid = project_config["id"]
        ro = {**_STUB_RENDER_OUTPUT, "project_id": pid}
        registry.write_artifact(
            pid, run_id, "RenderOutput", ro,
            parent_refs=[],
            creation_params={"stage": "stage5_render_preview"},
        )
        return ro

    with patch("orchestrator.stages.stage5_render_preview.run", side_effect=_stub):
        yield

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


class TestMalformedMetaStillValid:
    def test_malformed_meta_treated_as_absent(self, registry):
        """exists_and_valid returns True for valid artifact with unparseable meta."""
        registry.write_artifact(
            "proj", "run-004", "Script", VALID_SCRIPT,
            parent_refs=[], creation_params={}
        )
        meta_p = registry.meta_path("proj", "run-004", "Script")
        meta_p.write_text("not valid json {{{", encoding="utf-8")
        assert registry.exists_and_valid("proj", "run-004", "Script") is True


class TestMissingHashKeyStillValid:
    def test_meta_without_hash_key_treated_as_absent(self, registry):
        """exists_and_valid returns True when meta exists but has no 'hash' key."""
        registry.write_artifact(
            "proj", "run-005", "Script", VALID_SCRIPT,
            parent_refs=[], creation_params={}
        )
        meta_p = registry.meta_path("proj", "run-005", "Script")
        meta_p.write_text('{"artifact_type": "Script"}', encoding="utf-8")
        assert registry.exists_and_valid("proj", "run-005", "Script") is True


class TestHashMismatchInvalidatesArtifact:
    def test_hash_mismatch_returns_false(self, registry):
        """exists_and_valid returns False when artifact content differs from stored hash."""
        registry.write_artifact(
            "proj", "run-006", "Script", VALID_SCRIPT,
            parent_refs=[], creation_params={}
        )
        # Overwrite artifact with a different (still schema-valid) Script; leave meta unchanged
        modified_script = {**VALID_SCRIPT, "title": "Modified Title"}
        path = registry.artifact_path("proj", "run-006", "Script")
        path.write_text(
            json.dumps(modified_script, indent=2, sort_keys=True), encoding="utf-8"
        )
        assert registry.exists_and_valid("proj", "run-006", "Script") is False


# ---------------------------------------------------------------------------
# PipelineRunner tests
# ---------------------------------------------------------------------------


class TestPipelineSkipsExistingStage:
    def test_pipeline_skips_existing_stage(self, tmp_path, mock_stage5):
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
    def test_pipeline_force_reruns(self, tmp_path, mock_stage5):
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
    def test_from_stage_reruns_from_n(self, tmp_path, mock_stage5):
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
