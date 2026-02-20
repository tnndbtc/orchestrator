"""Tests for RunIndex.json generation, explain, and replay commands."""

import json
import re
import time
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from orchestrator.cli import cli
from orchestrator.pipeline import PipelineRunner, compute_run_id, write_run_index
from orchestrator.registry import ArtifactRegistry
from orchestrator.utils.hashing import hash_artifact, hash_file_bytes

# ---------------------------------------------------------------------------
# Stage 5 stub — identical to test_resume.py (each test file is self-contained)
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
# Shared project config used across all tests
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


# ---------------------------------------------------------------------------
# Helper: run the full pipeline and return (summary, run_dir)
# ---------------------------------------------------------------------------

def _run_full_pipeline(
    tmp_path: Path,
    project_config: dict = PROJECT_CONFIG,
    project_path: str = "",
    force: bool = False,
) -> tuple[dict, Path]:
    registry = ArtifactRegistry(tmp_path)
    run_id = compute_run_id(project_config)
    runner = PipelineRunner(
        project_config=project_config,
        registry=registry,
        artifacts_dir=tmp_path,
        run_id=run_id,
        project_path=project_path,
        force=force,
    )
    summary = runner.run()
    run_dir = tmp_path / project_config["id"] / run_id
    return summary, run_dir


# ===========================================================================
# TestHashFileBytes
# ===========================================================================

class TestHashFileBytes:
    def test_hash_length_64(self, tmp_path):
        f = tmp_path / "data.bin"
        f.write_bytes(b"some content")
        assert len(hash_file_bytes(f)) == 64

    def test_hash_is_hex(self, tmp_path):
        f = tmp_path / "data.bin"
        f.write_bytes(b"some content")
        h = hash_file_bytes(f)
        assert re.fullmatch(r"[0-9a-f]{64}", h) is not None

    def test_differs_from_hash_artifact(self, tmp_path):
        """hash_file_bytes and hash_artifact produce different digests for the same data."""
        data = {"key": "value", "num": 42}
        f = tmp_path / "data.json"
        f.write_text(json.dumps(data), encoding="utf-8")
        file_hash = hash_file_bytes(f)
        artifact_hash = hash_artifact(data)
        assert file_hash != artifact_hash

    def test_changes_with_content(self, tmp_path):
        f = tmp_path / "data.bin"
        f.write_bytes(b"version 1")
        h1 = hash_file_bytes(f)
        f.write_bytes(b"version 2")
        h2 = hash_file_bytes(f)
        assert h1 != h2

    def test_deterministic(self, tmp_path):
        f = tmp_path / "data.bin"
        f.write_bytes(b"stable content")
        h1 = hash_file_bytes(f)
        h2 = hash_file_bytes(f)
        assert h1 == h2


# ===========================================================================
# TestWriteRunIndex
# ===========================================================================

class TestWriteRunIndex:
    def test_file_created(self, tmp_path, mock_stage5):
        summary, run_dir = _run_full_pipeline(tmp_path)
        assert summary["status"] == "completed"
        assert (run_dir / "RunIndex.json").exists()

    def test_schema_fields(self, tmp_path, mock_stage5):
        _, run_dir = _run_full_pipeline(tmp_path)
        idx = json.loads((run_dir / "RunIndex.json").read_text(encoding="utf-8"))
        assert idx["schema_id"] == "RunIndex"
        assert idx["schema_version"] == "0.0.1"
        assert "run_id" in idx
        assert len(idx["run_id"]) == 64, "run_id should be a 64-char hex digest"
        assert idx["pipeline_version"] == "phase0"
        assert isinstance(idx["stages"], list)

    def test_five_stages(self, tmp_path, mock_stage5):
        _, run_dir = _run_full_pipeline(tmp_path)
        idx = json.loads((run_dir / "RunIndex.json").read_text(encoding="utf-8"))
        assert len(idx["stages"]) == 5

    def test_stage1_empty_inputs(self, tmp_path, mock_stage5):
        _, run_dir = _run_full_pipeline(tmp_path)
        idx = json.loads((run_dir / "RunIndex.json").read_text(encoding="utf-8"))
        stage1 = next(s for s in idx["stages"] if s["name"] == "stage1_generate_script")
        assert stage1["inputs"] == [], (
            "stage1_generate_script has no upstream artifacts and must have empty inputs"
        )

    def test_relative_paths(self, tmp_path, mock_stage5):
        _, run_dir = _run_full_pipeline(tmp_path)
        idx = json.loads((run_dir / "RunIndex.json").read_text(encoding="utf-8"))
        for stage in idx["stages"]:
            for entry in stage["inputs"] + stage["outputs"]:
                assert not Path(entry["path"]).is_absolute(), (
                    f"Expected relative path but got absolute: {entry['path']}"
                )

    def test_sha256_correctness(self, tmp_path, mock_stage5):
        _, run_dir = _run_full_pipeline(tmp_path)
        idx = json.loads((run_dir / "RunIndex.json").read_text(encoding="utf-8"))
        for stage in idx["stages"]:
            for entry in stage["outputs"]:
                file_path = run_dir / entry["path"]
                assert file_path.exists(), f"Output file {entry['path']} must exist"
                assert hash_file_bytes(file_path) == entry["sha256"], (
                    f"sha256 mismatch for {entry['path']}"
                )

    def test_not_written_on_failure(self, tmp_path):
        """RunIndex.json must NOT be created when the pipeline fails."""
        def _fail(project_config, run_id, registry):
            raise RuntimeError("Stage 1 intentionally failed")

        with patch("orchestrator.stages.stage1_generate_script.run", side_effect=_fail):
            summary, run_dir = _run_full_pipeline(tmp_path)

        assert summary["status"] == "failed"
        assert not (run_dir / "RunIndex.json").exists(), (
            "RunIndex.json must not be written for a failed pipeline run"
        )

    def test_run_id_deterministic(self, tmp_path, mock_stage5):
        """The RunIndex run_id is stable across two runs with identical input files."""
        _, run_dir = _run_full_pipeline(tmp_path)
        idx1 = json.loads((run_dir / "RunIndex.json").read_text(encoding="utf-8"))

        # Force re-run to regenerate RunIndex.json in-place
        _run_full_pipeline(tmp_path, force=True)
        idx2 = json.loads((run_dir / "RunIndex.json").read_text(encoding="utf-8"))

        assert idx1["run_id"] == idx2["run_id"], (
            "RunIndex run_id must be deterministic for identical input files"
        )


# ===========================================================================
# TestExplainCommand
# ===========================================================================

class TestExplainCommand:
    def _get_run_dir(self, tmp_path) -> Path:
        """Run the full pipeline and return the run directory (stage5 must be mocked first)."""
        _, run_dir = _run_full_pipeline(tmp_path)
        return run_dir

    def test_stage_names_present(self, tmp_path, mock_stage5):
        run_dir = self._get_run_dir(tmp_path)
        result = CliRunner().invoke(cli, ["explain", "--run", str(run_dir)])
        assert result.exit_code == 0
        assert "stage1_generate_script" in result.output
        assert "stage5_render_preview" in result.output

    def test_no_timestamps(self, tmp_path, mock_stage5):
        run_dir = self._get_run_dir(tmp_path)
        result = CliRunner().invoke(cli, ["explain", "--run", str(run_dir)])
        assert result.exit_code == 0
        # ISO-8601 datetime pattern: YYYY-MM-DDTHH:MM:SS
        assert not re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", result.output), (
            "explain output must contain no timestamps"
        )

    def test_exit_1_without_run_index(self, tmp_path):
        run_dir = tmp_path / "empty_run"
        run_dir.mkdir()
        result = CliRunner().invoke(cli, ["explain", "--run", str(run_dir)])
        assert result.exit_code == 1

    def test_sha256_hashes_in_output(self, tmp_path, mock_stage5):
        run_dir = self._get_run_dir(tmp_path)
        result = CliRunner().invoke(cli, ["explain", "--run", str(run_dir)])
        assert result.exit_code == 0
        hex_hashes = re.findall(r"\b[0-9a-f]{64}\b", result.output)
        assert len(hex_hashes) > 0, "explain output must include at least one sha256 hash"

    def test_stage1_inputs_empty(self, tmp_path, mock_stage5):
        run_dir = self._get_run_dir(tmp_path)
        result = CliRunner().invoke(cli, ["explain", "--run", str(run_dir)])
        assert result.exit_code == 0

        output = result.output
        # Locate the stage1 block (everything between "Stage: stage1..." and next "Stage:")
        stage1_start = output.index("stage1_generate_script")
        rest = output[stage1_start + 1:]
        next_stage_match = re.search(r"^Stage:", rest, re.MULTILINE)
        stage1_block = rest[: next_stage_match.start()] if next_stage_match else rest

        # The inputs section is everything between "inputs:" and "outputs:"
        inputs_start = stage1_block.index("inputs:") + len("inputs:")
        outputs_start = stage1_block.index("outputs:")
        inputs_section = stage1_block[inputs_start:outputs_start]

        assert not re.search(r"[0-9a-f]{64}", inputs_section), (
            "stage1_generate_script should have no sha256 hashes in its inputs section"
        )


# ===========================================================================
# TestReplayCommand
# ===========================================================================

class TestReplayCommand:
    def _setup_run(self, tmp_path: Path) -> Path:
        """Run the pipeline with a real project.json and return the run directory.
        Stage5 must be mocked by the calling test via mock_stage5 fixture.
        """
        project_file = tmp_path / "project.json"
        project_file.write_text(json.dumps(PROJECT_CONFIG), encoding="utf-8")
        _, run_dir = _run_full_pipeline(tmp_path, project_path=str(project_file))
        return run_dir

    def test_noop_when_all_valid(self, tmp_path, mock_stage5):
        """replay exits 0 and reports success when all outputs are valid."""
        run_dir = self._setup_run(tmp_path)
        result = CliRunner().invoke(cli, ["replay", "--run", str(run_dir)])
        assert result.exit_code == 0, result.output
        assert "completed" in result.output.lower()

    def test_detects_mismatch_and_reruns(self, tmp_path, mock_stage5):
        """replay detects a hash mismatch and re-runs the affected stage."""
        run_dir = self._setup_run(tmp_path)
        # Corrupt ShotList.json by appending whitespace (still valid JSON)
        corrupt_file = run_dir / "ShotList.json"
        with open(corrupt_file, "ab") as fh:
            fh.write(b" ")
        result = CliRunner().invoke(cli, ["replay", "--run", str(run_dir)])
        assert result.exit_code == 0, result.output
        assert "Hash mismatch" in result.output
        assert "ShotList.json" in result.output

    def test_exit_1_without_run_index(self, tmp_path):
        run_dir = tmp_path / "empty_run"
        run_dir.mkdir()
        result = CliRunner().invoke(cli, ["replay", "--run", str(run_dir)])
        assert result.exit_code == 1

    def test_never_overwrites_valid_outputs(self, tmp_path, mock_stage5):
        """Replay must not rewrite files whose hashes are still valid."""
        run_dir = self._setup_run(tmp_path)
        script_file = run_dir / "Script.json"
        original_mtime_ns = script_file.stat().st_mtime_ns

        # Brief pause so that any re-write would produce a detectably newer mtime
        time.sleep(0.05)

        result = CliRunner().invoke(cli, ["replay", "--run", str(run_dir)])
        assert result.exit_code == 0, result.output
        assert script_file.stat().st_mtime_ns == original_mtime_ns, (
            "Script.json must not be rewritten when its hash is still valid"
        )

    def test_removes_corrupted_file_and_meta(self, tmp_path, mock_stage5):
        """replay deletes a corrupted artifact and its .meta.json before re-running."""
        run_dir = self._setup_run(tmp_path)
        corrupt_file = run_dir / "ShotList.json"
        meta_file = run_dir / "ShotList.meta.json"

        # Corrupt the file
        with open(corrupt_file, "ab") as fh:
            fh.write(b" ")

        result = CliRunner().invoke(cli, ["replay", "--run", str(run_dir)])
        assert result.exit_code == 0, result.output

        # File should have been re-created by the re-run stage
        assert corrupt_file.exists(), "ShotList.json should be re-created after replay"
        assert meta_file.exists(), "ShotList.meta.json should be re-created after replay"
