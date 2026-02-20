"""Wave 4 tests: validate-run and diff CLI commands + integration smoke test."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from orchestrator.cli import cli
from orchestrator.pipeline import PipelineRunner, compute_run_id
from orchestrator.registry import ArtifactRegistry
from orchestrator.utils.hashing import hash_file_bytes


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _write_artifact(path: Path, data: dict) -> str:
    """Write JSON artifact to path, return sha256."""
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return hash_file_bytes(path)


def _make_minimal_run_dir(
    base: Path,
    artifacts: dict[str, dict],
    run_id: str = "test-run-001",
    status: str | None = None,
    failure_reason: str | None = None,
) -> Path:
    """Write artifacts + RunIndex.json; return run dir path."""
    base.mkdir(parents=True, exist_ok=True)
    stages = []
    for art_name, data in artifacts.items():
        sha = _write_artifact(base / f"{art_name}.json", data)
        stages.append({
            "name": f"stage_{art_name.lower()}",
            "inputs": [],
            "outputs": [{"path": f"{art_name}.json", "sha256": sha,
                          "schema_version": data.get("schema_version", "1.0.0"),
                          "schema_id": data.get("schema_id", art_name)}],
        })
    run_index: dict = {
        "schema_id": "RunIndex", "schema_version": "0.0.2",
        "run_id": run_id, "pipeline_version": "phase0", "stages": stages,
    }
    if status is not None:
        run_index["status"] = status
    if failure_reason is not None:
        run_index["failure_reason"] = failure_reason
    (base / "RunIndex.json").write_text(json.dumps(run_index, indent=2), encoding="utf-8")
    return base


_MINIMAL_ARTIFACT = {
    "schema_id": "Script",
    "schema_version": "1.0.0",
    "title": "Test Script",
    "scenes": [],
}


# ===========================================================================
# TestValidateRunCommand
# ===========================================================================

class TestValidateRunCommand:
    def test_valid_run_success(self, tmp_path):
        """Valid dir with correct sha256 + allow CanonDecision → 'OK: run valid', exit 0."""
        run_dir = _make_minimal_run_dir(tmp_path / "run", {"Script": _MINIMAL_ARTIFACT})
        # Write a CanonDecision.json with allow
        canon = {
            "schema_id": "CanonDecision",
            "schema_version": "1.0.0",
            "decision": "allow",
            "decision_id": "test-allow-01",
        }
        (run_dir / "CanonDecision.json").write_text(json.dumps(canon), encoding="utf-8")
        result = CliRunner(mix_stderr=False).invoke(cli, ["validate-run", "--run", str(run_dir)])
        assert result.exit_code == 0, result.output
        assert "OK: run valid" in result.output

    def test_hash_mismatch(self, tmp_path):
        """Corrupt artifact bytes after RunIndex written → 'ERROR: hash mismatch for Script.json', exit non-zero."""
        run_dir = _make_minimal_run_dir(tmp_path / "run", {"Script": _MINIMAL_ARTIFACT})
        # Corrupt the artifact after RunIndex was written
        (run_dir / "Script.json").write_bytes(b"corrupted content")
        result = CliRunner(mix_stderr=False).invoke(cli, ["validate-run", "--run", str(run_dir)])
        assert result.exit_code != 0
        assert "ERROR: hash mismatch for Script.json" in result.output

    def test_missing_schema_metadata(self, tmp_path):
        """Artifact lacks schema_id → 'ERROR: missing schema metadata for', exit non-zero."""
        run_dir = tmp_path / "run"
        run_dir.mkdir(parents=True, exist_ok=True)
        # Write artifact without schema_id
        bare_artifact = {"schema_version": "1.0.0", "title": "No schema_id here"}
        sha = _write_artifact(run_dir / "Script.json", bare_artifact)
        # Write RunIndex with the correct sha256 for this bare artifact
        run_index = {
            "schema_id": "RunIndex", "schema_version": "0.0.2",
            "run_id": "test-run-001", "pipeline_version": "phase0",
            "stages": [{
                "name": "stage_script",
                "inputs": [],
                "outputs": [{"path": "Script.json", "sha256": sha,
                              "schema_version": "1.0.0", "schema_id": "Script"}],
            }],
        }
        (run_dir / "RunIndex.json").write_text(json.dumps(run_index, indent=2), encoding="utf-8")
        result = CliRunner(mix_stderr=False).invoke(cli, ["validate-run", "--run", str(run_dir)])
        assert result.exit_code != 0
        assert "ERROR: missing schema metadata for" in result.output

    def test_canon_decision_consistency_ok(self, tmp_path):
        """RunIndex no status + allow CanonDecision → passes (exit 0)."""
        run_dir = _make_minimal_run_dir(tmp_path / "run", {"Script": _MINIMAL_ARTIFACT})
        canon = {
            "schema_id": "CanonDecision",
            "schema_version": "1.0.0",
            "decision": "allow",
            "decision_id": "test-allow-02",
        }
        (run_dir / "CanonDecision.json").write_text(json.dumps(canon), encoding="utf-8")
        result = CliRunner(mix_stderr=False).invoke(cli, ["validate-run", "--run", str(run_dir)])
        assert result.exit_code == 0

    def test_canon_decision_inconsistency(self, tmp_path):
        """RunIndex no status + deny CanonDecision → 'ERROR: CanonDecision inconsistency', exit non-zero."""
        run_dir = _make_minimal_run_dir(tmp_path / "run", {"Script": _MINIMAL_ARTIFACT})
        # RunIndex has no "status" (implies completed), but decision=deny is inconsistent
        canon = {
            "schema_id": "CanonDecision",
            "schema_version": "1.0.0",
            "decision": "deny",
            "decision_id": "test-deny-01",
        }
        (run_dir / "CanonDecision.json").write_text(json.dumps(canon), encoding="utf-8")
        result = CliRunner(mix_stderr=False).invoke(cli, ["validate-run", "--run", str(run_dir)])
        assert result.exit_code != 0
        assert "ERROR: CanonDecision inconsistency" in result.output


# ===========================================================================
# TestDiffCommand
# ===========================================================================

class TestDiffCommand:
    def test_identical_dirs(self, tmp_path):
        """Same artifact content in both dirs → 'OK: no differences', exit 0."""
        dir_a = _make_minimal_run_dir(tmp_path / "run_a", {"Script": _MINIMAL_ARTIFACT})
        dir_b = _make_minimal_run_dir(tmp_path / "run_b", {"Script": _MINIMAL_ARTIFACT})
        result = CliRunner(mix_stderr=False).invoke(
            cli, ["diff", "--run", str(dir_a), "--against", str(dir_b)]
        )
        assert result.exit_code == 0, result.output
        assert "OK: no differences" in result.output

    def test_sha256_differs(self, tmp_path):
        """Different artifact bytes in dir_b → output contains '/sha256:' diff line."""
        dir_a = _make_minimal_run_dir(tmp_path / "run_a", {"Script": _MINIMAL_ARTIFACT})
        modified = {**_MINIMAL_ARTIFACT, "title": "Different Title"}
        dir_b = _make_minimal_run_dir(tmp_path / "run_b", {"Script": modified})
        result = CliRunner(mix_stderr=False).invoke(
            cli, ["diff", "--run", str(dir_a), "--against", str(dir_b)]
        )
        assert result.exit_code != 0
        assert "/sha256:" in result.output

    def test_json_field_differs(self, tmp_path):
        """Different title field → output contains '/json[title]:' diff line."""
        dir_a = _make_minimal_run_dir(tmp_path / "run_a", {"Script": _MINIMAL_ARTIFACT})
        modified = {**_MINIMAL_ARTIFACT, "title": "Changed Title"}
        dir_b = _make_minimal_run_dir(tmp_path / "run_b", {"Script": modified})
        result = CliRunner(mix_stderr=False).invoke(
            cli, ["diff", "--run", str(dir_a), "--against", str(dir_b)]
        )
        assert result.exit_code != 0
        assert "/json[title]:" in result.output

    def test_output_is_deterministic(self, tmp_path):
        """Run diff twice → assert output1 == output2."""
        dir_a = _make_minimal_run_dir(tmp_path / "run_a", {"Script": _MINIMAL_ARTIFACT})
        modified = {**_MINIMAL_ARTIFACT, "title": "Title X"}
        dir_b = _make_minimal_run_dir(tmp_path / "run_b", {"Script": modified})

        runner = CliRunner(mix_stderr=False)
        result1 = runner.invoke(cli, ["diff", "--run", str(dir_a), "--against", str(dir_b)])
        result2 = runner.invoke(cli, ["diff", "--run", str(dir_a), "--against", str(dir_b)])
        assert result1.output == result2.output


# ===========================================================================
# TestIntegrationSmoke
# ===========================================================================

_SMOKE_PROJECT = {
    "id": "smoke-test", "title": "Smoke Test", "genre": "test",
    "visual_style": "minimal", "target_duration": 10,
    "render_profiles": ["preview_local"], "continuity_mode": "sequential",
    "cost_policy": {"max_budget_usd": 0.0, "external_ai": "disabled"},
}

_CANON_ALLOW = {
    "schema_version": "1.0.0", "schema_id": "CanonDecision",
    "decision": "allow", "decision_id": "smoke-allow-01",
}

_STUB_RENDER_OUTPUT = {
    "schema_version": "1.0.0", "schema_id": "RenderOutput",
    "output_id": "smoke-001", "video_uri": "file:///tmp/smoke.mp4",
    "captions_uri": "file:///tmp/smoke.srt",
    "hashes": {"video_sha256": "a" * 64, "captions_sha256": "b" * 64},
}


def _stub_stage5(project_config, run_id, registry):
    pid = project_config["id"]
    ro = {**_STUB_RENDER_OUTPUT, "project_id": pid}
    registry.write_artifact(pid, run_id, "RenderOutput", ro,
                             parent_refs=[], creation_params={"stage": "stage5"})
    return ro


class TestIntegrationSmoke:
    def test_smoke_run_validate_diff(self, tmp_path):
        """Full pipeline → validate-run → diff-self smoke test."""
        # 1. Write project.json
        project_file = tmp_path / "project.json"
        project_file.write_text(json.dumps(_SMOKE_PROJECT), encoding="utf-8")

        # 2. Compute run_id and write CanonDecision.json
        run_id = compute_run_id(_SMOKE_PROJECT)
        artifacts_dir = tmp_path / "artifacts"
        run_dir = artifacts_dir / _SMOKE_PROJECT["id"] / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "CanonDecision.json").write_text(
            json.dumps(_CANON_ALLOW), encoding="utf-8"
        )

        # 3. Run the pipeline with stage5 stubbed
        registry = ArtifactRegistry(artifacts_dir)
        with patch("orchestrator.stages.stage5_render_preview.run", side_effect=_stub_stage5):
            runner = PipelineRunner(_SMOKE_PROJECT, registry, artifacts_dir)
            summary = runner.run()

        assert summary["status"] == "completed", f"Pipeline failed: {summary.get('errors')}"

        # 4. Run validate-run on the produced run dir
        result_validate = CliRunner(mix_stderr=False).invoke(
            cli, ["validate-run", "--run", str(run_dir)]
        )
        assert "OK: run valid" in result_validate.output, (
            f"validate-run failed:\n{result_validate.output}"
        )
        assert result_validate.exit_code == 0

        # 5. Run diff --run <dir> --against <dir> (self-diff should show no differences)
        result_diff = CliRunner(mix_stderr=False).invoke(
            cli, ["diff", "--run", str(run_dir), "--against", str(run_dir)]
        )
        assert "OK: no differences" in result_diff.output, (
            f"diff self-check failed:\n{result_diff.output}"
        )
        assert result_diff.exit_code == 0
