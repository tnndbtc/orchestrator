"""Wave 8b tests: derived-hash normalization for investigate-determinism."""

import hashlib
import json
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from orchestrator.cli import _compare_contract_artifacts, cli


# ---------------------------------------------------------------------------
# Shared project fixture
# ---------------------------------------------------------------------------

_PROJECT = {
    "id": "proj-det-hash",
    "title": "Determinism Hash Test",
    "genre": "sci-fi",
}

# Stable semantic content used across tests
_CHAR_PACKS = [{"pack_id": "char-alice", "character_id": "alice", "display_name": "Alice"}]
_TIMING_LOCK = "tlh-stable-abc123"


# ---------------------------------------------------------------------------
# Helper: build a complete run directory for direct _compare_contract_artifacts tests
# ---------------------------------------------------------------------------

def _canonical_bytes(obj: dict) -> bytes:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def _raw_render_hashes(manifest: dict, plan: dict) -> dict:
    """Compute raw hashes as the real renderer would (from non-normalized data)."""
    mb = _canonical_bytes(manifest)
    pb = _canonical_bytes(plan)
    return {
        "asset_manifest_hash": hashlib.sha256(mb).hexdigest(),
        "render_plan_hash": hashlib.sha256(pb).hexdigest(),
        "inputs_digest": hashlib.sha256(mb + b"\n" + pb).hexdigest(),
    }


def _write_run_dir(
    run_dir: Path,
    suffix: str,
    fps: int = 24,
) -> None:
    """Populate a run directory with all five contract artifacts.

    *suffix* is embedded in run-identity ID fields; *fps* lets tests vary a
    semantic field to produce a genuine mismatch.
    """
    run_dir.mkdir(parents=True, exist_ok=True)

    # CanonDecision — identical across runs
    (run_dir / "CanonDecision.json").write_text(
        json.dumps({
            "schema_id": "CanonDecision",
            "schema_version": "1.0.0",
            "decision": "allow",
            "decision_id": "test-canon-001",
        }, indent=2),
        encoding="utf-8",
    )

    # ShotList — run-identity in shotlist_id, stable semantic content
    (run_dir / "ShotList.json").write_text(
        json.dumps({
            "schema_id": "ShotList",
            "schema_version": "1.0.0",
            "shotlist_id": f"shotlist-proj-{suffix}",
            "timing_lock_hash": _TIMING_LOCK,
            "total_duration_sec": 60.0,
        }, indent=2),
        encoding="utf-8",
    )

    # AssetManifest — run-identity in manifest_id / shotlist_ref
    manifest = {
        "schema_id": "AssetManifest",
        "schema_version": "1.0.0",
        "manifest_id": f"manifest-proj-{suffix}",
        "shotlist_ref": f"shotlist-proj-{suffix}",
        "character_packs": _CHAR_PACKS,
    }
    (run_dir / "AssetManifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )

    # RenderPlan — run-identity in plan_id / manifest_ref; fps is semantic
    plan = {
        "schema_id": "RenderPlan",
        "schema_version": "1.0.0",
        "plan_id": f"plan-proj-{suffix}",
        "manifest_ref": f"manifest-proj-{suffix}",
        "timing_lock_hash": _TIMING_LOCK,
        "fps": fps,
    }
    (run_dir / "RenderPlan.json").write_text(
        json.dumps(plan, indent=2), encoding="utf-8"
    )

    # RenderOutput — raw derived hashes computed from the *non-normalized* inputs
    # (this simulates the real renderer, which embeds run-specific IDs in the hash)
    raw = _raw_render_hashes(manifest, plan)
    (run_dir / "RenderOutput.json").write_text(
        json.dumps({
            "schema_id": "RenderOutput",
            "schema_version": "1.0.0",
            "output_id": f"ro-{suffix}",
            "hashes": {"video_sha256": "aabbccdd"},
            "inputs_digest": raw["inputs_digest"],
            "lineage": {
                "asset_manifest_hash": raw["asset_manifest_hash"],
                "render_plan_hash": raw["render_plan_hash"],
            },
        }, indent=2),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Stage stubs for CLI-based tests
# ---------------------------------------------------------------------------

def _write_artifact_json(registry, project_id, run_id, artifact_type, data):
    run_dir = registry.run_dir(project_id, run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / f"{artifact_type}.json").write_text(
        json.dumps(data, indent=2), encoding="utf-8"
    )


def _stage1_stub(project_config, run_id, registry):
    pid = project_config["id"]
    data = {"schema_id": "Script", "schema_version": "1.0.0",
            "script_id": "script-stub", "title": "Test"}
    _write_artifact_json(registry, pid, run_id, "Script", data)
    return data


def _stage2_stub(project_config, run_id, registry):
    pid = project_config["id"]
    data = {"schema_id": "ShotList", "schema_version": "1.0.0",
            "shotlist_id": f"shotlist-{pid}-{run_id[:8]}",
            "timing_lock_hash": _TIMING_LOCK, "total_duration_sec": 60.0}
    _write_artifact_json(registry, pid, run_id, "ShotList", data)
    return data


def _stage3_stub(project_config, run_id, registry):
    pid = project_config["id"]
    manifest = {"schema_id": "AssetManifest", "schema_version": "1.0.0",
                "manifest_id": f"manifest-{pid}-{run_id[:8]}",
                "shotlist_ref": f"shotlist-{pid}-{run_id[:8]}",
                "character_packs": _CHAR_PACKS}
    _write_artifact_json(registry, pid, run_id, "AssetManifest", manifest)
    return manifest


def _stage4_stub(project_config, run_id, registry):
    pid = project_config["id"]
    plan = {"schema_id": "RenderPlan", "schema_version": "1.0.0",
            "plan_id": f"plan-{pid}-{run_id[:8]}",
            "manifest_ref": f"manifest-{pid}-{run_id[:8]}",
            "timing_lock_hash": _TIMING_LOCK, "fps": 24}
    _write_artifact_json(registry, pid, run_id, "RenderPlan", plan)
    return plan


def _stage5_stub(project_config, run_id, registry):
    pid = project_config["id"]
    run_dir = registry.run_dir(pid, run_id)
    # Load what stage3/4 wrote so we can compute raw hashes as the real renderer would
    try:
        manifest = json.loads((run_dir / "AssetManifest.json").read_text(encoding="utf-8"))
        plan = json.loads((run_dir / "RenderPlan.json").read_text(encoding="utf-8"))
        raw = _raw_render_hashes(manifest, plan)
    except (OSError, json.JSONDecodeError):
        raw = {"asset_manifest_hash": "?", "render_plan_hash": "?", "inputs_digest": "?"}
    data = {"schema_id": "RenderOutput", "schema_version": "1.0.0",
            "output_id": f"ro-{run_id[:8]}",
            "hashes": {"video_sha256": "aabbcc"},
            "inputs_digest": raw["inputs_digest"],
            "lineage": {
                "asset_manifest_hash": raw["asset_manifest_hash"],
                "render_plan_hash": raw["render_plan_hash"],
            }}
    _write_artifact_json(registry, pid, run_id, "RenderOutput", data)
    return data


_STAGE_PATCH_TARGETS = [
    ("orchestrator.stages.stage1_generate_script.run", _stage1_stub),
    ("orchestrator.stages.stage2_script_to_shotlist.run", _stage2_stub),
    ("orchestrator.stages.stage3_shotlist_to_assetmanifest.run", _stage3_stub),
    ("orchestrator.stages.stage4_build_renderplan.run", _stage4_stub),
    ("orchestrator.stages.stage5_render_preview.run", _stage5_stub),
]


def _patched_stages(overrides=None):
    stubs = dict(_STAGE_PATCH_TARGETS)
    if overrides:
        stubs.update(overrides)

    class _Ctx:
        def __enter__(self):
            self._stack = ExitStack()
            for target, fn in stubs.items():
                self._stack.enter_context(patch(target, new=fn))
            return self

        def __exit__(self, *args):
            return self._stack.__exit__(*args)

    return _Ctx()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDerivedHashNormalization:

    def test_run_identity_hashes_do_not_cause_diff(self, tmp_path):
        """Raw derived hashes differ (because source IDs differ) but normalized
        hashes are equal → _compare_contract_artifacts returns no diffs."""
        dir_a = tmp_path / "run_a"
        dir_b = tmp_path / "run_b"
        _write_run_dir(dir_a, suffix="invdet-a-aabbccdd")
        _write_run_dir(dir_b, suffix="invdet-b-11223344")

        # Sanity check: raw hashes in RenderOutput MUST actually differ so
        # the test is meaningful (the verifier really needs to normalize them).
        ro_a = json.loads((dir_a / "RenderOutput.json").read_text())
        ro_b = json.loads((dir_b / "RenderOutput.json").read_text())
        assert ro_a["inputs_digest"] != ro_b["inputs_digest"], (
            "Test setup error: raw hashes should differ between runs A and B"
        )

        diffs = _compare_contract_artifacts(dir_a, dir_b)
        assert diffs == [], (
            f"Expected no diffs after normalization, got: {diffs}"
        )

    def test_semantic_render_plan_change_causes_fail(self, tmp_path):
        """Changing a real semantic field (fps) in RenderPlan causes normalized
        hashes to differ → diffs are emitted including a normalized_input_mismatch."""
        dir_a = tmp_path / "run_a"
        dir_b = tmp_path / "run_b"
        _write_run_dir(dir_a, suffix="invdet-a-aabbccdd", fps=24)
        _write_run_dir(dir_b, suffix="invdet-b-11223344", fps=30)  # different fps!

        diffs = _compare_contract_artifacts(dir_a, dir_b)

        assert len(diffs) > 0, "Expected diffs when a semantic field differs"
        # Must include at least one hash-related mismatch in RenderOutput
        hash_paths = {"[inputs_digest]", "[lineage][render_plan_hash]"}
        diff_paths = {d["path"] for d in diffs if d["artifact"] == "RenderOutput.json"}
        assert diff_paths & hash_paths, (
            f"Expected hash-related diff in RenderOutput.json, diff paths were: {diff_paths}"
        )
        # Must also include a normalized_input_mismatch diagnostic entry
        assert any(d["type"] == "normalized_input_mismatch" for d in diffs), (
            f"Expected normalized_input_mismatch diagnostic entry, got: {diffs}"
        )

    def test_determinism_report_is_deterministic_with_hash_stubs(self, tmp_path):
        """Two invocations of investigate-determinism using stubs that embed
        run_id in IDs and lineage hashes → both DeterminismReport.json files
        must be byte-identical."""
        project_file = tmp_path / "project.json"
        project_file.write_text(json.dumps(_PROJECT), encoding="utf-8")
        out1 = tmp_path / "out1"
        out2 = tmp_path / "out2"

        runner = CliRunner()
        with _patched_stages():
            r1 = runner.invoke(cli, [
                "investigate-determinism",
                "--project", str(project_file),
                "--out", str(out1),
            ])
            r2 = runner.invoke(cli, [
                "investigate-determinism",
                "--project", str(project_file),
                "--out", str(out2),
            ])

        assert r1.exit_code == 0, r1.output
        assert r2.exit_code == 0, r2.output

        bytes1 = (out1 / "DeterminismReport.json").read_bytes()
        bytes2 = (out2 / "DeterminismReport.json").read_bytes()
        assert bytes1 == bytes2, (
            "DeterminismReport.json bytes differ between two invocations"
        )
