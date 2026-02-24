"""Tests for stage5_render_preview — video CLI interface, file-based output, placeholder fallback."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from orchestrator.registry import ArtifactRegistry
from orchestrator.stages import stage5_render_preview


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_call_agent_result(returncode: int, stderr: str = "") -> MagicMock:
    """Return a MagicMock that looks like a CompletedProcess from call_agent."""
    m = MagicMock()
    m.returncode = returncode
    m.stderr     = stderr
    return m


def _minimal_ro(
    video_uri:    str = "https://cdn.example.com/v.mp4",
    captions_uri: str = "https://cdn.example.com/c.srt",
) -> dict:
    """Return a schema-valid RenderOutput with configurable URIs."""
    return {
        "schema_id":    "RenderOutput",
        "schema_version": "1.0.0",
        "output_id":    "test-output-001",
        "video_uri":    video_uri,
        "captions_uri": captions_uri,
        "hashes": {
            "video_sha256":    "a" * 64,
            "captions_sha256": "b" * 64,
        },
    }


def _write_minimal_artifacts(registry: ArtifactRegistry, pid: str, run_id: str) -> None:
    """Write AssetManifest_final and RenderPlan so registry paths exist.

    RenderPlan must contain at least one non-placeholder resolved_asset so that
    stage5 proceeds to call the renderer (rather than taking the placeholder shortcut).
    """
    run_dir = registry.run_dir(pid, run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    rp = {"resolved_assets": [{"asset_id": "test-asset", "asset_type": "vo",
                                "uri": "file:///tmp/test.mp4", "is_placeholder": False,
                                "license_type": "cc0"}]}
    registry.artifact_path(pid, run_id, "RenderPlan").write_text(
        json.dumps(rp), encoding="utf-8"
    )
    registry.artifact_path(pid, run_id, "AssetManifest_final").write_text(
        "{}", encoding="utf-8"
    )


# Non-None sentinel: returned by find_agent_bin when the binary is "installed".
_VIDEO_BIN = MagicMock()


def _make_write_ro_side_effect(ro: dict, ok_result: MagicMock):
    """Return a call_agent side-effect that writes ro to the --out path on disk."""
    def _side_effect(name, args, **kwargs):
        out_idx = args.index("--out") + 1
        out_path = Path(args[out_idx])
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(ro), encoding="utf-8")
        return ok_result
    return _side_effect


# ---------------------------------------------------------------------------
# Placeholder fallback when RenderPlan has all-placeholder resolved assets
# ---------------------------------------------------------------------------

class TestAllPlaceholderRenderPlan:
    """Stage5 must short-circuit to a placeholder stub when every resolved asset
    in RenderPlan is flagged is_placeholder=True (step 1 of the §41.4 flow).
    The video binary must NOT be queried in this path.
    """

    def _write_all_placeholder_plan(self, registry: ArtifactRegistry, pid: str, run_id: str) -> None:
        """Write a RenderPlan where every resolved asset is a placeholder."""
        run_dir = registry.run_dir(pid, run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        rp = {
            "resolved_assets": [
                {
                    "asset_id": "char-placeholder",
                    "asset_type": "character",
                    "uri": "placeholder://character/char-placeholder",
                    "is_placeholder": True,
                    "license_type": "cc0",
                },
                {
                    "asset_id": "bg-placeholder",
                    "asset_type": "background",
                    "uri": "placeholder://background/bg-placeholder",
                    "is_placeholder": True,
                    "license_type": "cc0",
                },
            ]
        }
        registry.artifact_path(pid, run_id, "RenderPlan").write_text(
            json.dumps(rp), encoding="utf-8"
        )
        registry.artifact_path(pid, run_id, "AssetManifest_final").write_text(
            "{}", encoding="utf-8"
        )

    def _write_empty_plan(self, registry: ArtifactRegistry, pid: str, run_id: str) -> None:
        """Write a RenderPlan with an empty resolved_assets list."""
        run_dir = registry.run_dir(pid, run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        registry.artifact_path(pid, run_id, "RenderPlan").write_text(
            json.dumps({"resolved_assets": []}), encoding="utf-8"
        )
        registry.artifact_path(pid, run_id, "AssetManifest_final").write_text(
            "{}", encoding="utf-8"
        )

    def test_all_placeholder_returns_placeholder_stub(self, tmp_path):
        """All resolved assets placeholder → placeholder RenderOutput returned."""
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        self._write_all_placeholder_plan(registry, pid, run_id)

        result = stage5_render_preview.run({"id": pid}, run_id, registry)

        assert result["placeholder_render"] is True

    def test_all_placeholder_stub_has_schema(self, tmp_path):
        """Placeholder stub must carry correct schema_id and schema_version."""
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        self._write_all_placeholder_plan(registry, pid, run_id)

        result = stage5_render_preview.run({"id": pid}, run_id, registry)

        assert result["schema_id"] == "RenderOutput"
        assert result["schema_version"] == "1.0.0"

    def test_all_placeholder_written_to_registry(self, tmp_path):
        """Placeholder stub must be persisted to the registry as RenderOutput."""
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        self._write_all_placeholder_plan(registry, pid, run_id)

        stage5_render_preview.run({"id": pid}, run_id, registry)

        assert registry.exists_and_valid(pid, run_id, "RenderOutput")

    def test_all_placeholder_does_not_call_find_agent_bin(self, tmp_path):
        """Video binary must never be queried when the plan is all-placeholder."""
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        self._write_all_placeholder_plan(registry, pid, run_id)

        with patch(
            "orchestrator.stages.stage5_render_preview.find_agent_bin"
        ) as mock_find:
            stage5_render_preview.run({"id": pid}, run_id, registry)

        mock_find.assert_not_called()

    def test_all_placeholder_placeholder_reason_mentions_placeholder(self, tmp_path):
        """placeholder_reason should explain why the renderer was skipped."""
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        self._write_all_placeholder_plan(registry, pid, run_id)

        result = stage5_render_preview.run({"id": pid}, run_id, registry)

        assert "placeholder" in result["placeholder_reason"].lower()

    def test_empty_resolved_assets_also_returns_placeholder(self, tmp_path):
        """An empty resolved_assets list is also a skip condition."""
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        self._write_empty_plan(registry, pid, run_id)

        result = stage5_render_preview.run({"id": pid}, run_id, registry)

        assert result["placeholder_render"] is True
        assert registry.exists_and_valid(pid, run_id, "RenderOutput")

    def test_mixed_plan_does_not_take_placeholder_shortcut(self, tmp_path):
        """When at least one asset is NOT a placeholder, the renderer must be called."""
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        run_dir = registry.run_dir(pid, run_id)
        run_dir.mkdir(parents=True, exist_ok=True)

        # One real, one placeholder — should NOT skip to the early-exit path
        rp = {
            "resolved_assets": [
                {"asset_id": "real-asset",   "asset_type": "vo",
                 "uri": "file:///tmp/real.mp4", "is_placeholder": False, "license_type": "cc0"},
                {"asset_id": "placeholder-a", "asset_type": "character",
                 "uri": "placeholder://character/a", "is_placeholder": True, "license_type": "cc0"},
            ]
        }
        registry.artifact_path(pid, run_id, "RenderPlan").write_text(
            json.dumps(rp), encoding="utf-8"
        )
        registry.artifact_path(pid, run_id, "AssetManifest_final").write_text(
            "{}", encoding="utf-8"
        )

        with patch(
            "orchestrator.stages.stage5_render_preview.find_agent_bin",
            return_value=None,
        ) as mock_find:
            result = stage5_render_preview.run({"id": pid}, run_id, registry)

        # find_agent_bin must have been called (video binary check, not the placeholder shortcut)
        mock_find.assert_called_once()
        # And we get the "binary not installed" placeholder, not the all-placeholder one
        assert result["placeholder_render"] is True
        assert "video" in result["placeholder_reason"].lower()


# ---------------------------------------------------------------------------
# Placeholder fallback when video binary is not installed
# ---------------------------------------------------------------------------

class TestVideoNotInstalled:
    def test_placeholder_stub_returned(self, tmp_path):
        """When `video` binary is absent, stage5 returns a placeholder RenderOutput."""
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        _write_minimal_artifacts(registry, pid, run_id)

        with patch("orchestrator.stages.stage5_render_preview.find_agent_bin", return_value=None):
            result = stage5_render_preview.run({"id": pid}, run_id, registry)

        assert result["placeholder_render"] is True
        assert "video" in result["placeholder_reason"].lower()
        assert registry.exists_and_valid(pid, run_id, "RenderOutput")

    def test_placeholder_stub_has_schema(self, tmp_path):
        """Placeholder RenderOutput must carry schema_id and schema_version."""
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        _write_minimal_artifacts(registry, pid, run_id)

        with patch("orchestrator.stages.stage5_render_preview.find_agent_bin", return_value=None):
            result = stage5_render_preview.run({"id": pid}, run_id, registry)

        assert result["schema_id"]      == "RenderOutput"
        assert result["schema_version"] == "1.0.0"


# ---------------------------------------------------------------------------
# RuntimeError on non-zero subprocess exit
# ---------------------------------------------------------------------------

class TestRendererNonZeroExit:
    def test_nonzero_exit_raises_runtime_error(self, tmp_path):
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        _write_minimal_artifacts(registry, pid, run_id)
        bad = _make_call_agent_result(1, "fatal: something went wrong")

        with patch("orchestrator.stages.stage5_render_preview.find_agent_bin", return_value=_VIDEO_BIN):
            with patch("orchestrator.stages.stage5_render_preview.call_agent", return_value=bad):
                with pytest.raises(RuntimeError, match="code 1"):
                    stage5_render_preview.run({"id": pid}, run_id, registry)

    def test_runtime_error_includes_stderr(self, tmp_path):
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        _write_minimal_artifacts(registry, pid, run_id)
        bad = _make_call_agent_result(2, "my detailed error message")

        with patch("orchestrator.stages.stage5_render_preview.find_agent_bin", return_value=_VIDEO_BIN):
            with patch("orchestrator.stages.stage5_render_preview.call_agent", return_value=bad):
                with pytest.raises(RuntimeError, match="my detailed error message"):
                    stage5_render_preview.run({"id": pid}, run_id, registry)


# ---------------------------------------------------------------------------
# ValueError when video CLI exits 0 but RenderOutput.json is not written
# ---------------------------------------------------------------------------

class TestRenderOutputNotWritten:
    def test_missing_render_output_raises_value_error(self, tmp_path):
        """`video render` exits 0 but does not write RenderOutput.json → ValueError."""
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        _write_minimal_artifacts(registry, pid, run_id)
        ok = _make_call_agent_result(0)

        with patch("orchestrator.stages.stage5_render_preview.find_agent_bin", return_value=_VIDEO_BIN):
            with patch("orchestrator.stages.stage5_render_preview.call_agent", return_value=ok):
                with pytest.raises(ValueError, match="RenderOutput.json"):
                    stage5_render_preview.run({"id": pid}, run_id, registry)


# ---------------------------------------------------------------------------
# FileNotFoundError for missing file:// URIs
# ---------------------------------------------------------------------------

class TestFileUriMissing:
    def _run_with_ro(self, tmp_path, ro: dict) -> None:
        """Helper: mock call_agent to write ro to the expected --out path."""
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        _write_minimal_artifacts(registry, pid, run_id)
        ok = _make_call_agent_result(0)

        with patch("orchestrator.stages.stage5_render_preview.find_agent_bin", return_value=_VIDEO_BIN):
            with patch(
                "orchestrator.stages.stage5_render_preview.call_agent",
                side_effect=_make_write_ro_side_effect(ro, ok),
            ):
                stage5_render_preview.run({"id": pid}, run_id, registry)

    def test_video_uri_missing_raises_file_not_found(self, tmp_path):
        ro = _minimal_ro(
            video_uri=f"file://{tmp_path}/nonexistent_video.mp4",
            captions_uri=f"file://{tmp_path}/nonexistent_captions.srt",
        )
        with pytest.raises(FileNotFoundError, match="video_uri"):
            self._run_with_ro(tmp_path, ro)

    def test_captions_uri_missing_raises_file_not_found(self, tmp_path):
        video_file = tmp_path / "video.mp4"
        video_file.write_bytes(b"fake video")
        ro = _minimal_ro(
            video_uri=f"file://{video_file}",
            captions_uri=f"file://{tmp_path}/nonexistent_captions.srt",
        )
        with pytest.raises(FileNotFoundError, match="captions_uri"):
            self._run_with_ro(tmp_path, ro)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestHappyPath:
    def _run_with_ro(self, tmp_path, ro: dict):
        """Helper: mock call_agent to write ro as RenderOutput.json on disk."""
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        _write_minimal_artifacts(registry, pid, run_id)
        ok = _make_call_agent_result(0)

        with patch("orchestrator.stages.stage5_render_preview.find_agent_bin", return_value=_VIDEO_BIN):
            with patch(
                "orchestrator.stages.stage5_render_preview.call_agent",
                side_effect=_make_write_ro_side_effect(ro, ok),
            ):
                result = stage5_render_preview.run({"id": pid}, run_id, registry)

        return result, registry, pid, run_id

    def test_non_file_uri_skips_disk_check(self, tmp_path):
        """https:// URIs are returned without disk existence checks."""
        ro = _minimal_ro()  # uses https:// URIs by default
        result, registry, pid, run_id = self._run_with_ro(tmp_path, ro)
        assert result == ro
        assert registry.exists_and_valid(pid, run_id, "RenderOutput")

    def test_file_uri_happy_path(self, tmp_path):
        """file:// URIs that exist on disk pass; artifact is written to registry."""
        video_file    = tmp_path / "video.mp4"
        captions_file = tmp_path / "captions.srt"
        video_file.write_bytes(b"fake video content")
        captions_file.write_bytes(b"fake captions")

        ro = _minimal_ro(
            video_uri=f"file://{video_file}",
            captions_uri=f"file://{captions_file}",
        )
        result, registry, pid, run_id = self._run_with_ro(tmp_path, ro)
        assert result == ro
        assert registry.exists_and_valid(pid, run_id, "RenderOutput")

    def test_return_value_is_unchanged(self, tmp_path):
        """RenderOutput.json content must be returned without mutation."""
        ro = _minimal_ro()
        ro["provenance"] = {"rendered_at": "2025-01-15T12:00:00Z"}
        result, _, _, _ = self._run_with_ro(tmp_path, ro)
        assert result["provenance"]["rendered_at"] == "2025-01-15T12:00:00Z"

    def test_cli_called_with_correct_flags(self, tmp_path):
        """call_agent must be invoked with 'video' and all four §41.4 flags."""
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        _write_minimal_artifacts(registry, pid, run_id)
        ok = _make_call_agent_result(0)
        captured: dict = {}

        def capture_side_effect(name, args, **kwargs):
            captured["name"] = name
            captured["args"] = list(args)
            out_idx = args.index("--out") + 1
            Path(args[out_idx]).parent.mkdir(parents=True, exist_ok=True)
            Path(args[out_idx]).write_text(json.dumps(_minimal_ro()), encoding="utf-8")
            return ok

        with patch("orchestrator.stages.stage5_render_preview.find_agent_bin", return_value=_VIDEO_BIN):
            with patch("orchestrator.stages.stage5_render_preview.call_agent", side_effect=capture_side_effect):
                stage5_render_preview.run({"id": pid}, run_id, registry)

        assert captured["name"] == "video"
        assert captured["args"][0] == "render"
        assert "--manifest" in captured["args"]
        assert "--plan"     in captured["args"]
        assert "--out"      in captured["args"]
        assert "--video"    in captured["args"]
