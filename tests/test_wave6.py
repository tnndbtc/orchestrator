"""Wave 6 tests: EpisodeBundle packaging (package + validate-bundle CLI commands)."""

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from orchestrator.cli import cli
from orchestrator.packager import package_episode
from orchestrator.utils.hashing import hash_artifact, hash_file_bytes


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _make_run_dir(tmp_path: Path) -> Path:
    """Create a minimal (but self-consistent) run directory for packaging tests."""
    run_dir = tmp_path / "run"
    run_dir.mkdir()

    # Dummy media files
    video_path = run_dir / "video.mp4"
    captions_path = run_dir / "captions.srt"
    video_path.write_bytes(b"\x00\x01\x02\x03video")
    captions_path.write_text("1\n00:00:00,000 --> 00:00:01,000\nHello\n", encoding="utf-8")

    # Required JSON artifacts
    _write_json(run_dir / "Script.json", {
        "schema_id": "Script", "schema_version": "1.0.0", "title": "Test",
    })
    _write_json(run_dir / "ShotList.json", {
        "schema_id": "ShotList", "schema_version": "1.0.0",
        "shotlist_id": "sl-001", "timing_lock_hash": "abc",
        "created_at": "2026-01-01T00:00:00Z", "total_duration_sec": 60.0,
    })
    _write_json(run_dir / "CanonDecision.json", {
        "schema_id": "CanonDecision", "schema_version": "1.0.0", "decision": "allow",
    })
    _write_json(run_dir / "AssetManifest.json", {
        "schema_id": "AssetManifest", "schema_version": "1.0.0",
        "manifest_id": "am-001", "shotlist_ref": "sl-001",
    })
    _write_json(run_dir / "AssetManifestResolved.json", {
        "schema_id": "AssetManifestResolved", "schema_version": "1.0.0",
        "manifest_id": "am-resolved-001",
    })
    _write_json(run_dir / "RenderPlan.json", {
        "schema_id": "RenderPlan", "schema_version": "1.0.0",
        "plan_id": "rp-001", "manifest_ref": "am-001",
    })
    _write_json(run_dir / "RenderOutput.json", {
        "schema_id": "RenderOutput", "schema_version": "1.0.0",
        "output_id": "ro-001",
        "video_uri": f"file://{video_path}",
        "captions_uri": f"file://{captions_path}",
        "hashes": {"video_sha256": "abc", "captions_sha256": "def"},
    })

    run_id = "run-testabc123"
    _write_json(run_dir / "RunIndex.json", {
        "schema_id": "RunIndex", "schema_version": "1.0.0",
        "run_id": run_id, "stages": [],
    })

    return run_dir


def _write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# TestPackageCommand
# ---------------------------------------------------------------------------

class TestPackageCommand:
    def test_happy_path_deterministic(self, tmp_path, monkeypatch):
        """Package twice with fixed timestamp → identical EpisodeBundle.json bytes."""
        monkeypatch.setenv("PACKAGER_NOW_UTC", "2026-02-20T00:00:00Z")
        run_dir = _make_run_dir(tmp_path)
        runner = CliRunner()

        out1 = tmp_path / "out1"
        out2 = tmp_path / "out2"

        result1 = runner.invoke(cli, [
            "package",
            "--run", str(run_dir),
            "--episode-id", "ep001",
            "--out", str(out1),
        ])
        assert result1.exit_code == 0, result1.output
        assert "OK: packaged episode ep001" in result1.output

        result2 = runner.invoke(cli, [
            "package",
            "--run", str(run_dir),
            "--episode-id", "ep001",
            "--out", str(out2),
        ])
        assert result2.exit_code == 0, result2.output

        bundle1 = (out1 / "ep001" / "EpisodeBundle.json").read_bytes()
        bundle2 = (out2 / "ep001" / "EpisodeBundle.json").read_bytes()
        assert bundle1 == bundle2, "EpisodeBundle.json bytes differ between two identical runs"

    def test_bundle_hash_valid(self, tmp_path, monkeypatch):
        """bundle_hash in EpisodeBundle.json matches hash_artifact(bundle_without_hash)."""
        monkeypatch.setenv("PACKAGER_NOW_UTC", "2026-02-20T00:00:00Z")
        run_dir = _make_run_dir(tmp_path)
        out_dir = tmp_path / "out"

        runner = CliRunner()
        result = runner.invoke(cli, [
            "package",
            "--run", str(run_dir),
            "--episode-id", "ep001",
            "--out", str(out_dir),
        ])
        assert result.exit_code == 0, result.output

        bundle_data = json.loads(
            (out_dir / "ep001" / "EpisodeBundle.json").read_text(encoding="utf-8")
        )
        without_hash = {k: v for k, v in bundle_data.items()
                        if k not in ("bundle_hash", "created_utc")}
        expected = hash_artifact(without_hash)
        assert bundle_data["bundle_hash"] == expected

    def test_layout_paths_correct(self, tmp_path, monkeypatch):
        """All expected paths exist under the bundle root after packaging."""
        monkeypatch.setenv("PACKAGER_NOW_UTC", "2026-02-20T00:00:00Z")
        run_dir = _make_run_dir(tmp_path)
        out_dir = tmp_path / "out"

        runner = CliRunner()
        result = runner.invoke(cli, [
            "package",
            "--run", str(run_dir),
            "--episode-id", "ep001",
            "--out", str(out_dir),
        ])
        assert result.exit_code == 0, result.output

        bundle_root = out_dir / "ep001"
        expected_paths = [
            "EpisodeBundle.json",
            "artifacts/Script.json",
            "artifacts/ShotList.json",
            "artifacts/CanonDecision.json",
            "artifacts/AssetManifest.json",
            "artifacts/AssetManifestResolved.json",
            "artifacts/RenderPlan.json",
            "artifacts/RenderOutput.json",
            "artifacts/RunIndex.json",
            "media/video.mp4",
            "media/captions.srt",
        ]
        for rel in expected_paths:
            assert (bundle_root / rel).exists(), f"Missing expected path: {rel}"

    def test_sha256_entries_match_files(self, tmp_path, monkeypatch):
        """sha256 in each artifacts entry matches hash_file_bytes() of the actual file."""
        monkeypatch.setenv("PACKAGER_NOW_UTC", "2026-02-20T00:00:00Z")
        run_dir = _make_run_dir(tmp_path)
        out_dir = tmp_path / "out"

        runner = CliRunner()
        result = runner.invoke(cli, [
            "package",
            "--run", str(run_dir),
            "--episode-id", "ep001",
            "--out", str(out_dir),
        ])
        assert result.exit_code == 0, result.output

        bundle_root = out_dir / "ep001"
        bundle_data = json.loads(
            (bundle_root / "EpisodeBundle.json").read_text(encoding="utf-8")
        )
        for name, entry in bundle_data["artifacts"].items():
            fp = bundle_root / entry["path"]
            assert fp.exists(), f"Artifact file missing: {entry['path']}"
            actual = hash_file_bytes(fp)
            assert actual == entry["sha256"], (
                f"SHA-256 mismatch for {name}: expected {entry['sha256'][:12]}... "
                f"got {actual[:12]}..."
            )

    def test_missing_artifact_fails(self, tmp_path):
        """Missing CanonDecision.json → exit nonzero + error message."""
        run_dir = _make_run_dir(tmp_path)
        (run_dir / "CanonDecision.json").unlink()
        out_dir = tmp_path / "out"

        runner = CliRunner()
        result = runner.invoke(cli, [
            "package",
            "--run", str(run_dir),
            "--episode-id", "ep001",
            "--out", str(out_dir),
        ])
        assert result.exit_code != 0
        assert "ERROR: missing required artifact: CanonDecision" in result.output

    def test_optional_fingerprint_included(self, tmp_path, monkeypatch):
        """When render_fingerprint.json exists, RenderFingerprint key appears in artifacts."""
        monkeypatch.setenv("PACKAGER_NOW_UTC", "2026-02-20T00:00:00Z")
        run_dir = _make_run_dir(tmp_path)
        _write_json(run_dir / "render_fingerprint.json", {
            "schema_id": "RenderFingerprint", "schema_version": "1.0.0",
            "fingerprint": "abc123",
        })
        out_dir = tmp_path / "out"

        runner = CliRunner()
        result = runner.invoke(cli, [
            "package",
            "--run", str(run_dir),
            "--episode-id", "ep001",
            "--out", str(out_dir),
        ])
        assert result.exit_code == 0, result.output

        bundle_data = json.loads(
            (out_dir / "ep001" / "EpisodeBundle.json").read_text(encoding="utf-8")
        )
        assert "RenderFingerprint" in bundle_data["artifacts"], (
            "Expected RenderFingerprint key when render_fingerprint.json is present"
        )

    def test_optional_fingerprint_absent(self, tmp_path, monkeypatch):
        """When render_fingerprint.json is absent, RenderFingerprint key not in artifacts."""
        monkeypatch.setenv("PACKAGER_NOW_UTC", "2026-02-20T00:00:00Z")
        run_dir = _make_run_dir(tmp_path)
        # Ensure it is NOT present (default fixture doesn't include it)
        fingerprint = run_dir / "render_fingerprint.json"
        if fingerprint.exists():
            fingerprint.unlink()
        out_dir = tmp_path / "out"

        runner = CliRunner()
        result = runner.invoke(cli, [
            "package",
            "--run", str(run_dir),
            "--episode-id", "ep001",
            "--out", str(out_dir),
        ])
        assert result.exit_code == 0, result.output

        bundle_data = json.loads(
            (out_dir / "ep001" / "EpisodeBundle.json").read_text(encoding="utf-8")
        )
        assert "RenderFingerprint" not in bundle_data["artifacts"], (
            "RenderFingerprint key should be absent when render_fingerprint.json is missing"
        )

    def test_unsupported_uri_scheme_fails(self, tmp_path):
        """A non-file:// URI scheme in RenderOutput.json → exit nonzero + scheme error."""
        run_dir = _make_run_dir(tmp_path)
        # Overwrite RenderOutput.json with an https:// video_uri
        _write_json(run_dir / "RenderOutput.json", {
            "schema_id": "RenderOutput", "schema_version": "1.0.0",
            "output_id": "ro-001",
            "video_uri": "https://example.com/video.mp4",
            "captions_uri": f"file://{run_dir / 'captions.srt'}",
            "hashes": {"video_sha256": "abc", "captions_sha256": "def"},
        })
        out_dir = tmp_path / "out"

        runner = CliRunner()
        result = runner.invoke(cli, [
            "package",
            "--run", str(run_dir),
            "--episode-id", "ep001",
            "--out", str(out_dir),
        ])
        assert result.exit_code != 0
        assert "Unsupported URI scheme" in result.output


# ---------------------------------------------------------------------------
# TestValidateBundleCommand
# ---------------------------------------------------------------------------

class TestValidateBundleCommand:
    def _build_bundle(self, tmp_path: Path, monkeypatch) -> Path:
        """Helper: build a valid bundle and return bundle_root."""
        monkeypatch.setenv("PACKAGER_NOW_UTC", "2026-02-20T00:00:00Z")
        run_dir = _make_run_dir(tmp_path)
        out_dir = tmp_path / "out"
        bundle_root = package_episode(run_dir, "ep001", out_dir)
        return bundle_root

    def test_valid_bundle(self, tmp_path, monkeypatch):
        """A freshly-packaged bundle → 'OK: bundle valid', exit 0."""
        bundle_root = self._build_bundle(tmp_path, monkeypatch)

        runner = CliRunner()
        result = runner.invoke(cli, [
            "validate-bundle",
            "--bundle", str(bundle_root),
        ])
        assert result.exit_code == 0, result.output
        assert "OK: bundle valid" in result.output

    def test_corrupt_file_fails(self, tmp_path, monkeypatch):
        """Corrupting artifacts/Script.json bytes → hash mismatch error, exit nonzero."""
        bundle_root = self._build_bundle(tmp_path, monkeypatch)
        script_path = bundle_root / "artifacts" / "Script.json"
        # Corrupt the file by appending a byte
        script_path.write_bytes(script_path.read_bytes() + b"\x00")

        runner = CliRunner()
        result = runner.invoke(cli, [
            "validate-bundle",
            "--bundle", str(bundle_root),
        ])
        assert result.exit_code != 0
        assert "ERROR: hash mismatch for artifacts/Script.json" in result.output

    def test_corrupt_bundle_hash_fails(self, tmp_path, monkeypatch):
        """Tampering with bundle_hash in EpisodeBundle.json → bundle_hash mismatch error."""
        bundle_root = self._build_bundle(tmp_path, monkeypatch)
        bundle_json_path = bundle_root / "EpisodeBundle.json"

        bundle_data = json.loads(bundle_json_path.read_text(encoding="utf-8"))
        bundle_data["bundle_hash"] = "0" * 64  # corrupt the hash
        bundle_json_path.write_text(json.dumps(bundle_data, indent=2), encoding="utf-8")

        runner = CliRunner()
        result = runner.invoke(cli, [
            "validate-bundle",
            "--bundle", str(bundle_root),
        ])
        assert result.exit_code != 0
        assert "ERROR: bundle_hash mismatch" in result.output

    def test_bundle_hash_excludes_created_utc(self, tmp_path, monkeypatch):
        """Changing created_utc in EpisodeBundle.json does not invalidate bundle_hash."""
        bundle_root = self._build_bundle(tmp_path, monkeypatch)
        bundle_json_path = bundle_root / "EpisodeBundle.json"

        # Overwrite created_utc with a different timestamp
        bundle_data = json.loads(bundle_json_path.read_text(encoding="utf-8"))
        bundle_data["created_utc"] = "1999-01-01T00:00:00Z"
        bundle_json_path.write_text(json.dumps(bundle_data, indent=2), encoding="utf-8")

        runner = CliRunner()
        result = runner.invoke(cli, [
            "validate-bundle",
            "--bundle", str(bundle_root),
        ])
        assert result.exit_code == 0, result.output
        assert "OK: bundle valid" in result.output
