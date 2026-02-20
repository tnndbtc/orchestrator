"""EpisodeBundle packager — assembles a finished run dir into a portable bundle."""

import json
import os
import shutil
import urllib.parse
from pathlib import Path

from .utils.hashing import hash_artifact, hash_file_bytes

# ---------------------------------------------------------------------------
# Artifact configuration
# ---------------------------------------------------------------------------

# Keys match the "artifacts" dict keys in EpisodeBundle.json
REQUIRED_JSON: list[str] = [
    "Script",
    "ShotList",
    "CanonDecision",
    "AssetManifest",
    "AssetManifestResolved",
    "RenderPlan",
    "RenderOutput",
    "RunIndex",
]

# Optional JSON artifacts (key → filename stem)
OPTIONAL_JSON: list[str] = ["render_fingerprint"]  # file: render_fingerprint.json

# Map artifact key → filename in run_dir
_JSON_FILENAMES: dict[str, str] = {
    "Script": "Script.json",
    "ShotList": "ShotList.json",
    "CanonDecision": "CanonDecision.json",
    "AssetManifest": "AssetManifest.json",
    "AssetManifestResolved": "AssetManifestResolved.json",
    "RenderPlan": "RenderPlan.json",
    "RenderOutput": "RenderOutput.json",
    "RunIndex": "RunIndex.json",
    "render_fingerprint": "render_fingerprint.json",
}

# Destination paths within the bundle (relative to bundle_root)
_JSON_DEST: dict[str, str] = {
    "Script": "artifacts/Script.json",
    "ShotList": "artifacts/ShotList.json",
    "CanonDecision": "artifacts/CanonDecision.json",
    "AssetManifest": "artifacts/AssetManifest.json",
    "AssetManifestResolved": "artifacts/AssetManifestResolved.json",
    "RenderPlan": "artifacts/RenderPlan.json",
    "RenderOutput": "artifacts/RenderOutput.json",
    "RunIndex": "artifacts/RunIndex.json",
    "RenderFingerprint": "artifacts/render_fingerprint.json",
}


def _transfer(src: Path, dst: Path, mode: str) -> None:
    """Copy or hardlink src → dst.

    hardlink: attempts os.link; falls back to shutil.copy2 on OSError
              (e.g. cross-device link).
    copy:     always uses shutil.copy2.

    WARNING: When mode="hardlink", source artifacts must remain immutable
             after packaging.  Mutating a hardlinked source file will
             silently corrupt the bundle's artifact hashes.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    if mode == "hardlink":
        try:
            os.link(src, dst)
        except OSError:
            shutil.copy2(src, dst)
    else:
        shutil.copy2(src, dst)


def package_episode(
    run_dir: Path,
    episode_id: str,
    out_dir: Path,
    mode: str = "copy",
) -> Path:
    """Assemble a finished run_dir into a portable EpisodeBundle.

    Args:
        run_dir:    Path to a completed pipeline run directory.
        episode_id: Stable episode identifier (used as bundle subdirectory name).
        out_dir:    Parent directory to create the bundle under.
        mode:       Transfer mode — "copy" (safe default) or "hardlink"
                    (faster; source artifacts must remain immutable after
                    packaging — mutating them will corrupt bundle hashes).

    Returns:
        Path to the bundle root (<out_dir>/<episode_id>/).

    Raises:
        ValueError: If a required artifact is missing from run_dir.
        FileNotFoundError: If resolved media paths do not exist.
    """
    run_dir = run_dir.resolve()

    # ------------------------------------------------------------------
    # 1. Validate required JSON artifacts
    # ------------------------------------------------------------------
    for key in REQUIRED_JSON:
        src = run_dir / _JSON_FILENAMES[key]
        if not src.exists():
            raise ValueError(f"ERROR: missing required artifact: {key}")

    # ------------------------------------------------------------------
    # 2. Parse RenderOutput.json for media URIs
    # ------------------------------------------------------------------
    render_output = json.loads(
        (run_dir / "RenderOutput.json").read_text(encoding="utf-8")
    )
    video_uri = render_output.get("video_uri", "")
    captions_uri = render_output.get("captions_uri", "")

    video_src = _resolve_uri(video_uri, run_dir)
    captions_src = _resolve_uri(captions_uri, run_dir)

    if not video_src.exists():
        raise FileNotFoundError(f"Video file not found: {video_src}")
    if not captions_src.exists():
        raise FileNotFoundError(f"Captions file not found: {captions_src}")

    # ------------------------------------------------------------------
    # 3. Create bundle directory structure
    # ------------------------------------------------------------------
    bundle_root = out_dir / episode_id
    artifacts_dir = bundle_root / "artifacts"
    media_dir = bundle_root / "media"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    media_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 4. Transfer files + compute per-file sha256 on destination
    # ------------------------------------------------------------------
    artifacts_entries: dict[str, dict] = {}

    # Required JSON artifacts
    for key in REQUIRED_JSON:
        src = run_dir / _JSON_FILENAMES[key]
        dest_rel = _JSON_DEST[key]
        dst = bundle_root / dest_rel
        _transfer(src, dst, mode)
        artifacts_entries[key] = {
            "path": dest_rel,
            "sha256": hash_file_bytes(dst),
        }

    # Optional: render_fingerprint
    fingerprint_src = run_dir / "render_fingerprint.json"
    if fingerprint_src.exists():
        dest_rel = _JSON_DEST["RenderFingerprint"]
        dst = bundle_root / dest_rel
        _transfer(fingerprint_src, dst, mode)
        artifacts_entries["RenderFingerprint"] = {
            "path": dest_rel,
            "sha256": hash_file_bytes(dst),
        }

    # Media files
    video_dst = media_dir / "video.mp4"
    captions_dst = media_dir / "captions.srt"
    _transfer(video_src, video_dst, mode)
    _transfer(captions_src, captions_dst, mode)
    artifacts_entries["VideoMP4"] = {
        "path": "media/video.mp4",
        "sha256": hash_file_bytes(video_dst),
    }
    artifacts_entries["CaptionsSRT"] = {
        "path": "media/captions.srt",
        "sha256": hash_file_bytes(captions_dst),
    }

    # Sort entries by key for determinism
    artifacts_sorted = dict(sorted(artifacts_entries.items()))

    # ------------------------------------------------------------------
    # 5. Read run_id from RunIndex.json
    # ------------------------------------------------------------------
    run_index = json.loads(
        (run_dir / "RunIndex.json").read_text(encoding="utf-8")
    )
    run_id = run_index["run_id"]

    # ------------------------------------------------------------------
    # 6. Determine created_utc
    # ------------------------------------------------------------------
    created_utc = os.environ.get("PACKAGER_NOW_UTC")
    if not created_utc:
        import datetime
        created_utc = datetime.datetime.utcnow().isoformat() + "Z"

    # ------------------------------------------------------------------
    # 7. Build EpisodeBundle dict (without bundle_hash)
    # ------------------------------------------------------------------
    bundle: dict = {
        "schema_id": "EpisodeBundle",
        "schema_version": "0.0.1",
        "producer": {"repo": "orchestrator", "component": "Packager"},
        "episode_id": episode_id,
        "source_run_dir": "/".join(run_dir.parts[-2:]),
        "run_id": run_id,
        "created_utc": created_utc,
        "artifacts": artifacts_sorted,
    }

    # ------------------------------------------------------------------
    # 8. Compute bundle_hash and write EpisodeBundle.json
    # ------------------------------------------------------------------
    bundle["bundle_hash"] = hash_artifact(
        {k: v for k, v in bundle.items() if k != "created_utc"}
    )
    (bundle_root / "EpisodeBundle.json").write_text(
        json.dumps(bundle, indent=2), encoding="utf-8"
    )

    return bundle_root


def _resolve_uri(uri: str, run_dir: Path) -> Path:
    """Resolve a file:// URI or bare path to an absolute Path.

    Supported: file:// URIs and bare (relative or absolute) paths.
    Any other URI scheme raises ValueError.
    """
    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme == "file":
        return Path(urllib.parse.unquote(parsed.path))
    if parsed.scheme not in ("", "file"):
        raise ValueError(
            f"Unsupported URI scheme: {parsed.scheme!r}; "
            "only file:// URIs and bare paths are supported"
        )
    p = Path(uri)
    if p.is_absolute():
        return p
    return run_dir / p
