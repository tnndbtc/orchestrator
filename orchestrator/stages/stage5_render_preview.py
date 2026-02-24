"""Stage 5: Render preview via video CLI, or fall back to placeholder stub.

Invocation contract (§41.4):
    video render
        --manifest  <AssetManifest_final.json>
        --plan      <RenderPlan.json>
        --out       <render_preview/RenderOutput.json>
        --video     <render_preview/output.mp4>

RenderOutput.json is read from disk after the call — never from stdout (§41
file-flow contract).

Falls back to a placeholder-stub RenderOutput (no error raised) when:
  - All resolved assets in RenderPlan are placeholders (no real media to render), OR
  - The `video` binary is not installed in the active environment.
"""

import json
from pathlib import Path

from ..registry import ArtifactRegistry
from ..utils.agent_bin import call_agent, find_agent_bin


def _uri_to_path(uri: str) -> Path | None:
    """Return Path for a file:// URI, or None for any other scheme."""
    if uri.startswith("file://"):
        return Path(uri[len("file://"):])
    return None


def run(project_config: dict, run_id: str, registry: ArtifactRegistry) -> dict:
    """Render preview video via `video render` CLI, or produce a placeholder stub.

    Reads:  RenderPlan.json, AssetManifest_final.json
    Writes: render_preview/RenderOutput.json  (from video CLI or stub)
            render_preview/output.mp4          (from video CLI; absent in stub path)
            RenderOutput.json                  (registry artifact)
    """
    pid      = project_config["id"]
    run_dir  = registry.run_dir(pid, run_id)
    plan_path     = registry.artifact_path(pid, run_id, "RenderPlan")
    manifest_path = registry.artifact_path(pid, run_id, "AssetManifest_final")

    # ------------------------------------------------------------------
    # 1. Check for all-placeholder RenderPlan — skip renderer entirely.
    # ------------------------------------------------------------------
    try:
        plan     = json.loads(plan_path.read_text(encoding="utf-8"))
        resolved = plan.get("resolved_assets", [])
        all_placeholder = bool(resolved) and all(
            a.get("is_placeholder", False) for a in resolved
        )
    except (OSError, json.JSONDecodeError):
        resolved        = None
        all_placeholder = False

    def _placeholder_ro(reason: str) -> dict:
        try:
            shotlist         = registry.read_artifact(pid, run_id, "ShotList")
            timing_lock_hash = shotlist.get("timing_lock_hash", "")
        except Exception:
            timing_lock_hash = ""
        return {
            "schema_id":        "RenderOutput",
            "schema_version":   "1.0.0",
            "output_id":        f"placeholder-{run_id}",
            "video_uri":        "placeholder://video/preview.mp4",
            "captions_uri":     "placeholder://captions/preview.srt",
            "hashes":           {"video_sha256": None, "captions_sha256": None},
            "provenance":       {"timing_lock_hash": timing_lock_hash},
            "placeholder_render": True,
            "placeholder_reason": reason,
        }

    def _write_and_return(ro: dict) -> dict:
        registry.write_artifact(
            pid, run_id, "RenderOutput", ro,
            parent_refs=[],
            creation_params={
                "project_id": pid,
                "run_id":     run_id,
                "stage":      "stage5_render_preview",
            },
        )
        return ro

    if resolved is not None and (not resolved or all_placeholder):
        return _write_and_return(_placeholder_ro(
            "All resolved assets are placeholders; renderer skipped. "
            "Provide real file:// URIs in AssetManifest.media.json to enable rendering."
        ))

    # ------------------------------------------------------------------
    # 2. Locate video binary; fall back to placeholder if not installed.
    # ------------------------------------------------------------------
    if find_agent_bin("video") is None:
        return _write_and_return(_placeholder_ro(
            "`video` binary not found in the active environment. "
            "Install the video-agent package to enable rendering:\n"
            "  pip install -e /path/to/video-agent"
        ))

    # ------------------------------------------------------------------
    # 3. Invoke video render CLI (§41.4 contract).
    # ------------------------------------------------------------------
    out_dir    = run_dir / "render_preview"
    out_dir.mkdir(parents=True, exist_ok=True)
    ro_path    = out_dir / "RenderOutput.json"
    video_path = out_dir / "output.mp4"

    result = call_agent(
        "video",
        [
            "render",
            "--manifest", str(manifest_path),
            "--plan",     str(plan_path),
            "--out",      str(ro_path),
            "--video",    str(video_path),
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"`video render` exited with code {result.returncode}.\n"
            f"stderr:\n{result.stderr.strip()}"
        )

    # ------------------------------------------------------------------
    # 4. Read RenderOutput from disk (§41 file-flow — not stdout).
    # ------------------------------------------------------------------
    try:
        ro = json.loads(ro_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"`video render` succeeded but RenderOutput.json was not written "
            f"to {ro_path}: {exc}"
        ) from exc

    # ------------------------------------------------------------------
    # 5. Verify file:// URIs exist on disk.
    # ------------------------------------------------------------------
    for field in ("video_uri", "captions_uri"):
        p = _uri_to_path(str(ro.get(field, "")))
        if p is not None and not p.exists():
            raise FileNotFoundError(
                f"`video render` reported {field}={ro[field]!r} "
                f"but the file does not exist: {p}"
            )

    # ------------------------------------------------------------------
    # 6. Write artifact to registry.
    # ------------------------------------------------------------------
    return _write_and_return(ro)
