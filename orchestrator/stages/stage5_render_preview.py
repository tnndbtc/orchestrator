# stage5_render_preview.py
import json, os, subprocess, sys
from pathlib import Path


def _uri_to_path(uri: str) -> Path | None:
    """Return Path for a file:// URI, or None for any other scheme."""
    if uri.startswith("file://"):
        return Path(uri[len("file://"):])
    return None


def run(project_config, run_id, registry):
    pid = project_config["id"]

    # 1) Check for all-placeholder RenderPlan — skip renderer entirely
    plan_path     = registry.artifact_path(pid, run_id, "RenderPlan")
    manifest_path = registry.artifact_path(pid, run_id, "AssetManifest_final")
    try:
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        resolved = plan.get("resolved_assets", [])
        all_placeholder = bool(resolved) and all(a.get("is_placeholder", False) for a in resolved)
    except (OSError, json.JSONDecodeError):
        resolved = None  # RenderPlan missing — fall through to env var check
        all_placeholder = False

    if resolved is not None and (not resolved or all_placeholder):
        # Read timing_lock_hash from ShotList so provenance is consistent
        try:
            shotlist = registry.read_artifact(pid, run_id, "ShotList")
            timing_lock_hash = shotlist.get("timing_lock_hash", "")
        except (OSError, json.JSONDecodeError, KeyError):
            timing_lock_hash = ""

        ro = {
            "schema_id": "RenderOutput",
            "schema_version": "1.0.0",
            "output_id": f"placeholder-{run_id}",
            "video_uri": "placeholder://video/preview.mp4",
            "captions_uri": "placeholder://captions/preview.srt",
            "hashes": {"video_sha256": None, "captions_sha256": None},
            "provenance": {"timing_lock_hash": timing_lock_hash},
            "placeholder_render": True,
            "placeholder_reason": (
                "All resolved assets are placeholders; renderer skipped. "
                "Provide real file:// URIs in AssetManifest.media.json to enable rendering."
            ),
        }
        registry.write_artifact(
            pid, run_id, "RenderOutput", ro,
            parent_refs=[],
            creation_params={"project_id": pid, "run_id": run_id, "stage": "stage5_render_preview"},
        )
        return ro

    # 2) Resolve renderer from env var
    video_repo = os.environ.get("VIDEO_RENDERER_REPO")
    if not video_repo:
        raise EnvironmentError(
            "Environment variable VIDEO_RENDERER_REPO is not set. "
            "Set it to the root of the video renderer repository, e.g.:\n"
            "  export VIDEO_RENDERER_REPO=/path/to/video"
        )
    renderer        = Path(video_repo) / "scripts" / "render_from_orchestrator.py"
    renderer_python = os.environ.get("VIDEO_RENDERER_PYTHON", sys.executable)

    out_dir = Path(registry.run_dir(pid, run_id)) / "render_preview"
    out_dir.mkdir(parents=True, exist_ok=True)

    # 3) Invoke renderer; capture stdout JSON
    result = subprocess.run(
        [renderer_python, str(renderer),
         "--asset-manifest", str(manifest_path),
         "--render-plan",    str(plan_path),
         "--out-dir",        str(out_dir)],
        capture_output=True,
    )
    if result.returncode != 0:
        stderr_snippet = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(
            f"Renderer exited with code {result.returncode}.\nstderr:\n{stderr_snippet}"
        )

    # 3) Parse stdout — renderer is source of truth, no modifications
    try:
        ro = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Renderer stdout is not valid JSON: {exc}\n"
            f"stdout (first 500 chars):\n"
            f"{result.stdout.decode('utf-8', errors='replace')[:500]}"
        ) from exc

    # 4) Write artifact (validates against schema)
    registry.write_artifact(
        pid, run_id, "RenderOutput", ro,
        parent_refs=[],
        creation_params={"project_id": pid, "run_id": run_id, "stage": "stage5_render_preview"},
    )

    # 5) Verify file:// URIs actually exist on disk
    for field in ("video_uri", "captions_uri"):
        p = _uri_to_path(str(ro.get(field, "")))
        if p is not None and not p.exists():
            raise FileNotFoundError(
                f"Renderer reported {field}={ro[field]!r} "
                f"but the file does not exist: {p}"
            )

    return ro
